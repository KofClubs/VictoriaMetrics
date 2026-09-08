package storage

import (
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

var downsampleSpaceLogger = logger.WithThrottler("downsamplingSpace", time.Minute)

// mergeDownsampleParts 仅处理文件目标，成功发布前始终保留全部源 part。
func (pt *partition) mergeDownsampleParts(pws []*partWrapper, dstPartType partType, dstPartPath string, stopCh <-chan struct{}, startTime time.Time) (err error) {
	if isDedupEnabled() {
		return fmt.Errorf("downsampling cannot run with deduplication enabled")
	}
	if dstPartType == partInmemory || dstPartPath == "" {
		return fmt.Errorf("downsampling requires a file destination")
	}
	defer func() {
		// 外部进程也可能耗尽磁盘；实际写入错误与预检查不足使用相同的重试语义。
		if errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT) {
			err = errors.Join(errDownsampleNoSpace, err)
		}
		if errors.Is(err, errDownsampleNoSpace) {
			downsampleSpaceLogger.Warnf("downsampling merge postponed for %q: %s", pt.name, err)
		}
	}()
	budget := estimateDownsamplePartSize(pws)
	release, err := reserveDownsampleSpace(filepath.Dir(dstPartPath), budget)
	if err != nil {
		return err
	}
	defer release()

	var rowsMerged, rowsDeleted, mergesCount *atomic.Uint64
	var active *atomic.Int64
	switch dstPartType {
	case partSmall:
		rowsMerged, rowsDeleted = &pt.smallRowsMerged, &pt.smallRowsDeleted
		active, mergesCount = &pt.activeSmallMerges, &pt.smallMergesCount
	case partBig:
		rowsMerged, rowsDeleted = &pt.bigRowsMerged, &pt.bigRowsDeleted
		active, mergesCount = &pt.activeBigMerges, &pt.bigMergesCount
	default:
		return fmt.Errorf("unsupported downsampling destination %d", dstPartType)
	}
	active.Add(1)
	defer active.Add(-1)
	defer mergesCount.Add(1)

	var sourceRows, sourceBlocks uint64
	for _, pw := range pws {
		sourceRows += pw.p.ph.RowsCount
		sourceBlocks += pw.p.ph.BlocksCount
	}
	compressLevel := getCompressLevel(float64(sourceRows) / float64(max(sourceBlocks, 1)))
	w := getDownsampleWriter()
	defer putDownsampleWriter(w)
	if err := w.Init(dstPartPath, compressLevel); err != nil {
		return err
	}
	publicationStarted := false
	defer func() {
		if !publicationStarted {
			err = errors.Join(err, w.Abort())
		}
	}()
	m := getDownsampleMerger()
	defer putDownsampleMerger(m)
	deadline := startTime.UnixMilli() - pt.s.retentionMsecs
	stats, err := m.Merge(pws, w, stopCh, pt.idb.getDeletedMetricIDs(), deadline, downsampleWindowBuckets)
	rowsMerged.Add(stats.rowsMerged)
	rowsDeleted.Add(stats.rowsDeleted)
	if err != nil {
		return fmt.Errorf("cannot merge downsampling part %q: %w", dstPartPath, err)
	}
	ph, err := w.Finish()
	if err != nil {
		return err
	}
	if err := m.checkStopped(); err != nil {
		return err
	}
	// 发布前关闭归并 reader，避免旧源删除时仍被工作游标占用，尤其是 NFS 文件句柄。
	m.Reset()
	pwNew := pt.openCreatedPart(&ph, pws, nil, dstPartPath)
	// 从此由活动清单发布流程接管目标；清单提交后即使源清理失败，也不能删除目标。
	// 提交前的致命错误可能留下孤立目录，由重启时的既有清单清理流程回收。
	publicationStarted = true
	pt.swapSrcWithDstParts(pws, pwNew, dstPartType)
	return nil
}

// filePartExpired 按两种目标区间保守清理磁盘源，原始 inmemory 使用原有判断。
func (pt *partition) filePartExpired(p *part, deadline int64) bool {
	if !pt.s.downsamplingEnabled {
		return p.ph.MaxTimestamp < deadline
	}
	for _, resolution := range downsampleResolutions {
		end, err := downsampleBucketEnd(p.ph.MaxTimestamp, resolution)
		if err != nil || end > deadline {
			return false
		}
	}
	return true
}

// downsamplePartCandidate 以输出空间上界参与调度，不修改共享 part 的实际 size。
type downsamplePartCandidate struct {
	pw   *partWrapper
	size uint64
}

// getFilePartsToMerge 在降采样模式下使用文件输出上界控制候选和并发预算。
// 调用方持有 partsLock；原始内存归并仍调用原有 getPartsToMerge。
func (pt *partition) getFilePartsToMerge(pws []*partWrapper, maxOutBytes uint64) []*partWrapper {
	if !pt.s.downsamplingEnabled {
		return getPartsToMerge(pws, maxOutBytes)
	}
	maxInputSize := uint64(float64(maxOutBytes) / minMergeMultiplier)
	candidates := make([]downsamplePartCandidate, 0, len(pws))
	for _, pw := range pws {
		if pw.isInMerge {
			continue
		}
		one := [1]*partWrapper{pw}
		size := estimateDownsamplePartSize(one[:])
		if size > maxInputSize {
			continue
		}
		candidates = append(candidates, downsamplePartCandidate{pw: pw, size: size})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].size == candidates[j].size {
			return candidates[i].pw.p.ph.MinTimestamp > candidates[j].pw.p.ph.MinTimestamp
		}
		return candidates[i].size < candidates[j].size
	})
	maxParts := min(defaultPartsToMerge, len(candidates))
	minParts := max((maxParts+1)/2, 2)
	var selected []downsamplePartCandidate
	var bestMultiplier float64
	for n := minParts; n <= maxParts; n++ {
		for start := 0; start+n <= len(candidates); start++ {
			a := candidates[start : start+n]
			if float64(a[0].size)*float64(n) < float64(a[n-1].size) {
				continue
			}
			var total uint64
			for _, candidate := range a {
				if math.MaxUint64-total < candidate.size {
					total = math.MaxUint64
					break
				}
				total += candidate.size
			}
			if total > maxOutBytes {
				break
			}
			multiplier := float64(total) / float64(a[n-1].size)
			if multiplier >= bestMultiplier {
				bestMultiplier, selected = multiplier, a
			}
		}
	}
	if bestMultiplier < max(float64(defaultPartsToMerge)/2, minMergeMultiplier) {
		return nil
	}
	result := make([]*partWrapper, 0, len(selected))
	for _, candidate := range selected {
		candidate.pw.isInMerge = true
		result = append(result, candidate.pw)
	}
	return result
}

// splitDownsampleMergeBatch 限制强制 merge 和最终 flush 的单次源数量。
// 原调度未找到均衡组合时可能返回全部源，剩余项必须留给后续批次。
func splitDownsampleMergeBatch(selected, remaining []*partWrapper) ([]*partWrapper, []*partWrapper) {
	if len(selected) <= downsampleMaxMergeSources {
		return selected, remaining
	}
	remaining = append(remaining, selected[downsampleMaxMergeSources:]...)
	return selected[:downsampleMaxMergeSources:downsampleMaxMergeSources], remaining
}
