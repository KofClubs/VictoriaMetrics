package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

var downsampleSpaceLogger = logger.WithThrottler("downsamplingSpace", time.Minute)

// 仅标记降采样文件作业的普通失败；原始归并错误和程序不变量仍保留原有语义。
var errDownsampleMergeFailed = errors.New("downsampling merge failed")

var downsampleMergeLogger = logger.WithThrottler("downsamplingMerge", time.Minute)

// mergeDownsampleParts 仅处理文件目标，成功发布前始终保留全部源 part。
func (pt *partition) mergeDownsampleParts(pws []*partWrapper, dstPartType partType, dstPartPath string, stopCh <-chan struct{}, startTime time.Time) (err error) {
	defer func() {
		if err != nil {
			err = errors.Join(errDownsampleMergeFailed, err)
			if !errors.Is(err, errForciblyStopped) && !errors.Is(err, errDownsampleNoSpace) {
				downsampleMergeLogger.Warnf("downsampling merge stopped for %q: %s", pt.name, err)
			}
		}
	}()
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
	published := false
	defer func() {
		if !published {
			cleanupErr := w.Abort()
			if cleanupErr != nil {
				// 取消本身不告警，但不能让 errForciblyStopped 隐藏清理失败。
				downsampleMergeLogger.Warnf("cannot clean unpublished downsampling target %q: %s", dstPartPath, cleanupErr)
			}
			err = errors.Join(err, cleanupErr)
		}
	}()
	m := getDownsampleMerger()
	defer func() {
		cleanupErr := putDownsampleMerger(m)
		if cleanupErr != nil {
			downsampleMergeLogger.Warnf("cannot release downsampling readers for %q: %s", dstPartPath, cleanupErr)
		}
		err = errors.Join(err, cleanupErr)
	}()
	deadline := startTime.UnixMilli() - pt.s.retentionMsecs
	stats, err := m.Merge(pws, w, stopCh, pt.idb.getDeletedMetricIDs(), deadline)
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
	// Merge 返回前已关闭自有 reader；这里归还剩余工作缓冲。
	if err := m.Reset(); err != nil {
		return err
	}
	if pt.downsampleTestHook != nil {
		if err := pt.downsampleTestHook("before-open", dstPartPath); err != nil {
			return err
		}
	}
	var pwNew *partWrapper
	if ph.RowsCount == 0 {
		if err := w.Abort(); err != nil {
			return err
		}
	} else {
		p, err := openDownsamplePart(dstPartPath)
		if err != nil {
			return fmt.Errorf("cannot open unpublished downsampling part %q: %w", dstPartPath, err)
		}
		pwNew = &partWrapper{p: p}
		pwNew.incRef()
		defer func() {
			if !published {
				var cleanupErr error
				for _, f := range []*os.File{p.dsTimestampsFile, p.dsValuesFile, p.dsIndexFile} {
					if f != nil {
						cleanupErr = errors.Join(cleanupErr, f.Close())
					}
				}
				ibCache.RemoveBlocksForPart(p)
				if cleanupErr != nil {
					downsampleMergeLogger.Warnf("cannot close unpublished downsampling part %q: %s", dstPartPath, cleanupErr)
				}
				err = errors.Join(err, cleanupErr)
			}
		}()
	}
	return pt.publishDownsampleParts(pws, pwNew, dstPartType, stopCh, &published)
}

// publishDownsampleParts 以 manifest rename 为提交点。提交前不改变活动集合；
// 提交后即使目录同步失败，内存集合也必须与 manifest 一致，且不能 Abort 目标。
func (pt *partition) publishDownsampleParts(pws []*partWrapper, pwNew *partWrapper, dstPartType partType, stopCh <-chan struct{}, published *bool) (err error) {
	m := makeMapFromPartWrappers(pws)
	pt.partsLock.Lock()
	func() {
		defer pt.partsLock.Unlock()
		select {
		case <-stopCh:
			err = errForciblyStopped
			return
		default:
		}
		inmemory, removedInmemory := removeParts(append([]*partWrapper(nil), pt.inmemoryParts...), m)
		small, removedSmall := removeParts(append([]*partWrapper(nil), pt.smallParts...), m)
		big, removedBig := removeParts(append([]*partWrapper(nil), pt.bigParts...), m)
		if removedInmemory+removedSmall+removedBig != len(m) {
			logger.Panicf("BUG: unexpected number of parts removed from downsampling sources")
		}
		if pwNew != nil {
			switch dstPartType {
			case partSmall:
				small = append(small, pwNew)
			case partBig:
				big = append(big, pwNew)
			default:
				logger.Panicf("BUG: unknown downsampling partType=%d", dstPartType)
			}
		}
		if removedSmall+removedBig > 0 || pwNew != nil {
			err = pt.writeDownsamplePartNames(small, big, stopCh, published)
			if !*published {
				return
			}
		} else {
			// 全部内存源过期，既无文件目标也不改变磁盘 manifest。
			*published = true
		}
		pt.inmemoryParts, pt.smallParts, pt.bigParts = inmemory, small, big
		if err == nil && pwNew != nil {
			if dstPartType == partSmall {
				pt.startSmallPartsMergerLocked()
			} else {
				pt.startBigPartsMergerLocked()
			}
		}
	}()
	if !*published {
		return err
	}
	if err != nil {
		err = fmt.Errorf("downsampling manifest for %q was published, but directory sync failed; keeping old source files: %w", pt.name, err)
	}
	for _, pw := range pws {
		// rename 后目录同步失败时保留旧磁盘文件；内存源已由 fsync 完成的目标承载。
		// 不捕获引用计数等程序不变量的 panic。
		if err == nil {
			pw.mustDrop.Store(true)
		}
	}
	for _, pw := range pws {
		pw.decRef()
	}
	return err
}

func (pt *partition) writeDownsamplePartNames(small, big []*partWrapper, stopCh <-chan struct{}, published *bool) (err error) {
	data, err := json.Marshal(partNamesJSON{Small: getPartNames(small), Big: getPartNames(big)})
	if err != nil {
		logger.Panicf("BUG: cannot marshal downsampling part names: %s", err)
	}
	// 与 parts.json 位于同一目录，支持 small/big 挂载于不同文件系统。
	var f *filestream.Writer
	var tmpPath string
	for {
		tmpPath = filepath.Join(pt.smallPartsPath, fmt.Sprintf("%s.tmp.%d", partsFilename, pt.nextMergeIdx()))
		f, err = filestream.CreateExclusive(tmpPath, false)
		if !os.IsExist(err) {
			break
		}
	}
	if err != nil {
		return err
	}
	defer func() {
		if f != nil {
			err = errors.Join(err, f.Abort())
		}
		if e := os.Remove(tmpPath); e != nil && !os.IsNotExist(e) {
			err = errors.Join(err, fmt.Errorf("cannot remove unpublished manifest %q: %w", tmpPath, e))
			// 保留首次清理错误，并为短暂文件系统失败再尝试一次。
			if retryErr := os.Remove(tmpPath); retryErr != nil && !os.IsNotExist(retryErr) {
				err = errors.Join(err, fmt.Errorf("cannot remove unpublished manifest %q on retry: %w", tmpPath, retryErr))
			}
		}
	}()
	if n, err := f.Write(data); err != nil {
		return err
	} else if n != len(data) {
		return io.ErrShortWrite
	}
	closeErr := f.Close()
	f = nil
	if closeErr != nil {
		return closeErr
	}
	if pt.downsampleTestHook != nil {
		if err := pt.downsampleTestHook("before-commit", tmpPath); err != nil {
			return err
		}
	}
	select {
	case <-stopCh:
		return errForciblyStopped
	default:
	}
	if err := os.Rename(tmpPath, filepath.Join(pt.smallPartsPath, partsFilename)); err != nil {
		return err
	}
	*published = true
	if pt.downsampleTestHook != nil {
		if err := pt.downsampleTestHook("sync-commit-dir", pt.smallPartsPath); err != nil {
			return err
		}
	}
	return syncDownsampleDir(pt.smallPartsPath)
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
