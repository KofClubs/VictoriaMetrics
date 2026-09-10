package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// downsampleSpaceCacheLifetime 覆盖 fs.MustGetFreeSpace 的两秒缓存有效期。
const downsampleSpaceCacheLifetime = 2 * time.Second

var downsampleSpaceLogger = logger.WithThrottler("downsamplingSpace", time.Minute)

// 仅标记降采样文件作业的普通失败；原始归并错误和程序不变量仍保留原有语义。
var errDownsampleMergeFailed = errors.New("[downsampling] merge failed")

var downsampleMergeLogger = logger.WithThrottler("downsamplingMerge", time.Minute)

var downsampleDiskBudget downsampleSpaceBudget

// downsampleSpaceBudget 在同一进程的全部目录间保守共享预算，避免并发作业重复使用同一空闲空间。
type downsampleSpaceBudget struct {
	mu           sync.Mutex
	reserved     uint64
	retiredBytes uint64
	retired      []downsampleRetiredSpace
}

type downsampleRetiredSpace struct {
	size      uint64
	expiresAt time.Time
}

// downsamplePartCandidate 以输出空间上界参与调度，不修改共享 part 的实际 size。
type downsamplePartCandidate struct {
	pw   *partWrapper
	size uint64
}

// checkDownsamplingOpen 在 storage 持有 flock 时检查配置和活动文件，不修改目录或清单。
func checkDownsamplingOpen(path string, opts OpenOptions) error {
	if opts.DownsamplingEnabled && GetDedupInterval() != 0 {
		return fmt.Errorf("[downsampling] -storage.downsampling.enabled requires -dedup.minScrapeInterval=0; got %dms", GetDedupInterval())
	}
	dataPath := filepath.Join(path, dataDirname)
	rootPaths := [3]string{
		filepath.Join(dataPath, smallDirname),
		filepath.Join(dataPath, bigDirname),
		filepath.Join(dataPath, indexdbDirname),
	}
	var roots [3]map[string]bool
	partitionNames := make(map[string]bool)
	for i, rootPath := range rootPaths {
		names, err := readDownsamplePartitionNames(rootPath)
		if err != nil {
			return err
		}
		roots[i] = names
		for name := range names {
			partitionNames[name] = true
		}
	}
	names := make([]string, 0, len(partitionNames))
	for name := range partitionNames {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		paths := [2]string{filepath.Join(rootPaths[0], name), filepath.Join(rootPaths[1], name)}
		active := [2]bool{roots[0][name], roots[1][name]}
		partNames, err := readDownsampleActivePartNames(paths, active)
		if err != nil {
			return err
		}
		for i, names := range partNames {
			for _, partName := range names {
				partPath := filepath.Join(paths[i], partName)
				info, err := os.Stat(partPath)
				if err != nil {
					return fmt.Errorf("[downsampling] cannot inspect active part %q: %w", partPath, err)
				}
				if !info.IsDir() {
					return fmt.Errorf("[downsampling] active part %q is not a directory", partPath)
				}
				downsampled, err := detectDownsampleFormat(partPath)
				if err != nil {
					return fmt.Errorf("[downsampling] cannot inspect active part %q: %w", partPath, err)
				}
				if downsampled && !opts.DownsamplingEnabled {
					return fmt.Errorf("[downsampling] active part %q uses downsampling format %d; enable -storage.downsampling.enabled to open this storage", partPath, downsampleVersion)
				}
			}
		}
	}
	return nil
}

// readDownsamplePartitionNames 沿用 table 的目录识别规则，但不删除未完整清理的 partition。
// IndexDB 仅参与 partition 名称发现，不读取其中的索引文件。
func readDownsamplePartitionNames(path string) (map[string]bool, error) {
	des, err := os.ReadDir(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("[downsampling] cannot enumerate partitions at %q: %w", path, err)
	}
	names := make(map[string]bool)
	for _, de := range des {
		if !fs.IsDirOrSymlink(de) || de.Name() == snapshotsDirname {
			continue
		}
		partitionPath := filepath.Join(path, de.Name())
		entries, err := os.ReadDir(partitionPath)
		if err != nil {
			return nil, fmt.Errorf("[downsampling] cannot inspect partition directory %q: %w", partitionPath, err)
		}
		// 与 fs.IsPartiallyRemovedDir 保持一致：空目录和删除标记均不属于活动集合。
		partiallyRemoved := len(entries) == 0
		for _, entry := range entries {
			if !entry.IsDir() && entry.Name() == ".delete-this-dir" {
				partiallyRemoved = true
				break
			}
		}
		if partiallyRemoved {
			continue
		}
		var tr TimeRange
		if err := tr.fromPartitionName(de.Name()); err != nil {
			return nil, fmt.Errorf("[downsampling] invalid partition directory %q: %w", partitionPath, err)
		}
		names[de.Name()] = true
	}
	return names, nil
}

// readDownsampleActivePartNames 优先读取 parts.json；历史目录缺少清单时按原有规则发现 part。
func readDownsampleActivePartNames(paths [2]string, active [2]bool) ([2][]string, error) {
	var names [2][]string
	partsFile := filepath.Join(paths[0], partsFilename)
	var data []byte
	err := os.ErrNotExist
	if active[0] {
		data, err = os.ReadFile(partsFile)
	}
	if err == nil {
		partNames, err := parseDownsamplePartNames(data)
		if err != nil {
			return names, fmt.Errorf("[downsampling] cannot parse active part manifest %q: %w", partsFile, err)
		}
		names = [2][]string{partNames.Small, partNames.Big}
	} else if errors.Is(err, os.ErrNotExist) {
		for i, path := range paths {
			if !active[i] {
				continue
			}
			des, err := os.ReadDir(path)
			if err != nil {
				return names, fmt.Errorf("[downsampling] cannot enumerate historical parts at %q: %w", path, err)
			}
			for _, de := range des {
				if fs.IsDirOrSymlink(de) && !isSpecialDir(de.Name()) {
					names[i] = append(names[i], de.Name())
				}
			}
		}
	} else {
		return names, fmt.Errorf("[downsampling] cannot read active part manifest %q: %w", partsFile, err)
	}
	for i, partNames := range names {
		seen := make(map[string]bool, len(partNames))
		for _, name := range partNames {
			if name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\`) || isSpecialDir(name) {
				return names, fmt.Errorf("[downsampling] invalid active part name %q in %q", name, partsFile)
			}
			if !active[i] {
				return names, fmt.Errorf("[downsampling] active part %q is listed in %q, but its partition directory is missing or marked for deletion", filepath.Join(paths[i], name), partsFile)
			}
			if seen[name] {
				return names, fmt.Errorf("[downsampling] duplicate active part %q in %q", filepath.Join(paths[i], name), partsFile)
			}
			seen[name] = true
		}
	}
	return names, nil
}

// parseDownsamplePartNames 拒绝重复字段和未知字段，防止损坏清单隐式丢失活动 part。
func parseDownsamplePartNames(data []byte) (partNamesJSON, error) {
	var names partNamesJSON
	d := json.NewDecoder(bytes.NewReader(data))
	token, err := d.Token()
	if err != nil {
		return names, err
	}
	if token != json.Delim('{') {
		return names, fmt.Errorf("[downsampling] expected a JSON object")
	}
	seen := make(map[string]bool, 2)
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return names, err
		}
		key := token.(string)
		canonicalKey := strings.ToLower(key)
		if seen[canonicalKey] {
			return names, fmt.Errorf("[downsampling] duplicate field %q", key)
		}
		seen[canonicalKey] = true
		var dst *[]string
		switch canonicalKey {
		case "small":
			dst = &names.Small
		case "big":
			dst = &names.Big
		default:
			return names, fmt.Errorf("[downsampling] unknown field %q", key)
		}
		if err := d.Decode(dst); err != nil {
			return names, fmt.Errorf("[downsampling] invalid %q list: %w", key, err)
		}
	}
	if _, err := d.Token(); err != nil {
		return names, err
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return names, fmt.Errorf("[downsampling] unexpected trailing manifest data")
	}
	return names, nil
}

// mergeDownsampleParts 仅处理文件目标，成功发布前始终保留全部源 part。
func (pt *partition) mergeDownsampleParts(pws []*partWrapper, dstPartType partType, dstPartPath string, stopCh <-chan struct{}, startTime time.Time) (err error) {
	defer func() {
		if err != nil {
			err = errors.Join(errDownsampleMergeFailed, err)
			if !errors.Is(err, errForciblyStopped) && !errors.Is(err, errDownsampleNoSpace) {
				downsampleMergeLogger.Warnf("[downsampling] merge stopped for %q: %s", pt.name, err)
			}
		}
	}()
	if isDedupEnabled() {
		return fmt.Errorf("[downsampling] cannot merge with deduplication enabled")
	}
	if dstPartType == partInmemory || dstPartPath == "" {
		return fmt.Errorf("[downsampling] merge requires a file destination")
	}
	defer func() {
		// 外部进程也可能耗尽磁盘；实际写入错误与预检查不足使用相同的重试语义。
		if errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT) {
			err = errors.Join(errDownsampleNoSpace, err)
		}
		if errors.Is(err, errDownsampleNoSpace) {
			downsampleSpaceLogger.Warnf("[downsampling] merge postponed for %q: %s", pt.name, err)
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
		return fmt.Errorf("[downsampling] unsupported downsampling destination %d", dstPartType)
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
				downsampleMergeLogger.Warnf("[downsampling] cannot clean unpublished downsampling target %q: %s", dstPartPath, cleanupErr)
			}
			err = errors.Join(err, cleanupErr)
		}
	}()
	m := getDownsampleMerger()
	defer func() {
		cleanupErr := putDownsampleMerger(m)
		if cleanupErr != nil {
			downsampleMergeLogger.Warnf("[downsampling] cannot release downsampling readers for %q: %s", dstPartPath, cleanupErr)
		}
		err = errors.Join(err, cleanupErr)
	}()
	deadline := startTime.UnixMilli() - pt.s.retentionMsecs
	stats, err := m.Merge(pws, w, stopCh, pt.idb.getDeletedMetricIDs(), deadline)
	rowsMerged.Add(stats.rowsMerged)
	rowsDeleted.Add(stats.rowsDeleted)
	if err != nil {
		return fmt.Errorf("[downsampling] cannot merge downsampling part %q: %w", dstPartPath, err)
	}
	ph, err := w.Finish()
	if err != nil {
		return err
	}
	// Merge 返回前已归还全部 reader 和解码对象；发布前仍使用调用方的取消信号。
	select {
	case <-stopCh:
		return fmt.Errorf("[downsampling] merge stopped before publication: %w", errForciblyStopped)
	default:
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
			return fmt.Errorf("[downsampling] cannot open unpublished downsampling part %q: %w", dstPartPath, err)
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
					downsampleMergeLogger.Warnf("[downsampling] cannot close unpublished downsampling part %q: %s", dstPartPath, cleanupErr)
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
			err = fmt.Errorf("[downsampling] publication stopped: %w", errForciblyStopped)
			return
		default:
		}
		inmemory, removedInmemory := removeParts(append([]*partWrapper(nil), pt.inmemoryParts...), m)
		small, removedSmall := removeParts(append([]*partWrapper(nil), pt.smallParts...), m)
		big, removedBig := removeParts(append([]*partWrapper(nil), pt.bigParts...), m)
		if removedInmemory+removedSmall+removedBig != len(m) {
			logger.Panicf("[downsampling] BUG: unexpected number of parts removed from downsampling sources")
		}
		if pwNew != nil {
			switch dstPartType {
			case partSmall:
				small = append(small, pwNew)
			case partBig:
				big = append(big, pwNew)
			default:
				logger.Panicf("[downsampling] BUG: unknown downsampling partType=%d", dstPartType)
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
		err = fmt.Errorf("[downsampling] manifest for %q was published, but directory sync failed; keeping old source files: %w", pt.name, err)
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
		logger.Panicf("[downsampling] BUG: cannot marshal downsampling part names: %s", err)
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
		return fmt.Errorf("[downsampling] cannot create temporary manifest %q: %w", tmpPath, err)
	}
	defer func() {
		if f != nil {
			if closeErr := f.Abort(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("[downsampling] cannot close temporary manifest %q: %w", tmpPath, closeErr))
			}
			f = nil
		}
		if tmpPath == "" {
			return
		}
		if e := os.Remove(tmpPath); e != nil && !os.IsNotExist(e) {
			err = errors.Join(err, fmt.Errorf("[downsampling] cannot remove unpublished manifest %q: %w", tmpPath, e))
			// 保留首次清理错误，并为短暂文件系统失败再尝试一次。
			if retryErr := os.Remove(tmpPath); retryErr != nil && !os.IsNotExist(retryErr) {
				err = errors.Join(err, fmt.Errorf("[downsampling] cannot remove unpublished manifest %q on retry: %w", tmpPath, retryErr))
			}
		}
	}()
	if n, err := f.Write(data); err != nil {
		return fmt.Errorf("[downsampling] cannot write temporary manifest %q: %w", tmpPath, err)
	} else if n != len(data) {
		return fmt.Errorf("[downsampling] cannot write temporary manifest %q: %w", tmpPath, io.ErrShortWrite)
	}
	closeErr := f.Close()
	f = nil
	if closeErr != nil {
		return fmt.Errorf("[downsampling] cannot close temporary manifest %q: %w", tmpPath, closeErr)
	}
	if pt.downsampleTestHook != nil {
		if err := pt.downsampleTestHook("before-commit", tmpPath); err != nil {
			return err
		}
	}
	select {
	case <-stopCh:
		return fmt.Errorf("[downsampling] manifest commit stopped: %w", errForciblyStopped)
	default:
	}
	if err := os.Rename(tmpPath, filepath.Join(pt.smallPartsPath, partsFilename)); err != nil {
		return fmt.Errorf("[downsampling] cannot commit temporary manifest %q: %w", tmpPath, err)
	}
	// rename 已消费临时路径，后续只同步已发布清单，不再清理该路径。
	tmpPath = ""
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

// reserveDownsampleSpace 为一个作业预留完整目标空间；release 可以重复调用。
// 空闲空间已经扣除现有源文件，因此这里只预留额外输出，不能以删除源文件作为可用空间。
func reserveDownsampleSpace(path string, size uint64) (func(), error) {
	return reserveDownsampleSpaceWithBudget(&downsampleDiskBudget, path, size, freeDiskSpaceLimitBytes, fs.MustGetFreeSpace, time.Now)
}

func reserveDownsampleSpaceWithBudget(b *downsampleSpaceBudget, path string, size, minimumFree uint64,
	getFree func(string) uint64, now func() time.Time) (func(), error) {
	if size == 0 {
		return func() {}, nil
	}
	err := func() error {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.expireLocked(now())
		available := getFree(path)
		held := addDownsampleSpace(b.reserved, b.retiredBytes)
		if err := checkDownsampleAvailableSpace(available, held, size, minimumFree); err != nil {
			return err
		}
		b.reserved += size
		return nil
	}()
	if err != nil {
		return nil, fmt.Errorf("[downsampling] cannot reserve %d bytes at %q: %w", size, path, err)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			t := now()
			b.expireLocked(t)
			b.reserved -= size
			// 已写入的目标文件仍占磁盘；缓存刷新前不得把其预算立即交给后续作业。
			b.retiredBytes += size
			b.retired = append(b.retired, downsampleRetiredSpace{size: size, expiresAt: t.Add(downsampleSpaceCacheLifetime)})
		})
	}, nil
}

func (b *downsampleSpaceBudget) expireLocked(now time.Time) {
	i := 0
	for i < len(b.retired) && !now.Before(b.retired[i].expiresAt) {
		b.retiredBytes -= b.retired[i].size
		i++
	}
	if i != 0 {
		copy(b.retired, b.retired[i:])
		clear(b.retired[len(b.retired)-i:])
		b.retired = b.retired[:len(b.retired)-i]
	}
}
