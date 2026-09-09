package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// 使用真实 Storage/IndexDB 和源文件，但作业集合独立于后台 partition。
func newDownsampleFailurePartition(t *testing.T) (*partition, int64) {
	t.Helper()
	s := MustOpenStorage(t.TempDir(), OpenOptions{DownsamplingEnabled: true})
	t.Cleanup(s.MustClose)
	base := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour).UnixMilli()
	ptw := s.tb.MustGetPartition(base)
	t.Cleanup(func() { s.tb.PutPartition(ptw) })
	owner := ptw.pt
	pt := &partition{s: s, idb: owner.idb, name: owner.name, tr: owner.tr,
		smallPartsPath: owner.smallPartsPath, bigPartsPath: owner.bigPartsPath,
		indexDBPartsPath: owner.indexDBPartsPath, stopCh: make(chan struct{})}
	pt.mergeIdx.Store(0x10000)
	t.Cleanup(func() {
		close(pt.stopCh)
		pt.wg.Wait()
		for _, group := range [][]*partWrapper{pt.inmemoryParts, pt.smallParts, pt.bigParts} {
			for _, pw := range group {
				pw.decRef()
			}
		}
	})
	return pt, base
}

func addDownsampleFailureSources(t *testing.T, pt *partition, base int64, count int, kind partType) []*partWrapper {
	t.Helper()
	var result []*partWrapper
	for i := 0; i < count; i++ {
		rows := []rawRow{{TSID: TSID{AccountID: 11, ProjectID: 17, MetricID: uint64(i + 1)},
			Timestamp: base + 60000, Value: float64(i + 1), PrecisionBits: 64}}
		if kind == partInmemory {
			result = append(result, registerDownsampleTestInmemoryPart(pt, rows))
			continue
		}
		mp := getInmemoryPart()
		mp.InitFromRows(rows)
		path := pt.getDstPartPath(kind, uint64(i+1))
		mp.MustStoreToDisk(path)
		putInmemoryPart(mp)
		pw := &partWrapper{p: mustOpenFilePart(path), isInMerge: true}
		pw.incRef()
		if kind == partSmall {
			pt.smallParts = append(pt.smallParts, pw)
		} else {
			pt.bigParts = append(pt.bigParts, pw)
		}
		result = append(result, pw)
	}
	mustWritePartNames(pt.smallParts, pt.bigParts, pt.smallPartsPath)
	return result
}

func failureManifest(t *testing.T, pt *partition) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(pt.smallPartsPath, partsFilename))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func assertDownsampleFailurePreserved(t *testing.T, pt *partition, sources []*partWrapper, before []byte, targets []string) {
	t.Helper()
	if !bytes.Equal(before, failureManifest(t, pt)) {
		t.Fatal("失败作业修改了活动 manifest")
	}
	for _, source := range sources {
		if source.isInMerge || source.mustDrop.Load() || source.p == nil || source.refCount.Load() != 1 {
			t.Fatal("失败作业没有保留源或释放 isInMerge")
		}
		if source.p.path != "" {
			if _, err := os.Stat(source.p.path); err != nil {
				t.Fatal("失败作业删除了源文件", err)
			}
		}
	}
	for _, target := range targets {
		if _, err := os.Stat(target); !os.IsNotExist(err) {
			t.Fatalf("失败作业遗留未发布目标 %q: %v", target, err)
		}
	}
	files, err := filepath.Glob(filepath.Join(pt.smallPartsPath, partsFilename+".tmp.*"))
	if err != nil || len(files) != 0 {
		t.Fatalf("失败作业遗留临时 manifest: %v / %v", files, err)
	}
}

func TestDownsampleFailureBeforePublication(t *testing.T) {
	for _, stage := range []string{"cancel", "cancel-before-commit", "before-open", "invalid-target", "before-commit"} {
		t.Run(stage, func(t *testing.T) {
			pt, base := newDownsampleFailurePartition(t)
			sources := addDownsampleFailureSources(t, pt, base, 1, partSmall)
			before := failureManifest(t, pt)
			target := pt.getDstPartPath(partSmall, pt.mergeIdx.Load()+1)
			ch := make(chan struct{})
			pt.downsampleTestHook = func(got, path string) error {
				if stage == "cancel-before-commit" && got == "before-commit" {
					close(ch)
				}
				if stage == "invalid-target" && got == "before-open" {
					return os.Truncate(filepath.Join(path, indexFilename), 0)
				}
				if got == stage {
					return syscall.EIO
				}
				return nil
			}
			if stage == "cancel" {
				close(ch)
			}
			err := pt.mergeParts(sources, ch, true, false)
			if !errors.Is(err, errDownsampleMergeFailed) {
				t.Fatalf("错误未标记为降采样作业失败: %v", err)
			}
			if strings.HasPrefix(stage, "cancel") && !errors.Is(err, errForciblyStopped) {
				t.Fatalf("取消原因丢失: %v", err)
			}
			if (stage == "before-open" || stage == "before-commit") && !errors.Is(err, syscall.EIO) {
				t.Fatalf("I/O 根因丢失: %v", err)
			}
			assertDownsampleFailurePreserved(t, pt, sources, before, []string{target})
		})
	}
}

func TestDownsampleFailureSchedulers(t *testing.T) {
	for _, kind := range []partType{partSmall, partBig, partInmemory} {
		for _, cause := range []error{syscall.EIO, errForciblyStopped} {
			t.Run(fmt.Sprintf("kind-%d/%s", kind, cause), func(t *testing.T) {
				pt, base := newDownsampleFailurePartition(t)
				sources := addDownsampleFailureSources(t, pt, base, 8, kind)
				before := failureManifest(t, pt)
				pt.releasePartsToMerge(sources)
				for _, pw := range sources {
					pw.flushToDiskDeadline = time.Now().Add(-time.Hour)
				}
				var targets []string
				pt.downsampleTestHook = func(stage, path string) error {
					if stage == "before-open" {
						targets = append(targets, path)
						return cause
					}
					return nil
				}
				switch kind {
				case partSmall:
					pt.smallPartsMerger()
				case partBig:
					pt.bigPartsMerger()
				case partInmemory:
					pt.flushInmemoryPartsToFiles(false)
				}
				if len(targets) != 1 {
					t.Fatalf("没有执行并终止单个失败作业: %v", targets)
				}
				assertDownsampleFailurePreserved(t, pt, sources, before, targets)
			})
		}
	}
}

func TestDownsampleFailureStopsRemainingBatches(t *testing.T) {
	pt, base := newDownsampleFailurePartition(t)
	sources := addDownsampleFailureSources(t, pt, base, 32, partInmemory)
	before := failureManifest(t, pt)
	var targets []string
	pt.downsampleTestHook = func(stage, path string) error {
		if stage == "before-open" {
			targets = append(targets, path)
			return syscall.EIO
		}
		return nil
	}
	err := pt.mergePartsToFiles(append([]*partWrapper(nil), sources...), nil, make(chan struct{}, 1), false)
	if !errors.Is(err, syscall.EIO) || len(targets) != 1 {
		t.Fatalf("失败后仍调度了后续批次: targets=%v error=%v", targets, err)
	}
	assertDownsampleFailurePreserved(t, pt, sources, before, targets)
}

func TestDownsampleFailureFinalFlushPreservesRaw(t *testing.T) {
	pt, base := newDownsampleFailurePartition(t)
	sources := addDownsampleFailureSources(t, pt, base, 1, partInmemory)
	pt.releasePartsToMerge(sources)
	var failedTarget string
	pt.downsampleTestHook = func(stage, path string) error {
		if stage == "before-open" {
			failedTarget = path
			return syscall.EIO
		}
		return nil
	}
	pt.flushInmemoryPartsToFiles(true)
	if len(pt.inmemoryParts) != 0 || len(pt.smallParts) != 1 || pt.smallParts[0].p.dsMetadata != nil || !pt.s.downsamplingEnabled {
		t.Fatal("final flush 未保全原始数据，或修改了共享降采样配置")
	}
	if failedTarget == "" {
		t.Fatal("没有触发降采样失败")
	}
	if _, err := os.Stat(failedTarget); !os.IsNotExist(err) {
		t.Fatalf("降采样失败目标未清理: %v", err)
	}
	r := getBlockStreamReader()
	defer putBlockStreamReader(r)
	r.MustInitFromFilePart(pt.smallParts[0].p.path)
	if !r.NextBlock() {
		t.Fatalf("raw fallback 文件没有原始数据: %v", r.Error())
	}
	if err := r.Block.UnmarshalData(); err != nil {
		t.Fatal(err)
	}
	timestamps, values := r.Block.AppendRowsWithTimeRangeFilter(nil, nil, TimeRange{MinTimestamp: base, MaxTimestamp: base + 300000})
	if r.Block.bh.TSID != (TSID{AccountID: 11, ProjectID: 17, MetricID: 1}) || len(timestamps) != 1 || timestamps[0] != base+60000 || len(values) != 1 || values[0] != 1 {
		t.Fatalf("raw fallback 改变了原始数据: %+v %v %v", r.Block.bh.TSID, timestamps, values)
	}
	if r.NextBlock() || r.Error() != nil {
		t.Fatalf("raw fallback 增加了额外数据: %v", r.Error())
	}
}

func TestDownsampleFailureFinalLifecycle(t *testing.T) {
	for _, operation := range []string{"close", "snapshot"} {
		t.Run(operation, func(t *testing.T) {
			path := t.TempDir()
			s := MustOpenStorage(path, OpenOptions{DownsamplingEnabled: true})
			t.Cleanup(func() {
				if s != nil {
					s.MustClose()
				}
			})
			base := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour).UnixMilli()
			ptw := s.tb.MustGetPartition(base)
			pt := ptw.pt
			source := registerDownsampleTestInmemoryPart(pt, []rawRow{{TSID: TSID{MetricID: 999},
				Timestamp: base + 60000, Value: 5, PrecisionBits: 64}})
			pt.downsampleTestHook = func(stage, _ string) error {
				if stage == "before-open" {
					return syscall.EIO
				}
				return nil
			}
			pt.releasePartsToMerge([]*partWrapper{source})
			s.tb.PutPartition(ptw)
			if operation == "snapshot" {
				name := s.MustCreateSnapshot()
				path = filepath.Join(path, snapshotsDirname, name)
			}
			s.MustClose()
			s = nil
			reopened := MustOpenStorage(path, OpenOptions{DownsamplingEnabled: true})
			defer reopened.MustClose()
			assertDownsampleTestStorageFormats(t, reopened, true, false)
			if source.refCount.Load() != 0 {
				t.Fatal("final lifecycle 未释放原始内存源")
			}
		})
	}
}

func TestDownsampleFailureAfterPublication(t *testing.T) {
	pt, base := newDownsampleFailurePartition(t)
	sources := addDownsampleFailureSources(t, pt, base, 1, partSmall)
	sourcePath := sources[0].p.path
	before := failureManifest(t, pt)
	pt.downsampleTestHook = func(stage, _ string) error {
		if stage == "sync-commit-dir" {
			return syscall.EIO
		}
		return nil
	}
	err := pt.mergeParts(sources, nil, true, false)
	if !errors.Is(err, errDownsampleMergeFailed) || !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), "was published") {
		t.Fatalf("提交后失败状态不明确: %v", err)
	}
	if bytes.Equal(before, failureManifest(t, pt)) || len(pt.smallParts) != 1 || pt.smallParts[0] == sources[0] {
		t.Fatal("提交后内存集合与新 manifest 不一致")
	}
	if sources[0].isInMerge || sources[0].mustDrop.Load() || sources[0].p != nil {
		t.Fatal("提交后 sync 失败没有释放引用或保留旧磁盘源")
	}
	if _, err := os.Stat(sourcePath); err != nil {
		t.Fatalf("目录 sync 失败删除了旧磁盘源: %v", err)
	}
	target, err := openDownsamplePart(pt.smallParts[0].p.path)
	if err != nil {
		t.Fatalf("目录 sync 失败删除了已提交目标: %v", err)
	}
	target.MustClose()
}

func TestDownsampleFailureFinalSyncAfterPublication(t *testing.T) {
	pt, base := newDownsampleFailurePartition(t)
	sources := addDownsampleFailureSources(t, pt, base, 1, partInmemory)
	pt.releasePartsToMerge(sources)
	var commitSyncs, finalSyncs int
	pt.downsampleTestHook = func(stage, _ string) error {
		switch stage {
		case "sync-commit-dir":
			commitSyncs++
			return syscall.EIO
		case "sync-final-dir":
			finalSyncs++
		}
		return nil
	}
	pt.flushInmemoryPartsToFiles(true)
	if commitSyncs != 1 || finalSyncs != 1 || len(pt.inmemoryParts) != 0 || len(pt.smallParts) != 1 {
		t.Fatalf("提交后无剩余内存源时没有重新同步: commit=%d final=%d inmemory=%d small=%d", commitSyncs, finalSyncs, len(pt.inmemoryParts), len(pt.smallParts))
	}
	if pt.smallParts[0].p.dsMetadata == nil {
		t.Fatal("已发布目标被错误回退或覆盖")
	}
	target, err := openDownsamplePart(pt.smallParts[0].p.path)
	if err != nil {
		t.Fatal(err)
	}
	target.MustClose()
}
