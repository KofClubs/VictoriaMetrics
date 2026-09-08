package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDownsampleRecoveryKeepsCommittedTargetAfterPanic(t *testing.T) {
	setDownsampleOpenTestDedup(t, 0)
	path := t.TempDir()
	s := MustOpenStorage(path, OpenOptions{DownsamplingEnabled: true})
	defer func() {
		if s != nil {
			s.MustClose()
		}
	}()
	base := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour).UnixMilli()
	ptw := s.tb.MustGetPartition(base)
	defer func() {
		if ptw != nil {
			s.tb.PutPartition(ptw)
		}
	}()
	pt := ptw.pt
	rows := []rawRow{
		{TSID: TSID{MetricID: 987}, Timestamp: base + 60000, Value: 2, PrecisionBits: 64},
		{TSID: TSID{MetricID: 987}, Timestamp: base + 120000, Value: 8, PrecisionBits: 64},
	}
	source := registerDownsampleTestInmemoryPart(pt, rows)
	defer func() {
		if source.p == nil {
			return
		}
		// 故障注入只影响引用计数；恢复后回收已从活动集合移除的源。
		source.refCount.Store(1)
		if source.mustDrop.Load() {
			source.decRef()
		}
	}()
	// swap 在清单提交后才对旧源 decRef；负引用计数使该阶段同步 panic，避免后台故障的不确定性。
	source.refCount.Store(0)
	var panicValue any
	var mergeErr error
	func() {
		defer func() { panicValue = recover() }()
		mergeErr = pt.mergeParts([]*partWrapper{source}, nil, true, false)
	}()
	if panicValue == nil || !strings.Contains(fmt.Sprint(panicValue), "pw.refCount must be bigger than 0") {
		t.Fatalf("expected a source cleanup panic after publication; panic=%v; merge error=%v", panicValue, mergeErr)
	}
	if source.refCount.Load() != -1 || !source.mustDrop.Load() || source.p == nil || source.mp == nil {
		t.Fatalf("fault did not occur before source closure: references=%d; dropped=%v; part=%p; inmemory=%p",
			source.refCount.Load(), source.mustDrop.Load(), source.p, source.mp)
	}
	if source.isInMerge {
		t.Fatal("panic left the source marked as merging")
	}
	pt.partsLock.Lock()
	remainingInmemory := len(pt.inmemoryParts)
	pt.partsLock.Unlock()
	if remainingInmemory != 0 {
		t.Fatal("source cleanup panicked before the source was removed from the active set")
	}
	manifestPath := filepath.Join(pt.smallPartsPath, partsFilename)
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var manifest partNamesJSON
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Small)+len(manifest.Big) != 1 {
		t.Fatalf("committed manifest does not contain exactly one target: %s", data)
	}
	var targetPath string
	if len(manifest.Small) == 1 {
		targetPath = filepath.Join(pt.smallPartsPath, manifest.Small[0])
	} else {
		targetPath = filepath.Join(pt.bigPartsPath, manifest.Big[0])
	}
	// 独立打开清单指向的文件，确保外层清理没有删除已提交的目标及其任一列。
	target, err := openDownsamplePart(targetPath)
	if err != nil {
		t.Fatalf("committed target was deleted or corrupted after the panic: %s", err)
	}
	func() {
		defer target.MustClose()
		assertDownsampleTestRows(t, readDownsampleTestPart(t, target), referenceDownsampleTestRows(rows, nil, 0))
	}()
	source.refCount.Store(1)
	source.decRef()
	s.tb.PutPartition(ptw)
	ptw = nil
	s.MustClose()
	s = nil

	reopened := MustOpenStorage(path, OpenOptions{DownsamplingEnabled: true})
	defer reopened.MustClose()
	assertDownsampleTestStorageRows(t, reopened, referenceDownsampleTestRows(rows, nil, 0))
}

func TestDownsampleRecoveryDiscardsUnpublishedPart(t *testing.T) {
	setDownsampleOpenTestDedup(t, 0)
	path := t.TempDir()
	s := MustOpenStorage(path, OpenOptions{DownsamplingEnabled: true})
	defer func() {
		if s != nil {
			s.MustClose()
		}
	}()
	base := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour).UnixMilli()
	ptw := s.tb.MustGetPartition(base)
	defer func() {
		if ptw != nil {
			s.tb.PutPartition(ptw)
		}
	}()
	rows := []rawRow{{TSID: TSID{MetricID: 654}, Timestamp: base + 60000, Value: 5, PrecisionBits: 64}}
	source := registerDownsampleTestInmemoryPart(ptw.pt, rows)
	if err := ptw.pt.mergeParts([]*partWrapper{source}, nil, true, false); err != nil {
		t.Fatal(err)
	}
	orphanPath := filepath.Join(ptw.pt.smallPartsPath, "unpublished-output")
	manifestPath := filepath.Join(ptw.pt.smallPartsPath, partsFilename)
	s.tb.PutPartition(ptw)
	ptw = nil
	s.MustClose()
	s = nil
	before, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	// 模拟目标仅写出部分内容便退出；未知格式的孤立目录不得进入活动格式校验或恢复结果。
	writeDownsampleOpenTestFile(t, filepath.Join(orphanPath, metadataFilename), []byte(`{"FormatVersion":999}`))
	writeDownsampleOpenTestFile(t, filepath.Join(orphanPath, metaindexFilename), []byte("incomplete output"))
	reopened := MustOpenStorage(path, OpenOptions{DownsamplingEnabled: true})
	defer reopened.MustClose()
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Fatalf("restart did not remove the unpublished output: %v", err)
	}
	after, err := os.ReadFile(manifestPath)
	if err != nil || string(after) != string(before) {
		t.Fatalf("orphan recovery changed the active manifest: before=%s; after=%s; error=%v", before, after, err)
	}
	assertDownsampleTestStorageRows(t, reopened, referenceDownsampleTestRows(rows, nil, 0))
}
