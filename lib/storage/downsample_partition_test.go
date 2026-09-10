package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	vmfs "github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

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

func TestDownsampleManifestTemporaryFileCollision(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cancel bool
	}{
		{name: "publish"},
		{name: "cancel", cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pt := &partition{smallPartsPath: t.TempDir()}
			manifestPath := filepath.Join(pt.smallPartsPath, partsFilename)
			originalManifest := []byte(`{"Small":["old"],"Big":[]}`)
			if err := os.WriteFile(manifestPath, originalManifest, 0666); err != nil {
				t.Fatal(err)
			}
			existingTempDir := filepath.Join(pt.smallPartsPath, partsFilename+".tmp.1")
			if err := os.Mkdir(existingTempDir, 0755); err != nil {
				t.Fatal(err)
			}
			existingTempPaths := []string{
				filepath.Join(existingTempDir, partsFilename),
				filepath.Join(pt.smallPartsPath, partsFilename+".tmp.2"),
			}
			existingTempData := []byte("pre-existing temporary manifest\n")
			for _, path := range existingTempPaths {
				if err := os.WriteFile(path, existingTempData, 0666); err != nil {
					t.Fatal(err)
				}
			}
			var createdTempDir string
			pt.downsampleTestHook = func(stage, path string) error {
				if stage != "before-commit" {
					return nil
				}
				createdTempDir = filepath.Dir(path)
				if filepath.Dir(createdTempDir) != pt.smallPartsPath || createdTempDir == existingTempDir || filepath.Base(path) != partsFilename {
					t.Fatalf("manifest was not written to its own temporary directory: %q", path)
				}
				if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, []byte(`{"Small":["new"],"Big":[]}`)) {
					t.Fatalf("temporary manifest is incomplete before publication: got=%q err=%v", got, err)
				}
				return nil
			}
			stopCh := make(chan struct{})
			if tc.cancel {
				close(stopCh)
			}
			small := []*partWrapper{{p: &part{path: filepath.Join(pt.smallPartsPath, "new")}}}
			published := false
			err := pt.writeDownsamplePartNames(small, nil, stopCh, &published)
			wantManifest := []byte(`{"Small":["new"],"Big":[]}`)
			if tc.cancel {
				if !errors.Is(err, errForciblyStopped) || published {
					t.Fatalf("取消后仍发布清单或丢失取消原因: published=%v err=%v", published, err)
				}
				wantManifest = originalManifest
			} else if err != nil || !published {
				t.Fatalf("临时文件冲突后未成功发布清单: published=%v err=%v", published, err)
			}
			if got, err := os.ReadFile(manifestPath); err != nil || !bytes.Equal(got, wantManifest) {
				t.Fatalf("清单内容错误: got=%q want=%q err=%v", got, wantManifest, err)
			}
			for _, path := range existingTempPaths {
				if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, existingTempData) {
					t.Fatalf("pre-existing temporary manifest was changed: path=%q got=%q want=%q err=%v", path, got, existingTempData, err)
				}
			}
			if createdTempDir == "" {
				t.Fatal("temporary manifest did not reach the publication boundary")
			}
			if _, err := os.Stat(createdTempDir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("the operation's temporary directory was not removed: %v", err)
			}
			entries, err := os.ReadDir(pt.smallPartsPath)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 3 {
				t.Fatalf("unexpected leftovers beside the manifest and pre-existing paths: %v", entries)
			}
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

func TestCheckDownsamplingOpen(t *testing.T) {
	setDownsampleOpenTestDedup(t, 0)
	for _, tc := range []struct {
		name    string
		enabled bool
		setup   func(*testing.T, string)
		wantErr string
	}{
		{name: "empty"},
		{name: "enabled_empty", enabled: true},
		{
			name: "raw_manifest",
			setup: func(t *testing.T, path string) {
				createDownsampleOpenTestRaw(t, filepath.Join(path, "data/small/2025_01/raw"))
				writeDownsampleOpenTestManifest(t, path, "2025_01", []string{"raw"}, nil)
			},
		},
		{
			name: "raw_without_manifest_or_metadata",
			setup: func(t *testing.T, path string) {
				partPath := filepath.Join(path, "data/big/2025_01/1_1_20250101000000.000_20250101000000.000_1")
				createDownsampleOpenTestRaw(t, partPath)
				if err := os.Remove(filepath.Join(partPath, metadataFilename)); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "enabled_mixed_raw_summary",
			enabled: true,
			setup: func(t *testing.T, path string) {
				createDownsampleOpenTestRaw(t, filepath.Join(path, "data/small/2025_01/raw"))
				createDownsampleOpenTestSummary(t, filepath.Join(path, "data/big/2025_01/summary"))
				writeDownsampleOpenTestManifest(t, path, "2025_01", []string{"raw"}, []string{"summary"})
			},
		},
		{
			name: "disabled_active_summary_in_later_partition",
			setup: func(t *testing.T, path string) {
				createDownsampleOpenTestRaw(t, filepath.Join(path, "data/small/2024_12/raw"))
				createDownsampleOpenTestSummary(t, filepath.Join(path, "data/big/2025_01/summary"))
				writeDownsampleOpenTestManifest(t, path, "2025_01", nil, []string{"summary"})
			},
			wantErr: "enable -storage.downsampling.enabled",
		},
		{
			name: "summary_only_in_snapshots",
			setup: func(t *testing.T, path string) {
				createDownsampleOpenTestSummary(t, filepath.Join(path, "snapshots/snapshot/data/small/2025_01/summary"))
				createDownsampleOpenTestSummary(t, filepath.Join(path, "data/small/snapshots/snapshot/2025_01/summary"))
				createDownsampleOpenTestSummary(t, filepath.Join(path, "data/big/snapshots/snapshot/2025_01/summary"))
			},
		},
		{
			name: "manifest_excludes_unpublished_summary_and_special_directories",
			setup: func(t *testing.T, path string) {
				createDownsampleOpenTestRaw(t, filepath.Join(path, "data/small/2025_01/raw"))
				for _, name := range []string{"unpublished", "tmp", "txn", "snapshots"} {
					createDownsampleOpenTestSummary(t, filepath.Join(path, "data/small/2025_01", name))
				}
				writeDownsampleOpenTestManifest(t, path, "2025_01", []string{"raw"}, nil)
			},
		},
		{
			name: "historical_special_directories",
			setup: func(t *testing.T, path string) {
				createDownsampleOpenTestRaw(t, filepath.Join(path, "data/small/2025_01/raw"))
				for _, name := range []string{"tmp", "txn", "snapshots"} {
					createDownsampleOpenTestSummary(t, filepath.Join(path, "data/small/2025_01", name))
				}
			},
		},
		{
			name: "partially_removed_partition",
			setup: func(t *testing.T, path string) {
				createDownsampleOpenTestSummary(t, filepath.Join(path, "data/small/2025_01/summary"))
				writeDownsampleOpenTestFile(t, filepath.Join(path, "data/small/2025_01/.delete-this-dir"), nil)
			},
		},
		{
			name: "partially_removed_small_keeps_active_big",
			setup: func(t *testing.T, path string) {
				createDownsampleOpenTestSummary(t, filepath.Join(path, "data/small/2025_01/summary"))
				createDownsampleOpenTestRaw(t, filepath.Join(path, "data/big/2025_01/raw"))
				writeDownsampleOpenTestManifest(t, path, "2025_01", []string{"summary"}, nil)
				writeDownsampleOpenTestFile(t, filepath.Join(path, "data/small/2025_01/.delete-this-dir"), nil)
			},
		},
		{
			name: "indexdb_partition_without_data",
			setup: func(t *testing.T, path string) {
				writeDownsampleOpenTestFile(t, filepath.Join(path, "data/indexdb/2025_01/index.bin"), []byte("not decoded by the precheck"))
			},
		},
		{
			name: "missing_active_part",
			setup: func(t *testing.T, path string) {
				writeDownsampleOpenTestManifest(t, path, "2025_01", []string{"missing"}, nil)
			},
			wantErr: "missing",
		},
		{
			name: "corrupt_active_metadata",
			setup: func(t *testing.T, path string) {
				partPath := filepath.Join(path, "data/small/2025_01/corrupt")
				createDownsampleOpenTestRaw(t, partPath)
				writeDownsampleOpenTestFile(t, filepath.Join(partPath, metadataFilename), []byte("{"))
			},
			wantErr: "corrupt",
		},
		{
			name: "indexdb_invalid_partition_name",
			setup: func(t *testing.T, path string) {
				writeDownsampleOpenTestFile(t, filepath.Join(path, "data/indexdb/invalid/index.bin"), []byte("not decoded by the precheck"))
			},
			wantErr: "invalid partition directory",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "storage")
			if tc.setup != nil {
				tc.setup(t, path)
			}
			before := snapshotDownsampleOpenTestFiles(t, path)
			err := checkDownsamplingOpen(path, OpenOptions{DownsamplingEnabled: tc.enabled})
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected precheck error: %s", err)
				}
			} else if err == nil || !strings.HasPrefix(err.Error(), "[downsampling] ") || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("unexpected precheck error: got %v; want substring %q", err, tc.wantErr)
			}
			after := snapshotDownsampleOpenTestFiles(t, path)
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("precheck modified storage files: before=%v; after=%v", before, after)
			}
		})
	}
}

func TestCheckDownsamplingOpenInvalidManifests(t *testing.T) {
	setDownsampleOpenTestDedup(t, 0)
	for _, tc := range []struct {
		data    string
		wantErr string
	}{
		{`{`, "cannot parse"},
		{`null`, "expected a JSON object"},
		{`[]`, "expected a JSON object"},
		{`{"Small":[],"Unknown":[]}`, "unknown field"},
		{`{"Small":[],"small":["other"]}`, "duplicate field"},
		{`{"Small":["raw","raw"]}`, "duplicate active part"},
		{`{"Small":["../raw"]}`, "invalid active part name"},
		{`{"Small":["tmp"]}`, "invalid active part name"},
		{`{"Small":[""]}`, "invalid active part name"},
		{`{"Small":[1]}`, "invalid \"Small\" list"},
		{`{"Small":[]} {}`, "unexpected trailing"},
		{`{"Big":["raw"]}`, "partition directory is missing"},
	} {
		t.Run(tc.data, func(t *testing.T) {
			path := t.TempDir()
			manifest := filepath.Join(path, "data/small/2025_01", partsFilename)
			writeDownsampleOpenTestFile(t, manifest, []byte(tc.data))
			before := snapshotDownsampleOpenTestFiles(t, path)
			err := checkDownsamplingOpen(path, OpenOptions{DownsamplingEnabled: true})
			if err == nil || !strings.HasPrefix(err.Error(), "[downsampling] ") || !strings.Contains(err.Error(), tc.wantErr) || !strings.Contains(err.Error(), manifest) {
				t.Fatalf("unexpected manifest error: got %v; want %q and %q", err, tc.wantErr, manifest)
			}
			if after := snapshotDownsampleOpenTestFiles(t, path); !reflect.DeepEqual(before, after) {
				t.Fatalf("invalid manifest was modified during precheck")
			}
		})
	}
}

func TestMustOpenStorageDownsamplingRejectsDedup(t *testing.T) {
	for _, interval := range []time.Duration{time.Millisecond, -time.Millisecond} {
		t.Run(interval.String(), func(t *testing.T) {
			setDownsampleOpenTestDedup(t, interval)
			path := t.TempDir()
			func() {
				defer func() {
					r := recover()
					if r == nil || !strings.Contains(fmt.Sprint(r), "requires -dedup.minScrapeInterval=0") {
						t.Fatalf("unexpected storage API result: got %v; want a dedup configuration panic", r)
					}
				}()
				s := MustOpenStorage(path, OpenOptions{DownsamplingEnabled: true})
				s.MustClose()
			}()
			for _, name := range []string{dataDirname, indexdbDirname, snapshotsDirname, cacheDirname, metadataDirname} {
				if _, err := os.Stat(filepath.Join(path, name)); !os.IsNotExist(err) {
					t.Fatalf("storage initialized %q before rejecting dedup: %v", name, err)
				}
			}
			// 错误返回后必须释放 flock，允许修正配置后再次打开目录。
			f := vmfs.MustCreateFlockFile(path)
			vmfs.MustClose(f)
		})
	}
}

func TestMustOpenStorageDownsamplingRejectsDisabledModeBeforeBackgroundWork(t *testing.T) {
	setDownsampleOpenTestDedup(t, 0)
	path := t.TempDir()
	createDownsampleOpenTestRaw(t, filepath.Join(path, "data/small/2024_12/raw"))
	createDownsampleOpenTestSummary(t, filepath.Join(path, "data/big/2025_01/summary"))
	writeDownsampleOpenTestManifest(t, path, "2025_01", nil, []string{"summary"})
	before := snapshotDownsampleOpenTestFiles(t, filepath.Join(path, dataDirname))
	func() {
		defer func() {
			r := recover()
			if r == nil || !strings.Contains(fmt.Sprint(r), "enable -storage.downsampling.enabled") {
				t.Fatalf("unexpected storage API result: got %v; want a downsampling configuration panic", r)
			}
		}()
		s := MustOpenStorage(path, OpenOptions{})
		s.MustClose()
	}()
	if after := snapshotDownsampleOpenTestFiles(t, filepath.Join(path, dataDirname)); !reflect.DeepEqual(before, after) {
		t.Fatalf("storage modified source parts or manifests before rejecting active summary")
	}
	for _, name := range []string{indexdbDirname, snapshotsDirname, cacheDirname, metadataDirname} {
		if _, err := os.Stat(filepath.Join(path, name)); !os.IsNotExist(err) {
			t.Fatalf("storage initialized %q before rejecting active summary: %v", name, err)
		}
	}
	f := vmfs.MustCreateFlockFile(path)
	vmfs.MustClose(f)
}

func TestCheckDownsamplingOpenDisabledPreservesDedup(t *testing.T) {
	setDownsampleOpenTestDedup(t, time.Minute)
	path := t.TempDir()
	createDownsampleOpenTestRaw(t, filepath.Join(path, "data/small/2025_01/raw"))
	before := snapshotDownsampleOpenTestFiles(t, path)
	if err := checkDownsamplingOpen(path, OpenOptions{}); err != nil {
		t.Fatalf("disabled downsampling must retain the existing dedup configuration: %s", err)
	}
	if GetDedupInterval() != time.Minute.Milliseconds() {
		t.Fatalf("precheck changed the dedup interval: got %d", GetDedupInterval())
	}
	if after := snapshotDownsampleOpenTestFiles(t, path); !reflect.DeepEqual(before, after) {
		t.Fatalf("precheck modified raw source files")
	}
}

func TestDownsamplePartitionInmemoryAndFailedOutput(t *testing.T) {
	s := MustOpenStorage(t.TempDir(), OpenOptions{DownsamplingEnabled: true})
	defer s.MustClose()
	base := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour).UnixMilli()
	ptw := s.tb.MustGetPartition(base)
	defer s.tb.PutPartition(ptw)
	pt := ptw.pt
	rows := []rawRow{
		{TSID: TSID{MetricID: 123}, Timestamp: base + 60000, Value: 2, PrecisionBits: 64},
		{TSID: TSID{MetricID: 123}, Timestamp: base + 120000, Value: 8, PrecisionBits: 64},
	}
	sources := []*partWrapper{
		registerDownsampleTestInmemoryPart(pt, rows[:1]),
		registerDownsampleTestInmemoryPart(pt, rows[1:]),
	}
	if err := pt.mergeParts(sources, nil, false, false); err != nil {
		t.Fatalf("cannot merge raw inmemory target: %s", err)
	}
	parts := pt.GetParts(nil, true)
	defer pt.PutParts(parts)
	if len(parts) != 1 || parts[0].mp == nil || parts[0].p.dsMetadata != nil {
		t.Fatalf("inmemory merge changed the raw format: %d parts", len(parts))
	}
	assertDownsampleTestRawPart(t, parts[0], rows)
	manifestPath := filepath.Join(pt.smallPartsPath, partsFilename)
	before, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	pt.partsLock.Lock()
	parts[0].isInMerge = true
	pt.partsLock.Unlock()
	stopCh := make(chan struct{})
	close(stopCh)
	if err := pt.mergeParts(parts, stopCh, true, false); !errors.Is(err, errForciblyStopped) {
		t.Fatalf("unexpected cancellation result: %v", err)
	}
	if parts[0].isInMerge || parts[0].mustDrop.Load() || parts[0].mp == nil {
		t.Fatal("cancelled merge didn't release or preserve its source")
	}
	// 用已有清单文件占据目标路径，确定性触发 writer 初始化失败。
	// 失败路径既不能移除已有清单，也不能替换源。
	pt.partsLock.Lock()
	parts[0].isInMerge = true
	pt.partsLock.Unlock()
	err = pt.mergeDownsampleParts(parts, partSmall, manifestPath, nil, time.Now())
	pt.releasePartsToMerge(parts)
	if err == nil {
		t.Fatal("expected writer initialization failure")
	}
	if parts[0].mustDrop.Load() || parts[0].mp == nil {
		t.Fatal("failed output discarded source")
	}
	after, err := os.ReadFile(manifestPath)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("failed merge changed manifest: %v", err)
	}
	for _, path := range []string{pt.smallPartsPath, pt.bigPartsPath} {
		entries, err := os.ReadDir(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.IsDir() {
				t.Fatalf("failed merge left an unpublished directory %q", entry.Name())
			}
		}
	}
}

func TestDownsamplePartitionSmallBigMerge(t *testing.T) {
	s := MustOpenStorage(t.TempDir(), OpenOptions{DownsamplingEnabled: true})
	defer s.MustClose()
	base := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour).UnixMilli()
	ptw := s.tb.MustGetPartition(base)
	defer s.tb.PutPartition(ptw)
	pt := ptw.pt
	var rows []rawRow
	// 分别产生 small 与 big 源，不修改生产大小阈值或伪造源字节统计。
	for i, dstType := range []partType{partSmall, partBig} {
		row := rawRow{TSID: TSID{MetricID: 123}, Timestamp: base + int64(i+1)*60000, Value: float64(i + 2), PrecisionBits: 64}
		rows = append(rows, row)
		sources := []*partWrapper{registerDownsampleTestInmemoryPart(pt, []rawRow{row})}
		dstPath := pt.getDstPartPath(dstType, pt.nextMergeIdx())
		err := pt.mergeDownsampleParts(sources, dstType, dstPath, nil, time.Now())
		pt.releasePartsToMerge(sources)
		if err != nil {
			t.Fatal(err)
		}
	}
	pt.partsLock.Lock()
	if len(pt.smallParts) != 1 || len(pt.bigParts) != 1 {
		pt.partsLock.Unlock()
		t.Fatal("expected one small and one big source")
	}
	sources := append(append([]*partWrapper(nil), pt.smallParts...), pt.bigParts...)
	for _, pw := range sources {
		pw.isInMerge = true
	}
	pt.partsLock.Unlock()
	err := pt.mergeDownsampleParts(sources, partBig, pt.getDstPartPath(partBig, pt.nextMergeIdx()), nil, time.Now())
	pt.releasePartsToMerge(sources)
	if err != nil {
		t.Fatal(err)
	}
	pt.partsLock.Lock()
	smallCount, bigCount := len(pt.smallParts), len(pt.bigParts)
	pt.partsLock.Unlock()
	if smallCount != 0 || bigCount != 1 {
		t.Fatalf("unexpected target placement: small=%d, big=%d", smallCount, bigCount)
	}
	assertDownsampleTestStorageRows(t, s, referenceDownsampleTestRows(rows, nil, 0))
}

func TestDownsamplePartitionFilePartExpired(t *testing.T) {
	const base int64 = 1704067200000
	pt := partition{s: &Storage{downsamplingEnabled: true}}
	for _, summary := range []bool{false, true} {
		p := part{ph: partHeader{MaxTimestamp: base + 60000}}
		if summary {
			p.dsMetadata = &downsamplePartMetadata{}
		}
		for _, tc := range []struct {
			deadline int64
			want     bool
		}{
			{base + 120000, false},
			{base + downsampleResolution5m, false},
			{base + downsampleResolution1h - 1, false},
			{base + downsampleResolution1h, true},
			{base + downsampleResolution1h + 1, true},
		} {
			if got := pt.filePartExpired(&p, tc.deadline); got != tc.want {
				t.Fatalf("unexpected expiry for summary=%v, deadline=%d; got %v; want %v", summary, tc.deadline, got, tc.want)
			}
		}
	}
	invalid := part{ph: partHeader{MaxTimestamp: minUnixMilli - 1}}
	if pt.filePartExpired(&invalid, maxUnixMilli) {
		t.Fatal("cannot prove invalid time range fully expired")
	}
	pt.s.downsamplingEnabled = false
	raw := part{ph: partHeader{MaxTimestamp: base + 60000}}
	if pt.filePartExpired(&raw, raw.ph.MaxTimestamp) || !pt.filePartExpired(&raw, raw.ph.MaxTimestamp+1) {
		t.Fatal("disabled downsampling changed the raw expiry boundary")
	}
}

func TestDownsamplePartitionFileOutput(t *testing.T) {
	for _, sourcesCount := range []int{1, 2} {
		name := "single-inmemory-dump"
		if sourcesCount > 1 {
			name = "multiple-inmemory-merge"
		}
		t.Run(name, func(t *testing.T) {
			s := MustOpenStorage(t.TempDir(), OpenOptions{DownsamplingEnabled: true})
			defer s.MustClose()
			base := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour).UnixMilli()
			ptw := s.tb.MustGetPartition(base)
			defer s.tb.PutPartition(ptw)
			pt := ptw.pt
			var sources []*partWrapper
			var rows []rawRow
			for i := 0; i < sourcesCount; i++ {
				partRows := []rawRow{
					{TSID: TSID{MetricID: 123}, Timestamp: base + 60000, Value: float64(2 + i), PrecisionBits: 64},
					{TSID: TSID{MetricID: 123}, Timestamp: base + 240000, Value: float64(8 + i), PrecisionBits: 64},
				}
				mp := getInmemoryPart()
				mp.InitFromRows(append([]rawRow(nil), partRows...))
				pw := newPartWrapperFromInmemoryPart(mp, time.Now().Add(time.Hour))
				if pw.p.dsMetadata != nil || pw.p.ph.RowsCount != uint64(len(partRows)) {
					t.Fatal("inmemory part isn't in original raw format")
				}
				assertDownsampleTestRawPart(t, pw, partRows)
				pw.isInMerge = true
				sources = append(sources, pw)
				rows = append(rows, partRows...)
			}
			// 在同一锁域登记并标记源，避免后台任务抢占测试的目标输出路径。
			pt.partsLock.Lock()
			pt.inmemoryParts = append(pt.inmemoryParts, sources...)
			pt.partsLock.Unlock()
			if err := pt.mergeParts(sources, nil, true, false); err != nil {
				t.Fatalf("cannot create file part: %s", err)
			}
			parts := pt.GetParts(nil, true)
			defer pt.PutParts(parts)
			if len(parts) != 1 || parts[0].mp != nil || parts[0].p.dsMetadata == nil {
				t.Fatalf("file output must contain exactly one downsample part; got %d parts", len(parts))
			}
			assertDownsampleTestRows(t, readDownsampleTestPart(t, parts[0].p), referenceDownsampleTestRows(rows, nil, 0))
		})
	}
}

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
	writeDownsampleOpenTestFile(t, filepath.Join(orphanPath, metadataFilename), []byte(`{"FormatVersion":255}`))
	writeDownsampleOpenTestFile(t, filepath.Join(orphanPath, metaindexFilename), []byte("incomplete output"))
	// 同时模拟 rename 前退出，私有暂存目录内只有不完整清单。
	manifestTempDir, err := os.MkdirTemp(filepath.Dir(manifestPath), partsFilename+".tmp.")
	if err != nil {
		t.Fatal(err)
	}
	writeDownsampleOpenTestFile(t, filepath.Join(manifestTempDir, partsFilename), []byte(`{"Small":[`))
	reopened := MustOpenStorage(path, OpenOptions{DownsamplingEnabled: true})
	defer reopened.MustClose()
	for _, unpublishedPath := range []string{orphanPath, manifestTempDir} {
		if _, err := os.Stat(unpublishedPath); !os.IsNotExist(err) {
			t.Fatalf("restart did not remove unpublished data at %q: %v", unpublishedPath, err)
		}
	}
	after, err := os.ReadFile(manifestPath)
	if err != nil || string(after) != string(before) {
		t.Fatalf("orphan recovery changed the active manifest: before=%s; after=%s; error=%v", before, after, err)
	}
	assertDownsampleTestStorageRows(t, reopened, referenceDownsampleTestRows(rows, nil, 0))
}

func TestReserveDownsampleSpaceConcurrentAndCachedFreeSpace(t *testing.T) {
	var budget downsampleSpaceBudget
	now := time.Unix(100, 0)
	available := uint64(1000)
	getFree := func(string) uint64 { return available }
	getNow := func() time.Time { return now }
	results := make(chan func(), 32)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			release, err := reserveDownsampleSpaceWithBudget(&budget, "same-filesystem", 100, 0, getFree, getNow)
			if err != nil {
				if !errors.Is(err, errDownsampleNoSpace) {
					t.Errorf("unexpected reservation failure: %s", err)
				}
				return
			}
			results <- release
		})
	}
	wg.Wait()
	close(results)
	if len(results) != 10 {
		t.Fatalf("concurrent reservations oversubscribed or underused the same free-space sample: got %d; want 10", len(results))
	}
	for release := range results {
		wg.Go(release)
		wg.Go(release)
	}
	wg.Wait()
	if budget.reserved != 0 || budget.retiredBytes != 1000 {
		t.Fatalf("reservation release was not idempotent: active=%d; awaiting cache refresh=%d", budget.reserved, budget.retiredBytes)
	}
	if _, err := reserveDownsampleSpaceWithBudget(&budget, "another-directory", 1, 0, getFree, getNow); !errors.Is(err, errDownsampleNoSpace) {
		t.Fatalf("released budget was reused before the cached free-space sample could refresh: %v", err)
	}
	now = now.Add(downsampleSpaceCacheLifetime - time.Nanosecond)
	if _, err := reserveDownsampleSpaceWithBudget(&budget, "another-directory", 1, 0, getFree, getNow); !errors.Is(err, errDownsampleNoSpace) {
		t.Fatalf("released budget expired before the full free-space cache interval: %v", err)
	}
	now = now.Add(time.Nanosecond)
	// 新读数包含旧源和刚写入的目标占用；释放预算并不意味着删除这些文件。
	available = 550
	if _, err := reserveDownsampleSpaceWithBudget(&budget, "another-directory", 551, 0, getFree, getNow); !errors.Is(err, errDownsampleNoSpace) {
		t.Fatalf("reservation ignored the refreshed free-space value: %v", err)
	}
	release, err := reserveDownsampleSpaceWithBudget(&budget, "another-directory", 550, 0, getFree, getNow)
	if err != nil {
		t.Fatalf("expired cache debt did not release the remaining capacity: %s", err)
	}
	release()
}
