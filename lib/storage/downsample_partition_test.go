package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
)

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

func registerDownsampleTestInmemoryPart(pt *partition, rows []rawRow) *partWrapper {
	mp := getInmemoryPart()
	mp.InitFromRows(append([]rawRow(nil), rows...))
	pw := newPartWrapperFromInmemoryPart(mp, time.Now().Add(time.Hour))
	pw.isInMerge = true
	pt.partsLock.Lock()
	pt.inmemoryParts = append(pt.inmemoryParts, pw)
	pt.partsLock.Unlock()
	return pw
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

func TestDownsampleStorageLifecycle(t *testing.T) {
	path := t.TempDir()
	opts := OpenOptions{Retention: 48 * time.Hour}
	s := MustOpenStorage(path, opts)
	defer func() {
		if s != nil {
			s.MustClose()
		}
	}()
	base := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour).UnixMilli()
	var mn MetricName
	mn.MetricGroup = []byte("downsample_lifecycle")
	metricName := mn.marshalRaw(nil)
	mrs := []MetricRow{
		{MetricNameRaw: metricName, Timestamp: base + 60000, Value: 2},
		{MetricNameRaw: metricName, Timestamp: base + 240000, Value: 8},
	}
	s.AddRows(mrs, 64)
	s.DebugFlush()
	tsid := func() TSID {
		ptws := s.tb.GetAllPartitions(nil)
		defer s.tb.PutPartitions(ptws)
		if len(ptws) != 1 {
			t.Fatalf("unexpected partitions: %d", len(ptws))
		}
		ptws[0].pt.flushInmemoryRowsToFiles()
		parts := ptws[0].pt.GetParts(nil, true)
		defer ptws[0].pt.PutParts(parts)
		if len(parts) == 0 {
			t.Fatal("raw storage didn't create a file part")
		}
		var result TSID
		for _, pw := range parts {
			if pw.mp != nil || pw.p.dsMetadata != nil {
				t.Fatal("disabled downsampling must retain the raw file format")
			}
			func() {
				r := getBlockStreamReader()
				r.MustInitFromFilePart(pw.p.path)
				defer putBlockStreamReader(r)
				if !r.NextBlock() {
					t.Fatalf("raw part has no block: %v", r.Error())
				}
				result = r.Block.bh.TSID
			}()
		}
		return result
	}()
	s.MustClose()
	s = nil

	// 启用后读取合法 raw 格式；尚未参与 merge 的 raw 文件无需预先迁移。
	opts.DownsamplingEnabled = true
	s = MustOpenStorage(path, opts)
	assertDownsampleTestStorageFormats(t, s, true, false)
	late := []MetricRow{
		{MetricNameRaw: metricName, Timestamp: base + 60000, Value: 5},
		{MetricNameRaw: metricName, Timestamp: base + 180000, Value: 4},
		{MetricNameRaw: metricName, Timestamp: base + 60000, Value: 5},
	}
	s.AddRows(late, 64)
	s.DebugFlush()
	mrs = append(mrs, late...)
	want := referenceDownsampleTestRows(downsampleTestMetricRows(mrs, tsid), nil, 0)

	// snapshot 必须先把待落盘样本写为降采样格式；快照可以同时包含合法 raw 源。
	snapshotName := s.MustCreateSnapshot()
	assertDownsampleTestStorageFormats(t, s, false, true)
	func() {
		snapshot := MustOpenStorage(filepath.Join(path, snapshotsDirname, snapshotName), opts)
		defer snapshot.MustClose()
		if err := snapshot.ForceMergePartitions(""); err != nil {
			t.Fatalf("cannot merge snapshot: %s", err)
		}
		assertDownsampleTestStorageRows(t, snapshot, want)
	}()

	if err := s.ForceMergePartitions(""); err != nil {
		t.Fatal(err)
	}
	assertDownsampleTestStorageRows(t, s, want)
	s.MustClose()
	s = nil
	s = MustOpenStorage(path, opts)
	assertDownsampleTestStorageRows(t, s, want)

	// 关闭操作也必须把新 inmemory 输出为摘要，重开后可继续归并。
	closing := MetricRow{MetricNameRaw: metricName, Timestamp: base + 240000, Value: 3}
	s.AddRows([]MetricRow{closing}, 64)
	mrs = append(mrs, closing)
	s.MustClose()
	s = nil
	s = MustOpenStorage(path, opts)
	if err := s.ForceMergePartitions(""); err != nil {
		t.Fatal(err)
	}
	want = referenceDownsampleTestRows(downsampleTestMetricRows(mrs, tsid), nil, 0)
	assertDownsampleTestStorageRows(t, s, want)
}

func assertDownsampleTestRawPart(t *testing.T, pw *partWrapper, rows []rawRow) {
	t.Helper()
	r := getBlockStreamReader()
	defer putBlockStreamReader(r)
	r.MustInitFromInmemoryPart(pw.mp)
	var got []rawRow
	for r.NextBlock() {
		if err := r.Block.UnmarshalData(); err != nil {
			t.Fatal(err)
		}
		values := decimal.AppendDecimalToFloat(nil, r.Block.values, r.Block.bh.Scale)
		for i, timestamp := range r.Block.timestamps {
			got = append(got, rawRow{TSID: r.Block.bh.TSID, Timestamp: timestamp, Value: values[i], PrecisionBits: r.Block.bh.PrecisionBits})
		}
	}
	if err := r.Error(); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(rows) {
		t.Fatalf("inmemory rows changed; got %d; want %d", len(got), len(rows))
	}
	for i := range rows {
		if got[i] != rows[i] {
			t.Fatalf("inmemory row %d changed; got %+v; want %+v", i, got[i], rows[i])
		}
	}
}

func downsampleTestMetricRows(rows []MetricRow, tsid TSID) []rawRow {
	result := make([]rawRow, len(rows))
	for i, row := range rows {
		result[i] = rawRow{TSID: tsid, Timestamp: row.Timestamp, Value: row.Value, PrecisionBits: 64}
	}
	return result
}

func assertDownsampleTestStorageFormats(t *testing.T, s *Storage, requireRaw, requireSummary bool) {
	t.Helper()
	ptws := s.tb.GetAllPartitions(nil)
	defer s.tb.PutPartitions(ptws)
	var raw, summary int
	for _, ptw := range ptws {
		parts := ptw.pt.GetParts(nil, false)
		for _, pw := range parts {
			if pw.p.dsMetadata == nil {
				raw++
			} else {
				summary++
			}
		}
		ptw.pt.PutParts(parts)
	}
	if requireRaw && raw == 0 || requireSummary && summary == 0 {
		t.Fatalf("unexpected active formats: raw=%d, summary=%d", raw, summary)
	}
}

func assertDownsampleTestStorageRows(t *testing.T, s *Storage, want map[downsampleTestKey]downsamplePoint) {
	t.Helper()
	ptws := s.tb.GetAllPartitions(nil)
	defer s.tb.PutPartitions(ptws)
	got := make(map[downsampleTestKey]downsamplePoint)
	for _, ptw := range ptws {
		func() {
			parts := ptw.pt.GetParts(nil, true)
			defer ptw.pt.PutParts(parts)
			for _, pw := range parts {
				if pw.mp != nil || pw.p.dsMetadata == nil {
					t.Fatal("expected only persisted downsample parts after merge or reopen")
				}
				for key, point := range readDownsampleTestPart(t, pw.p) {
					if _, ok := got[key]; ok {
						t.Fatalf("duplicate target bucket after force merge: %+v", key)
					}
					got[key] = point
				}
			}
		}()
	}
	assertDownsampleTestRows(t, got, want)
}
