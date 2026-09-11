package storage

import (
	"fmt"
	"io"
	"sort"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/blockcache"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

// DownsampleQuery 选择已落盘 Block 的分辨率和特征；查询链路仅透传，不重新聚合原始样本。
type DownsampleQuery struct {
	// ResolutionMs 是目标分辨率，单位为毫秒。
	ResolutionMs int64
	// Feature 是 last、sum、count、min、max 对应的特征编号。
	Feature uint8
}

// ParseDownsampleQuery 解析分辨率和特征；两者均为空时使用原有查询路径。
func ParseDownsampleQuery(resolution, feature string) (*DownsampleQuery, error) {
	if resolution == "" && feature == "" {
		return nil, nil
	}
	if resolution == "" || feature == "" {
		return nil, fmt.Errorf("[downsampling] query.resolution and query.feature must be provided together")
	}
	var q DownsampleQuery
	switch resolution {
	case "5m":
		q.ResolutionMs = downsampleResolution5m
	case "1h":
		q.ResolutionMs = downsampleResolution1h
	default:
		return nil, fmt.Errorf("[downsampling] invalid query.resolution %q; expected 5m or 1h", resolution)
	}
	switch feature {
	case "last":
		q.Feature = downsampleFeatureLast
	case "sum":
		q.Feature = downsampleFeatureSum
	case "count":
		q.Feature = downsampleFeatureCount
	case "min":
		q.Feature = downsampleFeatureMin
	case "max":
		q.Feature = downsampleFeatureMax
	default:
		return nil, fmt.Errorf("[downsampling] invalid query.feature %q; expected last, sum, count, min or max", feature)
	}
	return &q, nil
}

// MarshalDownsampleWithoutTenant 为 search_downsampling_v2 编码查询条件，保留原有租户前缀的组织方式。
// 降采样查询在原有查询负载末尾增加八字节分辨率和一字节特征编号，不改变原生查询协议。
func (sq *SearchQuery) MarshalDownsampleWithoutTenant(dst []byte) ([]byte, error) {
	if !sq.DownsampleQuery.valid() {
		return dst, fmt.Errorf("[downsampling] search_downsampling_v2 requires a valid downsampling resolution and feature")
	}
	dst = sq.MarshalWithoutTenant(dst)
	dst = encoding.MarshalInt64(dst, sq.DownsampleQuery.ResolutionMs)
	dst = append(dst, sq.DownsampleQuery.Feature)
	return dst, nil
}

// UnmarshalDownsample 解码 search_downsampling_v2；调用方仍须拒绝未消费的尾部数据。
func (sq *SearchQuery) UnmarshalDownsample(src []byte) ([]byte, error) {
	tail, err := sq.Unmarshal(src)
	if err != nil {
		return tail, fmt.Errorf("[downsampling] cannot decode search_downsampling_v2 query: %w", err)
	}
	if len(tail) < 9 {
		return tail, fmt.Errorf("[downsampling] cannot decode search_downsampling_v2 selector: got %d bytes; need 9", len(tail))
	}
	downsampleQuery := DownsampleQuery{ResolutionMs: encoding.UnmarshalInt64(tail), Feature: tail[8]}
	if !downsampleQuery.valid() {
		return tail, fmt.Errorf("[downsampling] invalid search_downsampling_v2 selector: resolution=%d, feature=%d", downsampleQuery.ResolutionMs, downsampleQuery.Feature)
	}
	sq.DownsampleQuery = &downsampleQuery
	return tail[9:], nil
}

func (q *DownsampleQuery) valid() bool {
	return q != nil && validDownsampleResolution(q.ResolutionMs) && q.Feature < countOfDownsampleFeatures
}

// nextDownsampleBHS 从选定分辨率和特征的索引中取出原生 header。
// 后续 TSID、时间范围筛选和 BlockRef 构造复用 partSearch.searchBHS。
func (ps *partSearch) nextDownsampleBHS() bool {
	for len(ps.dsMetaindex) > 0 {
		if !ps.skipTSIDsSmallerThan(&ps.dsMetaindex[0].TSID) {
			return false
		}
		// 同一 TSID 可跨越多个 index；按末尾 TSID 定位，保留全部相等边界。
		start := sort.Search(len(ps.dsMetaindex), func(i int) bool {
			return !ps.dsMetaindex[i].LastTSID.Less(&ps.BlockRef.bh.TSID)
		})
		ps.dsMetaindex = ps.dsMetaindex[start:]
		if len(ps.dsMetaindex) == 0 {
			break
		}
		mr := &ps.dsMetaindex[0]
		ps.dsMetaindex = ps.dsMetaindex[1:]
		if mr.MaxTimestamp < ps.tr.MinTimestamp || mr.MinTimestamp > ps.tr.MaxTimestamp {
			continue
		}
		indexBlockKey := blockcache.Key{Part: ps.p, Offset: mr.IndexBlockOffset}
		cached := ibCache.GetBlock(indexBlockKey)
		if cached == nil {
			ib, err := ps.readDownsampleIndexBlock(mr)
			if err != nil {
				ps.err = fmt.Errorf("[downsampling] cannot read index block for part %q at offset %d with size %d: %w", ps.p.path, mr.IndexBlockOffset, mr.IndexBlockSize, err)
				return false
			}
			cached = ib
			ibCache.TryPutBlock(indexBlockKey, cached)
		}
		ps.bhs = cached.(*indexBlock).bhs
		return true
	}
	ps.err = io.EOF
	return false
}

func (ps *partSearch) readDownsampleIndexBlock(mr *downsampleMetaindexRow) (*indexBlock, error) {
	if mr.IndexBlockSize > downsampleMaxIndexSize || mr.BlockHeadersCount == 0 || uint64(mr.BlockHeadersCount)*uint64(marshaledBlockHeaderSize) > maxBlockSize {
		return nil, fmt.Errorf("[downsampling] invalid index block size or header count")
	}
	if err := checkDownsampleExtent(mr.IndexBlockOffset, mr.IndexBlockSize, ps.p.dsIndexSize); err != nil {
		return nil, err
	}
	if cap(ps.compressedIndexBuf) < int(mr.IndexBlockSize) {
		ps.compressedIndexBuf = make([]byte, mr.IndexBlockSize)
	} else {
		ps.compressedIndexBuf = ps.compressedIndexBuf[:mr.IndexBlockSize]
	}
	// 与 raw 查询共用 part 的 fs.ReaderAt，保留 mmap、页状态判断及读取统计。
	ps.p.indexFile.MustReadAt(ps.compressedIndexBuf, int64(mr.IndexBlockOffset))
	if len(ps.compressedIndexBuf) < len(downsampleIndexMagic) || string(ps.compressedIndexBuf[:len(downsampleIndexMagic)]) != downsampleIndexMagic {
		return nil, fmt.Errorf("[downsampling] invalid index magic")
	}
	var err error
	ps.indexBuf, err = encoding.DecompressZSTDLimited(ps.indexBuf[:0], ps.compressedIndexBuf[len(downsampleIndexMagic):], maxBlockSize)
	if err != nil {
		return nil, fmt.Errorf("[downsampling] cannot decompress index block: %w", err)
	}
	ib := &indexBlock{}
	ib.bhs, err = unmarshalDownsampleIndexBlock(nil, ps.indexBuf, mr, ps.p.dsTimestampsSize, ps.p.dsValuesSize, ps.p.dsIndexSize)
	if err != nil {
		return nil, err
	}
	return ib, nil
}
