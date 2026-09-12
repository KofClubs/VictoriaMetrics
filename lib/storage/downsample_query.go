package storage

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/blockcache"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
)

const (
	// 初始 map 会整组分配槽位，先预留 1 KiB，覆盖仅一两个 bucket 时的槽组及管理结构。
	downsampleQueryMapMemory = uint64(16 * (unsafe.Sizeof(int64(0)) + unsafe.Sizeof(downsampleSample{})))
	// 当前 TSID 的 map、扩容时的新旧存储以及排序键共同计费；四倍键值大小覆盖负载率和切片容量增长。
	downsampleQueryBucketMemory = uint64(4 * (unsafe.Sizeof(int64(0)) + unsafe.Sizeof(downsampleSample{})))
)

// 查询聚合共享进程可用内存的 5%，最多 256 MiB；预算不足立即返回错误，不等待其他查询释放内存。
var getDownsampleQueryMemoryLimiter = sync.OnceValue(func() *memory.Limiter {
	return &memory.Limiter{MaxSize: min(uint64(memory.Allowed())/20, 256<<20)}
})

// DownsampleQuery 指定目标分辨率和特征；每个租户、part 选择最大的可整除目标分辨率的已落盘源列。
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
		return nil, fmt.Errorf("[downsampling] resolution and feature must be provided together")
	}
	resolutionMs, err := parseDownsampleResolution(resolution)
	if err != nil {
		return nil, fmt.Errorf("[downsampling] invalid resolution %q: %w", resolution, err)
	}
	q := DownsampleQuery{ResolutionMs: resolutionMs}
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
		return nil, fmt.Errorf("[downsampling] invalid feature %q; expected last, sum, count, min or max", feature)
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
		return tail, err
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

// SourceTimeRange 扩展到完整目标 bucket，防止查询边界截断贡献或改变聚合结果的最后时间戳。
// 输出仍在聚合完成后使用原查询范围过滤；输入范围不得先按 retention 截断隐藏的 BASE 贡献。
func (q *DownsampleQuery) SourceTimeRange(tr TimeRange) (TimeRange, error) {
	if !q.valid() {
		return tr, fmt.Errorf("[downsampling] invalid query resolution or feature")
	}
	tr.MinTimestamp = max(tr.MinTimestamp, minUnixMilli)
	tr.MaxTimestamp = min(tr.MaxTimestamp, maxUnixMilli)
	if tr.MinTimestamp > tr.MaxTimestamp {
		return tr, nil
	}
	tr.MinTimestamp = max(tr.MinTimestamp-tr.MinTimestamp%q.ResolutionMs, minUnixMilli)
	bucketStart := tr.MaxTimestamp - tr.MaxTimestamp%q.ResolutionMs
	tr.MaxTimestamp = maxUnixMilli
	if q.ResolutionMs <= maxUnixMilli-bucketStart {
		tr.MaxTimestamp = bucketStart + q.ResolutionMs - 1
	}
	return tr, nil
}

// initDownsampleQuery 为每个租户选择最大的可整除目标分辨率的实际源区间，按完整 TSID 顺序组织遍历。
// 配置和索引均来自不可变 part 元数据；运行时配置变化不改变已经开始的查询。
func (ps *partSearch) initDownsampleQuery(q *DownsampleQuery) error {
	config := ps.p.dsMetadata.DownsamplingConfig
	if config == nil || config.BaseResolutionMs() <= 0 || q.ResolutionMs%config.BaseResolutionMs() != 0 {
		return fmt.Errorf("[downsampling] query resolution %d must be an integer multiple of the part base resolution", q.ResolutionMs)
	}
	var previousTenant TenantToken
	for i := range ps.tsids {
		tsid := &ps.tsids[i]
		tenant := TenantToken{AccountID: tsid.AccountID, ProjectID: tsid.ProjectID}
		if i > 0 && tenant == previousTenant {
			continue
		}
		previousTenant = tenant
		resolutions := config.resolutionsForTenant(tenant.AccountID, tenant.ProjectID)
		for i := len(resolutions) - 1; i >= 0; i-- {
			sourceResolution := resolutions[i]
			if sourceResolution > q.ResolutionMs || q.ResolutionMs%sourceResolution != 0 {
				continue
			}
			rows := ps.p.dsMetaindex
			start := sort.Search(len(rows), func(i int) bool {
				row := &rows[i]
				if row.ResolutionMs != sourceResolution {
					return row.ResolutionMs > sourceResolution
				}
				if row.feature != q.Feature {
					return row.feature > q.Feature
				}
				return row.TSID.AccountID > tenant.AccountID || (row.TSID.AccountID == tenant.AccountID && row.TSID.ProjectID >= tenant.ProjectID)
			})
			end := start + sort.Search(len(rows)-start, func(i int) bool {
				row := &rows[start+i]
				return row.ResolutionMs != sourceResolution || row.feature != q.Feature || row.TSID.AccountID != tenant.AccountID || row.TSID.ProjectID != tenant.ProjectID
			})
			if start != end {
				ps.dsMetaindexRanges = append(ps.dsMetaindexRanges, rows[start:end])
				break
			}
		}
	}
	return nil
}

// downsampleQueryState 只保留当前 TSID 的实际非空 bucket，并把原生索引堆提供的所有 part、partition 贡献合并后输出。
type downsampleQueryState struct {
	// selector 是本次请求的目标分辨率和单个特征，按值保存，避免调用方后续修改。
	selector DownsampleQuery
	// outputTimeRange 在完整 bucket 聚合后筛选输出，已包含查询 retention 下限。
	outputTimeRange TimeRange
	// deadline 沿用 Search 的期限；块读取和聚合定期调用原有 deadline/pace 检查。
	deadline uint64
	// sourceRows 用于沿用原有查询 pace 检查频率，不改变节点错误或部分响应策略。
	sourceRows uint64
	// currentSourceBlock 复用当前原生输入块，所有磁盘数据仍通过 BlockRef.MustReadBlock 获取。
	currentSourceBlock Block
	// currentSourceValues 仅解码请求特征的浮点值，不读取其他四个特征。
	currentSourceValues []float64
	// pendingSource 保存已经前进到下一 TSID 的原生引用；part 生命周期由 tableSearch 的快照保护。
	pendingSource BlockRef
	// hasPendingSource 表示下一 TSID 的首块已经由原生堆取出，尚未消费。
	hasPendingSource bool
	// currentTSID 标识当前聚合和输出的序列，包含完整租户信息。
	currentTSID TSID
	// currentTSIDBuckets 按实际遇到的 bucket 分配状态，不按整个查询时间跨度预留空槽。
	currentTSIDBuckets map[int64]downsampleSample
	// bucketMemoryLimiter 在所有降采样查询之间限制聚合内存，不改变 raw 查询及其错误传播方式。
	bucketMemoryLimiter *memory.Limiter
	// bucketMemoryReserved 对应 map 和排序键的容量高水位；清空 TSID 不释放容量，关闭查询时统一归还。
	bucketMemoryReserved uint64
	// currentTSIDBucketIDs 保存当前 TSID 的有序 bucket 编号，供分块输出。
	currentTSIDBucketIDs []int64
	// currentOutputPosition 指向下一条待输出 bucket；精度变化或原生行数上限会结束当前输出块。
	currentOutputPosition int
	// currentOutputTimestamps 复用当前输出批次的时间戳，编码后的 Block 自己拥有不可变数据。
	currentOutputTimestamps []int64
	// currentOutputValues 复用当前输出批次的单特征浮点值。
	currentOutputValues []float64
	// currentOutputDecimalValues 复用原生 decimal 编码转换缓冲。
	currentOutputDecimalValues []int64
	// currentOutputBlockRef 只持有当前不可变输出块；外部复制引用可由 Go 引用保持其存活，无需缓存整个查询输出。
	currentOutputBlockRef BlockRef
}

// nextBlock 完成当前 TSID 的所有源贡献后再输出，避免同 bucket 在不同 block、index 或 partition 中被提前结束。
func (q *downsampleQueryState) nextBlock(ts *tableSearch) bool {
	if ts.err != nil && ts.err != io.EOF {
		return false
	}
	for {
		if q.deadline > 0 {
			if err := checkSearchDeadlineAndPace(q.deadline); err != nil {
				ts.err = err
				return false
			}
		}
		if q.currentOutputPosition < len(q.currentTSIDBucketIDs) {
			if q.emitBlock(ts) {
				return true
			}
			if ts.Error() != nil {
				return false
			}
		}
		clear(q.currentTSIDBuckets)
		q.currentTSIDBucketIDs = q.currentTSIDBucketIDs[:0]
		q.currentOutputPosition = 0
		var source BlockRef
		if q.hasPendingSource {
			source = q.pendingSource
			q.pendingSource.reset()
			q.hasPendingSource = false
		} else {
			if !ts.nextSourceBlock() {
				return false
			}
			source = *ts.BlockRef
		}
		q.currentTSID = source.bh.TSID
		for {
			if err := q.addSourceBlock(&source); err != nil {
				ts.err = err
				return false
			}
			if !ts.nextSourceBlock() {
				if ts.Error() != nil {
					return false
				}
				break
			}
			source = *ts.BlockRef
			if source.bh.TSID != q.currentTSID {
				q.pendingSource = source
				q.hasPendingSource = true
				break
			}
		}
		for bucketID := range q.currentTSIDBuckets {
			if q.deadline > 0 && len(q.currentTSIDBucketIDs)&paceLimiterSlowIterationsMask == 0 {
				if err := checkSearchDeadlineAndPace(q.deadline); err != nil {
					ts.err = err
					return false
				}
			}
			q.currentTSIDBucketIDs = append(q.currentTSIDBucketIDs, bucketID)
		}
		sort.Slice(q.currentTSIDBucketIDs, func(i, j int) bool { return q.currentTSIDBucketIDs[i] < q.currentTSIDBucketIDs[j] })
	}
}

// addSourceBlock 只解码选定特征；sum/count 累加源值，last/min/max 沿用 downsampleSample 的统一合并语义。
func (q *downsampleQueryState) addSourceBlock(source *BlockRef) error {
	if q.deadline > 0 {
		if err := checkSearchDeadlineAndPace(q.deadline); err != nil {
			return err
		}
	}
	source.MustReadBlock(&q.currentSourceBlock)
	b := &q.currentSourceBlock
	if err := b.UnmarshalData(); err != nil {
		return fmt.Errorf("[downsampling] cannot unmarshal block: %w", err)
	}
	q.currentSourceValues = decimal.AppendDecimalToFloat(q.currentSourceValues[:0], b.values, b.bh.Scale)
	if q.currentTSIDBuckets == nil {
		q.currentTSIDBuckets = make(map[int64]downsampleSample)
	}
	for i, timestamp := range b.timestamps {
		if q.deadline > 0 && q.sourceRows&paceLimiterSlowIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(q.deadline); err != nil {
				return err
			}
		}
		q.sourceRows++
		bucketID, err := downsampleBucketID(timestamp, q.selector.ResolutionMs)
		if err != nil {
			return err
		}
		current, exists := q.currentTSIDBuckets[bucketID]
		if !exists {
			// 新增 bucket 前先取得预算；重复贡献以及复用上一 TSID 已保留的容量不重复计费。
			needed := downsampleQueryMapMemory + uint64(len(q.currentTSIDBuckets)+1)*downsampleQueryBucketMemory
			if needed > q.bucketMemoryReserved {
				if q.bucketMemoryLimiter == nil {
					q.bucketMemoryLimiter = getDownsampleQueryMemoryLimiter()
				}
				additional := needed - q.bucketMemoryReserved
				if !q.bucketMemoryLimiter.Get(additional) {
					return fmt.Errorf("[downsampling] query aggregation exceeds the shared memory budget of %d bytes; reduce the query time range or use a coarser resolution", q.bucketMemoryLimiter.MaxSize)
				}
				q.bucketMemoryReserved = needed
			}
		}
		if !current.isEmpty() && current.precisionBits != b.bh.PrecisionBits {
			return fmt.Errorf("[downsampling] source precision differs within a query bucket: %d vs %d", current.precisionBits, b.bh.PrecisionBits)
		}
		contribution := downsampleSample{timestamp: timestamp, precisionBits: b.bh.PrecisionBits}
		contribution.values[q.selector.Feature] = q.currentSourceValues[i]
		current.Merge(&contribution)
		q.currentTSIDBuckets[bucketID] = current
	}
	return nil
}

// emitBlock 生成独立的原生编码 Block；输出对象不复用、不归池，避免已复制的 BlockRef 读到后续批次。
func (q *downsampleQueryState) emitBlock(ts *tableSearch) bool {
	q.currentOutputTimestamps = q.currentOutputTimestamps[:0]
	q.currentOutputValues = q.currentOutputValues[:0]
	var precisionBits uint8
	for q.currentOutputPosition < len(q.currentTSIDBucketIDs) {
		if q.deadline > 0 && q.currentOutputPosition&paceLimiterSlowIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(q.deadline); err != nil {
				ts.err = err
				return false
			}
		}
		sample := q.currentTSIDBuckets[q.currentTSIDBucketIDs[q.currentOutputPosition]]
		if sample.timestamp < q.outputTimeRange.MinTimestamp || sample.timestamp > q.outputTimeRange.MaxTimestamp {
			q.currentOutputPosition++
			continue
		}
		if len(q.currentOutputTimestamps) == maxRowsPerBlock || (precisionBits != 0 && precisionBits != sample.precisionBits) {
			break
		}
		precisionBits = sample.precisionBits
		q.currentOutputTimestamps = append(q.currentOutputTimestamps, sample.timestamp)
		q.currentOutputValues = append(q.currentOutputValues, sample.values[q.selector.Feature])
		q.currentOutputPosition++
	}
	if len(q.currentOutputTimestamps) == 0 {
		return false
	}
	var scale int16
	q.currentOutputDecimalValues, scale = decimal.AppendFloatToDecimal(q.currentOutputDecimalValues[:0], q.currentOutputValues)
	block := &Block{}
	block.Init(&q.currentTSID, q.currentOutputTimestamps, q.currentOutputDecimalValues, scale, precisionBits)
	marshalDownsampleBlock(block, q.currentOutputTimestamps, 0, 0)
	q.currentOutputBlockRef = BlockRef{bh: block.bh, downsampleBlock: block}
	ts.BlockRef = &q.currentOutputBlockRef
	return true
}

// nextDownsampleBHS 从选定分辨率和特征的索引中取出原生 header。
// 后续 TSID、时间范围筛选和 BlockRef 构造复用 partSearch.searchBHS。
func (ps *partSearch) nextDownsampleBHS() bool {
	for {
		if len(ps.dsMetaindex) == 0 {
			if len(ps.dsMetaindexRanges) == 0 {
				ps.err = io.EOF
				return false
			}
			ps.dsMetaindex = ps.dsMetaindexRanges[0]
			ps.dsMetaindexRanges = ps.dsMetaindexRanges[1:]
		}
		if !ps.skipTSIDsSmallerThan(&ps.dsMetaindex[0].TSID) {
			return false
		}
		// 同一 TSID 可跨越多个 index；按末尾 TSID 定位，保留全部相等边界。
		start := sort.Search(len(ps.dsMetaindex), func(i int) bool {
			return !ps.dsMetaindex[i].LastTSID.Less(&ps.BlockRef.bh.TSID)
		})
		ps.dsMetaindex = ps.dsMetaindex[start:]
		if len(ps.dsMetaindex) == 0 {
			continue
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
