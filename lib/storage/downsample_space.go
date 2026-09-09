package storage

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

var errDownsampleNoSpace = errors.New("insufficient free space for downsampling")

// downsampleSpaceCacheLifetime 覆盖 fs.MustGetFreeSpace 的两秒缓存有效期。
const downsampleSpaceCacheLifetime = 2 * time.Second

// estimateDownsamplePartSize 估算整个目标 part 的编码上界，不假设源 part 已被删除。
// 估算以五个特征的聚合批次为单位；磁盘统计的五个单值 Block 行数须先换算。
func estimateDownsamplePartSize(pws []*partWrapper) uint64 {
	var rows uint64
	for _, pw := range pws {
		if pw == nil || pw.p == nil {
			return math.MaxUint64
		}
		n := pw.p.ph.RowsCount
		if pw.p.dsMetadata == nil {
			n = multiplyDownsampleSpace(n, uint64(len(downsampleResolutions)))
		} else {
			// 完整摘要每行对应五个单值 Block 行；非整除统计保守向上取整。
			physicalRows := n
			n /= countOfDownsampleFeatures
			if physicalRows%countOfDownsampleFeatures != 0 {
				n++
			}
		}
		rows = addDownsampleSpace(rows, n)
	}
	if rows == 0 {
		return 0
	}
	// 不知道 TSID 分布时，每一行都可能单独占据一个 block 和一个 index block。
	return estimateDownsampleOutputSize(rows, rows)
}

// estimateDownsampleOutputSize 以摘要行与五 Block 批次数计算共享时间戳、五值列及索引上界。
func estimateDownsampleOutputSize(rows, blocks uint64) uint64 {
	// 每个 int64 的 varint 最多十字节；MarshalValues/MarshalTimestamps 在压缩无效时退回原始 varint。
	payload := multiplyDownsampleSpace(rows, 10*uint64(countOfDownsampleFeatures+1))
	index := addDownsampleSpace(multiplyDownsampleSpace(uint64(downsampleBlockHeaderSize), 2), 256+uint64(len(downsampleIndexMagic)))
	metaindex := addDownsampleSpace(multiplyDownsampleSpace(uint64(downsampleMetaindexRowSize), 2), 256+uint64(len(downsampleMetaindexMagic)))
	indexBytes := multiplyDownsampleSpace(blocks, addDownsampleSpace(index, metaindex))
	return addDownsampleSpace(addDownsampleSpace(payload, indexBytes), downsampleMaxMetadataSize)
}

func addDownsampleSpace(a, b uint64) uint64 {
	if b > math.MaxUint64-a {
		return math.MaxUint64
	}
	return a + b
}

func multiplyDownsampleSpace(a, b uint64) uint64 {
	if a != 0 && b > math.MaxUint64/a {
		return math.MaxUint64
	}
	return a * b
}

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

var downsampleDiskBudget downsampleSpaceBudget

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
		return nil, fmt.Errorf("cannot reserve %d bytes at %q: %w", size, path, err)
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

// checkDownsampleWriteSpace 在写每个 block 前复查空间；作业的完整预算由 reserveDownsampleSpace 持有。
// 此处不重复扣除活动作业的完整预算，避免将当前 writer 自己的预留算作其他占用。
// 外部写入和缓存期间的空间变化仍可能引发实际 I/O 错误，writer 必须保留 Abort 路径。
func checkDownsampleWriteSpace(path string, rowsCount int) error {
	if rowsCount < 0 || rowsCount > maxRowsPerBlock {
		return fmt.Errorf("invalid downsampling block rows count %d", rowsCount)
	}
	size := estimateDownsampleOutputSize(uint64(rowsCount), 1)
	if rowsCount == 0 {
		// 未提供实际索引长度时，以格式允许的最大最终输出进行保守检查。
		size = downsampleMaxIndexSize + downsampleMaxMetaindexSize + downsampleMaxMetadataSize
	}
	return checkDownsamplePathSpace(path, size)
}

// checkDownsampleFinishSpace 根据 writer 尚未写入的索引长度检查最终输出，避免小 part 预留整个格式上限。
func checkDownsampleFinishSpace(path string, indexDataSize, metaindexDataSize int) error {
	if indexDataSize < 0 || indexDataSize > maxBlockSize || metaindexDataSize < 0 || metaindexDataSize > downsampleMaxMetaindexSize {
		return fmt.Errorf("invalid downsampling finish index sizes: index=%d, metaindex=%d", indexDataSize, metaindexDataSize)
	}
	var indexBytes uint64
	if indexDataSize > 0 {
		indexBytes = min(2*uint64(indexDataSize)+256+uint64(len(downsampleIndexMagic)), downsampleMaxIndexSize)
		metaindexDataSize += downsampleMetaindexRowSize
	}
	if metaindexDataSize > downsampleMaxMetaindexSize {
		return fmt.Errorf("downsampling metaindex exceeds the format size limit")
	}
	metaindexBytes := min(2*uint64(metaindexDataSize)+256+uint64(len(downsampleMetaindexMagic)), downsampleMaxMetaindexSize)
	return checkDownsamplePathSpace(path, indexBytes+metaindexBytes+downsampleMaxMetadataSize)
}

func checkDownsamplePathSpace(path string, size uint64) error {
	// 写入中的作业已经通过完整预算准入；此处只复查物理空间，避免重复扣除已反映在空闲读数中的输出。
	if err := checkDownsampleAvailableSpace(fs.MustGetFreeSpace(path), 0, size, freeDiskSpaceLimitBytes); err != nil {
		return fmt.Errorf("cannot write downsampling data at %q: %w", path, err)
	}
	return nil
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

// checkDownsampleAvailableSpace 使用逐次相减避免可用空间、预留和安全余量相加溢出。
func checkDownsampleAvailableSpace(available, held, requested, minimumFree uint64) error {
	if available < minimumFree {
		return fmt.Errorf("%w: available=%d, minimumFree=%d", errDownsampleNoSpace, available, minimumFree)
	}
	remaining := available - minimumFree
	if remaining < held || remaining-held < requested {
		return fmt.Errorf("%w: available=%d, held=%d, requested=%d, minimumFree=%d", errDownsampleNoSpace, available, held, requested, minimumFree)
	}
	return nil
}
