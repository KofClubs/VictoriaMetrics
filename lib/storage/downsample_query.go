package storage

import (
	"fmt"
	"io"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

// DownsampleQueryField 指定集群测试查询读取的分辨率与特征值。
// 该选项只选择已落盘的降采样 Block，不将原始样本转换为摘要。
type DownsampleQueryField struct {
	ResolutionMs int64
	Feature      uint8
}

// ParseDownsampleQueryField 解析 query.field；空值表示使用原有查询路径。
func ParseDownsampleQueryField(s string) (*DownsampleQueryField, error) {
	if s == "" {
		return nil, nil
	}
	resolution, feature, ok := strings.Cut(s, ":")
	if !ok {
		return nil, fmt.Errorf("[downsampling] invalid query.field %q; expected 5m:last, 5m:sum, 5m:count, 5m:min, 5m:max or the corresponding 1h field", s)
	}
	var q DownsampleQueryField
	switch resolution {
	case "5m":
		q.ResolutionMs = downsampleResolution5m
	case "1h":
		q.ResolutionMs = downsampleResolution1h
	default:
		return nil, fmt.Errorf("[downsampling] invalid query.field resolution %q; expected 5m or 1h", resolution)
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
		return nil, fmt.Errorf("[downsampling] invalid query.field feature %q; expected last, sum, count, min or max", feature)
	}
	return &q, nil
}

// MarshalDownsampleWithoutTenant 为 search_downsampling_v2 编码查询条件，保留原有租户前缀的组织方式。
// 字段查询在原有查询负载末尾增加八字节分辨率和一字节特征编号，不改变原生查询协议。
func (sq *SearchQuery) MarshalDownsampleWithoutTenant(dst []byte) ([]byte, error) {
	if !sq.DownsampleField.valid() {
		return dst, fmt.Errorf("[downsampling] search_downsampling_v2 requires a valid downsampling resolution and feature")
	}
	dst = sq.MarshalWithoutTenant(dst)
	dst = encoding.MarshalInt64(dst, sq.DownsampleField.ResolutionMs)
	dst = append(dst, sq.DownsampleField.Feature)
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
	field := DownsampleQueryField{ResolutionMs: encoding.UnmarshalInt64(tail), Feature: tail[8]}
	if !field.valid() {
		return tail, fmt.Errorf("[downsampling] invalid search_downsampling_v2 selector: resolution=%d, feature=%d", field.ResolutionMs, field.Feature)
	}
	sq.DownsampleField = &field
	return tail[9:], nil
}

func (q *DownsampleQueryField) valid() bool {
	return q != nil && validDownsampleResolution(q.ResolutionMs) && q.Feature < countOfDownsampleFeatures
}

// nextDownsampleBlock 只读取选定字段的 header，负载继续由 BlockRef 和 Block 处理。
func (ps *partSearch) nextDownsampleBlock() bool {
	for ps.tsidIdx < len(ps.tsids) {
		if !ps.dsFilterReady {
			ps.dsReader.SetFilter(&ps.tsids[ps.tsidIdx], ps.tr.MinTimestamp, ps.tr.MaxTimestamp)
			ps.dsFilterReady = true
		}
		if ps.dsReader.NextHeader() {
			bh, err := ps.dsReader.FieldHeader(ps.dsField.Feature)
			if err != nil {
				ps.err = err
				return false
			}
			ps.BlockRef.init(ps.p, &bh)
			return true
		}
		if err := ps.dsReader.Error(); err != nil {
			ps.err = err
			return false
		}
		ps.tsidIdx++
		ps.dsFilterReady = false
	}
	ps.err = io.EOF
	return false
}
