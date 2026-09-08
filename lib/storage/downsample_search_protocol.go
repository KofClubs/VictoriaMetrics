package storage

import (
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

// MarshalDownsampleWithoutTenant 为 search_downsampling_v2 编码查询条件，保留原有租户前缀的组织方式。
// 字段查询在原有查询负载末尾增加八字节分辨率和一字节特征编号，不改变原生查询协议。
func (sq *SearchQuery) MarshalDownsampleWithoutTenant(dst []byte) ([]byte, error) {
	if !sq.DownsampleField.valid() {
		return dst, fmt.Errorf("search_downsampling_v2 requires a valid downsampling resolution and feature")
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
		return tail, err
	}
	if len(tail) < 9 {
		return tail, fmt.Errorf("cannot decode search_downsampling_v2 selector: got %d bytes; need 9", len(tail))
	}
	field := DownsampleQueryField{ResolutionMs: encoding.UnmarshalInt64(tail), Feature: tail[8]}
	if !field.valid() {
		return tail, fmt.Errorf("invalid search_downsampling_v2 selector: resolution=%d, feature=%d", field.ResolutionMs, field.Feature)
	}
	sq.DownsampleField = &field
	return tail[9:], nil
}
