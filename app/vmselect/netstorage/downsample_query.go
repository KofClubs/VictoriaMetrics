package netstorage

import (
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

// marshalDataSearchQuery 只为指定降采样分辨率与特征的数据查询选择 search_downsampling_v2，原始数据查询保持原生协议。
func marshalDataSearchQuery(dst []byte, sq *storage.SearchQuery, tenant storage.TenantToken) ([]byte, string, error) {
	dst = tenant.Marshal(dst)
	if sq.DownsampleQuery == nil {
		return sq.MarshalWithoutTenant(dst), "search_v7", nil
	}
	dst, err := sq.MarshalDownsampleWithoutTenant(dst)
	return dst, "search_downsampling_v2", err
}

func (snr *storageNodesRequest) collectDataSearchResults(downsampleQuery *storage.DownsampleQuery, consume func(any) error) (bool, error) {
	if downsampleQuery != nil {
		// 全部目标节点必须成功，不能以部分响应或副本容错掩盖旧节点不支持降采样查询。
		if err := snr.collectAllResults(consume); err != nil {
			return false, fmt.Errorf("[downsampling] cannot collect query results: %w", err)
		}
		return false, nil
	}
	return snr.collectResults(partialSearchResults, consume)
}
