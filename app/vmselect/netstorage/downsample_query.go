package netstorage

import "github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"

// marshalDataSearchQuery 只为带字段的数据查询选择 search_downsampling_v2，原始数据查询保持原生协议。
func marshalDataSearchQuery(dst []byte, sq *storage.SearchQuery, tenant storage.TenantToken) ([]byte, string, error) {
	dst = tenant.Marshal(dst)
	if sq.DownsampleField == nil {
		return sq.MarshalWithoutTenant(dst), "search_v7", nil
	}
	dst, err := sq.MarshalDownsampleWithoutTenant(dst)
	return dst, "search_downsampling_v2", err
}

func (snr *storageNodesRequest) collectDataSearchResults(field *storage.DownsampleQueryField, consume func(any) error) (bool, error) {
	if field != nil {
		// 全部目标节点必须成功，不能以部分响应或副本容错掩盖旧节点不支持字段查询。
		return false, snr.collectAllResults(consume)
	}
	return snr.collectResults(partialSearchResults, consume)
}
