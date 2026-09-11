package netstorage

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/searchutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

func TestDownsampleClusterSearchQueryRoundtrip(t *testing.T) {
	for _, selection := range []struct{ resolution, feature string }{
		{},
		{"5m", "last"}, {"5m", "sum"}, {"5m", "count"}, {"5m", "min"}, {"5m", "max"},
		{"1h", "last"}, {"1h", "sum"}, {"1h", "count"}, {"1h", "min"}, {"1h", "max"},
	} {
		t.Run(selection.resolution+"_"+selection.feature, func(t *testing.T) {
			tenants := []storage.TenantToken{{AccountID: 7, ProjectID: 11}, {AccountID: 17, ProjectID: 19}}
			filters := [][]storage.TagFilter{{{Key: []byte("__name__"), Value: []byte("cluster_downsample_probe")}}}
			sq := storage.NewMultiTenantSearchQuery(tenants, 100000, 200000, filters, 37)
			downsampleQuery, err := storage.ParseDownsampleQuery(selection.resolution, selection.feature)
			if err != nil {
				t.Fatal(err)
			}
			sq.DownsampleQuery = downsampleQuery
			calls := 0
			// 使用数据查询实际调用的协议分派入口，不修改标签类 RPC 的旧编码。
			for _, tenant := range tenants {
				calls++
				data, rpcName, err := marshalDataSearchQuery(nil, sq, tenant)
				if err != nil {
					t.Fatal(err)
				}
				var received storage.SearchQuery
				var tail []byte
				if downsampleQuery == nil {
					if rpcName != "search_v7" || !bytes.Equal(data, sq.MarshalWithoutTenant(tenant.Marshal(nil))) {
						t.Fatal("原始数据查询改变了原生 RPC 或负载")
					}
					tail, err = received.Unmarshal(data)
				} else {
					if rpcName != "search_downsampling_v2" {
						t.Fatalf("降采样查询未选择 search_downsampling_v2: %s", rpcName)
					}
					tail, err = received.UnmarshalDownsample(data)
				}
				if err != nil || len(tail) != 0 {
					t.Fatalf("集群查询反序列化失败: tail=%d err=%v", len(tail), err)
				}
				if received.AccountID != tenant.AccountID || received.ProjectID != tenant.ProjectID {
					t.Fatalf("租户标识变化: got=%d:%d want=%v", received.AccountID, received.ProjectID, tenant)
				}
				if received.MinTimestamp != sq.MinTimestamp || received.MaxTimestamp != sq.MaxTimestamp || received.MaxMetrics != sq.MaxMetrics || !reflect.DeepEqual(received.TagFilterss, filters) {
					t.Fatal("集群查询的原有条件发生变化")
				}
				if !reflect.DeepEqual(received.DownsampleQuery, downsampleQuery) {
					t.Errorf("分辨率与特征在集群查询序列化后丢失: tenant=%v resolution=%q feature=%q got=%+v want=%+v", tenant, selection.resolution, selection.feature, received.DownsampleQuery, downsampleQuery)
				}
			}
			if calls != len(tenants) {
				t.Fatalf("未遍历全部租户: got=%d want=%d", calls, len(tenants))
			}
		})
	}
}

func TestDownsampleClusterSearchRejectsPartialResults(t *testing.T) {
	unsupported := errors.New("unsupported rpcName: search_downsampling_v2")
	for _, replication := range []int{1, 2} {
		group := &storageNodesGroup{name: "test", nodesCount: 2, groupsCount: 1, replicationFactor: replication}
		for _, withDownsampleQuery := range []bool{false, true} {
			results := make(chan rpcResult, 2)
			results <- rpcResult{group: group}
			results <- rpcResult{group: group, data: unsupported}
			snr := &storageNodesRequest{sns: []*storageNode{{group: group}, {group: group}}, resultsCh: results}
			var downsampleQuery *storage.DownsampleQuery
			if withDownsampleQuery {
				downsampleQuery = &storage.DownsampleQuery{ResolutionMs: 300000, Feature: 1}
			}
			isPartial, err := snr.collectDataSearchResults(downsampleQuery, func(v any) error {
				if v == nil {
					return nil
				}
				return v.(error)
			})
			if withDownsampleQuery {
				if !errors.Is(err, unsupported) || isPartial || !strings.HasPrefix(err.Error(), "[downsampling] ") {
					t.Fatalf("降采样查询吞掉节点错误: replication=%d partial=%v err=%v", replication, isPartial, err)
				}
			} else if err != nil || isPartial != (replication == 1) {
				t.Fatalf("原有查询容错行为改变: replication=%d partial=%v err=%v", replication, isPartial, err)
			}
		}
	}
}

func TestDownsampleClusterSearchDeadlinePrefix(t *testing.T) {
	for _, withDownsampleQuery := range []bool{true, false} {
		sq := storage.NewSearchQuery(7, 11, 86400001, 90000000, nil, 37)
		if withDownsampleQuery {
			sq.DownsampleQuery = &storage.DownsampleQuery{ResolutionMs: 300000, Feature: 1}
		}
		results, partial, err := ProcessSearchQuery(nil, false, sq, searchutil.DeadlineFromTimestamp(0))
		if results != nil || partial || err == nil || strings.HasPrefix(err.Error(), "[downsampling] ") != withDownsampleQuery {
			t.Fatalf("查询超时错误前缀不匹配: downsampleQuery=%v results=%v partial=%v err=%v", withDownsampleQuery, results, partial, err)
		}
	}
}
