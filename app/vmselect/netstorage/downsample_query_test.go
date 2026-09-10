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

func TestDownsampleClusterSearchQueryFieldRoundtrip(t *testing.T) {
	for _, selector := range []string{"", "5m:last", "5m:sum", "5m:count", "5m:min", "5m:max", "1h:last", "1h:sum", "1h:count", "1h:min", "1h:max"} {
		t.Run(selector, func(t *testing.T) {
			tenants := []storage.TenantToken{{AccountID: 7, ProjectID: 11}, {AccountID: 17, ProjectID: 19}}
			filters := [][]storage.TagFilter{{{Key: []byte("__name__"), Value: []byte("cluster_downsample_probe")}}}
			sq := storage.NewMultiTenantSearchQuery(tenants, 100000, 200000, filters, 37)
			field, err := storage.ParseDownsampleQueryField(selector)
			if err != nil {
				t.Fatal(err)
			}
			sq.DownsampleField = field
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
				if field == nil {
					if rpcName != "search_v7" || !bytes.Equal(data, sq.MarshalWithoutTenant(tenant.Marshal(nil))) {
						t.Fatal("原始数据查询改变了原生 RPC 或负载")
					}
					tail, err = received.Unmarshal(data)
				} else {
					if rpcName != "search_downsampling_v2" {
						t.Fatalf("字段查询未选择 search_downsampling_v2: %s", rpcName)
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
				if !reflect.DeepEqual(received.DownsampleField, field) {
					t.Errorf("query.field 在集群查询序列化后丢失: tenant=%v selector=%q got=%+v want=%+v", tenant, selector, received.DownsampleField, field)
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
		for _, withField := range []bool{false, true} {
			results := make(chan rpcResult, 2)
			results <- rpcResult{group: group}
			results <- rpcResult{group: group, data: unsupported}
			snr := &storageNodesRequest{sns: []*storageNode{{group: group}, {group: group}}, resultsCh: results}
			var field *storage.DownsampleQueryField
			if withField {
				field = &storage.DownsampleQueryField{ResolutionMs: 300000, Feature: 1}
			}
			isPartial, err := snr.collectDataSearchResults(field, func(v any) error {
				if v == nil {
					return nil
				}
				return v.(error)
			})
			if withField {
				if !errors.Is(err, unsupported) || isPartial || !strings.HasPrefix(err.Error(), "[downsampling] ") {
					t.Fatalf("字段查询吞掉节点错误: replication=%d partial=%v err=%v", replication, isPartial, err)
				}
			} else if err != nil || isPartial != (replication == 1) {
				t.Fatalf("原有查询容错行为改变: replication=%d partial=%v err=%v", replication, isPartial, err)
			}
		}
	}
}

func TestDownsampleClusterSearchDeadlinePrefix(t *testing.T) {
	for _, withField := range []bool{true, false} {
		sq := storage.NewSearchQuery(7, 11, 86400001, 90000000, nil, 37)
		if withField {
			sq.DownsampleField = &storage.DownsampleQueryField{ResolutionMs: 300000, Feature: 1}
		}
		results, partial, err := ProcessSearchQuery(nil, false, sq, searchutil.DeadlineFromTimestamp(0))
		if results != nil || partial || err == nil || strings.HasPrefix(err.Error(), "[downsampling] ") != withField {
			t.Fatalf("查询超时错误前缀不匹配: field=%v results=%v partial=%v err=%v", withField, results, partial, err)
		}
	}
}
