package vmselectapi

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/handshake"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/querytracer"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

type downsampleSearchTestAPI struct {
	API
	received *storage.SearchQuery
}

func (api *downsampleSearchTestAPI) InitSearch(_ *querytracer.Tracer, sq *storage.SearchQuery, _ uint64) (BlockIterator, error) {
	copy := *sq
	api.received = &copy
	value := int64(100)
	if sq.DownsampleQuery != nil {
		value += int64(sq.DownsampleQuery.Feature)
	}
	mn := storage.MetricName{AccountID: sq.AccountID, ProjectID: sq.ProjectID, MetricGroup: []byte("downsample_rpc_probe")}
	mb := storage.MetricBlock{MetricName: mn.Marshal(nil)}
	tsid := storage.TSID{AccountID: sq.AccountID, ProjectID: sq.ProjectID, MetricID: 123}
	mb.Block.Init(&tsid, []int64{sq.MinTimestamp}, []int64{value}, 0, 64)
	mb.Block.MarshalData(0, 0)
	return &downsampleSearchTestIterator{block: mb}, nil
}

type downsampleSearchTestIterator struct {
	block storage.MetricBlock
	done  bool
}

func (it *downsampleSearchTestIterator) NextBlock(dst []byte) ([]byte, bool) {
	if it.done {
		return dst, false
	}
	it.done = true
	return it.block.Marshal(dst), true
}

func (*downsampleSearchTestIterator) MustClose()   {}
func (*downsampleSearchTestIterator) Error() error { return nil }

func runDownsampleSearchRPC(t *testing.T, rpc string, payload []byte) ([]byte, *storage.SearchQuery, error) {
	t.Helper()
	client, peer := net.Pipe()
	defer client.Close()
	if err := client.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := peer.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	api := &downsampleSearchTestAPI{}
	done := make(chan error, 1)
	go func() {
		defer peer.Close()
		bc, err := handshake.VMSelectServer(peer, 0)
		if err != nil {
			done <- err
			return
		}
		defer bc.Close()
		s := &Server{api: api, concurrencyLimitCh: make(chan struct{}, 1), searchRequests: &metrics.Counter{}, metricBlocksRead: &metrics.Counter{}}
		ctx := &vmselectRequestCtx{bc: bc}
		// 模拟连接上一次执行过降采样查询，原生查询必须清除残留降采样参数。
		ctx.sq.DownsampleQuery = &storage.DownsampleQuery{ResolutionMs: 3600000, Feature: 4}
		err = s.processRPC(ctx, rpc)
		if err == nil {
			err = bc.Flush()
		}
		done <- err
	}()
	bc, err := handshake.VMSelectClient(client, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()
	framed := encoding.MarshalUint64(nil, uint64(len(payload)))
	framed = append(framed, payload...)
	if _, err := bc.Write(framed); err != nil {
		t.Fatal(err)
	}
	if err := bc.Flush(); err != nil {
		t.Fatal(err)
	}
	response, readErr := io.ReadAll(bc)
	if readErr != nil {
		t.Fatal(readErr)
	}
	err = <-done
	return response, api.received, err
}

func TestDownsampleSearchRPCDispatch(t *testing.T) {
	for _, tenant := range []storage.TenantToken{{AccountID: 7, ProjectID: 11}, {AccountID: 7, ProjectID: 19}, {AccountID: 17, ProjectID: 11}} {
		for _, selection := range []struct{ resolution, feature string }{
			{},
			{"5m", "last"}, {"5m", "sum"}, {"5m", "count"}, {"5m", "min"}, {"5m", "max"},
			{"1h", "last"}, {"1h", "sum"}, {"1h", "count"}, {"1h", "min"}, {"1h", "max"},
			{"1m", "sum"}, {"45m", "last"}, {"2h", "count"},
		} {
			t.Run(fmt.Sprintf("%s/%s/%s", tenant.String(), selection.resolution, selection.feature), func(t *testing.T) {
				sq := storage.NewSearchQuery(tenant.AccountID, tenant.ProjectID, 86400001, 90000000, nil, 37)
				downsampleQuery, err := storage.ParseDownsampleQuery(selection.resolution, selection.feature)
				if err != nil {
					t.Fatal(err)
				}
				sq.DownsampleQuery = downsampleQuery
				rpc := "search_v7"
				payload := sq.MarshalWithoutTenant(tenant.Marshal(nil))
				if downsampleQuery != nil {
					rpc = "search_downsampling_v2"
					payload, err = sq.MarshalDownsampleWithoutTenant(tenant.Marshal(nil))
					if err != nil {
						t.Fatal(err)
					}
				}
				response, received, err := runDownsampleSearchRPC(t, rpc, payload)
				if err != nil || received == nil {
					t.Fatalf("RPC 未到达存储 API: %v", err)
				}
				if received.AccountID != tenant.AccountID || received.ProjectID != tenant.ProjectID || !reflect.DeepEqual(received.DownsampleQuery, downsampleQuery) {
					t.Fatalf("RPC 分辨率、特征或租户丢失: %+v", received)
				}
				// 响应继续使用原有帧和 MetricBlock 编解码，不增加降采样专用响应格式。
				if len(response) < 24 || !bytes.Equal(response[:8], make([]byte, 8)) {
					t.Fatalf("RPC 响应缺少原有空错误帧: %x", response)
				}
				size := int(encoding.UnmarshalUint64(response[8:16]))
				if len(response) != 24+size || !bytes.Equal(response[16+size:], make([]byte, 8)) {
					t.Fatalf("RPC 响应帧边界错误: size=%d response=%d", size, len(response))
				}
				var mb storage.MetricBlock
				if tail, err := mb.Unmarshal(response[16 : 16+size]); err != nil || len(tail) != 0 {
					t.Fatalf("原有 MetricBlock 解码失败: %v", err)
				}
				if err := mb.Block.UnmarshalData(); err != nil {
					t.Fatal(err)
				}
				var mn storage.MetricName
				if err := mn.Unmarshal(mb.MetricName); err != nil || mn.AccountID != tenant.AccountID || mn.ProjectID != tenant.ProjectID {
					t.Fatalf("返回 MetricName 租户错误: %+v err=%v", mn, err)
				}
				ts, values := mb.Block.AppendRowsWithTimeRangeFilter(nil, nil, sq.GetTimeRange())
				want := float64(100)
				if downsampleQuery != nil {
					want += float64(downsampleQuery.Feature)
				}
				if len(ts) != 1 || ts[0] != sq.MinTimestamp || len(values) != 1 || values[0] != want {
					t.Fatalf("单特征 Block 响应错误: %v %v", ts, values)
				}
			})
		}
	}
}

func TestDownsampleSearchRPCRejectsInvalidPayload(t *testing.T) {
	tenant := storage.TenantToken{AccountID: 7, ProjectID: 11}
	sq := storage.NewSearchQuery(7, 11, 86400001, 90000000, nil, 37)
	legacy := sq.MarshalWithoutTenant(tenant.Marshal(nil))
	sq.DownsampleQuery = &storage.DownsampleQuery{ResolutionMs: 300000, Feature: 2}
	downsamplePayload, err := sq.MarshalDownsampleWithoutTenant(tenant.Marshal(nil))
	if err != nil {
		t.Fatal(err)
	}
	badFeature := append([]byte(nil), downsamplePayload...)
	badFeature[len(badFeature)-1] = 255
	for _, tc := range []struct {
		name    string
		rpc     string
		payload []byte
	}{
		{"降采样查询缺少分辨率与特征", "search_downsampling_v2", legacy},
		{"降采样查询特征被截断", "search_downsampling_v2", downsamplePayload[:len(downsamplePayload)-1]},
		{"降采样查询特征非法", "search_downsampling_v2", badFeature},
		{"降采样查询多余尾部", "search_downsampling_v2", append(append([]byte(nil), downsamplePayload...), 0)},
		{"原生查询不接受降采样参数", "search_v7", downsamplePayload},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, received, err := runDownsampleSearchRPC(t, tc.rpc, tc.payload)
			if err == nil || received != nil {
				t.Fatalf("非法请求进入存储 API: received=%v err=%v", received, err)
			}
		})
	}
}
