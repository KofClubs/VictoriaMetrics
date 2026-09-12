package netstorage

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/searchutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/handshake"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/metrics"
)

func TestInitStopNodes(t *testing.T) {
	if err := flag.Set("vmstorageDialTimeout", "1ms"); err != nil {
		t.Fatalf("cannot set vmstorageDialTimeout flag: %s", err)
	}
	for range 3 {
		Init([]string{"host1", "host2"})
		runtime.Gosched()
		MustStop()
	}

	// Try initializing the netstorage with bigger number of nodes
	for range 3 {
		Init([]string{"host1", "host2", "host3"})
		runtime.Gosched()
		MustStop()
	}

	// Try initializing the netstorage with smaller number of nodes
	for range 3 {
		Init([]string{"host1"})
		runtime.Gosched()
		MustStop()
	}
}

func TestMergeSortBlocks(t *testing.T) {
	f := func(blocks []*sortBlock, dedupInterval int64, expectedResult *Result) {
		t.Helper()
		var result Result
		sbh := getSortBlocksHeap()
		sbh.sbs = append(sbh.sbs[:0], blocks...)
		mergeSortBlocks(&result, sbh, dedupInterval)
		putSortBlocksHeap(sbh)
		if !reflect.DeepEqual(result.Values, expectedResult.Values) {
			t.Fatalf("unexpected values;\ngot\n%v\nwant\n%v", result.Values, expectedResult.Values)
		}
		if !reflect.DeepEqual(result.Timestamps, expectedResult.Timestamps) {
			t.Fatalf("unexpected timestamps;\ngot\n%v\nwant\n%v", result.Timestamps, expectedResult.Timestamps)
		}
	}

	// Zero blocks
	f(nil, 1, &Result{})

	// Single block without samples
	f([]*sortBlock{{}}, 1, &Result{})

	// Single block with a single samples.
	f([]*sortBlock{
		{
			Timestamps: []int64{1},
			Values:     []float64{4.2},
		},
	}, 1, &Result{
		Timestamps: []int64{1},
		Values:     []float64{4.2},
	})

	// Single block with multiple samples.
	f([]*sortBlock{
		{
			Timestamps: []int64{1, 2, 3},
			Values:     []float64{4.2, 2.1, 10},
		},
	}, 1, &Result{
		Timestamps: []int64{1, 2, 3},
		Values:     []float64{4.2, 2.1, 10},
	})

	// Single block with multiple samples with deduplication.
	f([]*sortBlock{
		{
			Timestamps: []int64{1, 2, 3},
			Values:     []float64{4.2, 2.1, 10},
		},
	}, 2, &Result{
		Timestamps: []int64{2, 3},
		Values:     []float64{2.1, 10},
	})

	// Multiple blocks without time range intersection.
	f([]*sortBlock{
		{
			Timestamps: []int64{3, 5},
			Values:     []float64{5.2, 6.1},
		},
		{
			Timestamps: []int64{1, 2},
			Values:     []float64{4.2, 2.1},
		},
	}, 1, &Result{
		Timestamps: []int64{1, 2, 3, 5},
		Values:     []float64{4.2, 2.1, 5.2, 6.1},
	})

	// Multiple blocks with time range intersection.
	f([]*sortBlock{
		{
			Timestamps: []int64{3, 5},
			Values:     []float64{5.2, 6.1},
		},
		{
			Timestamps: []int64{1, 2, 4},
			Values:     []float64{4.2, 2.1, 42},
		},
	}, 1, &Result{
		Timestamps: []int64{1, 2, 3, 4, 5},
		Values:     []float64{4.2, 2.1, 5.2, 42, 6.1},
	})

	// Multiple blocks with time range inclusion.
	f([]*sortBlock{
		{
			Timestamps: []int64{0, 3, 5},
			Values:     []float64{9, 5.2, 6.1},
		},
		{
			Timestamps: []int64{1, 2, 4},
			Values:     []float64{4.2, 2.1, 42},
		},
	}, 1, &Result{
		Timestamps: []int64{0, 1, 2, 3, 4, 5},
		Values:     []float64{9, 4.2, 2.1, 5.2, 42, 6.1},
	})

	// Multiple blocks with identical timestamps and identical values.
	f([]*sortBlock{
		{
			Timestamps: []int64{1, 2, 4, 5},
			Values:     []float64{9, 5.2, 6.1, 9},
		},
		{
			Timestamps: []int64{1, 2, 4},
			Values:     []float64{9, 5.2, 6.1},
		},
	}, 1, &Result{
		Timestamps: []int64{1, 2, 4, 5},
		Values:     []float64{9, 5.2, 6.1, 9},
	})

	// Multiple blocks with identical timestamps.
	f([]*sortBlock{
		{
			Timestamps: []int64{1, 2, 4, 5},
			Values:     []float64{9, 5.2, 6.1, 9},
		},
		{
			Timestamps: []int64{1, 2, 4},
			Values:     []float64{4.2, 2.1, 42},
		},
	}, 1, &Result{
		Timestamps: []int64{1, 2, 4, 5},
		Values:     []float64{9, 5.2, 42, 9},
	})
	// Multiple blocks with identical timestamps, disabled deduplication.
	f([]*sortBlock{
		{
			Timestamps: []int64{1, 2, 4},
			Values:     []float64{9, 5.2, 6.1},
		},
		{
			Timestamps: []int64{1, 2, 4},
			Values:     []float64{4.2, 2.1, 42},
		},
	}, 0, &Result{
		Timestamps: []int64{1, 1, 2, 2, 4, 4},
		Values:     []float64{9, 4.2, 2.1, 5.2, 6.1, 42},
	})

	// Multiple blocks with identical timestamp ranges.
	f([]*sortBlock{
		{
			Timestamps: []int64{1, 2, 5, 10, 11},
			Values:     []float64{9, 8, 7, 6, 5},
		},
		{
			Timestamps: []int64{1, 2, 4, 10, 11, 12},
			Values:     []float64{21, 22, 23, 24, 25, 26},
		},
	}, 1, &Result{
		Timestamps: []int64{1, 2, 4, 5, 10, 11, 12},
		Values:     []float64{21, 22, 23, 7, 24, 25, 26},
	})

	// Multiple blocks with identical timestamp ranges, no deduplication.
	f([]*sortBlock{
		{
			Timestamps: []int64{1, 2, 5, 10, 11},
			Values:     []float64{9, 8, 7, 6, 5},
		},
		{
			Timestamps: []int64{1, 2, 4, 10, 11, 12},
			Values:     []float64{21, 22, 23, 24, 25, 26},
		},
	}, 0, &Result{
		Timestamps: []int64{1, 1, 2, 2, 4, 5, 10, 10, 11, 11, 12},
		Values:     []float64{9, 21, 22, 8, 23, 7, 6, 24, 25, 5, 26},
	})

	// Multiple blocks with identical timestamp ranges with deduplication.
	f([]*sortBlock{
		{
			Timestamps: []int64{1, 2, 5, 10, 11},
			Values:     []float64{9, 8, 7, 6, 5},
		},
		{
			Timestamps: []int64{1, 2, 4, 10, 11, 12},
			Values:     []float64{21, 22, 23, 24, 25, 26},
		},
	}, 5, &Result{
		Timestamps: []int64{5, 10, 12},
		Values:     []float64{7, 24, 26},
	})
}

func TestEqualSamplesPrefix(t *testing.T) {
	f := func(a, b *sortBlock, expected int) {
		t.Helper()

		actual := equalSamplesPrefix(a, b)
		if actual != expected {
			t.Fatalf("unexpected result: got %d, want %d", actual, expected)
		}
	}

	// Empty blocks
	f(&sortBlock{}, &sortBlock{}, 0)

	// Identical blocks
	f(&sortBlock{
		Timestamps: []int64{1, 2, 3, 4},
		Values:     []float64{5, 6, 7, 8},
	}, &sortBlock{
		Timestamps: []int64{1, 2, 3, 4},
		Values:     []float64{5, 6, 7, 8},
	}, 4)

	// Non-zero NextIdx
	f(&sortBlock{
		Timestamps: []int64{1, 2, 3, 4},
		Values:     []float64{5, 6, 7, 8},
		NextIdx:    2,
	}, &sortBlock{
		Timestamps: []int64{10, 20, 3, 4},
		Values:     []float64{50, 60, 7, 8},
		NextIdx:    2,
	}, 2)

	// Non-zero NextIdx with mismatch
	f(&sortBlock{
		Timestamps: []int64{1, 2, 3, 4},
		Values:     []float64{5, 6, 7, 8},
		NextIdx:    1,
	}, &sortBlock{
		Timestamps: []int64{10, 2, 3, 4},
		Values:     []float64{50, 6, 7, 80},
		NextIdx:    1,
	}, 2)

	// Different lengths
	f(&sortBlock{
		Timestamps: []int64{1, 2, 3, 4},
		Values:     []float64{5, 6, 7, 8},
	}, &sortBlock{
		Timestamps: []int64{1, 2, 3},
		Values:     []float64{5, 6, 7},
	}, 3)

	// Timestamps diverge
	f(&sortBlock{
		Timestamps: []int64{1, 2, 3, 4},
		Values:     []float64{5, 6, 7, 8},
	}, &sortBlock{
		Timestamps: []int64{1, 2, 30, 4},
		Values:     []float64{5, 6, 7, 8},
	}, 2)

	// Values diverge
	f(&sortBlock{
		Timestamps: []int64{1, 2, 3, 4},
		Values:     []float64{5, 6, 7, 8},
	}, &sortBlock{
		Timestamps: []int64{1, 2, 3, 4},
		Values:     []float64{5, 60, 7, 8},
	}, 1)

	// Zero matches
	f(&sortBlock{
		Timestamps: []int64{1, 2, 3, 4},
		Values:     []float64{5, 6, 7, 8},
	}, &sortBlock{
		Timestamps: []int64{5, 6, 7, 8},
		Values:     []float64{1, 2, 3, 4},
	}, 0)

	// Compare staleness markers, matching
	f(&sortBlock{
		Timestamps: []int64{1, 2, 3, 4},
		Values:     []float64{5, decimal.StaleNaN, 7, 8},
	}, &sortBlock{
		Timestamps: []int64{1, 2, 3, 4},
		Values:     []float64{5, decimal.StaleNaN, 7, 8},
	}, 4)

	// Special float values: +Inf, -Inf, 0, -0
	f(&sortBlock{
		Timestamps: []int64{1, 2, 3, 4},
		Values:     []float64{math.Inf(1), math.Inf(-1), math.Copysign(0, +1), math.Copysign(0, -1)},
	}, &sortBlock{
		Timestamps: []int64{1, 2, 3, 4},
		Values:     []float64{math.Inf(1), math.Inf(-1), math.Copysign(0, +1), math.Copysign(0, -1)},
	}, 4)

	// Positive zero vs negative zero (bitwise different)
	f(&sortBlock{
		Timestamps: []int64{1, 2},
		Values:     []float64{5, math.Copysign(0, +1)},
	}, &sortBlock{
		Timestamps: []int64{1, 2},
		Values:     []float64{5, math.Copysign(0, -1)},
	}, 1)
}

func TestDownsampleSearchRPCDispatchAndPartialResults(t *testing.T) {
	oldDialTimeout, oldGlobalReplication, oldSkipSlow := *vmstorageDialTimeout, *globalReplicationFactor, *skipSlowReplicas
	*vmstorageDialTimeout, *globalReplicationFactor, *skipSlowReplicas = time.Second, 1, false
	t.Cleanup(func() {
		*vmstorageDialTimeout, *globalReplicationFactor, *skipSlowReplicas = oldDialTimeout, oldGlobalReplication, oldSkipSlow
	})
	tenants := []storage.TenantToken{{AccountID: 7, ProjectID: 11}, {AccountID: 17, ProjectID: 19}}
	completed := make(chan struct{}, 1)
	goodAddr, goodRequests := newSearchQueryRPCPeer(t, false, tenants[1], completed)
	failedAddr, failedRequests := newSearchQueryRPCPeer(t, true, tenants[1], completed)
	group := &storageNodesGroup{name: "query-test", nodesCount: 2, groupsCount: 1}
	ms := metrics.NewSet()
	sns := []*storageNode{newStorageNode(ms, group, goodAddr), newStorageNode(ms, group, failedAddr)}
	t.Cleanup(func() {
		for _, sn := range sns {
			sn.connPool.MustStop()
		}
	})
	for _, selection := range []*storage.DownsampleQuery{nil, {ResolutionMs: 300000, Feature: 1}, {ResolutionMs: 3600000, Feature: 4}} {
		for _, policy := range []struct {
			name        string
			replicas    int
			denyPartial bool
			wantPartial bool
			wantError   bool
		}{{"partial", 1, false, true, false}, {"denied", 1, true, false, true}, {"replicated", 2, true, false, false}} {
			t.Run(fmt.Sprintf("selection_%v/%s", selection, policy.name), func(t *testing.T) {
				group.replicationFactor = policy.replicas
				sq := storage.NewSearchQuery(0, 0, 86400001, 90000000, nil, 37)
				sq.IsMultiTenant, sq.TenantTokens, sq.DownsampleQuery = true, tenants, selection
				var gotValues []float64
				processBlock := func(data []byte, _ uint) error {
					var block storage.MetricBlock
					if tail, err := block.Unmarshal(data); err != nil || len(tail) != 0 {
						return fmt.Errorf("invalid returned block: tail=%d err=%v", len(tail), err)
					}
					if err := block.Block.UnmarshalData(); err != nil {
						return err
					}
					_, values := block.Block.AppendRowsWithTimeRangeFilter(nil, nil, sq.GetTimeRange())
					gotValues = append(gotValues, values...)
					return nil
				}
				partial, err := processBlocksInternal(nil, sns, policy.denyPartial, sq, processBlock, searchutil.NewDeadline(time.Now(), 5*time.Second, "test"))
				if partial != policy.wantPartial || (err != nil) != policy.wantError {
					t.Fatalf("unexpected node failure policy: partial=%v err=%v", partial, err)
				}
				if policy.wantError {
					var statusErr *httpserver.ErrorWithStatusCode
					if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusServiceUnavailable {
						t.Fatalf("denied partial response must preserve the shared HTTP 503 error: %v", err)
					}
				} else {
					wantValues := []float64{searchQueryRPCValue(tenants[0], selection), searchQueryRPCValue(tenants[1], selection)}
					if !reflect.DeepEqual(gotValues, wantValues) {
						t.Fatalf("tenant or selector data was lost: got=%v want=%v", gotValues, wantValues)
					}
				}
				// 请求记录在响应写完后才发送；等待三个实际 RPC 完成后再切换策略。
				for _, source := range []struct {
					requests <-chan storage.SearchQuery
					tenants  []storage.TenantToken
				}{{goodRequests, tenants}, {failedRequests, tenants[:1]}} {
					for _, tenant := range source.tenants {
						select {
						case received := <-source.requests:
							if received.AccountID != tenant.AccountID || received.ProjectID != tenant.ProjectID || received.MinTimestamp != sq.MinTimestamp || received.MaxTimestamp != sq.MaxTimestamp || received.MaxMetrics != sq.MaxMetrics || !reflect.DeepEqual(received.DownsampleQuery, selection) {
								t.Fatalf("outbound RPC changed tenant or query parameters: got=%+v selector=%+v", received, received.DownsampleQuery)
							}
						case <-time.After(5 * time.Second):
							t.Fatal("outbound tenant RPC did not complete")
						}
					}
				}
			})
		}
	}
}

func searchQueryRPCValue(tenant storage.TenantToken, selection *storage.DownsampleQuery) float64 {
	value := float64(tenant.AccountID)*1000 + float64(tenant.ProjectID)
	if selection != nil {
		value += float64(selection.ResolutionMs) + float64(selection.Feature)
	}
	return value
}

// newSearchQueryRPCPeer 使用真实连接与 RPC 帧，验证客户端逐租户发送的请求。
func newSearchQueryRPCPeer(t *testing.T, fail bool, lastTenant storage.TenantToken, completed chan struct{}) (string, <-chan storage.SearchQuery) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	requests := make(chan storage.SearchQuery, 16)
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
					t.Error(err)
					return
				}
				bc, err := handshake.VMSelectServer(conn, 0)
				if err != nil {
					t.Error(err)
					return
				}
				defer bc.Close()
				for {
					rpc, err := readBytes(nil, bc, 128)
					if err != nil {
						if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
							t.Error(err)
						}
						return
					}
					var preamble [5]byte // traceEnabled 与 timeoutSeconds。
					if _, err := io.ReadFull(bc, preamble[:]); err != nil {
						t.Error(err)
						return
					}
					data, err := readBytes(nil, bc, 1<<20)
					if err != nil {
						t.Error(err)
						return
					}
					var sq storage.SearchQuery
					switch string(rpc) {
					case "search_v7":
						data, err = sq.Unmarshal(data)
					case "search_downsampling_v2":
						data, err = sq.UnmarshalDownsample(data)
					default:
						err = fmt.Errorf("unexpected RPC %q", rpc)
					}
					if err != nil || len(data) != 0 {
						t.Errorf("invalid RPC payload: tail=%d err=%v", len(data), err)
						return
					}
					var frames [][]byte
					if fail {
						select {
						case <-completed:
						case <-stopCh:
							return
						}
						frames = [][]byte{[]byte("search.maxConcurrentRequests reached"), nil}
					} else {
						tenant := storage.TenantToken{AccountID: sq.AccountID, ProjectID: sq.ProjectID}
						mn := storage.MetricName{AccountID: tenant.AccountID, ProjectID: tenant.ProjectID, MetricGroup: []byte("rpc_probe")}
						block := storage.MetricBlock{MetricName: mn.Marshal(nil)}
						block.Block.Init(&storage.TSID{AccountID: tenant.AccountID, ProjectID: tenant.ProjectID, MetricID: 1}, []int64{sq.MinTimestamp}, []int64{int64(searchQueryRPCValue(tenant, sq.DownsampleQuery))}, 0, 64)
						block.Block.MarshalData(0, 0)
						frames = [][]byte{nil, block.Marshal(nil), nil, nil}
					}
					for _, frame := range frames {
						if err := writeBytes(bc, frame); err != nil {
							t.Error(err)
							return
						}
					}
					if err := bc.Flush(); err != nil {
						t.Error(err)
						return
					}
					select {
					case requests <- sq:
					case <-stopCh:
						return
					}
					if !fail && sq.AccountID == lastTenant.AccountID && sq.ProjectID == lastTenant.ProjectID {
						select {
						case completed <- struct{}{}:
						case <-stopCh:
							return
						}
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		close(stopCh)
		ln.Close()
		wg.Wait() // 节点连接池先执行 Cleanup，关闭全部已接受连接。
	})
	return ln.Addr().String(), requests
}
