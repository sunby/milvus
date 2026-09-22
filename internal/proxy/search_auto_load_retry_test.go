// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/ratelimitutil"
)

// A released collection loses its views. This server injects that transition at
// Phase 1 or Phase 2, and carries the actual ViewError through gRPC details.
type dqlRetryServer struct {
	viewpb.UnimplementedQueryPlanServiceServer
	viewpb.UnimplementedViewQueryServiceServer
	loaded    atomic.Bool
	injected  atomic.Bool
	plans     atomic.Int32
	searches  atomic.Int32
	queries   atomic.Int32
	failPhase string
}

func (s *dqlRetryServer) failure(phase string) error {
	if s.failPhase == "transport" && phase == "plan" {
		return status.Error(codes.Unavailable, "connection reset by peer")
	}
	if s.failPhase == phase && s.injected.CompareAndSwap(false, true) {
		s.loaded.Store(false)
		return viewerror.NewGRPCStatusFromViewError(viewerror.NewViewInvalidated("collection released during %s", phase)).Err()
	}
	if !s.loaded.Load() {
		return viewerror.NewGRPCStatusFromViewError(viewerror.NewViewNotFound("collection is released")).Err()
	}
	return nil
}

func (s *dqlRetryServer) GetQueryPlan(_ context.Context, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	s.plans.Add(1)
	if err := s.failure("plan"); err != nil {
		return nil, err
	}
	plan := &viewpb.QueryPlan{
		ShardId: &viewpb.ShardID{Vchannel: req.GetShardId().GetVchannel(), ReplicaId: 1},
		Version: &viewpb.QueryViewVersion{},
		Mvcc:    &viewpb.QueryPlanMVCC{},
		WorkNodes: []*viewpb.QueryPlanWorkNode{{Node: &viewpb.QueryPlanWorkNode_QueryNode{
			QueryNode: &viewpb.QueryWorkNode{NodeId: 11},
		}}},
	}
	if search := req.GetLegacySearchRequest(); search != nil {
		plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: search}
	} else {
		plan.Request = &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: req.GetLegacyRetrieveRequest()}
	}
	return &viewpb.GetQueryPlanResponse{Plan: plan}, nil
}

func (s *dqlRetryServer) SearchOnView(context.Context, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	s.searches.Add(1)
	if err := s.failure("search"); err != nil {
		return nil, err
	}
	return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success()}}, nil
}

func (s *dqlRetryServer) QueryOnView(context.Context, *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	s.queries.Add(1)
	if err := s.failure("query"); err != nil {
		return nil, err
	}
	return &viewpb.QueryOnViewResponse{LegacyResults: &internalpb.RetrieveResults{Status: merr.Success()}}, nil
}

type dqlRetryPlanClient struct {
	queryclient.QueryPlanClient
	client viewpb.QueryPlanServiceClient
}

func (c *dqlRetryPlanClient) GetQueryPlan(ctx context.Context, _ qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	resp, err := c.client.GetQueryPlan(ctx, req)
	return resp, viewerror.ConvertViewError("GetQueryPlan", err)
}

type dqlRetryServiceClient struct {
	queryclient.ViewQueryServiceClient
	client viewpb.ViewQueryServiceClient
}

func (c *dqlRetryServiceClient) SearchOnView(ctx context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	resp, err := c.client.SearchOnView(ctx, req)
	return resp, viewerror.ConvertViewError("SearchOnView", err)
}

func (c *dqlRetryServiceClient) QueryOnView(ctx context.Context, _ qviews.WorkNode, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	resp, err := c.client.QueryOnView(ctx, req)
	return resp, viewerror.ConvertViewError("QueryOnView", err)
}

type dqlRetryClient struct {
	queryclient.Client
	server *dqlRetryServer
	checks atomic.Int32
	waits  atomic.Int32
}

func (c *dqlRetryClient) ResolveVChannels(context.Context, int64) ([]string, error) {
	return []string{"v0"}, nil
}

func (c *dqlRetryClient) CheckCollectionReady(context.Context, int64, []string) error {
	c.checks.Add(1)
	if !c.server.loaded.Load() {
		return merr.WrapErrCollectionNotLoaded(100)
	}
	return nil
}

func (c *dqlRetryClient) WaitForCollectionReady(context.Context, int64, []string) error {
	c.waits.Add(1)
	if c.server.failure("wait") != nil {
		return merr.WrapErrCollectionNotLoaded(100)
	}
	return nil
}

func newDQLRetryClient(t *testing.T, phase string) *dqlRetryClient {
	t.Helper()
	server := &dqlRetryServer{failPhase: phase}
	server.loaded.Store(true)
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	viewpb.RegisterQueryPlanServiceServer(grpcServer, server)
	viewpb.RegisterViewQueryServiceServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() { grpcServer.Stop(); _ = listener.Close() })
	conn, err := grpc.NewClient("passthrough:///dql-retry", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := &dqlRetryClient{server: server}
	client.Client = queryclient.NewLegacyViewQueryClient(queryclient.ViewQueryClientConfig{},
		&dqlRetryPlanClient{client: viewpb.NewQueryPlanServiceClient(conn)},
		&dqlRetryServiceClient{client: viewpb.NewViewQueryServiceClient(conn)}, client)
	return client
}

// Use the real public Proxy entry, scheduler, task Execute, legacy query client,
// shard retries, gRPC error transport, automatic load task and readiness checks.
// Parsing/reducing results and coordinator storage are replaced by test doubles.
func TestDQLAutoLoadRetryThroughTaskAndGRPC(t *testing.T) {
	for _, test := range []struct{ method, phase string }{
		{"Search", "plan"},
		{"Search", "search"},
		{"HybridSearch", "search"},
		{"Query", "query"},
		{"Requery", "query"},
		{"SearchByPK", "query"},
		{"SearchByPK", "search"},
		{"Search", "wait"},
	} {
		t.Run(test.method+"/"+test.phase, func(t *testing.T) {
			node, coordinator, _ := newAutoLoadSchedulingProxy(t)
			if test.method == "SearchByPK" {
				schema, err := node.getMetaCache().GetCollectionSchema(context.Background(), "db", "collection")
				require.NoError(t, err)
				node.getMetaCache().(*MockCache).EXPECT().GetCollectionInfo(mock.Anything, "db", "collection", int64(0)).Return(
					&collectionInfo{CollID: 100, Schema: schema}, nil).Maybe()
			}
			oldRateCol := rateCol
			require.NoError(t, node.initRateCollector())
			t.Cleanup(func() { rateCol = oldRateCol })
			client := newDQLRetryClient(t, test.phase)
			node.viewQueryClient = client
			if test.phase == "wait" {
				client.server.loaded.Store(false)
			}
			var loads atomic.Int32
			coordinator.EXPECT().LoadCollection(mock.Anything, mock.Anything).RunAndReturn(
				func(_ context.Context, req *querypb.LoadCollectionRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
					require.EqualValues(t, 100, req.GetCollectionID())
					loads.Add(1)
					client.server.loaded.Store(true)
					return merr.Success(), nil
				})

			var searchTasks, queryTasks atomic.Int32
			searchPre := mockey.Mock((*searchTask).PreExecute).To(func(st *searchTask, _ context.Context) error {
				searchTasks.Add(1)
				st.CollectionID = 100
				st.Nq = 1
				return nil
			}).Build()
			t.Cleanup(func() { searchPre.UnPatch() })
			queryPre := mockey.Mock((*queryTask).PreExecute).To(func(qt *queryTask, _ context.Context) error {
				queryTasks.Add(1)
				qt.CollectionID = 100
				return nil
			}).Build()
			t.Cleanup(func() { queryPre.UnPatch() })
			queryPost := mockey.Mock((*queryTask).PostExecute).To(func(qt *queryTask, _ context.Context) error {
				qt.result = &milvuspb.QueryResults{Status: merr.Success()}
				if test.method == "SearchByPK" {
					qt.result.FieldsData = []*schemapb.FieldData{
						{FieldId: 100, FieldName: "id", Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}}}},
						{FieldId: 101, FieldName: "vector", Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 4, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}}}}},
					}
				}
				return nil
			}).Build()
			t.Cleanup(func() { queryPost.UnPatch() })
			searchPost := mockey.Mock((*searchTask).PostExecute).To(func(st *searchTask, ctx context.Context) error {
				if test.method == "Requery" {
					op := &requeryOperator{
						traceCtx: ctx, node: node, dbName: "db", collectionName: "collection",
						primaryFieldSchema: &schemapb.FieldSchema{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					}
					_, _, err := op.requery(ctx, trace.SpanFromContext(ctx),
						&schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}, []string{"id"})
					if err != nil {
						return err
					}
				}
				st.result = &milvuspb.SearchResults{Status: merr.Success(), Results: &schemapb.SearchResultData{NumQueries: 1}}
				return nil
			}).Build()
			t.Cleanup(func() { searchPost.UnPatch() })
			require.NoError(t, node.sched.Start())

			var status *commonpb.Status
			switch test.method {
			case "HybridSearch":
				resp, err := node.HybridSearch(node.ctx, &milvuspb.HybridSearchRequest{DbName: "db", CollectionName: "collection"})
				require.NoError(t, err)
				status = resp.GetStatus()
			case "Query":
				resp, err := node.Query(node.ctx, &milvuspb.QueryRequest{DbName: "db", CollectionName: "collection"})
				require.NoError(t, err)
				status = resp.GetStatus()
			default:
				req := &milvuspb.SearchRequest{DbName: "db", CollectionName: "collection", Nq: 1}
				if test.method == "SearchByPK" {
					req.SearchInput = &milvuspb.SearchRequest_Ids{Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}}
				}
				resp, err := node.Search(node.ctx, req)
				require.NoError(t, err)
				status = resp.GetStatus()
			}
			require.NoError(t, merr.Error(status))
			require.True(t, client.server.injected.Load())
			require.EqualValues(t, 2, client.checks.Load())
			if test.phase == "wait" {
				require.EqualValues(t, 2, loads.Load())
				require.EqualValues(t, 1, searchTasks.Load())
			} else {
				require.EqualValues(t, 1, loads.Load())
				if test.method == "SearchByPK" && test.phase == "search" {
					require.EqualValues(t, 1, queryTasks.Load())
					require.EqualValues(t, 2, searchTasks.Load())
				} else if test.method == "Query" || test.method == "SearchByPK" {
					require.EqualValues(t, 2, queryTasks.Load())
				} else {
					require.EqualValues(t, 2, searchTasks.Load())
				}
			}
		})
	}
}

func TestDQLAutoLoadRetryDoesNotReloadHealthyCollection(t *testing.T) {
	enableAutoLoad(t)
	client := &autoLoadViewQueryClient{}
	node := &Proxy{metaCache: mockSearchCollectionMeta(t, 100, []string{"v0"}), viewQueryClient: client}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	calls := 0
	err := node.retryDQL(context.Background(), "db", "collection", func(context.Context) (bool, error) {
		calls++
		if calls == 1 {
			return false, merr.Wrap(viewerror.NewViewInvalidated("view replaced by balance"), "executing search")
		}
		return false, nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, client.checkCalls)
	require.Equal(t, 0, client.waitCalls)
}

func TestDQLQueryPlanTransportRetryDoesNotMultiplyAttempts(t *testing.T) {
	for _, method := range []string{"Search", "HybridSearch", "Query"} {
		t.Run(method, func(t *testing.T) {
			enableAutoLoad(t)
			client := newDQLRetryClient(t, "transport")
			node := &Proxy{metaCache: mockSearchCollectionMeta(t, 100, []string{"v0"}), viewQueryClient: client}
			node.UpdateStateCode(commonpb.StateCode_Healthy)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			calls := 0
			err := node.retryDQL(ctx, "db", "collection", func(ctx context.Context) (bool, error) {
				calls++
				if method == "Query" {
					_, err := client.Legacy().Query(ctx, &queryclient.LegacyQueryRequest{Req: &internalpb.RetrieveRequest{CollectionID: 100}})
					return false, err
				}
				_, err := client.Legacy().Search(ctx, &queryclient.LegacySearchRequest{Req: &internalpb.SearchRequest{
					CollectionID: 100, IsAdvanced: method == "HybridSearch",
				}})
				return false, err
			})
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.Equal(t, 1, calls)
			require.EqualValues(t, 3, client.server.plans.Load())
			require.EqualValues(t, 1, client.checks.Load())
			require.Zero(t, client.waits.Load())
			require.Zero(t, client.server.searches.Load())
			require.Zero(t, client.server.queries.Load())
		})
	}
}

func TestDQLAutoLoadRetryStopsOnPermanentErrorsAndCancellation(t *testing.T) {
	enableAutoLoad(t)
	node := &Proxy{viewQueryClient: &autoLoadViewQueryClient{}}
	ready := mockey.Mock((*Proxy).ensureCollectionReady).Return(nil).Build()
	defer ready.UnPatch()
	for _, want := range []error{
		merr.ErrCollectionNotFound,
		merr.ErrParameterInvalid,
		viewerror.NewOnShutdownError("shutdown"),
		merr.WrapErrAsInputError(merr.ErrCollectionNotLoaded),
	} {
		calls := 0
		err := node.retryDQL(context.Background(), "db", "collection", func(context.Context) (bool, error) {
			calls++
			return false, want
		})
		require.ErrorIs(t, err, want)
		require.Equal(t, 1, calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := node.retryDQL(ctx, "db", "collection", func(context.Context) (bool, error) {
		calls++
		cancel()
		return false, viewerror.NewViewNotFound("released")
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
}

func TestDQLAutoLoadRetryDisabled(t *testing.T) {
	require.NoError(t, Params.Save(Params.ProxyCfg.EnableAutoLoad.Key, "false"))
	t.Cleanup(func() { require.NoError(t, Params.Reset(Params.ProxyCfg.EnableAutoLoad.Key)) })
	node := &Proxy{viewQueryClient: &autoLoadViewQueryClient{}}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	calls := 0
	want := viewerror.NewViewInvalidated("released")
	err := node.retryDQL(context.Background(), "db", "collection", func(context.Context) (bool, error) {
		calls++
		return false, want
	})
	require.ErrorIs(t, err, want)
	require.Equal(t, 1, calls)
}

func TestDQLAutoLoadRetryQueryCreatesFreshTask(t *testing.T) {
	enableAutoLoad(t)
	node := &Proxy{viewQueryClient: &autoLoadViewQueryClient{}}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	oldRateCol := rateCol
	require.NoError(t, node.initRateCollector())
	defer func() { rateCol = oldRateCol }()
	ready := mockey.Mock((*Proxy).ensureCollectionReady).Return(nil).Build()
	defer ready.UnPatch()
	request := &milvuspb.QueryRequest{DbName: "db", CollectionName: "collection", Expr: "id > 0", GuaranteeTimestamp: 1234}
	var tasks []*queryTask
	query := mockey.Mock((*Proxy).query).To(func(_ *Proxy, _ context.Context, qt *queryTask, _ trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
		tasks = append(tasks, qt)
		require.Same(t, request, qt.request)
		if len(tasks) == 1 {
			require.Equal(t, "id > 0", qt.request.Expr)
		} else {
			require.Equal(t, "changed", qt.request.Expr)
		}
		require.EqualValues(t, 1234, qt.request.GuaranteeTimestamp)
		qt.request.Expr = "changed"
		if len(tasks) == 1 {
			err := viewerror.NewViewInvalidated("released")
			return &milvuspb.QueryResults{Status: merr.Status(err)}, segcore.StorageCost{}, err
		}
		return &milvuspb.QueryResults{Status: merr.Success()}, segcore.StorageCost{}, nil
	}).Build()
	defer query.UnPatch()
	resp, err := node.Query(context.Background(), request)
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	require.Len(t, tasks, 2)
	require.NotSame(t, tasks[0], tasks[1])
	require.NotSame(t, tasks[0].Condition, tasks[1].Condition)
	require.Equal(t, "changed", request.Expr)
}

func TestDQLAutoLoadRetryHonorsDeadline(t *testing.T) {
	enableAutoLoad(t)
	node := &Proxy{viewQueryClient: &autoLoadViewQueryClient{}}
	ready := mockey.Mock((*Proxy).ensureCollectionReady).Return(nil).Build()
	defer ready.UnPatch()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	calls := 0
	err := node.retryDQL(ctx, "db", "collection", func(ctx context.Context) (bool, error) {
		calls++
		<-ctx.Done()
		return false, viewerror.NewViewInvalidated("released while request timed out")
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 1, calls)
}

func TestDQLAutoLoadRetryPreservesLatestReadinessTimeout(t *testing.T) {
	enableAutoLoad(t)
	node := &Proxy{viewQueryClient: &autoLoadViewQueryClient{}}
	readinessCalls := 0
	ready := mockey.Mock((*Proxy).ensureCollectionReady).To(
		func(_ *Proxy, ctx context.Context, _, _ string) error {
			readinessCalls++
			if readinessCalls == 1 {
				return nil
			}
			childCtx, cancel := context.WithTimeout(ctx, time.Nanosecond)
			defer cancel()
			<-childCtx.Done()
			return context.Cause(childCtx)
		}).Build()
	defer ready.UnPatch()

	parentCtx := context.Background()
	executeCalls := 0
	err := node.retryDQL(parentCtx, "db", "collection", func(context.Context) (bool, error) {
		executeCalls++
		return false, viewerror.NewViewNotFound("collection released")
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, parentCtx.Err())
	require.Equal(t, 2, readinessCalls)
	require.Equal(t, 1, executeCalls)
}

func TestDQLAutoLoadRetryAccumulatesAttemptTiming(t *testing.T) {
	enableAutoLoad(t)
	node := &Proxy{viewQueryClient: &autoLoadViewQueryClient{}}
	readinessCalls := 0
	ready := mockey.Mock((*Proxy).ensureCollectionReady).To(
		func(*Proxy, context.Context, string, string) error {
			readinessCalls++
			if readinessCalls == 1 {
				time.Sleep(5 * time.Millisecond)
			} else {
				time.Sleep(10 * time.Millisecond)
			}
			return nil
		}).Build()
	defer ready.UnPatch()

	ctx, timing := startDQL(context.Background(), "Query")
	defer timing.End(merr.Success(), nil)
	executeCalls := 0
	err := node.retryDQL(ctx, "db", "collection", func(context.Context) (bool, error) {
		executeCalls++
		if executeCalls == 1 {
			time.Sleep(20 * time.Millisecond)
			return false, viewerror.NewViewInvalidated("collection released")
		}
		time.Sleep(30 * time.Millisecond)
		return false, nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, readinessCalls)
	require.Equal(t, 2, executeCalls)
	require.Equal(t, dqlStageNone, timing.currentStage)
	require.GreaterOrEqual(t, timing.stageDurations[dqlStageReadiness], 15*time.Millisecond)
	require.GreaterOrEqual(t, timing.stageDurations[dqlStageExecution], 50*time.Millisecond)
	require.GreaterOrEqual(t, timing.stageDurations[dqlStageRetryWait], 200*time.Millisecond)
}

func TestDQLAutoLoadRetryPreservesSearchFallbackAndRecall(t *testing.T) {
	for _, mode := range []string{"fallback", "recall"} {
		t.Run(mode, func(t *testing.T) {
			enableAutoLoad(t)
			key := Params.AutoIndexConfig.EnableResultLimitCheck.Key
			previous := Params.AutoIndexConfig.EnableResultLimitCheck.GetValue()
			require.NoError(t, Params.Save(key, "true"))
			t.Cleanup(func() { require.NoError(t, Params.Save(key, previous)) })
			node := &Proxy{viewQueryClient: &autoLoadViewQueryClient{}}
			checks := 0
			ready := mockey.Mock((*Proxy).ensureCollectionReady).To(func(*Proxy, context.Context, string, string) error {
				checks++
				return nil
			}).Build()
			defer ready.UnPatch()
			calls := 0
			search := mockey.Mock((*Proxy).search).To(func(_ *Proxy, _ context.Context, _ *milvuspb.SearchRequest, optimized, recall bool) (*milvuspb.SearchResults, bool, bool, bool, error) {
				calls++
				if calls == 1 {
					require.True(t, optimized)
					return &milvuspb.SearchResults{Status: merr.Success()}, mode == "fallback", mode == "fallback", mode == "recall", nil
				}
				if calls == 2 {
					require.False(t, optimized)
					require.Equal(t, mode == "recall", recall)
					err := viewerror.NewViewInvalidated("released during extra search")
					return &milvuspb.SearchResults{Status: merr.Status(err)}, false, false, false, err
				}
				return &milvuspb.SearchResults{Status: merr.Success()}, false, false, false, nil
			}).Build()
			defer search.UnPatch()
			resp, err := node.Search(context.Background(), &milvuspb.SearchRequest{DbName: "db", CollectionName: "collection"})
			require.NoError(t, err)
			require.NoError(t, merr.Error(resp.GetStatus()))
			require.Equal(t, 3, calls)
			require.Equal(t, 2, checks)
		})
	}
}

func TestDQLAutoLoadRetryPreservesInconsistentRequery(t *testing.T) {
	require.NoError(t, Params.Save(Params.ProxyCfg.EnableAutoLoad.Key, "false"))
	t.Cleanup(func() { require.NoError(t, Params.Reset(Params.ProxyCfg.EnableAutoLoad.Key)) })
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	calls := 0
	search := mockey.Mock((*Proxy).hybridSearch).To(func(*Proxy, context.Context, *milvuspb.HybridSearchRequest, bool) (*milvuspb.SearchResults, bool, bool, error) {
		calls++
		if calls == 1 {
			return &milvuspb.SearchResults{Status: merr.Status(merr.ErrInconsistentRequery)}, false, false, nil
		}
		return &milvuspb.SearchResults{Status: merr.Success()}, false, false, nil
	}).Build()
	defer search.UnPatch()
	resp, err := node.HybridSearch(context.Background(), &milvuspb.HybridSearchRequest{DbName: "db", CollectionName: "collection"})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	require.Equal(t, 2, calls)
}

func TestDQLQueryRetryCountsRequestOnce(t *testing.T) {
	enableAutoLoad(t)
	node := &Proxy{viewQueryClient: &autoLoadViewQueryClient{}}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	previous := rateCol
	require.NoError(t, node.initRateCollector())
	t.Cleanup(func() { rateCol = previous })
	counter := metrics.ProxyReceivedNQ.WithLabelValues(paramtable.GetStringNodeID(), metrics.QueryLabel, "db", "retry_count")
	before := testutil.ToFloat64(counter)
	ready := mockey.Mock((*Proxy).ensureCollectionReady).Return(nil).Build()
	defer ready.UnPatch()
	calls := 0
	query := mockey.Mock((*Proxy).query).To(func(*Proxy, context.Context, *queryTask, trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
		calls++
		if calls <= 2 {
			err := viewerror.NewViewInvalidated("retry query")
			return &milvuspb.QueryResults{Status: merr.Status(err)}, segcore.StorageCost{}, err
		}
		return &milvuspb.QueryResults{Status: merr.Success()}, segcore.StorageCost{}, nil
	}).Build()
	defer query.UnPatch()
	resp, err := node.Query(context.Background(), &milvuspb.QueryRequest{DbName: "db", CollectionName: "retry_count"})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	require.Equal(t, 3, calls)
	require.Equal(t, before+1, testutil.ToFloat64(counter))
	rate, err := rateCol.Rate(internalpb.RateType_DQLQuery.String(), ratelimitutil.DefaultWindow)
	require.NoError(t, err)
	require.InDelta(t, 1/float64(ratelimitutil.DefaultWindow/ratelimitutil.DefaultGranularity), rate, 1e-9)
}
