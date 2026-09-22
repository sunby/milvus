package queryclient

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type retryPlanClient struct {
	QueryPlanClient
	get func(context.Context, qviews.ShardID, *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error)
}

func (c *retryPlanClient) GetQueryPlan(ctx context.Context, shard qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	return c.get(ctx, shard, req)
}

type retryQueryServiceClient struct {
	ViewQueryServiceClient
}

func (*retryQueryServiceClient) SearchOnView(context.Context, qviews.WorkNode, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success()}}, nil
}

func (*retryQueryServiceClient) QueryOnView(context.Context, qviews.WorkNode, *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	return &viewpb.QueryOnViewResponse{LegacyResults: &internalpb.RetrieveResults{Status: merr.Success(), AllRetrieveCount: 1}}, nil
}

func TestQueryPlanTransportRetryPreservesOtherShards(t *testing.T) {
	for _, method := range []string{"Search", "HybridSearch", "Query"} {
		t.Run(method, func(t *testing.T) {
			var healthyCalls, failedCalls atomic.Int32
			planClient := &retryPlanClient{get: func(_ context.Context, shard qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
				require.Equal(t, qviews.UnknownReplicaID, shard.ReplicaID)
				if shard.VChannel == "healthy" {
					healthyCalls.Add(1)
				} else if failedCalls.Add(1) == 1 {
					return nil, viewerror.ConvertViewError("GetQueryPlan", status.Error(codes.Unavailable, "connection reset by peer"))
				}
				shard.ReplicaID = 2
				plan := legacySearchPlan(shard, qviews.NewQueryNode(11))
				if method == "Query" {
					plan = legacyQueryPlan(shard, qviews.NewQueryNode(11))
				} else {
					require.Equal(t, method == "HybridSearch", req.GetLegacySearchRequest().GetIsAdvanced())
				}
				return &viewpb.GetQueryPlanResponse{Plan: plan}, nil
			}}
			client := NewLegacyViewQueryClient(ViewQueryClientConfig{}, planClient,
				&retryQueryServiceClient{}, &legacyResolver{vchannels: []string{"healthy", "failed"}})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if method == "Query" {
				result, err := client.Legacy().Query(ctx, &LegacyQueryRequest{Req: &internalpb.RetrieveRequest{CollectionID: 100}})
				require.NoError(t, err)
				require.Len(t, result.Plans, 2)
				require.Len(t, result.Results, 2)
			} else {
				result, err := client.Legacy().Search(ctx, &LegacySearchRequest{Req: &internalpb.SearchRequest{
					CollectionID: 100, IsAdvanced: method == "HybridSearch",
				}})
				require.NoError(t, err)
				require.Len(t, result.Results, 2)
			}
			require.EqualValues(t, 1, healthyCalls.Load())
			require.EqualValues(t, 2, failedCalls.Load())
		})
	}
}

func TestQueryPlanRetryClassificationAndBudget(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want int
	}{
		{"unavailable", status.Error(codes.Unavailable, "transport is closing"), 3},
		{"wrapped_rpc_reset", merr.Wrap(viewerror.ConvertViewError("GetQueryPlan", status.Error(codes.Unavailable, "connection reset by peer")), "planning"), 3},
		{"eof", io.EOF, 3},
		{"wrapped_eof", merr.Wrap(io.EOF, "planning"), 3},
		{"view_not_found", viewerror.NewViewNotFound("recovering"), 3},
		{"invalid_argument", status.Error(codes.InvalidArgument, "bad request"), 1},
		{"permission_denied", status.Error(codes.PermissionDenied, "denied"), 1},
		{"internal", status.Error(codes.Internal, "unexpected EOF"), 1},
		{"unknown", viewerror.NewUnknownError("planning failed"), 1},
		{"input_error", merr.WrapErrAsInputError(merr.Wrap(io.EOF, "invalid request")), 1},
		{"canceled", context.Canceled, 1},
		{"deadline", context.DeadlineExceeded, 1},
		{"rpc_canceled", status.Error(codes.Canceled, "canceled"), 1},
		{"rpc_deadline", status.Error(codes.DeadlineExceeded, "expired"), 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			client := newShardViewQueryClient(3, &retryPlanClient{get: func(context.Context, qviews.ShardID, *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
				calls++
				return nil, test.err
			}}, nil)
			_, err := client.Search(context.Background(), &ShardSearchRequest{VChannel: "v0", Req: &internalpb.SearchRequest{}, Reducer: fakeSearchResultReducer{}})
			require.Equal(t, test.err, err, "retry exhaustion must preserve the last error")
			require.Equal(t, test.want, calls)
		})
	}
}

func TestQueryPlanRetrySharesViewRetryBudget(t *testing.T) {
	finalErr := status.Error(codes.Unavailable, "last connection failure")
	errors := []error{io.EOF, viewerror.NewViewNotFound("recovering"), finalErr}
	calls := 0
	client := newShardViewQueryClient(3, &retryPlanClient{get: func(context.Context, qviews.ShardID, *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
		err := errors[calls]
		calls++
		return nil, err
	}}, nil)
	_, err := client.Search(context.Background(), &ShardSearchRequest{VChannel: "v0", Req: &internalpb.SearchRequest{}, Reducer: fakeSearchResultReducer{}})
	require.Same(t, finalErr, err)
	require.Equal(t, 3, calls)
}

func TestQueryPlanRetryPreservesPhaseTwoPolicy(t *testing.T) {
	for _, retryView := range []bool{false, true} {
		t.Run(map[bool]string{false: "transport_error", true: "view_invalidated"}[retryView], func(t *testing.T) {
			shard := qviews.ShardID{ReplicaID: 2, VChannel: "v0"}
			planCalls, dispatchCalls, resets := 0, 0, 0
			client := newShardViewQueryClient(3, &retryPlanClient{get: func(context.Context, qviews.ShardID, *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
				planCalls++
				if planCalls == 2 {
					return nil, io.EOF
				}
				return &viewpb.GetQueryPlanResponse{Plan: newTestSearchQueryPlan(shard, nil)}, nil
			}}, nil)
			transportErr := status.Error(codes.Unavailable, "Phase 2 disconnected")
			_, err := client.executeShard(context.Background(), shard.VChannel, &shardExecParams{
				buildPlanReq: func(id qviews.ShardID) *viewpb.GetQueryPlanRequest {
					return &viewpb.GetQueryPlanRequest{ShardId: id.IntoProto()}
				},
				dispatchNode: func(context.Context, qviews.WorkNode, *viewpb.QueryPlan, qviews.ShardID) error {
					dispatchCalls++
					if !retryView {
						return transportErr
					}
					if dispatchCalls == 1 {
						return viewerror.NewViewInvalidated("old plan expired")
					}
					return nil
				},
				resetShard: func(id qviews.ShardID) {
					require.Equal(t, shard, id)
					resets++
				},
			})
			if retryView {
				require.NoError(t, err)
				require.Equal(t, 3, planCalls)
				require.Equal(t, 2, dispatchCalls)
				require.Equal(t, 1, resets)
			} else {
				require.Same(t, transportErr, err)
				require.Equal(t, 1, planCalls)
				require.Equal(t, 1, dispatchCalls)
				require.Zero(t, resets)
			}
		})
	}
}

func TestQueryPlanRetryHonorsContext(t *testing.T) {
	for _, when := range []string{"before_call", "during_backoff", "last_attempt"} {
		t.Run(when, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := context.Canceled
			wantCalls := 0
			switch when {
			case "before_call":
				cancel()
			case "during_backoff":
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 20*time.Millisecond)
				defer stop()
				want = context.DeadlineExceeded
				wantCalls = 1
			case "last_attempt":
				wantCalls = 3
			}
			calls := 0
			client := newShardViewQueryClient(3, &retryPlanClient{get: func(callCtx context.Context, _ qviews.ShardID, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
				require.Same(t, ctx, callCtx)
				calls++
				if when == "last_attempt" && calls == 3 {
					cancel()
				}
				return nil, status.Error(codes.Unavailable, "disconnected")
			}}, nil)
			_, err := client.Search(ctx, &ShardSearchRequest{VChannel: "v0", Req: &internalpb.SearchRequest{}, Reducer: fakeSearchResultReducer{}})
			require.ErrorIs(t, err, want)
			require.Equal(t, wantCalls, calls)
		})
	}
}
