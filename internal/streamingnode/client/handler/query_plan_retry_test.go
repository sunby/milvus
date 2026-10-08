package handler

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/internal/views/qviews"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/util/mock_types"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Exercise the production resolver, picker, dial options, error conversion and
// shard retry loop. Assignment discovery, the SN plan provider and Phase 2
// execution are fakes.
func TestQueryPlanRetryThroughHandlerAndGRPC(t *testing.T) {
	paramtable.Init()
	for _, mode := range []string{"disconnect_and_reassign", "unavailable", "invalid_argument"} {
		t.Run(mode, func(t *testing.T) {
			vchannel := funcutil.GetVirtualChannel("p0", 100, 0)
			updates := make(chan *types.VersionedStreamingNodeAssignments, 2)
			var oldCalls, newCalls atomic.Int32
			var client *handlerClientImpl
			var oldServer *grpc.Server
			var newAssignment *types.VersionedStreamingNodeAssignments
			oldAddress, server := startQueryPlanRetryServer(t, func(ctx context.Context, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
				oldCalls.Add(1)
				if mode == "invalid_argument" {
					return nil, status.Error(codes.InvalidArgument, "invalid request")
				}
				if mode == "unavailable" {
					return nil, status.Error(codes.Unavailable, "transport unavailable")
				}
				// Commit this RPC before severing its connection, so transparent
				// gRPC retries cannot hide whether the shard loop retried it.
				if err := grpc.SendHeader(ctx, metadata.Pairs("plan-started", "true")); err != nil {
					return nil, err
				}
				updates <- newAssignment
				if !assert.Eventually(t, func() bool {
					assignment := client.watcher.Get(ctx, "p0")
					return assignment != nil && assignment.Channel.Term == 2
				}, time.Second, time.Millisecond) {
					return nil, status.Error(codes.Internal, "assignment did not update")
				}
				oldServer.Stop()
				return nil, ctx.Err()
			})
			oldServer = server
			newAddress, _ := startQueryPlanRetryServer(t, func(ctx context.Context, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
				newCalls.Add(1)
				pchannel, err := worknodehandler.DecodeQueryViewPChannelFromIncomingContext(ctx)
				assert.NoError(t, err)
				assert.Equal(t, types.PChannelInfo{Name: "p0", Term: 2, AccessMode: types.AccessModeRW}, pchannel)
				assert.Equal(t, qviews.UnknownReplicaID, req.GetShardId().GetReplicaId())
				return &viewpb.GetQueryPlanResponse{Plan: &viewpb.QueryPlan{
					ShardId: &viewpb.ShardID{Vchannel: vchannel, ReplicaId: 22},
					Version: &viewpb.QueryViewVersion{QueryVersion: 2},
					Request: &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: req.GetLegacySearchRequest()},
					WorkNodes: []*viewpb.QueryPlanWorkNode{{Node: &viewpb.QueryPlanWorkNode_QueryNode{
						QueryNode: &viewpb.QueryWorkNode{NodeId: 33},
					}}},
				}}, nil
			})
			assignment := func(term int64, owner int64) *types.VersionedStreamingNodeAssignments {
				nodes := map[int64]types.StreamingNodeAssignment{
					101: {NodeInfo: types.StreamingNodeInfo{ServerID: 101, Address: oldAddress}, Channels: map[string]types.PChannelInfo{}},
					102: {NodeInfo: types.StreamingNodeInfo{ServerID: 102, Address: newAddress}, Channels: map[string]types.PChannelInfo{}},
				}
				nodes[owner].Channels["p0"] = types.PChannelInfo{Name: "p0", Term: term, AccessMode: types.AccessModeRW}
				return &types.VersionedStreamingNodeAssignments{Version: typeutil.VersionInt64Pair{Global: 1, Local: term}, Assignments: nodes}
			}
			newAssignment = assignment(2, 102)
			updates <- assignment(1, 101)
			watcher := mock_types.NewMockAssignmentDiscoverWatcher(t)
			watcher.EXPECT().AssignmentDiscover(mock.Anything, mock.Anything).RunAndReturn(
				func(ctx context.Context, callback func(*types.VersionedStreamingNodeAssignments) error) error {
					for {
						select {
						case update := <-updates:
							if err := callback(update); err != nil {
								return err
							}
						case <-ctx.Done():
							return ctx.Err()
						}
					}
				})
			client = NewHandlerClient(watcher).(*handlerClientImpl)
			t.Cleanup(client.Close)
			plans := &recordingQueryPlanClient{QueryPlanClient: client.QueryViewClient()}
			queryService := &retryWorkNodeClient{t: t}
			query := queryclient.NewLegacyViewQueryClient(queryclient.ViewQueryClientConfig{}, plans, queryService,
				&retryShardResolver{vchannel: vchannel})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result, err := query.Legacy().Search(ctx, &queryclient.LegacySearchRequest{Req: &internalpb.SearchRequest{CollectionID: 100}})
			switch mode {
			case "disconnect_and_reassign":
				require.NoError(t, err)
				require.Len(t, result.Results, 1)
				require.EqualValues(t, 1, oldCalls.Load())
				require.EqualValues(t, 1, newCalls.Load())
				require.Equal(t, []codes.Code{codes.Unavailable, codes.OK}, plans.codes)
				require.EqualValues(t, 1, queryService.calls.Load())
			case "unavailable":
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.EqualValues(t, 3, oldCalls.Load())
				require.Len(t, plans.codes, 3)
			case "invalid_argument":
				require.Equal(t, codes.InvalidArgument, status.Code(err))
				require.EqualValues(t, 1, oldCalls.Load())
			}
		})
	}
}

type retryQueryPlanServer struct {
	viewpb.UnimplementedQueryPlanServiceServer
	get func(context.Context, *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error)
}

func (s *retryQueryPlanServer) GetQueryPlan(ctx context.Context, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	return s.get(ctx, req)
}

func startQueryPlanRetryServer(t *testing.T, get func(context.Context, *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error)) (string, *grpc.Server) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	viewpb.RegisterQueryPlanServiceServer(server, &retryQueryPlanServer{get: get})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String(), server
}

type recordingQueryPlanClient struct {
	queryclient.QueryPlanClient
	codes []codes.Code
}

func (c *recordingQueryPlanClient) GetQueryPlan(ctx context.Context, shard qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	resp, err := c.QueryPlanClient.GetQueryPlan(ctx, shard, req)
	c.codes = append(c.codes, status.Code(err))
	return resp, err
}

type retryShardResolver struct{ vchannel string }

func (r *retryShardResolver) ResolveVChannels(context.Context, int64) ([]string, error) {
	return []string{r.vchannel}, nil
}

type retryWorkNodeClient struct {
	queryclient.ViewQueryServiceClient
	t     *testing.T
	calls atomic.Int32
}

func (c *retryWorkNodeClient) SearchOnView(_ context.Context, node qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	c.calls.Add(1)
	assert.Equal(c.t, qviews.NewQueryNode(33), node)
	assert.EqualValues(c.t, 22, req.GetShardId().GetReplicaId())
	assert.EqualValues(c.t, 2, req.GetVersion().GetQueryVersion())
	return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success()}}, nil
}
