//go:build test && dynamic

package adaptor

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/wab"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type queryRecoveryShardManager struct{ shards.ShardManager }

func (queryRecoveryShardManager) Close() {}

func TestWALAdaptorGetQueryPlanDuringRecovery(t *testing.T) {
	for _, event := range []string{"ready", "unrecoverable", "close"} {
		t.Run(event, func(t *testing.T) {
			mockey.PatchConvey("GetQueryPlan follows the recovered view lifecycle", t, func() {
				w := newQueryPlanTestWALAdaptor(t)
				mgr := &queryPlanTestResourceManager{}
				view := newQueryPlanTestView(viewpb.QueryViewState_QueryViewStateUp)
				w.queryViewHandler = snview.RecoverPChannelSNQueryViewHandler(w.Channel().Name, queryPlanTestCatalog{}, mgr, []*viewpb.QueryViewOfShard{view.IntoProto()})
				require.Len(t, mgr.acquired, 1)
				entered := make(chan struct{})
				var acquire func(*snview.SNQueryViewHandler, context.Context, qviews.ShardID) (*snview.QueryViewLease, error)
				mockey.Mock((*snview.SNQueryViewHandler).AcquireLatestUpView).Origin(&acquire).To(func(h *snview.SNQueryViewHandler, ctx context.Context, id qviews.ShardID) (*snview.QueryViewLease, error) {
					// GetQueryPlan has acquired the WAL lifetime reference here.
					close(entered)
					return acquire(h, ctx, id)
				}).Build()

				if event == "close" {
					w.roWALAdaptorImpl = adaptImplsToROWAL(w.rwWALImpls, func() {})
					t.Cleanup(w.availableCancel)
					w.interceptorBuildResult = buildInterceptor(nil, w.param)
					w.writeMetrics = metricsutil.NewWriteMetrics(w.Channel(), w.WALName())
					w.param.ShardManager = queryRecoveryShardManager{}
					// This test has no append path; only buffer cleanup is stubbed.
					mockey.Mock((*wab.WriteAheadBuffer).Close).Return().Build()
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				type planResult struct {
					plan *viewpb.QueryPlan
					err  error
				}
				result := make(chan planResult, 1)
				go func() {
					plan, err := w.GetQueryPlan(ctx, &viewpb.GetQueryPlanRequest{
						CollectionId: 10,
						ShardId:      &viewpb.ShardID{ReplicaId: qviews.UnknownReplicaID, Vchannel: queryPlanTestVChannel},
						Mvcc: &viewpb.GetQueryPlanRequest_QueryPlanMvcc{QueryPlanMvcc: &viewpb.QueryPlanMVCC{
							GrowingTimetick: 123, TransformingTimetick: 122,
						}},
						Request: &viewpb.GetQueryPlanRequest_LegacySearchRequest{LegacySearchRequest: &internalpb.SearchRequest{CollectionID: 10}},
					})
					result <- planResult{plan, err}
				}()
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("GetQueryPlan did not acquire its lifetime reference")
				}
				select {
				case early := <-result:
					t.Fatalf("GetQueryPlan returned before recovery: %v", early.err)
				case <-time.After(50 * time.Millisecond):
				}

				closed := make(chan struct{})
				switch event {
				case "ready":
					mgr.acquired[0].OnReady()
				case "unrecoverable":
					mgr.acquired[0].OnUnrecoverable()
				case "close":
					go func() {
						w.Close()
						close(closed)
					}()
				}
				select {
				case got := <-result:
					switch event {
					case "ready":
						require.NoError(t, got.err)
						require.Equal(t, view.Version().IntoProto(), got.plan.GetVersion())
						require.Equal(t, view.ShardID().IntoProto(), got.plan.GetShardId())
						require.Len(t, got.plan.GetWorkNodes(), 3)
					case "unrecoverable":
						require.True(t, viewerror.AsViewError(got.err).IsViewNotFound(), "%v", got.err)
					case "close":
						require.True(t, viewerror.AsViewError(got.err).IsOnShutdown(), "%v", got.err)
					}
				case <-time.After(time.Second):
					t.Fatal("GetQueryPlan did not exit; WAL shutdown may be waiting on its lifetime reference")
				}
				if event == "close" {
					select {
					case <-closed:
					case <-time.After(time.Second):
						t.Fatal("WAL Close did not finish after the query exited")
					}
				}
			})
		})
	}
}
