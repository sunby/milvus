//go:build test && dynamic

package snview

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func withSNViewReplica(view qviews.QueryViewAtWorkNode, replicaID int64) qviews.QueryViewAtWorkNode {
	pb := view.IntoProto()
	pb.Meta.ReplicaId = replicaID
	return qviews.NewQueryViewAtWorkNodeFromProto(pb)
}

func withSNViewState(view qviews.QueryViewAtWorkNode, state qviews.QueryViewState) qviews.QueryViewAtWorkNode {
	pb := view.IntoProto()
	pb.Meta.State = viewpb.QueryViewState(state)
	return qviews.NewQueryViewAtWorkNodeFromProto(pb)
}

func TestSNHandler_UnknownViewsDoNotCreateShard(t *testing.T) {
	states := []viewpb.QueryViewState{
		viewpb.QueryViewState_QueryViewStateUnknown,
		viewpb.QueryViewState_QueryViewStateReady,
		viewpb.QueryViewState_QueryViewStateUp,
		viewpb.QueryViewState_QueryViewStateDown,
		viewpb.QueryViewState_QueryViewStateUnrecoverable,
		viewpb.QueryViewState_QueryViewStateDropping,
		viewpb.QueryViewState_QueryViewStateDropped,
		viewpb.QueryViewState_QueryViewStateUpRecovering,
	}
	for _, state := range states {
		t.Run(state.String(), func(t *testing.T) {
			cat := newMockCatalog()
			mgr := newMockResourceManager()
			h := recoverSNQueryViewHandler(testPChannel, cat, mgr, nil)
			view := newFullSNViewWithState(1, state, 101)
			rc := &reportCollector{}
			h.ApplyViews([]handler.ApplyView{{View: view, OnReport: func(report qviews.QueryViewAtWorkNode) {
				if assert.True(t, h.mu.TryLock(), "OnReport must run outside the handler lock") {
					h.mu.Unlock()
				}
				rc.onReport(report)
			}}})

			require.Equal(t, 1, rc.count())
			wantState := qviews.QueryViewStateUnrecoverable
			if state == viewpb.QueryViewState_QueryViewStateDropped {
				wantState = qviews.QueryViewStateDropped
			}
			assert.Equal(t, wantState, rc.last().State())
			assert.Equal(t, view.QueryViewKey(), rc.last().QueryViewKey())
			assert.Equal(t, view.IntoProto().GetQueryNode(), rc.last().IntoProto().GetQueryNode())
			assert.Equal(t, qviews.QueryViewState(state), view.State(), "reporting must not mutate the incoming view")
			assert.Empty(t, h.shards)
			assert.Empty(t, h.shardsByVChannel)
			assert.Zero(t, mgr.acquiredCount())
			assert.Zero(t, mgr.releasedCount())
			assert.Zero(t, cat.savedCount())

			// A disconnected stream can leave no callback; it still must not
			// create a query-planning candidate or acquire resources.
			h.ApplyViews([]handler.ApplyView{{View: view}})
			assert.Empty(t, h.shards)
			assert.Empty(t, h.shardsByVChannel)
			assert.Zero(t, mgr.acquiredCount())
		})
	}
}

func TestSNHandler_UnknownBatchPreservesReportOrder(t *testing.T) {
	h := recoverSNQueryViewHandler(testPChannel, newMockCatalog(), newMockResourceManager(), nil)
	rc := &reportCollector{}
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDropped), OnReport: rc.onReport},
		{View: newSNViewWithState(2, viewpb.QueryViewState_QueryViewStateReady), OnReport: rc.onReport},
		{View: newSNViewWithState(3, viewpb.QueryViewState_QueryViewStateUp), OnReport: rc.onReport},
		{View: newSNViewWithState(4, viewpb.QueryViewState_QueryViewStateDown), OnReport: rc.onReport},
	})

	reports := rc.get()
	require.Len(t, reports, 4)
	for i, version := range []int64{3, 1, 2, 4} {
		assert.Equal(t, version, reports[i].QueryViewKey().QueryViewVersion.QueryVersion)
	}
	assert.Equal(t, qviews.QueryViewStateDropped, reports[1].State())
	assert.Empty(t, h.shards)
	assert.Empty(t, h.shardsByVChannel)
}

func TestSNHandler_UnknownDroppedWithPreparingCreatesServingShard(t *testing.T) {
	cat := newMockCatalog()
	mgr := newMockResourceManager()
	h := recoverSNQueryViewHandler(testPChannel, cat, mgr, nil)
	rc := &reportCollector{}
	preparing := newPreparingSNView(2)
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDropped), OnReport: rc.onReport},
		{View: preparing, OnReport: rc.onReport},
	})

	reports := rc.get()
	require.Len(t, reports, 2)
	assert.Equal(t, qviews.QueryViewStatePreparing, reports[0].State())
	assert.Equal(t, preparing.QueryViewKey(), reports[0].QueryViewKey())
	assert.Equal(t, qviews.QueryViewStateDropped, reports[1].State())
	require.Contains(t, h.shards, preparing.ShardID())
	assert.Len(t, h.shards[preparing.ShardID()].views, 1)
	assert.Contains(t, h.shardsByVChannel[testVChannel], preparing.ShardID())
	require.Equal(t, 1, mgr.acquiredCount())
	acquired, ok := mgr.getAcquired(preparing.QueryViewKey())
	require.True(t, ok)
	acquired.OnReady()
	h.ApplyViews([]handler.ApplyView{{View: withSNViewState(preparing, qviews.QueryViewStateUp)}})

	lease, err := h.AcquireLatestUpView(context.Background(), preparing.ShardID())
	require.NoError(t, err)
	defer lease.Release()
	assert.Equal(t, preparing.QueryViewKey().QueryViewVersion, lease.Version)
}

func TestSNHandler_ClosedHandlerDoesNotReportUnknownViews(t *testing.T) {
	mgr := newMockResourceManager()
	h := recoverSNQueryViewHandler(testPChannel, newMockCatalog(), mgr, nil)
	h.CloseForHandoff()
	rc := &reportCollector{}
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp), OnReport: rc.onReport},
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDropped), OnReport: rc.onReport},
	})
	assert.Zero(t, rc.count())
	assert.Zero(t, mgr.acquiredCount())
	assert.Empty(t, h.shards)
	assert.Empty(t, h.shardsByVChannel)
}

func TestSNHandler_CloseForHandoffWaitsForUnknownReports(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mgr := newMockResourceManager()
		h := recoverSNQueryViewHandler(testPChannel, newMockCatalog(), mgr, nil)
		reportStarted := make(chan struct{})
		releaseReport := make(chan struct{})
		applyDone := make(chan struct{})
		rc := &reportCollector{}
		go func() {
			defer close(applyDone)
			h.ApplyViews([]handler.ApplyView{{
				View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp),
				OnReport: func(report qviews.QueryViewAtWorkNode) {
					if assert.True(t, h.mu.TryLock(), "OnReport must run outside the handler lock") {
						h.mu.Unlock()
					}
					close(reportStarted)
					<-releaseReport
					rc.onReport(report)
				},
			}})
		}()
		<-reportStarted

		closeDone := make(chan struct{})
		go func() {
			h.CloseForHandoff()
			close(closeDone)
		}()
		synctest.Wait()
		h.mu.Lock()
		closed := h.closed
		h.mu.Unlock()
		assert.True(t, closed, "close must reject new batches while draining accepted reports")
		select {
		case <-closeDone:
			assert.Fail(t, "CloseForHandoff returned while an accepted report was still blocked")
		default:
		}

		close(releaseReport)
		synctest.Wait()
		select {
		case <-closeDone:
		default:
			assert.Fail(t, "CloseForHandoff did not finish after the report completed")
		}
		<-applyDone
		require.Equal(t, 1, rc.count())
		h.ApplyViews([]handler.ApplyView{{
			View: newSNViewWithState(2, viewpb.QueryViewState_QueryViewStateUp), OnReport: rc.onReport,
		}})
		assert.Equal(t, 1, rc.count(), "closed handlers must not report later unknown views")
		assert.Zero(t, mgr.acquiredCount())
		assert.Empty(t, h.shards)
		assert.Empty(t, h.shardsByVChannel)
	})
}

func TestSNHandler_RestartAfterReadyLoadsReplacementReplica(t *testing.T) {
	cat := newMockCatalog()
	oldMgr := newMockResourceManager()
	oldHandler := recoverSNQueryViewHandler(testPChannel, cat, oldMgr, nil)
	oldView := newFullSNViewWithState(100, viewpb.QueryViewState_QueryViewStatePreparing, 101)
	oldReports := &reportCollector{}
	oldHandler.ApplyViews([]handler.ApplyView{{View: oldView, OnReport: oldReports.onReport}})
	oldAcquire, ok := oldMgr.getAcquired(oldView.QueryViewKey())
	require.True(t, ok)
	oldAcquire.OnReady()
	require.Equal(t, qviews.QueryViewStateReady, oldReports.last().State())
	require.Zero(t, cat.savedCount(), "a Ready view has no persisted Up recovery record")

	// Simulate losing the process after Ready, before receiving/persisting Up.
	// Only persisted views are available to the replacement handler.
	persisted, err := cat.ListQueryViews(context.Background(), testPChannel)
	require.NoError(t, err)
	mgr := newMockResourceManager()
	h := recoverSNQueryViewHandler(testPChannel, cat, mgr, persisted)
	h.ApplyViews([]handler.ApplyView{{
		View: withSNViewState(oldView, qviews.QueryViewStateUp), OnReport: oldReports.onReport,
	}})
	require.Equal(t, qviews.QueryViewStateUnrecoverable, oldReports.last().State())
	assert.Empty(t, h.shards)
	assert.Empty(t, h.shardsByVChannel)
	h.ApplyViews([]handler.ApplyView{{
		View: withSNViewState(oldView, qviews.QueryViewStateDropped), OnReport: oldReports.onReport,
	}})
	require.Equal(t, qviews.QueryViewStateDropped, oldReports.last().State())
	assert.Empty(t, h.shards)
	assert.Empty(t, h.shardsByVChannel)
	assert.Zero(t, mgr.acquiredCount())
	assert.Zero(t, mgr.releasedCount())

	newView := withSNViewReplica(newFullSNViewWithState(1, viewpb.QueryViewState_QueryViewStatePreparing, 102), testReplicaID+1)
	newReports := &reportCollector{}
	h.ApplyViews([]handler.ApplyView{{View: newView, OnReport: newReports.onReport}})
	newAcquire, ok := mgr.getAcquired(newView.QueryViewKey())
	require.True(t, ok)
	newAcquire.OnReady()
	h.ApplyViews([]handler.ApplyView{{
		View: withSNViewState(newView, qviews.QueryViewStateUp), OnReport: newReports.onReport,
	}})
	require.Equal(t, qviews.QueryViewStateUp, newReports.last().State())
	require.Equal(t, 1, cat.savedCount())

	// Phase 1 carries the vchannel with no replica choice. The replacement
	// replica must serve it even though its view version starts over at 1.
	lease, err := h.AcquireLatestUpView(context.Background(), qviews.ShardID{
		ReplicaID: qviews.UnknownReplicaID, VChannel: testVChannel,
	})
	require.NoError(t, err)
	defer lease.Release()
	assert.Equal(t, testReplicaID+1, lease.Meta.GetReplicaId())
	assert.Equal(t, testCollectionID, lease.Meta.GetCollectionId())
	assert.Equal(t, newView.QueryViewKey().QueryViewVersion, lease.Version)
	require.Len(t, lease.View.GetQueryNode(), 1)
	assert.Equal(t, int64(102), lease.View.GetQueryNode()[0].GetNodeId())

	// An explicitly requested lost replica must not use another replica.
	_, err = h.AcquireLatestUpView(context.Background(), oldView.ShardID())
	require.Error(t, err)
	assert.True(t, viewerror.AsViewError(err).IsViewNotFound())

	// Falling back by vchannel must still pin the chosen view's resources.
	h.ApplyViews([]handler.ApplyView{{
		View: withSNViewState(newView, qviews.QueryViewStateDropped), OnReport: newReports.onReport,
	}})
	assert.Zero(t, mgr.releasedCount())
	lease.Release()
	lease.Release()
	require.Equal(t, 1, mgr.releasedCount())
	mgr.invokeReleaseCallback(newView.QueryViewKey())
	assert.Equal(t, qviews.QueryViewStateDropped, newReports.last().State())
	assert.Empty(t, h.shards)
	assert.Empty(t, h.shardsByVChannel)
}
