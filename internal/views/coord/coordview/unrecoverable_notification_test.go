package coordview

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestRegistryNotifiesUnrecoverableAfterPersist(t *testing.T) {
	for _, tc := range []struct {
		name      string
		state     qviews.QueryViewState
		node      qviews.WorkNode
		lost      bool
		recovered bool
	}{
		{name: "SN preparing", state: qviews.QueryViewStatePreparing, node: testSN},
		{name: "SN ready", state: qviews.QueryViewStateReady, node: testSN},
		{name: "SN up", state: qviews.QueryViewStateUp, node: testSN},
		{name: "QN preparing", state: qviews.QueryViewStatePreparing, node: testQN1},
		{name: "QN lost", state: qviews.QueryViewStatePreparing, node: testQN1, lost: true},
		{name: "recovered SN preparing", state: qviews.QueryViewStatePreparing, node: testSN, recovered: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			catalog, stream := newMockCatalog(), newMockSyncer()
			if tc.recovered {
				view := buildTestViewWithVersion(1, 1, 1, 1)
				catalog.listed = []*viewpb.QueryViewOfShard{view}
			}
			registry := newTestRegistry(t, catalog, stream)
			stream.waitFlush = func() { require.NoError(t, registry.flushScheduler.Flush(ctx)) }
			type notification struct {
				shard  qviews.ShardID
				stats  *ShardStats
				states []viewpb.QueryViewState
			}
			notifications := make(chan notification, 8)
			registry.RegisterUnrecoverableNotifier(nil)
			registry.RegisterUnrecoverableNotifier(func(shard qviews.ShardID) {
				// Re-enter both objects to verify neither lock is held here.
				notifications <- notification{shard, registry.Get(shard).Stats(), catalog.savedStates()}
			})
			manager := registry.Ensure(testShardID)
			version := testVersion(1, 1, 1)
			if !tc.recovered {
				require.NoError(t, manager.AddPreparing(ctx, testBuilder(1, 1, 1)))
				stream.waitFlush()
				if tc.state != qviews.QueryViewStatePreparing {
					simulateNodeResponse(t, stream, testQN1, version, qviews.QueryViewStateReady, 1001)
					simulateNodeResponse(t, stream, testSN, version, qviews.QueryViewStateReady)
				}
				if tc.state == qviews.QueryViewStateUp {
					simulateNodeResponse(t, stream, testSN, version, qviews.QueryViewStateUp)
				}
			}
			require.Empty(t, notifications, "normal Preparing/Ready/Up must not notify")
			callback := stream.findOnSyncResponse(tc.node, version)
			require.NotNil(t, callback)
			view := buildTestViewWithVersion(1, 1, 1, 1)
			failure := snReport(view, qviews.QueryViewStateUnrecoverable)
			if node, ok := tc.node.(qviews.QueryNode); ok {
				failure = qnReport(view, node.ID, qviews.QueryViewStateUnrecoverable)
			}
			fail := func() { callback(failure) }
			if tc.lost {
				callback := stream.findOnQueryNodeLost(testQN1, version)
				require.NotNil(t, callback)
				fail = func() { callback(testQN1) }
			}
			fail()
			select {
			case event := <-notifications:
				require.Equal(t, testShardID, event.shard)
				require.Nil(t, event.stats.PreparingVersion)
				require.Nil(t, event.stats.UpVersion)
				require.NotEmpty(t, event.states)
				require.Equal(t, viewpb.QueryViewState_QueryViewStateUnrecoverable, event.states[len(event.states)-1])
			case <-ctx.Done():
				t.Fatal("node failure did not notify after persistence")
			}
			fail()
			require.Empty(t, notifications, "duplicate failure must not notify again")

			// Replacing and then preempting a Preparing view must not create a
			// scheduling feedback loop. Nor should explicit release or an old
			// failure callback arriving after replacement enqueue another job.
			require.NoError(t, manager.AddPreparing(ctx, testBuilder(1, 1, 1)))
			require.NoError(t, manager.AddPreparing(ctx, testBuilder(1, 1, 1)))
			require.NoError(t, manager.RequestRelease(ctx))
			fail()
			stream.waitFlush()
			require.Empty(t, notifications)
		})
	}
}

func TestRegistryDoesNotNotifyUnrecoverableDuringTeardown(t *testing.T) {
	ctx := context.Background()
	stream := newMockSyncer()
	registry := newTestRegistry(t, newMockCatalog(), stream)
	stream.waitFlush = func() { require.NoError(t, registry.flushScheduler.Flush(ctx)) }
	notifications := make(chan qviews.ShardID, 1)
	registry.RegisterUnrecoverableNotifier(func(shard qviews.ShardID) { notifications <- shard })
	manager := registry.Ensure(testShardID)
	require.NoError(t, manager.AddPreparing(ctx, testBuilder(1, 1, 0)))
	stream.waitFlush()
	version := testVersion(1, 1, 1)
	simulateNodeResponse(t, stream, testSN, version, qviews.QueryViewStateReady)
	simulateNodeResponse(t, stream, testSN, version, qviews.QueryViewStateUp)
	require.NoError(t, manager.RequestRelease(ctx))
	stream.waitFlush()
	simulateNodeResponse(t, stream, testSN, version, qviews.QueryViewStateUnrecoverable)
	require.Empty(t, notifications, "a Down view is already being torn down")
}

func TestRegistryUnrecoverableNotificationWaitsForFlush(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream := newMockSyncer()
	registry := newTestRegistry(t, newMockCatalog(), stream)
	notifications := make(chan qviews.ShardID, 1)
	registry.RegisterUnrecoverableNotifier(func(shard qviews.ShardID) { notifications <- shard })
	require.NoError(t, registry.Ensure(testShardID).AddPreparing(ctx, testBuilder(1, 1, 0)))
	require.NoError(t, registry.flushScheduler.Flush(ctx))

	// Hold the failure event before persistence. A synchronous stats observer
	// or notification directly from the report callback would fire too early.
	batch := registry.Begin()
	defer batch.Commit()
	simulateNodeResponse(t, stream, testSN, testVersion(1, 1, 1), qviews.QueryViewStateUnrecoverable)
	require.Empty(t, notifications)
	batch.Commit()
	require.NoError(t, registry.flushScheduler.Flush(ctx))
	select {
	case shard := <-notifications:
		require.Equal(t, testShardID, shard)
	default:
		t.Fatal("persisted failure did not notify")
	}
}
