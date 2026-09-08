package balancer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestSyncWarmupPlacementRequiresCapabilityAndEpoch(t *testing.T) {
	cfg := cfgFor(1, 10, nil, nil)
	cfg.SyncWarmup, cfg.SyncWarmupEpoch = true, 42
	shardID := qviews.ShardID{ReplicaID: 10, VChannel: "v0"}
	snap := baseSnap(cfg, shardID)
	stats := testShardStats(ver(1, 1, 1), 1, placement(100, 20, 1, coordview.SegmentStateUp))
	snap.ShardViewSnapshot = coordview.NewShardViewSnapshot(1, map[qviews.ShardID]*coordview.ShardStats{shardID: stats})
	snap.Nodes[1] = &BalanceNode{NodeID: 1, Alive: true, SyncLoadWarmup: true}
	require.Equal(t, actionMust, classifyShard(snap, shardID), "ordinary Up is not a sync completion")
	stats.UpSyncWarmup, stats.UpSyncWarmupEpoch = true, 41
	require.Equal(t, actionMust, classifyShard(snap, shardID), "old epoch must not complete the new lifecycle")
	stats.UpSyncWarmupEpoch = 42
	require.Equal(t, actionMayOptimize, classifyShard(snap, shardID))
	snap.Nodes[1].SyncLoadWarmup = false
	require.Equal(t, actionMust, classifyShard(snap, shardID))
}

func TestSyncWarmupAllocationRequiresCapableNodesAndCarriesEpoch(t *testing.T) {
	cfg := cfgFor(1, 10, []int64{1}, nil)
	cfg.SyncWarmup, cfg.SyncWarmupEpoch = true, 42
	shardID := qviews.ShardID{ReplicaID: 10, VChannel: "v0"}
	snap := baseSnap(cfg, shardID)
	snap.Config = policyTestConfig()
	setTestDataSnapshot(snap, 1, qviews.DataVersion{StreamingVersion: 1}, newMapSegmentSnapshot(map[int64]*SegmentInfo{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 100},
	}), shardDataView("v0", 1, 101))
	snap.Nodes[1] = &BalanceNode{NodeID: 1, Alive: true, ResourceGroup: "rg1"}
	require.Nil(t, allocate(snap, shardID, nil))
	snap.Nodes[2] = &BalanceNode{NodeID: 2, Alive: true, ResourceGroup: "rg1", SyncLoadWarmup: true}
	result := allocate(snap, shardID, nil)
	require.NotNil(t, result)
	require.Equal(t, map[int64]int64{101: 2}, result.assignments)
	meta := result.builder.Build().GetMeta()
	require.True(t, meta.GetSyncWarmup())
	require.EqualValues(t, 42, meta.GetSyncWarmupEpoch())
	require.Equal(t, snap.LoadConfigSnapshot.ConfigVersion(1), meta.GetLoadInfoVersion())
	require.Len(t, snap.Nodes, 2, "allocation must not mutate the shared node snapshot")
}

func TestApplyLoadConfigFenceSkipsStalePrepareAndRelease(t *testing.T) {
	store := storeWithConfig(t, 1, 10, nil, nil)
	b := &DefaultBalancer{configStore: store, queue: newTriggerQueue()}
	shardID := qviews.ShardID{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	called := false
	apply := func() error { called = true; return nil }
	// An old prepare cannot run after the config revision changes.
	stale := loadmgr.NewLoadConfigSnapshotWithVersions(99, store.Snapshot().ConfigsMap(), map[int64]uint64{1: 99})
	require.NoError(t, b.applyCurrentConfig(&BalancePlan{loadConfigSnapshot: stale}, shardID, apply))
	require.False(t, called)
	// A release planned while the collection was absent cannot release a reload.
	absent := loadmgr.NewLoadConfigSnapshot(1, nil)
	require.NoError(t, b.applyCurrentConfig(&BalancePlan{loadConfigSnapshot: absent}, shardID, apply))
	require.False(t, called)
	require.NoError(t, b.applyCurrentConfig(&BalancePlan{loadConfigSnapshot: store.Snapshot()}, shardID, apply))
	require.True(t, called)
	require.Contains(t, b.queue.takePending().dirtyColls, int64(1))
}
