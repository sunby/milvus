package querycoordv2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	qnmanager "github.com/milvus-io/milvus/internal/querynodev2/client/manager"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestResolveLoadCollectionSyncWarmup(t *testing.T) {
	for _, requested := range []bool{false, true} {
		enabled, epoch, err := resolveLoadCollectionSyncWarmup(100, requested, nil)
		require.NoError(t, err)
		require.Equal(t, requested, enabled)
		require.Zero(t, epoch, "only the caller allocates the new durable epoch")
		loaded := &loadmgr.LoadConfig{CollectionID: 100, SyncWarmup: true, SyncWarmupEpoch: 42}
		enabled, epoch, err = resolveLoadCollectionSyncWarmup(100, requested, loaded)
		require.NoError(t, err)
		require.True(t, enabled)
		require.EqualValues(t, 42, epoch)
	}
	_, _, err := resolveLoadCollectionSyncWarmup(100, true, &loadmgr.LoadConfig{CollectionID: 100})
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}

type syncWarmupNodeClient struct {
	qnmanager.ManagerClient
	nodes map[int64]*qnmanager.NodeInfo
	err   error
}

func (c *syncWarmupNodeClient) GetAllQueryNodes(context.Context) (map[int64]*qnmanager.NodeInfo, error) {
	return c.nodes, c.err
}

func TestSyncWarmupAdmissionFailsClosed(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().QueryCoordCfg.EnableLoadCollectionSyncWarmup.Key
	t.Cleanup(func() { _ = paramtable.Get().Reset(key) })
	client := &syncWarmupNodeClient{nodes: map[int64]*qnmanager.NodeInfo{1: {SyncLoadWarmup: true}}}
	server := &Server{qviewsRuntime: &qviewsRuntime{queryNodeManager: client}}
	require.NoError(t, paramtable.Get().Save(key, "false"))
	require.ErrorIs(t, server.checkSyncLoadWarmupCapability(context.Background()), merr.ErrServiceUnimplemented)
	require.NoError(t, paramtable.Get().Save(key, "true"))
	require.NoError(t, server.checkSyncLoadWarmupCapability(context.Background()))
	client.nodes[2] = &qnmanager.NodeInfo{}
	require.ErrorIs(t, server.checkSyncLoadWarmupCapability(context.Background()), merr.ErrServiceUnimplemented)
	client.nodes[2] = nil
	require.ErrorIs(t, server.checkSyncLoadWarmupCapability(context.Background()), merr.ErrServiceUnimplemented)
	delete(client.nodes, 2)
	client.nodes[1].Stopping = true
	require.ErrorIs(t, server.checkSyncLoadWarmupCapability(context.Background()), merr.ErrServiceUnavailable)
	client.err = context.DeadlineExceeded
	require.ErrorIs(t, server.checkSyncLoadWarmupCapability(context.Background()), context.DeadlineExceeded)
}

func TestSyncWarmupProgressRequiresEveryTargetShardAndReplica(t *testing.T) {
	cfg := &loadmgr.LoadConfig{
		SyncWarmup: true, SyncWarmupEpoch: 42,
		Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 1}, {ReplicaID: 2}},
	}
	channels := []string{"v0", "v1"}
	stats := make(map[qviews.ShardID]*coordview.ShardStats)
	require.Zero(t, syncWarmupLoadPercentage(cfg, channels, stats))
	first := &coordview.ShardStats{UpVersion: &qviews.QueryViewVersion{}, UpSyncWarmup: true, UpSyncWarmupEpoch: 42}
	stats[qviews.ShardID{VChannel: "v0", ReplicaID: 1}] = first
	require.EqualValues(t, 25, syncWarmupLoadPercentage(cfg, channels, stats))
	first.UpSyncWarmupEpoch = 41
	require.Zero(t, syncWarmupLoadPercentage(cfg, channels, stats))
	first.UpSyncWarmupEpoch, first.UpSyncWarmup = 42, false
	require.Zero(t, syncWarmupLoadPercentage(cfg, channels, stats))
	first.UpSyncWarmup = true
	for _, channel := range channels {
		for _, replica := range cfg.Replicas {
			stats[qviews.ShardID{VChannel: channel, ReplicaID: replica.ReplicaID}] = first
		}
	}
	require.EqualValues(t, 100, syncWarmupLoadPercentage(cfg, channels, stats))
	require.Zero(t, syncWarmupLoadPercentage(cfg, nil, stats))
}
