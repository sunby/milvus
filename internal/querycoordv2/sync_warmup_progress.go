package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

type syncWarmupTarget struct {
	epoch     int64
	vchannels []string
}

// qviewsLoadPercentageWithWarmup counts missing shards as not loaded. Ordinary
// loads retain the existing progress contract; sync loads must not complete
// merely because the first shard happens to reach Up before others are created.
func (s *Server) qviewsLoadPercentageWithWarmup(ctx context.Context, cfg *loadmgr.LoadConfig) (int64, error) {
	if !cfg.SyncWarmup {
		return s.qviewsLoadPercentage(cfg), nil
	}
	target, err := s.syncWarmupTarget(ctx, cfg)
	if err != nil {
		return 0, err
	}
	stats := s.qviewsRuntime.shardViewRegistry.Snapshot().StatsMap()
	return syncWarmupLoadPercentage(cfg, target.vchannels, stats), nil
}

func syncWarmupLoadPercentage(cfg *loadmgr.LoadConfig, vchannels []string, stats map[qviews.ShardID]*coordview.ShardStats) int64 {
	total := len(vchannels) * len(cfg.Replicas)
	if total == 0 || cfg.SyncWarmupEpoch <= 0 {
		return 0
	}
	loaded := 0
	for _, vchannel := range vchannels {
		for _, replica := range cfg.Replicas {
			shard := stats[qviews.ShardID{ReplicaID: replica.ReplicaID, VChannel: vchannel}]
			if shard != nil && shard.UpVersion != nil && shard.UpSyncWarmup && shard.UpSyncWarmupEpoch == cfg.SyncWarmupEpoch {
				loaded++
			}
		}
	}
	return int64(loaded) * 100 / int64(total)
}

func (s *Server) syncWarmupTarget(ctx context.Context, cfg *loadmgr.LoadConfig) (syncWarmupTarget, error) {
	runtime := s.qviewsRuntime
	if cached, ok := runtime.syncWarmupTargets.Load(cfg.CollectionID); ok {
		if target := cached.(syncWarmupTarget); target.epoch == cfg.SyncWarmupEpoch {
			return target, nil
		}
	}
	// Channel topology comes from collection metadata, never partial registry
	// discovery. Recovery reconstructs this cache before reporting completion.
	snapshot := runtime.loadConfigStore.Snapshot()
	collection, err := s.broker.DescribeCollection(ctx, cfg.CollectionID)
	if err != nil {
		return syncWarmupTarget{}, err
	}
	target := syncWarmupTarget{epoch: cfg.SyncWarmupEpoch, vchannels: append([]string(nil), collection.GetVirtualChannelNames()...)}
	accepted := false
	checked, err := runtime.loadConfigStore.WithConfigVersion(cfg.CollectionID, snapshot.ConfigVersion(cfg.CollectionID), func(current *loadmgr.LoadConfig) error {
		if current != nil && current.SyncWarmup && current.SyncWarmupEpoch == target.epoch {
			runtime.syncWarmupTargets.Store(cfg.CollectionID, target)
			accepted = true
		}
		return nil
	})
	if err != nil || !checked || !accepted {
		return syncWarmupTarget{}, err
	}
	return target, nil
}
