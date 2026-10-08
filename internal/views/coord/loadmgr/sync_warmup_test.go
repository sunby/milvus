package loadmgr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSyncWarmupPersistenceAndClone(t *testing.T) {
	cfg := sampleConfig()
	cfg.SyncWarmup, cfg.SyncWarmupEpoch = true, 42
	clone := cfg.Clone()
	require.True(t, clone.SyncWarmup)
	require.EqualValues(t, 42, clone.SyncWarmupEpoch)
	clone.SyncWarmupEpoch = 43
	require.EqualValues(t, 42, cfg.SyncWarmupEpoch)
	recovered := buildFromPersisted(cfg.toCollectionLoadInfoProto(), cfg.toPartitionLoadInfoProtos(), nil)
	require.True(t, recovered.SyncWarmup)
	require.EqualValues(t, 42, recovered.SyncWarmupEpoch)
	fromMessage := FromAlterLoadConfigMessage(&messagespb.AlterLoadConfigMessageHeader{
		CollectionId: 100, SyncWarmup: true, SyncWarmupEpoch: 42,
		ForceSyncWarmup: true,
	})
	require.True(t, fromMessage.SyncWarmup)
	require.EqualValues(t, 42, fromMessage.SyncWarmupEpoch)
	// Temporary namespace prewarm never creates a persistent load requirement.
	temporary := FromAlterLoadConfigMessage(&messagespb.AlterLoadConfigMessageHeader{ForceSyncWarmup: true})
	require.False(t, temporary.SyncWarmup)
	require.Zero(t, temporary.SyncWarmupEpoch)
}

func TestSyncWarmupStoreRejectsInPlaceEnablement(t *testing.T) {
	store, catalog := newTestStore(t)
	cfg := sampleConfig()
	expectFullSave(catalog, 1)
	require.NoError(t, store.Put(context.Background(), cfg))
	cfg.SyncWarmup, cfg.SyncWarmupEpoch = true, 42
	require.ErrorIs(t, store.Put(context.Background(), cfg), merr.ErrServiceInternal)
	require.False(t, store.Snapshot().ConfigsMap()[cfg.CollectionID].SyncWarmup)
}

func TestSyncWarmupRecoveryRejectsIncompleteEpoch(t *testing.T) {
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return([]*querypb.CollectionLoadInfo{{CollectionID: 100, SyncWarmup: true}}, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return(nil, nil).Once()
	store, err := RecoverLoadConfigStore(context.Background(), catalog)
	require.Nil(t, store)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestSyncWarmupStickyUntilRelease(t *testing.T) {
	ctx := context.Background()
	store, catalog := newTestStore(t)
	cfg := sampleConfig()
	cfg.SyncWarmup, cfg.SyncWarmupEpoch = true, 42
	expectFullSave(catalog, 3)
	require.NoError(t, store.Put(ctx, cfg))
	ordinaryDDL := sampleConfig()
	require.NoError(t, store.Put(ctx, ordinaryDDL))
	got := store.Snapshot().ConfigsMap()[cfg.CollectionID]
	require.True(t, got.SyncWarmup)
	require.EqualValues(t, 42, got.SyncWarmupEpoch)
	require.False(t, ordinaryDDL.SyncWarmup, "Put must not mutate the caller's object")
	catalog.EXPECT().ReleaseReplicas(mock.Anything, cfg.CollectionID).Return(nil).Once()
	catalog.EXPECT().ReleaseCollection(mock.Anything, cfg.CollectionID).Return(nil).Once()
	require.NoError(t, store.Remove(ctx, cfg.CollectionID))
	cfg.SyncWarmupEpoch = 43
	require.NoError(t, store.Put(ctx, cfg))
	require.EqualValues(t, 43, store.Snapshot().ConfigsMap()[cfg.CollectionID].SyncWarmupEpoch)
}

func TestSyncWarmupRejectInvalidEpoch(t *testing.T) {
	for _, tc := range []struct {
		enabled bool
		epoch   int64
	}{{true, 0}, {false, 4}, {true, -1}} {
		store, _ := newTestStore(t)
		cfg := sampleConfig()
		cfg.SyncWarmup, cfg.SyncWarmupEpoch = tc.enabled, tc.epoch
		err := store.Put(context.Background(), cfg)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.Empty(t, store.Snapshot().ConfigsMap())
	}
}

func TestLoadConfigVersionFenceRejectsStalePlan(t *testing.T) {
	ctx := context.Background()
	store, catalog := newTestStore(t)
	cfg := sampleConfig()
	expectFullSave(catalog, 2)
	require.NoError(t, store.Put(ctx, cfg))
	version := store.Snapshot().ConfigVersion(cfg.CollectionID)
	require.NoError(t, store.Put(ctx, cfg))
	called := false
	applied, err := store.WithConfigVersion(cfg.CollectionID, version, func(*LoadConfig) error { called = true; return nil })
	require.NoError(t, err)
	require.False(t, applied)
	require.False(t, called)
	applied, err = store.WithConfigVersion(cfg.CollectionID, store.Snapshot().ConfigVersion(cfg.CollectionID), func(current *LoadConfig) error {
		require.Equal(t, cfg.CollectionID, current.CollectionID)
		return nil
	})
	require.NoError(t, err)
	require.True(t, applied)
}
