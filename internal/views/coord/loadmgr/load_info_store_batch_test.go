package loadmgr

import (
	"context"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// memLoadInfoCatalog is an in-memory LoadInfoCatalog that counts calls.
type memLoadInfoCatalog struct {
	mu         sync.Mutex
	durable    map[loadInfoKey]*querypb.GetQueryViewLoadInfoResponse
	gets       int
	saves      int
	batchGets  int
	batchSaves int
	getsErr    error
	savesErr   error
}

func newMemLoadInfoCatalog() *memLoadInfoCatalog {
	return &memLoadInfoCatalog{durable: make(map[loadInfoKey]*querypb.GetQueryViewLoadInfoResponse)}
}

func (c *memLoadInfoCatalog) GetLoadInfo(_ context.Context, collectionID int64, version uint64) (*querypb.GetQueryViewLoadInfoResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gets++
	if info := c.durable[loadInfoKey{collectionID, version}]; info != nil {
		return proto.Clone(info).(*querypb.GetQueryViewLoadInfoResponse), nil
	}
	return nil, nil
}

func (c *memLoadInfoCatalog) SaveLoadInfo(_ context.Context, info *querypb.GetQueryViewLoadInfoResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.saves++
	c.durable[loadInfoKey{info.GetCollectionID(), info.GetVersion()}] = proto.Clone(info).(*querypb.GetQueryViewLoadInfoResponse)
	return nil
}

func (c *memLoadInfoCatalog) GetLoadInfos(_ context.Context, ids []queryview.LoadInfoID) ([]*querypb.GetQueryViewLoadInfoResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batchGets++
	if c.getsErr != nil {
		return nil, c.getsErr
	}
	infos := make([]*querypb.GetQueryViewLoadInfoResponse, len(ids))
	for i, id := range ids {
		if info := c.durable[loadInfoKey{id.CollectionID, id.Version}]; info != nil {
			infos[i] = proto.Clone(info).(*querypb.GetQueryViewLoadInfoResponse)
		}
	}
	return infos, nil
}

func (c *memLoadInfoCatalog) SaveLoadInfos(_ context.Context, infos []*querypb.GetQueryViewLoadInfoResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batchSaves++
	if c.savesErr != nil {
		return c.savesErr
	}
	for _, info := range infos {
		c.durable[loadInfoKey{info.GetCollectionID(), info.GetVersion()}] = proto.Clone(info).(*querypb.GetQueryViewLoadInfoResponse)
	}
	return nil
}

func noIndexes(int64) []*indexpb.IndexInfo { return nil }

func batchTestConfig(collectionID int64) *LoadConfig {
	return &LoadConfig{
		CollectionID:   collectionID,
		PartitionIDs:   []int64{collectionID * 10, collectionID*10 + 1},
		LoadFields:     []*messagespb.LoadFieldConfig{{FieldId: 101, IndexId: 201}},
		RequiredFields: []*messagespb.LoadFieldConfig{{FieldId: 100}, {FieldId: 101, IndexId: 201}},
	}
}

func TestLoadInfoStoreEnsureAllMatchesEnsure(t *testing.T) {
	ctx := context.Background()
	cfgs := []*LoadConfig{batchTestConfig(1), batchTestConfig(2), batchTestConfig(3)}
	key := func(cfg *LoadConfig) loadInfoKey { return loadInfoKey{cfg.CollectionID, loadInfoVersion(cfg)} }

	catalog := newMemLoadInfoCatalog()
	// A retained snapshot is reused as stored, never rewritten from the config.
	retained := newLoadInfoSnapshot(cfgs[0], key(cfgs[0]).version)
	retained.PartitionIDs = []int64{11, 10}
	catalog.durable[key(cfgs[0])] = proto.Clone(retained).(*querypb.GetQueryViewLoadInfoResponse)

	store := NewLoadInfoStore(catalog, noIndexes)
	require.NoError(t, store.EnsureAll(ctx, append(cfgs, cfgs[2].Clone())))
	require.Equal(t, 1, catalog.batchGets)
	require.Equal(t, 1, catalog.batchSaves)
	require.Zero(t, catalog.gets)
	require.Zero(t, catalog.saves)
	require.Len(t, catalog.durable, 3)
	require.True(t, proto.Equal(retained, catalog.durable[key(cfgs[0])]))

	// Missing snapshots are exactly what Ensure would persist.
	single := newMemLoadInfoCatalog()
	singleStore := NewLoadInfoStore(single, noIndexes)
	for _, cfg := range cfgs[1:] {
		require.NoError(t, singleStore.Ensure(ctx, cfg))
		require.True(t, proto.Equal(single.durable[key(cfg)], catalog.durable[key(cfg)]))
	}

	// Every snapshot is cached: later calls do no catalog I/O.
	for _, cfg := range cfgs {
		require.NoError(t, store.Ensure(ctx, cfg))
		info, err := store.Get(ctx, cfg.CollectionID, key(cfg).version)
		require.NoError(t, err)
		require.True(t, proto.Equal(catalog.durable[key(cfg)], info))
	}
	require.NoError(t, store.EnsureAll(ctx, cfgs))
	require.Zero(t, catalog.gets)
	require.Zero(t, catalog.saves)
	require.Equal(t, 1, catalog.batchGets)
	require.Equal(t, 1, catalog.batchSaves)

	// Nothing missing: one batch read and no write.
	restarted := NewLoadInfoStore(catalog, noIndexes)
	require.NoError(t, restarted.EnsureAll(ctx, cfgs))
	require.Equal(t, 2, catalog.batchGets)
	require.Equal(t, 1, catalog.batchSaves)
}

func TestLoadInfoStoreEnsureAllFailureCachesNothing(t *testing.T) {
	ctx := context.Background()
	cfg := batchTestConfig(1)
	injected := errors.New("injected")

	catalog := newMemLoadInfoCatalog()
	catalog.getsErr = injected
	store := NewLoadInfoStore(catalog, noIndexes)
	require.ErrorIs(t, store.EnsureAll(ctx, []*LoadConfig{cfg}), injected)
	require.Zero(t, catalog.batchSaves)

	catalog.getsErr = nil
	catalog.savesErr = injected
	require.ErrorIs(t, store.EnsureAll(ctx, []*LoadConfig{cfg}), injected)
	require.Empty(t, catalog.durable)

	// Neither failure cached the snapshot, so Ensure persists it.
	catalog.savesErr = nil
	require.NoError(t, store.Ensure(ctx, cfg))
	require.Equal(t, 1, catalog.gets)
	require.Equal(t, 1, catalog.saves)
}

func TestLoadInfoStoreEnsureAllRejectsMissingRequiredFields(t *testing.T) {
	ctx := context.Background()
	catalog := newMemLoadInfoCatalog()
	store := NewLoadInfoStore(catalog, noIndexes)
	cfg := batchTestConfig(1)
	cfg.RequiredFields = nil
	require.ErrorIs(t, store.EnsureAll(ctx, []*LoadConfig{batchTestConfig(2), cfg}), merr.ErrServiceInternal)
	require.Zero(t, catalog.batchGets)

	cfg = batchTestConfig(3)
	stored := newLoadInfoSnapshot(cfg, loadInfoVersion(cfg))
	stored.RequiredFields = nil
	catalog.durable[loadInfoKey{cfg.CollectionID, loadInfoVersion(cfg)}] = stored
	require.ErrorIs(t, store.EnsureAll(ctx, []*LoadConfig{cfg}), merr.ErrDataIntegrity)
	require.Zero(t, catalog.batchSaves)
}

func TestRecoverLoadConfigStoreBatchesLoadInfos(t *testing.T) {
	const numCollections = 1000
	collections := make([]*querypb.CollectionLoadInfo, 0, numCollections)
	partitions := make(map[int64][]*querypb.PartitionLoadInfo, numCollections)
	for id := int64(1); id <= numCollections; id++ {
		collections = append(collections, &querypb.CollectionLoadInfo{CollectionID: id, RequiredFields: []*messagespb.LoadFieldConfig{{FieldId: 100}}})
		partitions[id] = []*querypb.PartitionLoadInfo{{CollectionID: id, PartitionID: id * 10}}
	}
	patches := []*mockey.Mocker{
		mockey.Mock((*querycoord.Catalog).GetCollections).To(func(*querycoord.Catalog, context.Context) ([]*querypb.CollectionLoadInfo, error) {
			return collections, nil
		}).Build(),
		mockey.Mock((*querycoord.Catalog).GetPartitions).To(func(*querycoord.Catalog, context.Context, []int64) (map[int64][]*querypb.PartitionLoadInfo, error) {
			return partitions, nil
		}).Build(),
		mockey.Mock((*querycoord.Catalog).GetReplicas).To(func(*querycoord.Catalog, context.Context) ([]*querypb.Replica, error) { return nil, nil }).Build(),
	}
	defer func() {
		for _, patch := range patches {
			patch.UnPatch()
		}
	}()

	loadInfos := newMemLoadInfoCatalog()
	store, err := RecoverLoadConfigStore(t.Context(), querycoord.NewCatalog(nil), NewLoadInfoStore(loadInfos, noIndexes))
	require.NoError(t, err)
	require.Len(t, loadInfos.durable, numCollections)
	require.Equal(t, 1, loadInfos.batchGets)
	require.Equal(t, 1, loadInfos.batchSaves)
	require.Zero(t, loadInfos.gets)
	require.Zero(t, loadInfos.saves)
	for id := int64(1); id <= numCollections; id++ {
		version := store.Snapshot().ConfigVersion(id)
		require.NotNil(t, loadInfos.durable[loadInfoKey{id, version}])
	}

	_, err = RecoverLoadConfigStore(t.Context(), querycoord.NewCatalog(nil), NewLoadInfoStore(loadInfos, noIndexes))
	require.NoError(t, err)
	require.Equal(t, 2, loadInfos.batchGets)
	require.Equal(t, 1, loadInfos.batchSaves, "a restart with retained snapshots writes nothing")
}
