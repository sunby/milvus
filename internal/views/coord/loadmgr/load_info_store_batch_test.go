package loadmgr

import (
	"context"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
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

// normalizeTestConfigs mixes persisted required fields, which need no schema,
// with full-load and hinted configs that resolve one.
func normalizeTestConfigs(n int) []*LoadConfig {
	cfgs := make([]*LoadConfig, 0, n)
	for i := 0; i < n; i++ {
		id := int64((i*7919)%n + 1)
		cfg := &LoadConfig{CollectionID: id, PartitionIDs: []int64{id * 10}, Replicas: []*ReplicaAssignment{{ReplicaID: id * 100, ResourceGroup: "rg1"}}}
		switch i % 4 {
		case 0:
			cfg.RequiredFields = []*messagespb.LoadFieldConfig{{FieldId: 100}, {FieldId: 101}}
		case 1:
			cfg.LoadAllFields = proto.Bool(true)
			cfg.LoadFields = []*messagespb.LoadFieldConfig{{FieldId: 101}}
		case 2:
			cfg.LoadFields = []*messagespb.LoadFieldConfig{{FieldId: 102, IndexId: 7}}
		}
		cfgs = append(cfgs, cfg)
	}
	return cfgs
}

// normalizeTestSchema derives a schema from the collection ID, records the
// peak number of concurrent calls, and fails the collections in fail.
func normalizeTestSchema(fail map[int64]time.Duration, peak *atomic.Int32) func(context.Context, int64) (*schemapb.CollectionSchema, error) {
	var inflight atomic.Int32
	return func(_ context.Context, id int64) (*schemapb.CollectionSchema, error) {
		n := inflight.Add(1)
		defer inflight.Add(-1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		if delay, ok := fail[id]; ok {
			time.Sleep(delay)
			return nil, merr.WrapErrCollectionNotFound(id)
		}
		time.Sleep(time.Duration(id%3) * 20 * time.Microsecond)
		fields := []*schemapb.FieldSchema{{FieldID: 0}, {FieldID: 1}, {FieldID: 100}}
		for f := int64(0); f < id%3; f++ {
			fields = append(fields, &schemapb.FieldSchema{FieldID: 101 + f})
		}
		return &schemapb.CollectionSchema{Fields: fields}, nil
	}
}

func requireSameConfig(t *testing.T, want, got *LoadConfig) {
	t.Helper()
	require.True(t, proto.Equal(want.toCollectionLoadInfoProto(), got.toCollectionLoadInfoProto()), "collection %d", want.CollectionID)
	require.Equal(t, want.PartitionIDs, got.PartitionIDs)
	require.Equal(t, want.Replicas, got.Replicas)
	require.Equal(t, loadInfoVersion(want), loadInfoVersion(got))
}

func TestLoadInfoStoreNormalizeAllMatchesSerialNormalize(t *testing.T) {
	ctx := context.Background()
	var peak atomic.Int32
	store := NewLoadInfoStore(newMemLoadInfoCatalog(), noIndexes, normalizeTestSchema(nil, &peak))
	cfgs := normalizeTestConfigs(2000)
	inputs := make([]*LoadConfig, len(cfgs))
	serial := make([]*LoadConfig, len(cfgs))
	for i, cfg := range cfgs {
		inputs[i] = cfg.Clone()
		var err error
		serial[i], err = store.normalize(ctx, cfg)
		require.NoError(t, err)
	}

	peak.Store(0)
	parallel := slices.Clone(cfgs)
	require.NoError(t, store.normalizeAll(ctx, parallel))
	for i := range cfgs {
		requireSameConfig(t, serial[i], parallel[i])
		requireSameConfig(t, inputs[i], cfgs[i]) // Inputs are cloned, never mutated.
	}
	require.LessOrEqual(t, int(peak.Load()), hardware.GetCPUNum())
	if hardware.GetCPUNum() > 1 {
		require.Greater(t, peak.Load(), int32(1), "schemas are resolved concurrently")
	}
	require.NoError(t, store.normalizeAll(ctx, nil))
}

func TestRecoverLoadConfigStoreReturnsFirstNormalizeErrorInOrder(t *testing.T) {
	cfgs := normalizeTestConfigs(2000)
	collections := make([]*querypb.CollectionLoadInfo, 0, len(cfgs))
	for _, cfg := range cfgs {
		collections = append(collections, cfg.toCollectionLoadInfoProto())
	}
	patches := []*mockey.Mocker{
		mockey.Mock((*querycoord.Catalog).GetCollections).To(func(*querycoord.Catalog, context.Context) ([]*querypb.CollectionLoadInfo, error) {
			return collections, nil
		}).Build(),
		mockey.Mock((*querycoord.Catalog).GetPartitions).To(func(*querycoord.Catalog, context.Context, []int64) (map[int64][]*querypb.PartitionLoadInfo, error) {
			return nil, nil
		}).Build(),
		mockey.Mock((*querycoord.Catalog).GetReplicas).To(func(*querycoord.Catalog, context.Context) ([]*querypb.Replica, error) { return nil, nil }).Build(),
	}
	defer func() {
		for _, patch := range patches {
			patch.UnPatch()
		}
	}()

	// The first failing collection in recovery order resolves slowest, so a
	// later failure is observed first.
	first, later := cfgs[405].CollectionID, cfgs[1203].CollectionID
	fail := map[int64]time.Duration{first: 5 * time.Millisecond, later: 0, cfgs[1801].CollectionID: 0}
	for round := 0; round < 5; round++ {
		var peak atomic.Int32
		loadInfos := newMemLoadInfoCatalog()
		_, err := RecoverLoadConfigStore(t.Context(), querycoord.NewCatalog(nil), NewLoadInfoStore(loadInfos, noIndexes, normalizeTestSchema(fail, &peak)))
		require.ErrorIs(t, err, merr.ErrCollectionNotFound)
		require.ErrorContains(t, err, strconv.FormatInt(first, 10))
		require.Zero(t, loadInfos.batchGets, "a failed normalization persists nothing")
	}
}
