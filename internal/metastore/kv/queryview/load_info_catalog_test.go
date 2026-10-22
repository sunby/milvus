package queryview

import (
	"context"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLoadInfoCatalogPersistsHistoricalDescriptors(t *testing.T) {
	_, storage := newTestCatalog(t)
	ctx := context.Background()
	t.Cleanup(func() { require.NoError(t, storage.RemoveWithPrefix(context.Background(), "coord/qv-load-info/")) })
	catalog := NewLoadInfoCatalog(storage)
	missing, err := catalog.GetLoadInfo(ctx, 1, 10)
	require.NoError(t, err)
	require.Nil(t, missing)
	info := &querypb.GetQueryViewLoadInfoResponse{CollectionID: 1, Version: 10, Status: merr.Success(), PartitionIDs: []int64{100}, LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 101, IndexId: 201}}, IndexInfoList: []*indexpb.IndexInfo{{FieldID: 101, IndexID: 201, IndexName: "retained"}}}
	require.NoError(t, catalog.SaveLoadInfo(ctx, info))
	recovered, err := NewLoadInfoCatalog(storage).GetLoadInfo(ctx, 1, 10)
	require.NoError(t, err)
	require.True(t, proto.Equal(info, recovered))
	bytes, err := proto.Marshal(info)
	require.NoError(t, err)
	require.NoError(t, storage.Save(ctx, loadInfoKey(1, 11), string(bytes)))
	_, err = catalog.GetLoadInfo(ctx, 1, 11)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.NoError(t, storage.Save(ctx, loadInfoKey(1, 12), "invalid protobuf"))
	_, err = catalog.GetLoadInfo(ctx, 1, 12)
	require.Error(t, err)
}

// countingTxnKV caps transaction operations and counts batch calls.
type countingTxnKV struct {
	kv.MetaKv
	maxOps     int
	multiLoads atomic.Int32
	multiSaves atomic.Int32
}

func (s *countingTxnKV) MaxTxnOps() int { return s.maxOps }

func (s *countingTxnKV) MultiLoad(ctx context.Context, keys []string) ([]string, error) {
	s.multiLoads.Add(1)
	return s.MetaKv.MultiLoad(ctx, keys)
}

func (s *countingTxnKV) MultiSave(ctx context.Context, kvs map[string]string) error {
	s.multiSaves.Add(1)
	return s.MetaKv.MultiSave(ctx, kvs)
}

func TestLoadInfoCatalogBatchesSnapshots(t *testing.T) {
	_, storage := newTestCatalog(t)
	ctx := context.Background()
	t.Cleanup(func() { require.NoError(t, storage.RemoveWithPrefix(context.Background(), "coord/qv-load-info/")) })
	counting := &countingTxnKV{MetaKv: storage, maxOps: 2}
	catalog := NewLoadInfoCatalog(counting)

	infos := make([]*querypb.GetQueryViewLoadInfoResponse, 0, 5)
	for id := int64(1); id <= 5; id++ {
		infos = append(infos, &querypb.GetQueryViewLoadInfoResponse{CollectionID: id, Version: uint64(10 + id), Status: merr.Success(), PartitionIDs: []int64{100 + id}})
	}
	require.NoError(t, catalog.SaveLoadInfos(ctx, infos))
	require.EqualValues(t, 3, counting.multiSaves.Load())

	ids := []LoadInfoID{{1, 11}, {9, 9}, {3, 13}, {5, 15}, {2, 99}, {4, 14}, {2, 12}}
	got, err := catalog.GetLoadInfos(ctx, ids)
	require.NoError(t, err)
	require.EqualValues(t, 4, counting.multiLoads.Load())
	require.Len(t, got, len(ids))
	want := map[int]*querypb.GetQueryViewLoadInfoResponse{0: infos[0], 2: infos[2], 3: infos[4], 5: infos[3], 6: infos[1]}
	for i, id := range ids {
		single, err := catalog.GetLoadInfo(ctx, id.CollectionID, id.Version)
		require.NoError(t, err)
		if expected, ok := want[i]; ok {
			require.True(t, proto.Equal(expected, got[i]))
			require.True(t, proto.Equal(single, got[i]))
		} else {
			require.Nil(t, got[i])
			require.Nil(t, single)
		}
	}

	bytes, err := proto.Marshal(infos[0])
	require.NoError(t, err)
	require.NoError(t, storage.Save(ctx, loadInfoKey(1, 99), string(bytes)))
	_, err = catalog.GetLoadInfos(ctx, []LoadInfoID{{3, 13}, {1, 99}})
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.NoError(t, storage.Save(ctx, loadInfoKey(1, 98), "invalid protobuf"))
	_, err = catalog.GetLoadInfos(ctx, []LoadInfoID{{1, 98}})
	require.Error(t, err)

	got, err = catalog.GetLoadInfos(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, got)
	require.NoError(t, catalog.SaveLoadInfos(ctx, nil))
}

func TestLoadInfoCatalogSplitsLargeBatchesBySize(t *testing.T) {
	_, storage := newTestCatalog(t)
	ctx := context.Background()
	t.Cleanup(func() { require.NoError(t, storage.RemoveWithPrefix(context.Background(), "coord/qv-load-info/")) })
	counting := &countingTxnKV{MetaKv: storage, maxOps: 64}
	catalog := NewLoadInfoCatalog(counting)

	// About 300 KiB each: two snapshots exceed one transaction's byte budget.
	partitionIDs := make([]int64, 35000)
	for i := range partitionIDs {
		partitionIDs[i] = 469710000000000000 + int64(i)
	}
	infos := make([]*querypb.GetQueryViewLoadInfoResponse, 0, 3)
	ids := make([]LoadInfoID, 0, 3)
	for id := int64(1); id <= 3; id++ {
		infos = append(infos, &querypb.GetQueryViewLoadInfoResponse{CollectionID: id, Version: 1, Status: merr.Success(), PartitionIDs: partitionIDs})
		ids = append(ids, LoadInfoID{id, 1})
	}
	require.Greater(t, 2*proto.Size(infos[0]), maxLoadInfoTxnBytes)
	require.NoError(t, catalog.SaveLoadInfos(ctx, infos))
	require.EqualValues(t, 3, counting.multiSaves.Load())
	got, err := catalog.GetLoadInfos(ctx, ids)
	require.NoError(t, err)
	for i := range infos {
		require.True(t, proto.Equal(infos[i], got[i]))
	}
}

// recordingTxnKV records each transaction and the peak number in flight.
type recordingTxnKV struct {
	kv.MetaKv
	maxOps   int
	delay    time.Duration
	fail     func(call int) error // nil, or the error of a call
	mu       sync.Mutex
	batches  []map[string]string
	calls    int
	inflight atomic.Int32
	peak     atomic.Int32
}

func (s *recordingTxnKV) MaxTxnOps() int { return s.maxOps }

func (s *recordingTxnKV) MultiSave(_ context.Context, kvs map[string]string) error {
	n := s.inflight.Add(1)
	defer s.inflight.Add(-1)
	for peak := s.peak.Load(); n > peak && !s.peak.CompareAndSwap(peak, n); peak = s.peak.Load() {
	}
	time.Sleep(s.delay)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.fail != nil {
		if err := s.fail(s.calls); err != nil {
			return err
		}
	}
	s.batches = append(s.batches, maps.Clone(kvs))
	return nil
}

// serialLoadInfoBatches are the transactions of the former batch-by-batch
// write, in order.
func serialLoadInfoBatches(t *testing.T, infos []*querypb.GetQueryViewLoadInfoResponse, maxOps int) []map[string]string {
	var batches []map[string]string
	kvs := make(map[string]string)
	size := 0
	for _, info := range infos {
		value, err := proto.Marshal(info)
		require.NoError(t, err)
		key := loadInfoKey(info.GetCollectionID(), info.GetVersion())
		if (len(kvs) >= maxOps || size+len(key)+len(value) > maxLoadInfoTxnBytes) && len(kvs) > 0 {
			batches = append(batches, kvs)
			kvs, size = make(map[string]string), 0
		}
		kvs[key] = string(value)
		size += len(key) + len(value)
	}
	if len(kvs) > 0 {
		batches = append(batches, kvs)
	}
	return batches
}

// testLoadInfos has every seventh snapshot large enough that the byte budget,
// not the op limit, closes some batches.
func testLoadInfos(n int) []*querypb.GetQueryViewLoadInfoResponse {
	infos := make([]*querypb.GetQueryViewLoadInfoResponse, 0, n)
	for i := 0; i < n; i++ {
		partitions := []int64{int64(i)}
		if i%7 == 0 {
			partitions = make([]int64, 20000)
			for p := range partitions {
				partitions[p] = 469710000000000000 + int64(p)
			}
		}
		infos = append(infos, &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success(), CollectionID: int64(i + 1), Version: uint64(1<<63) | uint64(i), PartitionIDs: partitions, RequiredFields: []*messagespb.LoadFieldConfig{{FieldId: 100}}})
	}
	return infos
}

func TestLoadInfoCatalogSavesSerialBatchesConcurrently(t *testing.T) {
	ctx := context.Background()
	infos := testLoadInfos(400)
	storage := &recordingTxnKV{maxOps: 16, delay: 20 * time.Millisecond}
	require.NoError(t, NewLoadInfoCatalog(storage).SaveLoadInfos(ctx, infos))

	want := serialLoadInfoBatches(t, infos, 16)
	require.Greater(t, len(want), 400/16, "the byte budget closes some batches")
	require.ElementsMatch(t, want, storage.batches)
	written := make(map[string]int)
	for _, batch := range storage.batches {
		require.LessOrEqual(t, len(batch), 16)
		size := 0
		for key, value := range batch {
			written[key]++
			size += len(key) + len(value)
		}
		require.True(t, size <= maxLoadInfoTxnBytes || len(batch) == 1)
	}
	require.Len(t, written, len(infos))
	for _, info := range infos {
		require.Equal(t, 1, written[loadInfoKey(info.GetCollectionID(), info.GetVersion())])
	}
	require.Greater(t, storage.peak.Load(), int32(1))
	require.LessOrEqual(t, storage.peak.Load(), int32(saveLoadInfoConcurrency))
}

func TestLoadInfoCatalogWritesRepeatedSnapshotsInOrder(t *testing.T) {
	ctx := context.Background()
	infos := testLoadInfos(100)
	repeated := proto.Clone(infos[3]).(*querypb.GetQueryViewLoadInfoResponse)
	repeated.PartitionIDs = []int64{7}
	infos = append(infos, repeated)
	storage := &recordingTxnKV{maxOps: 16, delay: time.Millisecond}
	require.NoError(t, NewLoadInfoCatalog(storage).SaveLoadInfos(ctx, infos))
	require.Equal(t, serialLoadInfoBatches(t, infos, 16), storage.batches)
	require.EqualValues(t, 1, storage.peak.Load())

	// No batch starts after one has failed.
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	failing := &recordingTxnKV{maxOps: 16, fail: func(call int) error {
		if call == 2 {
			cancel()
			return context.Canceled
		}
		return nil
	}}
	require.ErrorIs(t, NewLoadInfoCatalog(failing).SaveLoadInfos(cancelCtx, infos), context.Canceled)
	require.Equal(t, 2, failing.calls)
	require.Equal(t, serialLoadInfoBatches(t, infos, 16)[:1], failing.batches)
}

func TestLoadInfoCatalogEncodingFailureWritesEarlierBatches(t *testing.T) {
	ctx := context.Background()
	infos := testLoadInfos(100)
	const broken = 50
	infos[broken].Status = &commonpb.Status{Reason: "\xff"}
	_, err := proto.Marshal(infos[broken])
	require.Error(t, err)

	storage := &recordingTxnKV{maxOps: 16}
	err = NewLoadInfoCatalog(storage).SaveLoadInfos(ctx, infos)
	require.ErrorContains(t, err, "encode query view load snapshot")
	// A batch-by-batch write had written every batch closed before the
	// broken snapshot, but not the open one.
	closed := serialLoadInfoBatches(t, infos[:broken], 16)
	require.ElementsMatch(t, closed[:len(closed)-1], storage.batches)
}
