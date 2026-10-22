package queryview

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

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
