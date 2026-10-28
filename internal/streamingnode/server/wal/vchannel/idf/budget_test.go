package idf

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func newSearchedTestOracle(t *testing.T) *oracleRuntime {
	t.Helper()
	r, _ := newTestOracle(t)
	r.ApplyLiveEvent(context.Background(), walview.VChannelResourceEvent{Message: bm25Insert(t, 20, 4)})
	return r
}

func TestStatsBudgetDropsLeastRecentlySearchedStatistics(t *testing.T) {
	patch := mockey.Mock((*Provider).getSealedBM25Resources).Return(nil, nil).Build()
	t.Cleanup(func() { patch.UnPatch() })
	older, newer := newSearchedTestOracle(t), newSearchedTestOracle(t)
	query := &schemapb.SparseFloatArray{Contents: [][]byte{typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{7: 1})}}
	before, avgdl, err := older.BuildIDF(context.Background(), qviews.DataVersion{}, 102, query)
	require.NoError(t, err)
	older.lastSearch.Store(1)

	budget := newStatsBudget()
	budget.oracles.Insert(older)
	budget.oracles.Insert(newer)
	budget.enforce(newer.statsMemSize())
	require.Nil(t, older.currentStats, "the least recently searched oracle drops its statistics")
	require.NotNil(t, newer.currentStats)
	require.True(t, budget.isFull())

	// The next search rebuilds the statistics, growing rows included.
	after, rebuiltAvgdl, err := older.BuildIDF(context.Background(), qviews.DataVersion{}, 102, query)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, avgdl, rebuiltAvgdl)
	require.NotNil(t, older.currentStats)

	budget.enforce(0)
	require.NotNil(t, older.currentStats, "a zero limit disables the budget")
	require.False(t, budget.isFull())
}

func TestFullStatsBudgetBuildsLazyOracles(t *testing.T) {
	patch := mockey.Mock((*Provider).getSealedBM25Resources).Return(nil, nil).Build()
	t.Cleanup(func() { patch.UnPatch() })
	base, _ := newTestOracle(t)
	provider := NewProvider(&mocks.MockMixCoordClient{}, WithChunkManager(base.provider.chunkManager))
	provider.statsBudget = newStatsBudget()
	view := walview.VChannelWALView{
		CollectionID: 1, VChannel: "v1", Schema: base.schema, SegmentSnapshot: walview.VisibleSegmentSnapshot{DataVersion: qviews.DataVersion{StreamingVersion: 10}},
	}
	for _, full := range []bool{false, true} {
		provider.statsBudget.full.Store(full)
		r, err := provider.buildOracle(context.Background(), view, false)
		require.NoError(t, err)
		t.Cleanup(r.Close)
		require.Equal(t, full, r.currentStats == nil, "a full budget defers loading the statistics to the first search")
		require.True(t, provider.statsBudget.oracles.Contain(r))
	}
}

func TestDematerializeSkipsAdvancingOracle(t *testing.T) {
	r := newSearchedTestOracle(t)
	r.advanceMu.Lock()
	require.False(t, r.dematerialize())
	r.advanceMu.Unlock()
	require.True(t, r.dematerialize())
	require.False(t, r.dematerialize(), "nothing left to drop")
}

func TestSearchRebuildsStatisticsDroppedAfterMaterialization(t *testing.T) {
	patch := mockey.Mock((*Provider).getSealedBM25Resources).Return(nil, nil).Build()
	t.Cleanup(func() { patch.UnPatch() })
	r := newSearchedTestOracle(t)
	var origin func(*oracleRuntime, context.Context) error
	drops := 0
	ensure := mockey.Mock((*oracleRuntime).ensureMaterialized).Origin(&origin).To(func(r *oracleRuntime, ctx context.Context) error {
		err := origin(r, ctx)
		if drops == 0 {
			drops++
			require.True(t, r.dematerialize(), "the budget drops the statistics before the search reads them")
		}
		return err
	}).Build()
	t.Cleanup(func() { ensure.UnPatch() })
	query := &schemapb.SparseFloatArray{Contents: [][]byte{typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{7: 1})}}
	_, _, err := r.BuildIDF(context.Background(), qviews.DataVersion{}, 102, query)
	require.NoError(t, err)
}
