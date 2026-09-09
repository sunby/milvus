// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querycoordv2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	metastoremocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type loadProgressSyncer struct{}

func (*loadProgressSyncer) SyncViews(context.Context, syncer.SyncGroup) error { return nil }
func (*loadProgressSyncer) Close() error                                      { return nil }

func TestScopedLoadReadsPreserveProgressAndResponseVersion(t *testing.T) {
	ctx := context.Background()
	previousCache := meta.GlobalFailedLoadCache
	meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()
	t.Cleanup(func() { meta.GlobalFailedLoadCache = previousCache })
	catalog := metastoremocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return([]*querypb.CollectionLoadInfo{
		{CollectionID: 1, LoadFields: []int64{100}, FieldIndexID: map[int64]int64{100: 200}},
		{CollectionID: 2},
	}, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).Return(map[int64][]*querypb.PartitionLoadInfo{
		1: {{CollectionID: 1, PartitionID: 101}},
	}, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return([]*querypb.Replica{
		{ID: 10, CollectionID: 1}, {ID: 20, CollectionID: 2},
	}, nil).Once()
	store, err := loadmgr.RecoverLoadConfigStore(ctx, catalog)
	require.NoError(t, err)
	first := qviews.ShardID{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_1v0"}
	missing := qviews.ShardID{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_1v1"}
	oldReplica := qviews.ShardID{ReplicaID: 11, VChannel: first.VChannel}
	other := qviews.ShardID{ReplicaID: 20, VChannel: "by-dev-rootcoord-dml_2v0"}
	up := testPersistedQueryView(1, first)
	registry, err := coordview.RecoverShardViewRegistry(ctx, &fakeQueryViewCatalog{
		views: []*viewpb.QueryViewOfShard{up, testPersistedQueryView(2, other)},
	}, &loadProgressSyncer{})
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	registry.Ensure(oldReplica) // A residual replica must not lower the current replica's progress.
	server := &Server{ctx: ctx, qviewsRuntime: &qviewsRuntime{loadConfigStore: store, shardViewRegistry: registry}}
	server.UpdateStateCode(commonpb.StateCode_Healthy)
	cfg := store.Get(1).Config
	require.EqualValues(t, 100, server.qviewsLoadPercentage(cfg))
	require.Zero(t, server.qviewsLoadPercentage(&loadmgr.LoadConfig{CollectionID: 3}))

	registry.Ensure(missing)
	assert.EqualValues(t, 50, server.qviewsLoadPercentage(cfg))

	// Advance only the other collection: the RPC's global version must remain
	// distinct from the selected collection's load-info version.
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Once()
	require.NoError(t, store.Put(ctx, store.Get(2).Config))
	entry := store.Get(1)
	require.NotEqual(t, entry.ConfigVersion, entry.StoreVersion)
	info, err := server.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1})
	require.NoError(t, merr.CheckRPCCall(info, err))
	assert.Equal(t, entry.StoreVersion, info.GetVersion())
	assert.Equal(t, []int64{101}, info.GetPartitionIDs())
	info.PartitionIDs[0] = -1
	info.LoadFields[0].IndexId = -1
	assert.Equal(t, []int64{101}, store.Get(1).Config.PartitionIDs)
	assert.EqualValues(t, 200, store.Get(1).Config.LoadFields[0].IndexId)

	selected, err := server.ShowLoadCollections(ctx, &querypb.ShowCollectionsRequest{CollectionIDs: []int64{1, 1}})
	require.NoError(t, merr.CheckRPCCall(selected, err))
	assert.Equal(t, []int64{1}, selected.CollectionIDs)
	assert.Equal(t, []int64{50}, selected.InMemoryPercentages)
	all, err := server.ShowLoadCollections(ctx, &querypb.ShowCollectionsRequest{})
	require.NoError(t, merr.CheckRPCCall(all, err))
	assert.ElementsMatch(t, []int64{1, 2}, all.CollectionIDs)
	partitions, err := server.ShowLoadPartitions(ctx, &querypb.ShowPartitionsRequest{CollectionID: 1})
	require.NoError(t, merr.CheckRPCCall(partitions, err))
	assert.Equal(t, []int64{101}, partitions.PartitionIDs)
	assert.Equal(t, []int64{50}, partitions.InMemoryPercentages)
	absent, err := server.ShowLoadCollections(ctx, &querypb.ShowCollectionsRequest{CollectionIDs: []int64{3}})
	require.NoError(t, err)
	assert.ErrorIs(t, merr.Error(absent.GetStatus()), merr.ErrCollectionNotLoaded)
}
