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

package balancer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestSnapshotBuilder_LoadConfigsFollowResolvedScope(t *testing.T) {
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return([]*querypb.CollectionLoadInfo{
		{CollectionID: 1}, {CollectionID: 2},
	}, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return([]*querypb.Replica{
		{ID: 10, CollectionID: 1}, {ID: 20, CollectionID: 2},
	}, nil).Once()
	store, err := loadmgr.RecoverLoadConfigStore(context.Background(), catalog)
	require.NoError(t, err)
	registry := triggerTestRegistry(t)
	first, second, residual := triggerShard(10, 1, 0), triggerShard(20, 2, 0), triggerShard(30, 3, 0)
	addShardWithPreparingView(t, registry, first, map[int64]map[int64][]int64{100: {1: {101}}})
	addShardWithPreparingView(t, registry, second, map[int64]map[int64][]int64{200: {2: {201}}})
	registry.Ensure(residual)
	malformed := qviews.ShardID{ReplicaID: 40, VChannel: "malformed"}
	addShardWithPreparingView(t, registry, malformed, map[int64]map[int64][]int64{300: {4: {401}}})
	allShards := []qviews.ShardID{first, second, residual, malformed}

	for _, test := range []struct {
		name    string
		batch   triggerBatch
		configs []int64
		shards  []qviews.ShardID
	}{
		{"collection", triggerBatch{dirtyColls: setOf[int64](1)}, []int64{1}, []qviews.ShardID{first}},
		{"shard", triggerBatch{dirtyShards: setOf(second)}, []int64{2}, []qviews.ShardID{second}},
		{"node", triggerBatch{dirtyNodes: setOf[int64](100)}, []int64{1}, []qviews.ShardID{first}},
		{"released", triggerBatch{dirtyColls: setOf[int64](3)}, nil, []qviews.ShardID{residual}},
		{"empty", triggerBatch{}, nil, nil},
		{"full", triggerBatch{full: true}, []int64{1, 2}, allShards},
		{"malformed shard", triggerBatch{dirtyShards: setOf(malformed)}, []int64{1, 2}, allShards},
		{"malformed node shard", triggerBatch{dirtyNodes: setOf[int64](300)}, []int64{1, 2}, allShards},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := &fakeDataViewProvider{collections: []*viewpb.DataViewOfCollection{
				{CollectionId: 1, DataVersion: &viewpb.DataVersion{}, Shards: []*viewpb.DataViewOfShard{{Vchannel: first.VChannel}}},
				{CollectionId: 2, DataVersion: &viewpb.DataVersion{}, Shards: []*viewpb.DataViewOfShard{{Vchannel: second.VChannel}}},
			}}
			builder := NewSnapshotBuilder(store, registry, &fakeNodeProvider{}, provider, policyTestConfig())
			snapshot, targets := builder.build(context.Background(), test.batch)
			assert.Len(t, snapshot.ConfigsMap(), len(test.configs))
			for _, id := range test.configs {
				require.Contains(t, snapshot.ConfigsMap(), id)
				assert.Equal(t, store.Get(id).ConfigVersion, snapshot.LoadConfigSnapshot.ConfigVersion(id))
				assert.Same(t, snapshot.ConfigsMap()[id], snapshot.ConfigForShard(triggerShard(id*10, id, 0)))
			}
			assert.ElementsMatch(t, test.shards, targets)
			assert.Nil(t, snapshot.ConfigForShard(residual))
			assert.Zero(t, snapshot.LoadConfigSnapshot.ConfigVersion(3))
		})
	}
}
