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

package loadmgr

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

func TestScopedConfigReadsPreserveVersionsAndSnapshots(t *testing.T) {
	ctx := context.Background()
	store, catalog := newTestStore(t)
	resident := store.Snapshot()
	expectFullSave(catalog, 4)
	first := sampleConfig()
	require.NoError(t, store.Put(ctx, first))
	other := sampleConfig()
	other.CollectionID++
	other.Replicas = []*ReplicaAssignment{{ReplicaID: 2000}}
	require.NoError(t, store.Put(ctx, other))

	entry := store.Get(first.CollectionID)
	require.NotNil(t, entry.Config)
	assert.Equal(t, uint64(2), entry.ConfigVersion)
	assert.Equal(t, uint64(3), entry.StoreVersion)
	scoped := store.SnapshotForCollections([]int64{first.CollectionID, -1, first.CollectionID})
	assert.Same(t, resident, store.snapshot, "point and scoped reads must not rebuild the full snapshot")
	assert.Equal(t, entry.StoreVersion, scoped.Version())
	assert.Equal(t, entry.ConfigVersion, scoped.ConfigVersion(first.CollectionID))
	assert.Equal(t, uint64(0), scoped.ConfigVersion(-1))
	require.Len(t, scoped.ConfigsMap(), 1)
	assert.Same(t, entry.Config, scoped.ConfigsMap()[first.CollectionID])
	assert.Len(t, scoped.ReplicaToConfigMap(), 2)
	assert.NotContains(t, scoped.ReplicaToConfigMap(), int64(2000))
	assert.Empty(t, store.SnapshotForCollections(nil).ConfigsMap())

	updated := first.Clone()
	updated.LoadFields[0].IndexId++
	require.NoError(t, store.Put(ctx, updated))
	assert.Equal(t, first.LoadFields[0].IndexId, entry.Config.LoadFields[0].IndexId)
	assert.Equal(t, first.LoadFields[0].IndexId, scoped.ConfigsMap()[first.CollectionID].LoadFields[0].IndexId)
	assert.Equal(t, uint64(4), store.Get(first.CollectionID).ConfigVersion)

	catalog.EXPECT().ReleaseReplicas(mock.Anything, first.CollectionID).Return(nil).Once()
	catalog.EXPECT().ReleaseCollection(mock.Anything, first.CollectionID).Return(nil).Once()
	require.NoError(t, store.Remove(ctx, first.CollectionID))
	absent := store.Get(first.CollectionID)
	assert.Nil(t, absent.Config)
	assert.Zero(t, absent.ConfigVersion)
	assert.Equal(t, uint64(5), absent.StoreVersion)
	assert.Empty(t, store.SnapshotForCollections([]int64{first.CollectionID}).ConfigsMap())

	require.NoError(t, store.Put(ctx, updated))
	assert.Equal(t, uint64(6), store.Get(first.CollectionID).ConfigVersion)
	assert.NotEqual(t, entry.ConfigVersion, store.Get(first.CollectionID).ConfigVersion)
	assert.NotEqual(t, absent.ConfigVersion, store.Get(first.CollectionID).ConfigVersion)
	assert.Same(t, resident, store.snapshot)
}

func TestScopedConfigReadsDuringPut(t *testing.T) {
	store, catalog := newTestStore(t)
	const updates = 200
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil).Times(updates + 1)
	put := func(revision int64) error {
		return store.Put(context.Background(), &LoadConfig{
			CollectionID: 1,
			LoadFields:   []*messagespb.LoadFieldConfig{{FieldId: 100, IndexId: revision}},
		})
	}
	require.NoError(t, put(1))
	done := make(chan struct{})
	go func() {
		defer close(done)
		for revision := int64(2); revision <= updates+1; revision++ {
			assert.NoError(t, put(revision))
		}
	}()
	for range updates {
		entry := store.Get(1)
		assert.Equal(t, uint64(entry.Config.LoadFields[0].IndexId+1), entry.ConfigVersion)
		assert.Equal(t, entry.ConfigVersion, entry.StoreVersion)
		snapshot := store.SnapshotForCollections([]int64{1})
		assert.Equal(t, uint64(snapshot.ConfigsMap()[1].LoadFields[0].IndexId+1), snapshot.ConfigVersion(1))
		assert.Equal(t, snapshot.ConfigVersion(1), snapshot.Version())
	}
	<-done
}

// Compare hot-path reads after a config update invalidates the full snapshot.
// Persistence is excluded so this measures the in-memory work that scales with
// the number of loaded collections.
func BenchmarkLoadConfigReads(b *testing.B) {
	for _, count := range []int{10000, 50000, 150000} {
		store := &LoadConfigStore{
			version:  1,
			configs:  make(map[int64]*LoadConfig, count),
			versions: make(map[int64]uint64, count),
		}
		for id := int64(1); id <= int64(count); id++ {
			store.configs[id] = &LoadConfig{CollectionID: id, Replicas: []*ReplicaAssignment{{ReplicaID: id}}}
			store.versions[id] = 1
		}
		for _, mode := range []string{"full", "point", "scoped"} {
			b.Run(fmt.Sprintf("%d/%s", count, mode), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					store.mu.Lock()
					store.version++
					store.versions[1] = store.version
					store.mu.Unlock()
					switch mode {
					case "full":
						store.Snapshot()
					case "point":
						store.Get(1)
					case "scoped":
						store.SnapshotForCollections([]int64{1})
					}
				}
			})
		}
	}
}
