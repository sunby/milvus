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

package coordview

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestRegistry_SnapshotForCollection(t *testing.T) {
	reg := newTestRegistry(t, newMockCatalog(), newMockSyncer())
	first := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_100v0"}
	second := qviews.ShardID{ReplicaID: 2, VChannel: "by-dev-rootcoord-dml_100v1"}
	other := qviews.ShardID{ReplicaID: 3, VChannel: "by-dev-rootcoord-dml_200v0"}
	for _, shard := range []qviews.ShardID{first, second, other} {
		reg.Ensure(shard)
	}
	resident := reg.Snapshot()
	stats := shardStatsForNodes(map[int64][]int64{101: {1}})
	reg.onShardStatsChanged(first, stats)
	scoped := reg.SnapshotForCollection(100)
	assert.Same(t, resident, reg.snapshot)
	assert.Equal(t, reg.version, scoped.Version())
	require.Len(t, scoped.StatsMap(), 2)
	assert.Same(t, stats, scoped.StatsMap()[first])
	assert.Contains(t, scoped.StatsMap(), second, "include resident shards that have no Up view")
	assert.NotContains(t, scoped.StatsMap(), other)
	assert.Empty(t, reg.SnapshotForCollection(300).StatsMap())

	reg.onShardStatsChanged(first, emptyShardStats())
	assert.Same(t, stats, scoped.StatsMap()[first], "later updates must not change a captured snapshot")
	delete(scoped.StatsMap(), second)
	assert.Contains(t, reg.SnapshotForCollection(100).StatsMap(), second, "scoped snapshots own their outer maps")
}

func BenchmarkCollectionShardReads(b *testing.B) {
	for _, count := range []int{10000, 50000, 150000} {
		reg := &ShardViewRegistry{
			version:          1,
			stats:            make(map[qviews.ShardID]*ShardStats, count),
			collectionShards: make(map[int64]map[qviews.ShardID]struct{}, count),
		}
		version := qviews.QueryViewVersion{}
		for id := int64(1); id <= int64(count); id++ {
			shard := qviews.ShardID{ReplicaID: id, VChannel: fmt.Sprintf("by-dev-rootcoord-dml_%dv0", id)}
			reg.stats[shard] = &ShardStats{UpVersion: &version}
			reg.collectionShards[id] = map[qviews.ShardID]struct{}{shard: {}}
		}
		for _, mode := range []string{"full", "scoped"} {
			b.Run(fmt.Sprintf("%d/%s", count, mode), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					reg.mu.Lock()
					reg.version++
					reg.mu.Unlock()
					var snapshot *ShardViewSnapshot
					if mode == "full" {
						snapshot = reg.Snapshot()
					} else {
						snapshot = reg.SnapshotForCollection(1)
					}
					loaded := 0
					for shard, stats := range snapshot.StatsMap() {
						if shard.ReplicaID == 1 && stats.UpVersion != nil {
							loaded++
						}
					}
					if loaded != 1 {
						b.Fatalf("expected one loaded shard, got %d", loaded)
					}
				}
			})
		}
	}
}
