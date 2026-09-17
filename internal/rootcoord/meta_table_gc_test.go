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

package rootcoord

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func newCollectionGCMeta(t *testing.T, state pb.CollectionState) (*MetaTable, *mocks.RootCoordCatalog) {
	t.Helper()
	channel.ResetStaticPChannelStatsManager()
	channel.RecoverPChannelStatsManager(nil)
	t.Cleanup(channel.ResetStaticPChannelStatsManager)
	catalog := mocks.NewRootCoordCatalog(t)
	meta := &MetaTable{
		catalog: catalog,
		dbName2Meta: map[string]*model.Database{
			util.DefaultDBName: {ID: util.DefaultDBID, Name: util.DefaultDBName},
		},
		collID2Meta:        make(map[int64]*model.Collection),
		partitionName2ID:   make(map[int64]map[string]int64),
		fileResourceRefCnt: make(map[int64]int),
		names:              newNameDb(),
		aliases:            newNameDb(),
	}
	for id, name := range map[int64]string{100: "first", 200: "second"} {
		coll := &model.Collection{
			CollectionID: id, DBID: util.DefaultDBID, DBName: util.DefaultDBName,
			Name: name, State: pb.CollectionState_CollectionCreated, ShardsNum: 2,
			FileResourceIds: []int64{1},
			Partitions: []*model.Partition{
				{CollectionID: id, PartitionID: id * 10, PartitionName: "live", State: pb.PartitionState_PartitionCreated},
				{CollectionID: id, PartitionID: id*10 + 1, PartitionName: "old", State: pb.PartitionState_PartitionDropping},
			},
		}
		if id == 100 {
			coll.State = state
		}
		meta.collID2Meta[id] = coll
		meta.names.insert(coll.DBName, coll.Name, id)
		meta.partitionName2ID[id] = map[string]int64{"live": id * 10}
		if coll.Available() {
			meta.generalCnt += 2
			meta.fileResourceRefCnt[1]++
		}
	}
	meta.rebuildAvailableCollectionCountLocked()
	return meta, catalog
}

// Release blocked catalog calls and join workers even when an assertion fails.
func startCollectionGCCall(t *testing.T, release func(), call func() error) <-chan error {
	t.Helper()
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		result <- call()
	}()
	t.Cleanup(func() {
		release()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("metadata operation did not finish after releasing the catalog")
		}
	})
	return result
}

func waitCollectionGCCall(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("metadata operation blocked")
	}
}

func waitCollectionGCCatalog(t *testing.T, started <-chan struct{}) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("catalog operation did not start")
	}
}

func TestMetaTable_CollectionGCDoesNotBlockUnrelatedMetadata(t *testing.T) {
	for _, stage := range []string{"mark dropping", "remove collection", "remove grants"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			state := pb.CollectionState_CollectionDropping
			if stage == "mark dropping" {
				state = pb.CollectionState_CollectionCreated
			}
			meta, catalog := newCollectionGCMeta(t, state)
			started, proceed := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(proceed) })
			block := func(mock.Arguments) { close(started); <-proceed }
			first := mock.MatchedBy(func(coll *model.Collection) bool { return coll.CollectionID == 100 })
			second := mock.MatchedBy(func(coll *model.Collection) bool { return coll.CollectionID == 200 })
			catalog.On("AlterCollection", mock.Anything, second, mock.Anything, metastore.MODIFY, mock.Anything, false).Return(nil).Once()
			catalog.On("DeleteGrantByCollectionName", mock.Anything, util.DefaultTenant, util.DefaultDBName, "second").Return(nil).Once()
			grant := catalog.On("DeleteGrantByCollectionName", mock.Anything, util.DefaultTenant, util.DefaultDBName, "first").Return(nil).Once()
			operation := func() error { return meta.RemoveCollection(ctx, 100, 30) }
			if stage == "mark dropping" {
				catalog.On("AlterCollection", mock.Anything, first, mock.Anything, metastore.MODIFY, mock.Anything, false).Run(block).Return(nil).Once()
				operation = func() error { return meta.DropCollection(ctx, 100, 30) }
			} else {
				drop := catalog.On("DropCollection", mock.Anything, first, mock.Anything).Return(nil).Once()
				if stage == "remove collection" {
					drop.Run(block)
				} else {
					grant.Run(block)
				}
			}
			done := startCollectionGCCall(t, release, operation)
			waitCollectionGCCatalog(t, started)

			reader := startCollectionGCCall(t, release, func() error {
				coll, err := meta.GetCollectionByID(ctx, util.DefaultDBName, 100, typeutil.MaxTimestamp, true)
				if assert.NoError(t, err) {
					assert.Equal(t, state, coll.State, "memory must not change before persistence completes")
				}
				return err
			})
			waitCollectionGCCall(t, reader)
			other := startCollectionGCCall(t, release, func() error { return meta.DropCollection(ctx, 200, 40) })
			waitCollectionGCCall(t, other)

			var added <-chan error
			if stage != "mark dropping" {
				// Delay publishing a reused name until name-based grant cleanup ends.
				catalog.On("CreateCollection", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
				added = startCollectionGCCall(t, release, func() error {
					return meta.AddCollection(ctx, &model.Collection{
						CollectionID: 300, DBID: util.DefaultDBID, DBName: util.DefaultDBName,
						Name: "first", State: pb.CollectionState_CollectionCreated,
					})
				})
				select {
				case err := <-added:
					t.Fatalf("reused collection name was published during GC: %v", err)
				case <-time.After(50 * time.Millisecond):
				}
			}
			release()
			waitCollectionGCCall(t, done)
			if stage != "mark dropping" {
				waitCollectionGCCall(t, added)
				assert.NotContains(t, meta.collID2Meta, int64(100))
				assert.NotContains(t, meta.partitionName2ID, int64(100))
				assert.Equal(t, int64(300), meta.GetCollectionID(ctx, util.DefaultDBName, "first"))
			}
		})
	}
}

func TestMetaTable_CollectionGCLeavesRecreatedCollectionGrants(t *testing.T) {
	ctx := context.Background()
	meta, catalog := newCollectionGCMeta(t, pb.CollectionState_CollectionDropping)
	catalog.On("CreateCollection", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	require.NoError(t, meta.AddCollection(ctx, &model.Collection{
		CollectionID: 300, DBID: util.DefaultDBID, DBName: util.DefaultDBName,
		Name: "first", State: pb.CollectionState_CollectionCreated,
	}))
	catalog.On("DropCollection", mock.Anything, mock.MatchedBy(func(coll *model.Collection) bool {
		return coll.CollectionID == 100
	}), mock.Anything).Return(nil).Once()
	require.NoError(t, meta.RemoveCollection(ctx, 100, 30))
	catalog.AssertNotCalled(t, "DeleteGrantByCollectionName", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	assert.Equal(t, int64(300), meta.GetCollectionID(ctx, util.DefaultDBName, "first"))
	assert.NotContains(t, meta.collID2Meta, int64(100))
}

func TestMetaTable_CollectionGCCatalogFailureAndRetry(t *testing.T) {
	for _, remove := range []bool{false, true} {
		name, state := "drop", pb.CollectionState_CollectionCreated
		if remove {
			name, state = "remove", pb.CollectionState_CollectionDropping
		}
		t.Run(name, func(t *testing.T) {
			meta, catalog := newCollectionGCMeta(t, state)
			before := meta.collID2Meta[100].Clone()
			count, refs := meta.generalCnt, meta.fileResourceRefCnt[1]
			durableState := state
			// Model an ambiguous timeout: persistence may already have succeeded.
			if remove {
				catalog.On("DropCollection", mock.Anything, mock.Anything, mock.Anything).
					Run(func(mock.Arguments) { durableState = pb.CollectionState_CollectionDropped }).Return(context.DeadlineExceeded).Once()
				catalog.On("DropCollection", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
			} else {
				catalog.On("AlterCollection", mock.Anything, mock.Anything, mock.Anything, metastore.MODIFY, mock.Anything, false).
					Run(func(mock.Arguments) { durableState = pb.CollectionState_CollectionDropping }).Return(context.DeadlineExceeded).Once()
				catalog.On("AlterCollection", mock.Anything, mock.Anything, mock.Anything, metastore.MODIFY, mock.Anything, false).Return(nil).Once()
			}
			catalog.On("DeleteGrantByCollectionName", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
			call := func() error { return meta.DropCollection(context.Background(), 100, 30) }
			if remove {
				call = func() error { return meta.RemoveCollection(context.Background(), 100, 30) }
			}
			require.ErrorIs(t, call(), context.DeadlineExceeded)
			assert.NotEqual(t, state, durableState)
			assert.Equal(t, before, meta.collID2Meta[100].Clone())
			assert.Equal(t, count, meta.generalCnt)
			assert.Equal(t, refs, meta.fileResourceRefCnt[1])
			catalog.AssertNotCalled(t, "DeleteGrantByCollectionName", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			require.NoError(t, call())
			require.NoError(t, call())
			assert.Equal(t, 2, meta.generalCnt)
			assert.Equal(t, 1, meta.fileResourceRefCnt[1])
			assert.Equal(t, 1, meta.availableCollectionCount)
			if remove {
				assert.NotContains(t, meta.collID2Meta, int64(100))
			} else {
				assert.Equal(t, pb.CollectionState_CollectionDropping, meta.collID2Meta[100].State)
			}
		})
	}
}

func TestMetaTable_ConcurrentCollectionGCIsIdempotent(t *testing.T) {
	for _, remove := range []bool{false, true} {
		name, state := "drop", pb.CollectionState_CollectionCreated
		if remove {
			name, state = "remove", pb.CollectionState_CollectionDropping
		}
		t.Run(name, func(t *testing.T) {
			meta, catalog := newCollectionGCMeta(t, state)
			started, proceed := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(proceed) })
			block := func(mock.Arguments) { close(started); <-proceed }
			call := func() error { return meta.DropCollection(context.Background(), 100, 30) }
			if remove {
				catalog.On("DropCollection", mock.Anything, mock.Anything, mock.Anything).Run(block).Return(nil).Once()
				call = func() error { return meta.RemoveCollection(context.Background(), 100, 30) }
			} else {
				catalog.On("AlterCollection", mock.Anything, mock.Anything, mock.Anything, metastore.MODIFY, mock.Anything, false).
					Run(block).Return(nil).Once()
			}
			catalog.On("DeleteGrantByCollectionName", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
			results := []<-chan error{startCollectionGCCall(t, release, call)}
			waitCollectionGCCatalog(t, started)
			for i := 0; i < 8; i++ {
				results = append(results, startCollectionGCCall(t, release, call))
			}
			release()
			for _, result := range results {
				waitCollectionGCCall(t, result)
			}
			assert.Equal(t, 2, meta.generalCnt)
			assert.Equal(t, 1, meta.availableCollectionCount)
			assert.Equal(t, 1, meta.fileResourceRefCnt[1])
		})
	}
}

func TestMetaTable_CollectionGCSerializesPartitionGC(t *testing.T) {
	for _, remove := range []bool{false, true} {
		name, state := "drop", pb.CollectionState_CollectionCreated
		if remove {
			name, state = "remove", pb.CollectionState_CollectionDropping
		}
		t.Run(name, func(t *testing.T) {
			meta, catalog := newCollectionGCMeta(t, state)
			started, proceed := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(proceed) })
			block := func(mock.Arguments) { close(started); <-proceed }
			call := func() error { return meta.DropCollection(context.Background(), 100, 30) }
			if remove {
				catalog.On("DropCollection", mock.Anything, mock.Anything, mock.Anything).Run(block).Return(nil).Once()
				call = func() error { return meta.RemoveCollection(context.Background(), 100, 30) }
			} else {
				catalog.On("AlterCollection", mock.Anything, mock.Anything, mock.Anything, metastore.MODIFY, mock.Anything, false).Run(block).Return(nil).Once()
				catalog.On("DropPartition", mock.Anything, util.DefaultDBID, int64(100), int64(1001), mock.Anything).Return(nil).Once()
			}
			catalog.On("DeleteGrantByCollectionName", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
			done := startCollectionGCCall(t, release, call)
			waitCollectionGCCatalog(t, started)
			partition := startCollectionGCCall(t, release, func() error {
				return meta.RemovePartition(context.Background(), 100, 1001, 40)
			})
			select {
			case err := <-partition:
				t.Fatalf("partition GC overlapped collection persistence: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			release()
			waitCollectionGCCall(t, done)
			waitCollectionGCCall(t, partition)
			if remove {
				catalog.AssertNotCalled(t, "DropPartition", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
				assert.NotContains(t, meta.collID2Meta, int64(100))
			} else {
				require.Len(t, meta.collID2Meta[100].Partitions, 1)
				assert.Equal(t, int64(1000), meta.collID2Meta[100].Partitions[0].PartitionID)
				assert.Equal(t, pb.CollectionState_CollectionDropping, meta.collID2Meta[100].State)
			}
		})
	}
}

func TestMetaTable_CollectionGCProtectsReassignedAliases(t *testing.T) {
	ctx := context.Background()
	meta, catalog := newCollectionGCMeta(t, pb.CollectionState_CollectionDropping)
	meta.aliases.insert(util.DefaultDBName, "alias", 100)
	started, proceed := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(proceed) })
	var storedAlias int64 = 100
	catalog.On("DropCollection", mock.Anything, mock.MatchedBy(func(coll *model.Collection) bool {
		return len(coll.Aliases) == 1 && coll.Aliases[0] == "alias"
	}), mock.Anything).Run(func(mock.Arguments) {
		close(started)
		<-proceed
		storedAlias = 0
	}).Return(nil).Once()
	catalog.On("DeleteGrantByCollectionName", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	catalog.On("AlterAlias", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		storedAlias = args.Get(1).(*model.Alias).CollectionID
	}).Return(nil).Once()
	done := startCollectionGCCall(t, release, func() error { return meta.RemoveCollection(ctx, 100, 30) })
	waitCollectionGCCatalog(t, started)
	cchannel := funcutil.GetControlChannel("by-dev-rootcoord-dml_1")
	alias := startCollectionGCCall(t, release, func() error {
		return meta.AlterAlias(ctx, message.BroadcastResultAlterAliasMessageV2{
			Message: message.MustAsBroadcastAlterAliasMessageV2(message.NewAlterAliasMessageBuilderV2().
				WithHeader(&message.AlterAliasMessageHeader{
					DbId: util.DefaultDBID, DbName: util.DefaultDBName, Alias: "alias", CollectionId: 200,
				}).WithBody(&message.AlterAliasMessageBody{}).WithBroadcast([]string{cchannel}).MustBuildBroadcast()),
			Results: map[string]*message.AppendResult{cchannel: {TimeTick: 40}},
		})
	})
	select {
	case err := <-alias:
		t.Fatalf("alias was reassigned before catalog deletion finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	release()
	waitCollectionGCCall(t, done)
	waitCollectionGCCall(t, alias)
	assert.Equal(t, int64(200), storedAlias)
	id, ok := meta.aliases.get(util.DefaultDBName, "alias")
	assert.True(t, ok)
	assert.Equal(t, int64(200), id)
}

func TestMetaTable_CollectionGCOrdersGrantCleanupBeforeRename(t *testing.T) {
	ctx := context.Background()
	meta, catalog := newCollectionGCMeta(t, pb.CollectionState_CollectionDropping)
	started, proceed := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(proceed) })
	grantsMigrated := false
	catalog.On("DropCollection", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	catalog.On("DeleteGrantByCollectionName", mock.Anything, util.DefaultTenant, util.DefaultDBName, "first").
		Run(func(mock.Arguments) {
			close(started)
			<-proceed
			grantsMigrated = false
		}).Return(nil).Once()
	catalog.On("AlterCollection", mock.Anything, mock.Anything, mock.Anything, metastore.MODIFY, mock.Anything, false).Return(nil).Once()
	catalog.On("MigrateGrantCollectionName", mock.Anything, util.DefaultTenant, util.DefaultDBName, "second", util.DefaultDBName, "first").
		Run(func(mock.Arguments) { grantsMigrated = true }).Return(nil).Once()
	done := startCollectionGCCall(t, release, func() error { return meta.RemoveCollection(ctx, 100, 30) })
	waitCollectionGCCatalog(t, started)
	cchannel := funcutil.GetControlChannel("by-dev-rootcoord-dml_1")
	rename := startCollectionGCCall(t, release, func() error {
		return meta.AlterCollection(ctx, message.BroadcastResultAlterCollectionMessageV2{
			Message: message.MustAsBroadcastAlterCollectionMessageV2(message.NewAlterCollectionMessageBuilderV2().
				WithHeader(&message.AlterCollectionMessageHeader{
					CollectionId: 200,
					UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionName}},
				}).WithBody(&message.AlterCollectionMessageBody{
				Updates: &message.AlterCollectionMessageUpdates{CollectionName: "first"},
			}).WithBroadcast([]string{cchannel}).MustBuildBroadcast()),
			Results: map[string]*message.AppendResult{cchannel: {TimeTick: 40}},
		})
	})
	select {
	case err := <-rename:
		t.Fatalf("rename overlapped name-based grant cleanup: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	release()
	waitCollectionGCCall(t, done)
	waitCollectionGCCall(t, rename)
	assert.True(t, grantsMigrated, "GC must not delete the renamed collection's grants")
	assert.Equal(t, int64(200), meta.GetCollectionID(ctx, util.DefaultDBName, "first"))
}
