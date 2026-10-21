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
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/rlsutil"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type recoveryWalkRecorder struct {
	mu        sync.Mutex
	calls     map[string]int
	pageSizes map[string]int
}

func newRecoveryMetaKV(
	t *testing.T,
	records map[string][]byte,
	recorder *recoveryWalkRecorder,
) *mocks.MetaKv {
	t.Helper()
	const rootPath = "test-root"

	metaKV := mocks.NewMetaKv(t)
	metaKV.On("GetPath", mock.Anything).Return(func(key string) string {
		return rootPath + "/" + key
	})
	metaKV.On(
		"WalkWithPrefix",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(func(
		ctx context.Context,
		prefix string,
		pageSize int,
		fn func([]byte, []byte) error,
	) error {
		recorder.mu.Lock()
		recorder.calls[prefix]++
		recorder.pageSizes[prefix] = pageSize
		recorder.mu.Unlock()

		keys := make([]string, 0, len(records))
		for key := range records {
			if strings.HasPrefix(key, prefix) {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		for _, key := range keys {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err := fn([]byte(rootPath+"/"+key), records[key]); err != nil {
				return err
			}
		}
		return nil
	})
	return metaKV
}

func marshalRecoveryProto(t *testing.T, value proto.Message) []byte {
	t.Helper()
	bytes, err := proto.Marshal(value)
	require.NoError(t, err)
	return bytes
}

func findRecoveryCollection(t *testing.T, collections []*model.Collection, collectionID int64) *model.Collection {
	t.Helper()
	for _, collection := range collections {
		if collection.CollectionID == collectionID {
			return collection
		}
	}
	t.Fatalf("collection %d not found", collectionID)
	return nil
}

func TestCatalogListCollectionsForRecoveryBatch(t *testing.T) {
	legacyCollection := &pb.CollectionInfo{
		ID:                         50,
		DbId:                       util.DefaultDBID,
		PartitionIDs:               []int64{500},
		PartitionNames:             []string{"legacy-partition"},
		PartitionCreatedTimestamps: []uint64{1},
		Schema: &schemapb.CollectionSchema{
			Name:   "legacy",
			Fields: []*schemapb.FieldSchema{{FieldID: 501, Name: "legacy-field"}},
		},
		State: pb.CollectionState_CollectionCreated,
	}
	collection100 := &pb.CollectionInfo{
		ID:     100,
		DbId:   1,
		Schema: &schemapb.CollectionSchema{Name: "c100"},
		State:  pb.CollectionState_CollectionCreated,
	}
	collection200 := &pb.CollectionInfo{
		ID:     200,
		DbId:   2,
		Schema: &schemapb.CollectionSchema{Name: "c200"},
		State:  pb.CollectionState_CollectionCreated,
	}
	collection300 := &pb.CollectionInfo{
		ID:                         300,
		DbId:                       1,
		PartitionIDs:               []int64{3000},
		PartitionNames:             []string{"embedded-partition"},
		PartitionCreatedTimestamps: []uint64{1},
		Schema: &schemapb.CollectionSchema{
			Name:   "c300",
			Fields: []*schemapb.FieldSchema{{FieldID: 3001, Name: "embedded-field"}},
		},
		State: pb.CollectionState_CollectionCreated,
	}

	records := map[string][]byte{
		CollectionMetaPrefix + "/50":                   marshalRecoveryProto(t, legacyCollection),
		CollectionInfoMetaPrefix + "/1/100":            marshalRecoveryProto(t, collection100),
		CollectionInfoMetaPrefix + "/1/300":            marshalRecoveryProto(t, collection300),
		CollectionInfoMetaPrefix + "/2/200":            marshalRecoveryProto(t, collection200),
		CollectionInfoMetaPrefix + "/2/999":            []byte(tombstone()),
		CollectionInfoMetaPrefix + "/not-a-db/1000":    marshalRecoveryProto(t, collection100),
		PartitionMetaPrefix + "/100/10":                marshalRecoveryProto(t, &pb.PartitionInfo{CollectionId: 100, PartitionID: 10, PartitionName: "p100"}),
		PartitionMetaPrefix + "/200/20":                marshalRecoveryProto(t, &pb.PartitionInfo{CollectionId: 200, PartitionID: 20, PartitionName: "p200"}),
		FieldMetaPrefix + "/100/11":                    marshalRecoveryProto(t, &schemapb.FieldSchema{FieldID: 11, Name: "f100"}),
		FieldMetaPrefix + "/200/21":                    marshalRecoveryProto(t, &schemapb.FieldSchema{FieldID: 21, Name: "f200"}),
		FieldMetaPrefix + "/999/1":                     []byte("orphan-corrupt-value"),
		FieldMetaPrefix + "/not-a-collection/1":        []byte("malformed-key-corrupt-value"),
		StructArrayFieldMetaPrefix + "/100/12":         marshalRecoveryProto(t, &schemapb.StructArrayFieldSchema{FieldID: 12, Name: "sf100"}),
		FunctionMetaPrefix + "/200/22":                 marshalRecoveryProto(t, &schemapb.FunctionSchema{Id: 22, Name: "fn200"}),
		FunctionMetaPrefix + "/100/tombstone-function": []byte(tombstone()),
	}
	recorder := &recoveryWalkRecorder{calls: make(map[string]int), pageSizes: make(map[string]int)}
	metaKV := newRecoveryMetaKV(t, records, recorder)
	catalog := NewCatalog(metaKV).(*Catalog)

	collectionsByDB, err := catalog.ListCollectionsForRecovery(
		context.Background(),
		[]int64{util.NonDBID, 1, 2},
		0,
	)
	require.NoError(t, err)
	require.Len(t, collectionsByDB[util.NonDBID], 1)
	require.Len(t, collectionsByDB[1], 2)
	require.Len(t, collectionsByDB[2], 1)

	c100 := findRecoveryCollection(t, collectionsByDB[1], 100)
	require.Len(t, c100.Partitions, 1)
	assert.Equal(t, int64(10), c100.Partitions[0].PartitionID)
	require.Len(t, c100.Fields, 1)
	assert.Equal(t, int64(11), c100.Fields[0].FieldID)
	require.Len(t, c100.StructArrayFields, 1)
	assert.Equal(t, int64(12), c100.StructArrayFields[0].FieldID)
	assert.NotNil(t, c100.Functions)
	assert.Empty(t, c100.Functions)

	c200 := findRecoveryCollection(t, collectionsByDB[2], 200)
	require.Len(t, c200.Functions, 1)
	assert.Equal(t, int64(22), c200.Functions[0].ID)

	c300 := findRecoveryCollection(t, collectionsByDB[1], 300)
	require.Len(t, c300.Partitions, 1)
	assert.Equal(t, int64(3000), c300.Partitions[0].PartitionID)
	require.Len(t, c300.Fields, 1)
	assert.Equal(t, int64(3001), c300.Fields[0].FieldID)

	for _, prefix := range []string{
		CollectionMetaPrefix + "/",
		CollectionInfoMetaPrefix + "/",
		PartitionMetaPrefix + "/",
		FieldMetaPrefix + "/",
		StructArrayFieldMetaPrefix + "/",
		FunctionMetaPrefix + "/",
	} {
		assert.Equal(t, 1, recorder.calls[prefix], prefix)
		assert.Equal(t, rootCoordRecoveryDefaultPageSize, recorder.pageSizes[prefix], prefix)
	}
}

func TestCatalogListCollectionsForRecoverySkipsChildScansForEmbeddedMetadata(t *testing.T) {
	collection := &pb.CollectionInfo{
		ID:                         100,
		DbId:                       1,
		PartitionIDs:               []int64{10},
		PartitionNames:             []string{"p100"},
		PartitionCreatedTimestamps: []uint64{1},
		Schema: &schemapb.CollectionSchema{
			Name:   "c100",
			Fields: []*schemapb.FieldSchema{{FieldID: 11, Name: "f100"}},
		},
		State: pb.CollectionState_CollectionCreated,
	}
	records := map[string][]byte{
		CollectionInfoMetaPrefix + "/1/100": marshalRecoveryProto(t, collection),
	}
	recorder := &recoveryWalkRecorder{calls: make(map[string]int), pageSizes: make(map[string]int)}
	metaKV := newRecoveryMetaKV(t, records, recorder)
	catalog := NewCatalog(metaKV).(*Catalog)

	collectionsByDB, err := catalog.ListCollectionsForRecovery(context.Background(), []int64{1}, 0)
	require.NoError(t, err)
	require.Len(t, collectionsByDB[1], 1)
	require.Len(t, collectionsByDB[1][0].Partitions, 1)
	require.Len(t, collectionsByDB[1][0].Fields, 1)
	assert.Equal(t, 1, recorder.calls[CollectionInfoMetaPrefix+"/"])
	assert.Zero(t, recorder.calls[PartitionMetaPrefix+"/"])
	assert.Zero(t, recorder.calls[FieldMetaPrefix+"/"])
	assert.Zero(t, recorder.calls[StructArrayFieldMetaPrefix+"/"])
	assert.Zero(t, recorder.calls[FunctionMetaPrefix+"/"])
}

func TestCatalogListCollectionsForRecoveryFallsBackWithoutWalker(t *testing.T) {
	collection := &pb.CollectionInfo{
		ID:                         100,
		DbId:                       1,
		PartitionIDs:               []int64{10},
		PartitionNames:             []string{"p"},
		PartitionCreatedTimestamps: []uint64{1},
		Schema: &schemapb.CollectionSchema{
			Name:   "c100",
			Fields: []*schemapb.FieldSchema{{FieldID: 11}},
		},
	}
	txnKV := mocks.NewTxnKV(t)
	txnKV.On("LoadWithPrefix", mock.Anything, BuildDatabasePrefixWithDBID(1)).Return(
		[]string{"collection"},
		[]string{string(marshalRecoveryProto(t, collection))},
		nil,
	)
	catalog := NewCatalog(txnKV).(*Catalog)

	collectionsByDB, err := catalog.ListCollectionsForRecovery(context.Background(), []int64{1}, 0)
	require.NoError(t, err)
	require.Len(t, collectionsByDB[1], 1)
	assert.Equal(t, int64(100), collectionsByDB[1][0].CollectionID)
}

func TestCatalogListCollectionsForRecoveryRejectsCorruptTargetMetadata(t *testing.T) {
	records := map[string][]byte{
		CollectionInfoMetaPrefix + "/1/100": []byte("corrupt-collection-metadata"),
	}
	recorder := &recoveryWalkRecorder{calls: make(map[string]int), pageSizes: make(map[string]int)}
	metaKV := newRecoveryMetaKV(t, records, recorder)
	catalog := NewCatalog(metaKV).(*Catalog)

	_, err := catalog.ListCollectionsForRecovery(context.Background(), []int64{1}, 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrDataIntegrity)
}

func TestCatalogListCollectionsForRecoveryRejectsCorruptTargetChildMetadata(t *testing.T) {
	collection := &pb.CollectionInfo{
		ID:     100,
		DbId:   1,
		Schema: &schemapb.CollectionSchema{Name: "c100"},
		State:  pb.CollectionState_CollectionCreated,
	}
	records := map[string][]byte{
		CollectionInfoMetaPrefix + "/1/100": marshalRecoveryProto(t, collection),
		FieldMetaPrefix + "/100/10":         []byte("corrupt-field-metadata"),
	}
	recorder := &recoveryWalkRecorder{calls: make(map[string]int), pageSizes: make(map[string]int)}
	metaKV := newRecoveryMetaKV(t, records, recorder)
	catalog := NewCatalog(metaKV).(*Catalog)

	_, err := catalog.ListCollectionsForRecovery(context.Background(), []int64{1}, 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrDataIntegrity)
}

func TestCatalogListCollectionsForRecoveryPreservesScanError(t *testing.T) {
	injected := merr.WrapErrIoFailedReason("injected recovery scan failure", "test")
	metaKV := mocks.NewMetaKv(t)
	metaKV.On("GetPath", mock.Anything).Return(func(key string) string {
		return "test-root/" + key
	})
	metaKV.On(
		"WalkWithPrefix",
		mock.Anything,
		CollectionInfoMetaPrefix+"/",
		mock.Anything,
		mock.Anything,
	).Return(injected).Once()
	catalog := NewCatalog(metaKV).(*Catalog)

	_, err := catalog.ListCollectionsForRecovery(context.Background(), []int64{1}, 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, injected)
	assert.ErrorIs(t, err, merr.ErrIoFailed)
}

func TestCatalogListCollectionsForRecoveryHonorsCancellationDuringWalk(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	metaKV := mocks.NewMetaKv(t)
	metaKV.On("GetPath", mock.Anything).Return(func(key string) string {
		return "test-root/" + key
	})
	metaKV.On(
		"WalkWithPrefix",
		mock.Anything,
		CollectionInfoMetaPrefix+"/",
		mock.Anything,
		mock.Anything,
	).Return(func(
		_ context.Context,
		_ string,
		_ int,
		fn func([]byte, []byte) error,
	) error {
		cancel()
		return fn(
			[]byte("test-root/"+CollectionInfoMetaPrefix+"/1/100"),
			[]byte("unused"),
		)
	}).Once()
	catalog := NewCatalog(metaKV).(*Catalog)

	_, err := catalog.ListCollectionsForRecovery(ctx, []int64{1}, 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestCatalogListCollectionsForRecoveryMigratesLegacyCollectionIntoDefaultBucket(t *testing.T) {
	legacyCollection := &pb.CollectionInfo{
		ID:     100,
		DbId:   util.NonDBID,
		Schema: &schemapb.CollectionSchema{Name: "legacy"},
		State:  pb.CollectionState_CollectionCreated,
	}
	records := map[string][]byte{
		CollectionMetaPrefix + "/100":          marshalRecoveryProto(t, legacyCollection),
		PartitionMetaPrefix + "/100/10":        marshalRecoveryProto(t, &pb.PartitionInfo{CollectionId: 100, PartitionID: 10, PartitionName: "partition"}),
		FieldMetaPrefix + "/100/11":            marshalRecoveryProto(t, &schemapb.FieldSchema{FieldID: 11, Name: "field"}),
		StructArrayFieldMetaPrefix + "/100/12": marshalRecoveryProto(t, &schemapb.StructArrayFieldSchema{FieldID: 12, Name: "struct-field"}),
		FunctionMetaPrefix + "/100/13":         marshalRecoveryProto(t, &schemapb.FunctionSchema{Id: 13, Name: "function"}),
	}
	recorder := &recoveryWalkRecorder{calls: make(map[string]int), pageSizes: make(map[string]int)}
	metaKV := newRecoveryMetaKV(t, records, recorder)
	metaKV.On(
		"MultiSaveAndRemove",
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Run(func(args mock.Arguments) {
		saves := args.Get(1).(map[string]string)
		removals := args.Get(2).([]string)
		for _, key := range removals {
			delete(records, key)
		}
		for key, value := range saves {
			records[key] = []byte(value)
		}
	}).Return(nil).Once()
	catalog := NewCatalog(metaKV).(*Catalog)

	collectionsByDB, err := catalog.ListCollectionsForRecovery(
		context.Background(),
		[]int64{util.NonDBID, util.DefaultDBID},
		0,
	)
	require.NoError(t, err)
	require.Len(t, collectionsByDB[util.NonDBID], 1)
	require.Len(t, collectionsByDB[util.DefaultDBID], 1)

	legacy := collectionsByDB[util.NonDBID][0]
	migrated := collectionsByDB[util.DefaultDBID][0]
	assert.Equal(t, util.DefaultDBID, legacy.DBID)
	assert.Equal(t, util.DefaultDBID, migrated.DBID)
	require.Len(t, legacy.Partitions, 1)
	require.Len(t, legacy.Fields, 1)
	require.Len(t, legacy.StructArrayFields, 1)
	require.Len(t, legacy.Functions, 1)
	require.Len(t, migrated.Partitions, 1)
	require.Len(t, migrated.Fields, 1)
	require.Len(t, migrated.StructArrayFields, 1)
	require.Len(t, migrated.Functions, 1)
	assert.NotSame(t, legacy.Partitions[0], migrated.Partitions[0])
	assert.NotSame(t, legacy.Fields[0], migrated.Fields[0])
	assert.NotSame(t, legacy.StructArrayFields[0], migrated.StructArrayFields[0])
	assert.NotSame(t, legacy.Functions[0], migrated.Functions[0])

	legacy.Fields[0].Name = "mutated"
	assert.Equal(t, "field", migrated.Fields[0].Name)
	assert.NotContains(t, records, CollectionMetaPrefix+"/100")
	assert.Contains(t, records, BuildCollectionKey(util.DefaultDBID, 100))
}

func TestCatalogListRLSPoliciesForRecoveryMatchesPerCollectionReads(t *testing.T) {
	ctx := context.Background()
	metaKV, cleanup := newTombstoneTestMetaKV(t)
	defer cleanup()
	catalog := NewCatalog(metaKV).(*Catalog)

	for _, policy := range []*model.RLSPolicy{
		{DBID: 1, CollectionID: 20, PolicyID: 102, PolicyName: "tenant", Actions: []rlsutil.PolicyAction{rlsutil.PolicyActionQuery}, UsingExpr: "true"},
		{DBID: 1, CollectionID: 20, PolicyID: 103, PolicyName: "owner", CheckExpr: "true"},
		// Same name as 103: listings order it by ID; MetaTable rejects the pair.
		{DBID: 1, CollectionID: 20, PolicyID: 101, PolicyName: "owner"},
		// Key prefix ".../2/" must not absorb ".../20/" or the other way round.
		{DBID: 2, CollectionID: 2, PolicyID: 200, PolicyName: "sibling"},
		{DBID: 3, CollectionID: 999, PolicyID: 900, PolicyName: "orphan"},
	} {
		require.NoError(t, catalog.SaveRLSPolicy(ctx, policy))
	}
	// A key equal to the per-collection prefix is still part of that collection's read.
	bare := &model.RLSPolicy{CollectionID: 30, PolicyID: 300, PolicyName: "bare-prefix-key"}
	require.NoError(t, metaKV.Save(ctx, BuildRLSPolicyPrefix(30), string(marshalRecoveryProto(t, model.MarshalRLSPolicyModel(bare)))))
	// Corrupt values under keys no per-collection read of a recovered collection returns.
	for _, key := range []string{
		BuildRLSPolicyKey(999, 901),
		RLSPolicyMetaPrefix + "/020/1",
		RLSPolicyMetaPrefix + "/+20/1",
		RLSPolicyMetaPrefix + "/20",
		RLSPolicyMetaPrefix + "/not-a-collection/1",
	} {
		require.NoError(t, metaKV.Save(ctx, key, "corrupt-policy"))
	}
	_, err := catalog.ListRLSPolicies(ctx, 999)
	require.ErrorIs(t, err, merr.ErrDataIntegrity, "the orphan value must be undecodable")

	recovered := typeutil.NewUniqueSet(2, 20, 30, 40)
	policiesByCollectionID, supported, err := catalog.ListRLSPoliciesForRecovery(ctx, func(collectionID int64) bool {
		return recovered.Contain(collectionID)
	})
	require.NoError(t, err)
	require.True(t, supported)
	assert.ElementsMatch(t, []int64{2, 20, 30}, lo.Keys(policiesByCollectionID))
	for collectionID := range recovered {
		expected, err := catalog.ListRLSPolicies(ctx, collectionID)
		require.NoError(t, err)
		if len(expected) == 0 {
			assert.NotContains(t, policiesByCollectionID, collectionID)
			continue
		}
		assert.Equal(t, expected, policiesByCollectionID[collectionID], "collection %d", collectionID)
	}
	assert.Equal(t, []int64{101, 103, 102}, lo.Map(policiesByCollectionID[20], func(policy *model.RLSPolicy, _ int) int64 {
		return policy.PolicyID
	}))
}

func TestCatalogListRLSPoliciesForRecoveryDecodesOnlyRecoveredCollections(t *testing.T) {
	records := map[string][]byte{
		BuildRLSPolicyKey(20, 100): marshalRecoveryProto(t, model.MarshalRLSPolicyModel(&model.RLSPolicy{CollectionID: 20, PolicyID: 100, PolicyName: "tenant"})),
		BuildRLSPolicyKey(30, 300): []byte("corrupt-policy"),
		BuildRLSPolicyKey(40, 400): []byte(tombstone()),
	}
	newCatalog := func() (*Catalog, *recoveryWalkRecorder) {
		recorder := &recoveryWalkRecorder{calls: make(map[string]int), pageSizes: make(map[string]int)}
		return NewCatalog(newRecoveryMetaKV(t, records, recorder)).(*Catalog), recorder
	}

	t.Run("orphan corrupt keys are skipped", func(t *testing.T) {
		catalog, recorder := newCatalog()
		var asked []int64
		policiesByCollectionID, supported, err := catalog.ListRLSPoliciesForRecovery(context.Background(), func(collectionID int64) bool {
			asked = append(asked, collectionID)
			return collectionID == 20
		})
		require.NoError(t, err)
		require.True(t, supported)
		require.Len(t, policiesByCollectionID, 1)
		require.Len(t, policiesByCollectionID[20], 1)
		assert.Equal(t, "tenant", policiesByCollectionID[20][0].PolicyName)
		assert.Equal(t, []int64{20, 30, 40}, asked)
		assert.Equal(t, 1, recorder.calls[RLSPolicyMetaPrefix+"/"])
	})

	for _, collectionID := range []int64{30, 40} {
		t.Run(fmt.Sprintf("corrupt policy of recovered collection %d fails", collectionID), func(t *testing.T) {
			catalog, _ := newCatalog()
			_, _, err := catalog.ListRLSPoliciesForRecovery(context.Background(), func(id int64) bool {
				return id == collectionID
			})
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
			assert.ErrorContains(t, err, fmt.Sprintf("collection %d", collectionID))
		})
	}
}

func TestCatalogListRLSPoliciesForRecoveryEmptyPrefix(t *testing.T) {
	recorder := &recoveryWalkRecorder{calls: make(map[string]int), pageSizes: make(map[string]int)}
	catalog := NewCatalog(newRecoveryMetaKV(t, map[string][]byte{}, recorder)).(*Catalog)

	policiesByCollectionID, supported, err := catalog.ListRLSPoliciesForRecovery(context.Background(), func(int64) bool {
		t.Fatal("no key to filter")
		return false
	})
	require.NoError(t, err)
	require.True(t, supported)
	assert.NotNil(t, policiesByCollectionID)
	assert.Empty(t, policiesByCollectionID)
	assert.Equal(t, 1, recorder.calls[RLSPolicyMetaPrefix+"/"])
}

func TestCatalogListRLSPoliciesForRecoveryRequiresWalker(t *testing.T) {
	// A TxnKV without WalkWithPrefix: the mock fails the test on any call.
	catalog := NewCatalog(mocks.NewTxnKV(t)).(*Catalog)

	policiesByCollectionID, supported, err := catalog.ListRLSPoliciesForRecovery(context.Background(), func(int64) bool {
		return true
	})
	require.NoError(t, err)
	assert.False(t, supported)
	assert.Nil(t, policiesByCollectionID)
}

func TestCatalogListRLSPoliciesForRecoveryPreservesScanError(t *testing.T) {
	injected := merr.WrapErrIoFailedReason("injected RLS policy scan failure", "test")
	metaKV := mocks.NewMetaKv(t)
	metaKV.On("GetPath", mock.Anything).Return(func(key string) string {
		return "test-root/" + key
	})
	metaKV.On("WalkWithPrefix", mock.Anything, RLSPolicyMetaPrefix+"/", mock.Anything, mock.Anything).
		Return(injected).Once()
	catalog := NewCatalog(metaKV).(*Catalog)

	_, supported, err := catalog.ListRLSPoliciesForRecovery(context.Background(), func(int64) bool {
		return true
	})
	assert.True(t, supported)
	assert.ErrorIs(t, err, injected)
	assert.ErrorIs(t, err, merr.ErrIoFailed)
}
