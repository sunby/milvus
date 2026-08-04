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
	"bytes"
	"context"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	rootCoordRecoveryDefaultPageSize = 50000
	rootCoordRecoveryLogInterval     = 100000
)

type collectionRecoveryEntry struct {
	dbID int64
	meta *pb.CollectionInfo
}

// ListCollectionsForRecovery loads collections for every requested database in
// one recovery pass. The point path preserves the existing per-collection
// reads. The batch path scans each global child-metadata prefix once and joins
// the results by globally unique collection ID.
func (kc *Catalog) ListCollectionsForRecovery(
	ctx context.Context,
	dbIDs []int64,
	ts typeutil.Timestamp,
) (map[int64][]*model.Collection, error) {
	uniqueDBIDs, collectionsByDB := initRecoveryResult(dbIDs)
	if kc.metaKV == nil {
		mlog.Warn(ctx, "metadata store does not support paginated rootcoord recovery, fallback to point reads")
		return kc.listCollectionsForRecoveryPoint(ctx, uniqueDBIDs, ts)
	}

	entries, err := kc.scanCollectionRecoveryEntries(ctx, uniqueDBIDs, ts)
	if err != nil {
		return nil, err
	}

	externalCollectionCount := 0
	for _, entry := range entries {
		if needsExternalCollectionMetadata(entry.meta) {
			externalCollectionCount++
		}
	}

	mlog.Info(ctx, "rootcoord metadata recovery path selected",
		mlog.String("selectedMode", "batch"),
		mlog.Int("numCollections", len(entries)),
		mlog.Int("numExternalCollections", externalCollectionCount))

	if len(entries) == 0 {
		return collectionsByDB, nil
	}
	return kc.buildBatchRecoveryCollections(ctx, uniqueDBIDs, entries, externalCollectionCount)
}

func initRecoveryResult(dbIDs []int64) ([]int64, map[int64][]*model.Collection) {
	uniqueDBIDs := make([]int64, 0, len(dbIDs))
	collectionsByDB := make(map[int64][]*model.Collection, len(dbIDs))
	for _, dbID := range dbIDs {
		if _, ok := collectionsByDB[dbID]; ok {
			continue
		}
		uniqueDBIDs = append(uniqueDBIDs, dbID)
		collectionsByDB[dbID] = make([]*model.Collection, 0)
	}
	return uniqueDBIDs, collectionsByDB
}

func (kc *Catalog) listCollectionsForRecoveryPoint(
	ctx context.Context,
	dbIDs []int64,
	ts typeutil.Timestamp,
) (map[int64][]*model.Collection, error) {
	_, collectionsByDB := initRecoveryResult(dbIDs)
	for _, dbID := range dbIDs {
		collections, err := kc.ListCollections(ctx, dbID, ts)
		if err != nil {
			return nil, err
		}
		collectionsByDB[dbID] = collections
	}
	return collectionsByDB, nil
}

func (kc *Catalog) scanCollectionRecoveryEntries(
	ctx context.Context,
	dbIDs []int64,
	ts typeutil.Timestamp,
) ([]collectionRecoveryEntry, error) {
	requestedDBs := make(map[int64]struct{}, len(dbIDs))
	for _, dbID := range dbIDs {
		requestedDBs[dbID] = struct{}{}
	}

	entries := make([]collectionRecoveryEntry, 0)
	if _, ok := requestedDBs[util.NonDBID]; ok {
		err := kc.walkRecoveryPrefix(ctx, CollectionMetaPrefix+"/", func(_ []byte, value []byte) error {
			if isRecoveryTombstone(value) {
				return nil
			}
			collectionMeta := &pb.CollectionInfo{}
			if err := proto.Unmarshal(value, collectionMeta); err != nil {
				return merr.WrapErrDataIntegrity(err, "unmarshal legacy collection metadata during recovery")
			}
			// Preserve the existing recovery order: legacy collections are migrated
			// before the database-scoped prefix is scanned, so a successfully moved
			// default-database collection is visible in both recovery buckets.
			kc.fixDefaultDBIDConsistency(ctx, collectionMeta, ts)
			entries = append(entries, collectionRecoveryEntry{dbID: util.NonDBID, meta: collectionMeta})
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	needDatabaseCollections := false
	for _, dbID := range dbIDs {
		if dbID != util.NonDBID {
			needDatabaseCollections = true
			break
		}
	}
	if !needDatabaseCollections {
		return entries, nil
	}

	collectionInfoPrefix := CollectionInfoMetaPrefix + "/"
	fullCollectionInfoPrefix := kc.metaKV.GetPath(collectionInfoPrefix)
	err := kc.walkRecoveryPrefix(ctx, collectionInfoPrefix, func(key, value []byte) error {
		if isRecoveryTombstone(value) {
			return nil
		}
		dbID, err := recoveryKeyFirstID(fullCollectionInfoPrefix, key)
		if err != nil {
			logSkippedRecoveryKey(ctx, collectionInfoPrefix, key, err)
			return nil
		}
		if _, ok := requestedDBs[dbID]; !ok {
			return nil
		}
		collectionMeta := &pb.CollectionInfo{}
		if err := proto.Unmarshal(value, collectionMeta); err != nil {
			return merr.WrapErrDataIntegrity(err, "unmarshal database collection metadata during recovery")
		}
		kc.fixDefaultDBIDConsistency(ctx, collectionMeta, ts)
		entries = append(entries, collectionRecoveryEntry{dbID: dbID, meta: collectionMeta})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func needsExternalCollectionMetadata(collectionMeta *pb.CollectionInfo) bool {
	return partitionVersionAfter210(collectionMeta) || fieldVersionAfter210(collectionMeta)
}

func (kc *Catalog) buildBatchRecoveryCollections(
	ctx context.Context,
	dbIDs []int64,
	entries []collectionRecoveryEntry,
	externalCollectionCount int,
) (map[int64][]*model.Collection, error) {
	_, collectionsByDB := initRecoveryResult(dbIDs)
	collectionCountsByDB := make(map[int64]int, len(collectionsByDB))
	for _, entry := range entries {
		collectionCountsByDB[entry.dbID]++
	}
	for dbID, count := range collectionCountsByDB {
		collectionsByDB[dbID] = make([]*model.Collection, 0, count)
	}

	targetsByCollectionID := make(map[int64]*model.Collection, externalCollectionCount)
	duplicateTargetsByCollectionID := make(map[int64][]*model.Collection)

	for i, entry := range entries {
		needsExternalMetadata := needsExternalCollectionMetadata(entry.meta)
		collection := model.UnmarshalCollectionModel(entry.meta)
		entries[i].meta = nil
		collectionsByDB[entry.dbID] = append(collectionsByDB[entry.dbID], collection)
		if needsExternalMetadata {
			initializeExternalCollectionMetadata(collection)
			if _, ok := targetsByCollectionID[collection.CollectionID]; ok {
				duplicateTargetsByCollectionID[collection.CollectionID] = append(
					duplicateTargetsByCollectionID[collection.CollectionID],
					collection,
				)
			} else {
				targetsByCollectionID[collection.CollectionID] = collection
			}
		}
	}
	if len(targetsByCollectionID) == 0 {
		return collectionsByDB, nil
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return kc.scanRecoveryPartitions(gCtx, targetsByCollectionID, duplicateTargetsByCollectionID)
	})
	g.Go(func() error {
		return kc.scanRecoveryFields(gCtx, targetsByCollectionID, duplicateTargetsByCollectionID)
	})
	g.Go(func() error {
		return kc.scanRecoveryStructArrayFields(gCtx, targetsByCollectionID, duplicateTargetsByCollectionID)
	})
	g.Go(func() error {
		return kc.scanRecoveryFunctions(gCtx, targetsByCollectionID, duplicateTargetsByCollectionID)
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return collectionsByDB, nil
}

func initializeExternalCollectionMetadata(collection *model.Collection) {
	collection.Partitions = make([]*model.Partition, 0)
	collection.Fields = make([]*model.Field, 0)
	collection.StructArrayFields = make([]*model.StructArrayField, 0)
	collection.Functions = make([]*model.Function, 0)
}

func (kc *Catalog) scanRecoveryPartitions(
	ctx context.Context,
	targetsByCollectionID map[int64]*model.Collection,
	duplicateTargetsByCollectionID map[int64][]*model.Collection,
) error {
	prefix := PartitionMetaPrefix + "/"
	fullPrefix := kc.metaKV.GetPath(prefix)
	err := kc.walkRecoveryPrefix(ctx, prefix, func(key, value []byte) error {
		if isRecoveryTombstone(value) {
			return nil
		}
		collectionID, err := recoveryKeyFirstID(fullPrefix, key)
		if err != nil {
			logSkippedRecoveryKey(ctx, prefix, key, err)
			return nil
		}
		target, ok := targetsByCollectionID[collectionID]
		if !ok {
			return nil
		}
		partitionMeta := &pb.PartitionInfo{}
		if err := proto.Unmarshal(value, partitionMeta); err != nil {
			return merr.WrapErrDataIntegrity(err, "unmarshal partition metadata during recovery")
		}
		partition := model.UnmarshalPartitionModel(partitionMeta)
		target.Partitions = append(target.Partitions, partition)
		for _, duplicateTarget := range duplicateTargetsByCollectionID[collectionID] {
			duplicateTarget.Partitions = append(duplicateTarget.Partitions, partition.Clone())
		}
		return nil
	})
	return err
}

func (kc *Catalog) scanRecoveryFields(
	ctx context.Context,
	targetsByCollectionID map[int64]*model.Collection,
	duplicateTargetsByCollectionID map[int64][]*model.Collection,
) error {
	prefix := FieldMetaPrefix + "/"
	fullPrefix := kc.metaKV.GetPath(prefix)
	err := kc.walkRecoveryPrefix(ctx, prefix, func(key, value []byte) error {
		if isRecoveryTombstone(value) {
			return nil
		}
		collectionID, err := recoveryKeyFirstID(fullPrefix, key)
		if err != nil {
			logSkippedRecoveryKey(ctx, prefix, key, err)
			return nil
		}
		target, ok := targetsByCollectionID[collectionID]
		if !ok {
			return nil
		}
		fieldMeta := &schemapb.FieldSchema{}
		if err := proto.Unmarshal(value, fieldMeta); err != nil {
			return merr.WrapErrDataIntegrity(err, "unmarshal field metadata during recovery")
		}
		field := model.UnmarshalFieldModel(fieldMeta)
		target.Fields = append(target.Fields, field)
		for _, duplicateTarget := range duplicateTargetsByCollectionID[collectionID] {
			duplicateTarget.Fields = append(duplicateTarget.Fields, field.Clone())
		}
		return nil
	})
	return err
}

func (kc *Catalog) scanRecoveryStructArrayFields(
	ctx context.Context,
	targetsByCollectionID map[int64]*model.Collection,
	duplicateTargetsByCollectionID map[int64][]*model.Collection,
) error {
	prefix := StructArrayFieldMetaPrefix + "/"
	fullPrefix := kc.metaKV.GetPath(prefix)
	err := kc.walkRecoveryPrefix(ctx, prefix, func(key, value []byte) error {
		if isRecoveryTombstone(value) {
			return nil
		}
		collectionID, err := recoveryKeyFirstID(fullPrefix, key)
		if err != nil {
			logSkippedRecoveryKey(ctx, prefix, key, err)
			return nil
		}
		target, ok := targetsByCollectionID[collectionID]
		if !ok {
			return nil
		}
		fieldMeta := &schemapb.StructArrayFieldSchema{}
		if err := proto.Unmarshal(value, fieldMeta); err != nil {
			return merr.WrapErrDataIntegrity(err, "unmarshal struct array field metadata during recovery")
		}
		field := model.UnmarshalStructArrayFieldModel(fieldMeta)
		target.StructArrayFields = append(target.StructArrayFields, field)
		for _, duplicateTarget := range duplicateTargetsByCollectionID[collectionID] {
			duplicateTarget.StructArrayFields = append(duplicateTarget.StructArrayFields, field.Clone())
		}
		return nil
	})
	return err
}

func (kc *Catalog) scanRecoveryFunctions(
	ctx context.Context,
	targetsByCollectionID map[int64]*model.Collection,
	duplicateTargetsByCollectionID map[int64][]*model.Collection,
) error {
	prefix := FunctionMetaPrefix + "/"
	fullPrefix := kc.metaKV.GetPath(prefix)
	err := kc.walkRecoveryPrefix(ctx, prefix, func(key, value []byte) error {
		if isRecoveryTombstone(value) {
			return nil
		}
		collectionID, err := recoveryKeyFirstID(fullPrefix, key)
		if err != nil {
			logSkippedRecoveryKey(ctx, prefix, key, err)
			return nil
		}
		target, ok := targetsByCollectionID[collectionID]
		if !ok {
			return nil
		}
		functionMeta := &schemapb.FunctionSchema{}
		if err := proto.Unmarshal(value, functionMeta); err != nil {
			return merr.WrapErrDataIntegrity(err, "unmarshal function metadata during recovery")
		}
		function := model.UnmarshalFunctionModel(functionMeta)
		target.Functions = append(target.Functions, function)
		for _, duplicateTarget := range duplicateTargetsByCollectionID[collectionID] {
			duplicateTarget.Functions = append(duplicateTarget.Functions, function.Clone())
		}
		return nil
	})
	return err
}

func (kc *Catalog) walkRecoveryPrefix(
	ctx context.Context,
	prefix string,
	fn func(key, value []byte) error,
) error {
	if err := ctx.Err(); err != nil {
		return merr.Wrapf(err, "scan rootcoord metadata prefix %s", prefix)
	}
	pageSize := rootCoordRecoveryDefaultPageSize
	start := time.Now()
	scanned := 0
	err := kc.metaKV.WalkWithPrefix(ctx, prefix, pageSize, func(key, value []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		scanned++
		if scanned%rootCoordRecoveryLogInterval == 0 {
			mlog.RatedInfo(ctx, 1.0, "rootcoord metadata batch scan progress",
				mlog.String("prefix", prefix),
				mlog.Int("numScanned", scanned),
				mlog.Duration("duration", time.Since(start)))
		}
		return fn(key, value)
	})
	if err != nil {
		return merr.Wrapf(err, "scan rootcoord metadata prefix %s", prefix)
	}
	mlog.Info(ctx, "rootcoord metadata batch scan done",
		mlog.String("prefix", prefix),
		mlog.Int("numScanned", scanned),
		mlog.Int("pageSize", pageSize),
		mlog.Duration("duration", time.Since(start)))
	return nil
}

func recoveryKeyFirstID(fullPrefix string, key []byte) (int64, error) {
	keyString := string(key)
	if !strings.HasPrefix(keyString, fullPrefix) {
		return 0, merr.WrapErrDataIntegrityMsg(
			"metadata key %q is outside expected prefix %q", keyString, fullPrefix)
	}
	suffix := strings.TrimPrefix(keyString, fullPrefix)
	firstPart, remainder, found := strings.Cut(suffix, "/")
	if firstPart == "" || !found || remainder == "" {
		return 0, merr.WrapErrDataIntegrityMsg(
			"metadata key %q has an invalid suffix after prefix %q", keyString, fullPrefix)
	}
	id, err := strconv.ParseInt(firstPart, 10, 64)
	if err != nil {
		return 0, merr.WrapErrDataIntegrity(err,
			"parse metadata identifier from key %q", keyString)
	}
	return id, nil
}

func logSkippedRecoveryKey(ctx context.Context, prefix string, key []byte, err error) {
	mlog.RatedWarn(ctx, 1.0, "skip malformed metadata key during rootcoord recovery",
		mlog.String("prefix", prefix),
		mlog.String("key", string(key)),
		mlog.Err(err))
}

func isRecoveryTombstone(value []byte) bool {
	return bytes.Equal(value, SuffixSnapshotTombstone)
}
