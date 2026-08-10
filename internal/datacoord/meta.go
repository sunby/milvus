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

// Package datacoord contains core functions in datacoord
package datacoord

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/segmentutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type CompactionMeta interface {
	GetSegment(ctx context.Context, segID UniqueID) *SegmentInfo
	GetSegmentInfos(segIDs []UniqueID) []*SegmentInfo
	SelectSegments(ctx context.Context, filters ...SegmentFilter) []*SegmentInfo
	GetHealthySegment(ctx context.Context, segID UniqueID) *SegmentInfo
	UpdateSegmentsInfo(ctx context.Context, mutations map[int64][]MutateFunc, newSegments ...*datapb.SegmentInfo) error
	SetSegmentsCompacting(ctx context.Context, segmentID []int64, compacting bool)
	CheckAndSetSegmentsCompacting(ctx context.Context, segmentIDs []int64) (bool, bool)
	CompleteCompactionMutation(ctx context.Context, t *datapb.CompactionTask, result *datapb.CompactionPlanResult) ([]*SegmentInfo, *segMetricMutation, error)
	ValidateSegmentStateBeforeCompleteCompactionMutation(t *datapb.CompactionTask) error
	CleanPartitionStatsInfo(ctx context.Context, info *datapb.PartitionStatsInfo) error

	SaveCompactionTask(ctx context.Context, task *datapb.CompactionTask) error
	DropCompactionTask(ctx context.Context, task *datapb.CompactionTask) error
	GetCompactionTasks(ctx context.Context) map[int64][]*datapb.CompactionTask
	GetCompactionTasksByTriggerID(ctx context.Context, triggerID int64) []*datapb.CompactionTask

	GetIndexMeta() *indexMeta
	GetAnalyzeMeta() *analyzeMeta
	GetPartitionStatsMeta() *partitionStatsMeta
	GetCompactionTaskMeta() *compactionTaskMeta
	GetFileResources(ctx context.Context, resourceIDs ...int64) ([]*internalpb.FileResourceInfo, error)
}

var _ CompactionMeta = (*meta)(nil)

type meta struct {
	ctx            context.Context
	catalog        metastore.DataCoordCatalog
	metaRootPath   string
	segmentPersist OptimisticTxnPersist[string, *datapb.SegmentInfo]

	collections            *typeutil.ConcurrentMap[UniqueID, *collectionInfo] // collection id to collection info
	recoveredCollectionIDs []int64

	segments        *CachedSegmentsInfo // segment id to segment info
	dataViewManager DataViewManager

	channelCPs   *channelCPs // vChannel -> channel checkpoint/see position
	chunkManager storage.ChunkManager

	indexMeta                     *indexMeta
	analyzeMeta                   *analyzeMeta
	partitionStatsMeta            *partitionStatsMeta
	compactionTaskMeta            *compactionTaskMeta
	statsTaskMeta                 *statsTaskMeta
	externalCollectionRefreshMeta *externalCollectionRefreshMeta

	// File Resource Meta
	resourceIDMap   map[int64]*internalpb.FileResourceInfo // id -> info
	resourceVersion uint64
	resourceLock    lock.RWMutex
	// Snapshot Meta
	snapshotMeta *snapshotMeta
}

func (m *meta) GetIndexMeta() *indexMeta {
	return m.indexMeta
}

func (m *meta) GetAnalyzeMeta() *analyzeMeta {
	return m.analyzeMeta
}

func (m *meta) GetPartitionStatsMeta() *partitionStatsMeta {
	return m.partitionStatsMeta
}

func (m *meta) GetCompactionTaskMeta() *compactionTaskMeta {
	return m.compactionTaskMeta
}

func (m *meta) GetSnapshotMeta() *snapshotMeta {
	return m.snapshotMeta
}

func (m *meta) isCollectionCompactionBlocked(collectionID int64) bool {
	if m.snapshotMeta == nil {
		return false
	}
	return m.snapshotMeta.IsCollectionCompactionBlocked(collectionID)
}

func (m *meta) isSegmentCompactionProtected(segmentID int64) bool {
	if m.snapshotMeta == nil {
		return false
	}
	return m.snapshotMeta.IsSegmentCompactionProtected(segmentID)
}

type channelCPs struct {
	lock.RWMutex
	checkpoints  map[string]*msgpb.MsgPosition
	channelLocks *lock.KeyLock[string]
	cond         *syncutil.ContextCond
}

func newChannelCps() *channelCPs {
	cp := &channelCPs{
		checkpoints:  make(map[string]*msgpb.MsgPosition),
		channelLocks: lock.NewKeyLock[string](),
	}
	// use the same lock as channelCPs
	cp.cond = syncutil.NewContextCond(&cp.RWMutex)
	return cp
}

func (cp *channelCPs) lockChannel(channel string) {
	cp.channelLocks.Lock(channel)
}

func (cp *channelCPs) unlockChannel(channel string) {
	cp.channelLocks.Unlock(channel)
}

func (cp *channelCPs) lockChannels(channels []string) []string {
	uniqueChannels := make(map[string]struct{}, len(channels))
	for _, channel := range channels {
		if channel == "" {
			continue
		}
		uniqueChannels[channel] = struct{}{}
	}

	lockedChannels := make([]string, 0, len(uniqueChannels))
	for channel := range uniqueChannels {
		lockedChannels = append(lockedChannels, channel)
	}
	sort.Strings(lockedChannels)
	for _, channel := range lockedChannels {
		cp.lockChannel(channel)
	}
	return lockedChannels
}

func (cp *channelCPs) unlockChannels(channels []string) {
	for i := len(channels) - 1; i >= 0; i-- {
		cp.unlockChannel(channels[i])
	}
}

type segmentMetricStateChange map[string]map[string]map[string]map[string]map[string]int

// A local cache of segment metric update. Must call commit() to take effect.
type segMetricMutation struct {
	stateChange       segmentMetricStateChange // segment level -> state -> isSorted -> storageVersion -> format change count.
	rowCountChange    int64                    // Change in # of rows.
	rowCountAccChange int64                    // Total # of historical added rows, accumulated.
}

type collectionInfo struct {
	ID             int64
	Schema         *schemapb.CollectionSchema
	Partitions     []int64
	StartPositions []*commonpb.KeyDataPair
	Properties     map[string]string
	CreatedAt      Timestamp
	DatabaseName   string
	DatabaseID     int64
	VChannelNames  []string
}

const (
	segmentMetricFormatLegacy  = "legacy"
	segmentMetricFormatUnknown = "unknown"
	segmentMetricFormatMixed   = "mixed"
)

func segmentMetricFormatLabel(segment *SegmentInfo) string {
	if segment == nil {
		return segmentMetricFormatUnknown
	}

	format := ""
	for _, fieldBinlog := range segment.GetBinlogs() {
		fieldFormat := strings.TrimSpace(fieldBinlog.GetFormat())
		if fieldFormat == "" {
			continue
		}
		if format == "" {
			format = fieldFormat
			continue
		}
		if format != fieldFormat {
			return segmentMetricFormatMixed
		}
	}
	if format == "" {
		if segment.GetStorageVersion() < storage.StorageV2 {
			return segmentMetricFormatLegacy
		}
		return segmentMetricFormatUnknown
	}
	return format
}

func segmentMetricLabelKey(segment *SegmentInfo) [5]string {
	return [5]string{
		segment.GetState().String(),
		segment.GetLevel().String(),
		getSortStatus(segment.GetIsSorted()),
		fmt.Sprint(segment.GetStorageVersion()),
		segmentMetricFormatLabel(segment),
	}
}

func segmentMetricLabelValues(segment *SegmentInfo) []string {
	labels := segmentMetricLabelKey(segment)
	return labels[:]
}

// IsExternal returns true when the collection schema references an external source or spec.
func (c *collectionInfo) IsExternal() bool {
	if c == nil {
		return false
	}
	if c.Schema == nil {
		return false
	}
	return typeutil.IsExternalCollection(c.Schema)
}

type dbInfo struct {
	ID         int64
	Name       string
	Properties []*commonpb.KeyValuePair
}

// showCollectionIDs retrieves all collection IDs from RootCoord with retry on ErrServiceUnimplemented.
func showCollectionIDs(ctx context.Context, broker broker.Broker) ([]int64, error) {
	var (
		err  error
		resp *rootcoordpb.ShowCollectionIDsResponse
	)
	retryErr := retry.Handle(ctx, func() (bool, error) {
		resp, err = broker.ShowCollectionIDs(ctx)
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			return true, err
		}
		return false, err
	})
	if retryErr != nil {
		return nil, retryErr
	}

	collectionIDs := make([]int64, 0, 4096)
	for _, collections := range resp.GetDbCollections() {
		collectionIDs = append(collectionIDs, collections.GetCollectionIDs()...)
	}
	return collectionIDs, nil
}

// NewMeta creates meta from provided `kv.TxnKV`
func (m *meta) joinMetaRootPath(key string) string {
	if m.metaRootPath == "" {
		return key
	}
	return strings.TrimSuffix(m.metaRootPath, "/") + "/" + key
}

func (m *meta) segmentKey(collectionID, partitionID, segmentID int64) string {
	return m.joinMetaRootPath(segmentKey(collectionID, partitionID, segmentID))
}

func (m *meta) segmentPrefix() string {
	return m.joinMetaRootPath(segmentMetaPrefix)
}

const (
	collectionRecoveryProgressLogInterval   = 10000
	segmentCacheRecoveryProgressLogInterval = 100000
)

func newMeta(ctx context.Context, catalog metastore.DataCoordCatalog, chunkManager storage.ChunkManager, broker broker.Broker, segmentPersist OptimisticTxnPersist[string, *datapb.SegmentInfo], metaRootPaths ...string) (*meta, error) {
	metaRecoveryStart := time.Now()
	mlog.Info(ctx, "datacoord meta recovery started")

	// Collection IDs are retained for DataView repair and collection cache recovery.
	collectionIDRecoveryStart := time.Now()
	mlog.Info(ctx, "datacoord collection ID recovery started")
	collectionIDs, err := showCollectionIDs(ctx, broker)
	if err != nil {
		mlog.Warn(ctx, "datacoord collection ID recovery failed",
			mlog.Duration("duration", time.Since(collectionIDRecoveryStart)),
			mlog.Err(err))
		return nil, err
	}
	mlog.Info(ctx, "datacoord collection ID recovery done",
		mlog.Int("numCollections", len(collectionIDs)),
		mlog.Duration("duration", time.Since(collectionIDRecoveryStart)))

	var (
		im   *indexMeta
		am   *analyzeMeta
		psm  *partitionStatsMeta
		ctm  *compactionTaskMeta
		stm  *statsTaskMeta
		ecrm *externalCollectionRefreshMeta
		spm  *snapshotMeta
	)

	// Construct meta first so segment recovery can run in parallel with sub-meta loading.
	metaRootPath := ""
	if len(metaRootPaths) > 0 {
		metaRootPath = metaRootPaths[0]
	}

	mt := &meta{
		ctx:                    ctx,
		catalog:                catalog,
		metaRootPath:           metaRootPath,
		segmentPersist:         segmentPersist,
		collections:            typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		recoveredCollectionIDs: append([]int64(nil), collectionIDs...),
		segments:               NewCachedSegmentsInfo(),
		channelCPs:             newChannelCps(),
		chunkManager:           chunkManager,
		resourceIDMap:          make(map[int64]*internalpb.FileResourceInfo),
		resourceVersion:        0,
		resourceLock:           lock.RWMutex{},
	}

	g, _ := errgroup.WithContext(ctx)
	parallelRecoveryStart := time.Now()

	g.Go(func() error {
		var err error
		im, err = newIndexMeta(ctx, catalog)
		return err
	})

	g.Go(func() error {
		var err error
		am, err = newAnalyzeMeta(ctx, catalog)
		return err
	})

	g.Go(func() error {
		var err error
		psm, err = newPartitionStatsMeta(ctx, catalog)
		return err
	})

	g.Go(func() error {
		var err error
		ctm, err = newCompactionTaskMeta(ctx, catalog)
		return err
	})

	g.Go(func() error {
		var err error
		stm, err = newStatsTaskMeta(ctx, catalog)
		return err
	})

	g.Go(func() error {
		var err error
		ecrm, err = newExternalCollectionRefreshMeta(ctx, catalog)
		return err
	})

	g.Go(func() error {
		var err error
		spm, err = newSnapshotMeta(ctx, catalog, chunkManager)
		return err
	})

	g.Go(func() error {
		return mt.reloadFromKV(ctx, collectionIDs)
	})

	if err := g.Wait(); err != nil {
		mlog.Warn(ctx, "datacoord parallel metadata recovery failed",
			mlog.Duration("duration", time.Since(parallelRecoveryStart)),
			mlog.Err(err))
		return nil, err
	}
	mlog.Info(ctx, "datacoord parallel metadata recovery done",
		mlog.Duration("duration", time.Since(parallelRecoveryStart)))

	// Assign sub-metas after all goroutines complete
	mt.indexMeta = im
	mt.analyzeMeta = am
	mt.partitionStatsMeta = psm
	mt.compactionTaskMeta = ctm
	mt.statsTaskMeta = stm
	mt.externalCollectionRefreshMeta = ecrm
	mt.snapshotMeta = spm

	mlog.Info(ctx, "datacoord meta recovery done",
		mlog.Int("numCollections", len(collectionIDs)),
		mlog.Duration("duration", time.Since(metaRecoveryStart)))
	return mt, nil
}

type segmentRecoveryMetricAggregates struct {
	segmentCounts            map[[5]string]int64
	insertFileCountFrequency map[int]int64
	statFileCountFrequency   map[int]int64
	deleteFileCountFrequency map[int]int64
	numStoredRows            int64
}

func newSegmentRecoveryMetricAggregates() *segmentRecoveryMetricAggregates {
	return &segmentRecoveryMetricAggregates{
		segmentCounts:            make(map[[5]string]int64),
		insertFileCountFrequency: make(map[int]int64),
		statFileCountFrequency:   make(map[int]int64),
		deleteFileCountFrequency: make(map[int]int64),
	}
}

func (a *segmentRecoveryMetricAggregates) Add(info *SegmentInfo) {
	a.segmentCounts[segmentMetricLabelKey(info)]++
	if info.GetState() != commonpb.SegmentState_Flushed {
		return
	}

	a.numStoredRows += info.GetNumOfRows()
	insertFileCount := 0
	for _, fieldBinlog := range info.GetBinlogs() {
		insertFileCount += len(fieldBinlog.GetBinlogs())
	}
	a.insertFileCountFrequency[insertFileCount]++

	statFileCount := 0
	for _, fieldBinlog := range info.GetStatslogs() {
		statFileCount += len(fieldBinlog.GetBinlogs())
	}
	a.statFileCountFrequency[statFileCount]++

	deleteFileCount := 0
	for _, fieldBinlog := range info.GetDeltalogs() {
		deleteFileCount += len(fieldBinlog.GetBinlogs())
	}
	a.deleteFileCountFrequency[deleteFileCount]++
}

func (a *segmentRecoveryMetricAggregates) PublishSegmentCounts() {
	metrics.DataCoordNumSegments.Reset()
	for labels, count := range a.segmentCounts {
		metrics.DataCoordNumSegments.WithLabelValues(labels[:]...).Add(float64(count))
	}
}

func (a *segmentRecoveryMetricAggregates) ReplayFileHistogramsAsync() {
	go func() {
		for fileCount, frequency := range a.insertFileCountFrequency {
			for range frequency {
				metrics.FlushedSegmentFileNum.WithLabelValues(metrics.InsertFileLabel).Observe(float64(fileCount))
			}
		}
		for fileCount, frequency := range a.statFileCountFrequency {
			for range frequency {
				metrics.FlushedSegmentFileNum.WithLabelValues(metrics.StatFileLabel).Observe(float64(fileCount))
			}
		}
		for fileCount, frequency := range a.deleteFileCountFrequency {
			for range frequency {
				metrics.FlushedSegmentFileNum.WithLabelValues(metrics.DeleteFileLabel).Observe(float64(fileCount))
			}
		}
	}()
}

// reloadFromKV loads meta from KV storage
func (m *meta) reloadFromKV(ctx context.Context, collectionIDs []int64) error {
	record := timerecord.NewTimeRecorder("datacoord")
	scanStart := time.Now()
	prefix := m.segmentPrefix()
	mlog.Info(ctx, "datacoord global segment catalog scan started",
		mlog.Int("numCollections", len(collectionIDs)),
		mlog.String("prefix", prefix))
	_, segments, versions, err := m.segmentPersist.Scan(ctx, prefix)
	if err != nil {
		return err
	}
	totalScannedSegments := int64(len(segments))
	mlog.Info(ctx, "datacoord global segment catalog scan done",
		mlog.Int64("numSegments", totalScannedSegments),
		mlog.Duration("duration", time.Since(scanStart)))

	aggregates := newSegmentRecoveryMetricAggregates()
	cacheBuildStart := time.Now()
	var recoveredSegments int64
	for i, segment := range segments {
		info := NewSegmentInfo(segment)
		m.segments.SetSegment(segment.GetID(), info, versions[i])
		aggregates.Add(info)
		recoveredSegments++
		if recoveredSegments%segmentCacheRecoveryProgressLogInterval == 0 && recoveredSegments < totalScannedSegments {
			mlog.Info(ctx, "datacoord segment cache rebuild progress",
				mlog.Int64("completedSegments", recoveredSegments),
				mlog.Int64("totalSegments", totalScannedSegments),
				mlog.Duration("duration", time.Since(cacheBuildStart)))
		}
	}

	mlog.Info(ctx, "datacoord segment catalog recovery done",
		mlog.Int64("numScannedSegments", totalScannedSegments),
		mlog.Int64("numRecoveredSegments", recoveredSegments),
		mlog.Int64("numStoredRows", aggregates.numStoredRows),
		mlog.Duration("duration", time.Since(scanStart)))
	metrics.DataCoordNumCollections.WithLabelValues().Set(0)
	aggregates.PublishSegmentCounts()
	numSegments := int(recoveredSegments)

	checkpointScanStart := time.Now()
	mlog.Info(ctx, "datacoord channel checkpoint catalog scan started")
	channelCPs, err := m.catalog.ListChannelCheckpoint(m.ctx)
	if err != nil {
		return err
	}
	mlog.Info(ctx, "datacoord channel checkpoint catalog scan done",
		mlog.Int("numChannelCheckpoints", len(channelCPs)),
		mlog.Duration("duration", time.Since(checkpointScanStart)))
	checkpointCacheBuildStart := time.Now()
	for vChannel, pos := range channelCPs {
		// for 2.2.2 issue https://github.com/milvus-io/milvus/issues/22181
		pos.ChannelName = vChannel
		m.channelCPs.checkpoints[vChannel] = pos
		if pos.Timestamp != math.MaxUint64 {
			// Should not be set as metric since it's a tombstone value.
			ts, _ := tsoutil.ParseTS(pos.Timestamp)
			metrics.DataCoordCheckpointUnixSeconds.WithLabelValues(paramtable.GetStringNodeID(), vChannel).
				Set(float64(ts.Unix()))
		}
	}
	mlog.Info(ctx, "datacoord channel checkpoint cache rebuild done",
		mlog.Int("numChannelCheckpoints", len(channelCPs)),
		mlog.Duration("duration", time.Since(checkpointCacheBuildStart)))
	aggregates.ReplayFileHistogramsAsync()

	mlog.Info(ctx, "DataCoord meta reloadFromKV done",
		mlog.Int("numSegments", numSegments),
		mlog.Duration("duration", record.ElapseSpan()))
	return nil
}

func (m *meta) reloadCollectionsFromRootcoord(ctx context.Context, broker broker.Broker) error {
	recoveryStart := time.Now()
	mlog.Info(ctx, "datacoord collection cache recovery started")

	listDatabasesStart := time.Now()
	resp, err := broker.ListDatabases(ctx)
	if err != nil {
		mlog.Warn(ctx, "datacoord database list recovery failed",
			mlog.Duration("duration", time.Since(listDatabasesStart)),
			mlog.Err(err))
		return err
	}
	mlog.Info(ctx, "datacoord database list recovery done",
		mlog.Int("numDatabases", len(resp.GetDbNames())),
		mlog.Duration("duration", time.Since(listDatabasesStart)))

	collectionIDs := make([]int64, 0, len(m.recoveredCollectionIDs))
	showCollectionsDuration := time.Duration(0)
	for _, dbName := range resp.GetDbNames() {
		mlog.Info(ctx, "datacoord collection list recovery started", mlog.FieldDbName(dbName))
		showCollectionsStart := time.Now()
		collectionsResp, err := broker.ShowCollections(ctx, dbName)
		callDuration := time.Since(showCollectionsStart)
		showCollectionsDuration += callDuration
		if err != nil {
			mlog.Warn(ctx, "datacoord collection list recovery failed",
				mlog.FieldDbName(dbName),
				mlog.Duration("duration", callDuration),
				mlog.Err(err))
			return err
		}
		collectionIDs = append(collectionIDs, collectionsResp.GetCollectionIds()...)
		mlog.Info(ctx, "datacoord collection list recovery done",
			mlog.FieldDbName(dbName),
			mlog.Int("numCollections", len(collectionsResp.GetCollectionIds())),
			mlog.Duration("duration", callDuration))
	}
	mlog.Info(ctx, "datacoord collection lists recovered",
		mlog.Int("numCollections", len(collectionIDs)),
		mlog.Duration("showCollectionsRPCDuration", showCollectionsDuration),
		mlog.Duration("duration", time.Since(recoveryStart)))

	collectionCacheBuildStart := time.Now()
	mlog.Info(ctx, "datacoord collection detail recovery started",
		mlog.Int("totalCollections", len(collectionIDs)))
	describeCollectionRPCDuration := time.Duration(0)
	showPartitionsRPCDuration := time.Duration(0)
	cacheInsertDuration := time.Duration(0)
	slowestCollectionID := int64(0)
	slowestCollectionDuration := time.Duration(0)
	for index, collectionID := range collectionIDs {
		collectionStart := time.Now()

		describeStart := time.Now()
		descResp, err := broker.DescribeCollectionInternal(ctx, collectionID)
		describeDuration := time.Since(describeStart)
		describeCollectionRPCDuration += describeDuration
		if err != nil {
			mlog.Warn(ctx, "datacoord collection cache recovery failed",
				mlog.String("stage", "DescribeCollectionInternal"),
				mlog.FieldCollectionID(collectionID),
				mlog.Int("completedCollections", index),
				mlog.Int("totalCollections", len(collectionIDs)),
				mlog.Duration("callDuration", describeDuration),
				mlog.Err(err))
			return err
		}

		showPartitionsStart := time.Now()
		partitionIDs, err := broker.ShowPartitionsInternal(ctx, collectionID)
		showPartitionsDuration := time.Since(showPartitionsStart)
		showPartitionsRPCDuration += showPartitionsDuration
		if err != nil {
			mlog.Warn(ctx, "datacoord collection cache recovery failed",
				mlog.String("stage", "ShowPartitionsInternal"),
				mlog.FieldCollectionID(collectionID),
				mlog.Int("completedCollections", index),
				mlog.Int("totalCollections", len(collectionIDs)),
				mlog.Duration("callDuration", showPartitionsDuration),
				mlog.Err(err))
			return err
		}

		collection := &collectionInfo{
			ID:             collectionID,
			Schema:         descResp.GetSchema(),
			Partitions:     partitionIDs,
			StartPositions: descResp.GetStartPositions(),
			Properties:     funcutil.KeyValuePair2Map(descResp.GetProperties()),
			CreatedAt:      descResp.GetCreatedTimestamp(),
			DatabaseName:   descResp.GetDbName(),
			DatabaseID:     descResp.GetDbId(),
			VChannelNames:  descResp.GetVirtualChannelNames(),
		}
		cacheInsertStart := time.Now()
		m.addCollectionToCache(collection)
		cacheInsertDuration += time.Since(cacheInsertStart)

		collectionDuration := time.Since(collectionStart)
		if collectionDuration > slowestCollectionDuration {
			slowestCollectionID = collectionID
			slowestCollectionDuration = collectionDuration
		}
		completedCollections := index + 1
		if completedCollections%collectionRecoveryProgressLogInterval == 0 && completedCollections < len(collectionIDs) {
			mlog.Info(ctx, "datacoord collection cache recovery progress",
				mlog.Int("completedCollections", completedCollections),
				mlog.Int("totalCollections", len(collectionIDs)),
				mlog.Duration("averageCollectionDuration", time.Since(collectionCacheBuildStart)/time.Duration(completedCollections)),
				mlog.Duration("describeCollectionRPCDuration", describeCollectionRPCDuration),
				mlog.Duration("showPartitionsRPCDuration", showPartitionsRPCDuration),
				mlog.Duration("cacheInsertDuration", cacheInsertDuration),
				mlog.Int64("slowestCollectionID", slowestCollectionID),
				mlog.Duration("slowestCollectionDuration", slowestCollectionDuration),
				mlog.Duration("duration", time.Since(collectionCacheBuildStart)))
		}
	}
	metrics.DataCoordNumCollections.WithLabelValues().Set(float64(m.collections.Len()))
	averageCollectionDuration := time.Duration(0)
	if len(collectionIDs) > 0 {
		averageCollectionDuration = time.Since(collectionCacheBuildStart) / time.Duration(len(collectionIDs))
	}
	mlog.Info(ctx, "datacoord collection cache recovery done",
		mlog.Int("numCollections", len(collectionIDs)),
		mlog.Duration("averageCollectionDuration", averageCollectionDuration),
		mlog.Duration("describeCollectionRPCDuration", describeCollectionRPCDuration),
		mlog.Duration("showPartitionsRPCDuration", showPartitionsRPCDuration),
		mlog.Duration("cacheInsertDuration", cacheInsertDuration),
		mlog.Int64("slowestCollectionID", slowestCollectionID),
		mlog.Duration("slowestCollectionDuration", slowestCollectionDuration),
		mlog.Duration("duration", time.Since(collectionCacheBuildStart)),
		mlog.Duration("totalDuration", time.Since(recoveryStart)))
	return nil
}

func (m *meta) addCollectionToCache(collection *collectionInfo) {
	m.collections.Insert(collection.ID, collection)
}

// AddCollection adds a collection into meta
// Note that collection info is just for caching and will not be set into etcd from datacoord
func (m *meta) AddCollection(collection *collectionInfo) {
	mlog.Info(context.TODO(), "meta update: add collection", zap.Int64("collectionID", collection.ID))
	m.addCollectionToCache(collection)
	metrics.DataCoordNumCollections.WithLabelValues().Set(float64(m.collections.Len()))
	mlog.Info(context.TODO(), "meta update: add collection - complete", zap.Int64("collectionID", collection.ID))
}

// DropCollection drop a collection from meta
func (m *meta) DropCollection(collectionID int64) {
	mlog.Info(context.TODO(), "meta update: drop collection", zap.Int64("collectionID", collectionID))
	if _, ok := m.collections.GetAndRemove(collectionID); ok {
		metrics.CleanupDataCoordWithCollectionID(collectionID)
		metrics.DataCoordNumCollections.WithLabelValues().Set(float64(m.collections.Len()))
		mlog.Info(context.TODO(), "meta update: drop collection - complete", zap.Int64("collectionID", collectionID))
	}
}

// GetCollection returns collection info with provided collection id from local cache
func (m *meta) GetCollection(collectionID UniqueID) *collectionInfo {
	collection, ok := m.collections.Get(collectionID)
	if !ok {
		return nil
	}
	return collection
}

// GetCollections returns collections from local cache
func (m *meta) GetCollections() []*collectionInfo {
	return m.collections.Values()
}

func (m *meta) GetClonedCollectionInfo(collectionID UniqueID) *collectionInfo {
	coll, ok := m.collections.Get(collectionID)
	if !ok {
		return nil
	}

	clonedProperties := make(map[string]string)
	maps.Copy(clonedProperties, coll.Properties)
	cloneColl := &collectionInfo{
		ID:             coll.ID,
		Schema:         proto.Clone(coll.Schema).(*schemapb.CollectionSchema),
		Partitions:     coll.Partitions,
		StartPositions: common.CloneKeyDataPairs(coll.StartPositions),
		Properties:     clonedProperties,
		DatabaseName:   coll.DatabaseName,
		DatabaseID:     coll.DatabaseID,
		VChannelNames:  coll.VChannelNames,
	}

	return cloneColl
}

// GetSegmentsChanPart returns segments organized in Channel-Partition dimension with selector applied
// TODO: Move this function to the compaction module after reorganizing the DataCoord modules.
func GetSegmentsChanPart(m *meta, collectionID int64, filters ...SegmentFilter) []*chanPartSegments {
	type dim struct {
		partitionID int64
		channelName string
	}

	mDimEntry := make(map[dim]*chanPartSegments)

	filters = append(filters, WithCollection(collectionID))
	candidates := m.SelectSegments(context.Background(), filters...)
	for _, si := range candidates {
		d := dim{si.PartitionID, si.InsertChannel}
		entry, ok := mDimEntry[d]
		if !ok {
			entry = &chanPartSegments{
				collectionID: si.CollectionID,
				partitionID:  si.PartitionID,
				channelName:  si.InsertChannel,
			}
			mDimEntry[d] = entry
		}
		entry.segments = append(entry.segments, si)
	}
	result := make([]*chanPartSegments, 0, len(mDimEntry))
	for _, entry := range mDimEntry {
		result = append(result, entry)
	}
	return result
}

// GetNumRowsOfCollection returns total rows count of segments belongs to provided collection
func (m *meta) GetNumRowsOfCollection(ctx context.Context, collectionID UniqueID) int64 {
	var ret int64
	segments := m.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(func(si *SegmentInfo) bool {
		return isSegmentHealthy(si)
	}))
	for _, segment := range segments {
		ret += segment.GetNumOfRows()
	}
	return ret
}

func getBinlogFileCount(s *datapb.SegmentInfo) int {
	statsFieldFn := func(fieldBinlogs []*datapb.FieldBinlog) int {
		cnt := 0
		for _, fbs := range fieldBinlogs {
			cnt += len(fbs.Binlogs)
		}
		return cnt
	}

	cnt := 0
	cnt += statsFieldFn(s.GetBinlogs())
	cnt += statsFieldFn(s.GetStatslogs())
	cnt += statsFieldFn(s.GetDeltalogs())
	return cnt
}

func (m *meta) GetQuotaInfo() *metricsinfo.DataCoordQuotaMetrics {
	info := &metricsinfo.DataCoordQuotaMetrics{}
	collectionBinlogSize := make(map[UniqueID]int64)
	partitionBinlogSize := make(map[UniqueID]map[UniqueID]int64)
	collectionRowsNum := make(map[UniqueID]map[commonpb.SegmentState]int64)
	// collection id => l0 delta entry count
	collectionL0RowCounts := make(map[UniqueID]int64)

	segments := m.segments.GetSegments()
	var total int64
	storedBinlogSize := make(map[string]map[string]int64) // map[collectionID]map[segment_state]size
	binlogFileCount := make(map[string]int64)             // map[collectionID]count
	coll2DbName := make(map[string]string)

	for _, segment := range segments {
		segmentSize := segment.getSegmentSize()
		if isSegmentHealthy(segment) && !segment.GetIsImporting() {
			total += segmentSize
			collectionBinlogSize[segment.GetCollectionID()] += segmentSize

			partBinlogSize, ok := partitionBinlogSize[segment.GetCollectionID()]
			if !ok {
				partBinlogSize = make(map[int64]int64)
				partitionBinlogSize[segment.GetCollectionID()] = partBinlogSize
			}
			partBinlogSize[segment.GetPartitionID()] += segmentSize

			coll, ok := m.collections.Get(segment.GetCollectionID())
			if ok {
				collIDStr := strconv.FormatInt(segment.GetCollectionID(), 10)
				coll2DbName[collIDStr] = coll.DatabaseName
				if _, ok := storedBinlogSize[collIDStr]; !ok {
					storedBinlogSize[collIDStr] = make(map[string]int64)
				}

				storedBinlogSize[collIDStr][segment.GetState().String()] += segmentSize
				binlogFileCount[collIDStr] += int64(getBinlogFileCount(segment.SegmentInfo))
				// } else {
				// log.Ctx(context.TODO()).Warn("not found database name", zap.Int64("collectionID", segment.GetCollectionID()))
			}

			if _, ok := collectionRowsNum[segment.GetCollectionID()]; !ok {
				collectionRowsNum[segment.GetCollectionID()] = make(map[commonpb.SegmentState]int64)
			}
			collectionRowsNum[segment.GetCollectionID()][segment.GetState()] += segment.GetNumOfRows()

			if segment.GetLevel() == datapb.SegmentLevel_L0 {
				collectionL0RowCounts[segment.GetCollectionID()] += segment.getDeltaCount()
			}
		}
	}

	// Reset to remove dropped collection
	metrics.DataCoordStoredBinlogSize.Reset()
	for collectionID, state2Size := range storedBinlogSize {
		for state, size := range state2Size {
			metrics.DataCoordStoredBinlogSize.WithLabelValues(coll2DbName[collectionID], collectionID, state).Set(float64(size))
		}
	}
	// Reset to remove dropped collection
	metrics.DataCoordSegmentBinLogFileCount.Reset()
	for collectionID, size := range binlogFileCount {
		metrics.DataCoordSegmentBinLogFileCount.WithLabelValues(collectionID).Set(float64(size))
	}

	metrics.DataCoordNumStoredRows.Reset()
	for collectionID, statesRows := range collectionRowsNum {
		coll, ok := m.collections.Get(collectionID)
		if ok {
			for state, rows := range statesRows {
				metrics.DataCoordNumStoredRows.WithLabelValues(coll.DatabaseName, strconv.FormatInt(collectionID, 10), coll.Schema.GetName(), state.String()).Set(float64(rows))
			}
		}
	}

	metrics.DataCoordL0DeleteEntriesNum.Reset()
	for collectionID, entriesNum := range collectionL0RowCounts {
		coll, ok := m.collections.Get(collectionID)
		if ok {
			metrics.DataCoordL0DeleteEntriesNum.WithLabelValues(coll.DatabaseName, strconv.FormatInt(collectionID, 10)).Set(float64(entriesNum))
		}
	}

	info.TotalBinlogSize = total
	info.CollectionBinlogSize = collectionBinlogSize
	info.PartitionsBinlogSize = partitionBinlogSize
	info.CollectionL0RowCount = collectionL0RowCounts

	return info
}

func (m *meta) GetAllCollectionNumRows() map[int64]int64 {
	ret := make(map[int64]int64, m.collections.Len())
	segments := m.segments.GetSegments()
	for _, segment := range segments {
		if isSegmentHealthy(segment) {
			ret[segment.GetCollectionID()] += segment.GetNumOfRows()
		}
	}
	return ret
}

// AddSegment records segment info, persisting info into kv store.
// If the segment already exists in etcd, the operation is a no-op.
func (m *meta) AddSegment(ctx context.Context, segment *SegmentInfo) error {
	logger := mlog.With(zap.String("channel", segment.GetInsertChannel()))
	logger.Info(ctx, "meta update: adding segment - Start", zap.Int64("segmentID", segment.GetID()))

	key := m.segmentKey(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
	txn := m.segmentPersist.Txn(ctx)
	txn.Insert(key, segment.SegmentInfo)
	results, err := txn.Commit()
	if err != nil {
		if errors.Is(err, ErrKeyAlreadyExists) {
			logger.Info(ctx, "segment already exists, ignore the operation", zap.Int64("segmentID", segment.ID))
			return nil
		}
		logger.Error(ctx, "meta update: adding segment failed",
			zap.Int64("segmentID", segment.GetID()),
			zap.Error(err))
		return err
	}
	m.segments.SetSegment(segment.GetID(), segment, results[0].Version)

	metrics.DataCoordNumSegments.WithLabelValues(segmentMetricLabelValues(segment)...).Inc()
	logger.Info(ctx, "meta update: adding segment - complete", zap.Int64("segmentID", segment.GetID()))
	return nil
}

// DropSegment remove segment, etcd persistence also removed
func (m *meta) DropSegment(ctx context.Context, segment *SegmentInfo) error {
	logger := mlog.With()
	segmentID := segment.GetID()
	logger.Debug(ctx, "meta update: dropping segment", zap.Int64("segmentID", segmentID))
	key := m.segmentKey(segment.GetCollectionID(), segment.GetPartitionID(), segmentID)
	txn := m.segmentPersist.Txn(ctx)
	txn.Delete(key)
	results, err := txn.Commit()
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			logger.Info(ctx, "meta update: dropping segment - already deleted", zap.Int64("segmentID", segmentID))
			m.segments.DropSegment(segmentID, math.MaxInt64)
			return nil
		}
		logger.Warn(ctx, "meta update: dropping segment failed",
			zap.Int64("segmentID", segmentID),
			zap.Error(err))
		return err
	}
	metrics.DataCoordNumSegments.WithLabelValues(segmentMetricLabelValues(segment)...).Dec()

	m.segments.DropSegment(segmentID, results[0].Version)
	logger.Info(ctx, "meta update: dropping segment - complete",
		zap.Int64("segmentID", segmentID))
	return nil
}

// GetHealthySegment returns segment info with provided id
// if not segment is found, nil will be returned
func (m *meta) GetHealthySegment(ctx context.Context, segID UniqueID) *SegmentInfo {
	segment := m.segments.GetSegment(segID)
	if segment != nil && isSegmentHealthy(segment) {
		return segment
	}
	return nil
}

// Get segments By filter function
func (m *meta) GetSegments(segIDs []UniqueID, filterFunc SegmentInfoSelector) []UniqueID {
	var result []UniqueID
	for _, id := range segIDs {
		segment := m.segments.GetSegment(id)
		if segment != nil && filterFunc(segment) {
			result = append(result, id)
		}
	}
	return result
}

func (m *meta) GetSegmentInfos(segIDs []UniqueID) []*SegmentInfo {
	var result []*SegmentInfo
	for _, id := range segIDs {
		segment := m.segments.GetSegment(id)
		if segment != nil {
			result = append(result, segment)
		}
	}
	return result
}

// GetSegment returns segment info with provided id
// include the unhealthy segment
// if not segment is found, nil will be returned
func (m *meta) GetSegment(ctx context.Context, segID UniqueID) *SegmentInfo {
	return m.segments.GetSegment(segID)
}

// GetAllSegmentsUnsafe returns all segments
func (m *meta) GetAllSegmentsUnsafe() []*SegmentInfo {
	return m.segments.GetSegments()
}

func (m *meta) GetSegmentsTotalNumRows(segmentIDs []UniqueID) int64 {
	var sum int64 = 0
	for _, segmentID := range segmentIDs {
		segment := m.segments.GetSegment(segmentID)
		if segment == nil {
			mlog.Warn(context.TODO(), "cannot find segment", zap.Int64("segmentID", segmentID))
			continue
		}
		sum += segment.GetNumOfRows()
	}
	return sum
}

func (m *meta) GetSegmentsChannels(segmentIDs []UniqueID) (map[int64]string, error) {
	segChannels := make(map[int64]string)
	for _, segmentID := range segmentIDs {
		segment := m.segments.GetSegment(segmentID)
		if segment == nil {
			return nil, errors.New(fmt.Sprintf("cannot find segment %d", segmentID))
		}
		segChannels[segmentID] = segment.GetInsertChannel()
	}
	return segChannels, nil
}

// SetState setting segment with provided ID state
func (m *meta) SetState(ctx context.Context, segmentID UniqueID, targetState commonpb.SegmentState) error {
	logger := mlog.With()
	logger.Debug(context.TODO(), "meta update: setting segment state",
		zap.Int64("segmentID", segmentID),
		zap.Any("target state", targetState))
	curSegInfo := m.segments.GetSegment(segmentID)
	if curSegInfo == nil {
		return fmt.Errorf("segment is not exist with ID = %d", segmentID)
	}

	key := m.segmentKey(curSegInfo.GetCollectionID(), curSegInfo.GetPartitionID(), curSegInfo.GetID())
	txn := m.segmentPersist.Txn(ctx)
	txn.Update(key, func(existing *datapb.SegmentInfo) (*datapb.SegmentInfo, bool) {
		existing.State = targetState
		if targetState == commonpb.SegmentState_Sealed && curSegInfo.GetLastExpireTime() > existing.GetLastExpireTime() {
			existing.LastExpireTime = curSegInfo.GetLastExpireTime()
		}
		if targetState == commonpb.SegmentState_Dropped {
			existing.DroppedAt = uint64(time.Now().UnixNano())
		}
		return existing, true
	})
	results, err := txn.Commit()
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) && targetState == commonpb.SegmentState_Dropped {
			return nil
		}
		logger.Warn(context.TODO(), "meta update: setting segment state - failed to alter segments",
			zap.Int64("segmentID", segmentID),
			zap.String("target state", targetState.String()),
			zap.Error(err))
		return err
	}
	updatedSeg := NewSegmentInfo(results[0].Value)
	old, existed := m.segments.SetSegment(segmentID, updatedSeg, results[0].Version)
	if existed && old.GetState() != updatedSeg.GetState() {
		metricMutation := segMetricMutation{stateChange: make(segmentMetricStateChange)}
		metricMutation.appendSegmentLabelChange(old, updatedSeg)
		metricMutation.commit()
	}
	logger.Info(context.TODO(), "meta update: setting segment state - complete",
		zap.Int64("segmentID", segmentID),
		zap.String("target state", targetState.String()))
	return nil
}

func (m *meta) UpdateSegment(segmentID int64, operators ...SegmentOperator) error {
	logger := mlog.With()
	// Need cache to construct key (collection/partition IDs are immutable).
	info := m.segments.GetSegment(segmentID)
	if info == nil {
		logger.Warn(context.TODO(), "meta update: UpdateSegment - segment not found",
			zap.Int64("segmentID", segmentID))
		return merr.WrapErrSegmentNotFound(segmentID)
	}

	key := m.segmentKey(info.GetCollectionID(), info.GetPartitionID(), info.GetID())
	txn := m.segmentPersist.Txn(m.ctx)
	txn.Update(key, func(existing *datapb.SegmentInfo) (*datapb.SegmentInfo, bool) {
		seg := NewSegmentInfo(existing)
		updated := false
		for _, operator := range operators {
			if operator(seg) {
				updated = true
			}
		}
		return seg.SegmentInfo, updated
	})
	results, err := txn.Commit()
	if err != nil {
		logger.Warn(context.TODO(), "meta update: update segment - failed to alter segments",
			zap.Int64("segmentID", segmentID),
			zap.Error(err))
		return err
	}
	// Update in-memory meta.
	m.segments.SetSegment(segmentID, NewSegmentInfo(results[0].Value), results[0].Version)

	logger.Info(context.TODO(), "meta update: update segment - complete",
		zap.Int64("segmentID", segmentID))
	return nil
}

// MutateFunc modifies a *datapb.SegmentInfo in place.
// Returns true to proceed with the write, false to skip this segment's update.
// Runs inside UpdateFunc against the persisted value for CAS correctness.
type MutateFunc func(seg *datapb.SegmentInfo) bool

func singleSegmentMutation(segmentID int64, fn MutateFunc) map[int64][]MutateFunc {
	return map[int64][]MutateFunc{segmentID: {fn}}
}

func UpdateStatusOperator(segmentID int64, status commonpb.SegmentState) map[int64][]MutateFunc {
	return singleSegmentMutation(segmentID, func(segment *datapb.SegmentInfo) bool {
		if segment.GetState() == status {
			return false
		}
		segment.State = status
		if status == commonpb.SegmentState_Dropped {
			segment.DroppedAt = uint64(time.Now().UnixNano())
		}
		return true
	})
}

// Add binlogs in segmentInfo
func AddBinlogsOperator(segmentID int64, binlogs, statslogs, deltalogs, bm25logs []*datapb.FieldBinlog) map[int64][]MutateFunc {
	return singleSegmentMutation(segmentID, func(segment *datapb.SegmentInfo) bool {
		segment.Binlogs = mergeFieldBinlogs(segment.GetBinlogs(), binlogs)
		segment.Statslogs = mergeFieldBinlogs(segment.GetStatslogs(), statslogs)
		segment.Deltalogs = mergeFieldBinlogs(segment.GetDeltalogs(), deltalogs)
		segment.Bm25Statslogs = mergeFieldBinlogs(segment.GetBm25Statslogs(), bm25logs)
		segment.Stats = storage.BuildStatsFromFieldBinlogs(
			segment.GetBinlogs(), segment.GetStatslogs(), segment.GetBm25Statslogs(), segment.GetDeltalogs())
		return true
	})
}

// addDeltalogsToSegment merges only previously unseen deltalogs and advances
// the delta-related Statistics fields without rebuilding insert/statistics
// fields that may exist only in a StorageV3 manifest.
func addDeltalogsToSegment(segment *datapb.SegmentInfo, deltalogs []*datapb.FieldBinlog) bool {
	deltalogs = filterDuplicateFieldBinlogs(segment.GetDeltalogs(), deltalogs)
	if len(deltalogs) == 0 {
		return false
	}

	segment.Deltalogs = mergeFieldBinlogs(segment.GetDeltalogs(), deltalogs)
	if segment.Stats == nil {
		segment.Stats = &datapb.Statistics{}
	}
	for _, fieldBinlog := range deltalogs {
		for _, binlog := range fieldBinlog.GetBinlogs() {
			segment.Stats.DeltaBinlogSize += binlog.GetMemorySize()
			segment.Stats.DeleteNumRows += binlog.GetEntriesNum()
			segment.Stats.DeltaBinlogCount++
			if from := binlog.GetTimestampFrom(); from > 0 &&
				(segment.Stats.DeltaTimestampFrom == 0 || from < segment.Stats.DeltaTimestampFrom) {
				segment.Stats.DeltaTimestampFrom = from
			}
			if to := binlog.GetTimestampTo(); to > segment.Stats.DeltaTimestampTo {
				segment.Stats.DeltaTimestampTo = to
			}
		}
	}
	return true
}

func updateManifestPathIfNewer(segment *datapb.SegmentInfo, manifestPath string) (bool, error) {
	if manifestPath == "" || segment.GetManifestPath() == manifestPath {
		return false, nil
	}
	if segment.GetManifestPath() == "" {
		segment.ManifestPath = manifestPath
		return true, nil
	}

	currentBase, currentVersion, err := packed.UnmarshalManifestPath(segment.GetManifestPath())
	if err != nil {
		return false, err
	}
	incomingBase, incomingVersion, err := packed.UnmarshalManifestPath(manifestPath)
	if err != nil {
		return false, err
	}
	if currentBase != incomingBase {
		return false, merr.WrapErrServiceInternalMsg(
			"manifest base path mismatch for segment %d: current %s, incoming %s",
			segment.GetID(), currentBase, incomingBase)
	}
	if incomingVersion <= currentVersion {
		return false, nil
	}
	segment.ManifestPath = manifestPath
	return true, nil
}

func UpdateBinlogsOperator(segmentID int64, binlogs, statslogs, deltalogs, bm25logs []*datapb.FieldBinlog) map[int64][]MutateFunc {
	return singleSegmentMutation(segmentID, func(segment *datapb.SegmentInfo) bool {
		segment.Binlogs = binlogs
		segment.Statslogs = statslogs
		segment.Deltalogs = deltalogs
		segment.Bm25Statslogs = bm25logs
		segment.Stats = storage.BuildStatsFromFieldBinlogs(binlogs, statslogs, bm25logs, deltalogs)
		return true
	})
}

// UpdateSegmentStats stores a producer-provided cumulative Statistics object.
// A nil request is the rolling-upgrade fallback and derives Statistics from
// the segment's cumulative binlog arrays.
func UpdateSegmentStats(segmentID int64, requestStats *datapb.Statistics) map[int64][]MutateFunc {
	return singleSegmentMutation(segmentID, func(segment *datapb.SegmentInfo) bool {
		if requestStats != nil {
			segment.Stats = requestStats
		} else {
			segment.Stats = storage.BuildStatsFromFieldBinlogs(
				segment.GetBinlogs(), segment.GetStatslogs(), segment.GetBm25Statslogs(), segment.GetDeltalogs())
		}
		return true
	})
}

func UpdateBinlogsFromSaveBinlogPathsOperator(segmentID int64, binlogs, statslogs, deltalogs, bm25logs []*datapb.FieldBinlog) map[int64][]MutateFunc {
	return singleSegmentMutation(segmentID, func(segment *datapb.SegmentInfo) bool {
		segment.Binlogs = mergeFieldBinlogs(nil, binlogs)
		segment.Statslogs = mergeFieldBinlogs(nil, statslogs)
		segment.Deltalogs = mergeFieldBinlogs(nil, deltalogs)
		segment.Bm25Statslogs = mergeFieldBinlogs(nil, bm25logs)
		segment.Stats = storage.BuildStatsFromFieldBinlogs(
			segment.GetBinlogs(), segment.GetStatslogs(), segment.GetBm25Statslogs(), segment.GetDeltalogs())
		return true
	})
}

func UpdateStartPosition(positions []*datapb.SegmentStartPosition) map[int64][]MutateFunc {
	mutations := make(map[int64][]MutateFunc, len(positions))
	for _, pos := range positions {
		pos := pos
		mutations[pos.GetSegmentID()] = append(mutations[pos.GetSegmentID()], func(segment *datapb.SegmentInfo) bool {
			segment.StartPosition = pos.GetStartPosition()
			return true
		})
	}
	return mutations
}

func UpdateDeleteApplyStartAfterTimetick(segmentID int64, timetick uint64) map[int64][]MutateFunc {
	return singleSegmentMutation(segmentID, func(segment *datapb.SegmentInfo) bool {
		if timetick == 0 && segment.GetDeleteApplyStartAfterTimetick() != 0 {
			return false
		}
		if timetick == 0 {
			if ts := segment.GetCommitTimestamp(); ts != 0 {
				timetick = ts
			} else if segment.GetStartPosition() != nil {
				timetick = segment.GetStartPosition().GetTimestamp()
			}
		}
		if timetick == 0 || segment.GetDeleteApplyStartAfterTimetick() == timetick {
			return false
		}
		segment.DeleteApplyStartAfterTimetick = timetick
		return true
	})
}

func UpdateDmlPosition(segmentID int64, dmlPosition *msgpb.MsgPosition) map[int64][]MutateFunc {
	return singleSegmentMutation(segmentID, func(segment *datapb.SegmentInfo) bool {
		if len(dmlPosition.GetMsgID()) == 0 {
			return false
		}
		segment.DmlPosition = dmlPosition
		return true
	})
}

// UpdateCheckPointOperator updates segment checkpoint and num rows.
func UpdateCheckPointOperator(segmentID int64, checkpoints []*datapb.CheckPoint, skipDmlPositionCheck ...bool) map[int64][]MutateFunc {
	return singleSegmentMutation(segmentID, func(segment *datapb.SegmentInfo) bool {
		var cpNumRows int64
		for _, cp := range checkpoints {
			if cp.GetSegmentID() != segmentID || cp.GetPosition() == nil {
				continue
			}
			if segment.GetDmlPosition() != nil &&
				segment.GetDmlPosition().GetTimestamp() >= cp.GetPosition().GetTimestamp() &&
				(len(skipDmlPositionCheck) == 0 || !skipDmlPositionCheck[0]) {
				continue
			}
			cpNumRows = cp.GetNumOfRows()
			segment.DmlPosition = cp.GetPosition()
		}

		count := segmentutil.CalcRowCountFromBinLog(segment)
		if count > 0 {
			segment.NumOfRows = count
		} else if cpNumRows > 0 && segment.GetStorageVersion() == storage.StorageV3 {
			// V3 storage: binlogs are empty, use checkpoint's NumOfRows
			segment.NumOfRows = cpNumRows
		}
		return true
	})
}

func UpdateManifest(segmentID int64, manifestPath string) map[int64][]MutateFunc {
	return singleSegmentMutation(segmentID, func(segment *datapb.SegmentInfo) bool {
		if manifestPath == "" || segment.GetManifestPath() == manifestPath {
			return false
		}
		segment.ManifestPath = manifestPath
		return true
	})
}

func UpdateManifestVersion(segmentID int64, manifestVersion int64) map[int64][]MutateFunc {
	return singleSegmentMutation(segmentID, func(segment *datapb.SegmentInfo) bool {
		if segment.GetManifestPath() == "" {
			return false
		}
		basePath, currentVer, err := packed.UnmarshalManifestPath(segment.GetManifestPath())
		if err != nil || currentVer >= manifestVersion {
			return false
		}
		segment.ManifestPath = packed.MarshalManifestPath(basePath, manifestVersion)
		return true
	})
}

func UpdateImportedRows(segmentID int64, rows int64) map[int64][]MutateFunc {
	return singleSegmentMutation(segmentID, func(segment *datapb.SegmentInfo) bool {
		segment.NumOfRows = rows
		segment.MaxRowNum = rows
		return true
	})
}

// ResetImportingSegmentRows clears row counters for importing segments that will be retried.
func ResetImportingSegmentRows(segmentIDs ...int64) map[int64][]MutateFunc {
	mutations := make(map[int64][]MutateFunc, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		segmentID := segmentID
		mutations[segmentID] = []MutateFunc{func(segment *datapb.SegmentInfo) bool {
			if segment.GetState() != commonpb.SegmentState_Importing {
				return false
			}
			segment.NumOfRows = 0
			segment.MaxRowNum = 0
			return true
		}}
	}
	return mutations
}

func UpdateIsImporting(segmentID int64, isImporting bool) map[int64][]MutateFunc {
	return singleSegmentMutation(segmentID, func(segment *datapb.SegmentInfo) bool {
		segment.IsImporting = isImporting
		return true
	})
}

func UpdateCommitTimestamp(segmentID int64, ts uint64) map[int64][]MutateFunc {
	return singleSegmentMutation(segmentID, func(segment *datapb.SegmentInfo) bool {
		if ts != 0 {
			var maxTsTo uint64
			for _, fieldBinlogs := range segment.GetBinlogs() {
				for _, l := range fieldBinlogs.GetBinlogs() {
					if l.GetTimestampTo() > maxTsTo {
						maxTsTo = l.GetTimestampTo()
					}
				}
			}
			if ts < maxTsTo {
				return false
			}
		}
		segment.CommitTimestamp = ts
		if ts != 0 {
			segment.DeleteApplyStartAfterTimetick = ts
		}
		return true
	})
}

func UpdateImportSegmentPosition(segmentID int64, minTs, maxTs uint64) map[int64][]MutateFunc {
	return singleSegmentMutation(segmentID, func(segment *datapb.SegmentInfo) bool {
		channelName := segment.GetInsertChannel()
		segment.StartPosition = &msgpb.MsgPosition{ChannelName: channelName, Timestamp: minTs}
		segment.DmlPosition = &msgpb.MsgPosition{ChannelName: channelName, Timestamp: maxTs}
		return true
	})
}

func UpdateAsDroppedIfEmptyWhenFlushing(segmentID int64) map[int64][]MutateFunc {
	return singleSegmentMutation(segmentID, func(segment *datapb.SegmentInfo) bool {
		if segment.GetLevel() != datapb.SegmentLevel_L0 &&
			segment.GetNumOfRows() == 0 &&
			(segment.GetState() == commonpb.SegmentState_Flushing || segment.GetState() == commonpb.SegmentState_Flushed) {
			segment.State = commonpb.SegmentState_Dropped
			segment.DroppedAt = uint64(time.Now().UnixNano())
			return true
		}
		return false
	})
}

func UpdateSegmentColumnGroupsOperator(segmentID int64, groups map[int64]*datapb.FieldBinlog) map[int64][]MutateFunc {
	return singleSegmentMutation(segmentID, func(segment *datapb.SegmentInfo) bool {
		incomingChildFields := typeutil.NewSet[int64]()
		for _, group := range groups {
			incomingChildFields.Insert(group.GetChildFields()...)
		}

		kept := segment.Binlogs[:0]
		for _, existing := range segment.GetBinlogs() {
			if _, replaced := groups[existing.GetFieldID()]; replaced {
				continue
			}
			if len(existing.GetChildFields()) > 0 {
				existing.ChildFields = lo.Filter(existing.GetChildFields(), func(fieldID int64, _ int) bool {
					return !incomingChildFields.Contain(fieldID)
				})
				if len(existing.GetChildFields()) == 0 {
					continue
				}
			}
			kept = append(kept, existing)
		}
		for _, group := range groups {
			kept = append(kept, group)
		}
		segment.Binlogs = kept
		segment.DataVersion++
		return true
	})
}

// UpdateSegmentsInfo atomically persists mutations to existing segments
// and inserts newSegments. Each segment can have multiple MutateFuncs
// composed in order. If any MutateFunc returns false, that segment's
// update is skipped. All MutateFuncs run inside UpdateFunc against
// the persist value for CAS correctness.
func (m *meta) UpdateSegmentsInfo(ctx context.Context, mutations map[int64][]MutateFunc, newSegments ...*datapb.SegmentInfo) error {
	if len(mutations) == 0 && len(newSegments) == 0 {
		return nil
	}

	start := time.Now()

	txn := m.segmentPersist.Txn(ctx)

	type entry struct {
		segID    int64
		isInsert bool
		newSeg   *SegmentInfo // only for inserts
	}
	var entries []entry

	// Existing segments: run MutateFuncs inside UpdateFunc for CAS
	// Track max UpdateFunc duration (called inside txn.Commit, possibly during retries)
	var maxUpdateFuncNs atomic.Int64
	for segID, fns := range mutations {
		cached := m.segments.GetSegment(segID)
		if cached == nil {
			mlog.Warn(ctx, "meta update: segment not found, skipping",
				zap.Int64("segmentID", segID))
			continue
		}
		key := m.segmentKey(cached.GetCollectionID(), cached.GetPartitionID(), segID)
		funcs := fns // capture
		txn.Update(key, func(existing *datapb.SegmentInfo) (*datapb.SegmentInfo, bool) {
			fnStart := time.Now()
			for _, fn := range funcs {
				if !fn(existing) {
					return existing, false
				}
			}
			dur := time.Since(fnStart).Nanoseconds()
			for {
				cur := maxUpdateFuncNs.Load()
				if dur <= cur || maxUpdateFuncNs.CompareAndSwap(cur, dur) {
					break
				}
			}
			return existing, true
		})
		entries = append(entries, entry{segID: segID})
	}

	buildMutationsDur := time.Since(start)

	// New segments: insert directly
	for _, seg := range newSegments {
		key := m.segmentKey(seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID())
		info := NewSegmentInfo(seg)
		txn.Insert(key, seg)
		entries = append(entries, entry{segID: seg.GetID(), isInsert: true, newSeg: info})
	}

	if len(entries) == 0 {
		return nil
	}

	buildInsertsDur := time.Since(start) - buildMutationsDur

	// Persist to etcd/tikv
	commitStart := time.Now()
	results, err := txn.Commit()
	commitDur := time.Since(commitStart)
	if err != nil {
		mlog.Error(ctx, "meta update: failed to persist segments", zap.Error(err))
		return err
	}

	// Post-persist: update cache + compute metrics
	cacheStart := time.Now()
	metricMutation := &segMetricMutation{
		stateChange: make(segmentMetricStateChange),
	}
	for i, e := range entries {
		if e.isInsert {
			m.segments.SetSegment(e.segID, e.newSeg, results[i].Version)
			metricMutation.addNewSeg(e.newSeg.GetState(), e.newSeg.GetLevel(), e.newSeg.GetIsSorted(), e.newSeg.GetStorageVersion(), segmentMetricFormatLabel(e.newSeg), e.newSeg.GetNumOfRows())
		} else {
			newSeg := NewSegmentInfo(results[i].Value)
			oldSeg, existed := m.segments.SetSegment(e.segID, newSeg, results[i].Version)
			if existed && !sameSegmentMetricLabels(oldSeg, newSeg) {
				metricMutation.appendSegmentLabelChange(oldSeg, newSeg)
			}
		}
	}
	metricMutation.commit()
	cacheDur := time.Since(cacheStart)

	totalDur := time.Since(start)
	if totalDur > 40*time.Millisecond {
		mlog.Info(ctx, "UpdateSegmentsInfo slow",
			zap.Duration("total", totalDur),
			zap.Duration("buildMutations", buildMutationsDur),
			zap.Duration("buildInserts", buildInsertsDur),
			zap.Duration("txnCommit", commitDur),
			zap.Duration("maxUpdateFunc", time.Duration(maxUpdateFuncNs.Load())),
			zap.Duration("updateCache", cacheDur),
			zap.Int("numMutations", len(mutations)),
			zap.Int("numNewSegments", len(newSegments)),
			zap.Int("numEntries", len(entries)))
	}

	return nil
}

// UpdateDropChannelSegmentInfo updates segment checkpoints and binlogs before drop
// reusing segment info to pass segment id, binlogs, statslog, deltalog, start position and checkpoint
func (m *meta) UpdateDropChannelSegmentInfo(ctx context.Context, channel string, segments []*SegmentInfo) error {
	logger := mlog.With()
	logger.Debug(ctx, "meta update: update drop channel segment info",
		zap.String("channel", channel))

	// Build map of segment ID -> drop data for merge segments
	seg2DropMap := make(map[int64]*SegmentInfo)
	for _, seg2Drop := range segments {
		segment := m.segments.GetSegment(seg2Drop.ID)
		if segment == nil || !isSegmentHealthy(segment) {
			logger.Warn(ctx, "UpdateDropChannel skipping nil or unhealthy",
				zap.Bool("is nil", segment == nil),
				zap.Bool("isHealthy", isSegmentHealthy(segment)))
			continue
		}
		seg2DropMap[seg2Drop.GetID()] = seg2Drop
	}

	// Collect all healthy segments on this channel
	type segRef struct {
		id  int64
		key string
	}
	var segRefs []segRef
	for _, seg := range m.segments.GetSegmentsByChannel(channel) {
		segRefs = append(segRefs, segRef{
			id:  seg.GetID(),
			key: m.segmentKey(seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID()),
		})
	}

	if len(segRefs) == 0 {
		// No segments to drop, just mark channel deleted
		if err := m.catalog.Update(ctx, metastore.MarkChannelDropped(channel)); err != nil {
			return err
		}
		return nil
	}

	logger.Info(ctx, "meta update: batch save drop segments",
		zap.Int64s("drop segments", lo.Map(segRefs, func(r segRef, _ int) int64 { return r.id })))

	// Build txn with proper UpdateFunc per segment
	txn := m.segmentPersist.Txn(ctx)
	for _, ref := range segRefs {
		dropData, hasMerge := seg2DropMap[ref.id]
		if hasMerge {
			// Merge segments: apply drop data into persist value inside UpdateFunc
			mergeData := dropData
			txn.Update(ref.key, func(existing *datapb.SegmentInfo) (*datapb.SegmentInfo, bool) {
				existing.State = commonpb.SegmentState_Dropped
				existing.DroppedAt = uint64(time.Now().UnixNano())

				// Merge binlogs
				getFieldBinlogs := func(id UniqueID, binlogs []*datapb.FieldBinlog) *datapb.FieldBinlog {
					for _, binlog := range binlogs {
						if id == binlog.GetFieldID() {
							return binlog
						}
					}
					return nil
				}
				currBinlogs := existing.GetBinlogs()
				for _, tBinlogs := range mergeData.GetBinlogs() {
					fieldBinlogs := getFieldBinlogs(tBinlogs.GetFieldID(), currBinlogs)
					if fieldBinlogs == nil {
						currBinlogs = append(currBinlogs, tBinlogs)
					} else {
						fieldBinlogs.Binlogs = append(fieldBinlogs.Binlogs, tBinlogs.Binlogs...)
					}
				}
				existing.Binlogs = currBinlogs

				// Merge statslogs
				currStatsLogs := existing.GetStatslogs()
				for _, tStatsLogs := range mergeData.GetStatslogs() {
					fieldStatsLog := getFieldBinlogs(tStatsLogs.GetFieldID(), currStatsLogs)
					if fieldStatsLog == nil {
						currStatsLogs = append(currStatsLogs, tStatsLogs)
					} else {
						fieldStatsLog.Binlogs = append(fieldStatsLog.Binlogs, tStatsLogs.Binlogs...)
					}
				}
				existing.Statslogs = currStatsLogs

				// Merge deltalogs
				existing.Deltalogs = append(existing.Deltalogs, mergeData.GetDeltalogs()...)

				// Start position
				if mergeData.GetStartPosition() != nil {
					existing.StartPosition = mergeData.GetStartPosition()
				}
				// Checkpoint
				if mergeData.GetDmlPosition() != nil {
					existing.DmlPosition = mergeData.GetDmlPosition()
				}
				if mergeData.GetDeleteApplyStartAfterTimetick() != 0 {
					existing.DeleteApplyStartAfterTimetick = mergeData.GetDeleteApplyStartAfterTimetick()
				}
				existing.NumOfRows = mergeData.GetNumOfRows()

				return existing, true
			})
		} else {
			// Non-merge segments: just set state to Dropped
			txn.Update(ref.key, func(existing *datapb.SegmentInfo) (*datapb.SegmentInfo, bool) {
				existing.State = commonpb.SegmentState_Dropped
				existing.DroppedAt = uint64(time.Now().UnixNano())
				return existing, true
			})
		}
	}

	results, err := txn.Commit()
	if err != nil {
		logger.Warn(ctx, "meta update: update drop channel segment info failed",
			zap.String("channel", channel),
			zap.Error(err))
		return err
	}

	if err = m.catalog.Update(ctx, metastore.MarkChannelDropped(channel)); err != nil {
		return err
	}

	// Compute metrics and update cache post-persist
	metricMutation := &segMetricMutation{
		stateChange: make(segmentMetricStateChange),
	}
	for i, ref := range segRefs {
		newInfo := NewSegmentInfo(results[i].Value)
		oldSeg, existed := m.segments.SetSegment(ref.id, newInfo, results[i].Version)
		if existed && !sameSegmentMetricLabels(oldSeg, newInfo) {
			metricMutation.appendSegmentLabelChange(oldSeg, newInfo)
		}
	}
	metricMutation.commit()

	logger.Info(ctx, "meta update: update drop channel segment info - complete",
		zap.String("channel", channel))
	return nil
}

// GetSegmentsByChannel returns all segment info which insert channel equals provided `dmlCh`
func (m *meta) GetSegmentsByChannel(channel string) []*SegmentInfo {
	return m.SelectSegments(m.ctx, SegmentFilterFunc(isSegmentHealthy), WithChannel(channel))
}

// GetSegmentsOfCollection get all segments of collection
func (m *meta) GetSegmentsOfCollection(ctx context.Context, collectionID UniqueID) []*SegmentInfo {
	return m.SelectSegments(ctx, SegmentFilterFunc(isSegmentHealthy), WithCollection(collectionID))
}

// GetSegmentsIDOfCollection returns all segment ids which collection equals to provided `collectionID`
func (m *meta) GetSegmentsIDOfCollection(ctx context.Context, collectionID UniqueID) []UniqueID {
	segments := m.SelectSegments(ctx, SegmentFilterFunc(isSegmentHealthy), WithCollection(collectionID))

	return lo.Map(segments, func(segment *SegmentInfo, _ int) int64 {
		return segment.ID
	})
}

// GetSegmentsIDOfCollectionWithDropped returns all dropped segment ids which collection equals to provided `collectionID`
func (m *meta) GetSegmentsIDOfCollectionWithDropped(ctx context.Context, collectionID UniqueID) []UniqueID {
	segments := m.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return segment != nil &&
			segment.GetState() != commonpb.SegmentState_SegmentStateNone &&
			segment.GetState() != commonpb.SegmentState_NotExist
	}))

	return lo.Map(segments, func(segment *SegmentInfo, _ int) int64 {
		return segment.ID
	})
}

// GetSegmentsIDOfPartition returns all segments ids which collection & partition equals to provided `collectionID`, `partitionID`
func (m *meta) GetSegmentsIDOfPartition(ctx context.Context, collectionID, partitionID UniqueID) []UniqueID {
	segments := m.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return isSegmentHealthy(segment) &&
			segment.PartitionID == partitionID
	}))

	return lo.Map(segments, func(segment *SegmentInfo, _ int) int64 {
		return segment.ID
	})
}

// GetSegmentsIDOfPartitionWithDropped returns all dropped segments ids which collection & partition equals to provided `collectionID`, `partitionID`
func (m *meta) GetSegmentsIDOfPartitionWithDropped(ctx context.Context, collectionID, partitionID UniqueID) []UniqueID {
	segments := m.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return segment.GetState() != commonpb.SegmentState_SegmentStateNone &&
			segment.GetState() != commonpb.SegmentState_NotExist &&
			segment.PartitionID == partitionID
	}))

	return lo.Map(segments, func(segment *SegmentInfo, _ int) int64 {
		return segment.ID
	})
}

// GetNumRowsOfPartition returns row count of segments belongs to provided collection & partition
func (m *meta) GetNumRowsOfPartition(ctx context.Context, collectionID UniqueID, partitionID UniqueID) int64 {
	var ret int64
	segments := m.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(func(si *SegmentInfo) bool {
		return isSegmentHealthy(si) && si.GetPartitionID() == partitionID
	}))
	for _, segment := range segments {
		ret += segment.NumOfRows
	}
	return ret
}

// GetUnFlushedSegments get all segments which state is not `Flushing` nor `Flushed`
func (m *meta) GetUnFlushedSegments() []*SegmentInfo {
	return m.SelectSegments(m.ctx, SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return segment.GetState() == commonpb.SegmentState_Growing || segment.GetState() == commonpb.SegmentState_Sealed
	}))
}

// GetFlushingSegments get all segments which state is `Flushing`
func (m *meta) GetFlushingSegments() []*SegmentInfo {
	return m.SelectSegments(m.ctx, SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return segment.GetState() == commonpb.SegmentState_Flushing
	}))
}

// SelectSegments select segments with selector
func (m *meta) SelectSegments(ctx context.Context, filters ...SegmentFilter) []*SegmentInfo {

	return m.segments.GetSegmentsBySelector(filters...)
}

func (m *meta) GetCollectionIDsByPartition(ctx context.Context, partitionIDs []int64) []int64 {
	partitions := make(map[int64]struct{}, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		partitions[partitionID] = struct{}{}
	}
	collections := make(map[int64]struct{})
	for _, collection := range m.GetCollections() {
		for _, partitionID := range collection.Partitions {
			if _, ok := partitions[partitionID]; ok {
				collections[collection.ID] = struct{}{}
				break
			}
		}
	}
	for _, segment := range m.SelectSegments(ctx, SegmentFilterFunc(func(segment *SegmentInfo) bool {
		_, ok := partitions[segment.GetPartitionID()]
		return ok && segment.GetCollectionID() != 0
	})) {
		collections[segment.GetCollectionID()] = struct{}{}
	}
	collectionIDs := lo.Keys(collections)
	sort.Slice(collectionIDs, func(i, j int) bool { return collectionIDs[i] < collectionIDs[j] })
	return collectionIDs
}

func (m *meta) GetRealSegmentsForChannel(channel string) []*SegmentInfo {

	return m.segments.GetRealSegmentsForChannel(channel)
}

// AddAllocation add allocation in segment
func (m *meta) AddAllocation(segmentID UniqueID, allocation *Allocation) error {
	mlog.Debug(m.ctx, "meta update: add allocation",
		zap.Int64("segmentID", segmentID),
		zap.Any("allocation", allocation))

	curSegInfo := m.segments.GetSegment(segmentID)
	if curSegInfo == nil {
		// TODO: Error handling.
		mlog.Error(m.ctx, "meta update: add allocation failed - segment not found", zap.Int64("segmentID", segmentID))
		return errors.New("meta update: add allocation failed - segment not found")
	}
	// As we use global segment lastExpire to guarantee data correctness after restart
	// there is no need to persist allocation to meta store, only update allocation in-memory meta.
	m.segments.AddAllocation(segmentID, allocation)
	mlog.Info(m.ctx, "meta update: add allocation - complete", zap.Int64("segmentID", segmentID))
	return nil
}

func (m *meta) SetRowCount(segmentID UniqueID, rowCount int64) {

	m.segments.SetRowCount(segmentID, rowCount)
}

// SetAllocations set Segment allocations, will overwrite ALL original allocations
// Note that allocations is not persisted in KV store
func (m *meta) SetAllocations(segmentID UniqueID, allocations []*Allocation) {

	m.segments.SetAllocations(segmentID, allocations)
}

// SetLastExpire set lastExpire time for segment
// Note that last is not necessary to store in KV meta
func (m *meta) SetLastExpire(segmentID UniqueID, lastExpire uint64) {
	m.segments.SetLastExpire(segmentID, lastExpire)
}

// SetLastFlushTime set LastFlushTime for segment with provided `segmentID`
// Note that lastFlushTime is not persisted in KV store
func (m *meta) SetLastFlushTime(segmentID UniqueID, t time.Time) {

	m.segments.SetFlushTime(segmentID, t)
}

// SetLastWrittenTime set LastWrittenTime for segment with provided `segmentID`
// Note that lastWrittenTime is not persisted in KV store
func (m *meta) SetLastWrittenTime(segmentID UniqueID) {

	m.segments.SetLastWrittenTime(segmentID)
}

// SetSegmentCompacting sets compaction state for segment
func (m *meta) SetSegmentCompacting(segmentID UniqueID, compacting bool) {

	m.segments.SetIsCompacting(segmentID, compacting)
}

// IsSegmentCompacting check if segment is compacting
func (m *meta) IsSegmentCompacting(segmentID UniqueID) bool {

	seg := m.segments.GetSegment(segmentID)
	if seg == nil {
		return false
	}
	return seg.isCompacting
}

// CheckAndSetSegmentsCompacting check all segments are not compacting
// if true, set them compacting and return true
// if false, skip setting and
func (m *meta) CheckAndSetSegmentsCompacting(ctx context.Context, segmentIDs []UniqueID) (exist, canDo bool) {

	var hasCompacting bool
	exist = true
	for _, segmentID := range segmentIDs {
		seg := m.segments.GetSegment(segmentID)
		if seg != nil {
			if seg.isCompacting {
				hasCompacting = true
			}
		} else {
			exist = false
			break
		}
	}
	canDo = exist && !hasCompacting
	if canDo {
		for _, segmentID := range segmentIDs {
			m.segments.SetIsCompacting(segmentID, true)
		}
	}
	return exist, canDo
}

func (m *meta) SetSegmentsCompacting(ctx context.Context, segmentIDs []UniqueID, compacting bool) {

	for _, segmentID := range segmentIDs {
		m.segments.SetIsCompacting(segmentID, compacting)
	}
}

func getMinPosition(positions []*msgpb.MsgPosition) *msgpb.MsgPosition {
	var minPos *msgpb.MsgPosition
	for _, pos := range positions {
		if minPos == nil ||
			pos != nil && pos.GetTimestamp() < minPos.GetTimestamp() {
			minPos = pos
		}
	}
	return minPos
}

func getMaxPosition(positions []*msgpb.MsgPosition) *msgpb.MsgPosition {
	var maxPos *msgpb.MsgPosition
	for _, pos := range positions {
		if maxPos == nil ||
			pos != nil && pos.GetTimestamp() > maxPos.GetTimestamp() {
			maxPos = pos
		}
	}
	return maxPos
}

func recalculateSegmentPosition(binlogs []*datapb.FieldBinlog, channel string, fallbackStart, fallbackDml *msgpb.MsgPosition) (startPos, dmlPos *msgpb.MsgPosition) {
	stats := storage.BuildStatsFromFieldBinlogs(binlogs, nil, nil, nil)
	minTs, maxTs := stats.GetTimestampFrom(), stats.GetTimestampTo()
	if minTs > 0 && maxTs > 0 {
		return &msgpb.MsgPosition{
				ChannelName: channel,
				Timestamp:   minTs,
			}, &msgpb.MsgPosition{
				ChannelName: channel,
				Timestamp:   maxTs,
			}
	}
	return fallbackStart, fallbackDml
}

// normalizePositionTimestamp updates a position's timestamp to commitTs when
// compaction has already rewritten import row timestamps to the commit fence.
func normalizePositionTimestamp(pos *msgpb.MsgPosition, commitTs uint64) *msgpb.MsgPosition {
	if commitTs == 0 || pos == nil || pos.GetTimestamp() >= commitTs {
		return pos
	}
	return &msgpb.MsgPosition{
		ChannelName: pos.GetChannelName(),
		MsgID:       pos.GetMsgID(),
		Timestamp:   commitTs,
	}
}

func maxCommitTimestamp(compactFromSegInfos []*SegmentInfo) uint64 {
	var maxCommitTs uint64
	for _, info := range compactFromSegInfos {
		maxCommitTs = max(maxCommitTs, info.GetCommitTimestamp())
	}
	return maxCommitTs
}

func getCompactionFallbackPositions(compactFromSegInfos []*SegmentInfo) (fallbackStart, fallbackDml *msgpb.MsgPosition) {
	maxCommitTs := maxCommitTimestamp(compactFromSegInfos)
	fallbackStart = getMinPosition(lo.Map(compactFromSegInfos, func(info *SegmentInfo, _ int) *msgpb.MsgPosition {
		return info.GetStartPosition()
	}))
	fallbackDml = normalizePositionTimestamp(getMaxPosition(lo.Map(compactFromSegInfos, func(info *SegmentInfo, _ int) *msgpb.MsgPosition {
		return info.GetDmlPosition()
	})), maxCommitTs)
	return fallbackStart, fallbackDml
}

func (m *meta) completeClusterCompactionMutation(t *datapb.CompactionTask, result *datapb.CompactionPlanResult) ([]*SegmentInfo, *segMetricMutation, error) {
	logger := mlog.With(zap.Int64("planID", t.GetPlanID()),
		zap.String("type", t.GetType().String()),
		zap.Int64("collectionID", t.CollectionID),
		zap.Int64("partitionID", t.PartitionID),
		zap.String("channel", t.GetChannel()))

	metricMutation := &segMetricMutation{stateChange: make(segmentMetricStateChange)}
	compactFromSegIDs := make([]int64, 0)
	compactToSegIDs := make([]int64, 0)
	compactFromSegInfos := make([]*SegmentInfo, 0)
	compactToSegInfos := make([]*SegmentInfo, 0)

	for _, segmentID := range t.GetInputSegments() {
		segment := m.segments.GetSegment(segmentID)
		if segment == nil {
			return nil, nil, merr.WrapErrSegmentNotFound(segmentID)
		}

		// Re-validate segment health to prevent race condition with drop collection
		// between ValidateSegmentStateBeforeCompleteCompactionMutation and here
		if !isSegmentHealthy(segment) {
			logger.Warn(context.TODO(), "input segment was dropped during compaction mutation",
				zap.Int64("planID", t.GetPlanID()),
				zap.Int64("segmentID", segmentID),
				zap.String("state", segment.GetState().String()))
			return nil, nil, merr.WrapErrSegmentNotFound(segmentID, "input segment was dropped")
		}

		cloned := segment.Clone()

		compactFromSegInfos = append(compactFromSegInfos, cloned)
		compactFromSegIDs = append(compactFromSegIDs, cloned.GetID())
	}

	fallbackStart, fallbackDml := getCompactionFallbackPositions(compactFromSegInfos)
	deleteApplyStartAfterTimetick := minSegmentDeleteApplyStartAfterTimetick(compactFromSegInfos)

	for _, seg := range result.GetSegments() {
		startPos, dmlPos := recalculateSegmentPosition(seg.GetInsertLogs(), t.GetChannel(), fallbackStart, fallbackDml)
		segmentInfo := &datapb.SegmentInfo{
			ID:                  seg.GetSegmentID(),
			CollectionID:        compactFromSegInfos[0].CollectionID,
			PartitionID:         compactFromSegInfos[0].PartitionID,
			InsertChannel:       t.GetChannel(),
			NumOfRows:           seg.NumOfRows,
			State:               commonpb.SegmentState_Flushed,
			MaxRowNum:           compactFromSegInfos[0].MaxRowNum,
			Binlogs:             seg.GetInsertLogs(),
			Statslogs:           seg.GetField2StatslogPaths(),
			CreatedByCompaction: true,
			CompactionFrom:      compactFromSegIDs,
			LastExpireTime:      tsoutil.ComposeTSByTime(time.Unix(t.GetStartTime(), 0)),
			Level:               datapb.SegmentLevel_L2,
			StartPosition:       startPos,
			DmlPosition:         dmlPos,
			// visible after stats and index
			IsInvisible:                   true,
			StorageVersion:                seg.GetStorageVersion(),
			ManifestPath:                  seg.GetManifest(),
			ExpirQuantiles:                seg.GetExpirQuantiles(),
			SchemaVersion:                 t.GetSchema().GetVersion(),
			CommitTimestamp:               0, // Normalized: row timestamps already rewritten
			DeleteApplyStartAfterTimetick: deleteApplyStartAfterTimetick,
		}
		// Statistics is computed at the compactor and shipped on the
		// CompactionSegment. V3 outputs whose stats live in the manifest
		// are populated correctly there (the compactor sees the stats
		// blob size); the receiver does not recompute.
		segmentInfo.Stats = seg.GetStats()
		segment := NewSegmentInfo(segmentInfo)
		compactToSegInfos = append(compactToSegInfos, segment)
		compactToSegIDs = append(compactToSegIDs, segment.GetID())
		metricMutation.addNewSeg(segment.GetState(), segment.GetLevel(), segment.GetIsSorted(), segment.GetStorageVersion(), segmentMetricFormatLabel(segment), segment.GetNumOfRows())
	}

	logger = logger.With(zap.Int64s("compact from", compactFromSegIDs), zap.Int64s("compact to", compactToSegIDs))
	logger.Debug(context.TODO(), "meta update: prepare for meta mutation - complete")

	// Persist new compactTo segments
	txn := m.segmentPersist.Txn(m.ctx)
	for _, info := range compactToSegInfos {
		txn.Insert(m.segmentKey(info.GetCollectionID(), info.GetPartitionID(), info.GetID()), info.SegmentInfo)
	}
	results, err := txn.Commit()
	if err != nil {
		logger.Warn(context.TODO(), "fail to alter compactTo segments", zap.Error(err))
		return nil, nil, err
	}
	for i, info := range compactToSegInfos {
		m.segments.SetSegment(info.GetID(), info, results[i].Version)
	}
	logger.Info(context.TODO(), "meta update: alter in memory meta after compaction - complete")
	return compactToSegInfos, metricMutation, nil
}

func (m *meta) completeMixCompactionMutation(
	t *datapb.CompactionTask,
	result *datapb.CompactionPlanResult,
) ([]*SegmentInfo, *segMetricMutation, error) {
	logger := mlog.With(zap.Int64("planID", t.GetPlanID()),
		zap.String("type", t.GetType().String()),
		zap.Int64("collectionID", t.CollectionID),
		zap.Int64("partitionID", t.PartitionID),
		zap.String("channel", t.GetChannel()),
		zap.Int64("planID", t.GetPlanID()),
	)

	metricMutation := &segMetricMutation{stateChange: make(segmentMetricStateChange)}
	// Read compactFrom segments from cache (read-only for validation and new segment construction).
	var compactFromSegIDs []int64
	var compactFromCached []*SegmentInfo
	for _, segmentID := range t.GetInputSegments() {
		segment := m.segments.GetSegment(segmentID)
		if segment == nil {
			return nil, nil, merr.WrapErrSegmentNotFound(segmentID)
		}

		// Re-validate segment health to prevent race condition with drop collection
		// between ValidateSegmentStateBeforeCompleteCompactionMutation and here
		if !isSegmentHealthy(segment) {
			logger.Warn(context.TODO(), "input segment was dropped during compaction mutation",
				zap.Int64("planID", t.GetPlanID()),
				zap.Int64("segmentID", segmentID),
				zap.String("state", segment.GetState().String()))
			return nil, nil, merr.WrapErrSegmentNotFound(segmentID, "input segment was dropped")
		}

		compactFromCached = append(compactFromCached, segment)
		compactFromSegIDs = append(compactFromSegIDs, segmentID)

		logger.Info(context.TODO(), "compact from segment",
			zap.Int64("segmentID", segmentID),
			zap.Int64("segment size", segment.getSegmentSize()),
			zap.Int64("num rows", segment.GetNumOfRows()),
		)
	}

	logger = logger.With(zap.Int64s("compactFrom", compactFromSegIDs))

	if t.GetSchema() == nil {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("mix compaction task schema is nil")
	}
	outputSchemaVersion := t.GetSchema().GetVersion()

	fallbackStart, fallbackDml := getCompactionFallbackPositions(compactFromCached)
	deleteApplyStartAfterTimetick := minSegmentDeleteApplyStartAfterTimetick(compactFromCached)

	compactToSegments := make([]*SegmentInfo, 0)
	for _, compactToSegment := range result.GetSegments() {
		startPos, dmlPos := recalculateSegmentPosition(compactToSegment.GetInsertLogs(), t.GetChannel(), fallbackStart, fallbackDml)
		compactToProto := &datapb.SegmentInfo{
			ID:            compactToSegment.GetSegmentID(),
			CollectionID:  compactFromCached[0].CollectionID,
			PartitionID:   compactFromCached[0].PartitionID,
			InsertChannel: t.GetChannel(),
			NumOfRows:     compactToSegment.NumOfRows,
			State:         commonpb.SegmentState_Flushed,
			MaxRowNum:     compactFromCached[0].MaxRowNum,
			Binlogs:       compactToSegment.GetInsertLogs(),
			Statslogs:     compactToSegment.GetField2StatslogPaths(),
			Deltalogs:     compactToSegment.GetDeltalogs(),
			Bm25Statslogs: compactToSegment.GetBm25Logs(),
			TextStatsLogs: compactToSegment.GetTextStatsLogs(),

			CreatedByCompaction:           true,
			CompactionFrom:                compactFromSegIDs,
			LastExpireTime:                tsoutil.ComposeTSByTime(time.Unix(t.GetStartTime(), 0)),
			Level:                         datapb.SegmentLevel_L1,
			StorageVersion:                compactToSegment.GetStorageVersion(),
			StartPosition:                 startPos,
			DmlPosition:                   dmlPos,
			IsSorted:                      compactToSegment.GetIsSorted(),
			ManifestPath:                  compactToSegment.GetManifest(),
			IsSortedByNamespace:           compactToSegment.GetIsSortedByNamespace(),
			ExpirQuantiles:                compactToSegment.GetExpirQuantiles(),
			SchemaVersion:                 outputSchemaVersion,
			CommitTimestamp:               0, // Normalized: row timestamps already rewritten
			DeleteApplyStartAfterTimetick: deleteApplyStartAfterTimetick,
		}
		// Statistics is computed at the compactor and shipped on the
		// CompactionSegment. V3 outputs whose stats live in the manifest
		// are populated correctly there; the receiver does not recompute.
		compactToProto.Stats = compactToSegment.GetStats()
		compactToSegmentInfo := NewSegmentInfo(compactToProto)

		if compactToSegmentInfo.GetNumOfRows() == 0 {
			compactToSegmentInfo.State = commonpb.SegmentState_Dropped
		}

		// metrics mutation for compactTo segments
		metricMutation.addNewSeg(compactToSegmentInfo.GetState(), compactToSegmentInfo.GetLevel(), compactToSegmentInfo.GetIsSorted(), compactToSegmentInfo.GetStorageVersion(), segmentMetricFormatLabel(compactToSegmentInfo), compactToSegmentInfo.GetNumOfRows())

		logger.Info(context.TODO(), "Add a new compactTo segment",
			zap.Int64("compactTo", compactToSegmentInfo.GetID()),
			zap.Int64("compactTo segment numRows", compactToSegmentInfo.GetNumOfRows()),
			zap.Int("binlog count", len(compactToSegmentInfo.GetBinlogs())),
			zap.Int("statslog count", len(compactToSegmentInfo.GetStatslogs())),
			zap.Int("deltalog count", len(compactToSegmentInfo.GetDeltalogs())),
			zap.Int64("segment size", compactToSegmentInfo.getSegmentSize()),
			zap.Int64s("expirQuantiles", compactToSegmentInfo.GetExpirQuantiles()),
		)
		compactToSegments = append(compactToSegments, compactToSegmentInfo)
	}

	logger.Debug(context.TODO(), "meta update: prepare for meta mutation - complete")

	// Persist all segments atomically in one transaction
	txn := m.segmentPersist.Txn(m.ctx)
	for _, info := range compactToSegments {
		txn.Insert(m.segmentKey(info.GetCollectionID(), info.GetPartitionID(), info.GetID()), info.SegmentInfo)
	}
	for _, seg := range compactFromCached {
		txn.Update(m.segmentKey(seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID()), func(existing *datapb.SegmentInfo) (*datapb.SegmentInfo, bool) {
			existing.State = commonpb.SegmentState_Dropped
			existing.DroppedAt = uint64(time.Now().UnixNano())
			existing.Compacted = true
			return existing, true
		})
	}
	results, err := txn.Commit()
	if err != nil {
		logger.Warn(context.TODO(), "fail to alter segments for compaction", zap.Error(err))
		return nil, nil, err
	}
	toCount := len(compactToSegments)
	for i, info := range compactToSegments {
		m.segments.SetSegment(info.GetID(), info, results[i].Version)
	}
	for i, seg := range compactFromCached {
		newInfo := NewSegmentInfo(results[toCount+i].Value)
		old, existed := m.segments.SetSegment(seg.GetID(), newInfo, results[toCount+i].Version)
		if existed && !sameSegmentMetricLabels(old, newInfo) {
			metricMutation.appendSegmentLabelChange(old, newInfo)
		}
	}

	logger.Info(context.TODO(), "meta update: alter in memory meta after compaction - complete")
	return compactToSegments, metricMutation, nil
}

func (m *meta) ValidateSegmentStateBeforeCompleteCompactionMutation(t *datapb.CompactionTask) error {

	if t.GetType() != datapb.CompactionType_Level0DeleteCompaction {
		if m.isCollectionCompactionBlocked(t.GetCollectionID()) {
			mlog.Info(context.TODO(), "compaction rejected: collection has pending snapshot or unloaded RefIndex",
				zap.Int64("planID", t.GetPlanID()),
				zap.String("type", t.GetType().String()),
				zap.Int64("collectionID", t.GetCollectionID()),
				zap.String("channel", t.GetChannel()),
				zap.Int64s("inputSegments", t.GetInputSegments()))
			return merr.WrapErrCompactionBlocked(
				fmt.Sprintf("collection %d has pending snapshot or unloaded snapshot RefIndex", t.GetCollectionID()))
		}

		for _, segmentID := range t.GetInputSegments() {
			if m.isSegmentCompactionProtected(segmentID) {
				mlog.Info(context.TODO(), "compaction rejected: input segment is protected by snapshot",
					zap.Int64("planID", t.GetPlanID()),
					zap.String("type", t.GetType().String()),
					zap.Int64("collectionID", t.GetCollectionID()),
					zap.String("channel", t.GetChannel()),
					zap.Int64("segmentID", segmentID),
					zap.Int64s("inputSegments", t.GetInputSegments()))
				return merr.WrapErrCompactionBlocked(
					fmt.Sprintf("input segment %d is protected by a snapshot", segmentID))
			}
		}
	}

	for _, segmentID := range t.GetInputSegments() {
		segment := m.segments.GetSegment(segmentID)
		if !isSegmentHealthy(segment) {
			// SHOULD NOT HAPPEN: input segment was dropped.
			// This indicates that compaction tasks, which should be mutually exclusive,
			// may have executed concurrently.
			mlog.Warn(context.TODO(), "should not happen! input segment was dropped",
				zap.Int64("planID", t.GetPlanID()),
				zap.String("type", t.GetType().String()),
				zap.String("channel", t.GetChannel()),
				zap.Int64("partitionID", t.GetPartitionID()),
				zap.Int64("segmentID", segmentID),
			)
			return merr.WrapErrSegmentNotFound(segmentID, "input segment was dropped")
		}
	}
	return nil
}

func (m *meta) CompleteCompactionMutation(ctx context.Context, t *datapb.CompactionTask, result *datapb.CompactionPlanResult) ([]*SegmentInfo, *segMetricMutation, error) {
	var (
		newSegments    []*SegmentInfo
		metricMutation *segMetricMutation
		err            error
	)
	switch t.GetType() {
	case datapb.CompactionType_MixCompaction:
		newSegments, metricMutation, err = m.completeMixCompactionMutation(t, result)
	case datapb.CompactionType_ClusteringCompaction:
		newSegments, metricMutation, err = m.completeClusterCompactionMutation(t, result)
	case datapb.CompactionType_SortCompaction:
		newSegments, metricMutation, err = m.completeSortCompactionMutation(t, result)
	case datapb.CompactionType_BumpSchemaVersionCompaction:
		newSegments, metricMutation, err = m.completeBumpSchemaVersionCompactionMutation(t, result)
	default:
		err = merr.WrapErrIllegalCompactionPlan("illegal compaction type")
	}
	if err != nil {
		return nil, nil, err
	}
	m.publishDataViewAfterCompaction(ctx, t, lo.Map(newSegments, func(segment *SegmentInfo, _ int) int64 {
		return segment.GetID()
	}))
	return newSegments, metricMutation, nil
}

func (m *meta) publishDataViewAfterCompaction(ctx context.Context, t *datapb.CompactionTask, compactTo []int64) {
	if m.dataViewManager == nil {
		return
	}
	if _, err := m.dataViewManager.OnCompact(ctx, CompactDataViewEvent{
		CollectionID: t.GetCollectionID(),
		CompactFrom:  t.GetInputSegments(),
		CompactTo:    compactTo,
	}); err != nil {
		mlog.Warn(ctx, "failed to publish DataView after compaction",
			mlog.Int64("planID", t.GetPlanID()),
			mlog.FieldCollectionID(t.GetCollectionID()),
			mlog.Int64s("compactFrom", t.GetInputSegments()),
			mlog.Int64s("compactTo", compactTo),
			mlog.Err(err))
	}
}

// buildSegment utility function for compose datapb.SegmentInfo struct with provided info
func buildSegment(collectionID UniqueID, partitionID UniqueID, segmentID UniqueID, channelName string) *SegmentInfo {
	info := &datapb.SegmentInfo{
		ID:            segmentID,
		CollectionID:  collectionID,
		PartitionID:   partitionID,
		InsertChannel: channelName,
		NumOfRows:     0,
		State:         commonpb.SegmentState_Growing,
	}
	return NewSegmentInfo(info)
}

func isSegmentHealthy(segment *SegmentInfo) bool {
	return segment != nil &&
		segment.GetState() != commonpb.SegmentState_SegmentStateNone &&
		segment.GetState() != commonpb.SegmentState_NotExist &&
		segment.GetState() != commonpb.SegmentState_Dropped
}

func (m *meta) HasSegments(segIDs []UniqueID) (bool, error) {

	for _, segID := range segIDs {
		if m.segments.GetSegment(segID) == nil {
			return false, fmt.Errorf("segment is not exist with ID = %d", segID)
		}
	}
	return true, nil
}

// GetCompactionTo returns the segment info of the segment to be compacted to.
func (m *meta) GetCompactionTo(segmentID int64) ([]*SegmentInfo, bool) {

	return m.segments.GetCompactionTo(segmentID)
}

// GetMinGrowingSegmentCheckpoint returns the minimum DmlPosition of all growing
// non-L0 segments on the given channel that belong to TEXT collections.
func (m *meta) GetMinGrowingSegmentCheckpoint(channel string) *msgpb.MsgPosition {
	segments := m.SelectSegments(context.TODO(), WithChannel(channel))
	textCollectionCache := make(map[int64]bool)

	var minPos *msgpb.MsgPosition
	for _, s := range segments {
		if s.GetState() != commonpb.SegmentState_Growing || s.GetLevel() == datapb.SegmentLevel_L0 {
			continue
		}

		collID := s.GetCollectionID()
		isText, cached := textCollectionCache[collID]
		if !cached {
			isText = m.collectionHasTextFields(collID)
			textCollectionCache[collID] = isText
		}
		if !isText {
			continue
		}

		pos := s.GetDmlPosition()
		if pos == nil {
			pos = s.GetStartPosition()
		}
		if pos == nil {
			continue
		}
		if minPos == nil || pos.GetTimestamp() < minPos.GetTimestamp() {
			minPos = pos
		}
	}
	return minPos
}

func (m *meta) collectionHasTextFields(collectionID int64) bool {
	coll := m.GetCollection(collectionID)
	if coll == nil || coll.Schema == nil {
		return false
	}
	for _, field := range coll.Schema.GetFields() {
		if field.GetDataType() == schemapb.DataType_Text {
			return true
		}
	}
	return false
}

const (
	updateChannelCheckpointStageTotal                   = "total"
	updateChannelCheckpointStageValidate                = "validate"
	updateChannelCheckpointStageGetMinGrowingCheckpoint = "get_min_growing_checkpoint"
	updateChannelCheckpointStageLockWait                = "lock_wait"
	updateChannelCheckpointStageCheckUpdateNeeded       = "check_update_needed"
	updateChannelCheckpointStageSaveChannelCheckpoint   = "save_channel_checkpoint"
	updateChannelCheckpointStageUpdateMemory            = "update_memory"
	updateChannelCheckpointStageUpdateCheckpointMetric  = "update_checkpoint_metric"
)

func observeUpdateChannelCheckpointStage(stage string, start time.Time) {
	metrics.DataCoordUpdateChannelCheckpointStageDuration.WithLabelValues(stage).Observe(float64(time.Since(start).Microseconds()) / 1000.0)
}

// UpdateChannelCheckpoint updates and saves channel checkpoint.
func (m *meta) UpdateChannelCheckpoint(ctx context.Context, vChannel string, pos *msgpb.MsgPosition) error {
	totalStart := time.Now()
	defer observeUpdateChannelCheckpointStage(updateChannelCheckpointStageTotal, totalStart)

	stageStart := time.Now()
	if pos == nil || pos.GetMsgID() == nil {
		observeUpdateChannelCheckpointStage(updateChannelCheckpointStageValidate, stageStart)
		return merr.WrapErrServiceInternalMsg("channelCP is nil, vChannel=%s", vChannel)
	}
	observeUpdateChannelCheckpointStage(updateChannelCheckpointStageValidate, stageStart)

	stageStart = time.Now()
	minGrowingCP := m.GetMinGrowingSegmentCheckpoint(vChannel)
	observeUpdateChannelCheckpointStage(updateChannelCheckpointStageGetMinGrowingCheckpoint, stageStart)
	if minGrowingCP != nil && pos.GetTimestamp() > minGrowingCP.GetTimestamp() {
		mlog.Info(ctx, "clamping channel checkpoint to min growing segment checkpoint",
			mlog.String("vChannel", vChannel),
			mlog.Uint64("requestedTs", pos.GetTimestamp()),
			mlog.Uint64("clampedTs", minGrowingCP.GetTimestamp()))
		pos = minGrowingCP
	}

	stageStart = time.Now()
	m.channelCPs.lockChannel(vChannel)
	observeUpdateChannelCheckpointStage(updateChannelCheckpointStageLockWait, stageStart)
	defer m.channelCPs.unlockChannel(vChannel)

	stageStart = time.Now()
	m.channelCPs.RLock()
	oldPosition, ok := m.channelCPs.checkpoints[vChannel]
	m.channelCPs.RUnlock()
	needUpdate := !ok || oldPosition.Timestamp < pos.Timestamp || (oldPosition.Timestamp == pos.Timestamp && !bytes.Equal(oldPosition.MsgID, pos.MsgID))
	observeUpdateChannelCheckpointStage(updateChannelCheckpointStageCheckUpdateNeeded, stageStart)
	if needUpdate {
		stageStart = time.Now()
		err := m.catalog.SaveChannelCheckpoint(ctx, vChannel, pos)
		observeUpdateChannelCheckpointStage(updateChannelCheckpointStageSaveChannelCheckpoint, stageStart)
		if err != nil {
			return err
		}
		stageStart = time.Now()
		m.channelCPs.Lock()
		m.channelCPs.checkpoints[vChannel] = pos
		m.channelCPs.cond.UnsafeBroadcast()
		m.channelCPs.Unlock()
		observeUpdateChannelCheckpointStage(updateChannelCheckpointStageUpdateMemory, stageStart)
		stageStart = time.Now()
		ts, _ := tsoutil.ParseTS(pos.Timestamp)
		mlog.Info(ctx, "UpdateChannelCheckpoint done",
			mlog.String("vChannel", vChannel),
			mlog.Uint64("ts", pos.GetTimestamp()),
			mlog.ByteString("msgID", pos.GetMsgID()),
			mlog.Stringer("walName", pos.WALName),
			mlog.Time("time", ts))
		metrics.DataCoordCheckpointUnixSeconds.WithLabelValues(paramtable.GetStringNodeID(), vChannel).
			Set(float64(ts.Unix()))
		observeUpdateChannelCheckpointStage(updateChannelCheckpointStageUpdateCheckpointMetric, stageStart)
	}
	return nil
}

// MarkChannelCheckpointDropped set channel checkpoint to MaxUint64 preventing future update
// and remove the metrics for channel checkpoint lag.
func (m *meta) MarkChannelCheckpointDropped(ctx context.Context, channel string) error {
	m.channelCPs.lockChannel(channel)
	defer m.channelCPs.unlockChannel(channel)

	cp := &msgpb.MsgPosition{
		ChannelName: channel,
		Timestamp:   funcutil.DroppedChannelCheckpointTimestamp,
	}

	err := m.catalog.SaveChannelCheckpoints(ctx, []*msgpb.MsgPosition{cp})
	if err != nil {
		return err
	}

	m.channelCPs.Lock()
	m.channelCPs.checkpoints[channel] = cp
	m.channelCPs.cond.UnsafeBroadcast()
	m.channelCPs.Unlock()

	metrics.DataCoordCheckpointUnixSeconds.DeleteLabelValues(paramtable.GetStringNodeID(), channel)
	return nil
}

// UpdateChannelCheckpoints updates and saves channel checkpoints.
func (m *meta) UpdateChannelCheckpoints(ctx context.Context, positions []*msgpb.MsgPosition) error {
	logger := mlog.With()
	for i, pos := range positions {
		if pos == nil || pos.GetChannelName() == "" {
			continue
		}
		minGrowingCP := m.GetMinGrowingSegmentCheckpoint(pos.GetChannelName())
		if minGrowingCP != nil && pos.GetTimestamp() > minGrowingCP.GetTimestamp() {
			logger.Info(ctx, "clamping channel checkpoint to min growing segment checkpoint",
				zap.String("vChannel", pos.GetChannelName()),
				zap.Uint64("requestedTs", pos.GetTimestamp()),
				zap.Uint64("clampedTs", minGrowingCP.GetTimestamp()))
			positions[i] = minGrowingCP
		}
	}

	validPositions := lo.Filter(positions, func(pos *msgpb.MsgPosition, _ int) bool {
		if pos == nil || (pos.GetMsgID() == nil && pos.GetWALName() != commonpb.WALName_WoodPecker) || pos.GetChannelName() == "" {
			logger.Warn(ctx, "illegal channel cp", zap.Any("pos", pos))
			return false
		}
		return true
	})
	channels := lo.Map(validPositions, func(pos *msgpb.MsgPosition, _ int) string {
		return pos.GetChannelName()
	})
	lockedChannels := m.channelCPs.lockChannels(channels)
	defer m.channelCPs.unlockChannels(lockedChannels)

	m.channelCPs.RLock()
	toUpdates := lo.Filter(validPositions, func(pos *msgpb.MsgPosition, _ int) bool {
		vChannel := pos.GetChannelName()
		oldPosition, ok := m.channelCPs.checkpoints[vChannel]
		return !ok || oldPosition.Timestamp < pos.Timestamp || (oldPosition.Timestamp == pos.Timestamp && !bytes.Equal(oldPosition.MsgID, pos.MsgID))
	})
	m.channelCPs.RUnlock()
	if len(toUpdates) == 0 {
		return nil
	}
	err := m.catalog.SaveChannelCheckpoints(ctx, toUpdates)
	if err != nil {
		return err
	}
	m.channelCPs.Lock()
	for _, pos := range toUpdates {
		channel := pos.GetChannelName()
		m.channelCPs.checkpoints[channel] = pos
	}
	// broadcast the change of channel checkpoint for TruncateCollection op to drop segments
	m.channelCPs.cond.UnsafeBroadcast()
	m.channelCPs.Unlock()
	for _, pos := range toUpdates {
		channel := pos.GetChannelName()
		mlog.Info(context.TODO(), "UpdateChannelCheckpoint done", mlog.String("channel", channel),
			mlog.Stringer("walName", pos.WALName),
			mlog.Uint64("ts", pos.GetTimestamp()),
			mlog.Time("time", tsoutil.PhysicalTime(pos.GetTimestamp())))
		ts, _ := tsoutil.ParseTS(pos.Timestamp)
		metrics.DataCoordCheckpointUnixSeconds.WithLabelValues(paramtable.GetStringNodeID(), channel).Set(float64(ts.Unix()))
	}
	return nil
}

func (m *meta) GetChannelCheckpoint(vChannel string) *msgpb.MsgPosition {
	m.channelCPs.RLock()
	defer m.channelCPs.RUnlock()
	cp, ok := m.channelCPs.checkpoints[vChannel]
	if !ok {
		return nil
	}
	return proto.Clone(cp).(*msgpb.MsgPosition)
}

func (m *meta) DropChannelCheckpoint(vChannel string) error {
	m.channelCPs.lockChannel(vChannel)
	defer m.channelCPs.unlockChannel(vChannel)
	err := m.catalog.DropChannelCheckpoint(m.ctx, vChannel)
	if err != nil {
		return err
	}
	m.channelCPs.Lock()
	delete(m.channelCPs.checkpoints, vChannel)
	m.channelCPs.Unlock()
	metrics.DataCoordCheckpointUnixSeconds.DeleteLabelValues(paramtable.GetStringNodeID(), vChannel)
	mlog.Info(context.TODO(), "DropChannelCheckpoint done", zap.String("vChannel", vChannel))
	return nil
}

func (m *meta) GetChannelCheckpoints() map[string]*msgpb.MsgPosition {
	m.channelCPs.RLock()
	defer m.channelCPs.RUnlock()

	checkpoints := make(map[string]*msgpb.MsgPosition, len(m.channelCPs.checkpoints))
	for ch, cp := range m.channelCPs.checkpoints {
		checkpoints[ch] = proto.Clone(cp).(*msgpb.MsgPosition)
	}
	return checkpoints
}

func (m *meta) GcConfirm(ctx context.Context, collectionID, partitionID UniqueID) bool {
	return m.catalog.GcConfirm(ctx, collectionID, partitionID)
}

func (m *meta) GetCompactableSegmentGroupByCollection() map[int64][]*SegmentInfo {
	allSegs := m.SelectSegments(m.ctx, SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return isSegmentHealthy(segment) &&
			isFlushed(segment) && // sealed segment
			!segment.isCompacting && // not compacting now
			!segment.GetIsImporting() // not importing now
	}))

	ret := make(map[int64][]*SegmentInfo)
	for _, seg := range allSegs {
		if _, ok := ret[seg.CollectionID]; !ok {
			ret[seg.CollectionID] = make([]*SegmentInfo, 0)
		}

		ret[seg.CollectionID] = append(ret[seg.CollectionID], seg)
	}

	return ret
}

func (m *meta) GetEarliestStartPositionOfGrowingSegments(label *CompactionGroupLabel) *msgpb.MsgPosition {
	segments := m.SelectSegments(m.ctx, WithCollection(label.CollectionID), SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return segment.GetState() == commonpb.SegmentState_Growing &&
			(label.PartitionID == common.AllPartitionsID || segment.GetPartitionID() == label.PartitionID) &&
			segment.GetInsertChannel() == label.Channel
	}))

	earliest := &msgpb.MsgPosition{Timestamp: math.MaxUint64}
	for _, seg := range segments {
		if earliest.GetTimestamp() == math.MaxUint64 || earliest.GetTimestamp() > seg.GetStartPosition().GetTimestamp() {
			earliest = seg.GetStartPosition()
		}
	}
	return earliest
}

// initStateChangeEntry initializes the nested map structure for the given keys and returns the format change map.
func (s *segMetricMutation) initStateChangeEntry(level, state, sortedStatus, storageVersion string) map[string]int {
	if _, ok := s.stateChange[level]; !ok {
		s.stateChange[level] = make(map[string]map[string]map[string]map[string]int)
	}
	if _, ok := s.stateChange[level][state]; !ok {
		s.stateChange[level][state] = make(map[string]map[string]map[string]int)
	}
	if _, ok := s.stateChange[level][state][sortedStatus]; !ok {
		s.stateChange[level][state][sortedStatus] = make(map[string]map[string]int)
	}
	if _, ok := s.stateChange[level][state][sortedStatus][storageVersion]; !ok {
		s.stateChange[level][state][sortedStatus][storageVersion] = make(map[string]int)
	}
	return s.stateChange[level][state][sortedStatus][storageVersion]
}

// addNewSeg update metrics update for a new segment.
func (s *segMetricMutation) addNewSeg(state commonpb.SegmentState, level datapb.SegmentLevel, isSorted bool, storageVersion int64, format string, rowCount int64) {
	storageVersionStr := fmt.Sprint(storageVersion)
	sortedStatus := getSortStatus(isSorted)
	entry := s.initStateChangeEntry(level.String(), state.String(), sortedStatus, storageVersionStr)
	entry[format] += 1

	s.rowCountChange += rowCount
	s.rowCountAccChange += rowCount
}

// commit persists all updates in current segMetricMutation, should and must be called AFTER segment state change
// has persisted in Etcd.
func (s *segMetricMutation) commit() {
	for level, submap := range s.stateChange {
		for state, sortedMap := range submap {
			for sortedLabel, versionMap := range sortedMap {
				for storageVersion, formatMap := range versionMap {
					for format, change := range formatMap {
						metrics.DataCoordNumSegments.WithLabelValues(state, level, sortedLabel, storageVersion, format).Add(float64(change))
					}
				}
			}
		}
	}
}

// append updates current segMetricMutation when segment state change happens.
func (s *segMetricMutation) append(oldState, newState commonpb.SegmentState, level datapb.SegmentLevel, isSorted bool, storageVersion int64, format string, rowCountUpdate int64) {
	// Update # of rows on new flush operations and drop operations.
	if isFlushState(newState) && !isFlushState(oldState) {
		// If new flush.
		s.rowCountChange += rowCountUpdate
		s.rowCountAccChange += rowCountUpdate
	} else if newState == commonpb.SegmentState_Dropped && oldState != newState {
		// If new drop.
		s.rowCountChange -= rowCountUpdate
	}
}

func sameSegmentMetricLabels(oldSegment, newSegment *SegmentInfo) bool {
	return oldSegment.GetState() == newSegment.GetState() &&
		oldSegment.GetLevel() == newSegment.GetLevel() &&
		oldSegment.GetIsSorted() == newSegment.GetIsSorted() &&
		oldSegment.GetStorageVersion() == newSegment.GetStorageVersion() &&
		segmentMetricFormatLabel(oldSegment) == segmentMetricFormatLabel(newSegment)
}

func (s *segMetricMutation) appendSegmentLabelChange(oldSegment, newSegment *SegmentInfo) {
	oldEntry := s.initStateChangeEntry(
		oldSegment.GetLevel().String(),
		oldSegment.GetState().String(),
		getSortStatus(oldSegment.GetIsSorted()),
		fmt.Sprint(oldSegment.GetStorageVersion()),
	)
	oldEntry[segmentMetricFormatLabel(oldSegment)] -= 1

	newEntry := s.initStateChangeEntry(
		newSegment.GetLevel().String(),
		newSegment.GetState().String(),
		getSortStatus(newSegment.GetIsSorted()),
		fmt.Sprint(newSegment.GetStorageVersion()),
	)
	newEntry[segmentMetricFormatLabel(newSegment)] += 1

	s.append(oldSegment.GetState(), newSegment.GetState(), newSegment.GetLevel(), newSegment.GetIsSorted(), newSegment.GetStorageVersion(), segmentMetricFormatLabel(newSegment), newSegment.GetNumOfRows())
}

func isFlushState(state commonpb.SegmentState) bool {
	return state == commonpb.SegmentState_Flushing || state == commonpb.SegmentState_Flushed
}

func (m *meta) ListCollections() []int64 {
	return m.collections.Keys()
}

func (m *meta) DropCompactionTask(ctx context.Context, task *datapb.CompactionTask) error {
	return m.compactionTaskMeta.DropCompactionTask(ctx, task)
}

func (m *meta) SaveCompactionTask(ctx context.Context, task *datapb.CompactionTask) error {
	return m.compactionTaskMeta.SaveCompactionTask(ctx, task)
}

func (m *meta) GetCompactionTasks(ctx context.Context) map[int64][]*datapb.CompactionTask {
	return m.compactionTaskMeta.GetCompactionTasks()
}

func (m *meta) GetCompactionTasksByTriggerID(ctx context.Context, triggerID int64) []*datapb.CompactionTask {
	return m.compactionTaskMeta.GetCompactionTasksByTriggerID(triggerID)
}

func (m *meta) CleanPartitionStatsInfo(ctx context.Context, info *datapb.PartitionStatsInfo) error {
	removePaths := make([]string, 0)
	partitionStatsPath := path.Join(m.chunkManager.RootPath(), common.PartitionStatsPath,
		metautil.JoinIDPath(info.CollectionID, info.PartitionID),
		info.GetVChannel(), strconv.FormatInt(info.GetVersion(), 10))
	removePaths = append(removePaths, partitionStatsPath)
	analyzeT := m.analyzeMeta.GetTask(info.GetAnalyzeTaskID())
	if analyzeT != nil {
		centroidsFilePath := path.Join(m.chunkManager.RootPath(), common.AnalyzeStatsPath,
			metautil.JoinIDPath(analyzeT.GetTaskID(), analyzeT.GetVersion(), analyzeT.GetCollectionID(),
				analyzeT.GetPartitionID(), analyzeT.GetFieldID()),
			"centroids",
		)
		removePaths = append(removePaths, centroidsFilePath)
		for _, segID := range info.GetSegmentIDs() {
			segmentOffsetMappingFilePath := path.Join(m.chunkManager.RootPath(), common.AnalyzeStatsPath,
				metautil.JoinIDPath(analyzeT.GetTaskID(), analyzeT.GetVersion(), analyzeT.GetCollectionID(),
					analyzeT.GetPartitionID(), analyzeT.GetFieldID(), segID),
				"offset_mapping",
			)
			removePaths = append(removePaths, segmentOffsetMappingFilePath)
		}
	}

	mlog.Debug(ctx, "remove clustering compaction stats files",
		zap.Int64("collectionID", info.GetCollectionID()),
		zap.Int64("partitionID", info.GetPartitionID()),
		zap.String("vChannel", info.GetVChannel()),
		zap.Int64("planID", info.GetVersion()),
		zap.Strings("removePaths", removePaths))
	err := m.chunkManager.MultiRemove(context.Background(), removePaths)
	if err != nil {
		mlog.Warn(ctx, "remove clustering compaction stats files failed", zap.Error(err))
		return err
	}

	// Persist the analyze task removal, the current-partition-stats-version
	// rollback (if the dropped version is the current one), and the
	// partition-stats info removal as a single composite catalog write. Keep
	// both meta locks across compute, persistence, and in-memory apply so a
	// concurrent compaction cannot advance the current version in between.
	m.analyzeMeta.Lock()
	defer m.analyzeMeta.Unlock()
	m.partitionStatsMeta.Lock()
	defer m.partitionStatsMeta.Unlock()

	rollbackVersion := m.partitionStatsMeta.getRollbackVersionLocked(info)
	actions := []metastore.UpdateAction{metastore.DropAnalyzeTask(info.GetAnalyzeTaskID())}
	if rollbackVersion != nil {
		actions = append(actions, metastore.SavePartitionStatsVersion(
			info.GetCollectionID(), info.GetPartitionID(), info.GetVChannel(), *rollbackVersion))
	}
	actions = append(actions, metastore.DropPartitionStats(info))

	if err := m.catalog.Update(ctx, actions...); err != nil {
		mlog.Warn(ctx, "clean partition stats info failed",
			zap.Int64("collectionID", info.GetCollectionID()),
			zap.Int64("partitionID", info.GetPartitionID()),
			zap.String("vChannel", info.GetVChannel()),
			zap.Int64("planID", info.GetVersion()),
			zap.Int64("analyzeTaskID", info.GetAnalyzeTaskID()),
			zap.Error(err))
		return err
	}

	m.analyzeMeta.dropTaskFromMemoryLocked(info.GetAnalyzeTaskID())
	m.partitionStatsMeta.applyDropLocked(info, rollbackVersion)

	mlog.Debug(ctx, "drop partition stats meta",
		zap.Int64("collectionID", info.GetCollectionID()),
		zap.Int64("partitionID", info.GetPartitionID()),
		zap.String("vChannel", info.GetVChannel()),
		zap.Int64("planID", info.GetVersion()))
	return nil
}

func (m *meta) completeSortCompactionMutation(
	t *datapb.CompactionTask,
	result *datapb.CompactionPlanResult,
) ([]*SegmentInfo, *segMetricMutation, error) {
	logger := mlog.With(zap.Int64("planID", t.GetPlanID()),
		zap.String("type", t.GetType().String()),
		zap.Int64("collectionID", t.CollectionID),
		zap.Int64("partitionID", t.PartitionID),
		zap.String("channel", t.GetChannel()))

	metricMutation := &segMetricMutation{stateChange: make(segmentMetricStateChange)}
	compactFromSegID := t.GetInputSegments()[0]
	oldSegment := m.segments.GetSegment(compactFromSegID)
	if oldSegment == nil {
		return nil, nil, merr.WrapErrSegmentNotFound(compactFromSegID)
	}

	// Re-validate segment health to prevent race condition with drop collection
	// between ValidateSegmentStateBeforeCompleteCompactionMutation and here
	if !isSegmentHealthy(oldSegment) {
		logger.Warn(context.TODO(), "input segment was dropped during compaction mutation",
			zap.Int64("planID", t.GetPlanID()),
			zap.Int64("segmentID", compactFromSegID),
			zap.String("state", oldSegment.GetState().String()))
		return nil, nil, merr.WrapErrSegmentNotFound(compactFromSegID, "input segment was dropped")
	}

	resultInvisible := oldSegment.GetIsInvisible()
	if !oldSegment.GetCreatedByCompaction() {
		resultInvisible = false
	}

	resultSegment := result.GetSegments()[0]

	commitTs := oldSegment.GetCommitTimestamp()
	deleteApplyStartAfterTimetick := segmentDeleteApplyStartAfterTimetick(oldSegment.SegmentInfo)
	startPos, dmlPos := recalculateSegmentPosition(resultSegment.GetInsertLogs(), oldSegment.GetInsertChannel(),
		normalizePositionTimestamp(oldSegment.GetStartPosition(), commitTs),
		normalizePositionTimestamp(oldSegment.GetDmlPosition(), commitTs))

	if t.GetSchema() == nil {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("sort compaction task schema is nil")
	}
	outputSchemaVersion := t.GetSchema().GetVersion()

	segmentInfo := &datapb.SegmentInfo{
		CollectionID:                  oldSegment.GetCollectionID(),
		PartitionID:                   oldSegment.GetPartitionID(),
		InsertChannel:                 oldSegment.GetInsertChannel(),
		MaxRowNum:                     oldSegment.GetMaxRowNum(),
		LastExpireTime:                oldSegment.GetLastExpireTime(),
		StartPosition:                 startPos,
		DmlPosition:                   dmlPos,
		IsImporting:                   oldSegment.GetIsImporting(),
		State:                         commonpb.SegmentState_Flushed,
		Level:                         oldSegment.GetLevel(),
		LastLevel:                     oldSegment.GetLastLevel(),
		PartitionStatsVersion:         oldSegment.GetPartitionStatsVersion(),
		LastPartitionStatsVersion:     oldSegment.GetLastPartitionStatsVersion(),
		CreatedByCompaction:           oldSegment.GetCreatedByCompaction(),
		IsInvisible:                   resultInvisible,
		StorageVersion:                resultSegment.GetStorageVersion(),
		ID:                            resultSegment.GetSegmentID(),
		NumOfRows:                     resultSegment.GetNumOfRows(),
		Binlogs:                       resultSegment.GetInsertLogs(),
		Statslogs:                     resultSegment.GetField2StatslogPaths(),
		TextStatsLogs:                 resultSegment.GetTextStatsLogs(),
		Bm25Statslogs:                 resultSegment.GetBm25Logs(),
		Deltalogs:                     resultSegment.GetDeltalogs(),
		CompactionFrom:                []int64{compactFromSegID},
		IsSorted:                      resultSegment.GetIsSorted(),
		ManifestPath:                  resultSegment.GetManifest(),
		ExpirQuantiles:                resultSegment.GetExpirQuantiles(),
		IsSortedByNamespace:           resultSegment.GetIsSortedByNamespace(),
		SchemaVersion:                 outputSchemaVersion,
		CommitTimestamp:               0, // Normalized: row timestamps already rewritten
		DeleteApplyStartAfterTimetick: deleteApplyStartAfterTimetick,
	}
	// Statistics is computed at the compactor and shipped on the
	// CompactionSegment. V3 outputs whose stats live in the manifest are
	// populated correctly there; the receiver does not recompute.
	segmentInfo.Stats = resultSegment.GetStats()

	segment := NewSegmentInfo(segmentInfo)
	if segment.GetNumOfRows() > 0 {
		metricMutation.addNewSeg(segment.GetState(), segment.GetLevel(), segment.GetIsSorted(), segment.GetStorageVersion(), segmentMetricFormatLabel(segment), segment.GetNumOfRows())
	} else {
		segment.State = commonpb.SegmentState_Dropped
		segment.DroppedAt = uint64(time.Now().UnixNano())
		logger.Info(context.TODO(), "drop segment due to 0 rows", zap.Int64("segmentID", segment.GetID()))
	}

	logger = logger.With(zap.Int64s("compactFrom", []int64{oldSegment.GetID()}), zap.Int64("compactTo", segment.GetID()))

	logger.Info(context.TODO(), "meta update: prepare for complete stats mutation - complete",
		zap.Int64("num rows", segment.GetNumOfRows()),
		zap.Int64("segment size", segment.getSegmentSize()),
		zap.Int64s("expirQuantiles", segment.GetExpirQuantiles()))
	// Persist old (dropped) and new segments atomically — all modification in UpdateFunc.
	oldKey := m.segmentKey(oldSegment.GetCollectionID(), oldSegment.GetPartitionID(), oldSegment.GetID())
	newKey := m.segmentKey(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
	txn := m.segmentPersist.Txn(m.ctx)
	txn.Update(oldKey, func(existing *datapb.SegmentInfo) (*datapb.SegmentInfo, bool) {
		existing.State = commonpb.SegmentState_Dropped
		existing.DroppedAt = uint64(time.Now().UnixNano())
		existing.Compacted = true
		return existing, true
	})
	txn.Insert(newKey, segment.SegmentInfo)
	results, err := txn.Commit()
	if err != nil {
		logger.Warn(context.TODO(), "fail to persist segments for sort compaction", zap.Error(err))
		return nil, nil, err
	}

	// Update cache and compute metrics from returned old values.
	oldRetSeg := NewSegmentInfo(results[0].Value)
	old, existed := m.segments.SetSegment(oldSegment.GetID(), oldRetSeg, results[0].Version)
	if existed && old.GetState() != oldRetSeg.GetState() {
		metricMutation.appendSegmentLabelChange(old, oldRetSeg)
	}
	m.segments.SetSegment(segment.GetID(), segment, results[1].Version)
	logger.Info(context.TODO(), "meta update: alter in memory meta after compaction - complete")
	return []*SegmentInfo{segment}, metricMutation, nil
}

func (m *meta) completeBumpSchemaVersionCompactionMutation(
	t *datapb.CompactionTask,
	result *datapb.CompactionPlanResult,
) ([]*SegmentInfo, *segMetricMutation, error) {
	metricMutation := &segMetricMutation{stateChange: make(segmentMetricStateChange)}
	if len(t.GetInputSegments()) != 1 {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction should have exactly one input segment")
	}
	if len(result.GetSegments()) != 1 {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction result should have exactly one segment")
	}
	if t.GetSchema() == nil {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction requires task schema")
	}

	segmentID := t.GetInputSegments()[0]
	oldSegment := m.segments.GetSegment(segmentID)
	if oldSegment == nil {
		return nil, nil, merr.WrapErrSegmentNotFound(segmentID)
	}
	if !isSegmentHealthy(oldSegment) {
		return nil, nil, merr.WrapErrSegmentNotFound(segmentID, "input segment was dropped")
	}
	if oldSegment.GetIsInvisible() {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction input segment should not be invisible")
	}

	resultSegment := result.GetSegments()[0]
	newSchemaVersion := t.GetSchema().GetVersion()
	if newSchemaVersion < oldSegment.GetSchemaVersion() {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction schema version is older than input segment")
	}
	if resultSegment.GetNumOfRows() == 0 && resultSegment.GetSegmentID() != segmentID {
		if resultSegment.GetStorageVersion() < storage.StorageV3 {
			return nil, nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction result should contain a StorageV3 segment")
		}
		return m.completeBumpSchemaVersionReplacementMutation(metricMutation, t, oldSegment, resultSegment, newSchemaVersion)
	}

	resultManifest := resultSegment.GetManifest()
	if resultSegment.GetStorageVersion() < storage.StorageV3 || resultManifest == "" {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction result should contain a StorageV3 manifest")
	}
	if resultSegment.GetSegmentID() != segmentID {
		return m.completeBumpSchemaVersionReplacementMutation(metricMutation, t, oldSegment, resultSegment, newSchemaVersion)
	}
	currentManifest := oldSegment.GetManifestPath()
	if currentManifest == "" {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction input segment should contain a StorageV3 manifest")
	}
	// Optimistic-concurrency CAS on the manifest pointer. Adopt the in-place
	// result only when it is a valid successor of the current pointer:
	//   - result == current: idempotent replay of an adoption whose task state
	//     was lost to a crash after AlterSegments but before meta_saved.
	//   - base == current AND result strictly newer on the same base path: a
	//     fresh forward commit built on the current pointer.
	// Reject anything else — a stale base (a concurrent stats/index/bump commit
	// advanced the pointer), a rollback, a different base path, or an unparsable
	// result — so the task re-triggers and rebuilds on the current manifest
	// instead of overwriting the concurrent commit (mirrors the stats path's
	// errStatsResultStale). The check also self-heals a lost result: the pointer
	// stays put, so the retry re-pins the same base.
	baseManifest := resultSegment.GetBaseManifest()
	if baseManifest == "" {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("schema bump result missing base manifest")
	}
	if resultManifest != currentManifest {
		if baseManifest != currentManifest {
			return nil, nil, merr.WrapErrIllegalCompactionPlanMsg("schema bump result base manifest %s no longer matches current %s", baseManifest, currentManifest)
		}
		cmp, err := packed.CompareManifestPath(resultManifest, currentManifest)
		if err != nil {
			return nil, nil, merr.WrapErrIllegalCompactionPlanMsg("schema bump result manifest %s not comparable with current %s: %v", resultManifest, currentManifest, err)
		}
		if cmp <= 0 {
			return nil, nil, merr.WrapErrIllegalCompactionPlanMsg("schema bump result manifest %s does not advance current %s", resultManifest, currentManifest)
		}
	}

	key := m.segmentKey(oldSegment.GetCollectionID(), oldSegment.GetPartitionID(), oldSegment.GetID())
	txn := m.segmentPersist.Txn(m.ctx)
	txn.Update(key, func(existing *datapb.SegmentInfo) (*datapb.SegmentInfo, bool) {
		before := proto.Clone(existing).(*datapb.SegmentInfo)
		existing.Binlogs = resultSegment.GetInsertLogs()
		if newSchemaVersion > existing.GetSchemaVersion() {
			existing.SchemaVersion = newSchemaVersion
		}
		existing.StorageVersion = resultSegment.GetStorageVersion()
		existing.ManifestPath = resultManifest
		existing.CommitTimestamp = 0
		if !proto.Equal(before, existing) {
			existing.DataVersion = before.GetDataVersion() + 1
		}
		return existing, true
	})
	results, err := txn.Commit()
	if err != nil {
		return nil, nil, err
	}

	updated := NewSegmentInfo(results[0].Value)
	old, existed := m.segments.SetSegment(segmentID, updated, results[0].Version)
	if existed && !sameSegmentMetricLabels(old, updated) {
		metricMutation.appendSegmentLabelChange(old, updated)
	}
	return []*SegmentInfo{updated}, metricMutation, nil
}

func (m *meta) completeBumpSchemaVersionReplacementMutation(
	metricMutation *segMetricMutation,
	t *datapb.CompactionTask,
	oldSegment *SegmentInfo,
	resultSegment *datapb.CompactionSegment,
	schemaVersion int32,
) ([]*SegmentInfo, *segMetricMutation, error) {
	idRange := t.GetPreAllocatedSegmentIDs()
	if idRange == nil || idRange.GetBegin() >= idRange.GetEnd() || resultSegment.GetSegmentID() != idRange.GetBegin() {
		return nil, nil, merr.WrapErrIllegalCompactionPlanMsg("schema bump replacement result segment ID %d does not match the pre-allocated segment ID range", resultSegment.GetSegmentID())
	}

	deleteApplyStartAfterTimetick := segmentDeleteApplyStartAfterTimetick(oldSegment.SegmentInfo)
	commitTs := oldSegment.GetCommitTimestamp()
	startPos, dmlPos := recalculateSegmentPosition(resultSegment.GetInsertLogs(), oldSegment.GetInsertChannel(),
		normalizePositionTimestamp(oldSegment.GetStartPosition(), commitTs),
		normalizePositionTimestamp(oldSegment.GetDmlPosition(), commitTs))
	newSegment := NewSegmentInfo(&datapb.SegmentInfo{
		ID:                            resultSegment.GetSegmentID(),
		CollectionID:                  oldSegment.GetCollectionID(),
		PartitionID:                   oldSegment.GetPartitionID(),
		InsertChannel:                 oldSegment.GetInsertChannel(),
		MaxRowNum:                     oldSegment.GetMaxRowNum(),
		LastExpireTime:                oldSegment.GetLastExpireTime(),
		StartPosition:                 startPos,
		DmlPosition:                   dmlPos,
		IsImporting:                   oldSegment.GetIsImporting(),
		State:                         commonpb.SegmentState_Flushed,
		Level:                         oldSegment.GetLevel(),
		LastLevel:                     oldSegment.GetLastLevel(),
		PartitionStatsVersion:         oldSegment.GetPartitionStatsVersion(),
		LastPartitionStatsVersion:     oldSegment.GetLastPartitionStatsVersion(),
		CreatedByCompaction:           true,
		IsInvisible:                   false,
		StorageVersion:                resultSegment.GetStorageVersion(),
		NumOfRows:                     resultSegment.GetNumOfRows(),
		Binlogs:                       resultSegment.GetInsertLogs(),
		Statslogs:                     resultSegment.GetField2StatslogPaths(),
		TextStatsLogs:                 resultSegment.GetTextStatsLogs(),
		Bm25Statslogs:                 resultSegment.GetBm25Logs(),
		Deltalogs:                     resultSegment.GetDeltalogs(),
		CompactionFrom:                []int64{oldSegment.GetID()},
		IsSorted:                      oldSegment.GetIsSorted(),
		ManifestPath:                  resultSegment.GetManifest(),
		ExpirQuantiles:                resultSegment.GetExpirQuantiles(),
		IsSortedByNamespace:           oldSegment.GetIsSortedByNamespace(),
		SchemaVersion:                 schemaVersion,
		CommitTimestamp:               0,
		DeleteApplyStartAfterTimetick: deleteApplyStartAfterTimetick,
		// Statistics is computed at the compactor and shipped on the
		// CompactionSegment; the receiver copies it verbatim.
		Stats: resultSegment.GetStats(),
	})
	if newSegment.GetNumOfRows() > 0 {
		metricMutation.addNewSeg(newSegment.GetState(), newSegment.GetLevel(), newSegment.GetIsSorted(), newSegment.GetStorageVersion(), segmentMetricFormatLabel(newSegment), newSegment.GetNumOfRows())
	} else {
		newSegment.State = commonpb.SegmentState_Dropped
		newSegment.DroppedAt = uint64(time.Now().UnixNano())
	}

	oldKey := m.segmentKey(oldSegment.GetCollectionID(), oldSegment.GetPartitionID(), oldSegment.GetID())
	newKey := m.segmentKey(newSegment.GetCollectionID(), newSegment.GetPartitionID(), newSegment.GetID())
	txn := m.segmentPersist.Txn(m.ctx)
	txn.Update(oldKey, func(existing *datapb.SegmentInfo) (*datapb.SegmentInfo, bool) {
		existing.State = commonpb.SegmentState_Dropped
		existing.DroppedAt = uint64(time.Now().UnixNano())
		existing.Compacted = true
		return existing, true
	})
	txn.Insert(newKey, newSegment.SegmentInfo)
	results, err := txn.Commit()
	if err != nil {
		return nil, nil, err
	}

	dropped := NewSegmentInfo(results[0].Value)
	old, existed := m.segments.SetSegment(oldSegment.GetID(), dropped, results[0].Version)
	if existed && !sameSegmentMetricLabels(old, dropped) {
		metricMutation.appendSegmentLabelChange(old, dropped)
	}
	m.segments.SetSegment(newSegment.GetID(), newSegment, results[1].Version)
	return []*SegmentInfo{newSegment}, metricMutation, nil
}

func (m *meta) getSegmentsMetrics(collectionID int64) []*metricsinfo.Segment {

	allSegments := m.segments.GetSegments()
	segments := make([]*metricsinfo.Segment, 0, len(allSegments))
	for _, s := range allSegments {
		if collectionID <= 0 || s.GetCollectionID() == collectionID {
			segments = append(segments, &metricsinfo.Segment{
				SegmentID:    s.ID,
				CollectionID: s.CollectionID,
				PartitionID:  s.PartitionID,
				Channel:      s.InsertChannel,
				NumOfRows:    s.NumOfRows,
				State:        s.State.String(),
				MemSize:      s.getSegmentSize(),
				Level:        s.Level.String(),
				IsImporting:  s.IsImporting,
				Compacted:    s.Compacted,
				IsSorted:     s.IsSorted,
				NodeID:       paramtable.GetNodeID(),
			})
		}
	}

	return segments
}

func (m *meta) DropSegmentsOfPartition(ctx context.Context, partitionIDs []int64) error {

	// Collect segments to drop (read-only from cache for key construction).
	type segRef struct {
		id  int64
		key string
	}
	var segRefs []segRef
	for _, seg := range m.segments.GetSegments() {
		if contains(partitionIDs, seg.PartitionID) {
			segRefs = append(segRefs, segRef{
				id:  seg.GetID(),
				key: m.segmentKey(seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID()),
			})
		}
	}

	// All modification inside UpdateFunc.
	txn := m.segmentPersist.Txn(m.ctx)
	for _, ref := range segRefs {
		txn.Update(ref.key, func(existing *datapb.SegmentInfo) (*datapb.SegmentInfo, bool) {
			existing.State = commonpb.SegmentState_Dropped
			existing.DroppedAt = uint64(time.Now().UnixNano())
			return existing, true
		})
	}
	results, err := txn.Commit()
	if err != nil {
		return err
	}

	// Compute metrics and update cache from returned persist values.
	metricMutation := &segMetricMutation{
		stateChange: make(segmentMetricStateChange),
	}
	for i, ref := range segRefs {
		newSeg := NewSegmentInfo(results[i].Value)
		oldSeg, existed := m.segments.SetSegment(ref.id, newSeg, results[i].Version)
		if existed && !sameSegmentMetricLabels(oldSeg, newSeg) {
			metricMutation.appendSegmentLabelChange(oldSeg, newSeg)
		}
	}
	metricMutation.commit()
	return nil
}

func contains(arr []int64, target int64) bool {
	for _, val := range arr {
		if val == target {
			return true
		}
	}
	return false
}

func (m *meta) UpdateFileResources(ctx context.Context, resources []*internalpb.FileResourceInfo, version uint64) error {
	m.resourceLock.Lock()
	defer m.resourceLock.Unlock()
	m.resourceIDMap = make(map[int64]*internalpb.FileResourceInfo)
	for _, resource := range resources {
		m.resourceIDMap[resource.Id] = resource
	}
	m.resourceVersion = version

	return nil
}

func (m *meta) ListFileResources(ctx context.Context) ([]*internalpb.FileResourceInfo, uint64) {
	m.resourceLock.RLock()
	defer m.resourceLock.RUnlock()
	return lo.Values(m.resourceIDMap), m.resourceVersion
}

func (m *meta) GetFileResources(ctx context.Context, resourceIDs ...int64) ([]*internalpb.FileResourceInfo, error) {
	m.resourceLock.RLock()
	defer m.resourceLock.RUnlock()

	resources := make([]*internalpb.FileResourceInfo, 0)
	for _, id := range resourceIDs {
		if resource, ok := m.resourceIDMap[id]; ok {
			resources = append(resources, resource)
		} else {
			return nil, errors.Errorf("file resource %d not found", id)
		}
	}
	return resources, nil
}

// TruncateChannelByTime drops segments of a channel that were updated before the flush timestamp
func (m *meta) TruncateChannelByTime(ctx context.Context, vChannel string, flushTs uint64) error {

	segments := m.segments.GetSegmentsBySelector(SegmentFilterFunc(isSegmentHealthy), WithChannel(vChannel))

	// Collect segments to drop (read-only from cache for key construction and filtering).
	type segRef struct {
		id  int64
		key string
	}
	var segRefs []segRef
	for _, segment := range segments {
		if segmentEffectiveDmlTs(segment.SegmentInfo) <= flushTs && segment.GetState() != commonpb.SegmentState_Dropped {
			segRefs = append(segRefs, segRef{
				id:  segment.GetID(),
				key: m.segmentKey(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID()),
			})
		}
	}

	if len(segRefs) == 0 {
		return nil
	}

	// All modification inside UpdateFunc.
	txn := m.segmentPersist.Txn(ctx)
	for _, ref := range segRefs {
		txn.Update(ref.key, func(existing *datapb.SegmentInfo) (*datapb.SegmentInfo, bool) {
			existing.State = commonpb.SegmentState_Dropped
			existing.DroppedAt = uint64(time.Now().UnixNano())
			return existing, true
		})
	}
	results, err := txn.Commit()
	if err != nil {
		mlog.Warn(ctx, "Failed to batch set segments state to dropped", zap.Error(err))
		return err
	}

	// Compute metrics and update cache from returned persist values.
	metricMutation := &segMetricMutation{
		stateChange: make(segmentMetricStateChange),
	}
	for i, ref := range segRefs {
		newSeg := NewSegmentInfo(results[i].Value)
		oldSeg, existed := m.segments.SetSegment(ref.id, newSeg, results[i].Version)
		if existed && !sameSegmentMetricLabels(oldSeg, newSeg) {
			metricMutation.appendSegmentLabelChange(oldSeg, newSeg)
		}
	}
	metricMutation.commit()

	return nil
}

// WatchChannelCheckpoint waits until the checkpoint of the specified channel
// reaches or exceeds the target timestamp. Used for TruncateCollection.
func (m *meta) WatchChannelCheckpoint(ctx context.Context, vChannel string, targetTs uint64) error {
	m.channelCPs.cond.L.Lock()

	for {
		cp, ok := m.channelCPs.checkpoints[vChannel]
		if ok && cp != nil && cp.GetTimestamp() >= targetTs {
			m.channelCPs.cond.L.Unlock()
			return nil
		}

		if err := m.channelCPs.cond.Wait(ctx); err != nil {
			return err
		}
	}
}
