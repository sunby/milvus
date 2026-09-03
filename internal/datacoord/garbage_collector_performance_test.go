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

package datacoord

import (
	"context"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"

	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	runLargeGCPerfTestEnv           = "MILVUS_RUN_LARGE_GC_PERF_TEST"
	runDropIndexGCPerfTestEnv       = "MILVUS_RUN_DROP_INDEX_GC_PERF_TEST"
	runDroppedSegmentGCPerfTestEnv  = "MILVUS_RUN_DROPPED_SEGMENT_GC_PERF_TEST"
	gcPerfCollectionsEnv            = "MILVUS_GC_PERF_COLLECTIONS"
	gcPerfSegmentsEnv               = "MILVUS_GC_PERF_SEGMENTS"
	gcPerfDroppedEveryEnv           = "MILVUS_GC_PERF_DROPPED_EVERY"
	gcPerfIndexDefinitionsEnv       = "MILVUS_GC_PERF_INDEX_DEFINITIONS"
	gcPerfIndexEntriesEnv           = "MILVUS_GC_PERF_INDEX_ENTRIES"
	gcPerfIndexFilesPerEntryEnv     = "MILVUS_GC_PERF_INDEX_FILES_PER_ENTRY"
	gcPerfIndexKVDeleteLatencyEnv   = "MILVUS_GC_PERF_INDEX_KV_DELETE_LATENCY"
	gcPerfIndexFileLatencyEnv       = "MILVUS_GC_PERF_INDEX_FILE_DELETE_LATENCY"
	gcPerfIndexApplyLatencyEnv      = "MILVUS_GC_PERF_INDEX_APPLY_LATENCY"
	gcPerfIndexBatchDeleteEnv       = "MILVUS_GC_PERF_INDEX_NATIVE_FILE_BATCH"
	gcPerfIndexBatchSizeEnv         = "MILVUS_GC_PERF_INDEX_BATCH_SIZE"
	gcPerfIndexMetaBatchDeleteEnv   = "MILVUS_GC_PERF_INDEX_META_BATCH_DELETE"
	gcPerfDroppedSegmentsEnv        = "MILVUS_GC_PERF_DROPPED_SEGMENTS"
	gcPerfDroppedCollectionsEnv     = "MILVUS_GC_PERF_DROPPED_COLLECTIONS"
	gcPerfDroppedIndexesEnv         = "MILVUS_GC_PERF_DROPPED_INDEXES_PER_SEGMENT"
	gcPerfDroppedFilesEnv           = "MILVUS_GC_PERF_DROPPED_FILES_PER_INDEX"
	gcPerfDroppedDataViewLatencyEnv = "MILVUS_GC_PERF_DROPPED_DATAVIEW_LATENCY"
	gcPerfDroppedChannelLatencyEnv  = "MILVUS_GC_PERF_DROPPED_CHANNEL_LATENCY"
	gcPerfDroppedFileLatencyEnv     = "MILVUS_GC_PERF_DROPPED_FILE_LATENCY"
	gcPerfDroppedIndexLatencyEnv    = "MILVUS_GC_PERF_DROPPED_INDEX_KV_LATENCY"
	gcPerfDroppedSegmentLatencyEnv  = "MILVUS_GC_PERF_DROPPED_SEGMENT_KV_LATENCY"
	gcPerfDroppedApplyLatencyEnv    = "MILVUS_GC_PERF_DROPPED_APPLY_LATENCY"
	gcPerfDroppedChannelBatchEnv    = "MILVUS_GC_PERF_DROPPED_CHANNEL_STATE_BATCH_SIZE"
	gcPerfDroppedBatchDeleteEnv     = "MILVUS_GC_PERF_DROPPED_NATIVE_FILE_BATCH"
	gcPerfDroppedBatchSizeEnv       = "MILVUS_GC_PERF_DROPPED_BATCH_SIZE"

	gcPerfNativeDeleteRequestLimit = 1000
)

type gcPerfDroppedSegmentCatalog struct {
	metastore.DataCoordCatalog
	channelLatency time.Duration
	indexLatency   time.Duration
	applyLatency   bool

	channelExistenceCalls atomic.Int64
	dropSegmentIndexCalls atomic.Int64
	dropSegmentIndexTxns  atomic.Int64
}

func (c *gcPerfDroppedSegmentCatalog) LoadChannelExistence(_ context.Context, channels []string) (map[string]bool, error) {
	c.channelExistenceCalls.Add(1)
	if c.applyLatency && c.channelLatency > 0 {
		time.Sleep(c.channelLatency)
	}
	existence := make(map[string]bool, len(channels))
	for _, channel := range channels {
		existence[channel] = false
	}
	return existence, nil
}

func (c *gcPerfDroppedSegmentCatalog) DropSegmentIndex(context.Context, int64, int64, int64, int64) error {
	c.dropSegmentIndexCalls.Add(1)
	c.dropSegmentIndexTxns.Add(1)
	if c.applyLatency && c.indexLatency > 0 {
		time.Sleep(c.indexLatency)
	}
	return nil
}

type gcPerfDroppedSegmentBatchCatalog struct {
	*gcPerfDroppedSegmentCatalog
}

func (*gcPerfDroppedSegmentBatchCatalog) DropIndexes(context.Context, []*model.Index) error {
	return nil
}

func (c *gcPerfDroppedSegmentBatchCatalog) DropSegmentIndexes(_ context.Context, indexes []*model.SegmentIndex) error {
	requests := gcPerfEtcdTxnRequests(len(indexes))
	c.dropSegmentIndexCalls.Add(int64(len(indexes)))
	c.dropSegmentIndexTxns.Add(requests)
	if c.applyLatency && c.indexLatency > 0 {
		time.Sleep(time.Duration(requests) * c.indexLatency)
	}
	return nil
}

type gcPerfDroppedSegmentDataViewCatalog struct {
	gcPerfDataViewCatalog
	listLatency  time.Duration
	applyLatency bool
	listCalls    atomic.Int64
}

func (c *gcPerfDroppedSegmentDataViewCatalog) ListDataViews(context.Context, int64) ([]*viewpb.DataViewOfCollection, error) {
	c.listCalls.Add(1)
	if c.applyLatency && c.listLatency > 0 {
		time.Sleep(c.listLatency)
	}
	return nil, nil
}

type gcPerfDroppedSegmentPersist struct {
	deleteLatency time.Duration
	applyLatency  bool

	deleteCalls atomic.Int64
	getRequests atomic.Int64
	txnRequests atomic.Int64
	revision    atomic.Int64
}

func (p *gcPerfDroppedSegmentPersist) Txn(context.Context) Txn[string, *datapb.SegmentInfo] {
	return &gcPerfDroppedSegmentTxn{persist: p}
}

func (*gcPerfDroppedSegmentPersist) Scan(context.Context, string) ([]string, []*datapb.SegmentInfo, []int64, error) {
	return nil, nil, nil, nil
}

type gcPerfDroppedSegmentTxn struct {
	persist          *gcPerfDroppedSegmentPersist
	ops              int
	deletes          int
	versionedDeletes int
}

func (t *gcPerfDroppedSegmentTxn) Insert(string, *datapb.SegmentInfo) {
	t.ops++
}

func (t *gcPerfDroppedSegmentTxn) Update(string, UpdateFunc[*datapb.SegmentInfo]) {
	t.ops++
}

func (t *gcPerfDroppedSegmentTxn) Upsert(string, *datapb.SegmentInfo, UpdateFunc[*datapb.SegmentInfo]) {
	t.ops++
}

func (t *gcPerfDroppedSegmentTxn) Delete(string) {
	t.ops++
	t.deletes++
}

func (t *gcPerfDroppedSegmentTxn) DeleteIfVersion(_ string, version int64) {
	t.ops++
	t.deletes++
	if version > 0 {
		t.versionedDeletes++
	}
}

func (t *gcPerfDroppedSegmentTxn) Commit() ([]TxnResult[*datapb.SegmentInfo], error) {
	getRequests := int64(t.deletes - t.versionedDeletes)
	txnRequests := gcPerfEtcdTxnRequests(t.ops)
	t.persist.deleteCalls.Add(int64(t.deletes))
	t.persist.getRequests.Add(getRequests)
	t.persist.txnRequests.Add(txnRequests)
	if t.persist.applyLatency && t.persist.deleteLatency > 0 {
		time.Sleep(time.Duration(getRequests+txnRequests) * t.persist.deleteLatency)
	}
	version := t.persist.revision.Add(1)
	results := make([]TxnResult[*datapb.SegmentInfo], t.ops)
	for i := range results {
		results[i].Version = version
	}
	return results, nil
}

type gcPerfDropIndexCatalog struct {
	metastore.DataCoordCatalog
	deleteLatency time.Duration
	applyLatency  bool

	dropIndexCalls              atomic.Int64
	dropSegmentIndexCalls       atomic.Int64
	dropIndexTxnRequests        atomic.Int64
	dropSegmentIndexTxnRequests atomic.Int64
}

func (c *gcPerfDropIndexCatalog) waitForDelete(requests int64) {
	if c.applyLatency && c.deleteLatency > 0 && requests > 0 {
		time.Sleep(time.Duration(requests) * c.deleteLatency)
	}
}

func (c *gcPerfDropIndexCatalog) DropIndex(context.Context, int64, int64) error {
	c.dropIndexCalls.Add(1)
	c.dropIndexTxnRequests.Add(1)
	c.waitForDelete(1)
	return nil
}

func (c *gcPerfDropIndexCatalog) DropSegmentIndex(context.Context, int64, int64, int64, int64) error {
	c.dropSegmentIndexCalls.Add(1)
	c.dropSegmentIndexTxnRequests.Add(1)
	c.waitForDelete(1)
	return nil
}

func gcPerfEtcdTxnRequests(items int) int64 {
	if items <= 0 {
		return 0
	}
	limit := Params.MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	if limit <= 0 {
		limit = 64
	}
	return int64((items + limit - 1) / limit)
}

type gcPerfDroppedSegmentBatchModel struct {
	nativeFileRequests   int64
	fallbackFileRounds   int64
	segmentIndexRequests int64
	segmentRequests      int64
}

func gcPerfModelDroppedSegmentBatches(
	segmentCount int,
	indexesPerSegment int,
	filesPerSegment int,
	batchSize int,
	removeConcurrency int,
) gcPerfDroppedSegmentBatchModel {
	if segmentCount <= 0 {
		return gcPerfDroppedSegmentBatchModel{}
	}
	if batchSize <= 0 {
		batchSize = 1000
	}
	if removeConcurrency <= 0 {
		removeConcurrency = 1
	}

	// recycleDroppedSegmentsInBatches limits a batch by both candidate count
	// and the estimated number of exact paths. A candidate with no exact paths
	// still consumes one unit so metadata-only cleanup remains bounded.
	weight := max(1, filesPerSegment)
	segmentsPerBatch := batchSize
	if byWeight := batchSize / weight; byWeight < segmentsPerBatch {
		segmentsPerBatch = byWeight
	}
	segmentsPerBatch = max(1, segmentsPerBatch)

	model := gcPerfDroppedSegmentBatchModel{}
	for remaining := segmentCount; remaining > 0; {
		count := min(segmentsPerBatch, remaining)
		if fileCount := count * filesPerSegment; fileCount > 0 {
			model.nativeFileRequests += int64((fileCount + gcPerfNativeDeleteRequestLimit - 1) / gcPerfNativeDeleteRequestLimit)
			model.fallbackFileRounds += int64((fileCount + removeConcurrency - 1) / removeConcurrency)
		}
		model.segmentIndexRequests += gcPerfEtcdTxnRequests(count * indexesPerSegment)
		model.segmentRequests += gcPerfEtcdTxnRequests(count)
		remaining -= count
	}
	return model
}

type gcPerfDropIndexBatchCatalog struct {
	*gcPerfDropIndexCatalog
}

func (c *gcPerfDropIndexBatchCatalog) DropIndexes(_ context.Context, indexes []*model.Index) error {
	requests := gcPerfEtcdTxnRequests(len(indexes))
	c.dropIndexCalls.Add(int64(len(indexes)))
	c.dropIndexTxnRequests.Add(requests)
	c.waitForDelete(requests)
	return nil
}

func (c *gcPerfDropIndexBatchCatalog) DropSegmentIndexes(_ context.Context, indexes []*model.SegmentIndex) error {
	requests := gcPerfEtcdTxnRequests(len(indexes))
	c.dropSegmentIndexCalls.Add(int64(len(indexes)))
	c.dropSegmentIndexTxnRequests.Add(requests)
	c.waitForDelete(requests)
	return nil
}

type gcPerfDropIndexChunkManager struct {
	storage.ChunkManager
	deleteLatency time.Duration
	applyLatency  bool

	removeCalls       atomic.Int64
	batchRequestCalls atomic.Int64
}

func (*gcPerfDropIndexChunkManager) RootPath() string {
	return "gc-perf"
}

func (c *gcPerfDropIndexChunkManager) Remove(context.Context, string) error {
	c.removeCalls.Add(1)
	if c.applyLatency && c.deleteLatency > 0 {
		time.Sleep(c.deleteLatency)
	}
	return nil
}

type gcPerfBatchChunkManager struct {
	*gcPerfDropIndexChunkManager
}

func (c *gcPerfBatchChunkManager) MultiRemoveWithResult(_ context.Context, paths []string) []storage.RemoveResult {
	c.removeCalls.Add(int64(len(paths)))
	requestCount := (len(paths) + gcPerfNativeDeleteRequestLimit - 1) / gcPerfNativeDeleteRequestLimit
	c.batchRequestCalls.Add(int64(requestCount))
	if c.applyLatency && c.deleteLatency > 0 {
		time.Sleep(time.Duration(requestCount) * c.deleteLatency)
	}

	results := make([]storage.RemoveResult, len(paths))
	for i, path := range paths {
		results[i].Path = path
	}
	return results
}

type countingDataViewGarbageCollector struct {
	scanCalls  int
	applyCalls int
}

func (c *countingDataViewGarbageCollector) ListGarbageCollectionCandidates(
	context.Context,
	[]int64,
	int,
) (map[int64][]*viewpb.DataVersion, error) {
	c.scanCalls++
	return nil, nil
}

func (c *countingDataViewGarbageCollector) GarbageCollectCandidates(
	context.Context,
	int64,
	[]*viewpb.DataVersion,
) error {
	c.applyCalls++
	return nil
}

// gcPerfDataViewCatalog deliberately keeps no DataViews. Its counters prove
// that the production manager uses one key-only candidate scan per GC pass and
// never falls back to per-Collection catalog reads.
type gcPerfDataViewCatalog struct {
	listCalls          int
	listAllCalls       int
	candidateScanCalls int
}

func (*gcPerfDataViewCatalog) SaveDataView(context.Context, *viewpb.DataViewOfCollection) error {
	return nil
}

func (c *gcPerfDataViewCatalog) ListDataViews(context.Context, int64) ([]*viewpb.DataViewOfCollection, error) {
	c.listCalls++
	return nil, nil
}

func (c *gcPerfDataViewCatalog) ListAllDataViews(context.Context) ([]*viewpb.DataViewOfCollection, error) {
	c.listAllCalls++
	return nil, nil
}

func (c *gcPerfDataViewCatalog) ListDataViewGCCandidates(
	context.Context,
	[]int64,
	int,
) (map[int64][]*viewpb.DataVersion, error) {
	c.candidateScanCalls++
	return nil, nil
}

func (*gcPerfDataViewCatalog) DropDataView(context.Context, int64, *viewpb.DataVersion) error {
	return nil
}

func (*gcPerfDataViewCatalog) DropDataViews(context.Context, int64) error {
	return nil
}

func (*gcPerfDataViewCatalog) MarkDataViewCollectionDropped(context.Context, int64) error {
	return nil
}

func (*gcPerfDataViewCatalog) ListDroppedDataViewCollections(context.Context) ([]int64, error) {
	return nil, nil
}

func (*gcPerfDataViewCatalog) UnmarkDataViewCollectionDropped(context.Context, int64) error {
	return nil
}

type gcPerfMemSnapshot struct {
	heapAlloc    uint64
	heapInuse    uint64
	totalAlloc   uint64
	numGC        uint32
	pauseTotalNs uint64
}

func readGCPerfMemSnapshot() gcPerfMemSnapshot {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return gcPerfMemSnapshot{
		heapAlloc:    stats.HeapAlloc,
		heapInuse:    stats.HeapInuse,
		totalAlloc:   stats.TotalAlloc,
		numGC:        stats.NumGC,
		pauseTotalNs: stats.PauseTotalNs,
	}
}

func gcPerfEnvInt(t *testing.T, key string, defaultValue int) int {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		t.Fatalf("%s must be a positive integer, got %q", key, value)
	}
	return parsed
}

func gcPerfEnvDuration(t *testing.T, key string, defaultValue time.Duration) time.Duration {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed < 0 {
		t.Fatalf("%s must be a non-negative duration, got %q", key, value)
	}
	return parsed
}

func gcPerfEnvBool(t *testing.T, key string, defaultValue bool) bool {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		t.Fatalf("%s must be a boolean, got %q", key, value)
	}
	return parsed
}

func gcPerfMiB(bytes uint64) float64 {
	return float64(bytes) / (1024 * 1024)
}

func logGCPerfStage(t *testing.T, name string, items int, elapsed time.Duration, before, after gcPerfMemSnapshot) {
	t.Helper()
	t.Logf(
		"GC_PERF stage=%s items=%d elapsed=%s throughput=%.0f_items/s heap_before=%.1f_MiB heap_after=%.1f_MiB heap_inuse_after=%.1f_MiB allocated=%.1f_MiB gc_cycles=%d gc_pause=%s",
		name,
		items,
		elapsed,
		float64(items)/elapsed.Seconds(),
		gcPerfMiB(before.heapAlloc),
		gcPerfMiB(after.heapAlloc),
		gcPerfMiB(after.heapInuse),
		gcPerfMiB(after.totalAlloc-before.totalAlloc),
		after.numGC-before.numGC,
		time.Duration(after.pauseTotalNs-before.pauseTotalNs),
	)
}

// TestDataCoordGCLargeScalePerformance measures the in-memory DataCoord GC
// traversal and eligibility paths at Collection-per-Tenant scale. It is
// deliberately opt-in because its default fixture contains one million
// collections and ten million segments.
//
// The test body never creates an etcd client or a ChunkManager. The process
// must also use the standalone/embed environment below so package-level
// paramtable initialization does not add an external etcd configuration
// source. Object-store listing/deletion latency is intentionally outside this
// measurement; the result isolates DataCoord's metadata traversal and GC
// decision cost.
func TestDataCoordGCLargeScalePerformance(t *testing.T) {
	if os.Getenv(runLargeGCPerfTestEnv) != "1" {
		t.Skipf("set %s=1 to run the large-scale GC performance test", runLargeGCPerfTestEnv)
	}
	if os.Getenv("DEPLOY_MODE") != "STANDALONE" || os.Getenv("ETCD_USE_EMBED") != "true" {
		t.Fatal("large-scale GC performance test requires DEPLOY_MODE=STANDALONE and ETCD_USE_EMBED=true to isolate external etcd")
	}

	collectionCount := gcPerfEnvInt(t, gcPerfCollectionsEnv, 1_000_000)
	segmentCount := gcPerfEnvInt(t, gcPerfSegmentsEnv, 10_000_000)
	droppedEvery := gcPerfEnvInt(t, gcPerfDroppedEveryEnv, 100)
	droppedCandidates := segmentCount / droppedEvery

	t.Logf(
		"GC_PERF config collections=%d segments=%d dropped_every=%d dropped_candidates=%d gomaxprocs=%d",
		collectionCount,
		segmentCount,
		droppedEvery,
		droppedCandidates,
		runtime.GOMAXPROCS(0),
	)

	ctx := context.Background()
	m := &meta{
		ctx:         ctx,
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		segments:    NewCachedSegmentsInfo(),
		channelCPs:  newChannelCps(),
	}

	setupBefore := readGCPerfMemSnapshot()
	setupStart := time.Now()
	for collectionID := 1; collectionID <= collectionCount; collectionID++ {
		m.collections.Insert(int64(collectionID), &collectionInfo{ID: int64(collectionID)})
	}
	t.Logf("GC_PERF seeded_collections=%d elapsed=%s", collectionCount, time.Since(setupStart))

	progressEvery := segmentCount / 10
	if progressEvery == 0 {
		progressEvery = 1
	}
	for segmentID := 1; segmentID <= segmentCount; segmentID++ {
		state := commonpb.SegmentState_Flushed
		if segmentID%droppedEvery == 0 {
			state = commonpb.SegmentState_Dropped
		}
		collectionID := int64((segmentID-1)%collectionCount + 1)
		m.segments.SetSegment(int64(segmentID), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           int64(segmentID),
				CollectionID: collectionID,
				State:        state,
			},
		}, 0)
		if segmentID%progressEvery == 0 || segmentID == segmentCount {
			t.Logf("GC_PERF seeded_segments=%d/%d elapsed=%s", segmentID, segmentCount, time.Since(setupStart))
		}
	}

	runtime.GC()
	setupElapsed := time.Since(setupStart)
	setupAfter := readGCPerfMemSnapshot()
	logGCPerfStage(t, "fixture_setup", collectionCount+segmentCount, setupElapsed, setupBefore, setupAfter)

	dataViewGC := &countingDataViewGarbageCollector{}
	gc := newGarbageCollector(m, newMockHandler(), GcOption{
		enabled:       false,
		dropTolerance: 100 * 365 * 24 * time.Hour,
		dataViewGC:    dataViewGC,
	})
	defer gc.close()

	dataViewBefore := readGCPerfMemSnapshot()
	dataViewStart := time.Now()
	pprof.Do(ctx, pprof.Labels("gc_stage", "recycle_data_views"), func(profileCtx context.Context) {
		gc.recycleDataViews(profileCtx, nil)
	})
	dataViewElapsed := time.Since(dataViewStart)
	dataViewAfter := readGCPerfMemSnapshot()
	logGCPerfStage(t, "recycle_data_views", collectionCount, dataViewElapsed, dataViewBefore, dataViewAfter)
	if dataViewGC.scanCalls != 1 {
		t.Fatalf("DataView GC candidate scans = %d, want 1", dataViewGC.scanCalls)
	}
	if dataViewGC.applyCalls != 0 {
		t.Fatalf("DataView GC candidate applies = %d, want 0", dataViewGC.applyCalls)
	}

	// Release the temporary one-million-entry collection slice before the
	// segment traversal so the two stages have independent allocation deltas.
	runtime.GC()

	segmentBefore := readGCPerfMemSnapshot()
	segmentStart := time.Now()
	pprof.Do(ctx, pprof.Labels("gc_stage", "recycle_dropped_segments"), func(profileCtx context.Context) {
		gc.recycleDroppedSegments(profileCtx, nil)
	})
	segmentElapsed := time.Since(segmentStart)
	segmentAfter := readGCPerfMemSnapshot()
	logGCPerfStage(t, "recycle_dropped_segments", segmentCount, segmentElapsed, segmentBefore, segmentAfter)
	if m.segments.Len() != segmentCount {
		t.Fatalf("segment count changed during ineligible-candidate scan: got %d, want %d", m.segments.Len(), segmentCount)
	}

	runtime.KeepAlive(m)
}

// TestDataCoordDataViewGCLargeScalePerformance measures the production
// DataView reference manager and DataView manager at one-million-Collection
// scale after startup recovery has dropped its temporary RecoverySnapshot. Both passes
// exercise the production candidate-scan dispatch and prove it does not fall
// back to per-Collection catalog reads or create per-Collection GC state.
// Actual catalog I/O is intentionally not simulated.
func TestDataCoordDataViewGCLargeScalePerformance(t *testing.T) {
	if os.Getenv(runLargeGCPerfTestEnv) != "1" {
		t.Skipf("set %s=1 to run the large-scale GC performance test", runLargeGCPerfTestEnv)
	}
	if os.Getenv("DEPLOY_MODE") != "STANDALONE" || os.Getenv("ETCD_USE_EMBED") != "true" {
		t.Fatal("large-scale GC performance test requires DEPLOY_MODE=STANDALONE and ETCD_USE_EMBED=true to isolate external etcd")
	}

	collectionCount := gcPerfEnvInt(t, gcPerfCollectionsEnv, 1_000_000)
	ctx := context.Background()
	m := &meta{
		ctx:         ctx,
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		segments:    NewCachedSegmentsInfo(),
		channelCPs:  newChannelCps(),
	}
	for collectionID := 1; collectionID <= collectionCount; collectionID++ {
		m.collections.Insert(int64(collectionID), &collectionInfo{ID: int64(collectionID)})
	}
	runtime.GC()

	catalog := &gcPerfDataViewCatalog{}
	dataViews, recoverySnapshot, err := dataview.RecoverManager(ctx, catalog, &dataViewSegmentStore{meta: m})
	if err != nil {
		t.Fatalf("recover DataView manager: %v", err)
	}
	if err := recoverDataViewCollections(ctx, dataViews, recoverySnapshot, nil, nil); err != nil {
		t.Fatalf("finish DataView recovery: %v", err)
	}
	recoverySnapshot = nil
	dataViewReferences, err := recoverDataViewReferenceManager(ctx, catalog, dataViews, func(collectionID int64) bool {
		return m.GetCollection(collectionID) != nil
	})
	if err != nil {
		t.Fatalf("recover DataView reference manager: %v", err)
	}
	gc := newGarbageCollector(m, newMockHandler(), GcOption{
		enabled:    false,
		dataViewGC: dataViewReferences,
	})
	defer gc.close()

	for pass := 1; pass <= 2; pass++ {
		stage := "recycle_data_views_real_manager_first"
		if pass == 2 {
			stage = "recycle_data_views_real_manager_steady"
		}
		before := readGCPerfMemSnapshot()
		start := time.Now()
		pprof.Do(ctx, pprof.Labels("gc_stage", stage), func(profileCtx context.Context) {
			gc.recycleDataViews(profileCtx, nil)
		})
		elapsed := time.Since(start)
		after := readGCPerfMemSnapshot()
		logGCPerfStage(t, stage, collectionCount, elapsed, before, after)
		if catalog.listCalls != 0 {
			t.Fatalf("DataView catalog point-list calls after pass %d = %d, want 0", pass, catalog.listCalls)
		}
		if catalog.candidateScanCalls != pass {
			t.Fatalf("DataView catalog candidate scans after pass %d = %d, want %d", pass, catalog.candidateScanCalls, pass)
		}
		runtime.GC()
	}

	runtime.KeepAlive(dataViewReferences)
	runtime.KeepAlive(dataViews)
	runtime.KeepAlive(m)
}

// TestDataCoordDropIndexGCLargeScalePerformance measures the production
// DropIndex GC deletion pipeline with lightweight, configurable file and KV
// backends. The default ten-million-entry run does not sleep for every mocked
// I/O operation; it measures GC's traversal and dispatch cost and reports the
// I/O time implied by the configured latencies. Set
// MILVUS_GC_PERF_INDEX_APPLY_LATENCY=true on a smaller fixture to inject the
// latency into every fallback per-path delete or modeled native request and
// validate the projection against wall time.
// MILVUS_GC_PERF_INDEX_NATIVE_FILE_BATCH controls whether the file mock
// exposes native batch deletion, while
// MILVUS_GC_PERF_INDEX_META_BATCH_DELETE controls whether the catalog mock
// exposes bounded exact-key transactions. Both storage variants use the same
// bounded SegmentIndex GC traversal.
func TestDataCoordDropIndexGCLargeScalePerformance(t *testing.T) {
	if os.Getenv(runDropIndexGCPerfTestEnv) != "1" {
		t.Skipf("set %s=1 to run the DropIndex GC performance test", runDropIndexGCPerfTestEnv)
	}

	definitionCount := gcPerfEnvInt(t, gcPerfIndexDefinitionsEnv, 1_000_000)
	entryCount := gcPerfEnvInt(t, gcPerfIndexEntriesEnv, 10_000_000)
	filesPerEntry := gcPerfEnvInt(t, gcPerfIndexFilesPerEntryEnv, 1)
	kvDeleteLatency := gcPerfEnvDuration(t, gcPerfIndexKVDeleteLatencyEnv, time.Millisecond)
	fileDeleteLatency := gcPerfEnvDuration(t, gcPerfIndexFileLatencyEnv, 5*time.Millisecond)
	applyLatency := gcPerfEnvBool(t, gcPerfIndexApplyLatencyEnv, false)
	nativeFileBatchEnabled := gcPerfEnvBool(t, gcPerfIndexBatchDeleteEnv, false)
	batchSize := gcPerfEnvInt(t, gcPerfIndexBatchSizeEnv, Params.DataCoordCfg.GCIndexFileBatchSize.GetAsInt())
	metaBatchDeleteEnabled := gcPerfEnvBool(t, gcPerfIndexMetaBatchDeleteEnv, false)

	oldBatchSize := Params.DataCoordCfg.GCIndexFileBatchSize.GetValue()
	if err := Params.Save(Params.DataCoordCfg.GCIndexFileBatchSize.Key, strconv.Itoa(batchSize)); err != nil {
		t.Fatalf("set batch size: %v", err)
	}
	batchSize = Params.DataCoordCfg.GCIndexFileBatchSize.GetAsInt()
	t.Cleanup(func() {
		if err := Params.Save(Params.DataCoordCfg.GCIndexFileBatchSize.Key, oldBatchSize); err != nil {
			t.Errorf("restore batch size: %v", err)
		}
	})

	removeConcurrency := Params.DataCoordCfg.GCRemoveConcurrent.GetAsInt()
	if removeConcurrency <= 0 {
		t.Fatalf("DataCoord GC remove concurrency must be positive, got %d", removeConcurrency)
	}

	// The GC groups candidate SegmentIndexes by the configured file weight. A
	// native backend splits flattened files at its request limit; a base
	// ChunkManager removes the same flattened set through the bounded fallback
	// pool.
	candidatesPerBatch := batchSize / filesPerEntry
	if candidatesPerBatch == 0 {
		candidatesPerBatch = 1
	}
	fullCandidateBatches := entryCount / candidatesPerBatch
	remainingCandidates := entryCount % candidatesPerBatch
	modeledFileRequests := int64(entryCount) * int64(filesPerEntry)
	modeledFileLatencyUnits := int64(fullCandidateBatches) * int64((candidatesPerBatch*filesPerEntry+removeConcurrency-1)/removeConcurrency)
	if remainingCandidates > 0 {
		modeledFileLatencyUnits += int64((remainingCandidates*filesPerEntry + removeConcurrency - 1) / removeConcurrency)
	}
	if nativeFileBatchEnabled {
		requestsPerFullBatch := (candidatesPerBatch*filesPerEntry + gcPerfNativeDeleteRequestLimit - 1) / gcPerfNativeDeleteRequestLimit
		modeledFileRequests = int64(fullCandidateBatches * requestsPerFullBatch)
		if remainingCandidates > 0 {
			modeledFileRequests += int64((remainingCandidates*filesPerEntry + gcPerfNativeDeleteRequestLimit - 1) / gcPerfNativeDeleteRequestLimit)
		}
		modeledFileLatencyUnits = modeledFileRequests
	}
	modeledFieldKVRequests := int64(definitionCount)
	modeledSegmentKVRequests := int64(entryCount)
	if metaBatchDeleteEnabled {
		modeledFieldKVRequests = gcPerfEtcdTxnRequests(definitionCount)
		modeledSegmentKVRequests = int64(fullCandidateBatches) * gcPerfEtcdTxnRequests(candidatesPerBatch)
		modeledSegmentKVRequests += gcPerfEtcdTxnRequests(remainingCandidates)
	}
	modeledIOTime := time.Duration(modeledFieldKVRequests+modeledSegmentKVRequests)*kvDeleteLatency +
		time.Duration(modeledFileLatencyUnits)*fileDeleteLatency
	t.Logf(
		"DROP_INDEX_GC_PERF config definitions=%d entries=%d files_per_entry=%d native_file_batch=%t meta_batch_delete=%t file_batch_size=%d remove_concurrency=%d kv_delete_latency=%s file_delete_latency=%s apply_latency=%t modeled_field_kv_requests=%d modeled_segment_kv_requests=%d modeled_file_requests=%d modeled_total_io=%s",
		definitionCount,
		entryCount,
		filesPerEntry,
		nativeFileBatchEnabled,
		metaBatchDeleteEnabled,
		batchSize,
		removeConcurrency,
		kvDeleteLatency,
		fileDeleteLatency,
		applyLatency,
		modeledFieldKVRequests,
		modeledSegmentKVRequests,
		modeledFileRequests,
		modeledIOTime,
	)

	oldLogLevel := mlog.GetLevel()
	mlog.SetLevel(mlog.WarnLevel)
	defer mlog.SetLevel(oldLogLevel)

	ctx := context.Background()
	catalog := &gcPerfDropIndexCatalog{
		deleteLatency: kvDeleteLatency,
		applyLatency:  applyLatency,
	}
	gcCatalog := metastore.DataCoordCatalog(catalog)
	if metaBatchDeleteEnabled {
		gcCatalog = &gcPerfDropIndexBatchCatalog{gcPerfDropIndexCatalog: catalog}
	}
	chunkManager := &gcPerfDropIndexChunkManager{
		deleteLatency: fileDeleteLatency,
		applyLatency:  applyLatency,
	}
	gcChunkManager := storage.ChunkManager(chunkManager)
	if nativeFileBatchEnabled {
		gcChunkManager = &gcPerfBatchChunkManager{gcPerfDropIndexChunkManager: chunkManager}
	}

	const (
		partitionID = int64(2)
		segmentID   = int64(3)
		fieldID     = int64(4)
		indexID     = int64(5)
	)
	indexMeta := &indexMeta{
		ctx:              ctx,
		catalog:          gcCatalog,
		indexes:          make(map[UniqueID]map[UniqueID]*model.Index, definitionCount),
		segmentBuildInfo: newSegmentIndexBuildInfo(),
		keyLock:          lock.NewKeyLock[UniqueID](),
		segmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
	}
	m := &meta{
		ctx:       ctx,
		segments:  NewCachedSegmentsInfo(),
		indexMeta: indexMeta,
	}
	// A single live segment is sufficient to force the DropIndex eligibility
	// branch (!IsIndexExist) for every SegmentIndex entry after all field-index
	// definitions are removed. Sharing it keeps the fixture focused on file/KV
	// deletion instead of duplicating the separate ten-million SegmentInfo test.
	m.segments.SetSegment(segmentID, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: 1,
		PartitionID:  partitionID,
		State:        commonpb.SegmentState_Flushed,
	}}, 0)

	fileKeys := make([]string, filesPerEntry)
	for i := range fileKeys {
		fileKeys[i] = "index-file-" + strconv.Itoa(i)
	}
	setupBefore := readGCPerfMemSnapshot()
	setupStart := time.Now()
	for i := 1; i <= definitionCount; i++ {
		collectionID := int64(i)
		indexMeta.indexes[collectionID] = map[UniqueID]*model.Index{
			indexID: {
				CollectionID: collectionID,
				FieldID:      fieldID,
				IndexID:      indexID,
				IndexName:    "drop-index-gc-perf",
				IsDeleted:    true,
			},
		}
	}
	for i := 1; i <= entryCount; i++ {
		collectionID := int64((i-1)%definitionCount + 1)
		indexMeta.segmentBuildInfo.AddForRecovery(&model.SegmentIndex{
			SegmentID:             segmentID,
			CollectionID:          collectionID,
			PartitionID:           partitionID,
			IndexID:               indexID,
			BuildID:               int64(i),
			IndexVersion:          1,
			IndexState:            commonpb.IndexState_Finished,
			IndexFileKeys:         fileKeys,
			IndexStorePathVersion: 1,
		})
	}
	runtime.GC()
	setupElapsed := time.Since(setupStart)
	setupAfter := readGCPerfMemSnapshot()
	logGCPerfStage(t, "drop_index_fixture_setup", definitionCount+entryCount, setupElapsed, setupBefore, setupAfter)

	gc := newGarbageCollector(m, nil, GcOption{
		enabled: false,
		cli:     gcChunkManager,
	})
	defer gc.close()

	fieldIndexBefore := readGCPerfMemSnapshot()
	fieldIndexStart := time.Now()
	gc.recycleUnusedIndexes(ctx, nil)
	fieldIndexElapsed := time.Since(fieldIndexStart)
	fieldIndexAfter := readGCPerfMemSnapshot()
	logGCPerfStage(t, "drop_field_indexes", definitionCount, fieldIndexElapsed, fieldIndexBefore, fieldIndexAfter)

	segmentIndexBefore := readGCPerfMemSnapshot()
	segmentIndexStart := time.Now()
	pprof.Do(ctx, pprof.Labels("gc_stage", "drop_segment_indexes"), func(profileCtx context.Context) {
		gc.recycleUnusedSegIndexes(profileCtx, nil)
	})
	segmentIndexElapsed := time.Since(segmentIndexStart)
	segmentIndexAfter := readGCPerfMemSnapshot()
	logGCPerfStage(t, "drop_segment_indexes", entryCount, segmentIndexElapsed, segmentIndexBefore, segmentIndexAfter)

	wantFileDeletes := int64(entryCount) * int64(filesPerEntry)
	if got := catalog.dropIndexCalls.Load(); got != int64(definitionCount) {
		t.Fatalf("field-index KV deletes = %d, want %d", got, definitionCount)
	}
	if got := catalog.dropSegmentIndexCalls.Load(); got != int64(entryCount) {
		t.Fatalf("segment-index KV deletes = %d, want %d", got, entryCount)
	}
	if got := catalog.dropIndexTxnRequests.Load(); got != modeledFieldKVRequests {
		t.Fatalf("field-index KV transaction requests = %d, want %d", got, modeledFieldKVRequests)
	}
	if got := catalog.dropSegmentIndexTxnRequests.Load(); got != modeledSegmentKVRequests {
		t.Fatalf("segment-index KV transaction requests = %d, want %d", got, modeledSegmentKVRequests)
	}
	if got := chunkManager.removeCalls.Load(); got != wantFileDeletes {
		t.Fatalf("index file deletes = %d, want %d", got, wantFileDeletes)
	}
	wantBatchRequests := int64(0)
	if nativeFileBatchEnabled {
		wantBatchRequests = modeledFileRequests
	}
	if got := chunkManager.batchRequestCalls.Load(); got != wantBatchRequests {
		t.Fatalf("index file batch requests = %d, want %d", got, wantBatchRequests)
	}
	indexMeta.fieldIndexLock.RLock()
	remainingFieldIndexes := len(indexMeta.indexes)
	indexMeta.fieldIndexLock.RUnlock()
	if remainingFieldIndexes != 0 {
		t.Fatalf("field-index collections remaining after GC = %d, want 0", remainingFieldIndexes)
	}
	if remaining := len(indexMeta.segmentBuildInfo.List()); remaining != 0 {
		t.Fatalf("segment-index entries remaining after GC = %d, want 0", remaining)
	}

	t.Logf(
		"DROP_INDEX_GC_PERF result definitions=%d entries=%d native_file_batch=%t meta_batch_delete=%t field_kv_deletes=%d field_kv_txn_requests=%d segment_kv_deletes=%d segment_kv_txn_requests=%d logical_file_deletes=%d native_file_requests=%d measured_field_stage=%s measured_segment_stage=%s measured_segment_throughput=%.0f_entries/s latency_applied=%t modeled_total_io=%s",
		definitionCount,
		entryCount,
		nativeFileBatchEnabled,
		metaBatchDeleteEnabled,
		catalog.dropIndexCalls.Load(),
		catalog.dropIndexTxnRequests.Load(),
		catalog.dropSegmentIndexCalls.Load(),
		catalog.dropSegmentIndexTxnRequests.Load(),
		chunkManager.removeCalls.Load(),
		chunkManager.batchRequestCalls.Load(),
		fieldIndexElapsed,
		segmentIndexElapsed,
		float64(entryCount)/segmentIndexElapsed.Seconds(),
		applyLatency,
		modeledIOTime,
	)

	runtime.KeepAlive(fileKeys)
	runtime.KeepAlive(m)
}

// TestDataCoordDroppedSegmentGCLargeScalePerformance drives the production
// recycleDroppedSegments deletion path with configurable DataView, channel,
// object-store, SegmentIndex KV, and Segment KV latencies. Keep latency
// injection disabled for large CPU/allocation runs; enable it on a smaller
// fixture to validate the request model against wall time.
func TestDataCoordDroppedSegmentGCLargeScalePerformance(t *testing.T) {
	if os.Getenv(runDroppedSegmentGCPerfTestEnv) != "1" {
		t.Skipf("set %s=1 to run the dropped-segment GC performance test", runDroppedSegmentGCPerfTestEnv)
	}

	segmentCount := gcPerfEnvInt(t, gcPerfDroppedSegmentsEnv, 1_000_000)
	collectionCount := gcPerfEnvInt(t, gcPerfDroppedCollectionsEnv, 100_000)
	indexesPerSegment := gcPerfEnvInt(t, gcPerfDroppedIndexesEnv, 1)
	filesPerIndex := gcPerfEnvInt(t, gcPerfDroppedFilesEnv, 1)
	dataViewLatency := gcPerfEnvDuration(t, gcPerfDroppedDataViewLatencyEnv, time.Millisecond)
	channelLatency := gcPerfEnvDuration(t, gcPerfDroppedChannelLatencyEnv, time.Millisecond)
	fileLatency := gcPerfEnvDuration(t, gcPerfDroppedFileLatencyEnv, 5*time.Millisecond)
	indexLatency := gcPerfEnvDuration(t, gcPerfDroppedIndexLatencyEnv, time.Millisecond)
	segmentLatency := gcPerfEnvDuration(t, gcPerfDroppedSegmentLatencyEnv, time.Millisecond)
	applyLatency := gcPerfEnvBool(t, gcPerfDroppedApplyLatencyEnv, false)
	channelStateBatchSize := gcPerfEnvInt(t, gcPerfDroppedChannelBatchEnv, 64)
	nativeFileBatchEnabled := gcPerfEnvBool(t, gcPerfDroppedBatchDeleteEnv, false)
	batchSize := gcPerfEnvInt(t, gcPerfDroppedBatchSizeEnv, 1000)

	oldChannelBatchSize := Params.DataCoordCfg.GCDroppedSegmentChannelStateBatchSize.GetValue()
	oldBatchSize := Params.DataCoordCfg.GCDroppedSegmentBatchSize.GetValue()
	if err := Params.Save(Params.DataCoordCfg.GCDroppedSegmentChannelStateBatchSize.Key, strconv.Itoa(channelStateBatchSize)); err != nil {
		t.Fatalf("set dropped-segment channel batch size: %v", err)
	}
	if err := Params.Save(Params.DataCoordCfg.GCDroppedSegmentBatchSize.Key, strconv.Itoa(batchSize)); err != nil {
		t.Fatalf("set dropped-segment batch size: %v", err)
	}
	channelStateBatchSize = Params.DataCoordCfg.GCDroppedSegmentChannelStateBatchSize.GetAsInt()
	batchSize = Params.DataCoordCfg.GCDroppedSegmentBatchSize.GetAsInt()
	t.Cleanup(func() {
		if err := Params.Save(Params.DataCoordCfg.GCDroppedSegmentChannelStateBatchSize.Key, oldChannelBatchSize); err != nil {
			t.Errorf("restore dropped-segment channel batch size: %v", err)
		}
		if err := Params.Save(Params.DataCoordCfg.GCDroppedSegmentBatchSize.Key, oldBatchSize); err != nil {
			t.Errorf("restore dropped-segment batch size: %v", err)
		}
	})

	removeConcurrency := Params.DataCoordCfg.GCRemoveConcurrent.GetAsInt()
	if removeConcurrency <= 0 {
		t.Fatalf("DataCoord GC remove concurrency must be positive, got %d", removeConcurrency)
	}
	filesPerSegment := indexesPerSegment * filesPerIndex
	modeledDataViewRequests := int64(segmentCount)
	modeledChannelKeys := int64(min(collectionCount, segmentCount))
	modeledChannelRequests := (modeledChannelKeys + int64(channelStateBatchSize) - 1) / int64(channelStateBatchSize)
	modeledFileRequests := int64(segmentCount * filesPerSegment)
	modeledIndexRequests := int64(segmentCount * indexesPerSegment)
	batchModel := gcPerfModelDroppedSegmentBatches(
		segmentCount,
		indexesPerSegment,
		filesPerSegment,
		batchSize,
		removeConcurrency,
	)
	modeledFileBackendRequests := modeledFileRequests
	modeledFileLatencyUnits := batchModel.fallbackFileRounds
	if nativeFileBatchEnabled {
		modeledFileBackendRequests = batchModel.nativeFileRequests
		modeledFileLatencyUnits = batchModel.nativeFileRequests
	}
	modeledIndexTxnRequests := batchModel.segmentIndexRequests
	modeledSegmentGetRequests := int64(0)
	modeledSegmentTxnRequests := batchModel.segmentRequests
	modeledSerialIO := time.Duration(modeledDataViewRequests)*dataViewLatency +
		time.Duration(modeledChannelKeys)*channelLatency +
		time.Duration(modeledFileBackendRequests)*fileLatency +
		time.Duration(modeledIndexTxnRequests)*indexLatency +
		time.Duration(modeledSegmentGetRequests+modeledSegmentTxnRequests)*segmentLatency
	// Candidate admission (including DataView), channel-state reads, and metadata
	// batches are serial. Exact channel marker reads are grouped into catalog batches.
	modeledBatchedIO := time.Duration(modeledChannelRequests)*channelLatency +
		time.Duration(modeledDataViewRequests)*dataViewLatency +
		time.Duration(modeledFileLatencyUnits)*fileLatency +
		time.Duration(modeledIndexTxnRequests)*indexLatency +
		time.Duration(modeledSegmentGetRequests+modeledSegmentTxnRequests)*segmentLatency
	t.Logf(
		"DROPPED_SEGMENT_GC_PERF config segments=%d collections=%d indexes_per_segment=%d files_per_index=%d remove_concurrency=%d channel_state_batch_size=%d native_file_batch=%t batch_size=%d dataview_latency=%s channel_latency=%s file_latency=%s index_kv_latency=%s segment_kv_latency=%s apply_latency=%t modeled_serial_io=%s modeled_batched_io=%s",
		segmentCount,
		collectionCount,
		indexesPerSegment,
		filesPerIndex,
		removeConcurrency,
		channelStateBatchSize,
		nativeFileBatchEnabled,
		batchSize,
		dataViewLatency,
		channelLatency,
		fileLatency,
		indexLatency,
		segmentLatency,
		applyLatency,
		modeledSerialIO,
		modeledBatchedIO,
	)

	oldLogLevel := mlog.GetLevel()
	mlog.SetLevel(mlog.WarnLevel)
	defer mlog.SetLevel(oldLogLevel)

	ctx := context.Background()
	segmentCatalog := &gcPerfDroppedSegmentCatalog{
		channelLatency: channelLatency,
		indexLatency:   indexLatency,
		applyLatency:   applyLatency,
	}
	gcSegmentCatalog := metastore.DataCoordCatalog(&gcPerfDroppedSegmentBatchCatalog{gcPerfDroppedSegmentCatalog: segmentCatalog})
	segmentPersist := &gcPerfDroppedSegmentPersist{
		deleteLatency: segmentLatency,
		applyLatency:  applyLatency,
	}
	// The fixture publishes segment cache entries at revision 1; successful
	// deletes must return a strictly newer tombstone revision just like etcd.
	segmentPersist.revision.Store(1)
	chunkManager := &gcPerfDropIndexChunkManager{
		deleteLatency: fileLatency,
		applyLatency:  applyLatency,
	}
	gcChunkManager := storage.ChunkManager(chunkManager)
	if nativeFileBatchEnabled {
		gcChunkManager = &gcPerfBatchChunkManager{gcPerfDropIndexChunkManager: chunkManager}
	}
	indexMeta := &indexMeta{
		ctx:              ctx,
		catalog:          gcSegmentCatalog,
		indexes:          make(map[UniqueID]map[UniqueID]*model.Index),
		segmentBuildInfo: newSegmentIndexBuildInfo(),
		keyLock:          lock.NewKeyLock[UniqueID](),
		segmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
	}
	m := &meta{
		ctx:            ctx,
		catalog:        gcSegmentCatalog,
		segmentPersist: segmentPersist,
		segments:       NewCachedSegmentsInfo(),
		channelCPs:     newChannelCps(),
		indexMeta:      indexMeta,
	}
	dataViewCatalog := &gcPerfDroppedSegmentDataViewCatalog{
		listLatency:  dataViewLatency,
		applyLatency: applyLatency,
	}
	m.dataViewManager = dataview.NewManager(dataViewCatalog, &dataViewSegmentStore{meta: m})

	fileKeys := make([]string, filesPerIndex)
	for i := range fileKeys {
		fileKeys[i] = "dropped-segment-index-file-" + strconv.Itoa(i)
	}
	setupBefore := readGCPerfMemSnapshot()
	setupStart := time.Now()
	for i := 1; i <= segmentCount; i++ {
		segmentID := int64(i)
		collectionID := int64((i-1)%collectionCount + 1)
		partitionID := collectionID
		m.segments.SetSegment(segmentID, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            segmentID,
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: "dropped-segment-gc-perf-" + strconv.FormatInt(collectionID, 10),
			State:         commonpb.SegmentState_Dropped,
			DroppedAt:     1,
		}}, 1)

		segmentIndexes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		for indexOffset := 0; indexOffset < indexesPerSegment; indexOffset++ {
			indexID := int64(indexOffset + 1)
			buildID := int64((i-1)*indexesPerSegment + indexOffset + 1)
			segmentIndex := &model.SegmentIndex{
				SegmentID:             segmentID,
				CollectionID:          collectionID,
				PartitionID:           partitionID,
				IndexID:               indexID,
				BuildID:               buildID,
				IndexVersion:          1,
				IndexState:            commonpb.IndexState_Finished,
				IndexFileKeys:         fileKeys,
				IndexStorePathVersion: 1,
			}
			segmentIndexes.Insert(indexID, segmentIndex)
			indexMeta.segmentBuildInfo.AddForRecovery(segmentIndex)
		}
		indexMeta.segmentIndexes.Insert(segmentID, segmentIndexes)
	}
	runtime.GC()
	setupElapsed := time.Since(setupStart)
	setupAfter := readGCPerfMemSnapshot()
	logGCPerfStage(t, "dropped_segment_fixture_setup", segmentCount, setupElapsed, setupBefore, setupAfter)

	gc := newGarbageCollector(m, newMockHandler(), GcOption{
		enabled:       false,
		cli:           gcChunkManager,
		dropTolerance: 0,
	})
	defer gc.close()

	stageBefore := readGCPerfMemSnapshot()
	stageStart := time.Now()
	pprof.Do(ctx, pprof.Labels("gc_stage", "drop_segments"), func(profileCtx context.Context) {
		gc.recycleDroppedSegments(profileCtx, nil)
	})
	stageElapsed := time.Since(stageStart)
	stageAfter := readGCPerfMemSnapshot()
	logGCPerfStage(t, "drop_segments", segmentCount, stageElapsed, stageBefore, stageAfter)

	if got := dataViewCatalog.listCalls.Load(); got != modeledDataViewRequests {
		t.Fatalf("DataView list requests = %d, want %d", got, modeledDataViewRequests)
	}
	if got := segmentCatalog.channelExistenceCalls.Load(); got != modeledChannelRequests {
		t.Fatalf("channel existence requests = %d, want %d", got, modeledChannelRequests)
	}
	if got := chunkManager.removeCalls.Load(); got != modeledFileRequests {
		t.Fatalf("file delete calls = %d, want %d", got, modeledFileRequests)
	}
	if got := segmentCatalog.dropSegmentIndexCalls.Load(); got != modeledIndexRequests {
		t.Fatalf("SegmentIndex metadata deletes = %d, want %d", got, modeledIndexRequests)
	}
	if got := segmentCatalog.dropSegmentIndexTxns.Load(); got != modeledIndexTxnRequests {
		t.Fatalf("SegmentIndex metadata txn requests = %d, want %d", got, modeledIndexTxnRequests)
	}
	if got := chunkManager.batchRequestCalls.Load(); got != func() int64 {
		if nativeFileBatchEnabled {
			return modeledFileBackendRequests
		}
		return 0
	}() {
		t.Fatalf("native file delete requests = %d, want %d", got, modeledFileBackendRequests)
	}
	if got := segmentPersist.deleteCalls.Load(); got != int64(segmentCount) {
		t.Fatalf("segment metadata deletes = %d, want %d", got, segmentCount)
	}
	if got := segmentPersist.getRequests.Load(); got != modeledSegmentGetRequests {
		t.Fatalf("segment metadata get requests = %d, want %d", got, modeledSegmentGetRequests)
	}
	if got := segmentPersist.txnRequests.Load(); got != modeledSegmentTxnRequests {
		t.Fatalf("segment metadata txn requests = %d, want %d", got, modeledSegmentTxnRequests)
	}
	if remaining := m.segments.Len(); remaining != 0 {
		t.Fatalf("segments remaining after GC = %d, want 0", remaining)
	}
	if remaining := len(indexMeta.segmentBuildInfo.List()); remaining != 0 {
		t.Fatalf("SegmentIndex entries remaining after GC = %d, want 0", remaining)
	}

	t.Logf(
		"DROPPED_SEGMENT_GC_PERF result segments=%d collections=%d channel_state_batch_size=%d native_file_batch=%t batch_size=%d dataview_requests=%d channel_requests=%d logical_file_deletes=%d native_file_requests=%d segment_index_kv_deletes=%d segment_index_kv_txn_requests=%d segment_kv_get_requests=%d segment_kv_txn_requests=%d elapsed=%s throughput=%.0f_segments/s latency_applied=%t modeled_serial_io=%s modeled_batched_io=%s",
		segmentCount,
		collectionCount,
		channelStateBatchSize,
		nativeFileBatchEnabled,
		batchSize,
		dataViewCatalog.listCalls.Load(),
		segmentCatalog.channelExistenceCalls.Load(),
		chunkManager.removeCalls.Load(),
		chunkManager.batchRequestCalls.Load(),
		segmentCatalog.dropSegmentIndexCalls.Load(),
		segmentCatalog.dropSegmentIndexTxns.Load(),
		segmentPersist.getRequests.Load(),
		segmentPersist.txnRequests.Load(),
		stageElapsed,
		float64(segmentCount)/stageElapsed.Seconds(),
		applyLatency,
		modeledSerialIO,
		modeledBatchedIO,
	)

	runtime.KeepAlive(fileKeys)
	runtime.KeepAlive(m)
}
