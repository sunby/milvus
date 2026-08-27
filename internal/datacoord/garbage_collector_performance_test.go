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
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"

	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	runLargeGCPerfTestEnv = "MILVUS_RUN_LARGE_GC_PERF_TEST"
	gcPerfCollectionsEnv  = "MILVUS_GC_PERF_COLLECTIONS"
	gcPerfSegmentsEnv     = "MILVUS_GC_PERF_SEGMENTS"
	gcPerfDroppedEveryEnv = "MILVUS_GC_PERF_DROPPED_EVERY"
)

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
