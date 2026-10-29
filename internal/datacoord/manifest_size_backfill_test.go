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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestManifestSizeBackfillConfiguration(t *testing.T) {
	p := &paramtable.ComponentParam{}
	p.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	m := &meta{chunkManager: mocks.NewChunkManager(t)}
	backfill, err := newManifestSizeBackfill(p, m)
	require.NoError(t, err)
	require.Nil(t, backfill)
	require.NoError(t, p.Save(p.DataCoordCfg.ManifestSizeBackfillEnabled.Key, "true"))
	require.NoError(t, p.Save(p.CommonCfg.StorageType.Key, "local"))
	backfill, err = newManifestSizeBackfill(p, m)
	require.NoError(t, err)
	require.Nil(t, backfill)
	require.NoError(t, p.Save(p.CommonCfg.StorageType.Key, "remote"))
	for _, concurrency := range []string{"0", "257", "garbage"} {
		require.NoError(t, p.Save(p.DataCoordCfg.ManifestSizeBackfillConcurrency.Key, concurrency))
		_, err = newManifestSizeBackfill(p, m)
		require.Error(t, err, concurrency)
	}
	require.NoError(t, p.Save(p.DataCoordCfg.ManifestSizeBackfillConcurrency.Key, "16"))
	backfill, err = newManifestSizeBackfill(p, m)
	require.NoError(t, err)
	require.Equal(t, 16, backfill.concurrency)
}

// newManifestSizeTestMeta returns a meta holding segments of collection 1
// under remote storage, Flushed StorageV3 unless they say otherwise, with a
// recorder of the QueryView notifications.
func newManifestSizeTestMeta(t *testing.T, segments ...*datapb.SegmentInfo) (*meta, *queryViewLoadInfoNotificationRecorder) {
	storageType := Params.CommonCfg.StorageType.SwapTempValue("remote")
	t.Cleanup(func() { Params.CommonCfg.StorageType.SwapTempValue(storageType) })
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	for _, segment := range segments {
		segment.CollectionID, segment.PartitionID, segment.StorageVersion = 1, 10, storage.StorageV3
		if segment.State == commonpb.SegmentState_SegmentStateNone {
			segment.State = commonpb.SegmentState_Flushed
		}
		require.NoError(t, m.AddSegment(context.Background(), NewSegmentInfo(segment)))
	}
	recorder := newQueryViewLoadInfoNotificationRecorder()
	m.queryViewLoadInfoNotifier = recorder
	return m, recorder
}

func manifestSizeTestBase(segmentID int64) string {
	return fmt.Sprintf("files/insert_log/1/10/%d", segmentID)
}

// The backfill sizes the revision each live segment points to when its path
// records no size, and records the size only while the segment still points
// to that revision, once; QueryView does not hear of it.
func TestManifestSizeBackfill(t *testing.T) {
	ctx := context.Background()
	unsized := func(segmentID int64) string { return packed.MarshalManifestPath(manifestSizeTestBase(segmentID), 4) }
	sized := packed.CommittedManifestPath(manifestSizeTestBase(2), 4, 4096, "remote")
	m, recorder := newManifestSizeTestMeta(t,
		&datapb.SegmentInfo{ID: 1, ManifestPath: unsized(1)},
		&datapb.SegmentInfo{ID: 2, ManifestPath: sized},
		&datapb.SegmentInfo{ID: 3, ManifestPath: unsized(3), State: commonpb.SegmentState_Dropped},
		&datapb.SegmentInfo{ID: 4, ManifestPath: packed.MarshalManifestPath(manifestSizeTestBase(4), packed.ManifestEarliest)},
		&datapb.SegmentInfo{ID: 5, ManifestPath: unsized(5)},
		&datapb.SegmentInfo{ID: 6, ManifestPath: unsized(6)},
		&datapb.SegmentInfo{ID: 7, ManifestPath: unsized(7), IsImporting: true},
	)
	segmentOf := make(map[string]int64)
	for _, segmentID := range []int64{1, 5, 6} {
		key, err := packed.ManifestFilePath(unsized(segmentID))
		require.NoError(t, err)
		segmentOf[key] = segmentID
	}
	moved := packed.MarshalManifestPath(manifestSizeTestBase(6), 5)
	var mu sync.Mutex
	var sizedSegments []int64
	b := &manifestSizeBackfill{meta: m, concurrency: 2, size: func(ctx context.Context, key string) (int64, error) {
		mu.Lock()
		sizedSegments = append(sizedSegments, segmentOf[key])
		mu.Unlock()
		switch segmentOf[key] {
		case 5:
			return 0, errors.New("storage unavailable")
		case 6:
			// A commit moves the segment to its next revision while the
			// backfill sizes the previous one.
			_, err := m.updateSegmentsInfo(ctx, UpdateManifest(6, moved))
			return 8192, err
		}
		return 1024, nil
	}}
	b.run(ctx)

	require.ElementsMatch(t, []int64{1, 5, 6}, sizedSegments)
	recorded := packed.CommittedManifestPath(manifestSizeTestBase(1), 4, 1024, "remote")
	for segmentID, path := range map[int64]string{1: recorded, 3: unsized(3), 7: unsized(7)} {
		n, err := m.RecordManifestSizes(ctx, []manifestSize{{segmentID: segmentID, manifestPath: path, size: 8192}})
		require.NoError(t, err)
		require.Zero(t, n, "a size lands once, on a live segment")
	}
	for segmentID, path := range map[int64]string{
		1: recorded,
		2: sized,
		3: unsized(3),
		4: packed.MarshalManifestPath(manifestSizeTestBase(4), packed.ManifestEarliest),
		5: unsized(5),
		6: moved,
		7: unsized(7),
	} {
		require.Equal(t, path, m.segments.GetSegment(segmentID).GetManifestPath(), segmentID)
	}
	require.Empty(t, recorder.segments())
}

// A commit that republishes the current revision under another spelling, such
// as one recording its size, does not change the manifest for QueryView.
func TestCommitSegmentManifestRespelling(t *testing.T) {
	ctx := context.Background()
	m, recorder := newManifestSizeTestMeta(t, &datapb.SegmentInfo{ID: 10, ManifestPath: packed.MarshalManifestPath(manifestSizeTestBase(10), 1)})
	commit := func(manifestPath string) {
		require.NoError(t, m.CommitSegmentManifest(ctx, SegmentManifestCommit{
			SegmentID: 10, Mutation: ManifestMutation{Type: ManifestMutationNoop, ManifestPath: manifestPath},
		}))
		require.Equal(t, manifestPath, m.GetSegment(ctx, 10).GetManifestPath())
	}
	commit(packed.CommittedManifestPath(manifestSizeTestBase(10), 1, 4096, "remote"))
	require.Empty(t, recorder.segments())
	commit(packed.MarshalManifestPath(manifestSizeTestBase(10), 2))
	require.Len(t, recorder.segments(), 1)
}

// A run that keeps failing stops, so the prewarm waiting for it can start.
func TestManifestSizeBackfillStopsOnFailures(t *testing.T) {
	segments := make([]*datapb.SegmentInfo, 0, 2*manifestSizeBackfillMaxConsecutiveFailures)
	for segmentID := int64(1); segmentID <= 2*manifestSizeBackfillMaxConsecutiveFailures; segmentID++ {
		segments = append(segments, &datapb.SegmentInfo{ID: segmentID, ManifestPath: packed.MarshalManifestPath(manifestSizeTestBase(segmentID), 4)})
	}
	m, _ := newManifestSizeTestMeta(t, segments...)
	var calls atomic.Int64
	b := &manifestSizeBackfill{meta: m, concurrency: 1, size: func(context.Context, string) (int64, error) {
		calls.Add(1)
		return 0, errors.New("storage unavailable")
	}}
	b.run(context.Background())
	// The run checks before it starts each request, while the one before it
	// may still be failing.
	require.LessOrEqual(t, calls.Load(), int64(manifestSizeBackfillMaxConsecutiveFailures+1))
}

// The Talon prewarm starts once the manifest size backfill finishes, so its
// first round loads the manifests the backfill sized without a stat.
func TestManifestSizeBackfillPrecedesTalonPrewarm(t *testing.T) {
	s, c := newPrewarmStartTestServer(t, packed.MarshalManifestPath("files/insert_log/10/20/1", 4))
	sizing, release := make(chan struct{}), make(chan struct{})
	s.manifestSizeBackfill = &manifestSizeBackfill{meta: s.meta, concurrency: 1, size: func(ctx context.Context, _ string) (int64, error) {
		close(sizing)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return 0, errors.New("storage unavailable")
	}}

	require.NoError(t, s.Start())
	select {
	case <-sizing:
	case <-time.After(time.Second):
		t.Fatal("DC did not start the manifest size backfill after becoming Healthy")
	}
	s.talonPrewarmInspector.mu.Lock()
	started := s.talonPrewarmInspector.started
	s.talonPrewarmInspector.mu.Unlock()
	require.False(t, started, "the prewarm started before the backfill finished")
	close(release)
	select {
	case <-c.pending:
	case <-time.After(time.Second):
		t.Fatal("DC did not prewarm after the manifest size backfill")
	}
}
