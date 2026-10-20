package datacoord

import (
	"context"
	"math/rand"
	"slices"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// Segment writes on the optimistic segment store.
//
// Readers use the lock-free segment cache directly. A writer takes no lock of
// the segments it changes; writers of different segments run in parallel. It
//  1. builds the new values from the cached ones;
//  2. persists them through the segment store, which compares each written
//     segment against its cached version (casSegmentStore). A conflict means
//     the store moved under the cache (another writer wrote one of the
//     segments): the store refreshes their cached values and the write fails
//     with ErrVersionConflict;
//  3. runs again from step 1 on a conflict (retrySegmentWrite), building on
//     the refreshed values, until it does not conflict or ctx ends;
//  4. publishes the persisted values with their new versions
//     (publishSegments). Publications of concurrent writers may arrive out of
//     order: the cache keeps the newer version.
//
// Lock order: segment manifest lock -> index keyLock(buildID) -> groupMu ->
// segments.pubMu.

const (
	// segmentWriteRetryMinBackoff and segmentWriteRetryMaxBackoff bound the
	// wait before a write that conflicted runs again; it doubles, jittered.
	segmentWriteRetryMinBackoff = time.Millisecond
	segmentWriteRetryMaxBackoff = time.Second
)

// retrySegmentWrite runs write, which builds a segment write from the cached
// segments and commits it, until it does not fail with a version conflict, or
// ctx ends. A conflict refreshed the cached values of the segments the write
// touched, so each run builds on what the store holds. Writes of one segment
// rarely conflict, so there is no attempt limit.
func (m *meta) retrySegmentWrite(ctx context.Context, write func() error) error {
	backoff := segmentWriteRetryMinBackoff
	for attempt := 1; ; attempt++ {
		err := write()
		if err == nil || !errors.Is(err, ErrVersionConflict) || ctx.Err() != nil {
			return err
		}
		mlog.Info(ctx, "meta update: segment meta moved under the cache, rebuilding the write",
			mlog.Int("attempt", attempt), mlog.Err(err))
		timer := time.NewTimer(backoff/2 + time.Duration(rand.Int63n(int64(backoff/2)+1)))
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return err
		}
		backoff = min(2*backoff, segmentWriteRetryMaxBackoff)
	}
}

// store returns the segment store, defaulting to the catalog for a meta built
// without one (unit tests construct meta directly).
func (m *meta) store() segmentStore {
	m.segmentStoreOnce.Do(func() {
		if m.segmentStore == nil {
			m.segmentStore = newCatalogSegmentStore(func() metastore.DataCoordCatalog { return m.catalog })
		}
	})
	return m.segmentStore
}

// publishSegment installs the persisted value of one segment.
func (m *meta) publishSegment(segment *SegmentInfo, version int64) *SegmentInfo {
	previous, _ := m.segments.PublishSegment(segment.GetID(), segment, version)
	m.notifyStatsChange(previous, segment)
	return previous
}

// publishSegments installs the persisted values of a mutation and returns the
// values they replaced. A multi-segment mutation is installed atomically
// (CachedSegmentsInfo.PublishSegments), so a consistent reader observes all
// of it or none of it.
func (m *meta) publishSegments(segments []*SegmentInfo, versions map[int64]int64) map[int64]*SegmentInfo {
	previous := m.segments.PublishSegments(segments, versions)
	// Stats discovery hints go out once the whole mutation is visible.
	for _, segment := range segments {
		m.notifyStatsChange(previous[segment.GetID()], segment)
	}
	return previous
}

// selectSegmentsConsistent is SelectSegments for readers that must not observe
// a half-installed multi-segment mutation.
func (m *meta) selectSegmentsConsistent(ctx context.Context, filters ...SegmentFilter) []*SegmentInfo {
	var segments []*SegmentInfo
	m.segments.ReadConsistent(func() {
		segments = m.SelectSegments(ctx, filters...)
	})
	return segments
}

// withCachedVersions adds to base the cached version of each of segments, 0
// for a segment the cache does not hold. It is the base of the segments a
// write creates under IDs no other write uses: an earlier run of the write
// may have stored them already.
func (m *meta) withCachedVersions(base map[int64]int64, segments ...*SegmentInfo) map[int64]int64 {
	for _, segment := range segments {
		_, base[segment.GetID()] = m.segments.GetSegmentWithVersion(segment.GetID())
	}
	return base
}

// runOperators builds the update pack of operators from the cached segments.
func (m *meta) runOperators(operators []UpdateOperator) (*updateSegmentPack, error) {
	pack := m.newUpdateSegmentPack()
	if err := pack.run(operators); err != nil {
		return nil, err
	}
	return pack, nil
}

// MutateFunc modifies a *datapb.SegmentInfo in place. Returns true to proceed
// with the write, false to skip this segment's update.
type MutateFunc func(seg *datapb.SegmentInfo) bool

// MutationsOperator adapts per-segment MutateFuncs to an UpdateOperator. The
// funcs of a segment run in order against its latest cached value; when any of
// them returns false the segment is left untouched by this operator.
func MutationsOperator(mutations map[int64][]MutateFunc) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segmentIDs := make([]int64, 0, len(mutations))
		for segmentID := range mutations {
			segmentIDs = append(segmentIDs, segmentID)
		}
		slices.Sort(segmentIDs)
		updated := false
		for _, segmentID := range segmentIDs {
			_, inPack := modPack.segments[segmentID]
			segment := modPack.Get(segmentID)
			if segment == nil {
				continue
			}
			mutated := proto.Clone(segment.SegmentInfo).(*datapb.SegmentInfo)
			apply := true
			for _, fn := range mutations[segmentID] {
				if !fn(mutated) {
					apply = false
					break
				}
			}
			if !apply {
				if !inPack {
					delete(modPack.segments, segmentID)
				}
				continue
			}
			segment.SegmentInfo = mutated
			updated = true
		}
		return updated
	}
}

// AddSegmentsOperator inserts new segments; a segment that already exists in
// meta is left as it is.
func AddSegmentsOperator(segments ...*datapb.SegmentInfo) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		added := false
		for _, segment := range segments {
			if modPack.Peek(segment.GetID()) != nil {
				continue
			}
			info := NewSegmentInfo(segment)
			modPack.segments[segment.GetID()] = info
			modPack.metricMutation.addNewSeg(info.GetState(), info.GetLevel(), info.GetIsSorted(), info.GetStorageVersion(), segmentMetricFormatLabel(info), info.GetNumOfRows())
			added = true
		}
		return added
	}
}
