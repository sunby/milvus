//go:build test
// +build test

package datacoord

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

// helper to build a simple SegmentInfo (non-Growing to avoid paramtable).
func newTestCachedSegment(id int64, collID int64, channel string) *SegmentInfo {
	return NewSegmentInfo(&datapb.SegmentInfo{
		ID:            id,
		CollectionID:  collID,
		InsertChannel: channel,
		State:         commonpb.SegmentState_Flushed,
	})
}

func TestCachedSegmentsInfoNew(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	require.NotNil(t, cs)
	assert.Equal(t, 0, cs.Len())
}

func TestCachedSegmentsInfoSetAndGetSegment(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	seg := newTestCachedSegment(1, 100, "ch-0")

	old, existed := cs.SetSegment(1, seg, 1)
	assert.False(t, existed)
	assert.Nil(t, old)
	assert.Equal(t, 1, cs.Len())

	got := cs.GetSegment(1)
	require.NotNil(t, got)
	assert.Equal(t, int64(1), got.GetID())
}

func TestCachedSegmentsInfoSetSegmentOverwrite(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	seg1 := newTestCachedSegment(1, 100, "ch-0")
	cs.SetSegment(1, seg1, 1)

	seg2 := NewSegmentInfo(&datapb.SegmentInfo{
		ID:            1,
		CollectionID:  100,
		InsertChannel: "ch-0",
		NumOfRows:     500,
		State:         commonpb.SegmentState_Flushed,
	})
	old, existed := cs.SetSegment(1, seg2, 2)
	assert.True(t, existed)
	require.NotNil(t, old)
	assert.Equal(t, int64(0), old.GetNumOfRows())

	got := cs.GetSegment(1)
	require.NotNil(t, got)
	assert.Equal(t, int64(500), got.GetNumOfRows())
	assert.Equal(t, 1, cs.Len())
}

func TestCachedSegmentsInfoSetSegmentReturnsOld(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	seg1 := newTestCachedSegment(1, 100, "ch-0")
	cs.SetSegment(1, seg1, 1)

	seg2 := newTestCachedSegment(1, 100, "ch-0")
	old, existed := cs.SetSegment(1, seg2, 2)
	assert.True(t, existed)
	require.NotNil(t, old)
	assert.Equal(t, int64(1), old.GetID())
}

func TestCachedSegmentsInfoGetSegmentNotFound(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	assert.Nil(t, cs.GetSegment(999))
}

func TestCachedSegmentsInfoGetSegments(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)
	cs.SetSegment(2, newTestCachedSegment(2, 100, "ch-0"), 1)
	cs.SetSegment(3, newTestCachedSegment(3, 200, "ch-1"), 1)

	segs := cs.GetSegments()
	assert.Len(t, segs, 3)

	ids := make(map[int64]bool)
	for _, s := range segs {
		ids[s.GetID()] = true
	}
	assert.True(t, ids[1])
	assert.True(t, ids[2])
	assert.True(t, ids[3])
}

func TestCachedSegmentsInfoDropSegment(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)
	cs.SetSegment(2, newTestCachedSegment(2, 100, "ch-0"), 1)
	assert.Equal(t, 2, cs.Len())

	cs.DropSegment(1, 2)
	assert.Equal(t, 1, cs.Len())
	assert.Nil(t, cs.GetSegment(1))
	assert.NotNil(t, cs.GetSegment(2))
}

func TestCachedSegmentsInfoDropSegmentTombstone(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)
	cs.DropSegment(1, 5)

	// Tombstone prevents stale insert.
	old, existed := cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 3)
	assert.False(t, existed)
	assert.Nil(t, old)
	assert.Nil(t, cs.GetSegment(1), "stale insert after drop should be rejected")

	// Higher version succeeds.
	_, _ = cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 6)
	assert.NotNil(t, cs.GetSegment(1))
}

func TestCachedSegmentsInfoDropSegmentNonExistent(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	// Should not panic.
	cs.DropSegment(999, 1)
	assert.Equal(t, 0, cs.Len())
}

func TestCachedSegmentsInfoPrune(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)
	cs.DropSegment(1, 2)
	cs.PruneSegment(1)

	// After prune, even a low-version insert succeeds.
	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)
	assert.NotNil(t, cs.GetSegment(1))
}

// --- Secondary index tests ---

func TestCachedSegmentsInfoSecondaryIndexByCollection(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)
	cs.SetSegment(2, newTestCachedSegment(2, 100, "ch-1"), 1)
	cs.SetSegment(3, newTestCachedSegment(3, 200, "ch-0"), 1)

	segs := cs.GetSegmentsBySelector(WithCollection(100))
	assert.Len(t, segs, 2)
	for _, seg := range segs {
		assert.Equal(t, int64(100), seg.GetCollectionID())
	}
}

func TestCachedSegmentsInfoSecondaryIndexByChannel(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)
	cs.SetSegment(2, newTestCachedSegment(2, 100, "ch-1"), 1)
	cs.SetSegment(3, newTestCachedSegment(3, 200, "ch-0"), 1)

	segs := cs.GetSegmentsByChannel("ch-0")
	assert.Len(t, segs, 2)
	for _, seg := range segs {
		assert.Equal(t, "ch-0", seg.GetInsertChannel())
	}
}

func TestCachedSegmentsInfoSecondaryIndexByCollectionAndChannel(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)
	cs.SetSegment(2, newTestCachedSegment(2, 100, "ch-1"), 1)
	cs.SetSegment(3, newTestCachedSegment(3, 200, "ch-0"), 1)

	segs := cs.GetSegmentsBySelector(WithCollection(100), WithChannel("ch-0"))
	assert.Len(t, segs, 1)
	assert.Equal(t, int64(1), segs[0].GetID())
}

func TestCachedSegmentsInfoSecondaryIndexAfterDrop(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)
	cs.SetSegment(2, newTestCachedSegment(2, 100, "ch-0"), 1)

	cs.DropSegment(1, 2)

	segs := cs.GetSegmentsBySelector(WithCollection(100))
	assert.Len(t, segs, 1)
	assert.Equal(t, int64(2), segs[0].GetID())

	segs = cs.GetSegmentsByChannel("ch-0")
	assert.Len(t, segs, 1)
	assert.Equal(t, int64(2), segs[0].GetID())
}

func TestCachedSegmentsInfoSecondaryIndexAfterOverwrite(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)

	// Move segment to a different channel.
	seg2 := newTestCachedSegment(1, 100, "ch-1")
	cs.SetSegment(1, seg2, 2)

	segs := cs.GetSegmentsByChannel("ch-0")
	assert.Len(t, segs, 0, "old channel index should be cleaned up")

	segs = cs.GetSegmentsByChannel("ch-1")
	assert.Len(t, segs, 1)
	assert.Equal(t, int64(1), segs[0].GetID())
}

// --- CompactionTo tests ---

func TestCachedSegmentsInfoGetCompactionTo(t *testing.T) {
	cs := NewCachedSegmentsInfo()

	// Segments 1 and 2 are compacted into segment 3.
	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)
	cs.SetSegment(2, newTestCachedSegment(2, 100, "ch-0"), 1)

	seg3 := NewSegmentInfo(&datapb.SegmentInfo{
		ID:             3,
		CollectionID:   100,
		InsertChannel:  "ch-0",
		CompactionFrom: []int64{1, 2},
		State:          commonpb.SegmentState_Flushed,
	})
	cs.SetSegment(3, seg3, 1)

	// From segment 1: compactionTo should be segment 3.
	tos, exist := cs.GetCompactionTo(1)
	assert.True(t, exist)
	require.Len(t, tos, 1)
	assert.Equal(t, int64(3), tos[0].GetID())

	// From segment 2: same.
	tos, exist = cs.GetCompactionTo(2)
	assert.True(t, exist)
	require.Len(t, tos, 1)
	assert.Equal(t, int64(3), tos[0].GetID())

	// Segment 3 itself has no compactionTo.
	tos, exist = cs.GetCompactionTo(3)
	assert.True(t, exist)
	assert.Nil(t, tos)
}

func TestCachedSegmentsInfoGetCompactionToBroken(t *testing.T) {
	cs := NewCachedSegmentsInfo()

	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)
	seg3 := NewSegmentInfo(&datapb.SegmentInfo{
		ID:             3,
		CollectionID:   100,
		InsertChannel:  "ch-0",
		CompactionFrom: []int64{1, 2}, // segment 2 does not exist
		State:          commonpb.SegmentState_Flushed,
	})
	cs.SetSegment(3, seg3, 1)

	// compactionTo for segment 1 points to segment 3, which has compactionFrom [1,2].
	// But segment 2 is missing, and segment 3 itself exists.
	// The lookup for segment 1 yields the compactionTo entry [3]. Segment 3 exists, so it returns.
	tos, exist := cs.GetCompactionTo(1)
	assert.True(t, exist)
	require.Len(t, tos, 1)

	// Now drop segment 3 to create a broken relation.
	cs.DropSegment(3, 2)

	// Segment 1 still has compactionTo=[3], but segment 3 is gone (tombstone).
	tos, exist = cs.GetCompactionTo(1)
	assert.True(t, exist)
	assert.Nil(t, tos, "broken relation should return nil")
}

func TestCachedSegmentsInfoGetCompactionToNonExistent(t *testing.T) {
	cs := NewCachedSegmentsInfo()

	tos, exist := cs.GetCompactionTo(999)
	assert.False(t, exist)
	assert.Nil(t, tos)
}

// --- GetRealSegmentsForChannel ---

func TestCachedSegmentsInfoGetRealSegmentsForChannel(t *testing.T) {
	cs := NewCachedSegmentsInfo()

	real1 := newTestCachedSegment(1, 100, "ch-0")
	fake := NewSegmentInfo(&datapb.SegmentInfo{
		ID:            2,
		CollectionID:  100,
		InsertChannel: "ch-0",
		IsFake:        true,
		State:         commonpb.SegmentState_Flushed,
	})
	real2 := newTestCachedSegment(3, 100, "ch-0")

	cs.SetSegment(1, real1, 1)
	cs.SetSegment(2, fake, 1)
	cs.SetSegment(3, real2, 1)

	segs := cs.GetRealSegmentsForChannel("ch-0")
	assert.Len(t, segs, 2, "fake segment should be filtered out")
	for _, seg := range segs {
		assert.False(t, seg.GetIsFake())
	}
}

func TestCachedSegmentsInfoGetRealSegmentsForChannelNotFound(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	segs := cs.GetRealSegmentsForChannel("unknown")
	assert.Nil(t, segs)
}

// --- GetSegmentsBySelector ---

func TestCachedSegmentsInfoGetSegmentsBySelector(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)
	cs.SetSegment(2, newTestCachedSegment(2, 100, "ch-1"), 1)
	cs.SetSegment(3, newTestCachedSegment(3, 200, "ch-0"), 1)

	// Filter with a custom SegmentFilterFunc.
	segs := cs.GetSegmentsBySelector(SegmentFilterFunc(func(s *SegmentInfo) bool {
		return s.GetID() > 1
	}))
	assert.Len(t, segs, 2)

	// No filters returns all.
	segs = cs.GetSegmentsBySelector()
	assert.Len(t, segs, 3)
}

func TestCachedSegmentsInfoGetSegmentsBySelectorEmpty(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	segs := cs.GetSegmentsBySelector(WithCollection(100))
	assert.Len(t, segs, 0)
}

// --- Local-only update tests ---

func TestCachedSegmentsInfoSetRowCount(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)

	cs.SetRowCount(1, 999)

	seg := cs.GetSegment(1)
	require.NotNil(t, seg)
	assert.Equal(t, int64(999), seg.GetNumOfRows())
}

func TestCachedSegmentsInfoSetIsCompacting(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)

	cs.SetIsCompacting(1, true)
	seg := cs.GetSegment(1)
	require.NotNil(t, seg)
	assert.True(t, seg.isCompacting)

	cs.SetIsCompacting(1, false)
	seg = cs.GetSegment(1)
	require.NotNil(t, seg)
	assert.False(t, seg.isCompacting)
}

func TestCachedSegmentsInfoSetLevel(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)

	cs.SetLevel(1, datapb.SegmentLevel_L1)
	seg := cs.GetSegment(1)
	require.NotNil(t, seg)
	assert.Equal(t, datapb.SegmentLevel_L1, seg.GetLevel())
}

func TestCachedSegmentsInfoSetDmlPosition(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)

	pos := &msgpb.MsgPosition{ChannelName: "ch-0", Timestamp: 12345}
	cs.SetDmlPosition(1, pos)

	seg := cs.GetSegment(1)
	require.NotNil(t, seg)
	assert.Equal(t, uint64(12345), seg.GetDmlPosition().GetTimestamp())
}

func TestCachedSegmentsInfoSetStartPosition(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)

	pos := &msgpb.MsgPosition{ChannelName: "ch-0", Timestamp: 111}
	cs.SetStartPosition(1, pos)

	seg := cs.GetSegment(1)
	require.NotNil(t, seg)
	assert.Equal(t, uint64(111), seg.GetStartPosition().GetTimestamp())
}

func TestCachedSegmentsInfoSetAllocations(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)

	allocs := []*Allocation{{SegmentID: 1, NumOfRows: 10, ExpireTime: 100}}
	cs.SetAllocations(1, allocs)

	seg := cs.GetSegment(1)
	require.NotNil(t, seg)
	require.Len(t, seg.allocations, 1)
	assert.Equal(t, int64(10), seg.allocations[0].NumOfRows)
}

func TestCachedSegmentsInfoAddAllocation(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)

	cs.AddAllocation(1, &Allocation{SegmentID: 1, NumOfRows: 5, ExpireTime: 50})
	cs.AddAllocation(1, &Allocation{SegmentID: 1, NumOfRows: 10, ExpireTime: 100})

	seg := cs.GetSegment(1)
	require.NotNil(t, seg)
	assert.Len(t, seg.allocations, 2)
}

func TestCachedSegmentsInfoSetFlushTime(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)

	flushTime := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	cs.SetFlushTime(1, flushTime)

	seg := cs.GetSegment(1)
	require.NotNil(t, seg)
	assert.Equal(t, flushTime, seg.lastFlushTime)
}

func TestCachedSegmentsInfoSetLastWrittenTime(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)

	before := time.Now()
	cs.SetLastWrittenTime(1)

	seg := cs.GetSegment(1)
	require.NotNil(t, seg)
	assert.False(t, seg.lastWrittenTime.Before(before))
}

func TestCachedSegmentsInfoSetLastExpire(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)

	cs.SetLastExpire(1, 9999)
	seg := cs.GetSegment(1)
	require.NotNil(t, seg)
	assert.Equal(t, uint64(9999), seg.GetLastExpireTime())
}

func TestCachedSegmentsInfoLocalUpdateOnMissingSegment(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	// These should not panic on non-existent segment.
	cs.SetRowCount(999, 100)
	cs.SetIsCompacting(999, true)
	cs.SetLevel(999, datapb.SegmentLevel_L1)
}

// --- Len ---

func TestCachedSegmentsInfoLen(t *testing.T) {
	cs := NewCachedSegmentsInfo()
	assert.Equal(t, 0, cs.Len())

	cs.SetSegment(1, newTestCachedSegment(1, 100, "ch-0"), 1)
	assert.Equal(t, 1, cs.Len())

	cs.SetSegment(2, newTestCachedSegment(2, 100, "ch-0"), 1)
	assert.Equal(t, 2, cs.Len())

	cs.DropSegment(1, 2)
	assert.Equal(t, 1, cs.Len())

	cs.DropSegment(2, 2)
	assert.Equal(t, 0, cs.Len())
}
