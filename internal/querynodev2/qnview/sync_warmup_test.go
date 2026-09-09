//go:build dynamic && test

package qnview

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestSyncWarmupExecutionAndQualification(t *testing.T) {
	ctx := context.Background()
	snapshot := testSegmentLoadSnapshot(1000, 10)
	loader := &fakePhysicalLoader{loadFn: func(info *querypb.SegmentLoadInfo, _ CollectionRuntime) (TransformSegment, error) {
		require.True(t, info.GetForceSyncWarmup())
		return &fakeTransformSegment{id: 1000}, nil
	}}
	var loaded TransformSegment
	task := newSegmentLoadTask(loader, nil, SegmentLoadTask{
		Context: ctx, SegmentID: 1000, SyncWarmupEpoch: 42,
		Collection: &fakeCollectionRuntimeGuard{collectionID: testCollectionID},
		Snapshot:   snapshot, OnLoaded: func(segment TransformSegment) { loaded = segment },
	})
	require.NoError(t, task.Execute(ctx))
	require.True(t, satisfiesSyncWarmup(loaded, 42))
	require.False(t, satisfiesSyncWarmup(loaded, 43))
	require.True(t, satisfiesSyncWarmup(loaded, 0))
	require.False(t, snapshot.LoadInfo.GetForceSyncWarmup(), "shared watch snapshot was mutated")
	require.EqualValues(t, 1000, UnwrapTransformSegment(loaded).ID())
}

func TestSyncWarmupLoadFailureCannotQualify(t *testing.T) {
	called := false
	loader := &fakePhysicalLoader{loadFn: func(*querypb.SegmentLoadInfo, CollectionRuntime) (TransformSegment, error) {
		return nil, context.DeadlineExceeded
	}}
	task := newSegmentLoadTask(loader, nil, SegmentLoadTask{
		SegmentID: 1000, SyncWarmupEpoch: 42,
		Collection: &fakeCollectionRuntimeGuard{collectionID: testCollectionID},
		Snapshot:   testSegmentLoadSnapshot(1000, 10), OnLoaded: func(TransformSegment) { called = true },
	})
	require.ErrorIs(t, task.Execute(context.Background()), context.DeadlineExceeded)
	require.False(t, called)
}

func TestSyncWarmupRequirementValidation(t *testing.T) {
	require.NoError(t, validateSyncWarmupRequirement(&viewpb.QueryViewMeta{}))
	require.NoError(t, validateSyncWarmupRequirement(&viewpb.QueryViewMeta{SyncWarmup: true, SyncWarmupEpoch: 42}))
	require.Error(t, validateSyncWarmupRequirement(&viewpb.QueryViewMeta{SyncWarmup: true}))
	require.Error(t, validateSyncWarmupRequirement(&viewpb.QueryViewMeta{SyncWarmupEpoch: 42}))
}

func TestSyncWarmupLateLoadCannotReplaceNewAttempt(t *testing.T) {
	old := &physicalSegmentState{loading: true, loadEpoch: 1}
	current := &physicalSegmentState{loading: true, loadEpoch: 1}
	manager := &ViewScopedPhysicalSegmentManager{segments: map[int64]*physicalSegmentState{1000: current}}
	segment := &syncWarmedSegment{TransformSegment: &fakeTransformSegment{id: 1000}, epoch: 42}
	notifications, retries, kept := manager.completePhysicalSegmentLoad(segment, segmentLoadSubmission{segmentID: 1000, state: old, epoch: 1})
	require.False(t, kept)
	require.Empty(t, notifications)
	require.Empty(t, retries)
	require.Nil(t, current.segment)
	require.True(t, current.loading)
}

func TestSyncWarmupCancellationAfterNativeCompletionCannotQualify(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loader := &fakePhysicalLoader{loadFn: func(*querypb.SegmentLoadInfo, CollectionRuntime) (TransformSegment, error) {
		cancel()
		return &fakeTransformSegment{id: 1000}, nil
	}}
	called := false
	task := newSegmentLoadTask(loader, nil, SegmentLoadTask{
		Context: ctx, SegmentID: 1000, SyncWarmupEpoch: 42,
		Snapshot: testSegmentLoadSnapshot(1000, 10),
		OnLoaded: func(TransformSegment) { called = true },
	})
	require.ErrorIs(t, task.Execute(context.Background()), context.Canceled)
	require.False(t, called)
}

func TestSyncWarmupDecoratorPreservesReadableSegmentAndTransformStart(t *testing.T) {
	collection := &segments.Collection{}
	physical := &fakeReadableTransformSegment{collection: collection}
	loader := &fakePhysicalLoader{loaded: physical}
	var loaded TransformSegment
	task := newSegmentLoadTask(loader, nil, SegmentLoadTask{
		SegmentID: 1000, SyncWarmupEpoch: 42, TransformStartAfterTimeTick: 99,
		Snapshot: testSegmentLoadSnapshot(1000, 10),
		OnLoaded: func(segment TransformSegment) { loaded = segment },
	})
	require.NoError(t, task.Execute(context.Background()))
	readable, ok := loaded.(ReadableSealedSegment)
	require.True(t, ok)
	require.Same(t, collection, readable.Collection())
	require.EqualValues(t, 99, loaded.TransformStartAfterTimeTick())
	require.Same(t, physical, UnwrapTransformSegment(loaded))
}

func TestSyncWarmupUpdateKeepsForceWithoutMutatingWatchSnapshot(t *testing.T) {
	snapshot := testSegmentLoadSnapshot(1000, 10)
	called := false
	loader := &fakePhysicalLoader{updateFn: func(_ TransformSegment, _ CollectionRuntime, next SegmentLoadInfoSnapshot, _ SegmentUpdateAction) error {
		called = true
		require.True(t, next.LoadInfo.GetForceSyncWarmup())
		return nil
	}}
	task := newSegmentUpdateTask(loader, SegmentUpdateTask{
		Segment:  &syncWarmedSegment{TransformSegment: &fakeTransformSegment{id: 1000}, epoch: 42},
		Snapshot: snapshot,
	})
	require.NoError(t, task.Execute(context.Background()))
	require.True(t, called)
	require.False(t, snapshot.LoadInfo.GetForceSyncWarmup())
}

func TestSyncWarmupWaitsForOldQueryReferencesBeforeActivating(t *testing.T) {
	meta := buildHandlerTestMeta(2)
	meta.SyncWarmup, meta.SyncWarmupEpoch = true, 42
	view := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}}}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	old := &transformSegmentState{
		state: transformSegmentLoaded, segment: &fakeTransformSegment{id: 1000}, queryRefs: 1,
		refs: make(map[qviews.QueryViewKey]struct{}), waiters: make(map[qviews.QueryViewKey]transformSegmentWaiter),
	}
	m := &QueryViewSegmentReadinessManager{
		views: make(map[qviews.QueryViewKey]*transformViewRef), segments: map[int64]*transformSegmentState{1000: old},
	}
	req := AcquireSegments{Key: key, Meta: meta, View: view}
	ref, ok := m.recordPendingAcquire(req, func() {}, nil)
	require.True(t, ok)
	require.Empty(t, old.refs, "new view must not pin the incompatible old lifecycle")
	guard := &fakeCollectionRuntimeGuard{collectionID: testCollectionID}
	activation, err := m.activateAcquire(req, ref, guard)
	require.NoError(t, err)
	require.True(t, activation.current)
	require.True(t, activation.blocked)
	require.Empty(t, activation.readyNow)
	queryView := qviews.NewQueryViewAtQueryNode(meta, view).(*qviews.QueryViewAtQueryNode)
	require.ErrorIs(t, m.continueAcquire(req, ref, queryView, context.Background(), func() {}), nodescheduler.ErrDelay)
	m.releaseSealedSegmentHandle(1000)
	require.NotContains(t, m.segments, int64(1000))
	activation, err = m.activateAcquire(req, ref, guard)
	require.NoError(t, err)
	require.True(t, activation.current)
	require.False(t, activation.blocked)
	require.Empty(t, activation.readyNow)
	require.Equal(t, []int64{1000}, activation.physicalRefSegments)
	require.EqualValues(t, 42, m.segments[1000].warmupEpoch)
	kept, schedule := m.markPhysicalLoaded(&fakeTransformSegment{id: 1000})
	require.False(t, kept, "late ordinary load cannot install into the new state")
	require.False(t, schedule)
}

func TestSyncWarmupRejectsIncompatibleActiveViewOwnership(t *testing.T) {
	meta := buildHandlerTestMeta(2)
	meta.SyncWarmup, meta.SyncWarmupEpoch = true, 42
	view := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}}}
	queryView := qviews.NewQueryViewAtQueryNode(meta, view).(*qviews.QueryViewAtQueryNode)
	key := queryView.QueryViewKey()
	oldKey := qviews.NewQueryViewAtQueryNode(buildHandlerTestMeta(1), view).QueryViewKey()
	old := &transformSegmentState{
		state: transformSegmentLoaded, segment: &fakeTransformSegment{id: 1000},
		refs: map[qviews.QueryViewKey]struct{}{oldKey: {}}, waiters: make(map[qviews.QueryViewKey]transformSegmentWaiter),
	}
	m := &QueryViewSegmentReadinessManager{
		views: map[qviews.QueryViewKey]*transformViewRef{oldKey: {}}, segments: map[int64]*transformSegmentState{1000: old},
	}
	failed := false
	req := AcquireSegments{Key: key, Meta: meta, View: view, OnUnrecoverable: func() { failed = true }}
	ref, ok := m.recordPendingAcquire(req, func() {}, nil)
	require.True(t, ok)
	err := m.continueAcquire(req, ref, queryView, context.Background(), func() {})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.True(t, failed)
	require.NotContains(t, m.views, key)
	require.Contains(t, m.views, oldKey)
	require.Same(t, old, m.segments[1000])
	require.Len(t, old.refs, 1)
}

func TestSyncWarmupLateViewFailureCannotFailReplacement(t *testing.T) {
	key := qviews.QueryViewKey{}
	old := &transformViewRef{}
	called := false
	current := &transformViewRef{onUnrecoverable: func() { called = true }}
	m := &QueryViewSegmentReadinessManager{views: map[qviews.QueryViewKey]*transformViewRef{key: current}}
	m.failView(key, old)
	require.False(t, current.unrecoverable)
	require.False(t, called)
	m.failView(key, current)
	require.True(t, current.unrecoverable)
	require.True(t, called)
}

func TestSyncWarmupLateCatchupCannotReadyReplacement(t *testing.T) {
	old := &syncWarmedSegment{TransformSegment: &fakeTransformSegment{id: 1000}, epoch: 42}
	current := &syncWarmedSegment{TransformSegment: &fakeTransformSegment{id: 1000}, epoch: 42}
	state := &transformSegmentState{warmupEpoch: 42, state: transformSegmentCatchingUp, segment: current}
	m := &QueryViewSegmentReadinessManager{segments: map[int64]*transformSegmentState{1000: state}}
	require.Empty(t, m.markSegmentReady(old))
	require.Equal(t, transformSegmentCatchingUp, state.state)
}

func TestSyncWarmupLateFailureResetCannotRemoveReplacement(t *testing.T) {
	old := &syncWarmedSegment{TransformSegment: &fakeTransformSegment{id: 1000}, epoch: 42}
	current := &syncWarmedSegment{TransformSegment: &fakeTransformSegment{id: 1000}, epoch: 43}
	key := qviews.QueryViewKey{}
	state := &physicalSegmentState{segment: current, refs: map[qviews.QueryViewKey]struct{}{key: {}}}
	ref := &viewRef{segments: map[int64]int64{1000: 10}}
	m := &ViewScopedPhysicalSegmentManager{
		segments: map[int64]*physicalSegmentState{1000: state}, views: map[qviews.QueryViewKey]*viewRef{key: ref},
	}
	m.ResetSegment(old)
	require.Same(t, state, m.segments[1000])
	require.Contains(t, ref.segments, int64(1000))
	m.ResetSegment(current)
	require.NotContains(t, m.segments, int64(1000))
	require.NotContains(t, ref.segments, int64(1000))
}
