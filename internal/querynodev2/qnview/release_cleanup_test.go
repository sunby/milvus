//go:build test && dynamic

package qnview

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type cleanupTestSegment struct {
	fakeTransformSegment
	started   chan struct{}
	resume    <-chan struct{}
	once      sync.Once
	releases  atomic.Int32
	onRelease func()
}

func (s *cleanupTestSegment) Release(context.Context) error {
	s.once.Do(func() {
		if s.onRelease != nil {
			s.onRelease()
		}
		if s.started != nil {
			close(s.started)
		}
		if s.resume != nil {
			<-s.resume
		}
		s.releases.Add(1)
	})
	return nil
}

type cleanupTestBuffer struct{}

func (cleanupTestBuffer) Acquire(context.Context, *qviews.QueryViewAtQueryNode) (TransformLogGuard, error) {
	return instantTransformGuard{}, nil
}

func (cleanupTestBuffer) RegisterSegment(context.Context, TransformSegment) (TransformRegistration, error) {
	return instantTransformRegistration{}, nil
}

func awaitCleanupEvent[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for cleanup event")
		var zero T
		return zero
	}
}

func cleanupTestView(version, segmentID int64, state viewpb.QueryViewState) qviews.QueryViewAtWorkNode {
	meta := buildHandlerTestMeta(version)
	meta.State = state
	return qviews.NewQueryViewAtQueryNode(meta, &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{segmentID}}},
	})
}

func TestQNHandler_DropCleanupDoesNotBlockNextPrepare(t *testing.T) {
	resume := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	t.Cleanup(unblock)
	old := &cleanupTestSegment{fakeTransformSegment: fakeTransformSegment{id: 1000}, started: make(chan struct{}), resume: resume}
	fresh := &cleanupTestSegment{fakeTransformSegment: fakeTransformSegment{id: 2000}}
	physical := fakePhysicalSegmentManager{
		acquire: func(req AcquirePhysicalSegments) {
			segment := old
			if req.View.GetPartitions()[0].GetSegmentIds()[0] == fresh.ID() {
				segment = fresh
			}
			req.OnLoaded([]TransformSegment{segment})
		},
		release: func(req ReleaseSegments) { req.OnDropped() },
	}
	mgr := newTestQueryViewSegmentReadinessManager(t, physical, cleanupTestBuffer{})
	h := NewQNQueryViewHandler(mgr)
	reports := make(chan qviews.QueryViewAtWorkNode, 8)
	apply := func(view qviews.QueryViewAtWorkNode) {
		h.ApplyViews([]handler.ApplyView{{View: view, OnReport: func(report qviews.QueryViewAtWorkNode) { reports <- report }}})
	}
	apply(cleanupTestView(1, old.ID(), viewpb.QueryViewState_QueryViewStatePreparing))
	require.Equal(t, qviews.QueryViewStateReady, awaitCleanupEvent(t, reports).State())

	// Match the serial receive path: the next Preparing cannot be dispatched
	// until ApplyViews returns for the preceding Dropped update.
	dispatched := make(chan struct{})
	go func() {
		apply(cleanupTestView(1, old.ID(), viewpb.QueryViewState_QueryViewStateDropped))
		apply(cleanupTestView(2, fresh.ID(), viewpb.QueryViewState_QueryViewStatePreparing))
		close(dispatched)
	}()
	awaitCleanupEvent(t, old.started)
	awaitCleanupEvent(t, dispatched)
	report := awaitCleanupEvent(t, reports)
	require.Equal(t, cleanupTestView(2, fresh.ID(), viewpb.QueryViewState_QueryViewStatePreparing).QueryViewKey(), report.QueryViewKey())
	require.Equal(t, qviews.QueryViewStateReady, report.State())
	require.Zero(t, old.releases.Load(), "Dropped must wait for actual cleanup")
	unblock()
	require.Equal(t, qviews.QueryViewStateDropped, awaitCleanupEvent(t, reports).State())
	require.EqualValues(t, 1, old.releases.Load())
	apply(cleanupTestView(2, fresh.ID(), viewpb.QueryViewState_QueryViewStateDropped))
	require.Equal(t, qviews.QueryViewStateDropped, awaitCleanupEvent(t, reports).State())
}

func acquireCleanupView(t *testing.T, mgr *QueryViewSegmentReadinessManager, version, segmentID int64) (qviews.QueryViewKey, <-chan map[int64][]int64) {
	t.Helper()
	view := cleanupTestView(version, segmentID, viewpb.QueryViewState_QueryViewStatePreparing).(*qviews.QueryViewAtQueryNode)
	ready := make(chan map[int64][]int64, 2)
	mgr.Acquire(AcquireSegments{
		Key: view.QueryViewKey(), Meta: view.IntoProto().GetMeta(), View: view.ViewOfQueryNode(),
		OnReady: func(segments map[int64][]int64) { ready <- segments },
	})
	return view.QueryViewKey(), ready
}

func releaseCleanupView(mgr *QueryViewSegmentReadinessManager, key qviews.QueryViewKey) <-chan struct{} {
	dropped := make(chan struct{}, 2)
	mgr.Release(ReleaseSegments{Key: key, OnDropped: func() { dropped <- struct{}{} }})
	return dropped
}

func TestQueryViewCleanup_BoundedWorkersDoNotStarveLoads(t *testing.T) {
	const count = segmentCleanupConcurrency * 3
	resume := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	t.Cleanup(unblock)
	entered := make(chan struct{}, count)
	segments := make([]*cleanupTestSegment, count)
	for i := range segments {
		segments[i] = &cleanupTestSegment{
			fakeTransformSegment: fakeTransformSegment{id: int64(i + 1)}, resume: resume,
			onRelease: func() { entered <- struct{}{} },
		}
	}
	physical := fakePhysicalSegmentManager{
		acquire: func(req AcquirePhysicalSegments) {
			id := req.View.GetPartitions()[0].GetSegmentIds()[0]
			if id <= count {
				req.OnLoaded([]TransformSegment{segments[id-1]})
			} else {
				req.OnLoaded([]TransformSegment{&cleanupTestSegment{fakeTransformSegment: fakeTransformSegment{id: id}}})
			}
		},
		release: func(req ReleaseSegments) { req.OnDropped() },
	}
	mgr := newTestQueryViewSegmentReadinessManager(t, physical, cleanupTestBuffer{})
	drops := make([]<-chan struct{}, count)
	for i := range segments {
		key, ready := acquireCleanupView(t, mgr, int64(i+1), segments[i].ID())
		awaitCleanupEvent(t, ready)
		drops[i] = releaseCleanupView(mgr, key)
	}
	for i := 0; i < segmentCleanupConcurrency; i++ {
		awaitCleanupEvent(t, entered)
	}
	mgr.mu.Lock()
	workers, queued := mgr.cleanupWorkers, len(mgr.cleanupTasks)
	mgr.mu.Unlock()
	require.Equal(t, segmentCleanupConcurrency, workers)
	require.Equal(t, count-segmentCleanupConcurrency, queued)
	select {
	case <-entered:
		t.Fatal("cleanup exceeded its worker limit")
	default:
	}
	key, ready := acquireCleanupView(t, mgr, count+1, count+1)
	awaitCleanupEvent(t, ready)
	for _, dropped := range drops {
		select {
		case <-dropped:
			t.Fatal("Dropped reported before cleanup completed")
		default:
		}
	}
	unblock()
	for i, dropped := range drops {
		awaitCleanupEvent(t, dropped)
		require.EqualValues(t, 1, segments[i].releases.Load())
		select {
		case <-dropped:
			t.Fatal("duplicate Dropped callback")
		default:
		}
	}
	awaitCleanupEvent(t, releaseCleanupView(mgr, key))
	require.Eventually(t, func() bool {
		mgr.mu.Lock()
		defer mgr.mu.Unlock()
		return mgr.cleanupWorkers == 0 && mgr.cleanupTasks == nil
	}, time.Second, time.Millisecond)
}

// Keep the production physical manager and its callback accounting, but let
// tests choose exactly when each physical load completes.
type cleanupLoadScheduler struct {
	nodescheduler.Scheduler
	loads chan *SegmentLoadTask
}

func (s cleanupLoadScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
	if load, ok := task.(*SegmentLoadTask); ok {
		s.loads <- load
		return noopNodeTaskHandle{}
	}
	return s.Scheduler.Submit(task)
}

func TestQueryViewCleanup_ReloadSameSegmentWhileOldDestructionBlocks(t *testing.T) {
	scheduler := nodescheduler.New(2)
	t.Cleanup(scheduler.Close)
	loads := make(chan *SegmentLoadTask, 8)
	physical := NewViewScopedPhysicalSegmentManagerWithNodeScheduler(cleanupLoadScheduler{Scheduler: scheduler, loads: loads}, &fakePhysicalLoader{})
	mgr := NewQueryViewSegmentReadinessManagerWithScheduler(scheduler, physical, cleanupTestBuffer{})
	resume := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	t.Cleanup(unblock)
	old := &cleanupTestSegment{fakeTransformSegment: fakeTransformSegment{id: 1000}, started: make(chan struct{}), resume: resume}
	key1, ready1 := acquireCleanupView(t, mgr, 1, old.ID())
	awaitCleanupEvent(t, loads).OnLoaded(old)
	awaitCleanupEvent(t, ready1)
	dropped1 := releaseCleanupView(mgr, key1)
	awaitCleanupEvent(t, old.started)

	key2, ready2 := acquireCleanupView(t, mgr, 2, old.ID())
	fresh := &cleanupTestSegment{fakeTransformSegment: fakeTransformSegment{id: old.ID()}}
	awaitCleanupEvent(t, loads).OnLoaded(fresh)
	awaitCleanupEvent(t, ready2)
	unblock()
	awaitCleanupEvent(t, dropped1)
	mgr.mu.Lock()
	current := mgr.segments[fresh.ID()].segment
	mgr.mu.Unlock()
	require.Same(t, fresh, current)
	require.Zero(t, fresh.releases.Load())
	awaitCleanupEvent(t, releaseCleanupView(mgr, key2))
	require.EqualValues(t, 1, fresh.releases.Load())
}

func TestQueryViewCleanup_LateLoadCannotPopulateReplacementLifecycle(t *testing.T) {
	requests := make(chan AcquirePhysicalSegments, 4)
	physical := fakePhysicalSegmentManager{
		acquire: func(req AcquirePhysicalSegments) { requests <- req },
		release: func(req ReleaseSegments) { req.OnDropped() },
	}
	mgr := newTestQueryViewSegmentReadinessManager(t, physical, cleanupTestBuffer{})
	key1, ready1 := acquireCleanupView(t, mgr, 1, 1000)
	oldRequest := awaitCleanupEvent(t, requests)
	awaitCleanupEvent(t, releaseCleanupView(mgr, key1))
	key2, ready2 := acquireCleanupView(t, mgr, 2, 1000)
	newRequest := awaitCleanupEvent(t, requests)
	old := &cleanupTestSegment{fakeTransformSegment: fakeTransformSegment{id: 1000}}
	oldRequest.OnLoaded([]TransformSegment{old})
	require.EqualValues(t, 1, old.releases.Load())
	select {
	case <-ready1:
		t.Fatal("released view became Ready")
	case <-ready2:
		t.Fatal("old load made replacement view Ready")
	default:
	}
	fresh := &cleanupTestSegment{fakeTransformSegment: fakeTransformSegment{id: 1000}}
	newRequest.OnLoaded([]TransformSegment{fresh})
	awaitCleanupEvent(t, ready2)
	mgr.mu.Lock()
	current := mgr.segments[1000].segment
	mgr.mu.Unlock()
	require.Same(t, fresh, current)
	awaitCleanupEvent(t, releaseCleanupView(mgr, key2))
}

func TestQueryViewCleanup_LatePhysicalCompletionCannotAffectReplacement(t *testing.T) {
	for _, outcome := range []string{"loaded", "nil_result", "failed"} {
		t.Run(outcome, func(t *testing.T) {
			scheduler := nodescheduler.New(2)
			t.Cleanup(scheduler.Close)
			loads := make(chan *SegmentLoadTask, 8)
			physical := NewViewScopedPhysicalSegmentManagerWithNodeScheduler(cleanupLoadScheduler{Scheduler: scheduler, loads: loads}, &fakePhysicalLoader{})
			mgr := NewQueryViewSegmentReadinessManagerWithScheduler(scheduler, physical, cleanupTestBuffer{})
			key1, _ := acquireCleanupView(t, mgr, 1, 1000)
			oldLoad := awaitCleanupEvent(t, loads)
			dropped1 := releaseCleanupView(mgr, key1)
			key2, ready2 := acquireCleanupView(t, mgr, 2, 1000)
			newLoad := awaitCleanupEvent(t, loads)
			old := &cleanupTestSegment{fakeTransformSegment: fakeTransformSegment{id: 1000}}
			switch outcome {
			case "loaded":
				oldLoad.OnLoaded(old)
				require.EqualValues(t, 1, old.releases.Load())
			case "nil_result":
				oldLoad.OnLoaded(nil)
			case "failed":
				oldLoad.OnUnrecoverable(context.Canceled)
			}
			awaitCleanupEvent(t, dropped1)
			select {
			case <-ready2:
				t.Fatal("obsolete physical completion made the replacement Ready")
			default:
			}
			fresh := &cleanupTestSegment{fakeTransformSegment: fakeTransformSegment{id: 1000}}
			newLoad.OnLoaded(fresh)
			awaitCleanupEvent(t, ready2)
			// A delayed cleanup from the old instance must not reset the new one.
			physical.ResetSegment(old)
			physical.mu.Lock()
			state := physical.segments[1000]
			var current TransformSegment
			if state != nil {
				current = state.segment
			}
			physical.mu.Unlock()
			require.Same(t, fresh, current)
			require.Zero(t, fresh.releases.Load())
			awaitCleanupEvent(t, releaseCleanupView(mgr, key2))
		})
	}
}

type cleanupDelayedCatchupRegistration struct {
	instantTransformRegistration
	entered chan struct{}
	result  <-chan error
}

func (r cleanupDelayedCatchupRegistration) WaitCatchup(context.Context) error {
	close(r.entered)
	// Simulate a result already in flight when cancellation arrives.
	return <-r.result
}

type cleanupDelayedCatchupBuffer struct {
	cleanupTestBuffer
	registrations map[TransformSegment]TransformRegistration
}

func (b cleanupDelayedCatchupBuffer) RegisterSegment(_ context.Context, segment TransformSegment) (TransformRegistration, error) {
	return b.registrations[segment], nil
}

func TestQueryViewCleanup_LateCatchupCannotAffectReplacement(t *testing.T) {
	for _, outcome := range []string{"success", "canceled"} {
		t.Run(outcome, func(t *testing.T) {
			scheduler := nodescheduler.New(2)
			t.Cleanup(scheduler.Close)
			old := &cleanupTestSegment{fakeTransformSegment: fakeTransformSegment{id: 1000}}
			fresh := &cleanupTestSegment{fakeTransformSegment: fakeTransformSegment{id: 1000}}
			oldResult, newResult := make(chan error, 1), make(chan error, 1)
			t.Cleanup(func() {
				close(oldResult)
				close(newResult)
			})
			oldReg := cleanupDelayedCatchupRegistration{entered: make(chan struct{}), result: oldResult}
			newReg := cleanupDelayedCatchupRegistration{entered: make(chan struct{}), result: newResult}
			buffer := cleanupDelayedCatchupBuffer{registrations: map[TransformSegment]TransformRegistration{old: oldReg, fresh: newReg}}
			loaded := make(chan struct{}, 2)
			physical := fakePhysicalSegmentManager{
				acquire: func(req AcquirePhysicalSegments) {
					segment := old
					if req.Meta.GetVersion().GetQueryVersion() == 2 {
						segment = fresh
					}
					req.OnLoaded([]TransformSegment{segment})
					loaded <- struct{}{}
				},
				release: func(req ReleaseSegments) { req.OnDropped() },
			}
			mgr := NewQueryViewSegmentReadinessManagerWithSchedulerAndCatchupConcurrency(scheduler, physical, buffer, 1)
			key1, _ := acquireCleanupView(t, mgr, 1, 1000)
			awaitCleanupEvent(t, loaded)
			awaitCleanupEvent(t, oldReg.entered)
			awaitCleanupEvent(t, releaseCleanupView(mgr, key1))
			key2, ready2 := acquireCleanupView(t, mgr, 2, 1000)
			awaitCleanupEvent(t, loaded)
			if outcome == "canceled" {
				oldResult <- context.Canceled
			} else {
				oldResult <- nil
			}
			// With one catch-up worker, this also joins the old completion.
			awaitCleanupEvent(t, newReg.entered)
			select {
			case <-ready2:
				t.Fatal("obsolete catch-up made the replacement Ready")
			default:
			}
			newResult <- nil
			awaitCleanupEvent(t, ready2)
			require.Zero(t, fresh.releases.Load())
			awaitCleanupEvent(t, releaseCleanupView(mgr, key2))
		})
	}
}

type cleanupDelayedAcquire struct {
	PhysicalSegmentManager
	entered chan struct{}
	resume  <-chan struct{}
	calls   atomic.Int32
}

func (p *cleanupDelayedAcquire) AcquireReferences(req AcquirePhysicalSegments) func() {
	start := p.PhysicalSegmentManager.AcquireReferences(req)
	if p.calls.Add(1) != 1 {
		return start
	}
	return func() {
		close(p.entered)
		<-p.resume
		start()
	}
}

func TestQueryViewCleanup_ReleaseBeforePhysicalLoadStarts(t *testing.T) {
	scheduler := nodescheduler.New(2)
	t.Cleanup(scheduler.Close)
	loads := make(chan *SegmentLoadTask, 8)
	physical := NewViewScopedPhysicalSegmentManagerWithNodeScheduler(cleanupLoadScheduler{Scheduler: scheduler, loads: loads}, &fakePhysicalLoader{})
	resume := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	t.Cleanup(unblock)
	delayed := &cleanupDelayedAcquire{PhysicalSegmentManager: physical, entered: make(chan struct{}), resume: resume}
	collections := &fakeQueryViewCollectionRuntimeManager{}
	mgr := NewQueryViewSegmentReadinessManagerWithScheduler(scheduler, delayed, cleanupTestBuffer{}, collections)
	key1, _ := acquireCleanupView(t, mgr, 1, 1000)
	awaitCleanupEvent(t, delayed.entered)
	dropped1 := releaseCleanupView(mgr, key1)
	select {
	case <-dropped1:
		t.Fatal("Dropped reported with a pending load continuation")
	default:
	}
	collections.mu.Lock()
	guard := collections.guard
	collections.mu.Unlock()
	guard.mu.Lock()
	released := guard.released
	guard.mu.Unlock()
	require.False(t, released, "pending load still owns the collection pin")

	key2, ready2 := acquireCleanupView(t, mgr, 2, 1000)
	fresh := &cleanupTestSegment{fakeTransformSegment: fakeTransformSegment{id: 1000}}
	awaitCleanupEvent(t, loads).OnLoaded(fresh)
	awaitCleanupEvent(t, ready2)
	unblock()
	awaitCleanupEvent(t, dropped1)
	require.Zero(t, fresh.releases.Load())
	awaitCleanupEvent(t, releaseCleanupView(mgr, key2))
	physical.mu.Lock()
	require.Empty(t, physical.views)
	require.Empty(t, physical.dropping)
	require.Empty(t, physical.segments)
	physical.mu.Unlock()
}

type cleanupBlockingGuard struct {
	instantTransformGuard
	entered chan struct{}
	resume  <-chan struct{}
}

func (g cleanupBlockingGuard) Release() {
	close(g.entered)
	<-g.resume
}

func (g cleanupBlockingGuard) ReleaseReferences() func() { return g.Release }

type cleanupGuardBuffer struct {
	cleanupTestBuffer
	guard TransformLogGuard
}

func (b cleanupGuardBuffer) Acquire(context.Context, *qviews.QueryViewAtQueryNode) (TransformLogGuard, error) {
	return b.guard, nil
}

func TestQueryViewCleanup_DroppedWaitsForSubscriptionCloseAndLoadCallback(t *testing.T) {
	scheduler := nodescheduler.New(2)
	t.Cleanup(scheduler.Close)
	loads := make(chan *SegmentLoadTask, 8)
	physical := NewViewScopedPhysicalSegmentManagerWithNodeScheduler(cleanupLoadScheduler{Scheduler: scheduler, loads: loads}, &fakePhysicalLoader{})
	resume := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	t.Cleanup(unblock)
	guard := cleanupBlockingGuard{entered: make(chan struct{}), resume: resume}
	collections := &fakeQueryViewCollectionRuntimeManager{}
	mgr := NewQueryViewSegmentReadinessManagerWithScheduler(scheduler, physical, cleanupGuardBuffer{guard: guard}, collections)
	key, _ := acquireCleanupView(t, mgr, 1, 1000)
	load := awaitCleanupEvent(t, loads)
	dropped := releaseCleanupView(mgr, key)
	awaitCleanupEvent(t, guard.entered)
	load.OnFinished()
	// Complete the pending load while Close is still blocked. Neither Dropped
	// nor the collection pin may finish until both sides complete.
	collections.mu.Lock()
	collectionGuard := collections.guard
	collections.mu.Unlock()
	collectionGuard.mu.Lock()
	released := collectionGuard.released
	collectionGuard.mu.Unlock()
	require.False(t, released)
	select {
	case <-dropped:
		t.Fatal("Dropped reported before subscription Close completed")
	default:
	}
	unblock()
	awaitCleanupEvent(t, dropped)
	collectionGuard.mu.Lock()
	released = collectionGuard.released
	collectionGuard.mu.Unlock()
	require.True(t, released)
}

type cleanupBlockingRegistration struct {
	instantTransformRegistration
	resume <-chan struct{}
}

func (r cleanupBlockingRegistration) WaitCatchup(ctx context.Context) error {
	select {
	case <-r.resume:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type cleanupCatchupBuffer struct {
	cleanupTestBuffer
	registered chan TransformSegment
	resume     <-chan struct{}
}

func (b cleanupCatchupBuffer) RegisterSegment(ctx context.Context, segment TransformSegment) (TransformRegistration, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.registered <- segment
	if segment.ID() == 1 {
		return cleanupBlockingRegistration{resume: b.resume}, nil
	}
	return instantTransformRegistration{}, nil
}

func TestQueryViewCleanup_CancelsQueuedCatchupBeforeReplacementRegisters(t *testing.T) {
	scheduler := nodescheduler.New(2)
	t.Cleanup(scheduler.Close)
	resume := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	t.Cleanup(unblock)
	segments := []*cleanupTestSegment{
		{fakeTransformSegment: fakeTransformSegment{id: 1}},
		{fakeTransformSegment: fakeTransformSegment{id: 1000}},
		{fakeTransformSegment: fakeTransformSegment{id: 1000}},
	}
	loaded := make(chan struct{}, 4)
	physical := fakePhysicalSegmentManager{
		acquire: func(req AcquirePhysicalSegments) {
			req.OnLoaded([]TransformSegment{segments[req.Meta.GetVersion().GetQueryVersion()-1]})
			loaded <- struct{}{}
		},
		release: func(req ReleaseSegments) { req.OnDropped() },
	}
	buffer := cleanupCatchupBuffer{registered: make(chan TransformSegment, 4), resume: resume}
	mgr := NewQueryViewSegmentReadinessManagerWithSchedulerAndCatchupConcurrency(scheduler, physical, buffer, 1)
	key1, ready1 := acquireCleanupView(t, mgr, 1, 1)
	awaitCleanupEvent(t, loaded)
	require.Same(t, segments[0], awaitCleanupEvent(t, buffer.registered))
	key2, _ := acquireCleanupView(t, mgr, 2, 1000)
	awaitCleanupEvent(t, loaded)
	awaitCleanupEvent(t, releaseCleanupView(mgr, key2))
	key3, ready3 := acquireCleanupView(t, mgr, 3, 1000)
	awaitCleanupEvent(t, loaded)
	unblock()
	awaitCleanupEvent(t, ready1)
	require.Same(t, segments[2], awaitCleanupEvent(t, buffer.registered))
	awaitCleanupEvent(t, ready3)
	select {
	case <-buffer.registered:
		t.Fatal("released segment's queued catch-up was registered")
	default:
	}
	awaitCleanupEvent(t, releaseCleanupView(mgr, key1))
	awaitCleanupEvent(t, releaseCleanupView(mgr, key3))
}
