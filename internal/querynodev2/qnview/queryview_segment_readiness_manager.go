package qnview

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	qvobserve "github.com/milvus-io/milvus/internal/views/qviews/observe"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

// QueryViewSegmentReadinessManager turns physically loaded segments into
// QueryView-ready segments by registering them with the TransformLogBuffer and
// waiting for catch-up.
type QueryViewSegmentReadinessManager struct {
	scheduler    nodescheduler.Scheduler
	physical     PhysicalSegmentManager
	buffer       TransformLogBuffer
	collections  QueryViewCollectionRuntimeManager
	catchupTasks chan *transformCatchupTask

	mu             sync.Mutex
	views          map[qviews.QueryViewKey]*transformViewRef
	segments       map[int64]*transformSegmentState
	cleanupTasks   []func()
	cleanupWorkers int
}

const defaultTransformCatchupConcurrency = 4

const segmentCleanupConcurrency = 4

func NewQueryViewSegmentReadinessManager(physical PhysicalSegmentManager, buffer TransformLogBuffer, collections ...QueryViewCollectionRuntimeManager) *QueryViewSegmentReadinessManager {
	return NewQueryViewSegmentReadinessManagerWithScheduler(nodescheduler.Get(), physical, buffer, collections...)
}

func NewQueryViewSegmentReadinessManagerWithScheduler(scheduler nodescheduler.Scheduler, physical PhysicalSegmentManager, buffer TransformLogBuffer, collections ...QueryViewCollectionRuntimeManager) *QueryViewSegmentReadinessManager {
	return NewQueryViewSegmentReadinessManagerWithSchedulerAndCatchupConcurrency(
		scheduler,
		physical,
		buffer,
		defaultTransformCatchupConcurrency,
		collections...,
	)
}

func NewQueryViewSegmentReadinessManagerWithSchedulerAndCatchupConcurrency(
	scheduler nodescheduler.Scheduler,
	physical PhysicalSegmentManager,
	buffer TransformLogBuffer,
	catchupConcurrency int,
	collections ...QueryViewCollectionRuntimeManager,
) *QueryViewSegmentReadinessManager {
	if catchupConcurrency <= 0 {
		catchupConcurrency = defaultTransformCatchupConcurrency
	}
	var collectionManager QueryViewCollectionRuntimeManager
	if len(collections) > 0 {
		collectionManager = collections[0]
	}
	m := &QueryViewSegmentReadinessManager{
		scheduler:    scheduler,
		physical:     physical,
		buffer:       buffer,
		collections:  collectionManager,
		catchupTasks: make(chan *transformCatchupTask, 1024),
		views:        make(map[qviews.QueryViewKey]*transformViewRef),
		segments:     make(map[int64]*transformSegmentState),
	}
	for i := 0; i < catchupConcurrency; i++ {
		go m.catchupWorker()
	}
	return m
}

func (m *QueryViewSegmentReadinessManager) Acquire(req AcquireSegments) {
	req = cloneAcquireSegments(req)
	m.acquire(req)
}

func (m *QueryViewSegmentReadinessManager) Release(req ReleaseSegments) {
	m.release(req)
}

type transformSegmentLoadState int

const (
	transformSegmentWaiting transformSegmentLoadState = iota
	transformSegmentLoading
	transformSegmentCatchingUp
	transformSegmentLoaded
)

type transformViewRef struct {
	cancel          context.CancelFunc
	transformGuard  TransformLogGuard
	collectionGuard CollectionRuntimeGuard
	segments        map[int64]int64
	onUnrecoverable func()
	unrecoverable   bool
}

type transformSegmentState struct {
	state         transformSegmentLoadState
	segment       TransformSegment
	reg           TransformRegistration
	catchupCancel context.CancelFunc
	queryRefs     int
	refs          map[qviews.QueryViewKey]struct{}
	waiters       map[qviews.QueryViewKey]transformSegmentWaiter
}

type transformSegmentWaiter struct {
	key             qviews.QueryViewKey
	partitionID     int64
	segmentID       int64
	onReady         func(map[int64][]int64)
	onUnrecoverable func()
}

type transformCatchupTask struct {
	segment TransformSegment
	ctx     context.Context
	cancel  context.CancelFunc
}

func (m *QueryViewSegmentReadinessManager) acquire(req AcquireSegments) {
	ctx, cancel := context.WithCancel(context.Background())
	view := qviews.NewQueryViewAtQueryNode(req.Meta, req.View).(*qviews.QueryViewAtQueryNode)
	guard, err := m.buffer.Acquire(ctx, view)
	if err != nil {
		cancel()
		m.submitCallback(req.OnUnrecoverable)
		return
	}

	ref, ok := m.recordPendingAcquire(req, cancel, guard)
	if !ok {
		cancel()
		guard.Release()
		return
	}
	m.scheduler.Submit(schedulerTaskFunc(func(schedulerCtx context.Context) error {
		ctx, stop := mergeTaskContext(schedulerCtx, ctx)
		defer stop()
		return m.continueAcquire(req, ref, view, ctx, cancel)
	}))
}

func (m *QueryViewSegmentReadinessManager) submitCallback(callback func()) {
	if callback == nil {
		return
	}
	m.scheduler.Submit(schedulerTaskFunc(func(context.Context) error {
		callback()
		return nil
	}))
}

func (m *QueryViewSegmentReadinessManager) continueAcquire(req AcquireSegments, ref *transformViewRef, view *qviews.QueryViewAtQueryNode, ctx context.Context, cancel context.CancelFunc) error {
	if ctx.Err() != nil {
		return nil
	}
	collectionGuard, retryable, err := m.acquireCollectionRuntime(ctx, view)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		if retryable {
			return nodescheduler.ErrDelay
		}
		return m.failAcquire(req, ref, cancel, err)
	}

	activation, startLoad := m.activatePhysicalAcquire(req, ref, collectionGuard)
	if !activation.current {
		if collectionGuard != nil {
			collectionGuard.Release()
		}
		cancel()
		return nil
	}

	for _, waiter := range activation.readyNow {
		waiter.reportReady()
	}
	if activation.noAssignedSegments && req.OnReady != nil {
		req.OnReady(map[int64][]int64{})
	}
	if startLoad != nil {
		startLoad()
	}
	return nil
}

func (m *QueryViewSegmentReadinessManager) activatePhysicalAcquire(req AcquireSegments, ref *transformViewRef, collectionGuard CollectionRuntimeGuard) (transformAcquireActivation, func()) {
	// Register physical ownership under the same lock that detaches readiness
	// refs. Release must not overtake this registration and leave an orphaned
	// physical ref using an already released collection guard.
	m.mu.Lock()
	defer m.mu.Unlock()
	activation := m.activateAcquireLocked(req, ref, collectionGuard)
	if !activation.current || len(activation.physicalRefSegments) == 0 {
		return activation, nil
	}
	startLoad := m.physical.AcquireReferences(AcquirePhysicalSegments{
		Key:        req.Key,
		Meta:       proto.Clone(req.Meta).(*viewpb.QueryViewMeta),
		View:       filterViewSegments(req.View, activation.physicalRefSegments),
		Collection: collectionGuard,
		OnLoaded: func(loaded []TransformSegment) {
			m.onPhysicalLoaded(loaded, activation.physicalStates)
		},
		OnSegmentUnrecoverable: func(segmentID int64, err error) {
			m.failSegment(segmentID, err, func(state *transformSegmentState) bool {
				_, current := state.refs[req.Key]
				return current && m.views[req.Key] == ref
			})
		},
		OnUnrecoverable: func() {
			m.failView(req.Key, ref)
		},
	})
	return activation, startLoad
}

func (m *QueryViewSegmentReadinessManager) failAcquire(req AcquireSegments, ref *transformViewRef, cancel context.CancelFunc, err error) error {
	cancel()
	if detached, current := m.detachViewIfCurrent(req.Key, ref); current {
		detached.releaseTransform()
		detached.unregister()
		detached.releaseSegments()
		invokeUnrecoverable(req.OnUnrecoverable)
		return err
	}
	return nil
}

func (m *QueryViewSegmentReadinessManager) acquireCollectionRuntime(ctx context.Context, view *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
	if m.collections == nil {
		return nil, false, nil
	}
	return m.collections.Acquire(ctx, view)
}

func (m *QueryViewSegmentReadinessManager) recordPendingAcquire(req AcquireSegments, cancel context.CancelFunc, guard TransformLogGuard) (*transformViewRef, bool) {
	segmentPartitions := segmentPartitionMap(req.View)

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.views[req.Key] != nil {
		return nil, false
	}
	ref := &transformViewRef{
		cancel:          cancel,
		transformGuard:  guard,
		segments:        segmentPartitions,
		onUnrecoverable: req.OnUnrecoverable,
	}
	m.views[req.Key] = ref
	for segmentID, partitionID := range segmentPartitions {
		state := m.segments[segmentID]
		if state == nil {
			state = &transformSegmentState{
				state:   transformSegmentWaiting,
				refs:    make(map[qviews.QueryViewKey]struct{}),
				waiters: make(map[qviews.QueryViewKey]transformSegmentWaiter),
			}
			m.segments[segmentID] = state
		}
		state.refs[req.Key] = struct{}{}
		state.waiters[req.Key] = transformSegmentWaiter{
			key:             req.Key,
			partitionID:     partitionID,
			segmentID:       segmentID,
			onReady:         req.OnReady,
			onUnrecoverable: req.OnUnrecoverable,
		}
	}
	return ref, true
}

func (m *QueryViewSegmentReadinessManager) detachViewIfCurrent(key qviews.QueryViewKey, ref *transformViewRef) (transformViewDetach, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.views[key] != ref {
		return transformViewDetach{}, false
	}
	return m.detachViewLocked(key), true
}

type transformAcquireActivation struct {
	readyNow            []transformSegmentWaiter
	physicalRefSegments []int64
	physicalStates      map[int64]*transformSegmentState
	noAssignedSegments  bool
	current             bool
}

func (m *QueryViewSegmentReadinessManager) activateAcquireLocked(req AcquireSegments, ref *transformViewRef, collectionGuard CollectionRuntimeGuard) transformAcquireActivation {
	if m.views[req.Key] != ref {
		return transformAcquireActivation{}
	}
	activation := transformAcquireActivation{
		current: true, noAssignedSegments: len(ref.segments) == 0,
		physicalStates: make(map[int64]*transformSegmentState),
	}
	ref.collectionGuard = collectionGuard
	ref.onUnrecoverable = req.OnUnrecoverable
	for segmentID := range ref.segments {
		state := m.segments[segmentID]
		if state == nil {
			state = &transformSegmentState{
				state:   transformSegmentWaiting,
				refs:    make(map[qviews.QueryViewKey]struct{}),
				waiters: make(map[qviews.QueryViewKey]transformSegmentWaiter),
			}
			m.segments[segmentID] = state
		}
		state.refs[req.Key] = struct{}{}
		waiter := transformSegmentWaiter{
			key:             req.Key,
			partitionID:     ref.segments[segmentID],
			segmentID:       segmentID,
			onReady:         req.OnReady,
			onUnrecoverable: req.OnUnrecoverable,
		}
		if state.state == transformSegmentLoaded {
			activation.readyNow = append(activation.readyNow, waiter)
			delete(state.waiters, req.Key)
			continue
		}
		if state.state == transformSegmentWaiting {
			state.state = transformSegmentLoading
		}
		if state.state == transformSegmentLoading {
			activation.physicalRefSegments = append(activation.physicalRefSegments, segmentID)
			activation.physicalStates[segmentID] = state
		}
		state.waiters[req.Key] = waiter
	}
	return activation
}

func invokeUnrecoverable(cb func()) {
	if cb != nil {
		cb()
	}
}

func (m *QueryViewSegmentReadinessManager) onPhysicalLoaded(segments []TransformSegment, expected map[int64]*transformSegmentState) {
	for _, segment := range segments {
		if segment == nil {
			continue
		}
		if kept, task := m.markPhysicalLoaded(segment, expected[segment.ID()]); task != nil {
			m.scheduleCatchup(task)
		} else if !kept {
			_ = segment.Release(context.Background())
		}
	}
}

func (m *QueryViewSegmentReadinessManager) scheduleCatchup(task *transformCatchupTask) {
	select {
	case m.catchupTasks <- task:
	case <-task.ctx.Done():
	}
}

func (m *QueryViewSegmentReadinessManager) catchupWorker() {
	for task := range m.catchupTasks {
		m.registerAndCatchup(task)
	}
}

func (m *QueryViewSegmentReadinessManager) markPhysicalLoaded(segment TransformSegment, expected *transformSegmentState) (bool, *transformCatchupTask) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.segments[segment.ID()]
	if state == nil || state != expected || len(state.refs) == 0 {
		return false, nil
	}
	if state.state == transformSegmentLoaded || state.state == transformSegmentCatchingUp {
		return true, nil
	}
	state.segment = segment
	state.state = transformSegmentCatchingUp
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // canceled by the catch-up task or last-reference cleanup
	state.catchupCancel = cancel
	return true, &transformCatchupTask{segment: segment, ctx: ctx, cancel: cancel}
}

func (m *QueryViewSegmentReadinessManager) registerAndCatchup(task *transformCatchupTask) {
	defer task.cancel()
	if task.ctx.Err() != nil {
		return
	}
	segment := task.segment
	reg, err := m.buffer.RegisterSegment(task.ctx, segment)
	if err != nil {
		m.failSegment(segment.ID(), err, func(state *transformSegmentState) bool { return state.segment == segment })
		return
	}
	if !m.storeRegistration(segment.ID(), segment, reg) {
		reg.Unregister()
		return
	}
	if err := reg.WaitCatchup(task.ctx); err != nil {
		reg.Unregister()
		m.failSegment(segment.ID(), err, func(state *transformSegmentState) bool { return state.segment == segment })
		return
	}
	for _, waiter := range m.markSegmentReady(segment) {
		waiter.reportReady()
	}
}

func (m *QueryViewSegmentReadinessManager) storeRegistration(segmentID int64, segment TransformSegment, reg TransformRegistration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.segments[segmentID]
	if state == nil || state.segment != segment || len(state.refs) == 0 {
		return false
	}
	state.reg = reg
	state.state = transformSegmentCatchingUp
	return true
}

func (m *QueryViewSegmentReadinessManager) markSegmentReady(segment TransformSegment) []transformSegmentWaiter {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.segments[segment.ID()]
	if state == nil || state.segment != segment || state.state != transformSegmentCatchingUp {
		return nil
	}
	state.state = transformSegmentLoaded
	waiters := make([]transformSegmentWaiter, 0, len(state.waiters))
	for key, waiter := range state.waiters {
		if m.views[key] == nil {
			continue
		}
		waiters = append(waiters, waiter)
	}
	state.waiters = make(map[qviews.QueryViewKey]transformSegmentWaiter)
	return waiters
}

func (m *QueryViewSegmentReadinessManager) failSegment(segmentID int64, err error, current func(*transformSegmentState) bool) {
	m.mu.Lock()
	state := m.segments[segmentID]
	if state == nil || !current(state) {
		m.mu.Unlock()
		return
	}
	reg := state.reg
	cancel := state.catchupCancel
	segment := state.segment
	waiters := make([]transformSegmentWaiter, 0, len(state.waiters))
	for _, waiter := range state.waiters {
		waiters = append(waiters, waiter)
	}
	delete(m.segments, segmentID)
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if reg != nil {
		reg.Unregister()
	}
	if segment != nil {
		if resetter, ok := m.physical.(PhysicalSegmentResetter); ok {
			resetter.ResetSegment(segment)
		}
		_ = segment.Release(context.Background())
	}
	if err == nil {
		err = errors.New("segment became unrecoverable")
	}
	for _, waiter := range waiters {
		qvobserve.Observe(context.TODO(), qvobserve.QueryNodeSegmentFailureEvent{
			View:      waiter.key,
			SegmentID: segmentID,
			Err:       err,
		})
		m.notifyUnrecoverable(waiter.key, waiter.onUnrecoverable)
	}
}

func (m *QueryViewSegmentReadinessManager) failView(key qviews.QueryViewKey, expected *transformViewRef) {
	m.mu.Lock()
	ref := m.views[key]
	if ref == nil || ref != expected || ref.unrecoverable {
		m.mu.Unlock()
		return
	}
	ref.unrecoverable = true
	cb := ref.onUnrecoverable
	for segmentID := range ref.segments {
		if state := m.segments[segmentID]; state != nil {
			delete(state.waiters, key)
		}
	}
	m.mu.Unlock()

	if cb != nil {
		cb()
	}
}

func (m *QueryViewSegmentReadinessManager) notifyUnrecoverable(key qviews.QueryViewKey, cb func()) {
	m.mu.Lock()
	ref := m.views[key]
	if ref == nil || ref.unrecoverable {
		m.mu.Unlock()
		return
	}
	ref.unrecoverable = true
	for segmentID := range ref.segments {
		if state := m.segments[segmentID]; state != nil {
			delete(state.waiters, key)
		}
	}
	m.mu.Unlock()
	invokeUnrecoverable(cb)
}

func (m *QueryViewSegmentReadinessManager) release(req ReleaseSegments) {
	m.mu.Lock()
	detached := m.detachViewLocked(req.Key)
	// Cleanup and in-flight load callbacks must both finish before the
	// collection pin can be released. Neither side waits on the other while
	// occupying a cleanup or load worker.
	remaining := 2
	complete := func() {
		m.mu.Lock()
		remaining--
		done := remaining == 0
		m.mu.Unlock()
		if done {
			m.enqueueCleanup(func() {
				detached.releaseCollection()
				if req.OnDropped != nil {
					req.OnDropped()
				}
			})
		}
	}
	finishPhysical := m.physical.ReleaseReferences(ReleaseSegments{
		Key:       req.Key,
		OnDropped: complete,
	})
	// Unregister is local and must precede a replacement segment's catch-up.
	// Subscription Close and native destruction, in contrast, may block.
	detached.unregister()
	var finishTransform func()
	if detached.guards.transform != nil {
		finishTransform = detached.guards.transform.ReleaseReferences()
	}
	m.mu.Unlock()
	m.enqueueCleanup(func() {
		finishPhysical()
		detached.releaseSegments()
		if finishTransform != nil {
			finishTransform()
		}
		complete()
	})
}

// Cleanup uses separate, bounded workers: native destruction and subscription
// Close may block, so running them on NodeScheduler could starve segment loads.
// The queue retains pending releases rather than blocking ViewSync on capacity.
// Workers exit when drained; idle managers do not retain cleanup goroutines.
func (m *QueryViewSegmentReadinessManager) enqueueCleanup(task func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanupTasks = append(m.cleanupTasks, task)
	if m.cleanupWorkers < segmentCleanupConcurrency {
		m.cleanupWorkers++
		go m.cleanupWorker()
	}
}

func (m *QueryViewSegmentReadinessManager) cleanupWorker() {
	for {
		m.mu.Lock()
		if len(m.cleanupTasks) == 0 {
			m.cleanupTasks = nil
			m.cleanupWorkers--
			m.mu.Unlock()
			return
		}
		task := m.cleanupTasks[0]
		m.cleanupTasks[0] = nil
		m.cleanupTasks = m.cleanupTasks[1:]
		m.mu.Unlock()
		task()
	}
}

type transformViewGuards struct {
	transform  TransformLogGuard
	collection CollectionRuntimeGuard
}

type transformViewDetach struct {
	guards   transformViewGuards
	cancels  []context.CancelFunc
	regs     []TransformRegistration
	segments []TransformSegment
}

func (d transformViewDetach) releaseTransform() {
	if d.guards.transform != nil {
		d.guards.transform.Release()
	}
}

func (d transformViewDetach) releaseCollection() {
	if d.guards.collection != nil {
		d.guards.collection.Release()
	}
}

func (d transformViewDetach) unregister() {
	for _, cancel := range d.cancels {
		if cancel != nil {
			cancel()
		}
	}
	for _, reg := range d.regs {
		if reg != nil {
			reg.Unregister()
		}
	}
}

func (d transformViewDetach) releaseSegments() {
	for _, segment := range d.segments {
		if segment != nil {
			_ = segment.Release(context.Background())
		}
	}
}

func (m *QueryViewSegmentReadinessManager) detachViewLocked(key qviews.QueryViewKey) transformViewDetach {
	ref := m.views[key]
	if ref == nil {
		return transformViewDetach{}
	}
	delete(m.views, key)
	if ref.cancel != nil {
		ref.cancel()
	}
	detached := transformViewDetach{
		guards: transformViewGuards{transform: ref.transformGuard, collection: ref.collectionGuard},
	}
	for segmentID := range ref.segments {
		state := m.segments[segmentID]
		if state == nil {
			continue
		}
		delete(state.refs, key)
		delete(state.waiters, key)
		if len(state.refs) == 0 {
			if state.catchupCancel != nil {
				detached.cancels = append(detached.cancels, state.catchupCancel)
				state.catchupCancel = nil
			}
			if state.reg != nil {
				detached.regs = append(detached.regs, state.reg)
				state.reg = nil
			}
			if state.segment != nil {
				if state.queryRefs > 0 {
					continue
				}
				detached.segments = append(detached.segments, state.segment)
				delete(m.segments, segmentID)
				continue
			}
			delete(m.segments, segmentID)
		}
	}
	return detached
}

func (m *QueryViewSegmentReadinessManager) releaseDetachedSegment(segment TransformSegment) {
	if segment != nil {
		_ = segment.Release(context.Background())
	}
}

func (w transformSegmentWaiter) reportReady() {
	if w.onReady != nil {
		w.onReady(map[int64][]int64{w.partitionID: {w.segmentID}})
	}
}

func cloneAcquireSegments(req AcquireSegments) AcquireSegments {
	out := req
	if req.Meta != nil {
		out.Meta = proto.Clone(req.Meta).(*viewpb.QueryViewMeta)
	}
	if req.View != nil {
		out.View = proto.Clone(req.View).(*viewpb.QueryViewOfQueryNode)
	}
	return out
}
