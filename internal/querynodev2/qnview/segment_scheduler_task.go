package qnview

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/stage"
)

func newSegmentLoadTask(loader PhysicalSegmentLoader, estimator SegmentResourceEstimator, task SegmentLoadTask) *SegmentLoadTask {
	task.loader = loader
	task.estimator = estimator
	return &task
}

func (t *SegmentLoadTask) Execute(schedulerCtx context.Context) error {
	totalTimer := segmentLoadTotal.Begin()
	startedAt := time.Now()
	timing := segmentLoadTimingSample{result: stage.Error}
	logCtx := schedulerCtx
	defer func() {
		timing.total = time.Since(startedAt)
		timing.failed = timing.result != stage.Success
		recordSQNSegmentLoadTiming(logCtx, timing)
	}()
	if t.OnFinished != nil {
		defer t.OnFinished()
	}
	ctx, cancel := mergeTaskContext(schedulerCtx, t.Context)
	defer cancel()
	logCtx = ctx
	defer func() {
		totalTimer.EndResult(timing.result)
	}()
	if err := ctx.Err(); err != nil {
		timing.result = stage.Outcome(err)
		return nil
	}
	segment, err := t.load(ctx, &timing)
	if err != nil {
		timing.result = stage.Outcome(err)
		if t.OnUnrecoverable != nil {
			t.OnUnrecoverable(err)
		}
		return err
	}
	// OnLoaded may cancel the task context as part of successful completion.
	// Capture the load outcome before invoking callbacks or cleanup.
	timing.result = stage.Success
	if t.OnLoaded != nil {
		onLoadedStartedAt := time.Now()
		t.OnLoaded(segment)
		timing.onLoaded = time.Since(onLoadedStartedAt)
		segmentOnLoaded.Observe(timing.onLoaded, stage.Success)
	}
	return nil
}

func (t *SegmentLoadTask) load(ctx context.Context, timing *segmentLoadTimingSample) (TransformSegment, error) {
	loadInfo, indexes, err := t.loadInfo()
	if err != nil {
		return nil, err
	}
	updateIndexMetaStartedAt := time.Now()
	err = updateCollectionIndexMeta(ctx, t.Collection, indexes)
	timing.updateIndexMeta = time.Since(updateIndexMetaStartedAt)
	segmentUpdateIndex.Observe(timing.updateIndexMeta, stage.Outcome(err))
	if err != nil {
		return nil, err
	}
	reserveResourceStartedAt := time.Now()
	reservation, err := t.reserve(ctx, loadInfo)
	timing.reserveResource = time.Since(reserveResourceStartedAt)
	segmentReserve.Observe(timing.reserveResource, stage.Outcome(err))
	if err != nil {
		return nil, err
	}
	if reservation != nil {
		defer func() {
			releaseResourceStartedAt := time.Now()
			reservation.Release()
			timing.releaseResource = time.Since(releaseResourceStartedAt)
			segmentUnreserve.Observe(timing.releaseResource, stage.Success)
		}()
	}
	physicalLoadDetail := &segments.PhysicalLoadTiming{}
	ctx = segments.WithPhysicalLoadTiming(ctx, physicalLoadDetail)
	physicalLoadStartedAt := time.Now()
	segment, err := t.loader.Load(ctx, loadInfo, t.Collection)
	timing.physicalLoad = time.Since(physicalLoadStartedAt)
	segmentPhysical.Observe(timing.physicalLoad, stage.Outcome(err))
	timing.physicalDetail = *physicalLoadDetail
	if err != nil {
		return nil, err
	}
	if t.TransformStartAfterTimeTick > 0 {
		segment = &transformStartSegment{
			TransformSegment: segment,
			startAfter:       t.TransformStartAfterTimeTick,
		}
	}
	return segment, nil
}

func (t *SegmentLoadTask) loadInfo() (*querypb.SegmentLoadInfo, []*indexpb.IndexInfo, error) {
	if t.Snapshot.LoadInfo != nil {
		return t.Snapshot.LoadInfo, t.Snapshot.IndexInfos, nil
	}
	return nil, nil, merr.WrapErrServiceInternalMsg("query view segment load requires watch snapshot, segmentID=%d", t.SegmentID)
}

func (t *SegmentLoadTask) reserve(ctx context.Context, info *querypb.SegmentLoadInfo) (ResourceReservation, error) {
	if t.estimator == nil {
		return nil, nil
	}
	return t.estimator.Reserve(ctx, info, t.Collection)
}

func newSegmentUpdateTask(loader PhysicalSegmentLoader, task SegmentUpdateTask) *SegmentUpdateTask {
	task.loader = loader
	return &task
}

func (t *SegmentUpdateTask) Execute(schedulerCtx context.Context) error {
	ctx, cancel := mergeTaskContext(schedulerCtx, t.Context)
	defer cancel()
	if ctx.Err() != nil {
		t.fail(ctx.Err())
		return nil
	}
	if err := t.update(ctx); err != nil {
		if ctx.Err() != nil {
			t.fail(ctx.Err())
			return nil
		}
		return nodescheduler.ErrDelay
	}
	return nil
}

func (t *SegmentUpdateTask) update(ctx context.Context) error {
	action := classifySegmentUpdate(t.Current, t.Snapshot.Revision)
	if action == SegmentUpdateNone {
		if t.OnUpdated != nil {
			t.OnUpdated(t.Current)
		}
		return nil
	}
	if err := updateCollectionIndexMeta(ctx, t.Collection, t.Snapshot.IndexInfos); err != nil {
		return err
	}
	if err := t.loader.Update(ctx, t.Segment, t.Collection, t.Snapshot, action); err != nil {
		return err
	}
	if t.OnUpdated != nil {
		t.OnUpdated(t.Snapshot.Revision)
	}
	return nil
}

func (t *SegmentUpdateTask) fail(err error) {
	if t.OnFailed != nil {
		t.OnFailed(err)
	}
}

func mergeTaskContext(schedulerCtx context.Context, taskCtx context.Context) (context.Context, context.CancelFunc) {
	if taskCtx == nil {
		taskCtx = context.Background()
	}
	ctx, cancel := context.WithCancel(taskCtx)
	stop := context.AfterFunc(schedulerCtx, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

func classifySegmentUpdate(current, next SegmentLoadInfoRevision) SegmentUpdateAction {
	if next.Empty() || current == next {
		return SegmentUpdateNone
	}
	return SegmentUpdateReopen | SegmentUpdateLoadIndex
}

type schedulerTaskFunc func(context.Context) error

func (f schedulerTaskFunc) Execute(ctx context.Context) error {
	return f(ctx)
}

var (
	_ nodescheduler.Task = schedulerTaskFunc(nil)
	_ nodescheduler.Task = (*SegmentLoadTask)(nil)
	_ nodescheduler.Task = (*SegmentUpdateTask)(nil)
)

func updateCollectionIndexMeta(ctx context.Context, collection CollectionRuntime, indexes []*indexpb.IndexInfo) error {
	updater, ok := collection.(CollectionIndexMetaUpdater)
	if !ok {
		return nil
	}
	return updater.UpdateIndexMeta(ctx, indexes)
}

type transformStartSegment struct {
	TransformSegment
	startAfter uint64
}

func (s *transformStartSegment) UnwrapTransformSegment() TransformSegment {
	return s.TransformSegment
}

func (s *transformStartSegment) TransformStartAfterTimeTick() uint64 {
	return s.startAfter
}

func (s *transformStartSegment) QuerySegment() segments.Segment {
	readable, ok := s.TransformSegment.(ReadableSealedSegment)
	if !ok {
		return nil
	}
	return readable.QuerySegment()
}

func (s *transformStartSegment) Collection() *segments.Collection {
	readable, ok := s.TransformSegment.(ReadableSealedSegment)
	if !ok {
		return nil
	}
	return readable.Collection()
}

var (
	segmentLoadTotal   = stage.New("queryNode", "segment_load", "total")
	segmentUpdateIndex = stage.New("queryNode", "segment_load", "update_index")
	segmentReserve     = stage.New("queryNode", "segment_load", "reserve")
	segmentUnreserve   = stage.New("queryNode", "segment_load", "release_reservation")
	segmentPhysical    = stage.New("queryNode", "segment_load", "physical_load")
	segmentOnLoaded    = stage.New("queryNode", "segment_load", "on_loaded")
)
