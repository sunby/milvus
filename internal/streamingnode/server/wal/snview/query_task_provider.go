package snview

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/stage"
)

var _ viewquery.TaskProvider = (*SNQueryViewHandler)(nil)

func (h *SNQueryViewHandler) AcquireSearchSegmentTasks(
	ctx context.Context,
	shardID qviews.ShardID,
	version qviews.QueryViewVersion,
	mvcc *viewpb.QueryPlanMVCC,
	req *internalpb.SearchRequest,
) (viewquery.SearchSegmentTasks, error) {
	if req.GetIgnoreGrowing() {
		return NewSNSearchSegmentTasks(nil), nil
	}
	leaseCtx, leaseTimer := queryLease.Start(ctx)
	lease, err := h.AcquireUpView(leaseCtx, shardID, version)
	leaseTimer.End(err)
	if err != nil {
		return nil, err
	}
	defer lease.Release()

	queryOptimizeCtx, queryOptimizeTimer := queryOptimize.Start(ctx)
	queryOptimizeErr := h.localOptimizer.OptimizeSearch(queryOptimizeCtx, req)
	queryOptimizeTimer.End(queryOptimizeErr)
	if err := queryOptimizeErr; err != nil {
		return nil, err
	}
	runtime, err := h.queryRuntime(qviews.QueryViewKey{ShardID: shardID, QueryViewVersion: version})
	if err != nil {
		return nil, err
	}
	mlog.Debug(ctx, "acquire streamingnode search segment tasks wait mvcc",
		mlog.FieldCollectionID(req.GetCollectionID()),
		mlog.FieldVChannel(shardID.VChannel),
		mlog.Int64("replicaID", shardID.ReplicaID),
		mlog.Uint64("growingTimeTick", mvcc.GetGrowingTimetick()),
		mlog.Uint64("transformingTimeTick", mvcc.GetTransformingTimetick()),
	)
	queryVisibleCtx, queryVisibleTimer := queryVisible.Start(ctx)
	queryVisibleErr := runtime.WaitMVCCVisible(queryVisibleCtx, mvcc.GetGrowingTimetick(), mvcc.GetTransformingTimetick())
	queryVisibleTimer.End(queryVisibleErr)
	if err := queryVisibleErr; err != nil {
		return nil, err
	}
	handlesCtx, handlesTimer := queryHandles.Start(ctx)
	handles, err := runtime.AcquireGrowingSegmentHandles(handlesCtx, selectedPartitionIDs(req.GetPartitionIDs()))
	handlesTimer.End(err)
	if err != nil {
		return nil, err
	}
	mlog.Debug(ctx, "acquired streamingnode search segment tasks",
		mlog.FieldCollectionID(req.GetCollectionID()),
		mlog.FieldVChannel(shardID.VChannel),
		mlog.Int("segmentCount", len(handles)),
	)
	tasks := make([]SNSearchSegmentTask, 0, len(handles))
	for _, handle := range handles {
		tasks = append(tasks, SNSearchSegmentTask{
			Handle:   handle,
			Request:  req,
			MVCC:     mvcc,
			VChannel: lease.Meta.GetVchannel(),
		})
	}
	return NewSNSearchSegmentTasks(tasks), nil
}

func (h *SNQueryViewHandler) AcquireQuerySegmentTasks(
	ctx context.Context,
	shardID qviews.ShardID,
	version qviews.QueryViewVersion,
	mvcc *viewpb.QueryPlanMVCC,
	req *internalpb.RetrieveRequest,
) (viewquery.QuerySegmentTasks, error) {
	leaseCtx, leaseTimer := queryLease.Start(ctx)
	lease, err := h.AcquireUpView(leaseCtx, shardID, version)
	leaseTimer.End(err)
	if err != nil {
		return nil, err
	}
	defer lease.Release()

	queryOptimizeCtx, queryOptimizeTimer := queryOptimize.Start(ctx)
	queryOptimizeErr := h.localOptimizer.OptimizeRetrieve(queryOptimizeCtx, req)
	queryOptimizeTimer.End(queryOptimizeErr)
	if err := queryOptimizeErr; err != nil {
		return nil, err
	}
	runtime, err := h.queryRuntime(qviews.QueryViewKey{ShardID: shardID, QueryViewVersion: version})
	if err != nil {
		return nil, err
	}
	mlog.Debug(ctx, "acquire streamingnode query segment tasks wait mvcc",
		mlog.FieldCollectionID(req.GetCollectionID()),
		mlog.FieldVChannel(shardID.VChannel),
		mlog.Int64("replicaID", shardID.ReplicaID),
		mlog.Uint64("growingTimeTick", mvcc.GetGrowingTimetick()),
		mlog.Uint64("transformingTimeTick", mvcc.GetTransformingTimetick()),
	)
	queryVisibleCtx, queryVisibleTimer := queryVisible.Start(ctx)
	queryVisibleErr := runtime.WaitMVCCVisible(queryVisibleCtx, mvcc.GetGrowingTimetick(), mvcc.GetTransformingTimetick())
	queryVisibleTimer.End(queryVisibleErr)
	if err := queryVisibleErr; err != nil {
		return nil, err
	}
	handlesCtx, handlesTimer := queryHandles.Start(ctx)
	handles, err := runtime.AcquireGrowingSegmentHandles(handlesCtx, selectedPartitionIDs(req.GetPartitionIDs()))
	handlesTimer.End(err)
	if err != nil {
		return nil, err
	}
	mlog.Debug(ctx, "acquired streamingnode query segment tasks",
		mlog.FieldCollectionID(req.GetCollectionID()),
		mlog.FieldVChannel(shardID.VChannel),
		mlog.Int("segmentCount", len(handles)),
	)
	tasks := make([]SNQuerySegmentTask, 0, len(handles))
	for _, handle := range handles {
		tasks = append(tasks, SNQuerySegmentTask{
			Handle:   handle,
			Request:  req,
			MVCC:     mvcc,
			VChannel: lease.Meta.GetVchannel(),
		})
	}
	return NewSNQuerySegmentTasks(tasks), nil
}

var (
	queryLease    = stage.New("streamingNode", "query_acquire", "lease")
	queryOptimize = stage.New("streamingNode", "query_acquire", "local_optimize")
	queryVisible  = stage.New("streamingNode", "query_acquire", "visibility_wait")
	queryHandles  = stage.New("streamingNode", "query_acquire", "segment_handles")
)
