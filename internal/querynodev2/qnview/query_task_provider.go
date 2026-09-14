package qnview

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/stage"
)

var _ viewquery.TaskProvider = (*QNQueryViewHandler)(nil)

func (h *QNQueryViewHandler) AcquireSearchSegmentTasks(
	ctx context.Context,
	shardID qviews.ShardID,
	version qviews.QueryViewVersion,
	mvcc *viewpb.QueryPlanMVCC,
	req *internalpb.SearchRequest,
) (viewquery.SearchSegmentTasks, error) {
	leaseCtx, leaseTimer := queryLease.Start(ctx)
	lease, err := h.AcquireReadyView(leaseCtx, shardID, version)
	leaseTimer.End(err)
	if err != nil {
		return nil, err
	}
	defer lease.Release()

	view := filterQueryNodeViewByPartitions(lease.View, req.GetPartitionIDs())
	queryOptimizeCtx, queryOptimizeTimer := queryOptimize.Start(ctx)
	queryOptimizeErr := h.localOptimizer.OptimizeSearch(queryOptimizeCtx, req)
	queryOptimizeTimer.End(queryOptimizeErr)
	if err := queryOptimizeErr; err != nil {
		return nil, err
	}
	key := qviews.QueryViewKey{ShardID: shardID, QueryViewVersion: version}
	queryVisibleCtx, queryVisibleTimer := queryVisible.Start(ctx)
	queryVisibleErr := h.segMgr.WaitTransformVisible(queryVisibleCtx, key, mvcc.GetTransformingTimetick())
	queryVisibleTimer.End(queryVisibleErr)
	if err := queryVisibleErr; err != nil {
		return nil, err
	}
	handlesCtx, handlesTimer := queryHandles.Start(ctx)
	handles, err := h.segMgr.AcquireSealedSegmentHandles(handlesCtx, key, view)
	handlesTimer.End(err)
	if err != nil {
		return nil, err
	}
	tasks := make([]QNSearchSegmentTask, 0, len(handles))
	for _, handle := range handles {
		tasks = append(tasks, QNSearchSegmentTask{
			Handle:  handle,
			Request: req,
			MVCC:    mvcc,
		})
	}
	return NewQNSearchSegmentTasks(tasks), nil
}

func (h *QNQueryViewHandler) AcquireQuerySegmentTasks(
	ctx context.Context,
	shardID qviews.ShardID,
	version qviews.QueryViewVersion,
	mvcc *viewpb.QueryPlanMVCC,
	req *internalpb.RetrieveRequest,
) (viewquery.QuerySegmentTasks, error) {
	leaseCtx, leaseTimer := queryLease.Start(ctx)
	lease, err := h.AcquireReadyView(leaseCtx, shardID, version)
	leaseTimer.End(err)
	if err != nil {
		return nil, err
	}
	defer lease.Release()

	view := filterQueryNodeViewByPartitions(lease.View, req.GetPartitionIDs())
	queryOptimizeCtx, queryOptimizeTimer := queryOptimize.Start(ctx)
	queryOptimizeErr := h.localOptimizer.OptimizeRetrieve(queryOptimizeCtx, req)
	queryOptimizeTimer.End(queryOptimizeErr)
	if err := queryOptimizeErr; err != nil {
		return nil, err
	}
	key := qviews.QueryViewKey{ShardID: shardID, QueryViewVersion: version}
	queryVisibleCtx, queryVisibleTimer := queryVisible.Start(ctx)
	queryVisibleErr := h.segMgr.WaitTransformVisible(queryVisibleCtx, key, mvcc.GetTransformingTimetick())
	queryVisibleTimer.End(queryVisibleErr)
	if err := queryVisibleErr; err != nil {
		return nil, err
	}
	handlesCtx, handlesTimer := queryHandles.Start(ctx)
	handles, err := h.segMgr.AcquireSealedSegmentHandles(handlesCtx, key, view)
	handlesTimer.End(err)
	if err != nil {
		return nil, err
	}
	tasks := make([]QNQuerySegmentTask, 0, len(handles))
	for _, handle := range handles {
		tasks = append(tasks, QNQuerySegmentTask{
			Handle:  handle,
			Request: req,
			MVCC:    mvcc,
		})
	}
	return NewQNQuerySegmentTasks(tasks), nil
}

var (
	queryLease    = stage.New("queryNode", "query_acquire", "lease")
	queryOptimize = stage.New("queryNode", "query_acquire", "local_optimize")
	queryVisible  = stage.New("queryNode", "query_acquire", "visibility_wait")
	queryHandles  = stage.New("queryNode", "query_acquire", "segment_handles")
)
