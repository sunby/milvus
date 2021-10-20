package datacoord

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
)

// TODO this num should be determined by resources of datanode, for now, we set to a fixed value for simple
const (
	maxParallelCompactionTaskNum = 100
	compactionTimeout            = 10 * time.Second
)

type compactionPlanContext interface {
	// execCompactionPlan start to execute plan and return immediately
	execCompactionPlan(plan *datapb.CompactionPlan) error
	// completeCompaction record the result of a compaction
	completeCompaction(result *datapb.CompactionResult) error
	// getCompaction return compaction task. If planId does not exist, return nil.
	getCompaction(planID int64) *compactionTask
	// expireCompaction set the compaction state to expired
	expireCompaction(ts Timestamp) error
	// isFull return true if the task pool is full
	isFull() bool
}

type compactionTaskState int8

const (
	executing compactionTaskState = iota + 1
	completed
	failed
	timeout
)

var errNotEnoughDataNode = errors.New("there is not enough datanode")

type compactionTask struct {
	triggerInfo *compactionSignal
	plan        *datapb.CompactionPlan
	state       compactionTaskState
	dataNodeID  int64
}

func (t *compactionTask) shadowClone(opts ...compactionTaskOpt) *compactionTask {
	task := &compactionTask{
		plan:       t.plan,
		state:      t.state,
		dataNodeID: t.dataNodeID,
	}
	for _, opt := range opts {
		opt(task)
	}
	return task
}

var _ compactionPlanContext = (*compactionPlanHandler)(nil)

type compactionPlanHandler struct {
	plans            map[int64]*compactionTask // planid -> task
	sessions         *SessionManager
	meta             *meta
	mu               sync.RWMutex
	executingTaskNum int
}

func newCompactionPlanHandler(sessions *SessionManager) *compactionPlanHandler {
	return &compactionPlanHandler{
		plans:    make(map[int64]*compactionTask),
		sessions: sessions,
	}
}

// execCompactionPlan start to execute plan and return immediately
func (c *compactionPlanHandler) execCompactionPlan(plan *datapb.CompactionPlan) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	sessions := c.sessions.GetSessions()
	nodeID, err := c.findNodeWithLeastTask(sessions)
	if err != nil {
		return err
	}

	c.setSegmentsCompacting(plan, true)

	ctx, cancel := context.WithTimeout(context.Background(), compactionTimeout)
	defer cancel()
	// FIXME: check response of compaction call and restore segment state if failed
	c.sessions.Compaction(ctx, nodeID, plan)

	task := &compactionTask{
		plan:       plan,
		state:      executing,
		dataNodeID: nodeID,
	}
	c.plans[plan.PlanID] = task
	c.executingTaskNum++
	return nil
}

func (c *compactionPlanHandler) findNodeWithLeastTask(sessions []*Session) (int64, error) {
	if len(sessions) == 0 {
		return -1, errNotEnoughDataNode
	}
	if len(c.plans) == 0 {
		return sessions[0].info.NodeID, nil
	}

	taskNums := make(map[int64]int)
	for _, plan := range c.plans {
		taskNums[plan.dataNodeID]++
	}
	for _, session := range sessions {
		if _, ok := taskNums[session.info.NodeID]; !ok {
			return session.info.NodeID, nil
		}
	}

	min := math.MaxInt64
	var node int64

	for id, num := range taskNums {
		if num < min {
			min = num
			node = id
		}
	}

	return node, nil
}

func (c *compactionPlanHandler) setSegmentsCompacting(plan *datapb.CompactionPlan, compacting bool) {
	for _, mg := range plan.GetMergeGroup() {
		for _, tmp := range mg.GetSegmentBinlogs() {
			c.meta.SetSegmentCompacting(tmp.GetSegmentID(), compacting)
		}
	}
}

// completeCompaction record the result of a compaction
func (c *compactionPlanHandler) completeCompaction(result *datapb.CompactionResult) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	planID := result.PlanID
	if _, ok := c.plans[planID]; !ok {
		return fmt.Errorf("plan %d is not found", planID)
	}

	if c.plans[planID].state != executing {
		return fmt.Errorf("plan %d's state is %v", planID, c.plans[planID].state)
	}

	plan := c.plans[planID].plan
	switch plan.GetType() {
	case datapb.CompactionType_InnerCompaction:
		if err := c.handleInnerCompactionResult(plan, result); err != nil {
			return err
		}
	case datapb.CompactionType_MergeCompaction:
		if err := c.handleMergeCompactionResult(plan, result); err != nil {
			return err
		}
	default:
		return errors.New("unknown compaction type")
	}
	c.plans[planID] = c.plans[planID].shadowClone(setState(completed))
	c.executingTaskNum--
	// FIXME: when to remove plan
	return nil
}

func (c *compactionPlanHandler) handleInnerCompactionResult(plan *datapb.CompactionPlan, result *datapb.CompactionResult) error {
	mergeGroup := plan.GetMergeGroup()[0]
	segmentBinlogs := mergeGroup.GetSegmentBinlogs()[0]
	return c.meta.CompleteInnerCompaction(segmentBinlogs, result)
}

func (c *compactionPlanHandler) handleMergeCompactionResult(plan *datapb.CompactionPlan, result *datapb.CompactionResult) error {
	compacted := make([]UniqueID, 0)
	for _, mg := range plan.GetMergeGroup() {
		for _, s := range mg.GetSegmentBinlogs() {
			compacted = append(compacted, s.GetSegmentID())
		}
	}
	return c.meta.CompleteMergeCompaction(compacted, result)
}

// getCompaction return compaction task. If planId does not exist, return nil.
func (c *compactionPlanHandler) getCompaction(planID int64) *compactionTask {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.plans[planID]
}

// expireCompaction set the compaction state to expired
func (c *compactionPlanHandler) expireCompaction(ts Timestamp) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	tasks := c.getExecutingCompactions()
	for _, task := range tasks {
		if !c.isTimeout(ts, task.plan.GetStartTime(), task.plan.GetTimeoutInSeconds()) {
			continue
		}

		c.setSegmentsCompacting(task.plan, false)

		planID := task.plan.PlanID
		c.plans[planID] = c.plans[planID].shadowClone(setState(timeout))
		c.executingTaskNum--
	}

	return nil
}

func (c *compactionPlanHandler) isTimeout(now Timestamp, start Timestamp, timeout int32) bool {
	starttime, _ := tsoutil.ParseTS(start)
	ts, _ := tsoutil.ParseTS(now)
	return int32(ts.Sub(starttime).Seconds()) >= timeout
}

// isFull return true if the task pool is full
func (c *compactionPlanHandler) isFull() bool {
	return c.executingTaskNum >= maxParallelCompactionTaskNum
}

func (c *compactionPlanHandler) getExecutingCompactions() []*compactionTask {
	tasks := make([]*compactionTask, 0, len(c.plans))
	for _, plan := range c.plans {
		if plan.state == executing {
			tasks = append(tasks, plan)
		}
	}
	return tasks
}

type compactionTaskOpt func(task *compactionTask)

func setState(state compactionTaskState) compactionTaskOpt {
	return func(task *compactionTask) {
		task.state = state
	}
}
