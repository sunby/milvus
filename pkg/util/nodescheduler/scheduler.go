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

package nodescheduler

import (
	"container/list"
	"context"
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type Scheduler interface {
	// Submit enqueues the task into an unbounded queue and returns without
	// waiting for queue capacity or task execution. Tasks may call Submit from
	// Execute, so implementations must preserve this non-blocking contract to
	// avoid exhausting all workers on nested submissions.
	Submit(Task) TaskHandle
}

type Task interface {
	Execute(context.Context) error
}

type TaskHandle interface {
	Cancel()
	Wait(context.Context) error
}

type scheduleErrorKind int

const scheduleErrorKindDelay scheduleErrorKind = iota + 1

type ScheduleError struct {
	kind scheduleErrorKind
}

func (e *ScheduleError) Error() string {
	if e != nil && e.kind == scheduleErrorKindDelay {
		return "delay node scheduler task"
	}
	return "unknown node scheduler error"
}

func (e *ScheduleError) Is(target error) bool {
	other, ok := target.(*ScheduleError)
	return ok && e != nil && other != nil && e.kind == other.kind
}

var ErrDelay = &ScheduleError{kind: scheduleErrorKindDelay}

type delayError struct {
	cause error
}

func (e *delayError) Error() string {
	return e.cause.Error()
}

func (e *delayError) Unwrap() error {
	return e.cause
}

func (e *delayError) Is(target error) bool {
	return ErrDelay.Is(target)
}

// MarkDelay preserves err as the cause while asking the scheduler to retry the
// task. Unlike cockroachdb/errors.Mark, it does not build reflection-based type
// markers on this hot path.
func MarkDelay(err error) error {
	if err == nil || errors.Is(err, ErrDelay) {
		return err
	}
	return &delayError{cause: err}
}

type nodeScheduler struct {
	ctx     context.Context
	cancel  context.CancelFunc
	stats   *schedulerStats
	metrics schedulerMetrics

	mu     sync.Mutex
	cond   *sync.Cond
	queue  *list.List
	closed bool

	concurrency int
	workerCount int
	workers     sync.WaitGroup
	reporter    sync.WaitGroup
}

type taskEntry struct {
	task   Task
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
	wakeup func()
	// nextRun is the earliest time a delayed (ErrDelay) requeue may execute
	// again. Zero means the entry may run immediately.
	nextRun    time.Time
	taskType   string
	enqueuedAt time.Time
}

func (e *taskEntry) finish() {
	e.once.Do(func() {
		e.cancel()
		close(e.done)
	})
}

type taskHandle struct {
	entry *taskEntry
}

func (h *taskHandle) Cancel() {
	h.entry.cancel()
	h.entry.wakeup()
}

func (h *taskHandle) Wait(ctx context.Context) error {
	select {
	case <-h.entry.done:
		return nil
	default:
	}

	select {
	case <-h.entry.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func New(concurrency int) *nodeScheduler {
	if concurrency <= 0 {
		panic("node scheduler concurrency must be greater than zero")
	}

	ctx, cancel := context.WithCancel(context.Background())
	scheduler := &nodeScheduler{
		ctx:     ctx,
		cancel:  cancel,
		stats:   newSchedulerStats(concurrency),
		metrics: newSchedulerMetrics(paramtable.GetStringNodeID()),
		queue:   list.New(),
	}
	scheduler.cond = sync.NewCond(&scheduler.mu)
	scheduler.resize(concurrency)
	scheduler.reporter.Add(1)
	go scheduler.reportStats()
	return scheduler
}

func (s *nodeScheduler) resize(concurrency int) {
	if concurrency <= 0 {
		panic("node scheduler concurrency must be greater than zero")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.concurrency == concurrency {
		return
	}

	s.concurrency = concurrency
	s.stats.setCapacity(concurrency)
	s.metrics.setConcurrency(concurrency)
	if concurrency > s.workerCount {
		additional := concurrency - s.workerCount
		s.workerCount += additional
		s.workers.Add(additional)
		for i := 0; i < additional; i++ {
			go s.runWorker()
		}
		return
	}
	// Shrinking: wake the excess workers so they observe workerCount > concurrency
	// and exit from dequeue. Broadcasting is required since an arbitrary number of
	// workers may need to wake and terminate.
	s.cond.Broadcast()
}

func (s *nodeScheduler) Submit(task Task) TaskHandle {
	ctx, cancel := context.WithCancel(s.ctx) // #nosec G118 -- task completion invokes the retained cancel function.
	entry := &taskEntry{
		task:       task,
		ctx:        ctx,
		cancel:     cancel,
		done:       make(chan struct{}),
		wakeup:     s.wakeup,
		taskType:   TaskTypeName(task),
		enqueuedAt: time.Now(),
	}
	handle := &taskHandle{entry: entry}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		entry.finish()
		return handle
	}
	// The queue is intentionally unbounded: Submit must never wait for capacity.
	s.queue.PushBack(entry)
	s.stats.submit(entry.taskType)
	s.metrics.observeEnqueued(entry.taskType)
	s.cond.Signal()
	s.mu.Unlock()
	return handle
}

func (s *nodeScheduler) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.workers.Wait()
		s.reporter.Wait()
		s.metrics.setConcurrency(0)
		return
	}

	s.closed = true
	for element := s.queue.Front(); element != nil; element = element.Next() {
		entry := element.Value.(*taskEntry)
		s.stats.cancelQueued(entry.taskType)
		s.metrics.observeDequeued(entry.taskType, time.Since(entry.enqueuedAt))
		entry.finish()
	}
	s.queue.Init()
	s.cancel()
	s.cond.Broadcast()
	s.mu.Unlock()

	s.workers.Wait()
	s.reporter.Wait()
	s.metrics.setConcurrency(0)
}

func (s *nodeScheduler) wakeup() {
	s.mu.Lock()
	s.cond.Signal()
	s.mu.Unlock()
}

func (s *nodeScheduler) runWorker() {
	defer s.workers.Done()
	for {
		entry := s.dequeue()
		if entry == nil {
			return
		}
		s.metrics.observeDequeued(entry.taskType, time.Since(entry.enqueuedAt))
		if entry.ctx.Err() != nil {
			s.stats.cancelStarted(entry.taskType)
			entry.finish()
			continue
		}

		startedAt := time.Now()
		s.metrics.observeExecutionStarted(entry.taskType)
		err := entry.task.Execute(entry.ctx)
		executeDuration := time.Since(startedAt)
		s.metrics.observeExecutionFinished(
			entry.taskType,
			taskExecutionStatus(entry.ctx.Err(), err),
			executeDuration,
		)
		if entry.ctx.Err() != nil {
			// Context canceled (e.g. shutdown): finish the entry without
			// requeueing. Note the task itself may not have done its queue
			// bookkeeping — with a canceled ctx a retryable segment task
			// stays in pendingTasks[0] and the segment stops submitting. That
			// is confined to the shutdown path (Submit handles are dropped,
			// Cancel is never called) and must be drained by the owner before
			// Close completes; see ViewConfig.Runtime.
			s.stats.finishCanceled(entry.taskType, executeDuration)
			entry.finish()
			continue
		}
		if err != nil && errors.Is(err, ErrDelay) {
			if s.requeue(entry, executeDuration) {
				continue
			}
			s.stats.finishDelayed(entry.taskType, executeDuration, false)
			entry.finish()
			continue
		}
		if err != nil {
			s.stats.finishFailed(entry.taskType, executeDuration)
			mlog.Error(entry.ctx, "node scheduler task failed",
				mlog.String("taskType", entry.taskType),
				mlog.Err(err))
		} else {
			s.stats.finishCompleted(entry.taskType, executeDuration)
		}
		entry.finish()
	}
}

func (s *nodeScheduler) dequeue() *taskEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		if s.closed || s.workerCount > s.concurrency {
			s.workerCount--
			return nil
		}
		if s.queue.Len() > 0 {
			now := time.Now()
			for element := s.queue.Front(); element != nil; element = element.Next() {
				entry := element.Value.(*taskEntry)
				// A delayed requeue is not runnable yet: skip it so it cannot
				// head-of-line block runnable entries behind it. Ordering among
				// tasks is the caller's responsibility, not this FIFO's. If every
				// entry is delayed, fall through to wait for the requeue's
				// wake-up timer instead of spinning.
				if !entry.nextRun.IsZero() && now.Before(entry.nextRun) {
					continue
				}
				s.stats.start(entry.taskType, time.Since(entry.enqueuedAt))
				s.queue.Remove(element)
				return entry
			}
			s.cond.Wait()
			continue
		}
		s.cond.Wait()
	}
}

func (s *nodeScheduler) requeue(entry *taskEntry, executeDuration time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed || entry.ctx.Err() != nil {
		return false
	}
	// Back off a delayed retry: without the delay, a task whose Execute keeps
	// failing (e.g. an object-storage outage surfaced as ErrDelay) is dequeued
	// and re-executed in a tight loop, burning a worker at 100% CPU. The timer
	// wakes the condition variable once the entry becomes runnable again.
	entry.nextRun = time.Now().Add(delayOnRequeue)
	entry.enqueuedAt = time.Now()
	s.queue.PushBack(entry)
	s.stats.finishDelayed(entry.taskType, executeDuration, true)
	s.metrics.observeEnqueued(entry.taskType)
	s.cond.Signal()
	time.AfterFunc(delayOnRequeue, s.wakeup)
	return true
}

// delayOnRequeue is the minimum pause between a failed (ErrDelay) execution
// and its retry. It bounds the retry rate of every scheduler task without
// blocking the queue: delayed entries are simply not runnable until the pause
// elapses.
const delayOnRequeue = 100 * time.Millisecond

var getGlobalScheduler = sync.OnceValue(func() *nodeScheduler {
	params := paramtable.Get()
	ratioParam := &params.CommonCfg.NodeSchedulerMaxConcurrencyRatio
	cpu := hardware.GetCPUNum()
	concurrency, ok := concurrencyFromRatio(cpu, ratioParam.GetAsFloat())
	if !ok {
		concurrency = cpu
		mlog.Warn(context.TODO(), "invalid node scheduler concurrency ratio, use default concurrency",
			mlog.String("value", ratioParam.GetValue()),
			mlog.Int("concurrency", concurrency))
	}

	scheduler := New(concurrency)
	params.Watch(ratioParam.Key, config.NewHandler("node-scheduler-concurrency", func(event *config.Event) {
		if !event.HasUpdated {
			return
		}
		ratio := ratioParam.GetAsFloat()
		concurrency, ok := concurrencyFromRatio(hardware.GetCPUNum(), ratio)
		if !ok {
			mlog.Warn(context.TODO(), "ignore invalid node scheduler concurrency ratio",
				mlog.String("value", ratioParam.GetValue()))
			return
		}
		scheduler.resize(concurrency)
		mlog.Info(context.TODO(), "node scheduler concurrency resized",
			mlog.Float64("ratio", ratio),
			mlog.Int("concurrency", concurrency))
	}))
	return scheduler
})

func concurrencyFromRatio(cpu int, ratio float64) (int, bool) {
	if cpu <= 0 || ratio <= 0 || math.IsNaN(ratio) || math.IsInf(ratio, 0) {
		return 0, false
	}
	return max(1, int(float64(cpu)*ratio)), true
}

func Get() Scheduler {
	return getGlobalScheduler()
}
