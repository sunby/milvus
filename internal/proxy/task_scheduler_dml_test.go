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

package proxy

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type dmlQueueAllocatorFunc func(context.Context) (Timestamp, error)

func (f dmlQueueAllocatorFunc) AllocOne(ctx context.Context) (Timestamp, error) {
	return f(ctx)
}

func TestDmTaskQueue_SlowAllocationDoesNotBlockOtherTasks(t *testing.T) {
	for _, operation := range []string{"enqueue", "complete and activate"} {
		t.Run(operation, func(t *testing.T) {
			slowCtx, cancel := context.WithCancel(context.Background())
			entered := make(chan struct{})
			queue := newDmTaskQueue(dmlQueueAllocatorFunc(func(ctx context.Context) (Timestamp, error) {
				if ctx == slowCtx {
					close(entered)
					<-ctx.Done()
					return 0, ctx.Err()
				}
				return 100, nil
			}))
			slow := newMockDmlTask(slowCtx)
			other := newDefaultMockDmlTask()
			active := newDefaultMockDmlTask()
			slow.SetID(-1)
			other.SetID(2)
			active.SetID(1)
			queue.AddActiveTask(active)
			var wg sync.WaitGroup
			t.Cleanup(func() {
				cancel()
				wg.Wait()
			})
			slowResult := make(chan error, 1)
			wg.Go(func() { slowResult <- queue.Enqueue(slow) })
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("slow task did not reach timestamp allocation")
			}

			result := make(chan error, 1)
			var completed task
			wg.Go(func() {
				if operation == "enqueue" {
					result <- queue.Enqueue(other)
					return
				}
				completed = queue.PopActiveTask(active.ID())
				queue.AddActiveTask(other)
				result <- nil
			})
			select {
			case err := <-result:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("unrelated task blocked behind timestamp allocation")
			}
			require.Same(t, other, queue.getTaskByReqID(other.ID()))
			if operation == "complete and activate" {
				require.Same(t, active, completed)
				require.Nil(t, queue.getTaskByReqID(active.ID()))
			}
			cancel()
			require.ErrorIs(t, <-slowResult, context.Canceled)
			require.Nil(t, queue.getTaskByReqID(slow.ID()))
		})
	}
}

func TestDmTaskQueue_ConcurrentAdmissionRespectsCapacity(t *testing.T) {
	const capacity, producers = 8, 32
	entered := make(chan struct{}, producers)
	release := make(chan struct{})
	var releaseOnce sync.Once
	var wg sync.WaitGroup
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		wg.Wait()
	})
	var next atomic.Uint64
	queue := newDmTaskQueue(dmlQueueAllocatorFunc(func(context.Context) (Timestamp, error) {
		entered <- struct{}{}
		<-release
		return next.Add(1), nil
	}))
	queue.setMaxTaskNum(capacity)
	tasks := make([]*mockDmlTask, producers)
	errs := make([]error, producers)
	for i := range tasks {
		tasks[i] = newDefaultMockDmlTask()
		wg.Go(func() { errs[i] = queue.Enqueue(tasks[i]) })
	}
	// Every producer passes the advisory full check before any can append.
	for range producers {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("timestamp allocation was serialized")
		}
	}
	releaseOnce.Do(func() { close(release) })
	wg.Wait()
	accepted := make(map[UniqueID]task)
	for i, err := range errs {
		if err != nil {
			require.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
			require.Nil(t, queue.getTaskByReqID(tasks[i].ID()))
			continue
		}
		require.NotContains(t, accepted, tasks[i].ID())
		accepted[tasks[i].ID()] = tasks[i]
	}
	require.Len(t, accepted, capacity)
	require.Len(t, queue.utBufChan, 1)
	for queued := queue.PopUnissuedTask(); queued != nil; queued = queue.PopUnissuedTask() {
		require.Same(t, accepted[queued.ID()], queued)
		delete(accepted, queued.ID())
	}
	require.Empty(t, accepted)
}

type failingDMLTask struct {
	*mockDmlTask
	stage string
	err   error
}

func (t *failingDMLTask) fail(stage string) error {
	if t.stage == stage {
		return t.err
	}
	return nil
}

func (t *failingDMLTask) setChannels() error                { return t.fail("channels") }
func (t *failingDMLTask) OnEnqueue() error                  { return t.fail("enqueue") }
func (t *failingDMLTask) PreExecute(context.Context) error  { return t.fail("pre") }
func (t *failingDMLTask) Execute(context.Context) error     { return t.fail("execute") }
func (t *failingDMLTask) PostExecute(context.Context) error { return t.fail("post") }

func TestDmTaskQueue_FailuresDoNotLeakTasks(t *testing.T) {
	for _, stage := range []string{"channels", "enqueue", "timestamp", "pre", "execute", "post"} {
		t.Run(stage, func(t *testing.T) {
			failure := context.DeadlineExceeded
			queue := newDmTaskQueue(dmlQueueAllocatorFunc(func(context.Context) (Timestamp, error) {
				if stage == "timestamp" {
					return 0, failure
				}
				return 100, nil
			}))
			candidate := &failingDMLTask{mockDmlTask: newDefaultMockDmlTask(), stage: stage, err: failure}
			switch stage {
			case "channels", "enqueue", "timestamp":
				require.ErrorIs(t, queue.Enqueue(candidate), failure)
				require.Empty(t, queue.utBufChan)
			default:
				require.NoError(t, queue.Enqueue(candidate))
				require.Same(t, candidate, queue.PopUnissuedTask())
				(&taskScheduler{}).processTask(candidate, queue)
				require.ErrorIs(t, candidate.WaitToFinish(), failure)
			}
			require.True(t, queue.utEmpty())
			require.Nil(t, queue.getTaskByReqID(candidate.ID()))
			require.Empty(t, queue.activeTasks)
		})
	}
}

func TestDeleteRunner_ConcurrentProduceInitializesSharedRequest(t *testing.T) {
	for _, base := range []*commonpb.MsgBase{nil, {MsgID: 42, Timestamp: 123, SourceID: 17}} {
		name := "nil base"
		if base != nil {
			name = "existing base"
		}
		t.Run(name, func(t *testing.T) {
			const producers = 32
			req := &milvuspb.DeleteRequest{Base: base, CollectionName: "test", Expr: "pk > 0"}
			expected := proto.Clone(req).(*milvuspb.DeleteRequest)
			if expected.Base == nil {
				expected.Base = &commonpb.MsgBase{}
			}
			expected.Base.MsgType = commonpb.MsgType_Delete
			expected.Base.SourceID = paramtable.GetNodeID()
			queue := newDmTaskQueue(newMockTsoAllocator())
			queue.setMaxTaskNum(producers)
			runner := &deleteRunner{
				req: req, queue: queue, collectionID: 10,
				vChannels: []string{"p0_10v0"}, pChannels: []string{"p0"},
			}
			start := make(chan struct{})
			tasks := make([]*deleteTask, producers)
			errs := make([]error, producers)
			var wg sync.WaitGroup
			for i := range producers {
				wg.Go(func() {
					<-start
					keys := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{int64(i)}}}}
					tasks[i], errs[i] = runner.produce(context.Background(), keys, 20)
				})
			}
			close(start)
			wg.Wait()
			require.True(t, proto.Equal(expected, req), "only the request type and source may change")
			ids := make(map[UniqueID]struct{}, producers)
			for i, task := range tasks {
				require.NoError(t, errs[i])
				require.Same(t, req, task.req)
				require.NotContains(t, ids, task.ID())
				ids[task.ID()] = struct{}{}
				require.Equal(t, Timestamp(task.ID()), task.BeginTs())
				require.Equal(t, []int64{int64(i)}, task.primaryKeys.GetIntId().GetData())
				require.Equal(t, runner.vChannels, task.vChannels)
				require.Equal(t, runner.pChannels, task.pChannels)
				require.Same(t, task, queue.getTaskByReqID(task.ID()))
			}
		})
	}
}
