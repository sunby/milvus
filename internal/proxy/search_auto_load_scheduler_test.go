// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Exercise the real Proxy load entry points, task lifecycle and scheduler. Only
// metadata and the coordinator/readiness endpoints are test doubles.
func newAutoLoadSchedulingProxy(t *testing.T) (*Proxy, *mocks.MockMixCoordClient, *indexpb.DescribeIndexResponse) {
	t.Helper()
	enableAutoLoad(t)
	for _, setting := range []struct {
		key, previous, value string
	}{
		{Params.ProxyCfg.MaxTaskNum.Key, Params.ProxyCfg.MaxTaskNum.GetValue(), "1"},
		{Params.ProxyCfg.DDLConcurrency.Key, Params.ProxyCfg.DDLConcurrency.GetValue(), "1"},
		{Params.CommonCfg.AuthorizationEnabled.Key, Params.CommonCfg.AuthorizationEnabled.GetValue(), "false"},
	} {
		require.NoError(t, Params.Save(setting.key, setting.value))
		t.Cleanup(func() { require.NoError(t, Params.Save(setting.key, setting.previous)) })
	}

	schema, err := newSchemaInfo(&schemapb.CollectionSchema{
		Name: "collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}},
			},
		},
	})
	require.NoError(t, err)
	cache := NewMockCache(t)
	cache.EXPECT().GetCollectionID(mock.Anything, "db", "collection").Return(int64(100), nil).Maybe()
	cache.EXPECT().GetCollectionInfo(mock.Anything, "db", "collection", int64(100)).Return(&collectionInfo{
		CollID: 100, VChannels: []string{"v0"},
	}, nil).Maybe()
	cache.EXPECT().GetCollectionSchema(mock.Anything, "db", "collection").Return(schema, nil).Maybe()

	coordinator := mocks.NewMockMixCoordClient(t)
	coordinator.EXPECT().ShowLoadCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
		Status: merr.Status(merr.ErrCollectionNotLoaded),
	}, nil).Maybe()
	indexes := &indexpb.DescribeIndexResponse{
		Status:     merr.Success(),
		IndexInfos: []*indexpb.IndexInfo{{FieldID: 101, IndexID: 1000}},
	}
	coordinator.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(indexes, nil).Maybe()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	allocator, err := newTimestampAllocator(newMockTimestampAllocatorInterface(), 0)
	require.NoError(t, err)
	scheduler, err := newTaskScheduler(ctx, allocator)
	require.NoError(t, err)
	t.Cleanup(func() {
		cancel()
		scheduler.Close()
	})
	node := &Proxy{
		ctx: ctx, sched: scheduler, metaCache: cache, mixCoord: coordinator, tsoAllocator: allocator,
		viewQueryClient: &autoLoadViewQueryClient{checkErr: merr.ErrCollectionNotLoaded},
	}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	return node, coordinator, indexes
}

type autoLoadSchedulerTask struct {
	*mockTask
	execute   func(context.Context) error
	isSubTask bool
}

func (t *autoLoadSchedulerTask) Execute(ctx context.Context) error {
	return t.execute(ctx)
}

func (t *autoLoadSchedulerTask) IsSubTask() bool {
	return t.isSubTask
}

func awaitAutoLoadSchedulerResult[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for scheduler progress")
		var zero T
		return zero
	}
}

func TestAutoLoadBypassesSaturatedTaskQueues(t *testing.T) {
	node, coordinator, _ := newAutoLoadSchedulingProxy(t)
	loaded := make(chan *querypb.LoadCollectionRequest, 1)
	coordinator.EXPECT().LoadCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, request *querypb.LoadCollectionRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
			loaded <- request
			return merr.Success(), nil
		}).Once()
	require.NoError(t, node.sched.Start())

	for _, queue := range []taskQueue{node.sched.ddQueue, node.sched.dqQueue} {
		started := make(chan struct{})
		release := make(chan struct{})
		t.Cleanup(func() { close(release) })
		blocker := &autoLoadSchedulerTask{
			mockTask: newMockTask(node.ctx),
			execute: func(ctx context.Context) error {
				close(started)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		}
		require.NoError(t, queue.Enqueue(blocker))
		awaitAutoLoadSchedulerResult(t, started)
		// Occupy the dispatcher in Submit, then fill the pending queue too.
		require.NoError(t, queue.Enqueue(newMockTask(node.ctx)))
		require.Eventually(t, queue.utEmpty, time.Second, time.Millisecond)
		require.NoError(t, queue.Enqueue(newMockTask(node.ctx)))
	}

	require.NoError(t, node.ensureCollectionReady(node.ctx, "db", "collection"))
	request := awaitAutoLoadSchedulerResult(t, loaded)
	require.Equal(t, int64(100), request.GetCollectionID())
	require.Equal(t, commonpb.MsgType_LoadCollection, request.GetBase().GetMsgType())
	require.NotZero(t, request.GetBase().GetMsgID())
	require.NotZero(t, request.GetBase().GetTimestamp())
	require.Equal(t, map[int64]int64{101: 1000}, request.GetFieldIndexID())
	require.Equal(t, commonpb.LoadPriority_HIGH, request.GetPriority())

	// The explicit API must still respect the saturated DDL queue.
	status, err := node.LoadCollection(node.ctx, &milvuspb.LoadCollectionRequest{DbName: "db", CollectionName: "collection"})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(status), merr.ErrServiceTooManyRequests)
}

func TestAutoLoadDoesNotRequireScheduler(t *testing.T) {
	node, coordinator, _ := newAutoLoadSchedulingProxy(t)
	node.sched = nil
	coordinator.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()
	require.NoError(t, node.ensureCollectionReady(node.ctx, "db", "collection"))
}

func TestAutoLoadAllowsRequeryWhileDQLPoolIsFull(t *testing.T) {
	node, coordinator, _ := newAutoLoadSchedulingProxy(t)
	coordinator.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()
	require.NoError(t, node.sched.Start())
	parentCtx, cancelParent := context.WithCancel(node.ctx)
	defer cancelParent()
	parentStarted := make(chan struct{})
	startRequery := make(chan struct{})
	requeryStarted := make(chan struct{})
	parent := &autoLoadSchedulerTask{
		mockTask: newMockTask(parentCtx),
		execute: func(ctx context.Context) error {
			close(parentStarted)
			select {
			case <-startRequery:
			case <-ctx.Done():
				return ctx.Err()
			}
			requery := &autoLoadSchedulerTask{
				mockTask:  newMockTask(ctx),
				isSubTask: true,
				execute: func(context.Context) error {
					close(requeryStarted)
					return nil
				},
			}
			if err := node.sched.dqQueue.Enqueue(requery); err != nil {
				return err
			}
			return requery.WaitToFinish()
		},
	}
	parent.name, parent.tType = SearchTaskName, commonpb.MsgType_Search
	require.NoError(t, node.sched.dqQueue.Enqueue(parent))
	awaitAutoLoadSchedulerResult(t, parentStarted)

	// A cold collection can load while a search on another collection holds
	// the sole main worker. Its subsequent requery can still be dispatched.
	loadCtx, cancelLoad := context.WithTimeout(node.ctx, time.Second)
	defer cancelLoad()
	require.NoError(t, node.ensureCollectionReady(loadCtx, "db", "collection"))
	close(startRequery)
	awaitAutoLoadSchedulerResult(t, requeryStarted)
	require.NoError(t, parent.WaitToFinish())
}

func TestAutoLoadDoesNotOccupyDQLWorkerAfterCallerCancellation(t *testing.T) {
	node, coordinator, _ := newAutoLoadSchedulingProxy(t)
	loadStarted := make(chan context.Context, 1)
	releaseLoad := make(chan struct{}, 1)
	var loadFinished atomic.Bool
	coordinator.EXPECT().LoadCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, _ *querypb.LoadCollectionRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
			loadStarted <- ctx
			defer loadFinished.Store(true)
			select {
			case <-releaseLoad:
				return merr.Success(), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}).Once()
	require.NoError(t, node.sched.Start())

	callerCtx, cancelCaller := context.WithCancel(node.ctx)
	defer cancelCaller()
	result := make(chan error, 1)
	go func() { result <- node.ensureCollectionReady(callerCtx, "db", "collection") }()
	loadCtx := awaitAutoLoadSchedulerResult(t, loadStarted)
	cancelCaller()
	require.ErrorIs(t, awaitAutoLoadSchedulerResult(t, result), context.Canceled)
	require.NoError(t, loadCtx.Err())

	// A slow shared load must leave the main DQL worker available, even after
	// its caller has canceled. The load still uses its own lifecycle context.
	probeStarted := make(chan bool, 1)
	probe := &autoLoadSchedulerTask{
		mockTask: newMockTask(node.ctx),
		execute: func(context.Context) error {
			probeStarted <- loadFinished.Load()
			return nil
		},
	}
	probe.name, probe.tType = SearchTaskName, commonpb.MsgType_Search
	require.NoError(t, node.sched.dqQueue.Enqueue(probe))
	require.False(t, awaitAutoLoadSchedulerResult(t, probeStarted))
	require.NoError(t, probe.WaitToFinish())
	releaseLoad <- struct{}{}
	awaitAutoLoadSchedulerResult(t, loadCtx.Done())
}

func TestAutoLoadPreservesTimestampAllocationError(t *testing.T) {
	node, coordinator, _ := newAutoLoadSchedulingProxy(t)
	node.tsoAllocator.tso = coordinator
	coordinator.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).Return(nil, merr.ErrServiceUnavailable).Once()

	err := node.ensureCollectionReady(node.ctx, "db", "collection")
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Equal(t, merr.Code(merr.ErrServiceUnavailable), merr.Status(err).GetCode())
	require.True(t, merr.Status(err).GetRetriable())
	coordinator.AssertNotCalled(t, "DescribeIndex", mock.Anything, mock.Anything)
	coordinator.AssertNotCalled(t, "LoadCollection", mock.Anything, mock.Anything)
}

func TestAutoLoadPreservesLoadValidationAndErrors(t *testing.T) {
	for _, test := range []struct {
		name          string
		collection    string
		missingIndex  bool
		describeError bool
		loadStatus    *commonpb.Status
		loadError     error
		expectedError error
	}{
		{name: "invalid name", collection: "invalid name", expectedError: merr.ErrParameterInvalid},
		{name: "missing vector index", missingIndex: true, expectedError: merr.ErrParameterInvalid},
		{name: "index RPC status", describeError: true, expectedError: merr.ErrServiceUnavailable},
		{name: "load RPC error", loadError: merr.ErrServiceUnavailable, expectedError: merr.ErrServiceUnavailable},
		{name: "load RPC status", loadStatus: merr.Status(merr.ErrServiceUnavailable), expectedError: merr.ErrServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			node, coordinator, indexes := newAutoLoadSchedulingProxy(t)
			if test.missingIndex {
				indexes.IndexInfos = nil
			}
			if test.describeError {
				indexes.Status = merr.Status(test.expectedError)
			}
			if test.loadError != nil || test.loadStatus != nil {
				coordinator.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(test.loadStatus, test.loadError).Once()
			}
			require.NoError(t, node.sched.Start())
			collection := test.collection
			if collection == "" {
				collection = "collection"
			}
			status, err := node.loadCollectionForDQL(node.ctx, &milvuspb.LoadCollectionRequest{
				DbName: "db", CollectionName: collection,
			})
			require.NoError(t, err)
			require.ErrorIs(t, merr.Error(status), test.expectedError)
			require.Equal(t, merr.Code(test.expectedError), status.GetCode())
			require.Equal(t, merr.IsRetryableErr(test.expectedError), status.GetRetriable())
		})
	}
}

func TestAutoLoadRejectsUnhealthyProxyWithoutScheduler(t *testing.T) {
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Abnormal)
	status, err := node.loadCollectionForDQL(context.Background(), &milvuspb.LoadCollectionRequest{})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(status), merr.ErrServiceNotReady)
}
