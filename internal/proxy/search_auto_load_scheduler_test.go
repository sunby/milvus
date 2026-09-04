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
	scheduler, err := newTaskScheduler(ctx, newMockTsoAllocator())
	require.NoError(t, err)
	t.Cleanup(func() {
		cancel()
		scheduler.Close()
	})
	node := &Proxy{
		ctx: ctx, sched: scheduler, metaCache: cache, mixCoord: coordinator,
		viewQueryClient: &autoLoadViewQueryClient{checkErr: merr.ErrCollectionNotLoaded},
	}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	return node, coordinator, indexes
}

type autoLoadSchedulerTask struct {
	*mockTask
	execute func(context.Context) error
}

func (t *autoLoadSchedulerTask) Execute(ctx context.Context) error {
	return t.execute(ctx)
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

func TestAutoLoadUsesDQLWhenDDLIsBusy(t *testing.T) {
	node, coordinator, _ := newAutoLoadSchedulingProxy(t)
	loaded := make(chan *querypb.LoadCollectionRequest, 1)
	coordinator.EXPECT().LoadCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, request *querypb.LoadCollectionRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
			loaded <- request
			return merr.Success(), nil
		}).Once()
	require.NoError(t, node.sched.Start())

	ddlStarted := make(chan struct{})
	releaseDDL := make(chan struct{})
	t.Cleanup(func() { close(releaseDDL) })
	blocker := &autoLoadSchedulerTask{
		mockTask: newMockTask(node.ctx),
		execute: func(ctx context.Context) error {
			close(ddlStarted)
			select {
			case <-releaseDDL:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
	require.NoError(t, node.sched.ddQueue.Enqueue(blocker))
	awaitAutoLoadSchedulerResult(t, ddlStarted)
	// The dispatcher pops one task before blocking on the occupied DDL worker.
	require.NoError(t, node.sched.ddQueue.Enqueue(newMockTask(node.ctx)))
	require.Eventually(t, node.sched.ddQueue.utEmpty, time.Second, time.Millisecond)
	// Fill the actual pending queue too, without releasing the worker.
	require.NoError(t, node.sched.ddQueue.Enqueue(newMockTask(node.ctx)))

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

func TestAutoLoadRejectedWhenDQLQueueIsFull(t *testing.T) {
	node, coordinator, _ := newAutoLoadSchedulingProxy(t)
	// Do not start dispatching: the sole DQL queue slot remains occupied.
	require.NoError(t, node.sched.dqQueue.Enqueue(newMockTask(node.ctx)))
	err := node.ensureCollectionReady(node.ctx, "db", "collection")
	require.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	require.Equal(t, merr.Code(merr.ErrServiceTooManyRequests), merr.Status(err).GetCode())
	require.True(t, merr.Status(err).GetRetriable())
	coordinator.AssertNotCalled(t, "LoadCollection", mock.Anything, mock.Anything)
	coordinator.AssertNotCalled(t, "DescribeIndex", mock.Anything, mock.Anything)
}

func TestAutoLoadKeepsDQLWorkerAfterCallerCancellation(t *testing.T) {
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

	// A regular query shares the same single worker. It must not start merely
	// because the search caller stopped waiting for its shared background load.
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
	require.Eventually(t, node.sched.dqQueue.utEmpty, time.Second, time.Millisecond)
	require.Never(t, func() bool {
		select {
		case <-probeStarted:
			return true
		default:
			return false
		}
	}, 100*time.Millisecond, time.Millisecond, "DQL worker was released before the shared load finished")
	releaseLoad <- struct{}{}
	require.True(t, awaitAutoLoadSchedulerResult(t, probeStarted))
	require.NoError(t, probe.WaitToFinish())
}

func TestAutoLoadKeepsDQLQueueSlotAfterCallerCancellation(t *testing.T) {
	node, coordinator, _ := newAutoLoadSchedulingProxy(t)
	loaded := make(chan struct{}, 1)
	coordinator.EXPECT().LoadCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(context.Context, *querypb.LoadCollectionRequest, ...grpc.CallOption) (*commonpb.Status, error) {
			loaded <- struct{}{}
			return merr.Success(), nil
		}).Once()

	callerCtx, cancelCaller := context.WithCancel(node.ctx)
	defer cancelCaller()
	result := make(chan error, 1)
	go func() { result <- node.ensureCollectionReady(callerCtx, "db", "collection") }()
	// Keep dispatch stopped until the calling search has canceled.
	require.Eventually(t, func() bool { return !node.sched.dqQueue.utEmpty() }, time.Second, time.Millisecond)
	queued := node.sched.dqQueue.FrontUnissuedTask()
	require.NotNil(t, queued)
	cancelCaller()
	require.ErrorIs(t, awaitAutoLoadSchedulerResult(t, result), context.Canceled)
	require.NoError(t, queued.TraceCtx().Err())

	status, err := node.loadCollectionForDQL(node.ctx, &milvuspb.LoadCollectionRequest{
		DbName: "db", CollectionName: "collection",
	})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(status), merr.ErrServiceTooManyRequests)
	require.NoError(t, node.sched.Start())
	awaitAutoLoadSchedulerResult(t, loaded)
	// The shared load's deferred cancel signals that its task wait completed.
	awaitAutoLoadSchedulerResult(t, queued.TraceCtx().Done())
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
