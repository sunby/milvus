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

package querycoordv2

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	metastoremocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type readinessCatalog struct {
	views []*viewpb.QueryViewOfShard
}

func (c *readinessCatalog) ListQueryViews(context.Context) ([]*viewpb.QueryViewOfShard, error) {
	return c.views, nil
}

func (c *readinessCatalog) SaveQueryViews(context.Context, []*viewpb.QueryViewOfShard) error {
	return nil
}

type readinessSyncer struct{ sent chan syncer.SyncView }

func (s *readinessSyncer) SyncViews(_ context.Context, group syncer.SyncGroup) error {
	for _, views := range group.ViewsByNode {
		for _, view := range views {
			s.sent <- view
		}
	}
	return nil
}
func (*readinessSyncer) Close() error { return nil }

func newReadinessTestServer(t *testing.T, views ...*viewpb.QueryViewOfShard) (*Server, *metastoremocks.QueryCoordCatalog, *readinessSyncer) {
	t.Helper()
	return newReadinessTestServerWithConfig(t, &querypb.CollectionLoadInfo{CollectionID: 100}, views...)
}

func newReadinessTestServerWithConfig(t *testing.T, info *querypb.CollectionLoadInfo, views ...*viewpb.QueryViewOfShard) (*Server, *metastoremocks.QueryCoordCatalog, *readinessSyncer) {
	t.Helper()
	ctx := context.Background()
	catalog := metastoremocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return([]*querypb.CollectionLoadInfo{info}, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).Return(map[int64][]*querypb.PartitionLoadInfo{}, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return([]*querypb.Replica{{ID: 1000, CollectionID: 100}}, nil).Once()
	store, err := loadmgr.RecoverLoadConfigStore(ctx, catalog)
	require.NoError(t, err)
	syncer := &readinessSyncer{sent: make(chan syncer.SyncView, 64)}
	registry, err := coordview.RecoverShardViewRegistry(ctx, &readinessCatalog{views: views}, syncer)
	require.NoError(t, err)
	runtime := &qviewsRuntime{loadConfigStore: store, shardViewRegistry: registry, readyChanges: newCollectionReadiness(store, registry)}
	t.Cleanup(func() { runtime.readyChanges.Close(); registry.Close() })
	s := &Server{ctx: ctx, qviewsRuntime: runtime}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	return s, catalog, syncer
}

func readinessRequest(channels ...string) *querypb.WaitCollectionReadyRequest {
	return &querypb.WaitCollectionReadyRequest{CollectionID: 100, ExpectedVchannels: channels}
}

func receiveReadinessSync(t *testing.T, s *readinessSyncer, state qviews.QueryViewState) syncer.SyncView {
	t.Helper()
	select {
	case view := <-s.sent:
		require.Equal(t, state, view.View.State())
		return view
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for node sync")
		return syncer.SyncView{}
	}
}

func TestWaitCollectionReadyWakesFromStreamingNodeUp(t *testing.T) {
	first, second := "by-dev-rootcoord-dml_100v0", "by-dev-rootcoord-dml_100v1"
	s, _, syncer := newReadinessTestServer(t, testPersistedQueryView(100, qviews.ShardID{ReplicaID: 1000, VChannel: first}))
	req := readinessRequest(first, second)
	req.CheckOnly = true
	status, err := s.WaitCollectionReady(context.Background(), req)
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(status), merr.ErrCollectionNotLoaded, "one Up shard must not hide a missing shard")
	req.CheckOnly = false
	result := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { status, err := s.WaitCollectionReady(ctx, req); result <- merr.CheckRPCCall(status, err) }()
	assertReadinessBlocked(t, result)
	shardID := qviews.ShardID{ReplicaID: 1000, VChannel: second}
	builder := qviews.NewQueryViewAtCoordBuilder(1000, &viewpb.DataViewOfCollection{
		CollectionId: 100, DataVersion: &viewpb.DataVersion{StreamingVersion: 1}, Shards: []*viewpb.DataViewOfShard{{Vchannel: second}},
	}, second)
	require.NoError(t, s.qviewsRuntime.shardViewRegistry.Ensure(shardID).AddPreparing(ctx, builder))
	preparing := receiveReadinessSync(t, syncer, qviews.QueryViewStatePreparing)
	ready := preparing.View.IntoProto()
	ready.Meta.State = viewpb.QueryViewState_QueryViewStateReady
	preparing.OnSyncResponse(qviews.NewQueryViewAtWorkNodeFromProto(ready))
	up := receiveReadinessSync(t, syncer, qviews.QueryViewStateUp)
	assertReadinessBlocked(t, result)
	// Drive the real Coord state machine, registry observer and RPC waiter from
	// the SN's Up acknowledgement; do not directly notify the test waiter.
	up.OnSyncResponse(up.View)
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("SN Up did not wake the readiness RPC")
	}
	status, err = s.WaitCollectionReady(ctx, req)
	require.NoError(t, merr.CheckRPCCall(status, err), "an already-ready collection needs no new notification")
}

type readinessNodeProvider struct{}

func (*readinessNodeProvider) Snapshot() *balancer.NodeSnapshot {
	return balancer.NewNodeSnapshot(1, nil)
}

type readinessDataViewProvider struct {
	fakeRuntimeDataViewProvider
	view *viewpb.DataViewOfCollection
}

func (p *readinessDataViewProvider) DataViewSnapshotForCollections(_ context.Context, ids map[int64]struct{}) *balancer.DataViewSnapshot {
	if _, ok := ids[p.view.GetCollectionId()]; !ok && ids != nil {
		return balancer.NewDataViewSnapshot(1, nil, nil)
	}
	return balancer.NewDataViewSnapshot(1, []*viewpb.DataViewOfCollection{p.view}, nil)
}

func newReadinessRecoveryBalancer(runtime *qviewsRuntime, channel string) *balancer.DefaultBalancer {
	config := balancer.DefaultBalanceConfig()
	config.TickerInterval = 0
	return balancer.NewDefaultBalancer(balancer.NewSnapshotBuilder(
		runtime.loadConfigStore, runtime.shardViewRegistry, &readinessNodeProvider{},
		&readinessDataViewProvider{view: &viewpb.DataViewOfCollection{
			CollectionId: 100, DataVersion: &viewpb.DataVersion{StreamingVersion: 1},
			Shards: []*viewpb.DataViewOfShard{{Vchannel: channel}},
		}}, config,
	), runtime.shardViewRegistry, nil)
}

func TestWaitCollectionReadyRebuildsUnrecoverableWithoutPeriodicScan(t *testing.T) {
	const channel = "by-dev-rootcoord-dml_100v0"
	s, _, stream := newReadinessTestServer(t)
	b := newReadinessRecoveryBalancer(s.qviewsRuntime, channel)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	b.Start(ctx)
	defer b.Stop()

	preparing := receiveReadinessSync(t, stream, qviews.QueryViewStatePreparing)
	ready := preparing.View.IntoProto()
	ready.Meta.State = viewpb.QueryViewState_QueryViewStateReady
	preparing.OnSyncResponse(qviews.NewQueryViewAtWorkNodeFromProto(ready))
	up := receiveReadinessSync(t, stream, qviews.QueryViewStateUp)
	result := make(chan error, 1)
	go func() {
		status, err := s.WaitCollectionReady(ctx, readinessRequest(channel))
		result <- merr.CheckRPCCall(status, err)
	}()
	assertReadinessBlocked(t, result)

	// Reproduce an SN losing its Ready view before acknowledging Up. The
	// running balancer must replace it without any explicit Trigger/Reconcile.
	failed := up.View.IntoProto()
	failed.Meta.State = viewpb.QueryViewState_QueryViewStateUnrecoverable
	up.OnSyncResponse(qviews.NewQueryViewAtWorkNodeFromProto(failed))
	replacement := receiveRecoverySync(t, stream, qviews.QueryViewStatePreparing)
	require.True(t, replacement.View.Version().GT(preparing.View.Version()))
	assertReadinessBlocked(t, result)
	ready = replacement.View.IntoProto()
	ready.Meta.State = viewpb.QueryViewState_QueryViewStateReady
	replacement.OnSyncResponse(qviews.NewQueryViewAtWorkNodeFromProto(ready))
	replacementUp := receiveRecoverySync(t, stream, qviews.QueryViewStateUp)
	assertReadinessBlocked(t, result)
	replacementUp.OnSyncResponse(replacementUp.View)
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("replacement Up did not complete the original readiness request")
	}
}

func TestLateUnrecoverableNotificationDoesNotRestoreReleasedReplica(t *testing.T) {
	for _, reload := range []bool{false, true} {
		name := "released"
		if reload {
			name = "reloaded with a new replica"
		}
		t.Run(name, func(t *testing.T) {
			const channel = "by-dev-rootcoord-dml_100v0"
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			s, catalog, stream := newReadinessTestServer(t)
			runtime := s.qviewsRuntime
			b := newReadinessRecoveryBalancer(runtime, channel)
			b.Trigger(balancer.TriggerScope{DirtyCollections: []int64{100}})
			require.NoError(t, b.Reconcile(ctx))
			preparing := receiveReadinessSync(t, stream, qviews.QueryViewStatePreparing)

			catalog.EXPECT().ReleaseReplicas(mock.Anything, int64(100)).Return(nil).Once()
			catalog.EXPECT().ReleaseCollection(mock.Anything, int64(100)).Return(nil).Once()
			require.NoError(t, runtime.loadConfigStore.Remove(ctx, 100))
			if reload {
				catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil).Once()
				catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Once()
				require.NoError(t, runtime.loadConfigStore.Put(ctx, &loadmgr.LoadConfig{
					CollectionID: 100, Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 2000}},
				}))
			}

			// Deliver the old replica's failure after its desired state has
			// been removed. Wait until the real notifier enqueues this shard.
			notified := make(chan struct{}, 1)
			runtime.shardViewRegistry.RegisterUnrecoverableNotifier(func(qviews.ShardID) { notified <- struct{}{} })
			failed := preparing.View.IntoProto()
			failed.Meta.State = viewpb.QueryViewState_QueryViewStateUnrecoverable
			preparing.OnSyncResponse(qviews.NewQueryViewAtWorkNodeFromProto(failed))
			select {
			case <-notified:
			case <-ctx.Done():
				t.Fatal("late failure was not enqueued")
			}
			require.NoError(t, b.Reconcile(ctx))
			stats := runtime.shardViewRegistry.Get(preparing.View.ShardID()).Stats()
			require.Nil(t, stats.PreparingVersion, "the removed replica must not be reloaded")
			require.Nil(t, stats.UpVersion)
		})
	}
}

func receiveRecoverySync(t *testing.T, s *readinessSyncer, state qviews.QueryViewState) syncer.SyncView {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		select {
		case view := <-s.sent:
			if view.View.State() == qviews.QueryViewStateDropped {
				view.OnSyncResponse(view.View)
				continue
			}
			require.Equal(t, state, view.View.State())
			return view
		case <-timer.C:
			t.Fatal("timed out waiting for replacement view sync")
			return syncer.SyncView{}
		}
	}
}

func assertReadinessBlocked(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("readiness returned before the collection was ready: %v", err)
	case <-time.After(35 * time.Millisecond):
	}
}

func TestWaitCollectionReadyEndsOnReleaseCancelAndShutdown(t *testing.T) {
	for _, scenario := range []string{"release", "cancel", "shutdown"} {
		t.Run(scenario, func(t *testing.T) {
			s, catalog, _ := newReadinessTestServer(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- s.waitCollectionReady(ctx, readinessRequest("by-dev-rootcoord-dml_100v0")) }()
			assertReadinessBlocked(t, result)
			var want error = merr.ErrCollectionNotLoaded
			switch scenario {
			case "release":
				catalog.EXPECT().ReleaseReplicas(mock.Anything, int64(100)).Return(nil).Once()
				catalog.EXPECT().ReleaseCollection(mock.Anything, int64(100)).Return(nil).Once()
				require.NoError(t, s.qviewsRuntime.loadConfigStore.Remove(ctx, 100))
			case "cancel":
				cancel()
				want = context.Canceled
			case "shutdown":
				s.qviewsRuntime.readyChanges.Close()
				want = merr.ErrServiceUnavailable
			}
			select {
			case err := <-result:
				require.ErrorIs(t, err, want)
			case <-time.After(time.Second):
				t.Fatal("waiter was not released")
			}
		})
	}
}

func TestWaitCollectionReadyGRPCContract(t *testing.T) {
	s, catalog, _ := newReadinessTestServer(t)
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	querypb.RegisterQueryCoordServer(grpcServer, s)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	conn, err := grpc.NewClient("passthrough:///readiness", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := querypb.NewQueryCoordClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_, err = client.WaitCollectionReady(ctx, readinessRequest("by-dev-rootcoord-dml_100v0"))
	require.Error(t, err, "a real RPC must honor its deadline")
	catalog.EXPECT().ReleaseReplicas(mock.Anything, int64(100)).Return(nil).Once()
	catalog.EXPECT().ReleaseCollection(mock.Anything, int64(100)).Return(nil).Once()
	require.NoError(t, s.qviewsRuntime.loadConfigStore.Remove(context.Background(), 100))
	status, err := client.WaitCollectionReady(context.Background(), readinessRequest("by-dev-rootcoord-dml_100v0"))
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(status), merr.ErrCollectionNotLoaded, "release must preserve the typed error across gRPC")
}

func TestWaitCollectionReadyCancellationDoesNotWaitForCatalog(t *testing.T) {
	s, catalog, _ := newReadinessTestServer(t)
	writeStarted, finishWrite := make(chan struct{}), make(chan struct{})
	var finishOnce sync.Once
	finish := func() { finishOnce.Do(func() { close(finishWrite) }) }
	defer finish()
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Run(func(context.Context, *querypb.CollectionLoadInfo, ...*querypb.PartitionLoadInfo) {
		close(writeStarted)
		<-finishWrite
	}).Return(nil).Once()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Once()
	writeResult := make(chan error, 1)
	go func() {
		writeResult <- s.qviewsRuntime.loadConfigStore.Put(context.Background(), s.qviewsRuntime.loadConfigStore.Get(100).Config)
	}()
	select {
	case <-writeStarted:
	case <-time.After(time.Second):
		t.Fatal("catalog write did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- s.waitCollectionReady(ctx, readinessRequest("by-dev-rootcoord-dml_100v0")) }()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("readiness cancellation waited on a blocked catalog write")
	}
	finish()
	require.NoError(t, <-writeResult)
}
