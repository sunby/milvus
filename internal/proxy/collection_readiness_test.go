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

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCollectionReadinessUsesOneRPC(t *testing.T) {
	for _, checkOnly := range []bool{true, false} {
		for _, want := range []error{nil, merr.ErrCollectionNotLoaded, merr.ErrServiceUnavailable} {
			calls := 0
			node := &Proxy{mixCoord: &MixCoordMock{
				WaitCollectionReadyFunc: func(_ context.Context, req *querypb.WaitCollectionReadyRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
					calls++
					require.EqualValues(t, 100, req.GetCollectionID())
					require.Equal(t, []string{"v0", "v1"}, req.GetExpectedVchannels())
					require.Equal(t, checkOnly, req.GetCheckOnly())
					return merr.Status(want), nil
				},
				ShowLoadCollectionsFunc: func(context.Context, *querypb.ShowCollectionsRequest, ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error) {
					t.Fatal("readiness must not poll ShowLoadCollections")
					return nil, nil
				},
			}}
			var err error
			if checkOnly {
				err = node.CheckCollectionReady(context.Background(), 100, []string{"v0", "v1"})
			} else {
				err = node.WaitForCollectionReady(context.Background(), 100, []string{"v0", "v1"})
			}
			require.ErrorIs(t, err, want)
			require.Equal(t, 1, calls)
		}
	}
}

func TestCollectionReadinessCancellationDoesNotPoll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	var calls atomic.Int32
	node := &Proxy{mixCoord: &MixCoordMock{
		WaitCollectionReadyFunc: func(ctx context.Context, _ *querypb.WaitCollectionReadyRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
			calls.Add(1)
			close(started)
			<-ctx.Done()
			return nil, grpcstatus.FromContextError(ctx.Err()).Err()
		},
	}}
	result := make(chan error, 1)
	go func() { result <- node.WaitForCollectionReady(ctx, 100, []string{"v0"}) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("RPC did not start")
	}
	select {
	case err := <-result:
		t.Fatalf("wait returned before cancellation: %v", err)
	case <-time.After(35 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("RPC did not stop on cancellation")
	}
	require.EqualValues(t, 1, calls.Load())
}

func TestCollectionReadinessRejectsEmptyTopologyAndCanceledContext(t *testing.T) {
	node := &Proxy{}
	require.ErrorIs(t, node.CheckCollectionReady(context.Background(), 100, nil), merr.ErrCollectionNotLoaded)
	require.ErrorIs(t, node.WaitForCollectionReady(context.Background(), 100, nil), merr.ErrCollectionNotLoaded)
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(context.DeadlineExceeded)
	require.ErrorIs(t, node.WaitForCollectionReady(ctx, 100, []string{"v0"}), context.DeadlineExceeded)
}
