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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type sharedReadinessClient struct {
	autoLoadViewQueryClient
	started  chan struct{}
	finish   chan struct{}
	canceled chan struct{}
	once     sync.Once
	waits    atomic.Int32
}

func (c *sharedReadinessClient) WaitForCollectionReady(ctx context.Context, _ int64, _ []string) error {
	c.waits.Add(1)
	c.once.Do(func() { close(c.started) })
	select {
	case <-c.finish:
		return nil
	case <-ctx.Done():
		select {
		case c.canceled <- struct{}{}:
		default:
		}
		return context.Cause(ctx)
	}
}

func TestEnsureCollectionReadySharesWaitingAndPreservesCallerCancellation(t *testing.T) {
	for _, initial := range []commonpb.LoadState{commonpb.LoadState_LoadStateNotLoad, commonpb.LoadState_LoadStateLoading} {
		t.Run(initial.String(), func(t *testing.T) {
			enableAutoLoad(t)
			client := &sharedReadinessClient{
				autoLoadViewQueryClient: autoLoadViewQueryClient{checkErr: merr.WrapErrCollectionNotLoaded(100)},
				started:                 make(chan struct{}), finish: make(chan struct{}), canceled: make(chan struct{}, 1),
			}
			var finishOnce sync.Once
			finish := func() { finishOnce.Do(func() { close(client.finish) }) }
			t.Cleanup(finish)
			nodeCtx, stop := context.WithCancel(context.Background())
			defer stop()
			node := &Proxy{ctx: nodeCtx, metaCache: mockSearchCollectionMeta(t, 100, []string{"v0", "v1"}), viewQueryClient: client}
			node.UpdateStateCode(commonpb.StateCode_Healthy)
			var loading atomic.Bool
			loading.Store(initial == commonpb.LoadState_LoadStateLoading)
			var checks, loads atomic.Int32
			stateMock := mockey.Mock((*Proxy).GetLoadState).To(func(*Proxy, context.Context, *milvuspb.GetLoadStateRequest) (*milvuspb.GetLoadStateResponse, error) {
				checks.Add(1)
				state := commonpb.LoadState_LoadStateNotLoad
				if loading.Load() {
					state = commonpb.LoadState_LoadStateLoading
				}
				return &milvuspb.GetLoadStateResponse{Status: merr.Success(), State: state}, nil
			}).Build()
			defer stateMock.UnPatch()
			privilegeMock := mockey.Mock(PrivilegeInterceptor).To(func(ctx context.Context, _ interface{}) (context.Context, error) { return ctx, nil }).Build()
			defer privilegeMock.UnPatch()
			loadMock := mockey.Mock((*Proxy).loadCollectionForDQL).To(func(*Proxy, context.Context, *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {
				loads.Add(1)
				loading.Store(true)
				return merr.Success(), nil
			}).Build()
			defer loadMock.UnPatch()
			leaderCtx, cancelLeader := context.WithCancel(context.Background())
			defer cancelLeader()
			leaderResult := make(chan error, 1)
			go func() { leaderResult <- node.ensureCollectionReady(leaderCtx, "db", "collection") }()
			select {
			case <-client.started:
			case <-time.After(3 * time.Second):
				t.Fatal("shared wait did not start")
			}
			const followers = 16
			results := make(chan error, followers)
			for i := 0; i < followers; i++ {
				go func() { results <- node.ensureCollectionReady(context.Background(), "db", "collection") }()
			}
			require.Eventually(t, func() bool { return checks.Load() >= followers+1 }, time.Second, time.Millisecond)
			cancelLeader()
			select {
			case err := <-leaderResult:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("leader could not cancel independently")
			}
			select {
			case <-client.canceled:
				t.Fatal("one caller canceled the shared RPC")
			case <-time.After(35 * time.Millisecond):
			}
			require.EqualValues(t, 1, client.waits.Load(), "Loading callers must join the existing wait")
			finish()
			for i := 0; i < followers; i++ {
				select {
				case err := <-results:
					require.NoError(t, err)
				case <-time.After(time.Second):
					t.Fatal("follower did not receive readiness")
				}
			}
			require.EqualValues(t, 1, client.waits.Load())
			expectedLoads := int32(0)
			if initial == commonpb.LoadState_LoadStateNotLoad {
				expectedLoads = 1
			}
			require.Equal(t, expectedLoads, loads.Load())
		})
	}
}
