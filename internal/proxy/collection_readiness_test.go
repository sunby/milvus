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
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCheckCollectionReadyUsesQueryCoordReadiness(t *testing.T) {
	tests := []struct {
		name string
		resp *querypb.ShowCollectionsResponse
		err  error
		want error
	}{
		{
			name: "loaded",
			resp: &querypb.ShowCollectionsResponse{
				Status: merr.Success(), CollectionIDs: []int64{100}, QueryServiceAvailable: []bool{true},
			},
		},
		{
			name: "views not yet available",
			resp: &querypb.ShowCollectionsResponse{
				Status: merr.Success(), CollectionIDs: []int64{100}, QueryServiceAvailable: []bool{false},
			},
			want: merr.ErrCollectionNotLoaded,
		},
		{
			name: "another collection cannot satisfy readiness",
			resp: &querypb.ShowCollectionsResponse{
				Status: merr.Success(), CollectionIDs: []int64{101}, QueryServiceAvailable: []bool{true},
			},
			want: merr.ErrCollectionNotLoaded,
		},
		{
			name: "released",
			resp: &querypb.ShowCollectionsResponse{Status: merr.Status(merr.WrapErrCollectionNotLoaded(100))},
			want: merr.ErrCollectionNotLoaded,
		},
		{
			name: "querycoord error is preserved",
			resp: &querypb.ShowCollectionsResponse{Status: merr.Status(merr.WrapErrServiceUnavailableMsg("recovering"))},
			want: merr.ErrServiceUnavailable,
		},
		{
			name: "transport error is preserved",
			err:  context.DeadlineExceeded,
			want: context.DeadlineExceeded,
		},
		{
			name: "malformed readiness response",
			resp: &querypb.ShowCollectionsResponse{Status: merr.Success(), CollectionIDs: []int64{100}},
			want: merr.ErrServiceInternal,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node := &Proxy{mixCoord: &MixCoordMock{
				ShowLoadCollectionsFunc: func(_ context.Context, req *querypb.ShowCollectionsRequest, _ ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error) {
					require.Equal(t, []int64{100}, req.GetCollectionIDs())
					return test.resp, test.err
				},
			}}
			err := node.CheckCollectionReady(context.Background(), 100, []string{"v0", "v1"})
			if test.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, test.want)
			}
		})
	}
}

func TestWaitForCollectionReadyUsesQueryCoord(t *testing.T) {
	t.Run("waits until query service is available", func(t *testing.T) {
		calls := 0
		node := &Proxy{mixCoord: &MixCoordMock{
			ShowLoadCollectionsFunc: func(context.Context, *querypb.ShowCollectionsRequest, ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error) {
				calls++
				return &querypb.ShowCollectionsResponse{
					Status: merr.Success(), CollectionIDs: []int64{100}, QueryServiceAvailable: []bool{calls >= 3},
				}, nil
			},
		}}
		require.NoError(t, node.WaitForCollectionReady(context.Background(), 100, []string{"v0"}))
		require.Equal(t, 3, calls)
	})

	t.Run("cancellation stops waiting", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		calls := 0
		node := &Proxy{mixCoord: &MixCoordMock{
			ShowLoadCollectionsFunc: func(context.Context, *querypb.ShowCollectionsRequest, ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error) {
				calls++
				cancel(context.DeadlineExceeded)
				return &querypb.ShowCollectionsResponse{Status: merr.Status(merr.WrapErrCollectionNotLoaded(100))}, nil
			},
		}}
		require.ErrorIs(t, node.WaitForCollectionReady(ctx, 100, []string{"v0"}), context.DeadlineExceeded)
		require.Equal(t, 1, calls)
	})

	t.Run("other errors do not loop", func(t *testing.T) {
		calls := 0
		node := &Proxy{mixCoord: &MixCoordMock{
			ShowLoadCollectionsFunc: func(context.Context, *querypb.ShowCollectionsRequest, ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error) {
				calls++
				return &querypb.ShowCollectionsResponse{Status: merr.Status(merr.WrapErrServiceInternalMsg("failed"))}, nil
			},
		}}
		require.ErrorIs(t, node.WaitForCollectionReady(context.Background(), 100, []string{"v0"}), merr.ErrServiceInternal)
		require.Equal(t, 1, calls)
	})

	t.Run("empty topology does not wait", func(t *testing.T) {
		node := &Proxy{}
		require.ErrorIs(t, node.CheckCollectionReady(context.Background(), 100, nil), merr.ErrCollectionNotLoaded)
		require.ErrorIs(t, node.WaitForCollectionReady(context.Background(), 100, nil), merr.ErrCollectionNotLoaded)
	})
}
