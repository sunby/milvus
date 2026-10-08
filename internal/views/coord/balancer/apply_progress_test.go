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

package balancer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type blockingCollectionPin struct{ entered, release chan struct{} }

func (r *blockingCollectionPin) PinDataView(ctx context.Context, id int64, _ qviews.DataVersion) error {
	if id == 100 {
		close(r.entered)
		select {
		case <-r.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (*blockingCollectionPin) RecoverDataViewReference(context.Context, int64, qviews.DataVersion) (bool, error) {
	return true, nil
}
func (*blockingCollectionPin) UnpinDataView(int64, qviews.DataVersion) {}

type progressSyncer struct{ views chan syncer.SyncView }

func (s *progressSyncer) SyncViews(ctx context.Context, group syncer.SyncGroup) error {
	for _, views := range group.ViewsByNode {
		for _, view := range views {
			select {
			case s.views <- view:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}
func (*progressSyncer) Close() error { return nil }

// An unrelated collection's Ready callback must progress while apply is still
// blocked pinning a DataView. Restoring the old global Begin/Commit fails here.
func TestApplyDoesNotHoldUnrelatedReadyView(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	refs := &blockingCollectionPin{entered: make(chan struct{}), release: make(chan struct{})}
	var unblock sync.Once
	defer unblock.Do(func() { close(refs.release) })
	stream := &progressSyncer{views: make(chan syncer.SyncView, 32)}
	registry, err := coordview.RecoverShardViewRegistry(ctx, &stubCatalog{}, stream, refs)
	require.NoError(t, err)
	defer registry.Close()
	build := func(id int64, vchannel string) (qviews.ShardID, *qviews.QueryViewAtCoordBuilder) {
		sid := qviews.ShardID{ReplicaID: id, VChannel: vchannel}
		dv := &viewpb.DataViewOfCollection{CollectionId: id, Shards: []*viewpb.DataViewOfShard{{Vchannel: vchannel}}, DataVersion: &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1}}
		b := qviews.NewQueryViewAtCoordBuilder(id, dv, vchannel)
		b.SetAssignments(map[int64]map[int64][]int64{10: {20: {30}}})
		return sid, b
	}
	a, ab := build(100, "by-dev-rootcoord-dml_0_100v0")
	b, bb := build(200, "by-dev-rootcoord-dml_0_200v0")
	require.NoError(t, registry.Ensure(b).AddPreparing(ctx, bb))
	preparing := make([]syncer.SyncView, 0, 2)
	for len(preparing) < 2 {
		select {
		case v := <-stream.views:
			preparing = append(preparing, v)
		case <-ctx.Done():
			t.Fatal("B Preparing was not persisted and queued")
		}
	}
	balancer := &DefaultBalancer{viewRegistry: registry}
	done := make(chan error, 1)
	go func() {
		done <- balancer.apply(ctx, &BalancePlan{Prepares: map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder{a: ab}})
	}()
	select {
	case <-refs.entered:
	case <-ctx.Done():
		t.Fatal("A did not enter PinDataView")
	}
	for _, v := range preparing {
		pb := v.View.IntoProto()
		pb.Meta.State = viewpb.QueryViewState(qviews.QueryViewStateReady)
		for _, qn := range pb.QueryNode {
			for _, part := range qn.Partitions {
				part.ReadySegmentIds = part.SegmentIds
			}
		}
		v.OnSyncResponse(qviews.NewQueryViewAtWorkNodeFromProto(pb))
	}
	for {
		select {
		case v := <-stream.views:
			if v.View.ShardID() != b || v.View.State() != qviews.QueryViewStateUp {
				continue
			}
			v.OnSyncResponse(v.View)
			require.True(t, registry.AllShardsUp([]qviews.ShardID{b}))
			unblock.Do(func() { close(refs.release) })
			require.NoError(t, <-done)
			return
		case <-ctx.Done():
			t.Fatal("B Ready->Up was held by unrelated A apply")
		}
	}
}
