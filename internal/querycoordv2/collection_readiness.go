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
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/readiness"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/stage"
)

// WaitCollectionReady observes the existing load; it never initiates a load.
func (s *Server) WaitCollectionReady(ctx context.Context, req *querypb.WaitCollectionReadyRequest) (*commonpb.Status, error) {
	return merr.Status(s.waitCollectionReady(ctx, req)), nil
}

var (
	collectionReadyWait  = stage.New("coord", "readiness", "wait")
	collectionReadyCheck = stage.New("coord", "readiness", "check")
)

func (s *Server) waitCollectionReady(ctx context.Context, req *querypb.WaitCollectionReadyRequest) (retErr error) {
	recorder := collectionReadyWait
	if req.GetCheckOnly() {
		recorder = collectionReadyCheck
	}
	ctx, timer := recorder.Start(ctx)
	defer func() {
		if req.GetCheckOnly() && errors.Is(retErr, merr.ErrCollectionNotLoaded) {
			timer.EndResult(stage.NotReady)
		} else {
			timer.End(retErr)
		}
	}()
	if err := merr.CheckHealthy(s.State()); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	collectionID := req.GetCollectionID()
	if len(req.GetExpectedVchannels()) == 0 {
		return merr.WrapErrCollectionNotLoaded(collectionID)
	}
	runtime := s.qviewsRuntime
	if runtime == nil || runtime.readyChanges == nil {
		return merr.WrapErrServiceNotReady("querycoord", 0, "query view readiness is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.LoadTimeoutSeconds.GetAsDuration(time.Second))
	defer cancel()

	var subscription *readiness.Subscription
	if !req.GetCheckOnly() {
		// Subscribe before checking: both an already-Up view and a change between
		// checking and blocking must be visible without a timer or a replay scan.
		subscription = runtime.readyChanges.Subscribe(collectionID)
		defer subscription.Close()
	}
	var cachedConfig *loadmgr.LoadConfig
	var shards []qviews.ShardID
	for {
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}
		select {
		case <-runtime.readyChanges.Done():
			return merr.WrapErrServiceUnavailableMsg("querycoord stopped while waiting for collection %d", collectionID)
		default:
		}
		var changed <-chan struct{}
		if subscription != nil {
			var released bool
			changed, released = subscription.Observe()
			if released {
				return merr.WrapErrCollectionNotLoaded(collectionID)
			}
		}
		entry := runtime.loadConfigStore.Get(collectionID)
		if entry.Config == nil {
			return merr.WrapErrCollectionNotLoaded(collectionID)
		}
		if cachedConfig != entry.Config {
			cachedConfig = entry.Config
			shards = make([]qviews.ShardID, 0, len(req.GetExpectedVchannels())*len(cachedConfig.Replicas))
			for _, vchannel := range req.GetExpectedVchannels() {
				for _, replica := range cachedConfig.Replicas {
					shards = append(shards, qviews.ShardID{VChannel: vchannel, ReplicaID: replica.ReplicaID})
				}
			}
		}
		ready := runtime.shardViewRegistry.AllShardsUp(shards)
		// Revalidate the immutable config after reading shard state. Do not take
		// the collection persistence guard: a slow catalog write must not make
		// this read-only RPC ignore cancellation. Release notifies under that
		// guard before any reload can commit, so a replaced load cannot satisfy
		// an existing subscription even when updates are coalesced.
		if latest := runtime.loadConfigStore.Get(collectionID); latest.ConfigVersion != entry.ConfigVersion {
			continue
		}
		if subscription != nil {
			if _, released := subscription.Observe(); released {
				return merr.WrapErrCollectionNotLoaded(collectionID)
			}
		}
		if ready {
			return nil
		}
		if req.GetCheckOnly() {
			return merr.WrapErrCollectionNotLoaded(collectionID)
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-runtime.readyChanges.Done():
			return merr.WrapErrServiceUnavailableMsg("querycoord stopped while waiting for collection %d", collectionID)
		case <-changed:
		}
	}
}

func newCollectionReadiness(loadConfigStore *loadmgr.LoadConfigStore, shardViewRegistry *coordview.ShardViewRegistry) *readiness.Notifications {
	readyChanges := readiness.NewNotifications()
	loadConfigStore.RegisterObserver(func(collectionID int64, released bool) {
		if released {
			readyChanges.Release(collectionID)
		} else {
			readyChanges.Notify(collectionID)
		}
	})
	shardViewRegistry.RegisterStatsObserver(func(shardID qviews.ShardID, _ *coordview.ShardStats) {
		channel, err := metautil.ParseChannel(shardID.VChannel, metautil.NewDynChannelMapper())
		if err == nil {
			readyChanges.Notify(channel.CollectionID())
		}
	})
	return readyChanges
}
