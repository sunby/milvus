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
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var _ queryclient.CollectionReadiness = (*Proxy)(nil)

// CheckCollectionReady checks QueryCoord's query-service readiness. Assignment
// discovery no longer carries collection shard views, and the metadata cache's
// vchannels describe topology regardless of whether the collection is loaded.
func (node *Proxy) CheckCollectionReady(ctx context.Context, collectionID int64, expectedVChannels []string) error {
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	if len(expectedVChannels) == 0 {
		return merr.WrapErrCollectionNotLoaded(collectionID)
	}
	resp, err := node.mixCoord.ShowLoadCollections(ctx, &querypb.ShowCollectionsRequest{
		CollectionIDs: []int64{collectionID},
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return err
	}
	if len(resp.GetCollectionIDs()) != len(resp.GetQueryServiceAvailable()) {
		return merr.WrapErrServiceInternalMsg("query service readiness does not match collection IDs")
	}
	for i, id := range resp.GetCollectionIDs() {
		if id == collectionID && resp.GetQueryServiceAvailable()[i] {
			return nil
		}
	}
	return merr.WrapErrCollectionNotLoaded(collectionID)
}

// WaitForCollectionReady waits for QueryCoord to report available query views.
func (node *Proxy) WaitForCollectionReady(ctx context.Context, collectionID int64, expectedVChannels []string) error {
	if len(expectedVChannels) == 0 {
		return merr.WrapErrCollectionNotLoaded(collectionID)
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := node.CheckCollectionReady(ctx, collectionID, expectedVChannels)
		if !errors.Is(err, merr.ErrCollectionNotLoaded) {
			return err
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
		}
	}
}
