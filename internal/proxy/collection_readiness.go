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

	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var _ queryclient.CollectionReadiness = (*Proxy)(nil)

func (node *Proxy) CheckCollectionReady(ctx context.Context, collectionID int64, expectedVChannels []string) error {
	return node.checkOrWaitCollectionReady(ctx, collectionID, expectedVChannels, true)
}

// WaitForCollectionReady sends one RPC. QueryCoord wakes it on state changes;
// the Proxy does not poll ShowLoadCollections while the collection loads.
func (node *Proxy) WaitForCollectionReady(ctx context.Context, collectionID int64, expectedVChannels []string) error {
	return node.checkOrWaitCollectionReady(ctx, collectionID, expectedVChannels, false)
}

func (node *Proxy) checkOrWaitCollectionReady(ctx context.Context, collectionID int64, expectedVChannels []string, checkOnly bool) error {
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	if len(expectedVChannels) == 0 {
		return merr.WrapErrCollectionNotLoaded(collectionID)
	}
	resp, err := node.mixCoord.WaitCollectionReady(ctx, &querypb.WaitCollectionReadyRequest{
		CollectionID:      collectionID,
		ExpectedVchannels: expectedVChannels,
		CheckOnly:         checkOnly,
	})
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return merr.CheckRPCCall(resp, err)
}
