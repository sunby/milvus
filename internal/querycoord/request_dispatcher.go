// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querycoord

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/internal/proto/milvuspb"

	"github.com/milvus-io/milvus/internal/proto/commonpb"

	"github.com/milvus-io/milvus/internal/proto/querypb"
)

// requestDispatcher connects to QueryNodes automatically and sends request to QueryNodes.
type requestDispatcher struct {
	connsMu struct {
		sync.RWMutex
		conns map[string]types.QueryNode
	}
	connCreator func(ctx context.Context, addr string) (types.QueryNode, error)
}

func newRequestDispatcher(connCreator func(ctx context.Context, addr string) (types.QueryNode, error)) *requestDispatcher {
	return &requestDispatcher{
		connsMu: struct {
			sync.RWMutex
			conns map[string]types.QueryNode
		}{conns: make(map[string]types.QueryNode)},
		connCreator: connCreator,
	}
}

func (d *requestDispatcher) loadSegments(ctx context.Context, addr string, request *querypb.LoadSegmentsRequest) (resp *commonpb.Status, err error) {
	err = d.sendRequest(ctx, addr, func(c types.QueryNode) error {
		var err1 error
		resp, err1 = c.LoadSegments(ctx, request)
		return err1
	})
	return resp, err
}

func (d *requestDispatcher) releaseSegments(ctx context.Context, addr string, request *querypb.ReleaseSegmentsRequest) (resp *commonpb.Status, err error) {
	err = d.sendRequest(ctx, addr, func(c types.QueryNode) error {
		var err1 error
		resp, err1 = c.ReleaseSegments(ctx, request)
		return err1
	})
	return resp, err
}

func (d *requestDispatcher) watchDmChannels(ctx context.Context, addr string, request *querypb.WatchDmChannelsRequest) (resp *commonpb.Status, err error) {
	err = d.sendRequest(ctx, addr, func(c types.QueryNode) error {
		var err1 error
		resp, err1 = c.WatchDmChannels(ctx, request)
		return err1
	})
	return resp, err
}

func (d *requestDispatcher) watchDeltaChannels(ctx context.Context, addr string, request *querypb.WatchDeltaChannelsRequest) (resp *commonpb.Status, err error) {
	err = d.sendRequest(ctx, addr, func(c types.QueryNode) error {
		var err1 error
		resp, err1 = c.WatchDeltaChannels(ctx, request)
		return err1
	})
	return resp, err
}

func (d *requestDispatcher) releaseCollection(ctx context.Context, addr string, request *querypb.ReleaseCollectionRequest) (resp *commonpb.Status, err error) {
	err = d.sendRequest(ctx, addr, func(c types.QueryNode) error {
		var err1 error
		resp, err1 = c.ReleaseCollection(ctx, request)
		return err1
	})
	return resp, err
}

func (d *requestDispatcher) releasePartitions(ctx context.Context, addr string, request *querypb.ReleasePartitionsRequest) (resp *commonpb.Status, err error) {
	err = d.sendRequest(ctx, addr, func(c types.QueryNode) error {
		var err1 error
		resp, err1 = c.ReleasePartitions(ctx, request)
		return err1
	})
	return resp, err
}

func (d *requestDispatcher) addQueryChannel(ctx context.Context, addr string, request *querypb.AddQueryChannelRequest) (resp *commonpb.Status, err error) {
	err = d.sendRequest(ctx, addr, func(c types.QueryNode) error {
		var err1 error
		resp, err1 = c.AddQueryChannel(ctx, request)
		return err1
	})
	return resp, err
}

func (d *requestDispatcher) removeQueryChannel(ctx context.Context, addr string, request *querypb.RemoveQueryChannelRequest) (resp *commonpb.Status, err error) {
	err = d.sendRequest(ctx, addr, func(c types.QueryNode) error {
		var err1 error
		resp, err1 = c.RemoveQueryChannel(ctx, request)
		return err1
	})
	return resp, err
}

func (d *requestDispatcher) getSegmentInfo(ctx context.Context, addr string, request *querypb.GetSegmentInfoRequest) (resp *querypb.GetSegmentInfoResponse, err error) {
	err = d.sendRequest(ctx, addr, func(c types.QueryNode) error {
		var err1 error
		resp, err1 = c.GetSegmentInfo(ctx, request)
		return err1
	})
	return resp, err
}

func (d *requestDispatcher) getMetrics(ctx context.Context, addr string, request *milvuspb.GetMetricsRequest) (resp *milvuspb.GetMetricsResponse, err error) {
	err = d.sendRequest(ctx, addr, func(c types.QueryNode) error {
		var err1 error
		resp, err1 = c.GetMetrics(ctx, request)
		return err1
	})
	return resp, err
}

func (d *requestDispatcher) sendRequest(ctx context.Context, addr string, action func(c types.QueryNode) error) error {
	c, err := d.getOrCreateConn(ctx, addr)
	if err != nil {
		return err
	}
	return action(c)
}

func (d *requestDispatcher) getOrCreateConn(ctx context.Context, addr string) (types.QueryNode, error) {
	d.connsMu.Lock()
	defer d.connsMu.Unlock()
	c, ok := d.connsMu.conns[addr]
	if ok {
		return c, nil
	}
	c, err := d.connCreator(ctx, addr)
	if err != nil {
		return nil, err
	}
	d.connsMu.conns[addr] = c
	return c, nil
}
