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

package querynode

import (
	"context"
	"errors"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/retry"
	"go.etcd.io/etcd/api/v3/mvccpb"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type addrResolver func() (map[int64]string, error)

// etcdShardNodeDetector watches etcd prefix for node event.
type etcdShardNodeDetector struct {
	client *clientv3.Client
	path   string
	idAddr addrResolver
	evtCh  chan nodeEvent

	wg        sync.WaitGroup
	closeCh   chan struct{}
	closeOnce sync.Once
}

// NewEtcdShardNodeDetector returns a etcdShardNodeDetector with provided etcd client and prefix path.
func NewEtcdShardNodeDetector(client *clientv3.Client, rootPath string, resolver addrResolver) *etcdShardNodeDetector {
	return &etcdShardNodeDetector{
		client: client,
		path:   rootPath,
		idAddr: resolver,

		closeCh: make(chan struct{}),
		evtCh:   make(chan nodeEvent, 32),
	}
}

// Close closes all the workers by closing the signal channel.
func (nd *etcdShardNodeDetector) Close() {
	nd.closeOnce.Do(func() {
		close(nd.closeCh)
		nd.wg.Wait()
		close(nd.evtCh)
	})
}

// watchNodes lists current online nodes and returns a channel for incoming events.
func (nd *etcdShardNodeDetector) watchNodes(collectionID int64, replicaID int64, vchannelName string) ([]nodeEvent, <-chan nodeEvent) {
	log.Info("nodeDetector watch", zap.Int64("collectionID", collectionID), zap.Int64("replicaID", replicaID), zap.String("vchannelName", vchannelName))
	resp, err := nd.client.Get(context.Background(), nd.path, clientv3.WithPrefix())
	if err != nil {
		log.Warn("Etcd NodeDetector get replica info failed", zap.Error(err))
		panic(err)
	}

	idAddr, err := nd.idAddr()
	if err != nil {
		log.Error("Etcd NodeDetector session map failed", zap.Error(err))
		panic(err)
	}

	var nodes []nodeEvent
	for _, kv := range resp.Kvs {
		info, err := nd.parseReplicaInfo(kv.Value)
		if err != nil {
			log.Warn("Etcd NodeDetector kv parse failed", zap.String("key", string(kv.Key)), zap.Error(err))
			continue
		}
		// skip replica not related
		if info.GetCollectionID() != collectionID || info.GetID() != replicaID {
			continue
		}

		// generate node event
		for _, nodeID := range info.GetNodes() {
			addr, has := idAddr[nodeID]
			if !has {
				log.Warn("Node not found in session", zap.Int64("node id", nodeID))
				continue
			}
			nodes = append(nodes, nodeEvent{
				nodeID:    nodeID,
				nodeAddr:  addr,
				eventType: nodeAdd,
				isLeader:  nodeID == Params.QueryNodeCfg.GetNodeID(),
			})
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	go nd.cancelClose(cancel)
	revision := resp.Header.GetRevision() + 1
	watchCh := nd.client.Watch(ctx, nd.path, clientv3.WithRev(revision), clientv3.WithPrefix(), clientv3.WithPrevKV())

	nd.wg.Add(1)
	go nd.watch(watchCh, collectionID, replicaID, revision)

	return nodes, nd.evtCh
}

func (nd *etcdShardNodeDetector) cancelClose(cancel func()) {
	<-nd.closeCh
	cancel()
}

func (nd *etcdShardNodeDetector) watch(ch clientv3.WatchChan, collectionID, replicaID, revision int64) {
	defer nd.wg.Done()
	for {
		select {
		case <-nd.closeCh:
			log.Warn("Closed NodeDetector watch loop quit", zap.Int64("collectionID", collectionID), zap.Int64("replicaID", replicaID))
			return
		case evt, ok := <-ch:
			if !ok {
				log.Warn("watch channel closed, retry...")
				var watchCh clientv3.WatchChan
				var ok bool
				watchCh, ok, revision = nd.rewatch(collectionID, replicaID, revision)
				if !ok {
					// detector closed
					return
				}
				nd.wg.Add(1)
				go nd.watch(watchCh, collectionID, replicaID, revision)
				return
			}
			if err := evt.Err(); err != nil {
				if err == v3rpc.ErrCompacted {
					watchCh, ok, revision := nd.rewatch(collectionID, replicaID, evt.CompactRevision)
					if !ok {
						// detector closed
						return
					}
					nd.wg.Add(1)
					go nd.watch(watchCh, collectionID, replicaID, revision)
					return
				}
				log.Error("failed to handle watch node error", zap.Error(err))
				panic(err)
			}
			revision = evt.Header.GetRevision() + 1
			nd.handleEvt(evt, collectionID, replicaID)
		}
	}
}

func (nd *etcdShardNodeDetector) handleEvt(evt clientv3.WatchResponse, collectionID, replicaID int64) {
	for _, e := range evt.Events {
		switch e.Type {
		case mvccpb.PUT:
			nd.handlePutEvent(e, collectionID, replicaID)
		case mvccpb.DELETE:
			nd.handleDelEvent(e, collectionID, replicaID)
		}
	}
}

func (nd *etcdShardNodeDetector) rewatch(collectionID, replicaID, rev int64) (ch clientv3.WatchChan, ok bool, revision int64) {
	ctx := context.Background()
	revision = rev
	err := retry.Do(ctx, func() error {
		ch = nd.client.Watch(ctx, nd.path, clientv3.WithPrefix(), clientv3.WithRev(revision))
		select {
		case <-nd.closeCh:
			return retry.Unrecoverable(errors.New("detector closed"))

		case evt, ok := <-ch:
			if !ok {
				return errors.New("rewatch got closed ch")
			}
			if err := evt.Err(); err != nil {
				if err == v3rpc.ErrCompacted {
					revision = evt.CompactRevision
					return err
				}
				log.Error("failed to handle watch node error", zap.Error(err))
				panic(err)
			}
			revision = evt.Header.GetRevision() + 1
			nd.handleEvt(evt, collectionID, replicaID)
		default:
			// blocked, fine
		}
		return nil
	})
	// check detector closed
	if err != nil {
		select {
		case <-nd.closeCh:
			return nil, false, revision
		default:
			panic(err)
		}
	}

	return ch, true, revision
}

func (nd *etcdShardNodeDetector) handlePutEvent(e *clientv3.Event, collectionID, replicaID int64) {
	var err error
	var info, prevInfo *querypb.Replica
	info, err = nd.parseReplicaInfo(e.Kv.Value)
	if err != nil {
		log.Warn("failed to handle node event", zap.Any("event", e), zap.Error(err))
		return
	}
	if info.CollectionID != collectionID || info.GetID() != replicaID {
		return
	}
	if e.PrevKv != nil {
		prevInfo, _ = nd.parseReplicaInfo(e.PrevKv.Value)
	}

	idAddr, err := nd.idAddr()
	if err != nil {
		if errors.Is(err, context.Canceled) {
			// session canceled, query node is stopping.
			log.Warn("EtcdNodeDetector id resolve failed", zap.Error(err))
			return
		}
		log.Error("Etcd NodeDetector session map failed", zap.Error(err))
		panic(err)
	}
	// all node is added
	if prevInfo == nil {
		for _, nodeID := range info.GetNodes() {
			addr, ok := idAddr[nodeID]
			if !ok {
				log.Warn("node id not in session", zap.Int64("nodeID", nodeID))
				continue
			}
			nd.evtCh <- nodeEvent{
				nodeID:    nodeID,
				nodeAddr:  addr,
				eventType: nodeAdd,
			}
		}
		return
	}

	// maybe binary search is better here
	currIDs := make(map[int64]struct{})
	for _, id := range info.GetNodes() {
		currIDs[id] = struct{}{}
	}

	oldIDs := make(map[int64]struct{})
	for _, id := range prevInfo.GetNodes() {
		oldIDs[id] = struct{}{}
	}

	log.Info("hanldePutEvent infos", zap.Any("oldIds", oldIDs), zap.Any("currIds", currIDs), zap.Any("collection", collectionID), zap.Any("replica", replicaID))
	for id := range currIDs {
		_, has := oldIDs[id]
		if !has {
			addr, ok := idAddr[id]
			if !ok {
				log.Warn("node id not in session", zap.Int64("nodeID", id))
				continue
			}
			log.Info("event generated", zap.Int64("id", id))
			nd.evtCh <- nodeEvent{
				nodeID:    id,
				nodeAddr:  addr,
				eventType: nodeAdd,
			}
		}
	}

	for id := range oldIDs {
		_, has := currIDs[id]
		if !has {
			addr := idAddr[id]
			// best effort to notify node del
			log.Info("generate node event pos1", zap.Any("collection", collectionID), zap.Any("replica", replicaID), zap.Any("nodeID", id))
			nd.evtCh <- nodeEvent{
				nodeID:    id,
				nodeAddr:  addr,
				eventType: nodeDel,
			}
		}
	}
}

func (nd *etcdShardNodeDetector) handleDelEvent(e *clientv3.Event, collectionID, replicaID int64) {
	if e.PrevKv == nil {
		return
	}
	prevInfo, err := nd.parseReplicaInfo(e.PrevKv.Value)
	if err != nil {
		return
	}
	// skip replica not related
	if prevInfo.GetCollectionID() != collectionID || prevInfo.GetID() != replicaID {
		return
	}
	idAddr, err := nd.idAddr()
	if err != nil {
		if errors.Is(err, context.Canceled) {
			// session canceled, query node is stopping.
			log.Warn("EtcdNodeDetector id resolve failed", zap.Error(err))
			return
		}
		log.Error("Etcd NodeDetector session map failed", zap.Error(err))
		panic(err)
	}
	for _, id := range prevInfo.GetNodes() {
		//best effort to notify offline
		addr := idAddr[id]
		log.Info("generate node event pos2", zap.Any("collection", collectionID), zap.Any("replica", replicaID), zap.Any("nodeID", id))
		nd.evtCh <- nodeEvent{
			nodeID:    id,
			nodeAddr:  addr,
			eventType: nodeDel,
		}
	}
}

func (nd *etcdShardNodeDetector) parseReplicaInfo(bs []byte) (*querypb.Replica, error) {
	info := &querypb.Replica{}
	err := proto.Unmarshal(bs, info)
	return info, err
}
