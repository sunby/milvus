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
	"time"

	"github.com/milvus-io/milvus/configs"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util"
	"go.uber.org/zap"
)

type statsService struct {
	ctx context.Context
	cfg *configs.Config

	replica ReplicaInterface

	statsStream msgstream.MsgStream
	msFactory   msgstream.Factory
}

func newStatsService(ctx context.Context, cfg *configs.Config, replica ReplicaInterface, factory msgstream.Factory) *statsService {

	return &statsService{
		ctx:         ctx,
		cfg:         cfg,
		replica:     replica,
		statsStream: nil,
		msFactory:   factory,
	}
}

func (sService *statsService) start() {
	sleepTimeInterval := sService.cfg.QueryNode.StatsPublishInterval

	// start pulsar
	producerChannels := []string{util.GetPath(sService.cfg, util.QueryNodeStatsChannel)}

	statsStream, _ := sService.msFactory.NewMsgStream(sService.ctx)
	statsStream.AsProducer(producerChannels)
	log.Debug("QueryNode statsService AsProducer succeed", zap.Strings("channels", producerChannels))

	var statsMsgStream msgstream.MsgStream = statsStream

	sService.statsStream = statsMsgStream
	sService.statsStream.Start()

	// start service
	for {
		select {
		case <-sService.ctx.Done():
			log.Debug("stats service closed")
			return
		case <-time.After(time.Duration(sleepTimeInterval) * time.Millisecond):
			sService.publicStatistic(nil)
		}
	}
}

func (sService *statsService) close() {
	if sService.statsStream != nil {
		sService.statsStream.Close()
	}
}

func (sService *statsService) publicStatistic(fieldStats []*internalpb.FieldStats) {
	segStats := sService.replica.getSegmentStatistics()

	queryNodeStats := internalpb.QueryNodeStats{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_QueryNodeStats,
			SourceID: queryNodeID,
		},
		SegStats:   segStats,
		FieldStats: fieldStats,
	}

	var msg msgstream.TsMsg = &msgstream.QueryNodeStatsMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{0},
		},
		QueryNodeStats: queryNodeStats,
	}

	var msgPack = msgstream.MsgPack{
		Msgs: []msgstream.TsMsg{msg},
	}
	err := sService.statsStream.Produce(&msgPack)
	if err != nil {
		log.Error(err.Error())
	}
}
