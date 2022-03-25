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

package util

// Meta Prefix consts
const (
	SegmentMetaPrefix    = "queryCoord-segmentMeta"
	ChangeInfoMetaPrefix = "queryCoord-sealedSegmentChangeInfo"
)

const (
	// pulsar related
	RootCoordTimeTickChannelName      = "rootcoord-timetick"
	RootCoordStatisticsChannelName    = "rootcoord-statistics"
	RootCoordDmlChannelName           = "rootcoord-dml"
	RootCoordDeltaChannelName         = "rootcoord-delta"
	QueryCoordTimeTickChannelName     = "queryTimeTick"
	QueryCoordSearchChannelName       = "search"
	QueryCoordSearchResultChannelName = "searchResult"
	QueryNodeSubscribeName            = "queryNode"
	QueryNodeStatsChannelName         = "query-node-stats"
	DataCoordTimeTickChannelName      = "datacoord-timetick-channel"
	DataCoordSubscribeName            = "dataCoord"
	DataCoordSegmentInfoChannelName   = "segment-info-channel"
	DataNodeSubscribeName             = "dataNode"

	// etcd related
	EtcdMetaRootPath = "meta"
	EtcdKvRootPath   = "kv"

	// global
	DefaultPartitionName = "_default"
	DefaultIndexName     = "_default_idx"

	// Minio
	IndexStorageRootPath = "index_files"
	DeltaBinlogRootPath  = "delta_log"
	InsertBinlogRootPath = "insert_log"
	StatsBinlogRootPath  = "stats_log"
)

type pathType int

const (
	EtcdMeta pathType = iota
	EtcdKv
	RootCoordTimeTickChannel
	RootCoordStatsChannel
	RootCoordDmlChannel
	RootCoordDeltaChannel
	QueryCoordTimeTickChannel
	QueryCoordSearchChannel
	QueryCoordSearchResultChannel
	QueryNodeSubName
	QueryNodeStatsChannel
	IndexStorageRoot
	DataCoordTimeTickChannel
	DataCoordSubName
	DataCoordSegmentInfoChannel
	DataNodeSubName
	DeltaBinlogRoot
	InsertBinlogRoot
	StatsBinlogRoot
)

var pathMap = map[pathType]string{
	EtcdMeta:                      EtcdMetaRootPath,
	EtcdKv:                        EtcdKvRootPath,
	RootCoordTimeTickChannel:      RootCoordTimeTickChannelName,
	RootCoordStatsChannel:         RootCoordStatisticsChannelName,
	RootCoordDmlChannel:           RootCoordDmlChannelName,
	RootCoordDeltaChannel:         RootCoordDeltaChannelName,
	QueryCoordTimeTickChannel:     QueryCoordTimeTickChannelName,
	QueryCoordSearchChannel:       QueryCoordSearchChannelName,
	QueryCoordSearchResultChannel: QueryCoordSearchResultChannelName,
	QueryNodeSubName:              QueryNodeSubscribeName,
	QueryNodeStatsChannel:         QueryNodeStatsChannelName,
	IndexStorageRoot:              IndexStorageRootPath,
	DataCoordTimeTickChannel:      DataCoordTimeTickChannelName,
	DataCoordSubName:              DataCoordSubscribeName,
	DataCoordSegmentInfoChannel:   DataCoordSegmentInfoChannelName,
	DataNodeSubName:               DataCoordSubscribeName,
	DeltaBinlogRoot:               DeltaBinlogRootPath,
	InsertBinlogRoot:              InsertBinlogRootPath,
	StatsBinlogRoot:               StatsBinlogRootPath,
}
