package config_test

import (
	"strings"
	"testing"

	"github.com/milvus-io/milvus/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestParseFromFile(t *testing.T) {
	reader := strings.NewReader(`
    defaultPartitionName: "_default" 
    defaultIndexName: "_default_idx"
    etcd:
      endpoints:
        - localhost:2379
      rootPath: by-dev
      metaSubPath: meta 
      kvSubPath: kv 
      segmentBinlogSubPath: datacoord/binlog/segment  
      collectionBinlogSubPath: datacoord/binlog/collection 
      flushStreamPosSubPath: datacoord/flushstream 
      statsStreamPosSubPath: datacoord/statsstream 
    minio:
      address: localhost
      port: 9000
      accessKeyID: minioadmin
      secretAccessKey: minioadmin
      useSSL: false
      bucketName: "a-bucket"
    pulsar:
      address: localhost
      port: 6650
      maxMessageSize: 5242880 
    rocksmq:
      path: /var/lib/milvus/rdb_data
    rootCoord:
      address: localhost
      port: 53100
      maxPartitionNum: 4096 
      minSegmentSizeToEnableIndex: 1024
      timeout: 3600 
      timeTickInterval: 200 
    proxy:
      port: 19530
      timeTickInterval: 200 
      msgStream:
        insertBufSize: 1024 
        searchBufSize: 512
        searchResultRecvBufSize: 1024
        searchResultPulsarBufSize: 1024
        timeTickBufSize: 512
      maxNameLength: 255
      maxFieldNum: 64
      maxDimension: 32768
    queryCoord:
      address: localhost
      port: 19531
    queryNode:
      gracefulTime: 1000 
      port: 21123
      statsPublishInterval: 1000 
      flowGraph:
        maxQueueLength: 1024
        maxParallelism: 1024
      msgStream:
        searchRecvBufSize: 512
        searchPulsarBufSize: 512
        searchResultRecvBufSize: 64
    indexCoord:
      address: localhost
      port: 31000
    indexNode:
      port: 21121
    dataCoord:
      address: localhost
      port: 13333
      segment: 
        maxSize: 512 
        sealProportion: 0.75
        assignmentExpiration: 2000 
    dataNode:
      port: 21124
      flowGraph: #old
        maxQueueLength: 1024
        maxParallelism: 1024
      flushBufSize: 32000
    log:
      level: debug 
      file:
        rootPath: "" 
        maxSize: 300 
        maxAge: 10 
        maxBackups: 20
      format: text 
    msgChannel:
        rootCoordTimeTick: "rootcoord-timetick"
        rootCoordStatistics: "rootcoord-statistics"
        search: "search"
        searchResult: "searchResult"
        proxyTimeTick: "proxyTimeTick"
        queryTimeTick: "queryTimeTick"
        queryNodeStats: "query-node-stats"
        # cmd for loadIndex, flush, etc...
        cmd: "cmd"
        dataCoordInsertChannel: "insert-channel-"
        dataCoordStatistic: "datacoord-statistics-channel"
        dataCoordTimeTick: "datacoord-timetick-channel"
        dataCoordSegmentInfo: "segment-info-channel"
        rootCoordSubNamePrefix: "rootcoord"
        proxySubNamePrefix: "proxy"
        queryNodeSubNamePrefix: "queryNode"
        dataNodeSubNamePrefix: "dataNode"
        dataCoordSubNamePrefix: "dataCoord"
    `)
	conf := config.Config{}
	err := conf.ParseFromFile(reader)
	assert.Nil(t, err)
	err = conf.Valid()
	assert.Nil(t, err)
}
