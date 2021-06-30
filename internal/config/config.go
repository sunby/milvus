package config

// Config contains all options for milvus
type Config struct {
	DefaultPartitionName string           `yaml:"defaultPartitionName"`
	DefaultIndexName     string           `yaml:"defaultIndexName"`
	Etcd                 EtcdConfig       `yaml:"etcd"`
	Minio                MinioConfig      `yaml:"minio"`
	Pulsar               PulsarConfig     `yaml:"pulsar"`
	Rocksmq              RocksmqConfig    `yaml:"rocksmq"`
	RootCoord            RootCoordConfig  `yaml:"rootCoord"`
	Proxy                ProxyConfig      `yaml:"proxy"`
	QueryCoord           QueryCoordConfig `yaml:"queryCoord"`
	QueryNode            QueryNodeConfig  `yaml:"queryNode"`
	IndexCoord           IndexCoordConfig `yaml:"indexCoord"`
	IndexNode            IndexNodeConfig  `yaml:"indexNode"`
	DataCoord            DataCoordConfig  `yaml:"dataCoord"`
	DataNode             DataNodeConfig   `yaml:"dataNode"`
	Log                  LogConfig        `yaml:"log"`
	MsgChannel           MsgChannelConfig `yaml:"msgChannel"`
}

var defaultConfig = Config{
	DefaultPartitionName: "_default",
	DefaultIndexName:     "_default_idx",
	Etcd: EtcdConfig{
		EndPoints:               []string{"localhost:2379"},
		RootPath:                "by-dev",
		MetaSubpath:             "meta",
		KvSubpath:               "kv",
		SegmentBinlogSubpath:    "datacoord/binlog/segment",
		CollectionBinlogSubpath: "datacoord/binlog/collection",
		FlushStreamPosSubpath:   "datacoord/flushstream",
		StatsStreamPosSubpath:   "datacoord/statsstream",
	},
	Minio: MinioConfig{
		Address:         "localhost",
		Port:            9000,
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
		UseSsl:          false,
		BucketName:      "a-bucket",
	},
	Pulsar: PulsarConfig{
		Address:        "localhost",
		Port:           6650,
		MaxMessageSize: 5242880,
	},
	Rocksmq: RocksmqConfig{
		Path: "/var/lib/milvus/rdb_data",
	},
	RootCoord: RootCoordConfig{
		Address:                     "localhost",
		Port:                        53100,
		MaxPartitionNum:             4096,
		MinSegmentSizeToEnableIndex: 1024,
		Timeout:                     3600,
		TimeTickInterval:            200,
	},
	Proxy: ProxyConfig{
		Port:             19530,
		TimeTickInterval: 200,
		MaxNameLength:    255,
		MaxFieldNum:      64,
		MaxDimension:     32768,
		MsgStream: ProxyMsgStream{
			InsertBufSize:             1024,
			SearchBufSize:             512,
			SearchResultRecvBufSize:   1024,
			SearchResultPulsarBufSize: 1024,
			TimeTickBufSize:           512,
		},
	},
	QueryCoord: QueryCoordConfig{
		Address: "localhost",
		Port:    19531,
	},
	QueryNode: QueryNodeConfig{
		Port:                 21123,
		GracefulTime:         1000,
		StatsPublishInterval: 1000,
		FlowGraph: QueryNodeFlowGraph{
			MaxQueueLength: 1024,
			MaxParallelism: 1024,
		},
		MsgStream: QueryNodeMsgStream{
			SearchRecvBufSize:       512,
			SearchPulsarBufSize:     512,
			SearchResultRecvBufSize: 64,
		},
	},
	IndexCoord: IndexCoordConfig{
		Address: "localhost",
		Port:    31000,
	},
	IndexNode: IndexNodeConfig{
		Port: 21121,
	},
	DataCoord: DataCoordConfig{
		Address: "localhost",
		Port:    13333,
		Segment: SegmentConfig{
			MaxSize:              512,
			SealProportion:       0.75,
			AssignmentExpiration: 2000,
		},
	},
	DataNode: DataNodeConfig{
		Port:         21124,
		FlushBufSize: 32000,
		FlowGraph: DataNodeFlowGraph{
			MaxQueueLength: 1024,
			MaxParallelism: 1024,
		},
	},
	Log: LogConfig{
		Level:  "debug",
		Format: "text",
		File: LogFileConfig{
			RootPath:   "",
			MaxSize:    300,
			MaxAge:     10,
			MaxBackups: 20,
		},
	},
	MsgChannel: MsgChannelConfig{
		RootCoordTimeTick:      "rootcoord-timetick",
		RootCoordStatistics:    "rootcoord-statistics",
		Search:                 "search",
		SearchResult:           "searchResult",
		ProxyTimeTick:          "proxyTimeTick",
		QueryTimeTick:          "queryTimeTick",
		QueryNodeStats:         "query-node-stats",
		Cmd:                    "cmd",
		DataCoordInsertChannel: "insert-channel-",
		DataCoordStatistic:     "datacoord-statistics-channel",
		DataCoordTimeTick:      "datacoord-timetick-channel",
		DataCoordSegmentInfo:   "segment-info-channel",
		RootCoordSubnamePrefix: "rootcoord",
		ProxySubnamePrefix:     "proxy",
		QueryNodeSubnamePrefix: "queryNode",
		DataNodeSubnamePrefix:  "dataNode",
		DataCoordSubnamePrefix: "dataCoord",
	},
}

// EtcdConfig is the config for etcd
type EtcdConfig struct {
	EndPoints               []string `yaml:"endpoints"`
	RootPath                string   `yaml:"rootPath"`
	MetaSubpath             string   `yaml:"metaSubPath"`
	KvSubpath               string   `yaml:"kvSubPath"`
	SegmentBinlogSubpath    string   `yaml:"segmentBinlogSubPath"`
	CollectionBinlogSubpath string   `yaml:"collectionBinlogSubPath"`
	FlushStreamPosSubpath   string   `yaml:"flushStreamPosSubPath"`
	StatsStreamPosSubpath   string   `yaml:"statsStreamPosSubPath"`
}

// MinioConfig is the config for minio
type MinioConfig struct {
	Address         string `yaml:"address"`
	Port            int    `yaml:"port"`
	AccessKeyID     string `yaml:"accessKeyID"`
	SecretAccessKey string `yaml:"secretAccessKey"`
	UseSsl          bool   `yaml:"useSSL"`
	BucketName      string `yaml:"bucketName"`
}

// PulsarConfig is the config for pulsar
type PulsarConfig struct {
	Address        string `yaml:"address"`
	Port           int    `yaml:"port"`
	MaxMessageSize int    `yaml:"maxMessageSize"`
}

// RocksmqConfig is the config for rocksmq
type RocksmqConfig struct {
	Path string `yaml:"path"`
}

// RootCoordConfig is the config for root coordinator
type RootCoordConfig struct {
	Address                     string `yaml:"address"`
	Port                        int    `yaml:"port"`
	MaxPartitionNum             int    `yaml:"maxPartitionNum"`
	MinSegmentSizeToEnableIndex int    `yaml:"minSegmentSizeToEnableIndex"`
	Timeout                     int    `yaml:"timeout"`
	TimeTickInterval            int    `yaml:"timeTickInterval"`
}

// ProxyConfig is the config for proxy
type ProxyConfig struct {
	Port             int            `yaml:"port"`
	TimeTickInterval int            `yaml:"timeTickInterval"`
	MaxNameLength    int            `yaml:"maxNameLength"`
	MaxFieldNum      int            `yaml:"maxFieldNum"`
	MaxDimension     int            `yaml:"maxDimension"`
	MsgStream        ProxyMsgStream `yaml:"msgStream"`
}

// ProxyMsgStream is the config for msg stream in proxy
type ProxyMsgStream struct {
	InsertBufSize             int `yaml:"insertBufSize"`
	SearchBufSize             int `yaml:"searchBufSize"`
	SearchResultRecvBufSize   int `yaml:"searchResultRecvBufSize"`
	SearchResultPulsarBufSize int `yaml:"searchResultPulsarBufSize"`
	TimeTickBufSize           int `yaml:"timeTickBufSize"`
}

// QueryCoordConfig is the config for query coordinator
type QueryCoordConfig struct {
	Address string `yaml:"address"`
	Port    int    `yaml:"port"`
}

// QueryNodeConfig is the config for query node
type QueryNodeConfig struct {
	Port                 int                `yaml:"port"`
	GracefulTime         int                `yaml:"gracefulTime"`
	StatsPublishInterval int                `yaml:"statsPublishInterval"`
	FlowGraph            QueryNodeFlowGraph `yaml:"flowGraph"`
	MsgStream            QueryNodeMsgStream `yaml:"msgStream"`
}

// QueryNodeFlowGraph is the config for flow graph in query node
type QueryNodeFlowGraph struct {
	MaxQueueLength int `yaml:"maxQueueLength"`
	MaxParallelism int `yaml:"maxParallelism"`
}

// QueryNodeMsgStream is the config for msg stream in query node
type QueryNodeMsgStream struct {
	SearchRecvBufSize       int `yaml:"searchRecvBufSize"`
	SearchPulsarBufSize     int `yaml:"searchPulsarBufSize"`
	SearchResultRecvBufSize int `yaml:"searchResultRecvBufSize"`
}

// IndexCoordConfig is the config for index coordinator
type IndexCoordConfig struct {
	Address string `yaml:"address"`
	Port    int    `yaml:"port"`
}

// IndexNodeConfig is the config for index node
type IndexNodeConfig struct {
	Port int `yaml:"port"`
}

// DataCoordConfig is the config for data coordinator
type DataCoordConfig struct {
	Address string        `yaml:"address"`
	Port    int           `yaml:"port"`
	Segment SegmentConfig `yaml:"segment"`
}

// SegmentConfig is the config for segment manager in data coordinator
type SegmentConfig struct {
	MaxSize              int     `yaml:"maxSize"`
	SealProportion       float32 `yaml:"sealProportion"`
	AssignmentExpiration int     `yaml:"assignmentExpiration"`
}

// DataNodeConfig is the config for data node
type DataNodeConfig struct {
	Port         int               `yaml:"port"`
	FlushBufSize int               `yaml:"flushBufSize"`
	FlowGraph    DataNodeFlowGraph `yaml:"flowGraph"`
}

// DataNodeFlowGraph is the config for flow graph in data node
type DataNodeFlowGraph struct {
	MaxQueueLength int `yaml:"maxQueueLength"`
	MaxParallelism int `yaml:"maxParallelism"`
}

// LogConfig is the config for log
type LogConfig struct {
	Level  string        `yaml:"level"`
	Format string        `yaml:"format"`
	File   LogFileConfig `yaml:"file"`
}

// LogFileConfig is the config for log file output
type LogFileConfig struct {
	RootPath   string `yaml:"rootPath"`
	MaxSize    int    `yaml:"maxSize"`
	MaxAge     int    `yaml:"maxAge"`
	MaxBackups int    `yaml:"maxBackups"`
}

// MsgChannelConfig is the config for msg channel name
type MsgChannelConfig struct {
	RootCoordTimeTick      string `yaml:"rootCoordTimeTick"`
	RootCoordStatistics    string `yaml:"rootCoordStatistics"`
	Search                 string `yaml:"search"`
	SearchResult           string `yaml:"searchResult"`
	ProxyTimeTick          string `yaml:"proxyTimeTick"`
	QueryTimeTick          string `yaml:"queryTimeTick"`
	QueryNodeStats         string `yaml:"queryNodeStats"`
	Cmd                    string `yaml:"cmd"`
	DataCoordInsertChannel string `yaml:"dataCoordInsertChannel"`
	DataCoordStatistic     string `yaml:"dataCoordStatistic"`
	DataCoordTimeTick      string `yaml:"dataCoordTimeTick"`
	DataCoordSegmentInfo   string `yaml:"dataCoordSegmentInfo"`
	RootCoordSubnamePrefix string `yaml:"rootCoordSubNamePrefix"`
	ProxySubnamePrefix     string `yaml:"proxySubNamePrefix"`
	QueryNodeSubnamePrefix string `yaml:"queryNodeSubNamePrefix"`
	DataNodeSubnamePrefix  string `yaml:"dataNodeSubNamePrefix"`
	DataCoordSubnamePrefix string `yaml:"dataCoordSubNamePrefix"`
}
