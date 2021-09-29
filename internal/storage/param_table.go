package storage

import (
	"sync"

	"github.com/milvus-io/milvus/internal/util/paramtable"
)

var Params ParamTable
var once sync.Once

type ParamTable struct {
	paramtable.BaseTable

	compressType CompressType
}

func (pt *ParamTable) Init() {
	once.Do(func() {
		pt.BaseTable.Init()
		err := pt.LoadYaml("advanced/binlog.yaml")
		if err != nil {
			panic(err)
		}
		pt.initCompressType()
	})
}

func (pt *ParamTable) initCompressType() {
	ret, err := pt.Load("binlog.compress.type")
	if err != nil {
		panic(err)
	}
	if ret == "SNAPPY" {
		pt.compressType = CompressType_SNAPPY
	} else if ret == "GZIP" {
		pt.compressType = CompressType_GZIP
	} else {
		pt.compressType = CompressType_UNCOMPRESSED
	}

}
