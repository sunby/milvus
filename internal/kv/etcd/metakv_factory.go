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

package etcdkv

import (
	"github.com/milvus-io/milvus/configs"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

// NewMetaKvFactory returns an object that implements the kv.MetaKv interface using etcd.
// The UseEmbedEtcd in the param is used to determine whether the etcd service is external or embedded.
// TODO: Duplicated with `InitEtcdServer` in etcd_util.go
func NewMetaKvFactory(rootPath string, cfg *configs.Config) (kv.MetaKv, error) {
	log.Info("start etcd with rootPath",
		zap.String("rootpath", rootPath),
		zap.Bool("isEmbed", cfg.Etcd.UseEmbed))
	if cfg.Etcd.UseEmbed {
		path := cfg.Etcd.ConfigPath
		var etcdCfg *embed.Config
		if len(path) > 0 {
			cfgFromFile, err := embed.ConfigFromFile(path)
			if err != nil {
				return nil, err
			}
			etcdCfg = cfgFromFile
		} else {
			etcdCfg = embed.NewConfig()
		}
		etcdCfg.Dir = cfg.Etcd.DataPath
		metaKv, err := NewEmbededEtcdKV(etcdCfg, rootPath)
		if err != nil {
			return nil, err
		}
		return metaKv, err
	}
	client, err := etcd.GetEtcdClient(cfg)
	if err != nil {
		return nil, err
	}
	metaKv := NewEtcdKV(client, rootPath)
	return metaKv, err
}
