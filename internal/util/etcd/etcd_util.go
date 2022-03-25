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

package etcd

import (
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/configs"
	"github.com/milvus-io/milvus/internal/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
)

// EtcdServer is the singleton of embedded etcd server
var EtcdServer *embed.Etcd

// InitEtcdServer initializes embedded etcd server singleton.
func InitEtcdServer(cfg *configs.Config) error {
	if cfg.Etcd.UseEmbed {
		cfgPath := cfg.Etcd.ConfigPath
		log.Info("initialize embedded etcd", zap.String("configPath", cfgPath), zap.String("dataPath", cfg.Etcd.DataPath))
		var etcdCfg *embed.Config
		var err error
		if len(cfgPath) > 0 {
			etcdCfg, err = embed.ConfigFromFile(cfgPath)
			if err != nil {
				return err
			}
		} else {
			etcdCfg = embed.NewConfig()
		}
		etcdCfg.Dir = cfg.Etcd.DataPath
		etcdCfg.LogOutputs = []string{} // TODO: add config in Etcd
		etcdCfg.LogLevel = cfg.Etcd.LogLevel

		EtcdServer, err = embed.StartEtcd(etcdCfg)
		if err != nil {
			return err
		}
		log.Info("finish to initialize embedded etcd")
	}
	return nil
}

// StopEtcdServer stops embedded etcd server singleton.
func StopEtcdServer() {
	if EtcdServer != nil {
		EtcdServer.Close()
	}
}

// GetEtcdClient returns etcd client
func GetEtcdClient(cfg *configs.Config) (*clientv3.Client, error) {
	if cfg.Etcd.UseEmbed {
		return GetEmbedEtcdClient()
	}
	return GetRemoteEtcdClient(cfg.Etcd.Endpoints)
}

// GetEmbedEtcdClient returns client of embed etcd server
func GetEmbedEtcdClient() (*clientv3.Client, error) {
	client := v3client.New(EtcdServer.Server)
	return client, nil
}

// GetRemoteEtcdClient returns client of remote etcd by given endpoints
func GetRemoteEtcdClient(endpoints []string) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
}
