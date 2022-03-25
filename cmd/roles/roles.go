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

package roles

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	rocksmqimpl "github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/server"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/cmd/components"
	"github.com/milvus-io/milvus/configs"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/healthz"
	logutil "github.com/milvus-io/milvus/internal/util/logutil"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/trace"
)

func newMsgFactory(localMsg bool) msgstream.Factory {
	if localMsg {
		return msgstream.NewRmsFactory()
	}
	return msgstream.NewPmsFactory()
}

func initRocksmq() error {
	err := rocksmqimpl.InitRocksMQ()
	return err
}

func stopRocksmq() {
	rocksmqimpl.CloseRocksMQ()
}

// MilvusRoles decides which components are brought up with Milvus.
type MilvusRoles struct {
	EnableRootCoord  bool `env:"ENABLE_ROOT_COORD"`
	EnableProxy      bool `env:"ENABLE_PROXY"`
	EnableQueryCoord bool `env:"ENABLE_QUERY_COORD"`
	EnableQueryNode  bool `env:"ENABLE_QUERY_NODE"`
	EnableDataCoord  bool `env:"ENABLE_DATA_COORD"`
	EnableDataNode   bool `env:"ENABLE_DATA_NODE"`
	EnableIndexCoord bool `env:"ENABLE_INDEX_COORD"`
	EnableIndexNode  bool `env:"ENABLE_INDEX_NODE"`
}

// EnvValue not used now.
func (mr *MilvusRoles) EnvValue(env string) bool {
	env = strings.ToLower(env)
	env = strings.Trim(env, " ")
	return env == "1" || env == "true"
}

func (mr *MilvusRoles) runRootCoord(ctx context.Context, cfg *configs.Config, localMsg bool) *components.RootCoord {
	var rc *components.RootCoord
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		factory := newMsgFactory(localMsg)
		var err error
		rc, err = components.NewRootCoord(ctx, cfg, factory)
		if err != nil {
			panic(err)
		}
		if !localMsg {
			http.Handle(healthz.HealthzRouterPath, &componentsHealthzHandler{component: rc})
		}
		wg.Done()
		_ = rc.Run()
	}()
	wg.Wait()

	metrics.RegisterRootCoord()
	return rc
}

func (mr *MilvusRoles) runProxy(ctx context.Context, cfg *configs.Config, localMsg bool, alias string) *components.Proxy {
	var pn *components.Proxy
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		factory := newMsgFactory(localMsg)
		var err error
		pn, err = components.NewProxy(ctx, cfg, factory)
		if err != nil {
			panic(err)
		}
		if !localMsg {
			http.Handle(healthz.HealthzRouterPath, &componentsHealthzHandler{component: pn})
		}
		wg.Done()
		_ = pn.Run()
	}()
	wg.Wait()

	metrics.RegisterProxy()
	return pn
}

func (mr *MilvusRoles) runQueryCoord(ctx context.Context, localMsg bool) *components.QueryCoord {
	var qs *components.QueryCoord
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		factory := newMsgFactory(localMsg)
		var err error
		qs, err = components.NewQueryCoord(ctx, factory)
		if err != nil {
			panic(err)
		}
		if !localMsg {
			http.Handle(healthz.HealthzRouterPath, &componentsHealthzHandler{component: qs})
		}
		wg.Done()
		_ = qs.Run()
	}()
	wg.Wait()

	metrics.RegisterQueryCoord()
	return qs
}

func (mr *MilvusRoles) runQueryNode(ctx context.Context, localMsg bool, alias string) *components.QueryNode {
	var qn *components.QueryNode
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		factory := newMsgFactory(localMsg)
		var err error
		qn, err = components.NewQueryNode(ctx, factory)
		if err != nil {
			panic(err)
		}
		if !localMsg {
			http.Handle(healthz.HealthzRouterPath, &componentsHealthzHandler{component: qn})
		}
		wg.Done()
		_ = qn.Run()
	}()
	wg.Wait()

	metrics.RegisterQueryNode()
	return qn
}

func (mr *MilvusRoles) runDataCoord(ctx context.Context, localMsg bool) *components.DataCoord {
	var ds *components.DataCoord
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		factory := newMsgFactory(localMsg)

		dctx := logutil.WithModule(ctx, "DataCoord")
		var err error
		ds, err = components.NewDataCoord(dctx, factory)
		if err != nil {
			panic(err)
		}
		if !localMsg {
			http.Handle(healthz.HealthzRouterPath, &componentsHealthzHandler{component: ds})
		}
		wg.Done()
		_ = ds.Run()
	}()
	wg.Wait()

	metrics.RegisterDataCoord()
	return ds
}

func (mr *MilvusRoles) runDataNode(ctx context.Context, localMsg bool, alias string) *components.DataNode {
	var dn *components.DataNode
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		factory := newMsgFactory(localMsg)
		var err error
		dn, err = components.NewDataNode(ctx, factory)
		if err != nil {
			panic(err)
		}
		if !localMsg {
			http.Handle(healthz.HealthzRouterPath, &componentsHealthzHandler{component: dn})
		}
		wg.Done()
		_ = dn.Run()
	}()
	wg.Wait()

	metrics.RegisterDataNode()
	return dn
}

func (mr *MilvusRoles) runIndexCoord(ctx context.Context, localMsg bool) *components.IndexCoord {
	var is *components.IndexCoord
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		var err error
		is, err = components.NewIndexCoord(ctx)
		if err != nil {
			panic(err)
		}
		if !localMsg {
			http.Handle(healthz.HealthzRouterPath, &componentsHealthzHandler{component: is})
		}
		wg.Done()
		_ = is.Run()
	}()
	wg.Wait()

	metrics.RegisterIndexCoord()
	return is
}

func (mr *MilvusRoles) runIndexNode(ctx context.Context, localMsg bool, alias string) *components.IndexNode {
	var in *components.IndexNode
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		var err error
		in, err = components.NewIndexNode(ctx)
		if err != nil {
			panic(err)
		}
		if !localMsg {
			http.Handle(healthz.HealthzRouterPath, &componentsHealthzHandler{component: in})
		}
		wg.Done()
		_ = in.Run()
	}()
	wg.Wait()

	metrics.RegisterIndexNode()
	return in
}

// Run Milvus components.
func (mr *MilvusRoles) Run(cfg *configs.Config, local bool, alias string) {
	ctx, cancel := context.WithCancel(context.Background())

	// only standalone enable localMsg
	if local {
		if err := os.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode); err != nil {
			log.Error("Failed to set deploy mode: ", zap.Error(err))
		}
		if err := initRocksmq(); err != nil {
			panic(err)
		}
		defer stopRocksmq()

		if cfg.Etcd.UseEmbed {
			// start etcd server
			if err := etcd.InitEtcdServer(cfg); err != nil {
				log.Fatal("initialize etcd failure", zap.Error(err))
			}
			defer etcd.StopEtcdServer()
		}
	} else {
		if err := os.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.ClusterDeployMode); err != nil {
			log.Error("Failed to set deploy mode: ", zap.Error(err))
		}
	}

	if os.Getenv(metricsinfo.DeployModeEnvKey) == metricsinfo.StandaloneDeployMode {
		closer := trace.InitTracing("standalone")
		if closer != nil {
			defer closer.Close()
		}
	}

	var rc *components.RootCoord
	if mr.EnableRootCoord {
		rc = mr.runRootCoord(ctx, cfg, local)
		if rc != nil {
			defer rc.Stop()
		}
	}

	var pn *components.Proxy
	if mr.EnableProxy {
		pctx := logutil.WithModule(ctx, "Proxy")
		pn = mr.runProxy(pctx, cfg, local, alias)
		if pn != nil {
			defer pn.Stop()
		}
	}

	var qs *components.QueryCoord
	if mr.EnableQueryCoord {
		qs = mr.runQueryCoord(ctx, local)
		if qs != nil {
			defer qs.Stop()
		}
	}

	var qn *components.QueryNode
	if mr.EnableQueryNode {
		qn = mr.runQueryNode(ctx, local, alias)
		if qn != nil {
			defer qn.Stop()
		}
	}

	var ds *components.DataCoord
	if mr.EnableDataCoord {
		ds = mr.runDataCoord(ctx, local)
		if ds != nil {
			defer ds.Stop()
		}
	}

	var dn *components.DataNode
	if mr.EnableDataNode {
		dn = mr.runDataNode(ctx, local, alias)
		if dn != nil {
			defer dn.Stop()
		}
	}

	var is *components.IndexCoord
	if mr.EnableIndexCoord {
		is = mr.runIndexCoord(ctx, local)
		if is != nil {
			defer is.Stop()
		}
	}

	var in *components.IndexNode
	if mr.EnableIndexNode {
		in = mr.runIndexNode(ctx, local, alias)
		if in != nil {
			defer in.Stop()
		}
	}

	if local {
		standaloneHealthzHandler := func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			req := &internalpb.GetComponentStatesRequest{}
			validateResp := func(resp *internalpb.ComponentStates, err error) bool {
				return err == nil && resp != nil && resp.GetState().GetStateCode() == internalpb.StateCode_Healthy
			}
			if rc == nil || !validateResp(rc.GetComponentStates(ctx, req)) {
				rootCoordNotServingHandler(w, r)
				return
			}
			if qs == nil || !validateResp(qs.GetComponentStates(ctx, req)) {
				queryCoordNotServingHandler(w, r)
				return
			}

			if ds == nil || !validateResp(ds.GetComponentStates(ctx, req)) {
				dataCoordNotServingHandler(w, r)
				return
			}
			if is == nil || !validateResp(is.GetComponentStates(ctx, req)) {
				indexCoordNotServingHandler(w, r)
				return
			}

			if pn == nil || !validateResp(pn.GetComponentStates(ctx, req)) {
				proxyNotServingHandler(w, r)
				return
			}
			// TODO(dragondriver): need to check node state?

			w.WriteHeader(http.StatusOK)
			w.Header().Set(healthz.ContentTypeHeader, healthz.ContentTypeText)
			_, err := fmt.Fprint(w, "OK")
			if err != nil {
				log.Warn("Failed to send response",
					zap.Error(err))
			}

			// TODO(dragondriver): handle component states
		}
		http.HandleFunc(healthz.HealthzRouterPath, standaloneHealthzHandler)
	}

	metrics.ServeHTTP()
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	sig := <-sc
	log.Error("Get signal to exit\n", zap.String("signal", sig.String()))

	// some deferred Stop has race with context cancel
	cancel()
}
