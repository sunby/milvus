// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (node *Proxy) ensureCollectionReady(ctx context.Context, dbName, collectionName string) error {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return err
	}
	if !Params.ProxyCfg.EnableAutoLoad.GetAsBool() {
		return nil
	}

	readiness, ok := node.viewQueryClient.(queryclient.CollectionReadiness)
	if !ok {
		return nil
	}

	loadTimeout := Params.QueryCoordCfg.LoadTimeoutSeconds.GetAsDuration(time.Second)
	ctx, cancel := context.WithTimeout(ctx, loadTimeout)
	defer cancel()

	metaCache := node.getMetaCache()
	collectionID, err := metaCache.GetCollectionID(ctx, dbName, collectionName)
	if err != nil {
		return err
	}
	collectionInfo, err := metaCache.GetCollectionInfo(ctx, dbName, collectionName, collectionID)
	if err != nil {
		return err
	}

	err = readiness.CheckCollectionReady(ctx, collectionID, collectionInfo.VChannels)
	if err == nil {
		return nil
	}
	if !errors.Is(err, merr.ErrCollectionNotLoaded) {
		return err
	}

	loadState, err := node.GetLoadState(ctx, &milvuspb.GetLoadStateRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	if err := merr.CheckRPCCall(loadState, err); err != nil {
		return merr.Wrap(err, "check collection load state before DQL request")
	}

	switch loadState.GetState() {
	case commonpb.LoadState_LoadStateNotLoad:
		loadRequest := &milvuspb.LoadCollectionRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		}
		ctx, err = PrivilegeInterceptor(ctx, loadRequest)
		if err != nil {
			if grpcstatus.Code(err) == codes.PermissionDenied {
				return merr.WrapErrPrivilegeNotPermitted("%s", grpcstatus.Convert(err).Message())
			}
			return err
		}
		resultCh := node.autoLoadCollectionGroup.DoChan(strconv.FormatInt(collectionID, 10), func() (struct{}, error) {
			loadCtx, cancelLifecycle := contextutil.MergeContext(context.WithoutCancel(ctx), node.ctx)
			defer cancelLifecycle()
			loadCtx, cancelTimeout := context.WithTimeout(loadCtx, loadTimeout)
			defer cancelTimeout()

			loadState, err := node.GetLoadState(loadCtx, &milvuspb.GetLoadStateRequest{
				DbName:         dbName,
				CollectionName: collectionName,
			})
			if err := merr.CheckRPCCall(loadState, err); err != nil {
				return struct{}{}, merr.Wrap(err, "recheck collection load state before DQL request")
			}

			switch loadState.GetState() {
			case commonpb.LoadState_LoadStateNotLoad:
				mlog.Info(loadCtx, "load collection before DQL request",
					mlog.FieldDbName(dbName),
					mlog.FieldCollectionName(collectionName),
					mlog.FieldCollectionID(collectionID))
				status, err := node.loadCollectionForDQL(loadCtx, loadRequest)
				if err := merr.CheckRPCCall(status, err); err != nil {
					return struct{}{}, merr.Wrap(err, "load collection before DQL request")
				}
			case commonpb.LoadState_LoadStateLoading, commonpb.LoadState_LoadStateLoaded:
			case commonpb.LoadState_LoadStateNotExist:
				return struct{}{}, merr.WrapErrCollectionNotFoundWithDB(dbName, collectionName)
			default:
				return struct{}{}, merr.WrapErrServiceInternalMsg("unexpected collection load state %s", loadState.GetState().String())
			}
			return struct{}{}, nil
		})
		select {
		case result := <-resultCh:
			if result.Err != nil {
				return result.Err
			}
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	case commonpb.LoadState_LoadStateLoading, commonpb.LoadState_LoadStateLoaded:
		// The QueryCoord state may precede assignment propagation to this Proxy.
		// The resolver barrier below is the final DQL-readiness signal.
	case commonpb.LoadState_LoadStateNotExist:
		return merr.WrapErrCollectionNotFoundWithDB(dbName, collectionName)
	default:
		return merr.WrapErrServiceInternalMsg("unexpected collection load state %s", loadState.GetState().String())
	}

	waitStartedAt := time.Now()
	if err := readiness.WaitForCollectionReady(ctx, collectionID, collectionInfo.VChannels); err != nil {
		return err
	}
	mlog.Info(ctx, "[load on search] wait collection ready done",
		mlog.FieldDbName(dbName),
		mlog.FieldCollectionName(collectionName),
		mlog.FieldCollectionID(collectionID),
		mlog.Duration("wait", time.Since(waitStartedAt)))
	return nil
}

// loadCollectionForDQL submits a shared automatic load without a task queue.
// Blocking the DQL dispatcher on a full main pool would also prevent it from
// dispatching the requery subtasks needed by the searches occupying that pool.
func (node *Proxy) loadCollectionForDQL(ctx context.Context, request *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, span := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-LoadCollection")
	defer span.End()
	tr := timerecord.NewTimeRecorder("LoadCollection")
	lct := &loadCollectionTask{
		baseTask:              baseTask{metaCache: node.getMetaCache()},
		ctx:                   ctx,
		LoadCollectionRequest: request,
		mixCoord:              node.mixCoord,
	}
	if err := lct.OnEnqueue(); err != nil {
		return merr.Status(err), nil
	}
	// Preserve the request identity normally assigned by the task queue.
	ts, err := node.tsoAllocator.AllocOne(ctx)
	if err != nil {
		return merr.Status(err), nil
	}
	lct.SetTs(ts)
	lct.SetID(UniqueID(ts))
	if err := lct.PreExecute(ctx); err != nil {
		return merr.Status(err), nil
	}
	if err := lct.Execute(ctx); err != nil {
		return merr.Status(err), nil
	}
	if err := lct.PostExecute(ctx); err != nil {
		return merr.Status(err), nil
	}
	metrics.ProxyReqLatency.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10), "LoadCollection",
	).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return lct.result, nil
}
