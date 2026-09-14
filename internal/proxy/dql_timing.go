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
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/stage"
)

type (
	dqlTimingKey struct{}
	dqlTiming    struct {
		ctx             context.Context
		operation, path string
		started, ready  time.Time
		timer           stage.Timer
		span            trace.Span
	}
)

var dqlRecorders = map[string]*stage.Recorder{
	"Search":       stage.New("proxy", "Search", "request"),
	"HybridSearch": stage.New("proxy", "HybridSearch", "request"),
	"Query":        stage.New("proxy", "Query", "request"),
}

func startDQL(ctx context.Context, operation string) (context.Context, *dqlTiming) {
	ctx, span := otel.Tracer("milvus/query-stages").Start(ctx, "Proxy-"+operation+"-Request")
	t := &dqlTiming{ctx: ctx, operation: operation, path: "unknown", started: time.Now(), timer: dqlRecorders[operation].Begin(), span: span}
	return context.WithValue(ctx, dqlTimingKey{}, t), t
}

// Only the caller sets its observed readiness path. Shared load workers outlive
// callers and must never mutate this request-local state.
func setDQLPath(ctx context.Context, path string) {
	if t, ok := ctx.Value(dqlTimingKey{}).(*dqlTiming); ok {
		t.path = path
	}
}
func (t *dqlTiming) Ready() { t.ready = time.Now() }
func (t *dqlTiming) End(status *commonpb.Status, err error) {
	if err == nil {
		err = merr.Error(status)
	}
	t.finish(time.Now(), stage.Outcome(err))
}

func (t *dqlTiming) finish(ended time.Time, result stage.Result) {
	total := ended.Sub(t.started)
	readiness := total
	if !t.ready.IsZero() {
		readiness = t.ready.Sub(t.started)
	}
	execution := total - readiness
	cohort := "le1s"
	if total > time.Second {
		cohort = "gt1s"
	}
	for i, name := range [...]string{"total", "readiness", "execution"} {
		d := [...]time.Duration{total, readiness, execution}[i]
		metrics.QueryRequestStageDuration.WithLabelValues(t.operation, t.path, cohort, name, result.String()).Observe(d.Seconds())
	}
	t.span.SetAttributes(attribute.String("load.path", t.path), attribute.String("result", result.String()),
		attribute.Float64("readiness.seconds", readiness.Seconds()), attribute.Float64("execution.seconds", execution.Seconds()))

	recording := "unsampled"
	if t.span.IsRecording() {
		recording = "sampled"
	}
	metrics.QueryStageItems.WithLabelValues("proxy", t.operation, "trace_coverage", recording).Inc()
	if cohort == "gt1s" || result != stage.Success {
		metrics.QueryStageItems.WithLabelValues("proxy", t.operation, "slow_summary", "candidate").Inc()
		if mlog.LevelEnabled(mlog.InfoLevel) && dqlSummaryLimiter.Allow() {
			metrics.QueryStageItems.WithLabelValues("proxy", t.operation, "slow_summary", "emitted").Inc()
			mlog.Info(t.ctx, "DQL request stages", mlog.String("operation", t.operation), mlog.String("loadPath", t.path),
				mlog.String("result", result.String()), mlog.Duration("total", total), mlog.Duration("readiness", readiness), mlog.Duration("execution", execution),
				mlog.String("traceID", t.span.SpanContext().TraceID().String()))
		} else {
			metrics.QueryStageItems.WithLabelValues("proxy", t.operation, "slow_summary", "dropped").Inc()
		}
	}
	t.timer.EndResult(result)
	t.span.End()
}

type autoLoadResult struct{ SpanContext trace.SpanContext }

var (
	autoLoadTotal   = stage.New("proxy", "auto_load", "shared_lifecycle")
	autoLoadRecheck = stage.New("proxy", "auto_load", "recheck")
	autoLoadSubmit  = stage.New("proxy", "auto_load", "load_submit")
	autoLoadReady   = stage.New("proxy", "auto_load", "ready_wait")
	autoLoadCaller  = stage.New("proxy", "auto_load", "caller_wait")
)

var dqlSummaryLimiter = rate.NewLimiter(2, 10)
