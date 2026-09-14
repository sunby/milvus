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

// Package stage records bounded-cardinality query lifecycle metrics and sampled
// child spans. Recorders belong at package scope; request identifiers are trace
// attributes, never metric labels.
package stage

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Result is a bounded telemetry outcome, independent of application error policy.
type Result uint8

const (
	Success Result = iota
	Error
	Canceled
	Timeout
	Superseded
	NotReady
	resultCount
)

var resultNames = [resultCount]string{"success", "error", "canceled", "timeout", "superseded", "not_ready"}

func (r Result) String() string { return resultNames[r] }

// Outcome only describes an observed result. It never changes application
// errors, retries, or wire codes. Callers with embedded statuses must unwrap them.
func Outcome(err error) Result {
	if err == nil {
		return Success
	}
	if errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded {
		return Timeout
	}
	if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
		return Canceled
	}
	// Remote errors reconstructed from Status retain these numeric codes but
	// no longer wrap Go's context sentinels.
	switch merr.Code(err) {
	case merr.CanceledCode:
		return Canceled
	case merr.TimeoutCode:
		return Timeout
	}
	return Error
}

// Recorder is safe for concurrent use. Reuse it across operations with the same labels.
type Recorder struct {
	durations    [resultCount]prometheus.Observer
	once         [resultCount]sync.Once
	labels       [3]string
	inflight     prometheus.Gauge
	inflightOnce sync.Once
	name         string
}

// New accepts fixed labels from an instrumentation site, never request identifiers.
func New(component, operation, name string) *Recorder {
	r := &Recorder{
		labels: [3]string{component, operation, name}, name: component + "." + operation + "." + name,
	}
	return r
}

// Timer belongs to one operation. It must not be copied or ended concurrently.
type Timer struct {
	recorder *Recorder
	started  time.Time
	span     trace.Span
}

// Begin measures unsampled operations without allocating trace contexts.
func (r *Recorder) Begin() Timer {
	r.inflightOnce.Do(func() { r.inflight = metrics.QueryStageInflight.WithLabelValues(r.labels[0], r.labels[1], r.labels[2]) })
	r.inflight.Inc()
	return Timer{recorder: r, started: time.Now()}
}

// Start attaches a child only when the caller's trace is recording. Metrics are
// always recorded, including requests omitted by the trace sampler.
func (r *Recorder) Start(ctx context.Context) (context.Context, Timer) {
	t := r.Begin()
	if trace.SpanFromContext(ctx).IsRecording() {
		ctx, t.span = otel.Tracer("milvus/query-stages").Start(ctx, r.name)
	}
	return ctx, t
}

// End is idempotent on this timer. Do not copy an active timer to another owner.
func (t *Timer) End(err error)       { t.EndResult(Outcome(err)) }
func (t *Timer) EndError(err *error) { t.End(*err) }
func (t *Timer) EndResult(result Result) {
	if t.recorder == nil {
		return
	}
	t.recorder.Observe(time.Since(t.started), result)
	t.recorder.inflight.Dec()
	t.recorder = nil
	if t.span != nil {
		if result != Success {
			t.span.SetStatus(otelcodes.Error, result.String())
		}
		t.span.End()
	}
}

// Observe records a completed interval measured by an existing lifecycle owner.
// It does not create an inflight series: live intervals require Begin or Start.
func (r *Recorder) Observe(d time.Duration, result Result) {
	r.once[result].Do(func() {
		r.durations[result] = metrics.QueryStageDuration.WithLabelValues(r.labels[0], r.labels[1], r.labels[2], result.String())
	})
	r.durations[result].Observe(max(0, d.Seconds()))
}
