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

package stage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestOutcome(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want Result
	}{
		{merr.Error(merr.Status(context.Canceled)), Canceled},
		{merr.Error(merr.Status(context.DeadlineExceeded)), Timeout},
		{nil, Success},
		{errors.New("failure"), Error},
		{context.Canceled, Canceled},
		{fmt.Errorf("wrapped: %w", context.DeadlineExceeded), Timeout},
		{status.Error(codes.Canceled, "cancel"), Canceled},
		{status.Error(codes.DeadlineExceeded, "timeout"), Timeout},
	} {
		require.Equal(t, tc.want, Outcome(tc.err))
	}
}

func TestTimerBalancesEveryOutcomeAndIsIdempotent(t *testing.T) {
	r := New("test", "timer", "balanced")
	for result := Success; result < resultCount; result++ {
		timer := r.Begin()
		require.Equal(t, float64(1), testutil.ToFloat64(r.inflight))
		timer.EndResult(result)
		timer.EndResult(result)
		require.Zero(t, testutil.ToFloat64(r.inflight))
	}
}

func TestTimerOnlyCreatesSampledChild(t *testing.T) {
	for _, sample := range []bool{false, true} {
		spans := tracetest.NewSpanRecorder()
		sampler := sdktrace.NeverSample()
		if sample {
			sampler = sdktrace.AlwaysSample()
		}
		provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sampler), sdktrace.WithSpanProcessor(spans))
		old := otel.GetTracerProvider()
		otel.SetTracerProvider(provider)
		ctx, parent := provider.Tracer("test").Start(context.Background(), "parent")
		r := New("test", "timer", "trace")
		childCtx, timer := r.Start(ctx)
		require.NotNil(t, childCtx)
		timer.End(context.Canceled)
		parent.End()
		if sample {
			require.Len(t, spans.Ended(), 2)
		} else {
			require.Empty(t, spans.Ended())
		}
		require.Zero(t, testutil.ToFloat64(r.inflight))
		otel.SetTracerProvider(old)
		require.NoError(t, provider.Shutdown(context.Background()))
	}
}

func BenchmarkTimer(b *testing.B) {
	r := New("test", "benchmark", "timer")
	r.Observe(time.Nanosecond, Success)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := r.Begin()
		t.End(nil)
	}
}
