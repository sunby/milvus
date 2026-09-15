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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/stage"
)

func requestStageSum(t *testing.T, op, path, cohort, name, result string) float64 {
	t.Helper()
	m := &dto.Metric{}
	require.NoError(t, metrics.QueryRequestStageDuration.WithLabelValues(op, path, cohort, name, result).(prometheus.Metric).Write(m))
	return m.GetHistogram().GetSampleSum()
}

func TestDQLTimingUsesOnePopulationAndExactPartition(t *testing.T) {
	_, timer := startDQL(context.Background(), "Search")
	timer.path = "unloaded"
	timer.ready = timer.started.Add(1200 * time.Millisecond)
	before := [3]float64{}
	for i, s := range []string{"total", "readiness", "execution"} {
		before[i] = requestStageSum(t, "Search", "unloaded", "gt1s", s, "success")
	}
	timer.finish(timer.started.Add(2*time.Second), stage.Success)
	got := [3]float64{}
	for i, s := range []string{"total", "readiness", "execution"} {
		got[i] = requestStageSum(t, "Search", "unloaded", "gt1s", s, "success") - before[i]
	}
	require.InDelta(t, 2, got[0], 1e-9)
	require.InDelta(t, 1.2, got[1], 1e-9)
	require.InDelta(t, got[0], got[1]+got[2], 1e-9)
}

func TestDQLTimingCountsEmbeddedErrorBeforeExecution(t *testing.T) {
	_, timer := startDQL(context.Background(), "Query")
	timer.path = "loading"
	before := requestStageSum(t, "Query", "loading", "le1s", "execution", "error")
	metric := metrics.QueryRequestStageDuration.WithLabelValues("Query", "loading", "le1s", "execution", "error").(prometheus.Metric)
	beforeMetric := &dto.Metric{}
	require.NoError(t, metric.Write(beforeMetric))
	timer.End(merr.Status(merr.WrapErrCollectionNotLoaded(1)), nil)
	afterMetric := &dto.Metric{}
	require.NoError(t, metric.Write(afterMetric))
	require.Equal(t, beforeMetric.GetHistogram().GetSampleCount()+1, afterMetric.GetHistogram().GetSampleCount())
	require.Equal(t, before, requestStageSum(t, "Query", "loading", "le1s", "execution", "error"))
}
