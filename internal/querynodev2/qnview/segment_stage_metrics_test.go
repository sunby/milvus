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

package qnview

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
)

func TestSegmentLoadCanceledNilReturnIsNotSuccessful(t *testing.T) {
	observer := metrics.QueryStageDuration.WithLabelValues("queryNode", "segment_load", "total", "canceled").(prometheus.Metric)
	before := &dto.Metric{}
	require.NoError(t, observer.Write(before))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	task := &SegmentLoadTask{Context: ctx}
	require.NoError(t, task.Execute(context.Background()), "preserve scheduler cancellation behavior")
	after := &dto.Metric{}
	require.NoError(t, observer.Write(after))
	require.Equal(t, before.GetHistogram().GetSampleCount()+1, after.GetHistogram().GetSampleCount())
}
