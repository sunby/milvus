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

package deletebuffer

import (
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func newL0MetricsTestSegment(t *testing.T, id, collectionID int64, timestamp uint64) *segments.MockSegment {
	t.Helper()
	segment := segments.NewMockSegment(t)
	segment.EXPECT().ID().Return(id)
	segment.EXPECT().Collection().Return(collectionID)
	segment.EXPECT().StartPosition().Return(&msgpb.MsgPosition{Timestamp: timestamp})
	segment.EXPECT().Release(mock.Anything).Return()
	return segment
}

func setupL0MetricsTest(t *testing.T, mode string) *prometheus.Registry {
	t.Helper()
	previousMode := metrics.CollectionLevelMetricsMode()
	metrics.SetCollectionLevelMetricsMode(mode)
	metrics.QueryNodeNumSegments.Reset()
	t.Cleanup(func() {
		metrics.QueryNodeNumSegments.Reset()
		metrics.SetCollectionLevelMetricsMode(previousMode)
	})
	registry := prometheus.NewRegistry()
	registry.MustRegister(metrics.QueryNodeNumSegments)
	return registry
}

func gatherSegmentCounts(t *testing.T, registry *prometheus.Registry) map[string]float64 {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	counts := make(map[string]float64)
	for _, family := range families {
		require.Equal(t, "milvus_querynode_segment_num", family.GetName())
		for _, metric := range family.GetMetric() {
			labels := make(map[string]string)
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			require.Len(t, labels, 4)
			require.Equal(t, paramtable.GetStringNodeID(), labels["node_id"])
			counts[labels["collection_id"]+"/"+labels["segment_state"]+"/"+labels["segment_level"]] = metric.GetGauge().GetValue()
		}
	}
	return counts
}

func TestL0SegmentMetricsLifecycle(t *testing.T) {
	for _, mode := range []string{metrics.CollectionLevelMetricsModeFull, metrics.CollectionLevelMetricsModeAggregate} {
		for _, kind := range []string{"list", "double cache"} {
			t.Run(mode+"/"+kind, func(t *testing.T) {
				registry := setupL0MetricsTest(t, mode)
				newBuffer := func(channel string) DeleteBuffer[*Item] {
					if kind == "list" {
						return NewListDeleteBuffer[*Item](0, 1000, []string{paramtable.GetStringNodeID(), channel})
					}
					return NewDoubleCacheDeleteBuffer[*Item](0, 1000)
				}
				first := newBuffer("collection-1-v0")
				second := newBuffer("collection-1-v1")
				otherCollection := newBuffer("collection-2-v0")
				seg1 := newL0MetricsTestSegment(t, 1, 1, 10)
				seg2 := newL0MetricsTestSegment(t, 2, 1, 20)
				seg3 := newL0MetricsTestSegment(t, 3, 1, 30)
				seg4 := newL0MetricsTestSegment(t, 4, 2, 40)

				first.RegisterL0(nil)
				require.Empty(t, gatherSegmentCounts(t, registry))
				// Registration currently accepts duplicate entries; count and release
				// each entry without changing the buffer's ownership semantics.
				first.RegisterL0(seg1, nil, seg1, seg2)
				second.RegisterL0(seg3)
				otherCollection.RegisterL0(seg4)
				assertCounts := func(firstCollection, other float64) {
					t.Helper()
					expected := map[string]float64{"1/Sealed/L0": firstCollection, "2/Sealed/L0": other}
					if mode == metrics.CollectionLevelMetricsModeAggregate {
						expected = map[string]float64{"all/Sealed/L0": firstCollection + other}
					}
					require.Equal(t, expected, gatherSegmentCounts(t, registry))
				}
				assertCounts(4, 1)
				first.UnRegister(10) // The timestamp boundary is exclusive.
				assertCounts(4, 1)
				if kind == "list" {
					first.Pin(5, 99)
					first.UnRegister(11)
					assertCounts(4, 1)
					first.Unpin(5, 99)
				}
				first.UnRegister(11)
				first.UnRegister(11)
				assertCounts(2, 1)
				seg1.AssertNumberOfCalls(t, "Release", 2)
				first.Clear()
				first.Clear()
				assertCounts(1, 1)
				second.Clear()
				assertCounts(0, 1)

				// The final delegator is cleared before collection cleanup. A shared
				// aggregate series must retain the other collection's contribution.
				metrics.CleanupQueryNodeCollectionMetrics(paramtable.GetNodeID(), 1)
				expected := map[string]float64{"2/Sealed/L0": 1}
				if mode == metrics.CollectionLevelMetricsModeAggregate {
					expected = map[string]float64{"all/Sealed/L0": 1}
				}
				require.Equal(t, expected, gatherSegmentCounts(t, registry))
				otherCollection.Clear()
				otherCollection.UnRegister(100)
				metrics.CleanupQueryNodeCollectionMetrics(paramtable.GetNodeID(), 2)
				expected = map[string]float64{}
				if mode == metrics.CollectionLevelMetricsModeAggregate {
					expected["all/Sealed/L0"] = 0
				}
				require.Equal(t, expected, gatherSegmentCounts(t, registry))
				for _, segment := range []*segments.MockSegment{seg2, seg3, seg4} {
					segment.AssertNumberOfCalls(t, "Release", 1)
				}

				// The same metric family must also keep ordinary segment counts,
				// with state and level remaining independent of the L0 series.
				metrics.QueryNodeNumSegments.WithLabelValues(paramtable.GetStringNodeID(), "1", "Sealed", "L1").Inc()
				metrics.QueryNodeNumSegments.WithLabelValues(paramtable.GetStringNodeID(), "2", "Sealed", "L1").Inc()
				metrics.QueryNodeNumSegments.WithLabelValues(paramtable.GetStringNodeID(), "2", "Growing", "L1").Inc()
				if mode == metrics.CollectionLevelMetricsModeAggregate {
					expected["all/Sealed/L1"] = 2
					expected["all/Growing/L1"] = 1
				} else {
					expected["1/Sealed/L1"] = 1
					expected["2/Sealed/L1"] = 1
					expected["2/Growing/L1"] = 1
				}
				require.Equal(t, expected, gatherSegmentCounts(t, registry))
				mixedSegment := newL0MetricsTestSegment(t, 5, 1, 50)
				first.RegisterL0(mixedSegment)
				l0Key := "1/Sealed/L0"
				if mode == metrics.CollectionLevelMetricsModeAggregate {
					l0Key = "all/Sealed/L0"
				}
				expected[l0Key] = 1
				require.Equal(t, expected, gatherSegmentCounts(t, registry))
				first.Clear()
				expected[l0Key] = 0
				require.Equal(t, expected, gatherSegmentCounts(t, registry))
				mixedSegment.AssertNumberOfCalls(t, "Release", 1)
			})
		}
	}
}

func TestL0SegmentMetricsConcurrentCleanup(t *testing.T) {
	for _, kind := range []string{"list", "double cache"} {
		t.Run(kind, func(t *testing.T) {
			registry := setupL0MetricsTest(t, metrics.CollectionLevelMetricsModeAggregate)
			buffer := NewDoubleCacheDeleteBuffer[*Item](0, 1000)
			if kind == "list" {
				buffer = NewListDeleteBuffer[*Item](0, 1000, []string{paramtable.GetStringNodeID(), "v0"})
			}
			segmentList := make([]*segments.MockSegment, 32)
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := range segmentList {
				segmentList[i] = newL0MetricsTestSegment(t, int64(i), 1, 10)
				wg.Add(1)
				go func(segment *segments.MockSegment) {
					defer wg.Done()
					<-start
					buffer.RegisterL0(segment)
				}(segmentList[i])
			}
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				buffer.UnRegister(11)
			}()
			go func() {
				defer wg.Done()
				<-start
				buffer.Clear()
			}()
			close(start)
			wg.Wait()
			require.Equal(t, map[string]float64{"all/Sealed/L0": float64(len(buffer.ListL0()))}, gatherSegmentCounts(t, registry))
			buffer.Clear()
			buffer.Clear()
			require.Equal(t, map[string]float64{"all/Sealed/L0": 0}, gatherSegmentCounts(t, registry))
			for _, segment := range segmentList {
				segment.AssertNumberOfCalls(t, "Release", 1)
			}
		})
	}
}
