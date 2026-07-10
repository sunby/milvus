package flusherimpl

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

const (
	flusherStateInRecovering flusherState = "in_recovery"
	flusherStateInWorking    flusherState = "working"
	flusherStateOnClosing    flusherState = "closing"
)

type flusherState = string

func newFlusherMetrics(pchannel types.PChannelInfo) *flusherMetrics {
	constLabels := prometheus.Labels{
		metrics.NodeIDLabelName:         paramtable.GetStringNodeID(),
		metrics.WALChannelLabelName:     pchannel.Name,
		metrics.WALChannelTermLabelName: strconv.FormatInt(pchannel.Term, 10),
	}
	m := &flusherMetrics{
		constLabels:                 constLabels,
		info:                        metrics.WALFlusherInfo.MustCurryWith(constLabels),
		timetick:                    metrics.WALFlusherTimeTick.With(constLabels),
		dropCollectionStageDuration: metrics.WALFlusherDropCollectionStageDurationSeconds.MustCurryWith(constLabels),
		state:                       flusherStateInRecovering,
	}
	m.info.WithLabelValues(flusherStateInRecovering).Set(1)
	return m
}

type flusherMetrics struct {
	constLabels                 prometheus.Labels
	info                        *prometheus.GaugeVec
	timetick                    prometheus.Gauge
	dropCollectionStageDuration prometheus.ObserverVec
	state                       flusherState
}

func (m *flusherMetrics) IntoState(state flusherState) {
	metrics.WALFlusherInfo.DeletePartialMatch(m.constLabels)
	m.state = state
	m.info.WithLabelValues(m.state).Set(1)
}

func (m *flusherMetrics) ObserveMetrics(tickTime uint64) {
	m.timetick.Set(tsoutil.PhysicalTimeSeconds(tickTime))
}

func (m *flusherMetrics) ObserveDropCollectionStage(stage string, duration time.Duration) {
	m.dropCollectionStageDuration.WithLabelValues(stage).Observe(duration.Seconds())
}

func (m *flusherMetrics) Close() {
	metrics.WALFlusherInfo.DeletePartialMatch(m.constLabels)
	metrics.WALFlusherTimeTick.DeletePartialMatch(m.constLabels)
	metrics.WALFlusherDropCollectionStageDurationSeconds.DeletePartialMatch(m.constLabels)
}
