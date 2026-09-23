// Copyright 2026 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package packed

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/stage"
)

// ManifestReadOrigin is a bounded instrumentation site, never a request label.
type ManifestReadOrigin uint8

const (
	ManifestReadOther ManifestReadOrigin = iota
	ManifestReadCreate
	ManifestReadPreload
	ManifestReadPostSync
	ManifestReadReopen
	manifestReadOriginCount
)

const (
	manifestReadTotal = iota
	manifestReadProperties
	manifestReadBegin
	manifestReadFilesystem
	manifestReadCacheLookup
	manifestReadOpen
	manifestReadRead
	manifestReadDeserialize
	manifestReadPaths
	manifestReadCacheInsert
	manifestReadGet
	manifestReadExtract
	manifestReadRetryDelay
	manifestReadPhaseCount
)

var manifestReadPhaseNames = [manifestReadPhaseCount]string{
	"total", "properties", "transaction_begin", "filesystem", "cache_lookup",
	"open", "read", "deserialize", "paths", "cache_insert", "get_manifest",
	"extract_stats", "retry_delay_requested",
}

var manifestReadItemNames = [...]string{"cache_hit", "cache_miss", "read_bytes", "s3_503", "s3_retry"}

type manifestReadTiming struct {
	durations [manifestReadPhaseCount]time.Duration
	items     [len(manifestReadItemNames)]uint64
}

var manifestReadMetrics = func() [manifestReadOriginCount]struct {
	durations [manifestReadPhaseCount]*stage.Recorder
	items     [len(manifestReadItemNames)]prometheus.Counter
} {
	var result [manifestReadOriginCount]struct {
		durations [manifestReadPhaseCount]*stage.Recorder
		items     [len(manifestReadItemNames)]prometheus.Counter
	}
	for origin, operation := range [...]string{
		"manifest_other", "manifest_create", "manifest_preload", "manifest_postsync", "manifest_reopen",
	} {
		for phase, name := range manifestReadPhaseNames {
			result[origin].durations[phase] = stage.New("storage", operation, name)
		}
		for item, name := range manifestReadItemNames {
			result[origin].items[item] = metrics.QueryStageItems.WithLabelValues("storage", operation, "transaction_begin", name)
		}
	}
	return result
}()

// Observe once per GetManifestStats attempt with a single final outcome and
// zero for skipped stages. Native retry backoff is requested delay, overlaps
// open/read, and is not an extra disjoint wall-time stage.
func (t *manifestReadTiming) observe(origin ManifestReadOrigin, err error) {
	if origin >= manifestReadOriginCount {
		origin = ManifestReadOther
	}
	result := stage.Outcome(err)
	recorders := &manifestReadMetrics[origin]
	for i, duration := range t.durations {
		recorders.durations[i].Observe(duration, result)
	}
	for i, count := range t.items {
		recorders.items[i].Add(float64(count))
	}
}
