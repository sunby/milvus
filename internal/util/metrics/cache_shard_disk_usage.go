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

package metrics

/*
#cgo pkg-config: milvus_core

#include "monitor/monitor_c.h"
*/
import "C"

import (
	"strings"
	"unsafe"

	_ "github.com/milvus-io/milvus/internal/util/cgo"
	pkgmetrics "github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// InitCollectionLevelMetricsMode configures both Go and native metrics before
// components create cache slots. The native mode is immutable once configured
// or an attributed cache slot exists; repeated initialization must agree.
func InitCollectionLevelMetricsMode(mode string) error {
	normalized := strings.ToLower(strings.TrimSpace(mode))
	switch normalized {
	case pkgmetrics.CollectionLevelMetricsModeFull, pkgmetrics.CollectionLevelMetricsModeAggregate:
	default:
		return merr.WrapErrParameterInvalidMsg(
			"common.metrics.collectionLevelMode must be %q or %q, got %q",
			pkgmetrics.CollectionLevelMetricsModeFull,
			pkgmetrics.CollectionLevelMetricsModeAggregate,
			mode,
		)
	}

	aggregate := normalized == pkgmetrics.CollectionLevelMetricsModeAggregate
	if !bool(C.InitCacheShardDiskUsageMetricsMode(C.bool(aggregate))) {
		return merr.WrapErrServiceInternalMsg(
			"cannot initialize common.metrics.collectionLevelMode=%q: native cache metrics were already initialized with a different mode",
			normalized,
		)
	}
	// Do not change Go metric writers if native initialization was rejected.
	pkgmetrics.SetCollectionLevelMetricsMode(normalized)
	return nil
}

type CacheShardDiskUsageStats struct {
	DataType  string
	Shard     string
	DiskBytes float64
}

func goString(value *C.char) string {
	if value == nil {
		return ""
	}
	return C.GoString(value)
}

func GetCacheShardDiskUsageStats() []CacheShardDiskUsageStats {
	cStats := C.GetCacheShardDiskUsageStats()
	defer C.DeleteCacheShardDiskUsageStats(cStats)

	if cStats.len <= 0 || cStats.stats == nil {
		return nil
	}

	stats := unsafe.Slice(cStats.stats, int(cStats.len))
	result := make([]CacheShardDiskUsageStats, 0, len(stats))
	for _, stat := range stats {
		result = append(result, CacheShardDiskUsageStats{
			DataType:  goString(stat.data_type),
			Shard:     goString(stat.shard),
			DiskBytes: float64(stat.disk_bytes),
		})
	}
	return result
}
