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

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	pkgmetrics "github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestInitCollectionLevelMetricsMode(t *testing.T) {
	// The native configuration intentionally cannot be reset. Exercise each
	// startup mode in a new process against the real C ABI.
	const envKey = "MILVUS_TEST_CACHE_SHARD_METRICS_MODE"
	if mode := os.Getenv(envKey); mode != "" {
		require.ErrorIs(t, InitCollectionLevelMetricsMode("invalid"), merr.ErrParameterInvalid)
		require.NoError(t, InitCollectionLevelMetricsMode(mode))
		require.Equal(t, mode, pkgmetrics.CollectionLevelMetricsMode())
		require.NoError(t, InitCollectionLevelMetricsMode(" "+strings.ToUpper(mode)+" "))

		other := pkgmetrics.CollectionLevelMetricsModeFull
		if mode == other {
			other = pkgmetrics.CollectionLevelMetricsModeAggregate
		}
		require.ErrorIs(t, InitCollectionLevelMetricsMode(other), merr.ErrServiceInternal)
		require.Equal(t, mode, pkgmetrics.CollectionLevelMetricsMode())
		require.NoError(t, InitCollectionLevelMetricsMode(mode))
		return
	}

	executable, err := os.Executable()
	require.NoError(t, err)
	for _, mode := range []string{pkgmetrics.CollectionLevelMetricsModeFull, pkgmetrics.CollectionLevelMetricsModeAggregate} {
		t.Run(mode, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), executable, "-test.run=^TestInitCollectionLevelMetricsMode$")
			cmd.Env = append(os.Environ(), envKey+"="+mode)
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", output)
		})
	}
}
