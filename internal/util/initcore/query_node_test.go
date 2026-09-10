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

package initcore

import (
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	internalmetrics "github.com/milvus-io/milvus/internal/util/metrics"
	pkgmetrics "github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestInitQueryNodeRejectsConflictingMetricsMode(t *testing.T) {
	const envKey = "MILVUS_TEST_QUERY_NODE_METRICS_MODE"
	if os.Getenv(envKey) == "1" {
		paramtable.Init()
		require.NoError(t, internalmetrics.InitCollectionLevelMetricsMode(pkgmetrics.CollectionLevelMetricsModeAggregate))
		require.NoError(t, paramtable.Get().Save(
			paramtable.Get().CommonCfg.CollectionLevelMetricsMode.Key,
			pkgmetrics.CollectionLevelMetricsModeFull,
		))
		// The direct initialization path must fail before native initialization
		// and must not silently succeed when called again.
		for i := 0; i < 2; i++ {
			require.ErrorIs(t, InitQueryNode(t.Context()), merr.ErrServiceInternal)
			require.Equal(t, pkgmetrics.CollectionLevelMetricsModeAggregate, pkgmetrics.CollectionLevelMetricsMode())
		}
		return
	}

	executable, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.CommandContext(t.Context(), executable, "-test.run=^TestInitQueryNodeRejectsConflictingMetricsMode$") //nolint:gosec // Re-exec the current test binary returned by os.Executable.
	cmd.Env = append(os.Environ(), envKey+"=1")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
}
