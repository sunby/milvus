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

package segment

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommitL1LimiterDoesNotBlockSchedulerWorkers(t *testing.T) {
	limiter := newCommitL1Limiter(1)
	release, ok := limiter.TryAcquire()
	require.True(t, ok)

	_, ok = limiter.TryAcquire()
	assert.False(t, ok)

	release()
	release, ok = limiter.TryAcquire()
	require.True(t, ok)
	release()
}

func TestCommitL1LimiterCanBeDisabled(t *testing.T) {
	limiter := newCommitL1Limiter(1)
	limiter.UpdateConcurrency(0)

	release1, ok := limiter.TryAcquire()
	require.True(t, ok)
	release2, ok := limiter.TryAcquire()
	require.True(t, ok)
	release1()
	release2()
}
