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

package datacoord

import "context"

// unlimitedCompactionTaskLimit means that the corresponding limit is disabled.
const unlimitedCompactionTaskLimit = -1

type compactionTaskLimitContextKey struct{}

// withCompactionTaskLimit carries a per-trigger limit through the policy path.
// The value is scoped to one trigger invocation and is never persisted.
func withCompactionTaskLimit(ctx context.Context, limit int) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, compactionTaskLimitContextKey{}, limit)
}

func getCompactionTaskLimit(ctx context.Context) int {
	if ctx == nil {
		return unlimitedCompactionTaskLimit
	}
	if limit, ok := ctx.Value(compactionTaskLimitContextKey{}).(int); ok {
		return limit
	}
	return unlimitedCompactionTaskLimit
}

// compactionTaskRemainingProvider is implemented by the real inspector. It is
// optional so lightweight test implementations retain their existing behavior.
type compactionTaskRemainingProvider interface {
	getCompactionTaskRemaining() int
}

// getCompactionTaskBudget calculates a fresh submission limit for one trigger
// invocation. It combines the remaining global active-task capacity with the
// configured per-trigger limit. A negative value means unlimited.
func getCompactionTaskBudget(inspector CompactionInspector) int {
	remaining := unlimitedCompactionTaskLimit
	if provider, ok := inspector.(compactionTaskRemainingProvider); ok {
		remaining = provider.getCompactionTaskRemaining()
	}

	perTrigger := Params.DataCoordCfg.CompactionMaxTaskNumPerTrigger.GetAsInt()
	if perTrigger > 0 && (remaining < 0 || perTrigger < remaining) {
		remaining = perTrigger
	}
	return remaining
}

// getCompactionCandidateLimit bounds the temporary candidate state produced by
// one trigger. It follows both the current active-task budget and the
// per-trigger plan limit, while retaining enough candidates to form one valid
// merge when MinSegmentToMerge is larger than the remaining task budget.
func getCompactionCandidateLimit(ctx context.Context) int {
	limit := getCompactionTaskLimit(ctx)
	perTrigger := Params.DataCoordCfg.CompactionMaxTaskNumPerTrigger.GetAsInt()
	if perTrigger > 0 {
		if limit < 0 || perTrigger < limit {
			limit = perTrigger
		}
	}
	if limit >= 0 {
		minSegments := Params.DataCoordCfg.MinSegmentToMerge.GetAsInt()
		if minSegments > limit {
			return minSegments
		}
	}
	return limit
}

func compactionTaskLimitReached(limit, count int) bool {
	return limit >= 0 && count >= limit
}

// compactionTaskSubmissionLimit returns the number of additional tasks that
// may be attempted by a trigger. The trigger's original snapshot is retained
// as an upper bound, while a fresh inspector budget prevents concurrent
// triggers from overshooting the global running+pending limit.
func compactionTaskSubmissionLimit(snapshot, submitted int, inspector CompactionInspector) int {
	remaining := snapshot
	if remaining >= 0 {
		remaining -= submitted
		if remaining < 0 {
			return 0
		}
	}
	current := getCompactionTaskBudget(inspector)
	if current >= 0 && (remaining < 0 || current < remaining) {
		remaining = current
	}
	return remaining
}
