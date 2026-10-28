package idf

import (
	"cmp"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// statsBudget bounds the memory of the BM25 statistics materialized by the
// oracles of a node. Above the limit, the least recently searched oracles drop
// their statistics and rebuild them, like lazy oracles, on their next search.
type statsBudget struct {
	oracles *typeutil.ConcurrentSet[*oracleRuntime]
	// full is set while the last check found the statistics above the limit;
	// new oracles then start lazy instead of loading statistics to drop.
	full atomic.Bool
}

const statsBudgetInterval = time.Second

var getGlobalStatsBudget = sync.OnceValue(func() *statsBudget {
	budget := newStatsBudget()
	go func() {
		ticker := time.NewTicker(statsBudgetInterval)
		defer ticker.Stop()
		for range ticker.C {
			var limit int64
			if ratio := paramtable.Get().QueryViewCfg.IDFStatsMemoryLimitRatio.GetAsFloat(); ratio > 0 {
				limit = int64(ratio * float64(hardware.GetMemoryCount()))
			}
			budget.enforce(limit)
		}
	}()
	return budget
})

func newStatsBudget() *statsBudget {
	return &statsBudget{oracles: typeutil.NewConcurrentSet[*oracleRuntime]()}
}

func (b *statsBudget) isFull() bool {
	return b != nil && b.full.Load()
}

// enforce drops statistics, least recently searched first, until the
// materialized ones fit in the limit. A non-positive limit disables it.
func (b *statsBudget) enforce(limit int64) {
	if limit <= 0 {
		b.full.Store(false)
		return
	}
	type entry struct {
		oracle     *oracleRuntime
		size       int64
		lastSearch int64
	}
	var total int64
	var materialized []entry
	b.oracles.Range(func(r *oracleRuntime) bool {
		if size := r.statsMemSize(); size > 0 {
			materialized = append(materialized, entry{oracle: r, size: size, lastSearch: r.lastSearch.Load()})
			total += size
		}
		return true
	})
	b.full.Store(total > limit)
	if total <= limit {
		return
	}
	slices.SortFunc(materialized, func(x, y entry) int { return cmp.Compare(x.lastSearch, y.lastSearch) })
	for _, e := range materialized {
		if total <= limit {
			return
		}
		if e.oracle.dematerialize() {
			total -= e.size
		}
	}
}
