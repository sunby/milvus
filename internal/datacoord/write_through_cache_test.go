//go:build test
// +build test

package datacoord

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cloneInt returns the value itself (ints are value types).
func cloneInt(v int) int { return v }

func TestCacheNewCache(t *testing.T) {
	c := NewCache[string, int](cloneInt)
	require.NotNil(t, c)
	assert.Equal(t, 0, c.Len())
}

func TestCacheInsertBasic(t *testing.T) {
	c := NewCache[string, int](cloneInt)

	old, existed := c.Insert("a", 10, 1)
	assert.False(t, existed)
	assert.Equal(t, 0, old)
	assert.Equal(t, 1, c.Len())

	val, ok := c.Lookup("a")
	assert.True(t, ok)
	assert.Equal(t, 10, val)
}

func TestCacheInsertOverwrite(t *testing.T) {
	c := NewCache[string, int](cloneInt)

	c.Insert("a", 10, 1)
	old, existed := c.Insert("a", 20, 2)
	assert.True(t, existed)
	assert.Equal(t, 10, old)
	assert.Equal(t, 1, c.Len())

	val, ok := c.Lookup("a")
	assert.True(t, ok)
	assert.Equal(t, 20, val)
}

func TestCacheInsertStaleRejected(t *testing.T) {
	c := NewCache[string, int](cloneInt)

	c.Insert("a", 10, 5)

	// Same version — rejected (stale).
	old, existed := c.Insert("a", 20, 5)
	assert.True(t, existed)
	assert.Equal(t, 10, old)

	val, _ := c.Lookup("a")
	assert.Equal(t, 10, val, "value must not change on stale write")

	// Lower version — also rejected.
	old, existed = c.Insert("a", 30, 3)
	assert.True(t, existed)
	assert.Equal(t, 10, old)

	val, _ = c.Lookup("a")
	assert.Equal(t, 10, val)
}

func TestCacheInsertOverTombstone(t *testing.T) {
	c := NewCache[string, int](cloneInt)

	c.Insert("a", 10, 1)
	c.Erase("a", 2)
	assert.Equal(t, 0, c.Len())

	// Insert with version > tombstone version succeeds.
	old, existed := c.Insert("a", 20, 3)
	assert.False(t, existed) // tombstone does not count as "existed"
	assert.Equal(t, 0, old)
	assert.Equal(t, 1, c.Len())

	val, ok := c.Lookup("a")
	assert.True(t, ok)
	assert.Equal(t, 20, val)
}

func TestCacheInsertStaleTombstoneRejected(t *testing.T) {
	c := NewCache[string, int](cloneInt)

	c.Insert("a", 10, 1)
	c.Erase("a", 5)

	// Insert with version <= tombstone version is rejected.
	old, existed := c.Insert("a", 99, 4)
	assert.False(t, existed)
	assert.Equal(t, 0, old)

	_, ok := c.Lookup("a")
	assert.False(t, ok, "stale insert after tombstone must be rejected")
}

func TestCacheLookupNotFound(t *testing.T) {
	c := NewCache[string, int](cloneInt)
	val, ok := c.Lookup("missing")
	assert.False(t, ok)
	assert.Equal(t, 0, val)
}

func TestCacheLookupTombstone(t *testing.T) {
	c := NewCache[string, int](cloneInt)
	c.Insert("a", 10, 1)
	c.Erase("a", 2)

	val, ok := c.Lookup("a")
	assert.False(t, ok)
	assert.Equal(t, 0, val)
}

func TestCacheUpdateBasic(t *testing.T) {
	c := NewCache[string, int](cloneInt)
	c.Insert("a", 10, 1)

	old, existed := c.Update("a", func(v int) bool {
		// The cloned value is passed; we cannot mutate int, but the fn returns true.
		return true
	}, 2)
	assert.True(t, existed)
	assert.Equal(t, 10, old)
}

func TestCacheUpdateFnAbort(t *testing.T) {
	c := NewCache[string, int](cloneInt)
	c.Insert("a", 10, 1)

	old, existed := c.Update("a", func(v int) bool {
		return false // abort
	}, 2)
	assert.True(t, existed)
	assert.Equal(t, 10, old)

	// Value should remain unchanged.
	val, _ := c.Lookup("a")
	assert.Equal(t, 10, val)
}

func TestCacheUpdateVersionZeroKeepsCurrent(t *testing.T) {
	// Use a pointer type so the update fn can actually mutate.
	type box struct{ v int }
	c := NewCache[string, *box](func(b *box) *box { return &box{v: b.v} })

	c.Insert("a", &box{v: 10}, 5)

	old, existed := c.Update("a", func(b *box) bool {
		b.v = 20
		return true
	}, 0) // version 0 means keep current version
	assert.True(t, existed)
	assert.Equal(t, 10, old.v)

	val, _ := c.Lookup("a")
	assert.Equal(t, 20, val.v)

	// A subsequent update with version <= 5 should still be stale-rejected,
	// proving the internal version was retained at 5.
	_, existed = c.Update("a", func(b *box) bool {
		b.v = 99
		return true
	}, 5)
	assert.True(t, existed) // key exists, but update is stale

	val, _ = c.Lookup("a")
	assert.Equal(t, 20, val.v, "stale update must not change value")
}

func TestCacheUpdateStaleRejected(t *testing.T) {
	c := NewCache[string, int](cloneInt)
	c.Insert("a", 10, 5)

	old, existed := c.Update("a", func(v int) bool { return true }, 3)
	assert.True(t, existed)
	assert.Equal(t, 10, old)
}

func TestCacheUpdateKeyNotFound(t *testing.T) {
	c := NewCache[string, int](cloneInt)

	old, existed := c.Update("missing", func(v int) bool { return true }, 1)
	assert.False(t, existed)
	assert.Equal(t, 0, old)
}

func TestCacheUpdateOnTombstone(t *testing.T) {
	c := NewCache[string, int](cloneInt)
	c.Insert("a", 10, 1)
	c.Erase("a", 2)

	old, existed := c.Update("a", func(v int) bool { return true }, 3)
	assert.False(t, existed)
	assert.Equal(t, 0, old)
}

func TestCacheEraseBasic(t *testing.T) {
	c := NewCache[string, int](cloneInt)
	c.Insert("a", 10, 1)
	c.Insert("b", 20, 1)
	assert.Equal(t, 2, c.Len())

	old := c.Erase("a", 2)
	assert.Equal(t, 10, old)
	assert.Equal(t, 1, c.Len())

	_, ok := c.Lookup("a")
	assert.False(t, ok)
}

func TestCacheEraseNoop(t *testing.T) {
	c := NewCache[string, int](cloneInt)

	// Erase on non-existent key is a noop.
	old := c.Erase("missing", 1)
	assert.Equal(t, 0, old)
	assert.Equal(t, 0, c.Len())
}

func TestCacheEraseDouble(t *testing.T) {
	c := NewCache[string, int](cloneInt)
	c.Insert("a", 10, 1)
	c.Erase("a", 2)
	assert.Equal(t, 0, c.Len())

	// Second erase on already-tombstoned key is a noop.
	old := c.Erase("a", 3)
	assert.Equal(t, 0, old)
	assert.Equal(t, 0, c.Len())
}

func TestCachePrune(t *testing.T) {
	c := NewCache[string, int](cloneInt)
	c.Insert("a", 10, 1)
	c.Erase("a", 2)

	// Tombstone exists but is invisible.
	_, ok := c.Lookup("a")
	assert.False(t, ok)

	c.Prune("a")

	// After prune, insert with a lower version succeeds because the tombstone is gone.
	_, existed := c.Insert("a", 99, 1)
	assert.False(t, existed)
	assert.Equal(t, 1, c.Len())
}

func TestCachePruneNonTombstone(t *testing.T) {
	c := NewCache[string, int](cloneInt)
	c.Insert("a", 10, 1)

	// Prune on a live entry does nothing.
	c.Prune("a")
	val, ok := c.Lookup("a")
	assert.True(t, ok)
	assert.Equal(t, 10, val)
}

func TestCacheRange(t *testing.T) {
	c := NewCache[string, int](cloneInt)
	c.Insert("a", 1, 1)
	c.Insert("b", 2, 1)
	c.Insert("c", 3, 1)
	c.Erase("b", 2)

	collected := map[string]int{}
	c.Range(func(k string, v int) bool {
		collected[k] = v
		return true
	})
	assert.Len(t, collected, 2)
	assert.Equal(t, 1, collected["a"])
	assert.Equal(t, 3, collected["c"])
	_, hasTombstone := collected["b"]
	assert.False(t, hasTombstone, "Range must skip tombstones")
}

func TestCacheRangeEarlyStop(t *testing.T) {
	c := NewCache[string, int](cloneInt)
	c.Insert("a", 1, 1)
	c.Insert("b", 2, 1)
	c.Insert("c", 3, 1)

	count := 0
	c.Range(func(k string, v int) bool {
		count++
		return false // stop after first
	})
	assert.Equal(t, 1, count)
}

func TestCacheLen(t *testing.T) {
	c := NewCache[string, int](cloneInt)
	assert.Equal(t, 0, c.Len())

	c.Insert("a", 1, 1)
	assert.Equal(t, 1, c.Len())

	c.Insert("b", 2, 1)
	assert.Equal(t, 2, c.Len())

	// Overwrite does not change Len.
	c.Insert("a", 10, 2)
	assert.Equal(t, 2, c.Len())

	c.Erase("a", 3)
	assert.Equal(t, 1, c.Len())

	c.Erase("b", 3)
	assert.Equal(t, 0, c.Len())
}

func TestCacheConcurrentAccess(t *testing.T) {
	c := NewCache[int, int](cloneInt)
	const numKeys = 100
	const numGoroutines = 10

	var wg sync.WaitGroup

	// Concurrent inserts.
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for k := 0; k < numKeys; k++ {
				version := int64(gid*numKeys + k + 1)
				c.Insert(k, gid*1000+k, version)
			}
		}(g)
	}
	wg.Wait()

	assert.Equal(t, numKeys, c.Len())

	// Concurrent updates.
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < numKeys; k++ {
				c.Update(k, func(v int) bool { return true }, 0)
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, numKeys, c.Len())

	// Concurrent erases.
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for k := 0; k < numKeys; k++ {
				version := int64(10000 + gid*numKeys + k)
				c.Erase(k, version)
			}
		}(g)
	}
	wg.Wait()

	assert.Equal(t, 0, c.Len())
}

func TestCacheConcurrentInsertAndLookup(t *testing.T) {
	c := NewCache[int, int](cloneInt)
	const N = 200
	var wg sync.WaitGroup

	// Writers
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < N; i++ {
				c.Insert(i, gid*1000+i, int64(gid*N+i+1))
			}
		}(g)
	}

	// Readers
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < N; i++ {
				c.Lookup(i)
			}
		}()
	}

	wg.Wait()
	// Just verify no panic / data race.
	assert.True(t, c.Len() > 0)
}
