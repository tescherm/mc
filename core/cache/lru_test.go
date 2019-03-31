package cache

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const kvSize = 4 + 5

var value = []byte("value")

const (
	nKvSize      = 12 + 5 + 8 + 12 // namespace + ":key:" + keyspace + value
	nWorkers     = 10
	nPrivateKeys = 10000
	nSharedKeys  = 1000
	nRepeat      = 4
	nCacheSize   = nPrivateKeys*nWorkers + nSharedKeys
)

func set(c Cache, key string, val Value) {
	c.Set(key, val)
}

func remove(c Cache, key string) Value {
	return c.Remove(key)
}

func checkHit(t *testing.T, c Cache, key string, val Value) {
	i := c.Get(key)

	require.NotNil(t, i)
	require.EqualValues(t, val, i)
}

func checkMiss(t *testing.T, c Cache, key string) {
	i := c.Get(key)
	require.Nil(t, i)
}

func checkSize(t *testing.T, c Cache, size int) {
	require.EqualValues(t, size, c.Size())
}

func checkHitInRange(t *testing.T, c Cache, key string, vals [][]byte) {
	i := c.Get(key)
	require.NotNil(t, i)

	for _, val := range vals {
		if assert.ObjectsAreEqualValues(val, i) {
			return
		}
	}
	require.Fail(t, "Corrupted value in cache: %v\n", i)
}

func TestCacheSetGet(t *testing.T) {
	t.Parallel()

	cache := NewLRUCache(Config{Capacity: 100000})
	set(cache, "key1", value)
	checkHit(t, cache, "key1", value)

	stats := cache.Stats()
	require.EqualValues(t, 1, stats.Sets)
	require.EqualValues(t, 1, stats.Hits)
	require.EqualValues(t, 0, stats.Misses)
	require.EqualValues(t, 0, stats.Evicts)
	require.EqualValues(t, 0, stats.Removes)
	require.EqualValues(t, 0, stats.Clears)

	require.EqualValues(t, 1*kvSize, stats.CurrentCapacity)
	checkSize(t, cache, 1)
}

func TestCacheGetMiss(t *testing.T) {
	t.Parallel()

	cache := NewLRUCache(Config{Capacity: 100000})
	set(cache, "key1", value)
	checkMiss(t, cache, "key2")

	stats := cache.Stats()
	require.EqualValues(t, 1, stats.Sets)
	require.EqualValues(t, 0, stats.Hits)
	require.EqualValues(t, 1, stats.Misses)
	require.EqualValues(t, 0, stats.Evicts)
	require.EqualValues(t, 0, stats.Removes)
	require.EqualValues(t, 0, stats.Clears)

	require.EqualValues(t, 1*kvSize, stats.CurrentCapacity)
	require.EqualValues(t, 1, cache.Size())
}

func TestCacheSetGetMany(t *testing.T) {
	t.Parallel()

	cache := NewLRUCache(Config{Capacity: 100000})

	set(cache, "key1", value)
	set(cache, "key2", value)
	set(cache, "key3", value)

	checkHit(t, cache, "key1", value)
	checkHit(t, cache, "key2", value)
	checkHit(t, cache, "key3", value)
	checkMiss(t, cache, "key4")

	stats := cache.Stats()
	require.EqualValues(t, 3, stats.Sets)
	require.EqualValues(t, 3, stats.Hits)
	require.EqualValues(t, 1, stats.Misses)
	require.EqualValues(t, 0, stats.Evicts)
	require.EqualValues(t, 0, stats.Removes)
	require.EqualValues(t, 0, stats.Clears)

	require.EqualValues(t, 3*kvSize, stats.CurrentCapacity)
	checkSize(t, cache, 3)
}

func TestCacheRemove(t *testing.T) {
	t.Parallel()

	cache := NewLRUCache(Config{Capacity: 100000})
	set(cache, "key", value)
	checkHit(t, cache, "key", value)
	checkSize(t, cache, 1)
	removed := remove(cache, "key")
	require.NotNil(t, removed)
	checkMiss(t, cache, "key")

	stats := cache.Stats()
	require.EqualValues(t, 1, stats.Sets)
	require.EqualValues(t, 1, stats.Hits)
	require.EqualValues(t, 1, stats.Misses)
	require.EqualValues(t, 0, stats.Evicts)
	require.EqualValues(t, 1, stats.Removes)
	require.EqualValues(t, 0, stats.Clears)

	require.EqualValues(t, 0, stats.CurrentCapacity)
	checkSize(t, cache, 0)
}

func BenchmarkCacheRemove(b *testing.B) {
	cache := NewLRUCache(Config{Capacity: 100000})

	for n := 0; n < b.N; n++ {
		set(cache, "key", value)
		cache.Get("key")
		remove(cache, "key")
		cache.Get("key")
	}
}

func TestCacheLRU(t *testing.T) {
	t.Parallel()

	cache := NewLRUCache(Config{Capacity: 1 * kvSize})
	set(cache, "key1", value)
	checkSize(t, cache, 1)
	set(cache, "key2", value)
	checkMiss(t, cache, "key1")
	checkHit(t, cache, "key2", value)

	stats := cache.Stats()
	require.EqualValues(t, 2, stats.Sets)
	require.EqualValues(t, 1, stats.Hits)
	require.EqualValues(t, 1, stats.Misses)
	require.EqualValues(t, 1, stats.Evicts)
	require.EqualValues(t, 0, stats.Removes)
	require.EqualValues(t, 0, stats.Clears)

	require.EqualValues(t, 1*kvSize, stats.CurrentCapacity)
	checkSize(t, cache, 1)
}

func BenchmarkCacheLRU(b *testing.B) {
	cache := NewLRUCache(Config{Capacity: 1 * kvSize})

	for n := 0; n < b.N; n++ {
		set(cache, "key1", value)
		cache.Get("key1")
		set(cache, "key2", value)
		cache.Get("key1")
	}
}

func TestCacheLRUMany(t *testing.T) {
	t.Parallel()

	cache := NewLRUCache(Config{Capacity: 3 * kvSize})

	set(cache, "key1", value)
	set(cache, "key2", value)
	set(cache, "key3", value)
	set(cache, "key4", value)
	set(cache, "key5", value)

	checkMiss(t, cache, "key1")
	checkMiss(t, cache, "key2")
	checkHit(t, cache, "key3", value)
	checkHit(t, cache, "key4", value)
	checkHit(t, cache, "key5", value)
	checkMiss(t, cache, "key6")

	stats := cache.Stats()
	require.EqualValues(t, 5, stats.Sets)
	require.EqualValues(t, 3, stats.Hits)
	require.EqualValues(t, 3, stats.Misses)
	require.EqualValues(t, 2, stats.Evicts)
	require.EqualValues(t, 0, stats.Removes)
	require.EqualValues(t, 0, stats.Clears)

	require.EqualValues(t, 3*kvSize, stats.CurrentCapacity)
	checkSize(t, cache, 3)
}

func TestCacheLRUUpdates(t *testing.T) {
	t.Parallel()

	cache := NewLRUCache(Config{Capacity: 3 * kvSize})

	set(cache, "key1", value)
	set(cache, "key2", value)
	set(cache, "key3", value)
	checkSize(t, cache, 3)
	checkHit(t, cache, "key1", value)
	set(cache, "key4", value)
	set(cache, "key5", value)

	checkHit(t, cache, "key1", value)
	checkMiss(t, cache, "key2")
	checkMiss(t, cache, "key3")
	checkHit(t, cache, "key4", value)
	checkHit(t, cache, "key5", value)

	stats := cache.Stats()
	require.EqualValues(t, 5, stats.Sets)
	require.EqualValues(t, 4, stats.Hits)
	require.EqualValues(t, 2, stats.Misses)
	require.EqualValues(t, 2, stats.Evicts)
	require.EqualValues(t, 0, stats.Removes)
	require.EqualValues(t, 0, stats.Clears)

	require.EqualValues(t, 3*kvSize, stats.CurrentCapacity)
	checkSize(t, cache, 3)
}

func TestCacheClear(t *testing.T) {
	t.Parallel()

	cache := NewLRUCache(Config{Capacity: 100000})
	set(cache, "key", value)
	checkSize(t, cache, 1)
	checkHit(t, cache, "key", value)

	cache.Clear()

	stats := cache.Stats()
	require.EqualValues(t, 1, stats.Sets)
	require.EqualValues(t, 1, stats.Hits)
	require.EqualValues(t, 0, stats.Misses)
	require.EqualValues(t, 0, stats.Evicts)
	require.EqualValues(t, 0, stats.Removes)
	require.EqualValues(t, 1, stats.Clears)

	require.EqualValues(t, 0, stats.CurrentCapacity)
	checkSize(t, cache, 0)
}

func BenchmarkCacheClear(b *testing.B) {
	cache := NewLRUCache(Config{Capacity: 100000})

	for n := 0; n < b.N; n++ {
		set(cache, "key", value)
		cache.Get("key")

		cache.Clear()
	}
}

func TestCacheConcurrency(t *testing.T) {
	t.Parallel()

	cache := NewLRUCache(Config{Capacity: nCacheSize * nKvSize})

	var namespaces [][]byte
	for i := 0; i < nWorkers; i++ {
		name := []byte(fmt.Sprintf("TestWorker%02d", i))
		namespaces = append(namespaces, name)
	}

	for i := uint(0); i < nWorkers; i++ {
		id := i
		t.Run(string(namespaces[id]), func(t *testing.T) {
			t.Parallel()
			concurrencyWorker(t, cache, namespaces, id)
		})
	}
}

func concurrencyWorker(t *testing.T, cache *LRUCache, namespaces [][]byte, id uint) {
	var wg sync.WaitGroup
	namespace := namespaces[id]

	// run queries over private keys
	wg.Add(1)
	go func() {
		for j := 0; j < nRepeat; j++ {
			for i := 0; i < nPrivateKeys; i++ {
				key := fmt.Sprintf("%s:key:%08d", namespace, i)
				set(cache, key, namespace)
				checkHit(t, cache, key, namespace)
			}
			for i := 0; i < nPrivateKeys; i++ {
				key := fmt.Sprintf("%s:key:%08d", namespace, i)
				checkHit(t, cache, key, namespace)
			}
		}
		wg.Done()
	}()

	// run queries over shared keys
	wg.Add(1)
	go func() {
		for j := 0; j < nRepeat; j++ {
			for i := 0; i < nSharedKeys; i++ {
				key := fmt.Sprintf("%s:key:%08d", "SharedSpace0", i)
				set(cache, key, []byte(namespace))
			}
			for i := 0; i < nSharedKeys; i++ {
				key := fmt.Sprintf("%s:key:%08d", "SharedSpace0", i)
				checkHitInRange(t, cache, key, namespaces)
			}
		}
		wg.Done()
	}()

	wg.Wait()
}
