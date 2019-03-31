package caches

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

func set(c *Caches, key string, val []byte) {
	keyCache := c.CacheForKey(key)
	keyCache.Set(key, val)
}

func remove(c *Caches, key string) []byte {
	keyCache := c.CacheForKey(key)
	return keyCache.Remove(key)
}

func checkHit(t *testing.T, c *Caches, key string, val []byte) {
	keyCache := c.CacheForKey(key)
	i := keyCache.Get(key)

	require.NotNil(t, i)
	require.EqualValues(t, val, i)
}

func checkMiss(t *testing.T, c *Caches, key string) {
	keyCache := c.CacheForKey(key)
	i := keyCache.Get(key)
	require.Nil(t, i)
}

func checkSize(t *testing.T, c *Caches, size int) {
	require.EqualValues(t, size, c.Size())
}

func checkHitInRange(t *testing.T, c *Caches, key string, vals [][]byte) {
	keyCache := c.CacheForKey(key)
	i := keyCache.Get(key)
	require.NotNil(t, i)

	for _, val := range vals {
		if assert.ObjectsAreEqualValues(val, i) {
			return
		}
	}
	require.Fail(t, "Corrupted value in cache: %v\n", i)
}

func TestCachesSetGet(t *testing.T) {
	t.Parallel()

	caches := New(Config{
		CacheCount: 5,
		Capacity:   100000,
		Replicas:   160,
	})

	set(caches, "key1", value)
	checkHit(t, caches, "key1", value)

	stats := caches.Stats()
	require.EqualValues(t, 1, stats.Sets)
	require.EqualValues(t, 1, stats.Hits)
	require.EqualValues(t, 0, stats.Misses)
	require.EqualValues(t, 0, stats.Evicts)
	require.EqualValues(t, 0, stats.Removes)
	require.EqualValues(t, 0, stats.Clears)

	require.EqualValues(t, 1*kvSize, stats.CurrentCapacity)
	checkSize(t, caches, 1)
}

func TestCachesGetMiss(t *testing.T) {
	t.Parallel()

	caches := New(Config{
		CacheCount: 5,
		Capacity:   100000,
		Replicas:   160,
	})

	set(caches, "key1", value)
	checkMiss(t, caches, "key2")

	stats := caches.Stats()
	require.EqualValues(t, 1, stats.Sets)
	require.EqualValues(t, 0, stats.Hits)
	require.EqualValues(t, 1, stats.Misses)
	require.EqualValues(t, 0, stats.Evicts)
	require.EqualValues(t, 0, stats.Removes)
	require.EqualValues(t, 0, stats.Clears)

	require.EqualValues(t, 1*kvSize, stats.CurrentCapacity)
	require.EqualValues(t, 1, caches.Size())
}

func TestCachesSetGetMany(t *testing.T) {
	t.Parallel()

	caches := New(Config{
		CacheCount: 5,
		Capacity:   100000,
		Replicas:   160,
	})

	set(caches, "key1", value)
	set(caches, "key2", value)
	set(caches, "key3", value)

	checkHit(t, caches, "key1", value)
	checkHit(t, caches, "key2", value)
	checkHit(t, caches, "key3", value)
	checkMiss(t, caches, "key4")

	stats := caches.Stats()
	require.EqualValues(t, 3, stats.Sets)
	require.EqualValues(t, 3, stats.Hits)
	require.EqualValues(t, 1, stats.Misses)
	require.EqualValues(t, 0, stats.Evicts)
	require.EqualValues(t, 0, stats.Removes)
	require.EqualValues(t, 0, stats.Clears)

	require.EqualValues(t, 3*kvSize, stats.CurrentCapacity)
	checkSize(t, caches, 3)
}

func TestCachesRemove(t *testing.T) {
	t.Parallel()

	caches := New(Config{
		CacheCount: 5,
		Capacity:   100000,
		Replicas:   160,
	})

	set(caches, "key", value)
	checkHit(t, caches, "key", value)
	checkSize(t, caches, 1)
	removed := remove(caches, "key")
	require.NotNil(t, removed)
	checkMiss(t, caches, "key")

	stats := caches.Stats()
	require.EqualValues(t, 1, stats.Sets)
	require.EqualValues(t, 1, stats.Hits)
	require.EqualValues(t, 1, stats.Misses)
	require.EqualValues(t, 0, stats.Evicts)
	require.EqualValues(t, 1, stats.Removes)
	require.EqualValues(t, 0, stats.Clears)

	require.EqualValues(t, 0, stats.CurrentCapacity)
	checkSize(t, caches, 0)
}

func BenchmarkCachesRemove(b *testing.B) {
	caches := New(Config{
		CacheCount: 5,
		Capacity:   100000,
		Replicas:   160,
	})

	for n := 0; n < b.N; n++ {
		set(caches, "key", value)
		caches.CacheForKey("key")
		remove(caches, "key")
		caches.CacheForKey("key")
	}
}

func TestCachesLRU(t *testing.T) {
	t.Parallel()

	cacheCount := 1

	caches := New(Config{
		CacheCount: cacheCount,
		Capacity:   uint64(cacheCount * (1 * kvSize)),
		Replicas:   160,
	})

	set(caches, "key1", value)
	checkSize(t, caches, 1)
	set(caches, "key2", value)
	checkMiss(t, caches, "key1")
	checkHit(t, caches, "key2", value)

	stats := caches.Stats()
	require.EqualValues(t, 2, stats.Sets)
	require.EqualValues(t, 1, stats.Hits)
	require.EqualValues(t, 1, stats.Misses)
	require.EqualValues(t, 1, stats.Evicts)
	require.EqualValues(t, 0, stats.Removes)
	require.EqualValues(t, 0, stats.Clears)

	require.EqualValues(t, 1*kvSize, stats.CurrentCapacity)
	checkSize(t, caches, 1)
}

func BenchmarkCacheLRU(b *testing.B) {
	cacheCount := 1

	caches := New(Config{
		CacheCount: cacheCount,
		Capacity:   uint64(cacheCount * (1 * kvSize)),
		Replicas:   160,
	})

	for n := 0; n < b.N; n++ {
		set(caches, "key1", value)
		caches.CacheForKey("key1")
		set(caches, "key2", value)
		caches.CacheForKey("key1")
	}
}

func TestCachesLRUMany(t *testing.T) {
	t.Parallel()

	cacheCount := 1

	caches := New(Config{
		CacheCount: cacheCount,
		Capacity:   uint64(cacheCount * (3 * kvSize)),
		Replicas:   160,
	})

	set(caches, "key1", value)
	set(caches, "key2", value)
	set(caches, "key3", value)
	set(caches, "key4", value)
	set(caches, "key5", value)

	checkMiss(t, caches, "key1")
	checkMiss(t, caches, "key2")
	checkHit(t, caches, "key3", value)
	checkHit(t, caches, "key4", value)
	checkHit(t, caches, "key5", value)
	checkMiss(t, caches, "key6")

	stats := caches.Stats()
	require.EqualValues(t, 5, stats.Sets)
	require.EqualValues(t, 3, stats.Hits)
	require.EqualValues(t, 3, stats.Misses)
	require.EqualValues(t, 2, stats.Evicts)
	require.EqualValues(t, 0, stats.Removes)
	require.EqualValues(t, 0, stats.Clears)

	require.EqualValues(t, 3*kvSize, stats.CurrentCapacity)
	checkSize(t, caches, 3)
}

func TestCachesLRUUpdates(t *testing.T) {
	t.Parallel()

	cacheCount := 1

	caches := New(Config{
		CacheCount: cacheCount,
		Capacity:   uint64(cacheCount * (3 * kvSize)),
		Replicas:   160,
	})

	set(caches, "key1", value)
	set(caches, "key2", value)
	set(caches, "key3", value)
	checkSize(t, caches, 3)
	checkHit(t, caches, "key1", value)
	set(caches, "key4", value)
	set(caches, "key5", value)

	checkHit(t, caches, "key1", value)
	checkMiss(t, caches, "key2")
	checkMiss(t, caches, "key3")
	checkHit(t, caches, "key4", value)
	checkHit(t, caches, "key5", value)

	stats := caches.Stats()
	require.EqualValues(t, 5, stats.Sets)
	require.EqualValues(t, 4, stats.Hits)
	require.EqualValues(t, 2, stats.Misses)
	require.EqualValues(t, 2, stats.Evicts)
	require.EqualValues(t, 0, stats.Removes)
	require.EqualValues(t, 0, stats.Clears)

	require.EqualValues(t, 3*kvSize, stats.CurrentCapacity)
	checkSize(t, caches, 3)
}

func TestCachesClear(t *testing.T) {
	t.Parallel()

	caches := New(Config{
		CacheCount: 5,
		Capacity:   100000,
		Replicas:   160,
	})

	set(caches, "key", value)
	checkSize(t, caches, 1)
	checkHit(t, caches, "key", value)

	caches.Clear()

	stats := caches.Stats()
	require.EqualValues(t, 1, stats.Sets)
	require.EqualValues(t, 1, stats.Hits)
	require.EqualValues(t, 0, stats.Misses)
	require.EqualValues(t, 0, stats.Evicts)
	require.EqualValues(t, 0, stats.Removes)
	require.EqualValues(t, 5, stats.Clears)

	require.EqualValues(t, 0, stats.CurrentCapacity)
	checkSize(t, caches, 0)
}

func BenchmarkCachesClear(b *testing.B) {
	caches := New(Config{
		CacheCount: 5,
		Capacity:   100000,
		Replicas:   160,
	})

	for n := 0; n < b.N; n++ {
		set(caches, "key", value)
		caches.CacheForKey("key")

		caches.Clear()
	}
}

func TestCachesConcurrency(t *testing.T) {
	t.Parallel()

	cacheCount := 10

	caches := New(Config{
		CacheCount: cacheCount,
		Capacity:   uint64(cacheCount * (nCacheSize * nKvSize)),
		Replicas:   160,
	})

	var namespaces [][]byte
	for i := 0; i < nWorkers; i++ {
		name := []byte(fmt.Sprintf("TestWorker%02d", i))
		namespaces = append(namespaces, name)
	}

	for i := uint(0); i < nWorkers; i++ {
		id := i
		t.Run(string(namespaces[id]), func(t *testing.T) {
			t.Parallel()
			concurrencyWorker(t, caches, namespaces, id)
		})
	}
}

func concurrencyWorker(t *testing.T, caches *Caches, namespaces [][]byte, id uint) {
	var wg sync.WaitGroup
	namespace := namespaces[id]

	// run queries over private keys
	wg.Add(1)
	go func() {
		for j := 0; j < nRepeat; j++ {
			for i := 0; i < nPrivateKeys; i++ {
				key := fmt.Sprintf("%s:key:%08d", namespace, i)
				set(caches, key, namespace)
				checkHit(t, caches, key, namespace)
			}
			for i := 0; i < nPrivateKeys; i++ {
				key := fmt.Sprintf("%s:key:%08d", namespace, i)
				checkHit(t, caches, key, namespace)
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
				set(caches, key, []byte(namespace))
			}
			for i := 0; i < nSharedKeys; i++ {
				key := fmt.Sprintf("%s:key:%08d", "SharedSpace0", i)
				checkHitInRange(t, caches, key, namespaces)
			}
		}
		wg.Done()
	}()

	wg.Wait()
}
