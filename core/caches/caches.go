package caches

import (
	"fmt"
	"sync"

	"github.com/tescherm/mc/core/cache"
	"github.com/tescherm/mc/core/consistenthash"
)

type Config struct {
	CacheCount int
	Replicas   int
	Capacity   uint64
}

type Stats struct {
	Evicts          uint64
	Removes         uint64
	Clears          uint64
	Sets            uint64
	Hits            uint64
	Misses          uint64
	CurrentCapacity uint64

	Caches []cache.Stats
}

type Caches struct {
	sync.RWMutex

	hash *consistenthash.ConsistentHash

	cacheIDs []string
	cacheMap map[string]cache.Cache
}

func New(config Config) *Caches {
	cacheMap := make(map[string]cache.Cache, config.CacheCount)
	cacheIDs := make([]string, config.CacheCount)

	cacheCapacity := config.Capacity / uint64(config.CacheCount)

	for i := 0; i < config.CacheCount; i++ {
		cacheID := fmt.Sprintf("cache-%d", i)
		cacheIDs[i] = cacheID
		cacheMap[cacheID] = cache.NewLRUCache(cache.Config{
			Capacity: cacheCapacity,
		})
	}

	hash := consistenthash.New(cacheIDs, config.Replicas)

	return &Caches{
		cacheMap: cacheMap,
		cacheIDs: cacheIDs,
		hash:     hash,
	}
}

func (s *Caches) CacheForKey(key string) cache.Cache {
	s.RLock()
	defer s.RUnlock()

	cacheID := s.hash.GetNode(key)
	c, ok := s.cacheMap[cacheID]
	if !ok {
		return nil
	}
	return c
}

func (s *Caches) Clear() {
	s.Lock()
	defer s.Unlock()

	for _, v := range s.cacheMap {
		v.Clear()
	}
}

func (s *Caches) Size() uint64 {
	s.RLock()
	defer s.RUnlock()

	var total uint64
	for _, v := range s.cacheMap {
		total += v.Size()
	}
	return total
}

func (s *Caches) Stats() Stats {
	s.RLock()
	defer s.RUnlock()

	stats := Stats{
		Caches: make([]cache.Stats, len(s.cacheIDs)),
	}

	for i, cacheID := range s.cacheIDs {
		c := s.cacheMap[cacheID]
		s := c.Stats()

		stats.Clears += s.Clears
		stats.Removes += s.Removes
		stats.Evicts += s.Evicts
		stats.Misses += s.Misses
		stats.Hits += s.Hits
		stats.Sets += s.Sets
		stats.CurrentCapacity += s.CurrentCapacity

		stats.Caches[i] = s
	}

	return stats
}
