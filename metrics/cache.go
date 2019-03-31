package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tescherm/mc/core/caches"
)

type CacheCollector struct {
	numEvictsDesc  *prometheus.Desc
	numRemovesDesc *prometheus.Desc
	numClearsDesc  *prometheus.Desc
	numSetsDesc    *prometheus.Desc
	numHitsDesc    *prometheus.Desc
	numMissesDesc  *prometheus.Desc

	currentCapacityDesc *prometheus.Desc

	caches *caches.Caches
}

func (c *CacheCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, ch)
}

func (c *CacheCollector) Collect(ch chan<- prometheus.Metric) {
	stats := c.caches.Stats()

	for i, stats := range stats.Caches {
		cacheID := strconv.Itoa(i)

		ch <- prometheus.MustNewConstMetric(
			c.numEvictsDesc,
			prometheus.CounterValue,
			float64(stats.Evicts),
			cacheID,
		)

		ch <- prometheus.MustNewConstMetric(
			c.numRemovesDesc,
			prometheus.CounterValue,
			float64(stats.Removes),
			cacheID,
		)

		ch <- prometheus.MustNewConstMetric(
			c.numClearsDesc,
			prometheus.CounterValue,
			float64(stats.Clears),
			cacheID,
		)

		ch <- prometheus.MustNewConstMetric(
			c.numSetsDesc,
			prometheus.CounterValue,
			float64(stats.Sets),
			cacheID,
		)

		ch <- prometheus.MustNewConstMetric(
			c.numHitsDesc,
			prometheus.CounterValue,
			float64(stats.Hits),
			cacheID,
		)

		ch <- prometheus.MustNewConstMetric(
			c.numMissesDesc,
			prometheus.CounterValue,
			float64(stats.Misses),
			cacheID,
		)

		ch <- prometheus.MustNewConstMetric(
			c.currentCapacityDesc,
			prometheus.GaugeValue,
			float64(stats.CurrentCapacity),
			cacheID,
		)
	}
}

func cacheStatName(shortName string) string {
	return prometheus.BuildFQName(
		"mc",
		"cache",
		shortName,
	)
}

func NewCacheCollector(caches *caches.Caches) prometheus.Collector {
	constLabels := prometheus.Labels{}

	numEvictsDesc := prometheus.NewDesc(
		cacheStatName("evicts_total"),
		"Number of cache evictions",
		[]string{"cache"},
		constLabels,
	)

	numRemovesDesc := prometheus.NewDesc(
		cacheStatName("removes_total"),
		"Number of cache remove operations",
		[]string{"cache"},
		constLabels,
	)

	numClearsDesc := prometheus.NewDesc(
		cacheStatName("clears_total"),
		"Number of cache clear operations",
		[]string{"cache"},
		constLabels,
	)

	numSetsDesc := prometheus.NewDesc(
		cacheStatName("set_total"),
		"Number of cache set operations",
		[]string{"cache"},
		constLabels,
	)

	numHitsDesc := prometheus.NewDesc(
		cacheStatName("hits_total"),
		"Number of cache hits",
		[]string{"cache"},
		constLabels,
	)

	numMissesDesc := prometheus.NewDesc(
		cacheStatName("misses_total"),
		"Number of cache misses",
		[]string{"cache"},
		constLabels,
	)

	currentCapacity := prometheus.NewDesc(
		cacheStatName("current_capacity"),
		"The current cache capacity, in bytes",
		[]string{"cache"},
		constLabels,
	)

	return &CacheCollector{
		numEvictsDesc:  numEvictsDesc,
		numClearsDesc:  numClearsDesc,
		numSetsDesc:    numSetsDesc,
		numRemovesDesc: numRemovesDesc,
		numHitsDesc:    numHitsDesc,
		numMissesDesc:  numMissesDesc,

		currentCapacityDesc: currentCapacity,

		caches: caches,
	}
}
