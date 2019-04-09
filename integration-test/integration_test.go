// +build integration

package integration_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/tescherm/mc/client"
)

var mc client.MemcachedClient

func init() {
	addr := "localhost:8080"
	c, err := client.New(client.Config{
		ServiceURI: addr,
	})
	if err != nil {
		panic(errors.Wrap(err, "unable to create memcached client"))
	}

	mc = c
}

func TestCacheLifecycle(t *testing.T) {
	ctx := context.Background()

	defer func() {
		err := mc.Clear(ctx)
		require.NoError(t, err)
	}()

	key := randAlphaNumericString(10)
	value := randAlphaNumericString(20)

	retValue, err := mc.Get(ctx, key)
	require.NoError(t, err)
	require.Nil(t, retValue)

	item := &client.Item{
		Key:   key,
		Value: []byte(value),
	}
	err = mc.Set(ctx, item)
	require.NoError(t, err)

	retValue, err = mc.Get(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, retValue)
	require.Equal(t, value, string(retValue.Value))

	size, err := mc.Size(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, size)

	retValue, err = mc.Remove(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, retValue)
	require.Equal(t, value, string(retValue.Value))

	size, err = mc.Size(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 0, size)
}

func TestCompareAndSwap(t *testing.T) {
	ctx := context.Background()

	defer func() {
		err := mc.Clear(ctx)
		require.NoError(t, err)
	}()

	k := "key"
	v := func(b int) []byte {
		return []byte{byte(b)}
	}

	// let's simulate storing a counter, similar to what's mentioned at
	// https://en.wikipedia.org/wiki/Amazon_SimpleDB#Conditional_Put_and_Delete
	err := mc.Set(ctx, &client.Item{
		Key: k,
	})

	require.NoError(t, err)

	item1, err := mc.Get(ctx, k)
	require.NoError(t, err)
	require.NotNil(t, item1)

	item1.Value = v(2)

	item2, err := mc.Get(ctx, k)
	require.NoError(t, err)
	require.NotNil(t, item2)

	item2.Value = v(2)

	err = mc.CompareAndSwap(ctx, item1)
	require.NoError(t, err)

	err = mc.CompareAndSwap(ctx, item2)
	require.Error(t, err)
	require.EqualValues(t, client.ErrCASConflict, err)

	item2, err = mc.Get(ctx, k)
	require.NoError(t, err)
	require.NotNil(t, item2)

	item2.Value = v(3)
	err = mc.CompareAndSwap(ctx, item2)
	require.NoError(t, err)
}

func TestCacheConcurrency(t *testing.T) {
	ctx := context.Background()

	defer func() {
		err := mc.Clear(ctx)
		require.NoError(t, err)
	}()

	// number of cache sets
	numSets := 100

	start := time.Now()

	jobs := make(chan int, numSets)
	results := make(chan string, numSets)

	// start up workers
	for w := 1; w <= runtime.NumCPU(); w++ {
		go cacheWorker(t, jobs, results)
	}

	for set := 0; set < numSets; set++ {
		jobs <- set
	}

	close(jobs)

	for j := 0; j < numSets; j++ {
		<-results
	}

	elapsed := time.Since(start)
	t.Logf("TestCacheConcurrency took %s", elapsed)
}

func cacheWorker(t *testing.T, jobs <-chan int, results chan<- string) {
	for range jobs {
		ctx := context.Background()

		key := randAlphaNumericString(10)
		value := randAlphaNumericString(100)

		item := &client.Item{
			Key:   key,
			Value: []byte(value),
		}
		err := mc.Set(ctx, item)
		require.NoError(t, err)

		retValue, err := mc.Get(ctx, key)
		require.NoError(t, err)
		require.NotNil(t, retValue)
		require.Equal(t, value, string(retValue.Value))

		retValue, err = mc.Remove(ctx, key)
		require.NoError(t, err)
		require.NotNil(t, retValue)
		require.Equal(t, value, string(retValue.Value))

		results <- key
	}
}
