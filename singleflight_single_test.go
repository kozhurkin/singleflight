package singleflight

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSingle_Do_DeduplicatesConcurrent(t *testing.T) {
	g := NewSingle[int]()

	var calls int32
	fn := func() (int, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(10 * time.Millisecond)
		return 42, nil
	}

	const n = 20
	results := make([]int, n)
	errs := make([]error, n)

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			res, err := g.Do(fn)
			results[i] = res
			errs[i] = err
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		require.NoError(t, errs[i], "all Do calls should succeed")
		require.Equal(t, 42, results[i], "all Do calls should see the same result")
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&calls), "fn should be called exactly once")
}

func TestSingle_Do_CacheSuccessResult(t *testing.T) {
	const resultTTL = 5 * time.Millisecond

	g := NewSingleWithCache[int](resultTTL, 0, 0)

	var calls int32
	fn := func() (int, error) {
		return int(atomic.AddInt32(&calls, 1)), nil
	}

	// Первый вызов — вычисление и кеширование
	res1, err1 := g.Do(fn)
	require.NoError(t, err1)
	require.Equal(t, 1, res1)

	// В пределах resultTTL — должно использоваться кешированное значение
	res2, err2 := g.Do(fn)
	require.NoError(t, err2)
	require.Equal(t, 1, res2)

	// Ждём истечения TTL и проверяем, что значение пересчиталось
	time.Sleep(resultTTL + time.Millisecond)

	res3, err3 := g.Do(fn)
	require.NoError(t, err3)
	require.Equal(t, 2, res3)

	require.Equal(t, int32(2), atomic.LoadInt32(&calls), "fn should be called twice: initial + after TTL")
}
