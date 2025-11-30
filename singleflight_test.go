package singleflight

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGroup_Do_DeduplicatesConcurrent(t *testing.T) {
	g := NewGroup[string, int]()

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
			res, err := g.Do("key", fn)
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

func TestGroup_Do_NoCache_RecomputesEachTime(t *testing.T) {
	g := NewGroup[string, int]()

	var calls int32
	fn := func() (int, error) {
		return int(atomic.AddInt32(&calls, 1)), nil
	}

	res1, err1 := g.Do("key", fn)
	res2, err2 := g.Do("key", fn)

	require.NoError(t, err1)
	require.NoError(t, err2)
	require.Equal(t, 1, res1)
	require.Equal(t, 2, res2)
	require.Equal(t, int32(2), atomic.LoadInt32(&calls))
}

func TestGroup_Do_CacheSuccessResult(t *testing.T) {
	const cacheTime = 5 * time.Millisecond

	g := NewGroupWithCache[string, int](cacheTime, false, 0)

	var calls int32
	fn := func() (int, error) {
		return int(atomic.AddInt32(&calls, 1)), nil
	}

	// Первый вызов — вычисление и кеширование
	res1, err1 := g.Do("key", fn)
	require.NoError(t, err1)
	require.Equal(t, 1, res1)

	// В пределах cacheTime — должно использоваться кешированное значение
	res2, err2 := g.Do("key", fn)
	require.NoError(t, err2)
	require.Equal(t, 1, res2)

	// Ждём истечения TTL и проверяем, что значение пересчиталось
	time.Sleep(cacheTime + time.Millisecond)

	res3, err3 := g.Do("key", fn)
	require.NoError(t, err3)
	require.Equal(t, 2, res3)

	require.Equal(t, int32(2), atomic.LoadInt32(&calls), "fn should be called twice: initial + after TTL")
}

func TestGroup_Do_DoesNotCacheErrorsWhenDisabled(t *testing.T) {
	const cacheTime = 5 * time.Millisecond

	g := NewGroupWithCache[string, int](cacheTime, false, 0)

	var calls int32
	someErr := errors.New("boom")
	fn := func() (int, error) {
		atomic.AddInt32(&calls, 1)
		return 0, someErr
	}

	res1, err1 := g.Do("key", fn)
	res2, err2 := g.Do("key", fn)

	require.ErrorIs(t, err1, someErr)
	require.ErrorIs(t, err2, someErr)
	require.Equal(t, 0, res1)
	require.Equal(t, 0, res2)
	require.Equal(t, int32(2), atomic.LoadInt32(&calls), "errors should not be cached when cacheErrors=false")
}

func TestGroup_Do_CachesErrorsWhenEnabled(t *testing.T) {
	const cacheTime = 5 * time.Millisecond

	g := NewGroupWithCache[string, int](cacheTime, true, 0)

	var calls int32
	someErr := errors.New("boom")
	fn := func() (int, error) {
		atomic.AddInt32(&calls, 1)
		return 0, someErr
	}

	// Первый вызов — вычисление и кеширование ошибки
	res1, err1 := g.Do("key", fn)
	require.ErrorIs(t, err1, someErr)
	require.Equal(t, 0, res1)

	// В пределах cacheTime ошибка должна приходить из кеша
	res2, err2 := g.Do("key", fn)
	require.ErrorIs(t, err2, someErr)
	require.Equal(t, 0, res2)

	require.Equal(t, int32(1), atomic.LoadInt32(&calls), "error should be computed only once within TTL")

	// После TTL ошибка должна быть пересчитана
	time.Sleep(cacheTime + 2*time.Millisecond)

	res3, err3 := g.Do("key", fn)
	require.ErrorIs(t, err3, someErr)
	require.Equal(t, 0, res3)
	require.Equal(t, int32(2), atomic.LoadInt32(&calls), "error should be recomputed after TTL")
}

func TestGroup_Do_WarmingReplacesCachedValue(t *testing.T) {
	const (
		cacheTime = 5 * time.Millisecond
		warmTime  = 3 * time.Millisecond
	)

	g := NewGroupWithCache[string, int](cacheTime, false, warmTime)

	var initialCalls, warmCalls, thirdCalls int32

	fnInitial := func() (int, error) {
		atomic.AddInt32(&initialCalls, 1)
		return 1, nil
	}
	fnWarm := func() (int, error) {
		atomic.AddInt32(&warmCalls, 1)
		return 2, nil
	}
	fnThird := func() (int, error) {
		atomic.AddInt32(&thirdCalls, 1)
		return 3, nil
	}

	// Первое вычисление — значение 1, кладётся в кеш.
	res1, err1 := g.Do("key", fnInitial)
	require.NoError(t, err1)
	require.Equal(t, 1, res1)

	// Ждём чуть больше cacheTime, чтобы поставилась метка на прогрев.
	time.Sleep(cacheTime + time.Millisecond)

	// Этот вызов должен ещё вернуть старое значение (1),
	// но при этом запустить разогревочный Flight с fnWarm.
	res2, err2 := g.Do("key", fnWarm)
	require.NoError(t, err2)
	require.Equal(t, 1, res2)

	// Даём время прогреву завершиться и примениться.
	time.Sleep(time.Millisecond)

	// Теперь Group должен отдавать результат прогретого Flight (значение 2),
	// при этом fnThird вызываться не должен.
	res3, err3 := g.Do("key", fnThird)
	require.NoError(t, err3)
	require.Equal(t, 2, res3)
	require.Equal(t, int32(1), atomic.LoadInt32(&initialCalls), "initial fn should be called once")
	require.Equal(t, int32(1), atomic.LoadInt32(&warmCalls), "warm fn should be called once by warm-up")
	require.Equal(t, int32(0), atomic.LoadInt32(&thirdCalls), "third fn must not be called (should use warm result)")

	// Дополнительно: проверяем, что тёплое значение тоже когда-то протухает.
	time.Sleep(cacheTime + warmTime + time.Millisecond)

	res4, err4 := g.Do("key", fnThird)
	require.NoError(t, err4)
	require.Equal(t, 3, res4, "после TTL тёплое значение должно быть пересчитано")
	require.Equal(t, int32(1), atomic.LoadInt32(&thirdCalls),
		"fnThird должен быть вызван один раз после истечения TTL тёплого значения")
}

func TestGroup_Do_DifferentKeysIndependent(t *testing.T) {
	g := NewGroup[string, int]()

	var calls1, calls2 int32
	fn1 := func() (int, error) {
		return int(atomic.AddInt32(&calls1, 1)), nil
	}
	fn2 := func() (int, error) {
		return int(atomic.AddInt32(&calls2, 1)) * 10, nil
	}

	res1a, err1a := g.Do("k1", fn1)
	res2a, err2a := g.Do("k2", fn2)
	res1b, err1b := g.Do("k1", fn1)
	res2b, err2b := g.Do("k2", fn2)

	require.NoError(t, err1a)
	require.NoError(t, err2a)
	require.NoError(t, err1b)
	require.NoError(t, err2b)

	require.Equal(t, 1, res1a)
	require.Equal(t, 10, res2a)
	require.Equal(t, 2, res1b)
	require.Equal(t, 20, res2b)

	require.Equal(t, int32(2), atomic.LoadInt32(&calls1))
	require.Equal(t, int32(2), atomic.LoadInt32(&calls2))
}

func TestGroup_Do_WarmingWithoutRequests_CleansKey(t *testing.T) {
	const (
		cacheTime = 5 * time.Millisecond
		warmTime  = 3 * time.Millisecond
	)

	g := NewGroupWithCache[string, int](cacheTime, false, warmTime)

	var calls int32
	fn := func() (int, error) {
		return int(atomic.AddInt32(&calls, 1)), nil
	}

	// 1-й вызов — кладём в кеш
	res1, err1 := g.Do("key", fn)
	require.NoError(t, err1)
	require.Equal(t, 1, res1)

	// Ждём cacheTime + warmTime + небольшой запас, НО новых Do не вызываем.
	time.Sleep(cacheTime + warmTime + 2*time.Millisecond)

	// Теперь ключ должен быть очищен, следующий вызов — новое вычисление.
	res2, err2 := g.Do("key", fn)
	require.NoError(t, err2)
	require.Equal(t, 2, res2)

	require.Equal(t, int32(2), atomic.LoadInt32(&calls))
}

func TestGroup_Do_ZeroCacheTimeBehavesAsNoCache(t *testing.T) {
	g := NewGroupWithCache[string, int](0, true, time.Second)

	var calls int32
	fn := func() (int, error) {
		return int(atomic.AddInt32(&calls, 1)), nil
	}

	res1, err1 := g.Do("key", fn)
	res2, err2 := g.Do("key", fn)

	require.NoError(t, err1)
	require.NoError(t, err2)
	require.Equal(t, 1, res1)
	require.Equal(t, 2, res2)
	require.Equal(t, int32(2), atomic.LoadInt32(&calls))
}

// TestGroup_Warming_CallsCount — диагностический тест для оценки количества вызовов fn
// при включённом прогреве. Он не проверяет строгие инварианты, а лишь логирует
// число запусков fn за фиксированный интервал времени.
func TestGroup_Warming_CallsCount(t *testing.T) {
	const (
		cacheTime = 20 * time.Millisecond
		warmTime  = 10 * time.Millisecond
		duration  = 100 * time.Millisecond
	)
	startTime := time.Now()
	g := NewGroupWithCache[string, int](cacheTime, false, warmTime)

	var calls, hits int32
	fn := func() (int, error) {
		atomic.AddInt32(&calls, 1)
		t.Logf("calls=%d hits=%d elapsed=%v", calls, hits, time.Since(startTime))
		return 42, nil
	}

	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		time.Sleep(1 * time.Millisecond)
		atomic.AddInt32(&hits, 1)
		go g.Do("key", fn)
	}

	t.Logf("calls=%d, hits=%d, cacheTime=%v, warmTime=%v, duration=%v",
		calls, hits, cacheTime, warmTime, duration)
}
