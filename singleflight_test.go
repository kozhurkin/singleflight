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
	const resultTTL = 5 * time.Millisecond

	g := NewGroupWithCache[string, int](resultTTL, 0, 0)

	var calls int32
	fn := func() (int, error) {
		return int(atomic.AddInt32(&calls, 1)), nil
	}

	// Первый вызов — вычисление и кеширование
	res1, err1 := g.Do("key", fn)
	require.NoError(t, err1)
	require.Equal(t, 1, res1)

	// В пределах resultTTL — должно использоваться кешированное значение
	res2, err2 := g.Do("key", fn)
	require.NoError(t, err2)
	require.Equal(t, 1, res2)

	// Ждём истечения TTL и проверяем, что значение пересчиталось
	time.Sleep(resultTTL + time.Millisecond)

	res3, err3 := g.Do("key", fn)
	require.NoError(t, err3)
	require.Equal(t, 2, res3)

	require.Equal(t, int32(2), atomic.LoadInt32(&calls), "fn should be called twice: initial + after TTL")
}

func TestGroup_Do_DoesNotCacheErrorsWhenDisabled(t *testing.T) {
	const resultTTL = 5 * time.Millisecond

	g := NewGroupWithCache[string, int](resultTTL, 0, 0)

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
	require.Equal(t, int32(2), atomic.LoadInt32(&calls), "errors should not be cached when errorTTL=0")
}

func TestGroup_Do_CachesErrorsWhenEnabled(t *testing.T) {
	const (
		resultTTL = 5 * time.Millisecond
		errorTTL  = 5 * time.Millisecond
	)

	g := NewGroupWithCache[string, int](resultTTL, errorTTL, 0)

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

	// В пределах resultTTL ошибка должна приходить из кеша
	res2, err2 := g.Do("key", fn)
	require.ErrorIs(t, err2, someErr)
	require.Equal(t, 0, res2)

	require.Equal(t, int32(1), atomic.LoadInt32(&calls), "error should be computed only once within TTL")

	// После TTL ошибка должна быть пересчитана
	time.Sleep(resultTTL + 2*time.Millisecond)

	res3, err3 := g.Do("key", fn)
	require.ErrorIs(t, err3, someErr)
	require.Equal(t, 0, res3)
	require.Equal(t, int32(2), atomic.LoadInt32(&calls), "error should be recomputed after TTL")
}

// TestGroup_Do_SuccessAndErrorHaveDifferentTTL проверяет, что успешный результат
// и ошибка кешируются на разные времена (resultTTL и errorTTL соответственно).
func TestGroup_Do_SuccessAndErrorHaveDifferentTTL(t *testing.T) {
	const (
		resultTTL = 30 * time.Millisecond
		errorTTL  = 5 * time.Millisecond
	)

	g := NewGroupWithCache[string, int](resultTTL, errorTTL, 0)

	// Успешный результат: должен жить resultTTL, а не errorTTL.
	var successCalls int32
	successFn := func() (int, error) {
		return int(atomic.AddInt32(&successCalls, 1)), nil
	}

	// Первый вызов вычисляет и кеширует результат.
	ok1, err1 := g.Do("ok", successFn)
	require.NoError(t, err1)
	require.Equal(t, 1, ok1)

	// Повторный вызов сразу — кеш.
	ok2, err2 := g.Do("ok", successFn)
	require.NoError(t, err2)
	require.Equal(t, 1, ok2)
	require.Equal(t, int32(1), atomic.LoadInt32(&successCalls), "success should be computed once")

	// Ждём больше errorTTL, но меньше resultTTL — успешный результат всё ещё
	// должен быть в кеше, иначе он бы жил по errorTTL, а не по resultTTL.
	time.Sleep(2 * errorTTL)

	ok3, err3 := g.Do("ok", successFn)
	require.NoError(t, err3)
	require.Equal(t, 1, ok3)
	require.Equal(t, int32(1), atomic.LoadInt32(&successCalls),
		"success result must still be cached after errorTTL (TTL is resultTTL)")

	// Ошибка: должна жить errorTTL, а не resultTTL.
	var errorCalls int32
	someErr := errors.New("boom-diff-ttl")
	errorFn := func() (int, error) {
		atomic.AddInt32(&errorCalls, 1)
		return 0, someErr
	}

	// Первый вызов вычисляет и кеширует ошибку.
	errRes1, errE1 := g.Do("err", errorFn)
	require.ErrorIs(t, errE1, someErr)
	require.Equal(t, 0, errRes1)
	require.Equal(t, int32(1), atomic.LoadInt32(&errorCalls))

	// Повторный вызов до истечения errorTTL — ошибка из кеша.
	errRes2, errE2 := g.Do("err", errorFn)
	require.ErrorIs(t, errE2, someErr)
	require.Equal(t, 0, errRes2)
	require.Equal(t, int32(1), atomic.LoadInt32(&errorCalls))

	// Ждём чуть больше errorTTL, но существенно меньше resultTTL.
	time.Sleep(errorTTL + 2*time.Millisecond)

	// Теперь ошибка должна быть пересчитана, иначе она бы жила по resultTTL.
	errRes3, errE3 := g.Do("err", errorFn)
	require.ErrorIs(t, errE3, someErr)
	require.Equal(t, 0, errRes3)
	require.Equal(t, int32(2), atomic.LoadInt32(&errorCalls),
		"error must be recomputed after errorTTL even though resultTTL is larger")
}

func TestGroup_Do_WarmingReplacesCachedValue(t *testing.T) {
	const (
		resultTTL    = 5 * time.Millisecond
		warmupWindow = 3 * time.Millisecond
	)

	g := NewGroupWithCache[string, int](resultTTL, 0, warmupWindow)

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

	// Ждём чуть больше resultTTL, чтобы поставилась метка на прогрев.
	time.Sleep(resultTTL + time.Millisecond)

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
	time.Sleep(resultTTL + warmupWindow + time.Millisecond)

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
		resultTTL    = 5 * time.Millisecond
		warmupWindow = 3 * time.Millisecond
	)

	g := NewGroupWithCache[string, int](resultTTL, 0, warmupWindow)

	var calls int32
	fn := func() (int, error) {
		return int(atomic.AddInt32(&calls, 1)), nil
	}

	// 1-й вызов — кладём в кеш
	res1, err1 := g.Do("key", fn)
	require.NoError(t, err1)
	require.Equal(t, 1, res1)

	// Ждём resultTTL + warmupWindow + небольшой запас, НО новых Do не вызываем.
	time.Sleep(resultTTL + warmupWindow + 2*time.Millisecond)

	// Теперь ключ должен быть очищен, следующий вызов — новое вычисление.
	res2, err2 := g.Do("key", fn)
	require.NoError(t, err2)
	require.Equal(t, 2, res2)

	require.Equal(t, int32(2), atomic.LoadInt32(&calls))
}

func TestGroup_Do_ZeroCacheTimeBehavesAsNoCache(t *testing.T) {
	g := NewGroupWithCache[string, int](0, 0, time.Second)

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

// TestGroup_Do_ZeroCacheTime_HighConcurrencyBounded проверяет, что при resultTTL=0
// число реальных вызовов fn под высокой конкурентностью остаётся разумно ограниченным.
// Этот тест чувствителен к дополнительным фоновым таймерам/горутинкам в ветке resultTTL==0:
// без return внутри cacheFinalizerForKey он, как правило, показывает сильно завышенное
// значение calls и падает.
func TestGroup_Do_ZeroCacheTime_HighConcurrencyBounded(t *testing.T) {
	const (
		duration  = 300 * time.Millisecond
		fnLatency = 10 * time.Millisecond
		maxCalls  = 40 // эмпирический порог: "нормально" ~30–40, при баге — сотни
	)

	// Кейс, который бьёт именно ветку resultTTL == 0.
	g := NewGroupWithCache[string, int](0, 0, 0)

	var calls int32
	fn := func() (int, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(fnLatency)
		return 42, nil
	}

	deadline := time.Now().Add(duration)
	var wg sync.WaitGroup

	for time.Now().Before(deadline) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = g.Do("key", fn)
		}()
		time.Sleep(1 * time.Millisecond)
	}

	wg.Wait()

	got := atomic.LoadInt32(&calls)
	if got > maxCalls {
		t.Fatalf("too many fn calls under high concurrency with resultTTL=0: got=%d, max=%d", got, maxCalls)
	}
}

// TestGroup_Warming_CallsCount — диагностический тест для оценки количества вызовов fn
// при включённом прогреве. Он не проверяет строгие инварианты, а лишь логирует
// число запусков fn за фиксированный интервал времени.
func TestGroup_Warming_CallsCount(t *testing.T) {
	const (
		resultTTL    = 20 * time.Millisecond
		warmupWindow = 10 * time.Millisecond
		duration     = 100 * time.Millisecond
	)
	startTime := time.Now()
	g := NewGroupWithCache[string, int](resultTTL, 0, warmupWindow)

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

	t.Logf("calls=%d, hits=%d, resultTTL=%v, warmupWindow=%v, duration=%v",
		calls, hits, resultTTL, warmupWindow, duration)
}
