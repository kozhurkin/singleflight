package redisflight

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestGroup_ResultTTL_ExpiresAndRecomputes проверяет, что после истечения resultTTL
// значение в кеше пропадает и следующий запрос повторно вызывает fn.
func TestGroup_ResultTTL_ExpiresAndRecomputes(t *testing.T) {
	client := newTestRedisClientV9(t)
	backend := NewGoRedisV9Backend(client)

	key := newBackendTestKey(t, "group:result-ttl")

	const (
		lockTTL      = 2 * time.Second
		resultTTL    = 500 * time.Millisecond
		pollInterval = 50 * time.Millisecond
	)

	g := NewGroup[int](backend, lockTTL, resultTTL, pollInterval)

	var calls int32
	fn := func() (int, error) {
		n := atomic.AddInt32(&calls, 1)
		return int(n), nil
	}

	// Первый вызов — выполняет fn и кладёт результат в Redis.
	v1, err := g.Do(key, fn)
	require.NoError(t, err, "first Do should not return error")
	require.Equal(t, 1, v1, "first Do should return 1")
	require.Equal(t, int32(1), atomic.LoadInt32(&calls), "fn should be called once after first Do")

	// Второй вызов до истечения resultTTL должен взять результат из кеша,
	// а fn не должен быть вызван повторно.
	v2, err := g.Do(key, fn)
	require.NoError(t, err, "second Do should not return error")
	require.Equal(t, 1, v2, "second Do should return cached 1")
	require.Equal(t, int32(1), atomic.LoadInt32(&calls), "fn should still be called once after second Do")

	// Ждём, пока истечёт TTL результата.
	time.Sleep(resultTTL + time.Millisecond)

	// После истечения resultTTL кеш должен очиститься,
	// и следующий вызов снова выполнит fn.
	v3, err := g.Do(key, fn)
	require.NoError(t, err, "third Do should not return error")
	require.Equal(t, 2, v3, "third Do should return recomputed 2")
	require.Equal(t, int32(2), atomic.LoadInt32(&calls), "fn should be called twice after third Do")
}

// TestGroup_LockTTL_ErrorThenRecoveryWakesWaiters проверяет сценарий, когда первый вызов fn падает с ошибкой:
//   - блокировка висит lockTTL и не даёт другим горутинам выполнить fn повторно;
//   - после истечения lockTTL следующий успешный вызов fn записывает результат в Redis;
//   - ждущие горутины получают этот результат из кеша, не выполняя fn.
func TestGroup_LockTTL_ErrorThenRecoveryWakesWaiters(t *testing.T) {
	const (
		lockTTL      = 100 * time.Millisecond
		resultTTL    = 1 * time.Second
		pollInterval = 10 * time.Millisecond
		waiters      = 5
	)

	client := newTestRedisClientV9(t)
	backend := NewGoRedisV9Backend(client)

	key := newBackendTestKey(t, "group:lock-error")

	g := NewGroup[int](backend, lockTTL, resultTTL, pollInterval)

	var calls int32
	fn := func() (int, error) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			// Первый вызов "падает".
			return 0, fmt.Errorf("boom")
		}
		// Все последующие успешные.
		return 42, nil
	}

	type result struct {
		val int
		err error
	}

	var (
		wg      sync.WaitGroup
		results [waiters]result
	)

	wg.Add(waiters)

	// Группа горутин, которые одновременно вызывают Do и ждут результата.
	for i := 0; i < waiters; i++ {
		i := i
		go func() {
			defer wg.Done()
			v, err := g.Do(key, fn)
			results[i] = result{val: v, err: err}
		}()
	}

	wg.Add(1)

	// "Восстанавливающий" запрос, который выполняется после истечения lockTTL
	// и должен успешно записать результат в Redis и "разбудить" ждущие горутины.
	go func() {
		defer wg.Done()
		time.Sleep(lockTTL + 50*time.Millisecond)

		v, err := g.Do(key, fn)
		if err != nil {
			t.Errorf("recovery Do returned error: %v", err)
			return
		}
		if v != 42 {
			t.Errorf("recovery Do = %d, want 42", v)
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		require.FailNow(t, "test timeout: goroutines did not finish")
	}

	// Проверяем, что fn действительно вызывался дважды:
	//   - первый раз с ошибкой,
	//   - второй раз при восстановлении.
	require.Equal(t, int32(2), atomic.LoadInt32(&calls), "fn should be called twice (error + recovery)")

	var (
		errCount     int
		timeoutCount int
		successCount int
	)

	for _, r := range results {
		if r.err == nil {
			require.Equal(t, 42, r.val, "waiter should see recovered value 42")
			successCount++
			continue
		}

		switch r.err.Error() {
		case "boom":
			errCount++
		case "timeout waiting for result":
			timeoutCount++
		default:
			require.Failf(t, "unexpected waiter error", "error: %v", r.err)
		}
	}

	require.Equal(t, 1, errCount, "expected exactly 1 waiter to see fn error")
	require.Equal(t, 0, timeoutCount, "expected no timeouts")
	require.Equal(t, waiters-1, successCount, "unexpected number of successful waiters")
}

// countingBackend оборачивает Backend и считает количество вызовов GetResultWithTTL.
type countingBackend struct {
	Backend

	mu              sync.Mutex
	getWithTTLCalls int
}

func (c *countingBackend) GetResultWithTTL(ctx context.Context, resultKey string) ([]byte, bool, time.Duration, error) {
	c.mu.Lock()
	c.getWithTTLCalls++
	c.mu.Unlock()
	return c.Backend.GetResultWithTTL(ctx, resultKey)
}

// TestGroup_LocalDeduplication_ReducesBackendCalls проверяет, что локальная дедупликация
// снижает количество обращений к Backend (GetResultWithTTL) при множественных конкурентных Do.
func TestGroup_LocalDeduplication_ReducesBackendCalls(t *testing.T) {
	const (
		lockTTL      = 50 * time.Millisecond
		resultTTL    = 100 * time.Millisecond
		pollInterval = 10 * time.Millisecond
		workers      = 8
	)

	client := newTestRedisClientV9(t)
	baseBackend := NewGoRedisV9Backend(client)

	key1 := newBackendTestKey(t, "group:local-dedup-false")
	key2 := newBackendTestKey(t, "group:local-dedup-true")

	// Группа без локальной дедупликации.
	counting := &countingBackend{Backend: baseBackend}
	group := NewGroup[int](counting, lockTTL, resultTTL, pollInterval)

	var calls int32
	fn := func() (int, error) {
		n := atomic.AddInt32(&calls, 1)
		time.Sleep(10 * time.Millisecond)
		return int(n), nil
	}

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			if _, err := group.Do(key1, fn); err != nil {
				t.Errorf("group.Do returned error: %v", err)
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int32(1), atomic.LoadInt32(&calls),
		"without local dedup fn should be called once")

	require.Equal(t, workers, counting.getWithTTLCalls,
		"without local dedup GetResultWithTTL should be called for each worker")

	group = NewGroup(counting, lockTTL, resultTTL, pollInterval, WithLocalDeduplication[int](true))

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			if _, err := group.Do(key2, fn); err != nil {
				t.Errorf("group.Do returned error: %v", err)
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int32(2), atomic.LoadInt32(&calls),
		"with local dedup fn should be called exactly twice (once per key)")

	require.Equal(t, workers+1, counting.getWithTTLCalls,
		"with local dedup GetResultWithTTL should be called once for warm cache hit")
}

// TestGroup_WarmupWindow_BackgroundRecompute проверяет, что при включённом окне прогрева:
//   - первый hit после вычисления возвращает старое значение и запускает прогрев в фоне;
//   - фоновый прогрев выполняет fn ещё раз и обновляет результат;
//   - последующие запросы получают обновлённое значение.
func TestGroup_WarmupWindow_BackgroundRecompute(t *testing.T) {
	client := newTestRedisClientV9(t)
	backend := NewGoRedisV9Backend(client)

	key := newBackendTestKey(t, "group:warmup-window")

	const (
		lockTTL      = 200 * time.Millisecond
		resultTTL    = 500 * time.Millisecond
		pollInterval = 10 * time.Millisecond
		warmupWindow = 300 * time.Millisecond
		fnDelay      = 20 * time.Millisecond
	)

	g := NewGroup(
		backend,
		lockTTL,
		resultTTL,
		pollInterval,
		WithWarmupWindow[int](warmupWindow),
	)

	var calls int32
	fn := func() (int, error) {
		n := atomic.AddInt32(&calls, 1)
		time.Sleep(fnDelay)
		return int(n), nil
	}

	// Первый вызов вычисляет значение и кладёт его в Redis.
	v1, err := g.Do(key, fn)
	require.NoError(t, err, "first Do should not return error")
	require.Equal(t, 1, v1, "first Do should return 1")
	require.Equal(t, int32(1), atomic.LoadInt32(&calls), "fn should be called once after first Do")

	time.Sleep(resultTTL)

	// Второй вызов должен взять старое значение из кеша и запустить прогрев в фоне.
	v2, err := g.Do(key, fn)
	require.NoError(t, err, "second Do should not return error")
	require.Equal(t, 1, v2, "second Do should return cached 1")

	time.Sleep(fnDelay * 2)

	// Третий вызов должен получить обновлённое значение 2 из кеша.
	v3, err := g.Do(key, fn)
	require.NoError(t, err, "third Do should not return error")
	require.Equal(t, 2, v3, "third Do should return warmed-up 2")
	require.Equal(t, int32(2), atomic.LoadInt32(&calls), "fn should be called twice after warmup")
}

// TestGroup_TimeoutWaitingForResult проверяет, что если блокировка по ключу захвачена
// "зависшим" владельцем и результат так и не появляется в Redis, Do/DoCtx возвращает
// ErrTimeoutWaitingForResult после истечения lockTTL+resultTTL.
func TestGroup_TimeoutWaitingForResult(t *testing.T) {
	const (
		lockTTL      = 100 * time.Millisecond
		resultTTL    = 100 * time.Millisecond
		pollInterval = 10 * time.Millisecond
	)

	client := newTestRedisClientV9(t)
	backend := NewGoRedisV9Backend(client)

	key := newBackendTestKey(t, "group:timeout-waiting")
	g := NewGroup[int](backend, lockTTL, resultTTL, pollInterval)

	// Сначала выполняем Do с функцией, которая возвращает ошибку.
	// В этом случае computeAndStore не снимает блокировку и не пишет результат в кеше —
	// блокировка "зависает" до истечения lockTTL.
	var firstCalls int32
	firstFn := func() (int, error) {
		atomic.AddInt32(&firstCalls, 1)
		return 0, fmt.Errorf("intentional failure to keep lock without result")
	}

	_, err := g.Do(key, firstFn)
	require.Error(t, err, "first Do should return an error")
	require.Equal(t, int32(1), atomic.LoadInt32(&firstCalls), "first fn should be called once")

	// Теперь выполняем Do ещё раз: блокировка уже захвачена "зависшим" владельцем,
	// а результат в кеше так и не появился. Мы должны получить ErrTimeoutWaitingForResult,
	// при этом fn не должен вызываться.
	var calls int32
	fn := func() (int, error) {
		atomic.AddInt32(&calls, 1)
		return 0, fmt.Errorf("fn should not be called when lock is held by another owner")
	}

	start := time.Now()
	_, err = g.Do(key, fn)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, ErrTimeoutWaitingForResult,
		"Do should return ErrTimeoutWaitingForResult when lock is held and result never appears")

	require.Equal(t, int32(0), atomic.LoadInt32(&calls),
		"fn should not be called when lock is held by another owner")

	// Порядок величины: ожидание не должно быть заметно меньше lockTTL+resultTTL.
	minExpected := lockTTL + resultTTL
	require.GreaterOrEqual(t, elapsed, minExpected,
		"Do returned too early: elapsed=%v, want at least %v", elapsed, minExpected)
}
