package redisflight

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
	if err != nil {
		t.Fatalf("first Do returned error: %v", err)
	}
	if v1 != 1 {
		t.Fatalf("first Do = %d, want 1", v1)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("fn calls after first Do = %d, want 1", got)
	}

	// Второй вызов до истечения resultTTL должен взять результат из кеша,
	// а fn не должен быть вызван повторно.
	v2, err := g.Do(key, fn)
	if err != nil {
		t.Fatalf("second Do returned error: %v", err)
	}
	if v2 != 1 {
		t.Fatalf("second Do = %d, want 1 (cached)", v2)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("fn calls after second Do = %d, want still 1", got)
	}

	// Ждём, пока истечёт TTL результата.
	time.Sleep(resultTTL + time.Millisecond)

	// После истечения resultTTL кеш должен очиститься,
	// и следующий вызов снова выполнит fn.
	v3, err := g.Do(key, fn)
	if err != nil {
		t.Fatalf("third Do returned error: %v", err)
	}
	if v3 != 2 {
		t.Fatalf("third Do = %d, want 2 (recomputed)", v3)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("fn calls after third Do = %d, want 2", got)
	}
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
		t.Fatalf("test timeout: goroutines did not finish")
	}

	// Проверяем, что fn действительно вызывался дважды:
	//   - первый раз с ошибкой,
	//   - второй раз при восстановлении.
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("fn calls = %d, want 2", got)
	}

	var (
		errCount     int
		timeoutCount int
		successCount int
	)

	for _, r := range results {
		if r.err == nil {
			if r.val != 42 {
				t.Fatalf("waiter got value %d, want 42", r.val)
			}
			successCount++
			continue
		}

		switch r.err.Error() {
		case "boom":
			errCount++
		case "timeout waiting for result":
			timeoutCount++
		default:
			t.Fatalf("unexpected waiter error: %v", r.err)
		}
	}

	if errCount != 1 {
		t.Fatalf("expected exactly 1 waiter to see fn error, got %d", errCount)
	}
	if timeoutCount != 0 {
		t.Fatalf("expected no timeouts, got %d", timeoutCount)
	}
	if successCount != waiters-1 {
		t.Fatalf("expected %d successful waiters, got %d", waiters-1, successCount)
	}
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
		lockTTL      = 500 * time.Millisecond
		resultTTL    = 2 * time.Second
		pollInterval = 50 * time.Millisecond
		workers      = 8
	)

	client := newTestRedisClientV9(t)
	baseBackend := NewGoRedisV9Backend(client)

	key := newBackendTestKey(t, "group:local-dedup")

	// Группа без локальной дедупликации.
	cbNo := &countingBackend{Backend: baseBackend}
	gNo := NewGroup[int](cbNo, lockTTL, resultTTL, pollInterval)

	var callsNo int32
	fnNo := func() (int, error) {
		n := atomic.AddInt32(&callsNo, 1)
		time.Sleep(10 * time.Millisecond)
		return int(n), nil
	}

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			if _, err := gNo.Do(key, fnNo); err != nil {
				t.Errorf("gNo.Do returned error: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt32(&callsNo); got != 1 {
		t.Fatalf("without local dedup fn calls = %d, want 1", got)
	}

	noDedupCalls := func() int {
		cbNo.mu.Lock()
		defer cbNo.mu.Unlock()
		return cbNo.getWithTTLCalls
	}()

	// Группа с локальной дедупликацией.
	cbLocal := &countingBackend{Backend: baseBackend}
	gLocal := NewGroup(cbLocal, lockTTL, resultTTL, pollInterval, WithLocalDeduplication[int](true))

	var callsLocal int32
	fnLocal := func() (int, error) {
		n := atomic.AddInt32(&callsLocal, 1)
		time.Sleep(10 * time.Millisecond)
		return int(n), nil
	}

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			if _, err := gLocal.Do(key, fnLocal); err != nil {
				t.Errorf("gLocal.Do returned error: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt32(&callsLocal); got != 1 {
		t.Fatalf("with local dedup fn calls = %d, want 1", got)
	}

	localDedupCalls := func() int {
		cbLocal.mu.Lock()
		defer cbLocal.mu.Unlock()
		return cbLocal.getWithTTLCalls
	}()

	if localDedupCalls >= noDedupCalls {
		t.Fatalf("expected local dedup to reduce backend GetResultWithTTL calls, got local=%d, no-local=%d",
			localDedupCalls, noDedupCalls)
	}
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
		lockTTL      = 500 * time.Millisecond
		resultTTL    = 200 * time.Millisecond
		pollInterval = 10 * time.Millisecond
		warmupWindow = 500 * time.Millisecond
	)

	g := NewGroup[int](
		backend,
		lockTTL,
		resultTTL,
		pollInterval,
		WithWarmupWindow[int](warmupWindow),
	)

	var calls int32
	fn := func() (int, error) {
		n := atomic.AddInt32(&calls, 1)
		time.Sleep(20 * time.Millisecond)
		return int(n), nil
	}

	// Первый вызов вычисляет значение и кладёт его в Redis.
	v1, err := g.Do(key, fn)
	if err != nil {
		t.Fatalf("first Do returned error: %v", err)
	}
	if v1 != 1 {
		t.Fatalf("first Do = %d, want 1", v1)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("fn calls after first Do = %d, want 1", got)
	}

	// Второй вызов должен взять старое значение из кеша и запустить прогрев в фоне.
	v2, err := g.Do(key, fn)
	if err != nil {
		t.Fatalf("second Do returned error: %v", err)
	}
	if v2 != 1 {
		t.Fatalf("second Do = %d, want 1 (cached)", v2)
	}

	// Ждём, пока фоновый прогрев выполнит fn второй раз.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&calls) >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("fn calls after warmup = %d, want 2", got)
	}

	// Третий вызов должен получить обновлённое значение 2 из кеша.
	v3, err := g.Do(key, fn)
	if err != nil {
		t.Fatalf("third Do returned error: %v", err)
	}
	if v3 != 2 {
		t.Fatalf("third Do = %d, want 2 (warmed up)", v3)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("fn calls after third Do = %d, want still 2", got)
	}
}
