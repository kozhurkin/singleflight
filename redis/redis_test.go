package redisflight

import (
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
