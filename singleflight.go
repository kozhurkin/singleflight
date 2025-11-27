package singleflight

import (
	"sync"
	"time"

	"singleflight/inflight"
)

// FlightCache обобщённый кеш/дедупликатор запросов:
// по ключу K dedup-ит конкурентные вычисления значения V и
// опционально кеширует успешный результат на cacheTime.
type FlightCache[K comparable, V any] struct {
	mu       sync.Mutex
	inFlight map[K]*inflight.InFlight[V]
}

func NewFlightCache[K comparable, V any]() *FlightCache[K, V] {
	return &FlightCache[K, V]{
		inFlight: make(map[K]*inflight.InFlight[V]),
	}
}

func (c *FlightCache[K, V]) deleteInFlightLocked(key K) {
	c.mu.Lock()
	delete(c.inFlight, key)
	c.mu.Unlock()
}

// Do выполняет вычисление значения для key без кеширования (только дедупликация параллельных вызовов).
// fn вызывается только один раз для каждого key на единичный "рывок" запросов.
func (c *FlightCache[K, V]) Do(
	key K,
	fn func() (V, error),
) (V, error) {
	return c.DoWithCache(key, 0, false, fn)
}

// DoWithCache выполняет (или переиспользует) вычисление значения для key с опциональным кешированием.
// fn вызывается только один раз для каждого key в период cacheTime.
// Если cacheErrors == false, ошибки не кешируются (последующие вызовы будут пытаться ещё раз).
// Если cacheErrors == true, то и успешные результаты, и ошибки кешируются до истечения cacheTime.
func (c *FlightCache[K, V]) DoWithCache(
	key K,
	cacheTime time.Duration,
	cacheErrors bool,
	fn func() (V, error),
) (V, error) {
	c.mu.Lock()
	// Если для этого key запрос уже выполняется — просто ждём его
	if f, ok := c.inFlight[key]; ok {
		c.mu.Unlock()
		return f.Wait()
	}

	// Мы — первый для этого key
	f := inflight.NewInFlight(fn)
	c.inFlight[key] = f
	c.mu.Unlock()

	// Первый вызов запустит fn
	f.Run()

	// Ждём результат выполнения
	res, err := f.Wait()

	// Если cacheTime == 0 или (ошибка и мы не хотим кешировать ошибки) —
	// не кешируем: сразу удаляем запись
	if cacheTime == 0 || (!cacheErrors && err != nil) {
		c.deleteInFlightLocked(key)
		return res, err
	}

	// Успешный результат кешируем на cacheTime
	go func(key K, d time.Duration) {
		time.Sleep(d)
		c.deleteInFlightLocked(key)
	}(key, cacheTime)

	return res, err
}
