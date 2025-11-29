package singleflight

import (
	"sync"
	"time"

	"github.com/kozhurkin/singleflight/flight"
)

// Group — обобщённый дедупликатор/кеш запросов:
// по ключу K объединяет конкурентные вычисления значения V и
// опционально кеширует результат на cacheTime с поддержкой прогрева (warmTime).
type Group[K comparable, V any] struct {
	mu          sync.Mutex
	flights     map[K]*flight.Flight[V]
	warmings    map[K]*flight.Flight[V]
	cacheTime   time.Duration
	cacheErrors bool
	warmTime    time.Duration
}

// NewGroup создаёт новый экземпляр Group без кеширования (только дедупликация).
func NewGroup[K comparable, V any]() *Group[K, V] {
	return &Group[K, V]{
		flights: make(map[K]*flight.Flight[V]),
	}
}

// NewGroupWithCache создаёт новый Group с параметрами кеширования.
// cacheTime задаёт TTL кеша, cacheErrors определяет, нужно ли кешировать ошибки.
// warmTime задаёт время ожидания прогрева перед удалением ключа из кеша.
func NewGroupWithCache[K comparable, V any](cacheTime time.Duration, cacheErrors bool, warmTime time.Duration) *Group[K, V] {
	return &Group[K, V]{
		flights:     make(map[K]*flight.Flight[V]),
		warmings:    make(map[K]*flight.Flight[V]),
		cacheTime:   cacheTime,
		cacheErrors: cacheErrors,
		warmTime:    warmTime,
	}
}

// getOrCreateFlight атомарно получает существующий Flight по key
// или создаёт новый, если его ещё нет.
// Возвращает пару (Flight, created), где created == true, если запись была создана.
func (c *Group[K, V]) getOrCreateFlight(
	key K,
	fn func() (V, error),
) (f *flight.Flight[V], created bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Если для этого key уже есть вычисление — просто возвращаем его
	if existing, ok := c.flights[key]; ok {
		return existing, false
	}

	// Мы — первый для этого key: создаём Flight и сохраняем
	f = flight.NewFlight(fn)
	c.flights[key] = f
	return f, true
}

func (c *Group[K, V]) deleteFlight(key K) {
	c.mu.Lock()
	delete(c.flights, key)
	c.mu.Unlock()
}

// startWarmingIfNeeded проверяет флаг ожидания прогрева для key и,
// если он установлен, создаёт разогревочный Flight и запускает его.
func (c *Group[K, V]) startWarmingIfNeeded(
	key K,
	fn func() (V, error),
) {
	c.mu.Lock()
	if wf, ok := c.warmings[key]; !ok || wf != nil {
		// нет флага, или прогрев уже запущен
		c.mu.Unlock()
		return
	}

	wf := flight.NewFlight(fn)
	c.warmings[key] = wf
	c.mu.Unlock()
	go wf.Run()
}

// wrapFn оборачивает пользовательскую fn логикой кеширования и прогрева.
func (c *Group[K, V]) wrapFn(
	key K,
	fn func() (V, error),
) func() (V, error) {
	return func() (V, error) {
		res, err := fn()

		// Если cacheTime == 0 или (ошибка и мы не хотим кешировать ошибки) —
		// не кешируем: сразу удаляем запись
		if c.cacheTime == 0 || (!c.cacheErrors && err != nil) {
			c.deleteFlight(key)
			return res, err
		}
		// Успешный результат кешируем на cacheTime
		go func(key K, cacheTime time.Duration) {
			time.Sleep(cacheTime)
			if c.warmTime == 0 {
				c.deleteFlight(key)
				return
			}

			// ставим метку на разогрев
			c.markWarmingPending(key)

			// ждём разогрева и применяем результат
			time.Sleep(c.warmTime)
			c.applyWarmResult(key)
		}(key, c.cacheTime)

		return res, err
	}
}

// Do выполняет (или переиспользует) вычисление значения для key с учётом настроек кеша.
// fn вызывается только один раз для каждого key в период cacheTime (если cacheTime > 0).
// Если cacheErrors == false, ошибки не кешируются (последующие вызовы будут пытаться ещё раз).
// Если cacheErrors == true, то и успешные результаты, и ошибки кешируются до истечения cacheTime.
func (c *Group[K, V]) Do(
	key K,
	fn func() (V, error),
) (V, error) {
	fnWrapped := c.wrapFn(key, fn)
	// Атомарно получаем существующий Flight или создаём новый
	f, created := c.getOrCreateFlight(key, fnWrapped)

	// Если мы не создавали Flight (присоединились к уже существующему),
	// просто ждём результат и выходим — как и в исходной логике.
	if !created {
		c.startWarmingIfNeeded(key, fnWrapped)
		return f.Wait()
	}

	// Мы первые взяли блокировку на этот key: запускаем fn
	f.Run()

	return f.Wait()
}

// markWarmingPending помечает key как ожидающий прогрева.
func (c *Group[K, V]) markWarmingPending(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.warmings[key] = nil
}

// applyWarmResult применяет результат прогрева (если был) или очищает ключ.
func (c *Group[K, V]) applyWarmResult(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if wf := c.warmings[key]; wf == nil {
		// прогрев не запускался, очищаем
		delete(c.flights, key)
	} else {
		// прогрев запускался, перемещаем
		c.flights[key] = wf
	}
	delete(c.warmings, key)
}
