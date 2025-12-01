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
	cacheErrors time.Duration
	warmTime    time.Duration
}

// NewGroup создаёт новый экземпляр Group без кеширования (только дедупликация).
func NewGroup[K comparable, V any]() *Group[K, V] {
	return &Group[K, V]{
		flights: make(map[K]*flight.Flight[V]),
	}
}

// NewGroupWithCache создаёт новый Group с параметрами кеширования.
// cacheTime задаёт TTL кеша для успешных результатов.
// cacheErrors задаёт TTL кеша для ошибок (0 — ошибки не кешируются).
// warmTime задаёт время ожидания прогрева перед удалением ключа из кеша.
func NewGroupWithCache[K comparable, V any](cacheTime time.Duration, cacheErrors time.Duration, warmTime time.Duration) *Group[K, V] {
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
// Возвращает (основной Flight, created, разогревочный Flight), где:
//   - f - основной Flight
//   - created - true, если основной Flight был создан в этом вызове
//   - wf - разогревочный Flight
func (g *Group[K, V]) getOrCreateFlight(
	key K,
	fn func() (V, error),
) (f *flight.Flight[V], created bool, wf *flight.Flight[V]) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Если для этого key уже есть вычисление — возвращаем его
	if f, ok := g.flights[key]; ok {
		// Попробуем атомарно захватить "флаг прогрева":
		// если в warmings[key] лежит именно nil — значит,
		// помечен pending-прогрев, но ещё не создан разогревочный Flight.
		if g.warmTime > 0 {
			if wf, ok := g.warmings[key]; ok && wf == nil {
				wf = flight.NewFlight(fn)
				g.warmings[key] = wf
				return f, false, wf
			}
		}

		return f, false, nil
	}

	// Мы — первый для этого key: создаём Flight и сохраняем
	f = flight.NewFlight(fn)
	g.flights[key] = f
	return f, true, nil
}

func (g *Group[K, V]) deleteKey(key K) {
	g.mu.Lock()
	defer g.mu.Unlock()

	delete(g.flights, key)
	if g.warmings != nil {
		delete(g.warmings, key)
	}
}

// cacheFinalizerForKey возвращает функцию, которая применяет правила кеширования
// и прогрева для результата вычисления по ключу key.
func (g *Group[K, V]) cacheFinalizerForKey(key K) func(res V, err error) {
	return func(res V, err error) {
		// Если кеш выключен, то не кешируем: освобождаем ключ
		if g.cacheTime == 0 {
			g.deleteKey(key)
			return
		}
		// Ошибка и cacheErrors == 0: не кешируем ошибку, сразу освобождаем ключ
		if err != nil && g.cacheErrors == 0 {
			g.deleteKey(key)
			return
		}

		// Кешируем результат: успешный — на cacheTime,
		// ошибочный — на cacheErrors.
		go func(key K, err error) {
			if err == nil {
				time.Sleep(g.cacheTime)
			} else {
				time.Sleep(g.cacheErrors)
			}

			// Если прогрев выключен, то не прогреваем: освобождаем ключ
			if g.warmTime == 0 {
				g.deleteKey(key)
				return
			}

			// ПРОГРЕВ

			// Помечаем ключ как ожидающий прогрева
			g.markWarmupPending(key)

			// Ожидаем время прогрева и применяем результат, если прогрев случился
			time.Sleep(g.warmTime)

			// Если прогрев так и не стартовал — очищаем pending-состояние и удаляем ключ из кеша
			g.clearWarmupPending(key)
		}(key, err)
	}
}

// Do выполняет (или переиспользует) вычисление значения для key с учётом настроек кеша.
// fn вызывается только один раз для каждого key в период cacheTime (если cacheTime > 0).
// Ошибки кешируются отдельно: при cacheErrors == 0 ошибки не кешируются (последующие
// вызовы будут пытаться ещё раз), при cacheErrors > 0 ошибки живут в кеше cacheErrors.
func (g *Group[K, V]) Do(
	key K,
	fn func() (V, error),
) (V, error) {
	// Атомарно под одним lock:
	//   - получаем основной Flight (существующий или новый),
	//   - при необходимости создаём разогревочный Flight.
	f, created, wf := g.getOrCreateFlight(key, fn)

	// Если под блокировкой был создан разогревочный Flight — запускаем его
	if wf != nil {
		go func() {
			wf.Run()
			// Применяем результат прогрева: делаем wf основным Flight для key
			g.promoteWarmingFlight(key, wf)
			// Применяем правила кеширования/прогрева к результату прогрева
			wf.OnDone(g.cacheFinalizerForKey(key))
		}()
	}

	// Если мы не создавали Flight (присоединились к уже существующему),
	// просто ждём результат и выходим.
	if !created {
		return f.Wait()
	}

	// Мы первые взяли блокировку на этот key: запускаем fn
	f.Run()
	// Применяем правила кеширования/прогрева к результату основного вычисления
	f.OnDone(g.cacheFinalizerForKey(key))

	return f.Wait()
}

// markWarmupPending помечает key как ожидающий прогрева.
func (g *Group[K, V]) markWarmupPending(key K) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.warmings[key]; !ok {
		// ставим метку ожидания прогрева только если ключ не ожидает прогрева и не прогревается
		g.warmings[key] = nil
	}
}

// clearWarmupPending очищает pending-состояние прогрева для key и удаляет ключ из кеша,
// если по истечении окна прогрева warmup так и не был запущен.
func (g *Group[K, V]) clearWarmupPending(key K) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if wf, ok := g.warmings[key]; ok && wf == nil {
		delete(g.warmings, key)
		delete(g.flights, key)
	}
}

// promoteWarmingFlight под блокировкой делает разогревочный wf основным Flight
// для key (перемещает wf из warmings в flights).
func (g *Group[K, V]) promoteWarmingFlight(key K, wf *flight.Flight[V]) *flight.Flight[V] {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.flights[key] = wf
	if g.warmings != nil {
		delete(g.warmings, key)
	}
	return wf
}
