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

// wrapWithCacheLifecycle оборачивает пользовательскую fn логикой кеширования и прогрева.
func (g *Group[K, V]) wrapWithCacheLifecycle(key K) func(res V, err error) {
	return func(res V, err error) {
		// Если кеш выключен, то не кешируем: освобождаем ключ
		if g.cacheTime == 0 {
			g.deleteKey(key)
		}
		// Если ошибка и мы не хотим кешировать ошибки, то не кешируем: освобождаем ключ
		if !g.cacheErrors && err != nil {
			g.deleteKey(key)
		}
		// Если нужно кешировать, то откладываем освобождение ключа на cacheTime асинхронно
		go func(key K, cacheTime time.Duration) {
			time.Sleep(cacheTime)

			// Если прогрев выключен, то не прогреваем: освобождаем ключ
			if g.warmTime == 0 {
				g.deleteKey(key)
				return
			}

			// ПРОГРЕВ

			// Помечаем ключ как ожидающий прогрева
			g.markWarmingPending(key)

			// Ожидаем время прогрева и применяем результат, если прогрев случился
			time.Sleep(g.warmTime)

			// Снимаем метку ожидания прогрева
			g.unmarkWarmingPending(key)
		}(key, g.cacheTime)
	}
}

// Do выполняет (или переиспользует) вычисление значения для key с учётом настроек кеша.
// fn вызывается только один раз для каждого key в период cacheTime (если cacheTime > 0).
// Если cacheErrors == false, ошибки не кешируются (последующие вызовы будут пытаться ещё раз).
// Если cacheErrors == true, то и успешные результаты, и ошибки кешируются до истечения cacheTime.
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
			// Применяем результат прогрева
			if f := g.applyWarmingResult(key); f != nil {
				f.After(g.wrapWithCacheLifecycle(key))
			}
		}()
	}

	// Если мы не создавали Flight (присоединились к уже существующему),
	// просто ждём результат и выходим.
	if !created {
		return f.Wait()
	}

	// Мы первые взяли блокировку на этот key: запускаем fn
	f.Run()

	f.After(g.wrapWithCacheLifecycle(key))

	return f.Wait()
}

// markWarmingPending помечает key как ожидающий прогрева.
func (g *Group[K, V]) markWarmingPending(key K) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.warmings[key]; !ok {
		// ставим метку ожидания прогрева только если ключ не ожидает прогрева и не прогревается
		g.warmings[key] = nil
	}
}

// unmarkWarmingPending снимает метку ожидания прогрева для key, если она была установлена.
// Используется, когда необходимо отменить pending-прогрев, не затрагивая уже запущенный wf.
func (g *Group[K, V]) unmarkWarmingPending(key K) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if wf, ok := g.warmings[key]; ok && wf == nil {
		delete(g.warmings, key)
	}
}

// applyWarmingResult применяет результат прогрева (если был) или очищает ключ.
func (g *Group[K, V]) applyWarmingResult(key K) *flight.Flight[V] {
	g.mu.Lock()
	defer g.mu.Unlock()

	wf, ok := g.warmings[key]

	if !ok {
		// прогрев был применен параллельной горутиной, просто выходим
		return nil
	}

	if wf == nil {
		// прогрев не запускался, очищаем
		delete(g.flights, key)
		delete(g.warmings, key)
		return nil
	} else {
		// прогрев запускался, перемещаем
		g.flights[key] = wf
		delete(g.warmings, key)
		return wf
	}
}
