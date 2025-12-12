package redisflight

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/kozhurkin/singleflight"
)

// Group реализует распределённый singleflight поверх абстрактного Backend.
// Ключи всегда string.
type Group[V any] struct {
	backend      Backend
	lockTTL      time.Duration // TTL блокировки
	resultTTL    time.Duration // TTL хранения результата
	pollInterval time.Duration // интервал опроса результата
	warmupWindow time.Duration // окно прогрева результата (0 — прогрев отключён)
	prefix       string        // префикс для lock/result ключей
	localGroup   *singleflight.Group[string, V]
}

var (
	// ErrInvalidLockTTL означает, что lockTTL < 1ms.
	ErrInvalidLockTTL = errors.New("redisflight: lockTTL must be >= 1ms")
	// ErrInvalidResultTTL означает, что resultTTL < 1ms.
	ErrInvalidResultTTL = errors.New("redisflight: resultTTL must be >= 1ms")
	// ErrResultTTLNotGreaterThanPollInterval означает, что resultTTL <= pollInterval.
	ErrResultTTLNotGreaterThanPollInterval = errors.New("redisflight: resultTTL must be > pollInterval")
	// ErrResultUpdatedConcurrently — внутренний маркер: результат уже был обновлён другим процессом.
	ErrResultUpdatedConcurrently = errors.New("redisflight: result updated concurrently")
	// ErrTimeoutWaitingForResult означает, что ждём результат слишком долго.
	ErrTimeoutWaitingForResult = errors.New("redisflight: timeout waiting for result")
)

// NewGroup создаёт Group поверх указанного Backend с заданными базовыми параметрами.
// Дополнительные параметры могут быть заданы через опции (WithWarmupWindow, WithPrefix, WithLocalDeduplication и др.).
func NewGroup[V any](backend Backend, lockTTL, resultTTL, pollInterval time.Duration, opts ...Option[V]) *Group[V] {
	if lockTTL < time.Millisecond {
		panic(fmt.Errorf("%w: got %s", ErrInvalidLockTTL, lockTTL))
	}
	if resultTTL < time.Millisecond {
		panic(fmt.Errorf("%w: got %s", ErrInvalidResultTTL, resultTTL))
	}
	if resultTTL <= pollInterval {
		panic(fmt.Errorf("%w: resultTTL=%s, pollInterval=%s", ErrResultTTLNotGreaterThanPollInterval, resultTTL, pollInterval))
	}

	g := &Group[V]{
		backend:      backend,
		lockTTL:      lockTTL,
		resultTTL:    resultTTL,
		pollInterval: pollInterval,
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

// Do выполняет вычисление для key с использованием контекста Background.
// Для тонкого контроля отмены/дедлайнов используйте DoCtx.
func (g *Group[V]) Do(key string, fn func() (V, error)) (V, error) {
	// Если локальная дедупликация включена, используем её, чтобы несколько
	// конкурентных вызовов Do с одного процесса по одному key не ходили в Backend.
	if g.localGroup != nil {
		return g.localGroup.Do(key, func() (V, error) {
			return g.DoCtx(context.Background(), key, fn)
		})
	}

	return g.DoCtx(context.Background(), key, fn)
}

// DoCtx выполняет вычисление для key с использованием переданного контекста.
func (g *Group[V]) DoCtx(ctx context.Context, key string, fn func() (V, error)) (V, error) {
	var zero V

	lockKey := g.prefix + "lock:" + key
	resultKey := g.prefix + "result:" + key

	// Сначала пробуем получить результат из кеша вместе с TTL
	if res, found, ttl, err := g.getResultWithTTL(ctx, resultKey); err != nil {
		return zero, err
	} else if found {
		// Если прогрев включен и TTL меньше окна прогрева, запускаем асинхронный прогрев.
		if g.needWarmup(ttl) {
			// Функция-обёртка для избежания RACE condition.
			// Между тем, как мы посмотрели кеш через getResultWithTTL и запуском computeAndStore,
			// другой процесс теоретически уже мог успеть записать результат с новым TTL.
			// Попробуем явно отловить такой сценарий: если у результата в кеше обновился TTL,
			// то прогрев не нужен, кидаем ошибку, fn не будет вызвана.
			fnNoRace := func() (res V, err error) {
				if ttl, _ := g.backend.GetTTL(ctx, resultKey); g.needWarmup(ttl) {
					return fn()
				}
				return res, ErrResultUpdatedConcurrently
			}
			// Запускаем асинхронный прогрев значения.
			go g.computeAndStore(ctx, lockKey, resultKey, fnNoRace)
			return res, nil
		}
		return res, nil
	}

	// Функция-обёртка для избежания RACE condition.
	// Между тем, как мы посмотрели кеш через getResultWithTTL и дошли до этого места,
	// другой процесс теоретически уже мог успеть записать результат.
	// Попробуем явно отловить такой сценарий: если сейчас результат уже есть в кеше,
	// возвращаем результат без повторного вычисления.
	fnNoRace := func() (V, error) {
		if res, found, _ := g.getResult(ctx, resultKey); found {
			// Результат уже появился в кеше между первым чтением и захватом блокировки —
			// текущая попытка вычисления не нужна.
			return res, ErrResultUpdatedConcurrently
		}
		return fn()
	}

	// Результат в кеше отсутствует — пробуем вычислить его под блокировкой.
	res, ok, err := g.computeAndStore(ctx, lockKey, resultKey, fnNoRace)
	if err != nil {
		return zero, err
	} else if ok {
		return res, nil
	}

	// Блокировка занята — ждём появления результата в кеше
	ticker := time.NewTicker(g.pollInterval)
	defer ticker.Stop()

	timeout := time.After(g.lockTTL + g.resultTTL)

	for {
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-ticker.C:
			if res, found, err := g.getResult(ctx, resultKey); err != nil {
				return zero, err
			} else if found {
				return res, nil
			}
		case <-timeout:
			return zero, ErrTimeoutWaitingForResult
		}
	}
}

// needWarmup возвращает true, если для данного остаточного TTL нужно запускать прогрев.
func (g *Group[V]) needWarmup(ttl time.Duration) bool {
	return g.warmupWindow > 0 && ttl > 0 && ttl < g.warmupWindow
}

// computeAndStore захватывает блокировку для lockKey, вычисляет fn и,
// если всё прошло успешно, сериализует и сохраняет результат в resultKey.
// Возвращает:
//   - V: вычисленное значение (валидно только при ok == true и err == nil),
//   - bool: признак, удалось ли захватить блокировку,
//   - error: ошибка при работе с Backend или в fn.
func (g *Group[V]) computeAndStore(
	ctx context.Context,
	lockKey, resultKey string,
	fn func() (V, error),
) (V, bool, error) {
	var zero V

	myUUID, ok, err := g.lock(ctx, lockKey)
	if err != nil || !ok {
		return zero, ok, err
	}

	// Захватили блокировку — выполняем fn()
	res, err := fn()
	if err != nil {
		// Если результат уже был обновлён конкурентно (ErrResultUpdatedConcurrently),
		// просто отпускаем блокировку и не считаем это ошибкой для вызывающего кода.
		if errors.Is(err, ErrResultUpdatedConcurrently) {
			go g.backend.Unlock(ctx, lockKey, myUUID)
			return res, true, nil
		}
		// Не снимаем блокировку через Lua — она просто истечёт по TTL.
		return zero, true, err
	}

	// Снимаем блокировку и сохраняем результат одной операцией.
	if _, err := g.unlockAndSetResult(ctx, lockKey, resultKey, myUUID, res); err != nil {
		return res, true, err
	}

	return res, true, nil
}

// getResult пытается получить и распарсить результат из Backend.
// Возвращает:
//   - V: значение результата (валидно, только если found == true и err == nil),
//   - bool: признак, найден ли результат в кеше,
//   - error: ошибка при обращении к Backend или при unmarshal.
func (g *Group[V]) getResult(ctx context.Context, resultKey string) (V, bool, error) {
	var zero V

	data, found, err := g.backend.GetResult(ctx, resultKey)
	if err != nil {
		return zero, false, &BackendError{Op: "GetResult", Err: err}
	}
	if !found {
		return zero, false, nil
	}

	var res V
	if err := json.Unmarshal(data, &res); err != nil {
		return zero, false, err
	}
	return res, true, nil
}

// getResultWithTTL пытается получить и распарсить результат из Backend вместе с оставшимся TTL.
// Возвращает:
//   - V: значение результата (валидно, только если found == true и err == nil),
//   - bool: признак, найден ли результат в кеше,
//   - time.Duration: оставшийся TTL результата (0, если TTL не установлен или ключ не найден),
//   - error: ошибка при обращении к Backend или при unmarshal.
func (g *Group[V]) getResultWithTTL(ctx context.Context, resultKey string) (V, bool, time.Duration, error) {
	var zero V

	data, found, ttl, err := g.backend.GetResultWithTTL(ctx, resultKey)
	if err != nil {
		return zero, false, 0, &BackendError{Op: "GetResultWithTTL", Err: err}
	}
	if !found {
		return zero, false, 0, nil
	}

	var res V
	if err := json.Unmarshal(data, &res); err != nil {
		return zero, false, 0, err
	}
	return res, true, ttl, nil
}

// unlockAndSetResult сериализует результат, снимает блокировку и сохраняет результат в Backend.
// Возвращает:
//   - bool: признак, были ли реально сняты лок и записан результат,
//   - error: ошибка при обращении к Redis или выполнении Lua-скрипта.
func (g *Group[V]) unlockAndSetResult(ctx context.Context, lockKey, resultKey, lockValue string, res V) (bool, error) {
	data, err := json.Marshal(res)
	if err != nil {
		return false, err
	}

	ok, err := g.backend.UnlockAndSetResult(
		ctx,
		lockKey,
		resultKey,
		lockValue,
		data,
		g.resultTTL+g.warmupWindow,
	)
	if err != nil {
		return false, &BackendError{Op: "UnlockAndSetResult", Err: err}
	}

	return ok, nil
}

// lock генерирует значение блокировки и пытается установить её через Backend.
// Возвращает:
//   - string: значение блокировки (UUID), валидно только если ok == true и err == nil,
//   - bool: признак, удалось ли захватить блокировку,
//   - error: ошибка при обращении к Redis.
func (g *Group[V]) lock(ctx context.Context, lockKey string) (string, bool, error) {
	lockValue := uuid.NewString()

	ok, err := g.backend.TryLock(ctx, lockKey, lockValue, g.lockTTL)
	if err != nil {
		return "", false, &BackendError{Op: "TryLock", Err: err}
	}

	if !ok {
		return "", false, nil
	}

	return lockValue, true, nil
}
