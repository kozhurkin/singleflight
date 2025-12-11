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
		if g.warmupWindow > 0 && ttl < g.warmupWindow {
			// Запускаем асинхронный прогрев значения.
			go g.underLock(ctx, lockKey, func(lockValue string) bool {
				res, err := fn()
				if err == nil {
					g.setResult(ctx, resultKey, res)
				}
				return true
			})
			go g.computeAndStore(ctx, lockKey, resultKey, fn)
			return res, nil
		}
		return res, nil
	}

	// Результат в кеше отсутствует — пробуем вычислить его под блокировкой.
	if res, ok, err := g.computeAndStore(ctx, lockKey, resultKey, fn); err != nil {
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
			return zero, errors.New("timeout waiting for result")
		}
	}
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

	// DATA RACE detection:
	// Между тем, как мы посмотрели кеш через getResultWithTTL и дошли до этого места,
	// другой процесс теоретически уже мог успеть записать результат.
	// Попробуем явно отловить такой сценарий: если сейчас результат уже есть в кеше,
	// снимаем блокировку и возвращаем результат без повторного вычисления.
	if res, found, err := g.getResult(ctx, resultKey); err == nil && found {
		go g.backend.Unlock(ctx, lockKey, myUUID)
		return res, true, nil
	}

	// Захватили блокировку — выполняем fn()
	res, err := fn()
	if err != nil {
		// Не снимаем блокировку через Lua — она просто истечёт по TTL.
		return zero, true, err
	}

	// Сериализуем результат, снимаем блокировку и сохраняем результат.
	if _, err := g.unlockAndSetResult(ctx, lockKey, resultKey, myUUID, res); err != nil {
		return zero, true, err
	}

	return res, true, nil
}

// setResult сериализует результат и сохраняет его в Backend с TTL результата.
func (g *Group[V]) setResult(ctx context.Context, resultKey string, res V) error {
	data, err := json.Marshal(res)
	if err != nil {
		return err
	}

	if err := g.backend.SetResult(ctx, resultKey, data, g.resultTTL+g.warmupWindow); err != nil {
		return &BackendError{Op: "SetResult", Err: err}
	}
	return nil
}

// underLock пробует захватить блокировку по lockKey и, если это удалось,
// вызывает body(lockValue). Если body вернуло true, блокировка будет снята
// через Backend.Unlock, иначе останется до TTL или явного удаления.
// Возвращаемые значения:
//   - bool  — удалось ли захватить блокировку;
//   - error — ошибка при захвате блокировки или при снятии.
func (g *Group[V]) underLock(
	ctx context.Context,
	lockKey string,
	body func(lockValue string) bool,
) (bool, error) {
	lockValue, ok, err := g.lock(ctx, lockKey)
	if err != nil || !ok {
		return ok, err
	}

	shouldUnlock := body(lockValue)

	if shouldUnlock {
		if _, err := g.backend.Unlock(ctx, lockKey, lockValue); err != nil {
			return true, &BackendError{Op: "Unlock", Err: err}
		}
	}

	return true, nil
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
