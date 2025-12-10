package redisflight

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

// Group реализует распределённый singleflight поверх абстрактного Backend.
// Ключи всегда string.
type Group[V any] struct {
	backend      Backend
	lockTTL      time.Duration // TTL блокировки
	resultTTL    time.Duration // TTL хранения результата
	pollInterval time.Duration // интервал опроса результата
	warmupWindow time.Duration // окно прогрева результата (пока не используется)
}

// NewGroup создаёт Group поверх указанного Backend.
func NewGroup[V any](backend Backend, lockTTL, resultTTL, pollInterval, warmupWindow time.Duration) *Group[V] {
	return &Group[V]{
		backend:      backend,
		lockTTL:      lockTTL,
		resultTTL:    resultTTL,
		pollInterval: pollInterval,
		warmupWindow: warmupWindow,
	}
}

// Do выполняет вычисление для key с использованием контекста Background.
// Для тонкого контроля отмены/дедлайнов используйте DoCtx.
func (g *Group[V]) Do(key string, fn func() (V, error)) (V, error) {
	return g.DoCtx(context.Background(), key, fn)
}

// DoCtx выполняет вычисление для key с использованием переданного контекста.
func (g *Group[V]) DoCtx(ctx context.Context, key string, fn func() (V, error)) (V, error) {
	var zero V

	lockKey := "lock:" + key
	resultKey := "result:" + key

	// Сначала пробуем получить результат из кеша вместе с TTL
	if res, found, ttl, err := g.getResultWithTTL(ctx, resultKey); err != nil {
		return zero, err
	} else if found {
		// Если прогрев включен и TTL меньше окна прогрева, запускаем асинхронный прогрев.
		if g.warmupWindow > 0 && ttl < g.warmupWindow {
			// Запускаем асинхронный прогрев значения.
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
		g.resultTTL,
	)
	if err != nil {
		return false, &BackendError{Op: "UnlockAndSetResult", Err: err}
	}

	return ok, nil
}

// unlock пытается снять блокировку, только если она всё ещё принадлежит указанному значению lockValue.
// Делегирует работу Backend'у, чтобы сравнение значения и удаление ключа были атомарными.
// Возвращает:
//   - bool: признак, была ли реально снята блокировка,
//   - error: ошибка при обращении к Redis.
func (g *Group[V]) unlock(ctx context.Context, lockKey, lockValue string) (bool, error) {
	ok, err := g.backend.Unlock(ctx, lockKey, lockValue)
	if err != nil {
		return false, &BackendError{Op: "Unlock", Err: err}
	}
	return ok, nil
}

// setResult сериализует результат и сохраняет его в Backend с TTL результата.
// Этот метод не взаимодействует с блокировкой.
func (g *Group[V]) setResult(ctx context.Context, resultKey string, res V) error {
	data, err := json.Marshal(res)
	if err != nil {
		return err
	}

	if err := g.backend.SetResult(ctx, resultKey, data, g.resultTTL); err != nil {
		return &BackendError{Op: "SetResult", Err: err}
	}
	return nil
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