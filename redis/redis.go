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
}

// NewGroup создаёт Group поверх указанного Backend.
func NewGroup[V any](backend Backend, lockTTL, resultTTL, pollInterval time.Duration) *Group[V] {
	return &Group[V]{
		backend:      backend,
		lockTTL:      lockTTL,
		resultTTL:    resultTTL,
		pollInterval: pollInterval,
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

	// Сначала пробуем получить результат из кеша
	if res, found, err := g.getResult(ctx, resultKey); err != nil {
		return zero, err
	} else if found {
		return res, nil
	}

	// Результат в кеше отсутствует — пробуем захватить блокировку
	myUUID, ok, err := g.lock(ctx, lockKey)
	if err != nil {
		return zero, err
	}

	if ok {
		// Захватили блокировку — выполняем fn()
		res, err := fn()
		if err != nil {
			// Не снимаем блокировку через Lua — она просто истечёт по TTL
			return zero, err
		}

		// Сериализуем результат, снимаем блокировку и сохраняем результат
		if _, err := g.unlockAndSetResult(ctx, lockKey, resultKey, myUUID, res); err != nil {
			return zero, err
		}

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
//   - error: ошибка при обращении к Redis или при unmarshal.
func (g *Group[V]) getResult(ctx context.Context, resultKey string) (V, bool, error) {
	var zero V

	data, found, err := g.backend.GetResult(ctx, resultKey)
	if err != nil || !found {
		return zero, found, err
	}

	var res V
	if err := json.Unmarshal(data, &res); err != nil {
		return zero, false, err
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

	return g.backend.UnlockAndSetResult(
		ctx,
		lockKey,
		resultKey,
		lockValue,
		data,
		g.resultTTL,
	)
}

// unlock пытается снять блокировку, только если она всё ещё принадлежит указанному значению lockValue.
// Делегирует работу Backend'у, чтобы сравнение значения и удаление ключа были атомарными.
// Возвращает:
//   - bool: признак, была ли реально снята блокировка,
//   - error: ошибка при обращении к Redis.
func (g *Group[V]) unlock(ctx context.Context, lockKey, lockValue string) (bool, error) {
	return g.backend.Unlock(ctx, lockKey, lockValue)
}

// setResult сериализует результат и сохраняет его в Backend с TTL результата.
// Этот метод не взаимодействует с блокировкой.
func (g *Group[V]) setResult(ctx context.Context, resultKey string, res V) error {
	data, err := json.Marshal(res)
	if err != nil {
		return err
	}

	return g.backend.SetResult(ctx, resultKey, data, g.resultTTL)
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
		return "", false, err
	}

	if !ok {
		return "", false, nil
	}

	return lockValue, true, nil
}