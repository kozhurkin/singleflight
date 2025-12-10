package redisflight

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// KVClient — минимальный интерфейс для операций с ключом/значением.
// Конкретные адаптеры (go-redis v9/v8, Valkey GLIDE и др.) реализуют его поверх своих клиентов.
type KVClient interface {
	// GetBytes возвращает байты или ошибку.
	// Если ключ не существует, адаптер ДОЛЖЕН вернуть (nil, nil).
	// Если ключ существует и значение пустое, он возвращает ([]byte{}, nil).
	GetBytes(ctx context.Context, key string) ([]byte, error)

	// SetBytes сохраняет значение с TTL.
	SetBytes(ctx context.Context, key string, value []byte, ttl time.Duration) error

	// SetNX пытается установить значение с TTL, если ключ не существует.
	SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
}

// LuaScript — минимальный интерфейс для Lua-скрипта.
type LuaScript interface {
	// Run выполняет скрипт для набора ключей (KEYS) и строковых аргументов (ARGV)
	// и возвращает "сырое" значение, как его вернул Redis.
	Run(ctx context.Context, keys []string, args ...string) (any, error)
}

// Экспортируемые ошибки, которые может возвращать Redis-бэкенд.
var (
	// ErrInvalidLuaResult означает, что Lua-скрипт вернул результат неожиданного формата.
	ErrInvalidLuaResult = errors.New("redis backend: invalid lua result shape")

	// ErrInvalidLuaValueType означает, что Lua-скрипт вернул значение неожиданного типа.
	ErrInvalidLuaValueType = errors.New("redis backend: invalid lua value type")

	// ErrInvalidUnlockResult означает, что результат разблокировки имеет неожиданный тип.
	ErrInvalidUnlockResult = errors.New("redis backend: invalid unlock result type")

	// ErrInvalidUnlockAndSetResult означает, что результат unlock+set имеет неожиданный тип.
	ErrInvalidUnlockAndSetResult = errors.New("redis backend: invalid unlockAndSetResult result type")
)

// parseGetWithTTL разбирает ответ Lua-скрипта {value, ttlMs} в удобный вид.
// Ожидаемый формат:
//
//	{ v, ttl }
//
// где v — string или nil, ttl — int64 (PTTL в миллисекундах).
func parseGetWithTTL(raw any) (data []byte, found bool, ttl time.Duration, err error) {
	values, ok := raw.([]any)
	if !ok || len(values) != 2 {
		return nil, false, 0, fmt.Errorf("%w: %#v", ErrInvalidLuaResult, raw)
	}

	// Первый элемент — значение (string или nil).
	if values[0] == nil {
		// Ключа нет.
		return nil, false, 0, nil
	}

	s, ok := values[0].(string)
	if !ok {
		return nil, false, 0, fmt.Errorf("%w: %T", ErrInvalidLuaValueType, values[0])
	}

	// Второй элемент — PTTL в миллисекундах: >=0, -1 (без TTL), -2 (нет ключа).
	ttlMs, ok := values[1].(int64)
	if !ok {
		// На практике клиент должен возвращать int64, но если что-то пошло не так —
		// считаем TTL неизвестным (0), но значение возвращаем.
		return []byte(s), true, 0, nil
	}

	if ttlMs > 0 {
		return []byte(s), true, time.Duration(ttlMs) * time.Millisecond, nil
	}

	// -1 (нет expire) и -2 (нет ключа — но мы уже это отфильтровали) сводим к 0.
	return []byte(s), true, 0, nil
}

// Общие Lua-скрипты для Redis-бэкенда.
const (
	luaGetWithTTLSource = `
local v   = redis.call("GET", KEYS[1])
local ttl = redis.call("PTTL", KEYS[1])
return {v, ttl}
`

	luaUnlockSource = `
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
else
	return 0
end
`

	luaUnlockAndSetSource = `
if redis.call("get", KEYS[1]) == ARGV[1] then
	redis.call("del", KEYS[1])
	redis.call("set", KEYS[2], ARGV[2], "PX", ARGV[3])
	return 1
else
	return 0
end
`
)

// scripts инкапсулирует Lua-скрипты, используемые Backend'ом.
type scripts struct {
	getWithTTL         LuaScript
	unlock             LuaScript
	unlockAndSet       LuaScript
}

// redisBackend — реализация Backend поверх абстрактного KVClient и LuaScript.
type redisBackend struct {
	kv      KVClient
	scripts scripts
}

// GetResultWithTTL атомарно получает значение и оставшийся TTL ключа через Lua-скрипт.
// Если ключ не найден, возвращает found=false, ttl=0, err=nil.
// Если ключ существует, но TTL не установлен (permanent key), возвращает found=true, ttl=0.
func (b *redisBackend) GetResultWithTTL(ctx context.Context, resultKey string) ([]byte, bool, time.Duration, error) {
	raw, err := b.scripts.getWithTTL.Run(ctx, []string{resultKey})
	if err != nil {
		return nil, false, 0, err
	}
	return parseGetWithTTL(raw)
}

func (b *redisBackend) GetResult(ctx context.Context, resultKey string) ([]byte, bool, error) {
	data, err := b.kv.GetBytes(ctx, resultKey)
	if err != nil {
		return nil, false, err
	}
	if data == nil {
		// Ключ не найден.
		return nil, false, nil
	}
	return data, true, nil
}

func (b *redisBackend) SetResult(ctx context.Context, resultKey string, data []byte, ttl time.Duration) error {
	return b.kv.SetBytes(ctx, resultKey, data, ttl)
}

func (b *redisBackend) TryLock(ctx context.Context, lockKey, lockValue string, ttl time.Duration) (bool, error) {
	return b.kv.SetNX(ctx, lockKey, lockValue, ttl)
}

func (b *redisBackend) Unlock(ctx context.Context, lockKey, lockValue string) (bool, error) {
	raw, err := b.scripts.unlock.Run(ctx, []string{lockKey}, lockValue)
	if err != nil {
		return false, err
	}
	n, ok := raw.(int64)
	if !ok {
		return false, fmt.Errorf("%w: %T", ErrInvalidUnlockResult, raw)
	}
	return n > 0, nil
}

func (b *redisBackend) UnlockAndSetResult(
	ctx context.Context,
	lockKey, resultKey, lockValue string,
	data []byte,
	ttl time.Duration,
) (bool, error) {
	raw, err := b.scripts.unlockAndSet.Run(
		ctx,
		[]string{lockKey, resultKey},
		lockValue,
		string(data),
		strconv.FormatInt(ttl.Milliseconds(), 10),
	)
	if err != nil {
		return false, err
	}
	n, ok := raw.(int64)
	if !ok {
		return false, fmt.Errorf("%w: %T", ErrInvalidUnlockAndSetResult, raw)
	}
	return n > 0, nil
}
