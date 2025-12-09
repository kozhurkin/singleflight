package redisflight

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Lua-скрипты для Redis, переиспользуемые через NewScript.
var (
	luaGetWithTTLScript = goredis.NewScript(`
local v   = redis.call("GET", KEYS[1])
local ttl = redis.call("PTTL", KEYS[1])
return {v, ttl}
`)

	luaUnlockScript = goredis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
else
	return 0
end
`)

	luaUnlockAndSetResultScript = goredis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
	redis.call("del", KEYS[1])
	redis.call("set", KEYS[2], ARGV[2], "PX", ARGV[3])
	return 1
else
	return 0
end
`)
)

// GoRedisV9Backend — реализация Backend поверх github.com/redis/go-redis/v9.
type GoRedisV9Backend struct {
	client *goredis.Client
}

// NewGoRedisV9Backend оборачивает *redis.Client в Backend.
func NewGoRedisV9Backend(client *goredis.Client) *GoRedisV9Backend {
	return &GoRedisV9Backend{client: client}
}

// GetResultWithTTL атомарно получает значение и оставшийся TTL ключа через Lua-скрипт.
// Если ключ не найден, возвращает found=false, ttl=0, err=nil.
// Если ключ существует, но TTL не установлен (permanent key), возвращает found=true, ttl=0.
func (b *GoRedisV9Backend) GetResultWithTTL(ctx context.Context, resultKey string) ([]byte, bool, time.Duration, error) {
	raw, err := luaGetWithTTLScript.Run(ctx, b.client, []string{resultKey}).Result()
	if err != nil {
		return nil, false, 0, err
	}

	values, ok := raw.([]interface{})
	if !ok || len(values) != 2 {
		return nil, false, 0, fmt.Errorf("unexpected lua result shape: %#v", raw)
	}

	// Первый элемент — значение (string или nil).
	if values[0] == nil {
		// Ключа нет.
		return nil, false, 0, nil
	}

	s, ok := values[0].(string)
	if !ok {
		return nil, false, 0, fmt.Errorf("unexpected value type %T in lua result", values[0])
	}

	// Второй элемент — PTTL в миллисекундах: >=0, -1 (без TTL), -2 (нет ключа).
	ttlMs, ok := values[1].(int64)
	if !ok {
		// На практике go-redis должен возвращать int64, но если что-то пошло не так —
		// считаем TTL неизвестным (0), но значение возвращаем.
		return []byte(s), true, 0, nil
	}

	var ttl time.Duration
	if ttlMs > 0 {
		ttl = time.Duration(ttlMs) * time.Millisecond
	} else {
		// -1 (нет expire) и -2 (нет ключа — но мы уже это отфильтровали) сводим к 0.
		ttl = 0
	}

	return []byte(s), true, ttl, nil
}

func (b *GoRedisV9Backend) GetResult(ctx context.Context, resultKey string) ([]byte, bool, error) {
	data, err := b.client.Get(ctx, resultKey).Bytes()
	if err == nil {
		return data, true, nil
	}
	if err == goredis.Nil {
		return nil, false, nil
	}
	return nil, false, err
}

func (b *GoRedisV9Backend) SetResult(ctx context.Context, resultKey string, data []byte, ttl time.Duration) error {
	return b.client.Set(ctx, resultKey, data, ttl).Err()
}

func (b *GoRedisV9Backend) TryLock(ctx context.Context, lockKey, lockValue string, ttl time.Duration) (bool, error) {
	return b.client.SetNX(ctx, lockKey, lockValue, ttl).Result()
}

func (b *GoRedisV9Backend) Unlock(ctx context.Context, lockKey, lockValue string) (bool, error) {
	n, err := luaUnlockScript.Run(ctx, b.client, []string{lockKey}, lockValue).Int64()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (b *GoRedisV9Backend) UnlockAndSetResult(
	ctx context.Context,
	lockKey, resultKey, lockValue string,
	data []byte,
	ttl time.Duration,
) (bool, error) {
	n, err := luaUnlockAndSetResultScript.Run(
		ctx,
		b.client,
		[]string{lockKey, resultKey},
		lockValue,
		data,
		int(ttl.Milliseconds()),
	).Int64()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}


