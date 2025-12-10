package redisflight

import (
	"context"
	"fmt"
	"github.com/valkey-io/valkey-glide/go/v2/config"
	"strconv"
	"strings"
	"testing"
	"time"

	redisv8 "github.com/go-redis/redis/v8"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	glide "github.com/valkey-io/valkey-glide/go/v2"
)

func newBackendTestKey(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("sf:test:%s:%d", prefix, time.Now().UnixNano())
}

// newTestRedisClientV9 создаёт клиента Redis v9 и скипает тест, если Redis недоступен.
func newTestRedisClientV9(t *testing.T) *goredis.Client {
	t.Helper()

	client := goredis.NewClient(&goredis.Options{
		Addr: redisAddr(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis (v9) is not available at %s: %v", redisAddr(), err)
	}

	return client
}

// newTestRedisClientV8 создаёт клиента Redis v8 и скипает тест, если Redis недоступен.
func newTestRedisClientV8(t *testing.T) *redisv8.Client {
	t.Helper()

	client := redisv8.NewClient(&redisv8.Options{
		Addr: redisAddr(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis (v8) is not available at %s: %v", redisAddr(), err)
	}

	return client
}

// newTestValkeyGlideClient создаёт клиента Valkey GLIDE и скипает тест, если Valkey/Redis недоступен.
func newTestValkeyGlideClient(t *testing.T) *glide.Client {
	t.Helper()

	addr := strings.Split(redisAddr(), ":")
	o := &config.NodeAddress{Host: addr[0]}
	if len(addr) > 1 {
		o.Port, _ = strconv.Atoi(addr[1])
	}

	client, err := glide.NewClient(config.NewClientConfiguration().WithAddress(o))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := client.Ping(ctx); err != nil {
		t.Skipf("valkey-glide is not available at %s: %v", redisAddr(), err)
	}

	return client
}

type backendFixture struct {
	name     string
	backend  Backend
	del      func(ctx context.Context, keys ...string) error
	setNoTTL func(ctx context.Context, key string, val []byte) error
}

func redisBackends(t *testing.T) []backendFixture {
	v9 := newTestRedisClientV9(t)
	v8 := newTestRedisClientV8(t)
	valkey := newTestValkeyGlideClient(t)

	return []backendFixture{
		{
			name:    "valkey-glide",
			backend: NewValkeyGlideBackend(valkey),
			del: func(ctx context.Context, keys ...string) error {
				if len(keys) == 0 {
					return nil
				}
				_, err := valkey.Del(ctx, keys)
				return err
			},
			setNoTTL: func(ctx context.Context, key string, val []byte) error {
				_, err := valkey.Set(ctx, key, string(val))
				return err
			},
		},
		{
			name:    "v9",
			backend: NewGoRedisV9Backend(v9),
			del: func(ctx context.Context, keys ...string) error {
				return v9.Del(ctx, keys...).Err()
			},
			setNoTTL: func(ctx context.Context, key string, val []byte) error {
				return v9.Set(ctx, key, val, 0).Err()
			},
		},
		{
			name:    "v8",
			backend: NewGoRedisV8Backend(v8),
			del: func(ctx context.Context, keys ...string) error {
				return v8.Del(ctx, keys...).Err()
			},
			setNoTTL: func(ctx context.Context, key string, val []byte) error {
				return v8.Set(ctx, key, val, 0).Err()
			},
		},
	}
}

func TestBackends_GetResult_And_SetResult(t *testing.T) {
	ctx := context.Background()

	for _, fx := range redisBackends(t) {
		fx := fx
		t.Run(fx.name, func(t *testing.T) {
			key := newBackendTestKey(t, "get-"+fx.name)

			require.NoError(t, fx.del(ctx, key))

			data, found, err := fx.backend.GetResult(ctx, key)
			require.NoError(t, err)
			require.False(t, found)
			require.Nil(t, data)

			value := []byte("hello")
			require.NoError(t, fx.backend.SetResult(ctx, key, value, time.Second))

			data, found, err = fx.backend.GetResult(ctx, key)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, value, data)

			time.Sleep(1100 * time.Millisecond)

			data, found, err = fx.backend.GetResult(ctx, key)
			require.NoError(t, err)
			require.False(t, found)
			require.Nil(t, data)
		})
	}
}

func TestBackends_TryLock_And_Unlock(t *testing.T) {
	ctx := context.Background()

	for _, fx := range redisBackends(t) {
		fx := fx
		t.Run(fx.name, func(t *testing.T) {
			lockKey := newBackendTestKey(t, "lock-"+fx.name)

			require.NoError(t, fx.del(ctx, lockKey))

			ok, err := fx.backend.TryLock(ctx, lockKey, "v1", time.Second)
			require.NoError(t, err)
			require.True(t, ok)

			ok, err = fx.backend.TryLock(ctx, lockKey, "v2", time.Second)
			require.NoError(t, err)
			require.False(t, ok)

			released, err := fx.backend.Unlock(ctx, lockKey, "wrong")
			require.NoError(t, err)
			require.False(t, released)

			released, err = fx.backend.Unlock(ctx, lockKey, "v1")
			require.NoError(t, err)
			require.True(t, released)

			ok, err = fx.backend.TryLock(ctx, lockKey, "v3", time.Second)
			require.NoError(t, err)
			require.True(t, ok)
		})
	}
}

func TestBackends_UnlockAndSetResult(t *testing.T) {
	ctx := context.Background()

	for _, fx := range redisBackends(t) {
		fx := fx
		t.Run(fx.name, func(t *testing.T) {
			lockKey := newBackendTestKey(t, "unlockset:lock-"+fx.name)
			resultKey := newBackendTestKey(t, "unlockset:result-"+fx.name)

			require.NoError(t, fx.del(ctx, lockKey, resultKey))

			ok, err := fx.backend.TryLock(ctx, lockKey, "lock-ok", time.Second)
			require.NoError(t, err)
			require.True(t, ok)

			written, err := fx.backend.UnlockAndSetResult(ctx, lockKey, resultKey, "lock-ok", []byte("res"), time.Second)
			require.NoError(t, err)
			require.True(t, written)

			ok, err = fx.backend.TryLock(ctx, lockKey, "another", time.Second)
			require.NoError(t, err)
			require.True(t, ok, "lock key should have been deleted by UnlockAndSetResult")

			data, found, err := fx.backend.GetResult(ctx, resultKey)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, []byte("res"), data)

			require.NoError(t, fx.del(ctx, lockKey, resultKey))

			ok, err = fx.backend.TryLock(ctx, lockKey, "lock-bad", time.Second)
			require.NoError(t, err)
			require.True(t, ok)

			written, err = fx.backend.UnlockAndSetResult(ctx, lockKey, resultKey, "wrong-value", []byte("other"), time.Second)
			require.NoError(t, err)
			require.False(t, written, "operation should not succeed with wrong lock value")

			ok, err = fx.backend.TryLock(ctx, lockKey, "after-bad", time.Second)
			require.NoError(t, err)
			require.False(t, ok, "lock must still be held after failed UnlockAndSetResult")

			data, found, err = fx.backend.GetResult(ctx, resultKey)
			require.NoError(t, err)
			require.False(t, found)
			require.Nil(t, data)
		})
	}
}

func TestBackends_GetResultWithTTL(t *testing.T) {
	ctx := context.Background()

	for _, fx := range redisBackends(t) {
		fx := fx
		t.Run(fx.name, func(t *testing.T) {
			keyMissing := newBackendTestKey(t, "ttl:missing-"+fx.name)
			require.NoError(t, fx.del(ctx, keyMissing))

			data, found, ttl, err := fx.backend.GetResultWithTTL(ctx, keyMissing)
			require.NoError(t, err)
			require.False(t, found)
			require.Nil(t, data)
			require.Equal(t, time.Duration(0), ttl)

			keyWithTTL := newBackendTestKey(t, "ttl:with-"+fx.name)
			require.NoError(t, fx.del(ctx, keyWithTTL))

			value := []byte("ttl-value")
			require.NoError(t, fx.backend.SetResult(ctx, keyWithTTL, value, time.Second))

			data, found, ttl, err = fx.backend.GetResultWithTTL(ctx, keyWithTTL)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, value, data)
			require.Greater(t, ttl, time.Duration(0))
			require.LessOrEqual(t, ttl, time.Second)

			keyNoTTL := newBackendTestKey(t, "ttl:none-"+fx.name)
			require.NoError(t, fx.del(ctx, keyNoTTL))

			require.NoError(t, fx.setNoTTL(ctx, keyNoTTL, value))

			data, found, ttl, err = fx.backend.GetResultWithTTL(ctx, keyNoTTL)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, value, data)
			require.Equal(t, time.Duration(0), ttl)
		})
	}
}
