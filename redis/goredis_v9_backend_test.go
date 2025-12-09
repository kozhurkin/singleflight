package redisflight

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newBackendTestKey(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("sf:test:%s:%d", prefix, time.Now().UnixNano())
}

func TestGoRedisV9Backend_GetResult_And_SetResult(t *testing.T) {
	client := newTestRedisClient(t)
	backend := NewGoRedisV9Backend(client)
	ctx := context.Background()

	key := newBackendTestKey(t, "get")

	// Убедимся, что ключа нет
	_ = client.Del(ctx, key).Err()

	// При отсутствии ключа: found=false, data=nil, err=nil
	data, found, err := backend.GetResult(ctx, key)
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, data)

	// Сохраняем значение через SetResult
	value := []byte("hello")
	require.NoError(t, backend.SetResult(ctx, key, value, time.Second))

	// Теперь ключ должен читаться
	data, found, err = backend.GetResult(ctx, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, value, data)

	// Через TTL значение должно исчезнуть
	time.Sleep(1100 * time.Millisecond)

	data, found, err = backend.GetResult(ctx, key)
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, data)
}

func TestGoRedisV9Backend_TryLock_And_Unlock(t *testing.T) {
	client := newTestRedisClient(t)
	backend := NewGoRedisV9Backend(client)
	ctx := context.Background()

	lockKey := newBackendTestKey(t, "lock")

	_ = client.Del(ctx, lockKey).Err()

	// Первый захват блокировки должен быть успешным
	ok, err := backend.TryLock(ctx, lockKey, "v1", time.Second)
	require.NoError(t, err)
	require.True(t, ok)

	// Повторный захват с другим значением должен вернуть ok=false
	ok, err = backend.TryLock(ctx, lockKey, "v2", time.Second)
	require.NoError(t, err)
	require.False(t, ok)

	// Снятие блокировки с неверным значением не должно ничего удалить
	released, err := backend.Unlock(ctx, lockKey, "wrong")
	require.NoError(t, err)
	require.False(t, released)

	// Снятие блокировки с корректным значением должно сработать
	released, err = backend.Unlock(ctx, lockKey, "v1")
	require.NoError(t, err)
	require.True(t, released)

	// Теперь блокировка снова должна захватываться
	ok, err = backend.TryLock(ctx, lockKey, "v3", time.Second)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestGoRedisV9Backend_UnlockAndSetResult(t *testing.T) {
	client := newTestRedisClient(t)
	backend := NewGoRedisV9Backend(client)
	ctx := context.Background()

	lockKey := newBackendTestKey(t, "unlockset:lock")
	resultKey := newBackendTestKey(t, "unlockset:result")

	_ = client.Del(ctx, lockKey, resultKey).Err()

	// Сценарий: корректное значение блокировки
	ok, err := backend.TryLock(ctx, lockKey, "lock-ok", time.Second)
	require.NoError(t, err)
	require.True(t, ok)

	written, err := backend.UnlockAndSetResult(ctx, lockKey, resultKey, "lock-ok", []byte("res"), time.Second)
	require.NoError(t, err)
	require.True(t, written)

	// Лок должен быть снят, результат записан
	ok, err = backend.TryLock(ctx, lockKey, "another", time.Second)
	require.NoError(t, err)
	require.True(t, ok, "lock key should have been deleted by UnlockAndSetResult")

	data, found, err := backend.GetResult(ctx, resultKey)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("res"), data)

	// Сценарий: некорректное значение блокировки — ничего не должно измениться
	_ = client.Del(ctx, lockKey, resultKey).Err()

	ok, err = backend.TryLock(ctx, lockKey, "lock-bad", time.Second)
	require.NoError(t, err)
	require.True(t, ok)

	written, err = backend.UnlockAndSetResult(ctx, lockKey, resultKey, "wrong-value", []byte("other"), time.Second)
	require.NoError(t, err)
	require.False(t, written, "operation should not succeed with wrong lock value")

	// Лок всё ещё существует
	ok, err = backend.TryLock(ctx, lockKey, "after-bad", time.Second)
	require.NoError(t, err)
	require.False(t, ok, "lock must still be held after failed UnlockAndSetResult")

	// Результата быть не должно
	data, found, err = backend.GetResult(ctx, resultKey)
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, data)
}

func TestGoRedisV9Backend_GetResultWithTTL(t *testing.T) {
	client := newTestRedisClient(t)
	backend := NewGoRedisV9Backend(client)
	ctx := context.Background()

	// Ключ, которого нет
	keyMissing := newBackendTestKey(t, "ttl:missing")
	_ = client.Del(ctx, keyMissing).Err()

	data, found, ttl, err := backend.GetResultWithTTL(ctx, keyMissing)
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, data)
	require.Equal(t, time.Duration(0), ttl)

	// Ключ с TTL
	keyWithTTL := newBackendTestKey(t, "ttl:with")
	_ = client.Del(ctx, keyWithTTL).Err()

	value := []byte("ttl-value")
	require.NoError(t, backend.SetResult(ctx, keyWithTTL, value, time.Second))

	data, found, ttl, err = backend.GetResultWithTTL(ctx, keyWithTTL)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, value, data)
	require.Greater(t, ttl, time.Duration(0))
	require.LessOrEqual(t, ttl, time.Second)

	// Ключ без TTL (permanent key)
	keyNoTTL := newBackendTestKey(t, "ttl:none")
	_ = client.Del(ctx, keyNoTTL).Err()

	require.NoError(t, client.Set(ctx, keyNoTTL, value, 0).Err())

	data, found, ttl, err = backend.GetResultWithTTL(ctx, keyNoTTL)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, value, data)
	require.Equal(t, time.Duration(0), ttl)
}


