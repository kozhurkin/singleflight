package redisflight

import (
	"context"
	"fmt"
	"time"

	"github.com/valkey-io/valkey-glide/go/v2/constants"
	"github.com/valkey-io/valkey-glide/go/v2/models"
	"github.com/valkey-io/valkey-glide/go/v2/options"
)

type glideCommands interface {
	Get(ctx context.Context, key string) (models.Result[string], error)
	SetWithOptions(ctx context.Context, key string, value string, opts options.SetOptions) (models.Result[string], error)
	InvokeScriptWithOptions(ctx context.Context, script options.Script, opts options.ScriptOptions) (any, error)
}

// glideKVClient реализует KVClient поверх клиента Valkey GLIDE,
// удовлетворяющего интерфейсу glideCommands (например, *glide.Client или *glide.ClusterClient).
type glideKVClient struct {
	c glideCommands
}

// GetBytes должен вернуть значение ключа в виде []byte или ошибку.
func (v glideKVClient) GetBytes(ctx context.Context, key string) ([]byte, error) {
	res, err := v.c.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if res.IsNil() {
		return nil, nil
	}
	return []byte(res.Value()), nil
}

// SetBytes должен сохранить значение с TTL.
func (v glideKVClient) SetBytes(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	_, err := v.c.SetWithOptions(ctx, key, string(value), options.SetOptions{
		Expiry: &options.Expiry{
			Type:     constants.Milliseconds,
			Duration: uint64(ttl.Milliseconds()),
		},
	})
	return err
}

// SetNX должен попытаться установить значение, если ключ не существует (аналог SETNX) с TTL.
func (v glideKVClient) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	result, err := v.c.SetWithOptions(ctx, key, value, options.SetOptions{
		ConditionalSet: constants.OnlyIfDoesNotExist,
		Expiry: &options.Expiry{
			Type:     constants.Milliseconds,
			Duration: uint64(ttl.Milliseconds()),
		},
	})
	if err != nil {
		return false, err
	}
	return !result.IsNil(), nil
}

// TTL возвращает оставшийся TTL ключа. При отсутствии ключа или TTL возвращает 0, nil.
func (v glideKVClient) TTL(ctx context.Context, key string) (time.Duration, error) {
	// В GLIDE нет прямого метода PTTL в интерфейсе glideCommands, поэтому используем Lua-скрипт.
	script := *options.NewScript("return redis.call('PTTL', KEYS[1])")

	res, err := v.c.InvokeScriptWithOptions(ctx, script, options.ScriptOptions{
		Keys: []string{key},
	})
	if err != nil {
		return 0, err
	}

	ttlMs, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("glide TTL: unexpected result type %T", res)
	}

	if ttlMs <= 0 {
		return 0, nil
	}
	return time.Duration(ttlMs) * time.Millisecond, nil
}

// glideLuaScript реализует LuaScript поверх Valkey GLIDE с помощью InvokeScriptWithOptions.
type glideLuaScript struct {
	c glideCommands
	s string
}

func (s glideLuaScript) Run(ctx context.Context, keys []string, args ...string) (any, error) {
	script := *options.NewScript(s.s)

	scriptOpts := options.ScriptOptions{
		Keys: keys,
		Args: args,
	}

	return s.c.InvokeScriptWithOptions(ctx, script, scriptOpts)
}

// NewValkeyGlideBackend создаёт Backend поверх клиента Valkey GLIDE,
// удовлетворяющего интерфейсу glideCommands (например, *glide.Client или *glide.ClusterClient).
func NewValkeyGlideBackend(c glideCommands) Backend {
	kv := glideKVClient{c: c}

	return &redisBackend{
		kv: kv,
		scripts: scripts{
			getWithTTL:   glideLuaScript{c: c, s: luaGetWithTTLSource},
			unlock:       glideLuaScript{c: c, s: luaUnlockSource},
			unlockAndSet: glideLuaScript{c: c, s: luaUnlockAndSetSource},
		},
	}
}
