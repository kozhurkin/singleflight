package redisflight

import (
	"context"
	"time"

	glide "github.com/valkey-io/valkey-glide/go/v2"
	"github.com/valkey-io/valkey-glide/go/v2/constants"
	"github.com/valkey-io/valkey-glide/go/v2/options"
)

// glideKVClient реализует KVClient поверх *glide.Client.
type glideKVClient struct {
	c *glide.Client
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

// glideLuaScript — каркас реализации LuaScript поверх Valkey GLIDE.
// В продакшене её нужно реализовать через поддержку EVAL/EVALSHA (если она есть в клиенте).
type glideLuaScript struct {
	client *glide.Client
	source string
}

func (s glideLuaScript) Run(ctx context.Context, keys []string, args ...string) (interface{}, error) {
	script := *options.NewScript(s.source)

	scriptOpts := options.ScriptOptions{
		Keys: keys,
		Args: args,
	}

	return s.client.InvokeScriptWithOptions(ctx, script, scriptOpts)
}

// NewValkeyGlideBackend создаёт Backend поверх github.com/valkey-io/valkey-glide/go.
// Предполагается, что конфигурация и создание *glide.GlideClient выполняются снаружи.
func NewValkeyGlideBackend(c *glide.Client) Backend {
	kv := glideKVClient{c: c}

	return &redisBackend{
		kv: kv,
		scripts: scripts{
			getWithTTL: glideLuaScript{
				client: c,
				source: luaGetWithTTLSource,
			},
			unlock: glideLuaScript{
				client: c,
				source: luaUnlockSource,
			},
			unlockAndSetResult: glideLuaScript{
				client: c,
				source: luaUnlockAndSetResultSource,
			},
		},
	}
}
