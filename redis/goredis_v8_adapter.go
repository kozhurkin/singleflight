package redisflight

import (
	"context"
	"time"

	redisv8 "github.com/go-redis/redis/v8"
)

// v8KVClient реализует KVClient поверх *redisv8.Client.
type v8KVClient struct {
	c *redisv8.Client
}

func (v v8KVClient) GetBytes(ctx context.Context, key string) ([]byte, error) {
	res, err := v.c.Get(ctx, key).Result()
	if err == redisv8.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return []byte(res), nil
}

func (v v8KVClient) SetBytes(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return v.c.Set(ctx, key, value, ttl).Err()
}

func (v v8KVClient) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	return v.c.SetNX(ctx, key, value, ttl).Result()
}

// v8LuaScript реализует LuaScript поверх *redisv8.Script.
type v8LuaScript struct {
	c *redisv8.Client
	s *redisv8.Script
}

func (s v8LuaScript) Run(ctx context.Context, keys []string, args ...string) (any, error) {
	iargs := make([]any, len(args))
	for i := range args {
		iargs[i] = args[i]
	}
	cmd := s.s.Run(ctx, s.c, keys, iargs...)
	if err := cmd.Err(); err != nil {
		return nil, err
	}
	return cmd.Result()
}

// NewGoRedisV8Backend создаёт Backend поверх github.com/go-redis/redis/v8.
func NewGoRedisV8Backend(c *redisv8.Client) Backend {
	kv := v8KVClient{c: c}
	return &redisBackend{
		kv: kv,
		scripts: scripts{
			getWithTTL:         v8LuaScript{c: c, s: redisv8.NewScript(luaGetWithTTLSource)},
			unlock:             v8LuaScript{c: c, s: redisv8.NewScript(luaUnlockSource)},
			unlockAndSet:       v8LuaScript{c: c, s: redisv8.NewScript(luaUnlockAndSetSource)},
		},
	}
}
