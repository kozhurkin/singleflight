package redisflight

import (
	"context"
	"time"

	redisv9 "github.com/redis/go-redis/v9"
)

// v9KVClient реализует KVClient поверх *redisv9.Client.
type v9KVClient struct {
	c *redisv9.Client
}

func (v v9KVClient) GetBytes(ctx context.Context, key string) ([]byte, error) {
	res, err := v.c.Get(ctx, key).Result()
	if err == redisv9.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return []byte(res), nil
}

func (v v9KVClient) SetBytes(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return v.c.Set(ctx, key, value, ttl).Err()
}

func (v v9KVClient) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	return v.c.SetNX(ctx, key, value, ttl).Result()
}

// v9LuaScript реализует LuaScript поверх *redisv9.Script.
type v9LuaScript struct {
	c *redisv9.Client
	s *redisv9.Script
}

func (s v9LuaScript) Run(ctx context.Context, keys []string, args ...string) (any, error) {
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

// NewGoRedisV9Backend создаёт Backend поверх github.com/redis/go-redis/v9.
func NewGoRedisV9Backend(c *redisv9.Client) Backend {
	kv := v9KVClient{c: c}
	return &redisBackend{
		kv: kv,
		scripts: scripts{
			getWithTTL:         v9LuaScript{c: c, s: redisv9.NewScript(luaGetWithTTLSource)},
			unlock:             v9LuaScript{c: c, s: redisv9.NewScript(luaUnlockSource)},
			unlockAndSet:       v9LuaScript{c: c, s: redisv9.NewScript(luaUnlockAndSetSource)},
		},
	}
}
