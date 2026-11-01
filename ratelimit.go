package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Limit allows Limit requests per Window.
type Limit struct {
	Limit  int64         `yaml:"limit"`
	Window time.Duration `yaml:"window"`
}

type Decision struct {
	Allowed    bool
	RetryAfter time.Duration
}

// Every script takes KEYS[1] = state key, ARGV = limit, window_ms and returns
// {allowed, retry_after_ms}. Each one reads the clock from Redis TIME rather than
// from the gateway, so clock skew between gateway instances cannot affect limits.

// Refills continuously at limit/window tokens per ms. Tokens are stored as a string
// because Redis truncates Lua numbers to integers when converting replies.
var tokenBucket = redis.NewScript(`
local limit, window = tonumber(ARGV[1]), tonumber(ARGV[2])
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1]) or limit
local ts = tonumber(state[2]) or now
tokens = math.min(limit, tokens + (now - ts) * limit / window)

local allowed, retry = 0, 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry = math.ceil((1 - tokens) * window / limit)
end

redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', now)
redis.call('PEXPIRE', KEYS[1], window)
return {allowed, retry}
`)

var scripts = map[string]*redis.Script{
	"token_bucket": tokenBucket,
}

type Limiter struct {
	rdb       *redis.Client
	algorithm string
}

func NewLimiter(rdb *redis.Client, algorithm string) *Limiter {
	return &Limiter{rdb: rdb, algorithm: algorithm}
}

// Allow checks and consumes quota in one atomic script.
func (l *Limiter) Allow(ctx context.Context, key string, limit Limit) (Decision, error) {
	stateKey := fmt.Sprintf("ratelimit:%s:%s", l.algorithm, key)

	reply, err := scripts[l.algorithm].Run(ctx, l.rdb, []string{stateKey},
		limit.Limit, limit.Window.Milliseconds()).Int64Slice()
	if err != nil {
		return Decision{}, err
	}
	return Decision{Allowed: reply[0] == 1, RetryAfter: time.Duration(reply[1]) * time.Millisecond}, nil
}
