package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Limit allows Limit requests per Window. The same pair drives all three
// algorithms so they can be compared on identical quotas.
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

// One sorted-set entry per admitted request, so memory is bounded by limit.
// Members embed the count because two requests in the same millisecond would
// otherwise share a member, and the second ZADD would silently not count.
var slidingWindow = redis.NewScript(`
local limit, window = tonumber(ARGV[1]), tonumber(ARGV[2])
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - window)
local count = redis.call('ZCARD', KEYS[1])
if count < limit then
  redis.call('ZADD', KEYS[1], now, string.format('%.0f:%d', now, count))
  redis.call('PEXPIRE', KEYS[1], window)
  return {1, 0}
end

local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
return {0, tonumber(oldest[2]) + window - now}
`)

// The counter expires exactly on the window boundary, so one key is enough and
// no window id has to be derived inside the script.
var fixedWindow = redis.NewScript(`
local limit, window = tonumber(ARGV[1]), tonumber(ARGV[2])
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local reset = (math.floor(now / window) + 1) * window

local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('PEXPIREAT', KEYS[1], reset)
end
if count <= limit then
  return {1, 0}
end
return {0, reset - now}
`)

var scripts = map[string]*redis.Script{
	"token_bucket":   tokenBucket,
	"sliding_window": slidingWindow,
	"fixed_window":   fixedWindow,
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
