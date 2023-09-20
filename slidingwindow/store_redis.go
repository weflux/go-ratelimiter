package slidingwindow

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"strconv"
	"time"
)

var _ Store = new(RedisStore)

func NewRedisStore(rdb *redis.Client, prefix string) *RedisStore {
	return &RedisStore{
		rdb:    rdb,
		prefix: prefix,
	}
}

type RedisStore struct {
	rdb    *redis.Client
	prefix string
}

func (r *RedisStore) Increment(ctx context.Context, n int, prev, curr time.Time, ttl time.Duration) (int64, int64, error) {
	var incr *redis.IntCmd
	var prevCountCmd *redis.StringCmd
	var err error
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err = r.rdb.Pipelined(ctx, func(ppl redis.Pipeliner) error {
			currKey := strconv.FormatInt(curr.UnixNano(), 10)
			incr = ppl.IncrBy(ctx, redisKey(r.prefix, currKey), int64(n))
			ppl.PExpire(ctx, redisKey(r.prefix, currKey), ttl)
			prevCountCmd = ppl.Get(ctx, redisKey(r.prefix, strconv.FormatInt(prev.UnixNano(), 10)))
			return nil
		})
	}()

	var prevCount int64
	select {
	case <-done:
		if err == redis.TxFailedErr {
			return 0, 0, errors.Wrap(err, "redis transaction failed")
		} else if err == redis.Nil {
			prevCount = 0
		} else if err != nil {
			return 0, 0, errors.Wrap(err, "unexpected error from redis")
		} else {
			prevCount, err = strconv.ParseInt(prevCountCmd.Val(), 10, 64)
			if err != nil {
				return 0, 0, errors.Wrap(err, "failed to parse response from redis")
			}
		}
		return prevCount, incr.Val(), nil
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
}

func redisKey(prefix, key string) string {
	return fmt.Sprintf("%s/%s", prefix, key)
}
