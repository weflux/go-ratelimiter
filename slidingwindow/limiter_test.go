package slidingwindow

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/weflux/go-ratelimiter"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRedisLimiter(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   0,
	})
	store := NewRedisStore(rdb, "dstack.swl")
	limiter := NewLimiter(10, 1*time.Second, store, &ratelimiter.SystemClock{}, 1e-9)
	//require.True(t, limiter.AllowN(100))
	wg := sync.WaitGroup{}
	count := atomic.Int64{}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			before := time.Now()
			if err := limiter.WaitN(context.Background(), 2); err != nil {
				fmt.Println("error: ", err)
				return
			}
			count.Add(1)
			after := time.Now()
			fmt.Println(fmt.Sprintf("[%d] [%s] - [%d] limiter wait duration: %d ms, acquire time: %s", count.Load(), after.Format(time.RFC3339Nano), n, after.Sub(before).Milliseconds(), before.Format(time.RFC3339Nano)))
		}(i)
		time.Sleep(time.Duration(rand.Int63n(50)) * time.Millisecond)
	}
	wg.Wait()
}
