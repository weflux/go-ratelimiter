package slidingwindow

import (
	"context"
	"errors"
	"github.com/weflux/go-ratelimiter"
	"time"
)

var _ ratelimiter.Limiter = new(Limiter)

type Store interface {
	Increment(ctx context.Context, n int, prev, curr time.Time, ttl time.Duration) (prevCount, currCount int64, err error)
}

type Limiter struct {
	store    Store
	clock    ratelimiter.Clock
	rate     time.Duration
	capacity int64
	epsilon  float64
}

func NewLimiter(capacity int64, rate time.Duration, store Store, clock ratelimiter.Clock, epsilon float64) *Limiter {
	return &Limiter{store: store, clock: clock, rate: rate, capacity: capacity, epsilon: epsilon}
}

func (s *Limiter) Acquire(ctx context.Context, n int) (time.Duration, error) {
	now := s.clock.Now()
	currWindow := now.Truncate(s.rate)
	prevWindow := now.Truncate(-s.rate)
	ttl := s.rate - now.Sub(currWindow)
	prev, curr, err := s.store.Increment(ctx, n, prevWindow, currWindow, ttl+s.rate)
	if err != nil {
		return 0, err
	}

	total := float64(prev*int64(ttl))/float64(s.rate) + float64(curr)
	if total-float64(s.capacity) >= s.epsilon {
		var wait time.Duration
		if curr <= s.capacity-1 && prev > 0 {
			wait = ttl - time.Duration(float64(s.capacity-1-curr)/float64(prev)*float64(s.rate))
		} else {
			// If prev == 0.
			wait = ttl + time.Duration((1-float64(s.capacity-1)/float64(curr))*float64(s.rate))
		}
		//log.Printf("acquire %d token, duration: %dms", n, wait.Milliseconds())
		return wait, ratelimiter.ErrLimitExhausted
	}
	//log.Printf("acquire %d token, duration: %dms", n, 0)
	return 0, nil
}

func (s *Limiter) Allow() bool {
	return s.AllowN(1)
}

func (s *Limiter) AllowN(n int) bool {
	d, err := s.Acquire(context.TODO(), n)
	if err != nil {
		panic(s)
	}
	return d <= 0
}

func (s *Limiter) Wait(ctx context.Context) error {
	return s.WaitN(ctx, 1)
}

func (s *Limiter) WaitN(ctx context.Context, n int) error {
	var timer *time.Timer
loop:
	d, err := s.Acquire(ctx, n)
	if err != nil && !errors.Is(err, ratelimiter.ErrLimitExhausted) {
		return err
	}
	if d <= 0 {
		return nil
	}
	timer = time.NewTimer(d)
	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-timer.C:
		goto loop
		//return nil
	}
}
