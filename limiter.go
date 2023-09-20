package ratelimiter

import (
	"context"
	"errors"
)

var (
	// ErrLimitExhausted is returned by the Limiter in case the number of requests overflows the capacity of a Limiter.
	ErrLimitExhausted = errors.New("requests limit exhausted")

	// ErrRaceCondition is returned when there is a race condition while saving a state of a rate limiter.
	ErrRaceCondition = errors.New("race condition detected")
)

type Limiter interface {
	Allow() bool
	AllowN(n int) bool
	Wait(ctx context.Context) error
	WaitN(ctx context.Context, n int) error
}
