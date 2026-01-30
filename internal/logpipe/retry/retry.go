package retry

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

type Class int

const (
	Retryable Class = iota
	Fatal
)

type Policy struct {
	MaxAttempts int           // e.g. 5
	BaseDelay   time.Duration // e.g. 100ms
	MaxDelay    time.Duration // e.g. 5s
	Jitter      time.Duration // e.g. 100ms (<= BaseDelay recommended)

	// Classify decides whether an error is retryable.
	// If nil, default: retry on any non-nil error.
	Classify func(error) Class

	// OnRetry is optional hook for logging/metrics.
	OnRetry func(attempt int, wait time.Duration, err error)
}

func Do(ctx context.Context, p Policy, fn func(context.Context) error) error {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 1
	}
	if p.BaseDelay <= 0 {
		p.BaseDelay = 100 * time.Millisecond
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = 5 * time.Second
	}
	if p.Jitter < 0 {
		p.Jitter = 0
	}

	classify := p.Classify
	if classify == nil {
		classify = func(err error) Class {
			if err == nil {
				return Fatal // unused
			}
			return Retryable
		}
	}

	var lastErr error
	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := fn(ctx)
		if err == nil {
			return nil
		}
		lastErr = err

		if classify(err) == Fatal {
			return err
		}
		if attempt == p.MaxAttempts {
			break
		}

		// exponential backoff with cap + jitter
		wait := p.BaseDelay << (attempt - 1)
		if wait > p.MaxDelay {
			wait = p.MaxDelay
		}
		if p.Jitter > 0 {
			wait += time.Duration(rand.Int63n(int64(p.Jitter)))
		}

		if p.OnRetry != nil {
			p.OnRetry(attempt, wait, err)
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}

	if lastErr == nil {
		lastErr = errors.New("retry: exhausted with no error (unexpected)")
	}
	return lastErr
}
