package util

import (
	"fmt"
	"time"
)

var (
	DefaultRetryAttempts = 30
	DefaultRetryInterval = time.Second
)

type Retry struct {
	Attempts int
	Interval time.Duration
}

func NewRetry(attempts int, interval time.Duration) *Retry {
	return &Retry{Attempts: attempts, Interval: interval}
}

func (r *Retry) Run(task func() error) (err error) {
	for i := 0; i < r.Attempts; i++ {
		err = task()
		if err == nil {
			return
		}
		time.Sleep(r.Interval)
	}
	return fmt.Errorf("failed after %d retries: %w", r.Attempts, err)
}
