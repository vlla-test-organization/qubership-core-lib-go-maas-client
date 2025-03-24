package util

import (
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func Test_NewRetry(t *testing.T) {
	assertions := require.New(t)
	retry := NewRetry(1, 10*time.Millisecond)
	assertions.Equal(1, retry.Attempts)
	assertions.Equal(10*time.Millisecond, retry.Interval)
}

func Test_RetryRun(t *testing.T) {
	assertions := require.New(t)
	attempts := 10
	retry := NewRetry(attempts, 10*time.Millisecond)
	attempt := attempts
	err := retry.Run(func() error {
		attempt--
		if attempt > 0 {
			return errors.New("test")
		} else {
			return nil
		}
	})
	assertions.NoError(err)
}

func Test_RetryRunError(t *testing.T) {
	assertions := require.New(t)
	attempts := 10
	retry := NewRetry(attempts, 10*time.Millisecond)
	attempt := attempts + 1
	err := retry.Run(func() error {
		attempt--
		if attempt > 0 {
			return errors.New("test")
		} else {
			return nil
		}
	})
	assertions.Equal("failed after 10 retries: test", err.Error())
}
