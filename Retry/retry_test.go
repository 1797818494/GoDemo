package retry

import (
	"errors"
	"testing"
	"time"
)

func TestRetrySuccess(t *testing.T) {
	attempt := 0
	mockFn := func() (int, error) {
		attempt++
		if attempt < 3 {
			return 0, errors.New("temp error")
		}
		return 42, nil
	}

	cfg := Config{MaxAttempts: 3}
	wrapped := WithRetry(mockFn, cfg)

	result, err := wrapped()
	if err != nil || result != 42 {
		t.Fatalf("Expected success after 3 attempts, got %v", err)
	}
}

func TestJitterCalculation(t *testing.T) {
	base := 100 * time.Millisecond
	jittered := applyJitter(base, 0.5)

	if jittered < 50*time.Millisecond || jittered > 150*time.Millisecond {
		t.Errorf("Jitter out of range: got %v", jittered)
	}
}

type RetryableError struct{ Err error }

func (e RetryableError) Error() string { return e.Err.Error() }

func TestRetryAbleFail(t *testing.T) {
	attempt := 0
	cfg := Config{MaxAttempts: 3, RetryOnError: func(err error) bool {
		_, ok := err.(RetryableError)
		return ok
	}}
	mockFn := func() (int, error) {
		attempt++
		if attempt == 2 {
			return 32, errors.New("temp error")
		}
		if attempt < 3 {
			return 0, RetryableError{Err: errors.New("temp error")}
		}
		return 42, nil
	}
	wrapped := WithRetry(mockFn, cfg)

	_, err := wrapped()
	if !(err != nil && attempt == 2) {
		t.Fatalf("fail")
	}
}
