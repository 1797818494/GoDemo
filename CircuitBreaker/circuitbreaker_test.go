package circuitbreaker

import (
	"errors"
	"testing"
	"time"
)

func TestCircuitBreaker(t *testing.T) {
	t.Run("正常流程", func(t *testing.T) {
		cb := New(WithFailureThreshold(2))
		var callCount int
		fn := func() error {
			callCount++
			return nil
		}

		for i := 0; i < 5; i++ {
			if err := cb.Execute(fn); err != nil {
				t.Fatal("不应返回错误")
			}
		}
		if callCount != 5 {
			t.Errorf("期望调用5次，实际调用%d次", callCount)
		}
	})

	t.Run("触发熔断与恢复", func(t *testing.T) {
		cb := New(
			WithFailureThreshold(2),
			WithResetTimeout(100*time.Millisecond),
		)
		failFn := func() error { return errors.New("mock error") }

		// 触发熔断
		cb.Execute(failFn)
		cb.Execute(failFn)
		if err := cb.Execute(failFn); err != ErrCircuitOpen {
			t.Fatal("应触发熔断")
		}

		// 等待重置超时
		time.Sleep(150 * time.Millisecond)

		// 试探阶段
		var successCount int
		successFn := func() error {
			successCount++
			return nil
		}
		for i := 0; i < 3; i++ {
			cb.Execute(successFn) // 前两次允许调用
		}

		if successCount != 2 {
			t.Errorf("期望成功调用2次，实际%d次", successCount) // success达到3才进行成功到Close，而halfOpen下为2
		}
		time.Sleep(150 * time.Millisecond)
		// 验证状态恢复
		if err := cb.Execute(successFn); err != nil {
			t.Fatal("应恢复正常状态")
		}
	})

	t.Run("半开状态限流", func(t *testing.T) {
		cb := New(
			WithFailureThreshold(1),
			WithResetTimeout(50*time.Millisecond),
		)
		cb.Execute(func() error { return errors.New("fail") }) // 触发熔断

		time.Sleep(100 * time.Millisecond) // 进入Half-Open

		var allowedCalls int
		for i := 0; i < 5; i++ {
			if cb.Execute(func() error { return nil }) == nil {
				allowedCalls++
			}
		}

		if allowedCalls > 2 { // 默认halfOpenMaxCalls=2
			t.Errorf("半开状态允许调用超过限制: %d", allowedCalls)
		}
	})
}

func TestConcurrentAccess(t *testing.T) {
	cb := New(WithFailureThreshold(2))
	errCh := make(chan error, 10)

	for i := 0; i < 10; i++ {
		go func() {
			errCh <- cb.Execute(func() error {
				return errors.New("concurrent error")
			})
		}()
	}

	openCount := 0
	for i := 0; i < 10; i++ {
		if err := <-errCh; err == ErrCircuitOpen {
			openCount++
		}
	}

	if openCount < 8 { // 至少8次应被熔断
		t.Errorf("并发熔断不足，熔断次数: %d", openCount)
	}
}
