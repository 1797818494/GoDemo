package circuitbreaker

import (
	"errors"
	"sync"
	"time"
)

// State 表示熔断器状态
type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

var (
	ErrCircuitOpen     = errors.New("circuit breaker is open")
	ErrTooManyRequests = errors.New("too many requests in half-open state")
)

type CircuitBreaker struct {
	mu                     sync.RWMutex
	state                  State
	failureThreshold       int           // 触发熔断的失败次数阈值
	resetTimeout           time.Duration // Open状态转Half-Open的等待时间
	successThreshold       int           // Half-Open状态下成功次数阈值
	failures               int           // 当前连续失败次数
	successes              int           // Half-Open状态下的成功计数
	lastFailure            time.Time     // 最后一次失败时间
	halfOpenMaxCalls       int           // Half-Open状态最大允许请求数
	halfOpenMaxPeriodStart time.Time
	totalSuccess           int
}

// New 创建熔断器实例
func New(options ...Option) *CircuitBreaker {
	cb := &CircuitBreaker{
		state:            StateClosed,
		failureThreshold: 5,
		resetTimeout:     time.Minute,
		successThreshold: 3,
		halfOpenMaxCalls: 2,
	}

	for _, opt := range options {
		opt(cb)
	}
	return cb
}

// Execute 执行受保护的函数
func (cb *CircuitBreaker) Execute(fn func() error) error {
	if !cb.allowRequest() {
		return ErrCircuitOpen
	}

	return cb.doExecute(fn)
}

// 核心执行逻辑
func (cb *CircuitBreaker) doExecute(fn func() error) error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// 检查状态转换
	cb.checkStateTransition()

	switch cb.state {
	case StateClosed, StateHalfOpen:
		err := fn()
		if err != nil {
			cb.recordFailure()
			return err
		}
		cb.recordSuccess()
		return nil
	default:
		return ErrCircuitOpen
	}
}

// 允许请求的条件判断
func (cb *CircuitBreaker) allowRequest() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	cb.checkStateTransition()
	switch cb.state {
	case StateClosed:
		return true
	case StateHalfOpen:
		return cb.successes < cb.halfOpenMaxCalls
	default:
		return false
	}
}

// 状态转换检查
func (cb *CircuitBreaker) checkStateTransition() {
	switch cb.state {
	case StateOpen:
		if time.Since(cb.lastFailure) > cb.resetTimeout {
			cb.state = StateHalfOpen
			cb.successes = 0
			cb.halfOpenMaxPeriodStart = time.Now()
		}

	case StateHalfOpen:
		if time.Since(cb.halfOpenMaxPeriodStart) > cb.resetTimeout {
			cb.successes = 0
		}
	}
}

// 记录失败
func (cb *CircuitBreaker) recordFailure() {
	cb.failures++
	cb.lastFailure = time.Now()

	switch cb.state {
	case StateClosed:
		if cb.failures >= cb.failureThreshold {
			cb.state = StateOpen
			cb.failures = 0
		}
	case StateHalfOpen:
		cb.state = StateOpen // 试探失败立即转Open
	}
}

// 记录成功
func (cb *CircuitBreaker) recordSuccess() {
	switch cb.state {
	case StateHalfOpen:
		cb.successes++
		cb.totalSuccess++
		if cb.totalSuccess >= cb.successThreshold {
			cb.state = StateClosed
			cb.failures = 0
		}
	case StateClosed:
		cb.failures = 0 // 重置连续失败计数
	}
}

// Option 配置选项类型
type Option func(*CircuitBreaker)

func WithFailureThreshold(n int) Option {
	return func(cb *CircuitBreaker) {
		cb.failureThreshold = n
	}
}

func WithResetTimeout(d time.Duration) Option {
	return func(cb *CircuitBreaker) {
		cb.resetTimeout = d
	}
}
