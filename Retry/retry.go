package retry

import (
	"math"
	"math/rand"
	"time"
)

// 配置参数结构体
type Config struct {
	MaxAttempts  int              // 最大尝试次数（包含首次调用）
	InitialDelay time.Duration    // 初始延迟
	MaxDelay     time.Duration    // 最大延迟时间
	Factor       float64          // 延迟增长因子（指数退避）
	JitterFactor float64          // 随机抖动系数（0-1）
	RetryOnError func(error) bool // 错误重试判断函数
}

// 装饰器函数（泛型版本）
func WithRetry[T any](fn func() (T, error), cfg Config) func() (T, error) {
	return func() (T, error) {
		var zero T
		var err error
		delay := cfg.InitialDelay

		for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
			result, err := fn()
			if err == nil {
				return result, nil
			}

			if cfg.RetryOnError != nil && !cfg.RetryOnError(err) {
				return zero, err // 不可重试错误直接返回
			}

			if attempt == cfg.MaxAttempts {
				break // 已达最大尝试次数
			}

			// 计算延迟时间
			calculated := time.Duration(float64(delay) * cfg.Factor)
			delay = time.Duration(math.Min(float64(calculated), float64(cfg.MaxDelay)))
			delay = applyJitter(delay, cfg.JitterFactor)

			time.Sleep(delay)
		}
		return zero, err
	}
}

// 应用随机抖动
func applyJitter(d time.Duration, jitter float64) time.Duration {
	if jitter <= 0 {
		return d
	}
	jitterVal := rand.Float64() * jitter     // [0, jitterFactor]
	multiplier := 1 + (jitterVal - jitter/2) // 0.5-1.5倍区间
	return time.Duration(float64(d) * multiplier)
}
