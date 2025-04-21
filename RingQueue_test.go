package main

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRingQueue_Concurrent(t *testing.T) {
	startTime := time.Now() // 测试开始时间
	q := NewRingQueue(1024 * 1024 * 128)
	var ops uint64
	var activeProducers int32 = 100000 // 活跃生产者计数器
	var wg sync.WaitGroup

	// 启动4个生产者
	for i := 0; i < 100000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 10; n++ {
				if q.Push(n) {
					atomic.AddUint64(&ops, 1)
				}
			}
			atomic.AddInt32(&activeProducers, -1) // 生产者完成
		}()
	}

	// 启动4个消费者
	for i := 0; i < 100000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				// 队列有数据时消费
				if v := q.Pop(); v != nil {
					atomic.AddUint64(&ops, ^uint64(0))
					continue
				}

				// 队列空且无活跃生产者时退出
				if atomic.LoadInt32(&activeProducers) == 0 {
					return
				}
				runtime.Gosched() // 主动让出CPU
			}
		}()
	}

	wg.Wait() // 等待所有协程完成
	duration := time.Since(startTime)

	// 校验结果
	if final := atomic.LoadUint64(&ops); final != 0 {
		t.Fatalf("数据不一致: 剩余%d操作未处理", final)
	}

	// 性能统计
	t.Logf("\n=== 性能报告 ===")
	t.Logf("\n===%v===", failCount)
	t.Logf("总操作量: 400,000 (4生产者 x 100,000次/个)")
	t.Logf("实际耗时: %v", duration.Round(time.Millisecond))
	t.Logf("吞吐量: %.2f ops/ns", float64(400000)/float64(duration.Nanoseconds()))
	t.Logf("         ≈ %.2f million ops/sec", float64(400000)/duration.Seconds()/1e6)
}
