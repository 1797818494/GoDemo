package main

import (
	"sync/atomic"
)

type RingQueue struct {
	data     []interface{}
	capacity int32
	head     int32
	tail     int32
}

const (
	maxBackoff = 16 // 最大退避次数
	baseDelay  = 1  // 初始延迟（纳秒）
)

var failCount int32 = 0

func NewRingQueue(cap int32) *RingQueue {
	if cap&(cap-1) != 0 {
		panic("capacity must be power of two")
	}
	return &RingQueue{
		data:     make([]interface{}, cap),
		capacity: cap,
	}
}

func (q *RingQueue) Push(v interface{}) bool {
	var backoff int
	for {
		tail := atomic.LoadInt32(&q.tail)
		head := atomic.LoadInt32(&q.head)
		nextTail := (tail + 1) & (q.capacity - 1)

		if nextTail == head { // 队列满快速失败
			return false
		}

		if atomic.CompareAndSwapInt32(&q.tail, tail, nextTail) {
			backoff = 1
			q.data[tail] = v
			return true
		}

		// 指数退避策略
		backoff = expBackoff(backoff)
	}
}

func (q *RingQueue) Pop() interface{} {
	var backoff int
	for {
		head := atomic.LoadInt32(&q.head)
		tail := atomic.LoadInt32(&q.tail)

		if head == tail { // 队列空快速失败
			return nil
		}

		v := q.data[head]
		newHead := (head + 1) & (q.capacity - 1)

		if atomic.CompareAndSwapInt32(&q.head, head, newHead) {
			backoff = 1
			return v
		}
		// 指数退避策略
		backoff = expBackoff(backoff)
	}
}

// 指数退避+随机扰动
func expBackoff(prev int) int {
	if prev < maxBackoff {
		prev++
	}
	atomic.AddInt32(&failCount, 1)
	// delay := baseDelay << prev
	// time.Sleep(time.Duration(delay+rand.Intn(16)) * time.Nanosecond)
	// runtime.Gosched() // 主动让出时间片
	return prev
}
