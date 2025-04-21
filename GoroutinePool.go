package main

import (
	"fmt"
	"sync"
	"time"
)

type Task func()

type GoroutinePool struct {
	tasks       chan Task
	workerCount int
	wg          sync.WaitGroup
}

// 创建协程池
func NewGoroutinePool(workerCount, taskBuffer int) *GoroutinePool {
	pool := &GoroutinePool{
		tasks:       make(chan Task, taskBuffer),
		workerCount: workerCount,
	}
	// 初始化工作协程
	for i := 0; i < workerCount; i++ {
		pool.wg.Add(1)
		go pool.worker()
	}
	return pool
}

// 工作协程逻辑
func (p *GoroutinePool) worker() {
	defer p.wg.Done()
	for task := range p.tasks {
		task() // 执行任务
	}
}

// 提交任务
func (p *GoroutinePool) Submit(task Task) {
	p.tasks <- task
}

// 等待所有任务完成并关闭池
func (p *GoroutinePool) Wait() {
	close(p.tasks)
	p.wg.Wait()
}

func main() {
	pool := NewGoroutinePool(5, 100) // 5个协程，任务缓冲100
	defer pool.Wait()

	for i := 0; i < 20; i++ {
		id := i
		pool.Submit(func() {
			time.Sleep(time.Second)
			fmt.Printf("Task %d completed\n", id)
		})
	}
}
