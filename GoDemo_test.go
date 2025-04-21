package main

import (
	"sync"
	"testing"
)

// 重置全局变量避免测试间干扰
func resetGlobals() {
	sockNum = 5
	rollbackType = 0
	commitType = 1
}

func TestDemo_NormalOperation(t *testing.T) {
	resetGlobals()
	demo := InitDemo()
	demo.MergeJob() // 启动处理协程

	t.Run("正常库存扣减", func(t *testing.T) {
		request := UserRequst{OrderId: 1, UserId: 1001, Num: 1}
		result := demo.Operator(request)

		if !result.Success {
			t.Errorf("预期扣减成功，实际返回: %v", result)
		}
		if sockNum != 4 {
			t.Errorf("预期库存4，实际库存: %d", sockNum)
		}
	})
}

func TestDemo_TimeoutRollback(t *testing.T) {
	resetGlobals()
	sockNum = 6
	demo := InitDemo()
	demo.MergeJob()
	demo.Start()

}

func TestDemo_DataRace(t *testing.T) {
	resetGlobals()
	demo := InitDemo()
	demo.MergeJob()
	// 并发读写测试
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(2)
		go func(id int) {
			defer wg.Done()
			demo.Operator(UserRequst{OrderId: id, Num: 1})
		}(i)
		go func() {
			defer wg.Done()
			demo.Start()
		}()
	}
	wg.Wait()
}
