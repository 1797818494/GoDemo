package main

import (
	"sync"
	"testing"
	"time"
)

func TestConcurrentSub(t *testing.T) {
	shardNum := 10
	totalNum := 1000
	shardBucket := NewShardBucket(shardNum, totalNum)

	clients := 100
	subPerClient := 10 // 每个客户端扣除10次，每次1

	var wg sync.WaitGroup
	wg.Add(clients)

	for i := 0; i < clients; i++ {
		go func() {
			defer wg.Done()
			client := NewMockRequestClient(shardBucket)
			for j := 0; j < subPerClient; j++ {
				if !client.Sub(1) {
					t.Error("扣除失败")
				}
			}
		}()
	}

	wg.Wait()

	// 验证总数是否正确
	total := 0
	for _, bucket := range shardBucket.buckets {
		redisBucket := bucket.(*RedisMockBucket)
		redisBucket.mu.Lock()
		total += redisBucket.num
		redisBucket.mu.Unlock()
	}

	// 初始总数1000，扣除100*10=1000，剩余应为0
	if total != 0 {
		t.Errorf("期望总数0，实际%d", total)
	}
}

func BenchmarkSingleBucket(b *testing.B) {
	// 单分片桶初始化
	singleShard := NewShardBucket(1, 100000)
	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(1000)

	for i := 0; i < 1000; i++ {
		go func() {
			defer wg.Done()
			client := NewMockRequestClient(singleShard)
			for j := 0; j < 100; j++ {
				client.Sub(1) // 每个客户端扣除10次
			}
		}()
	}

	wg.Wait()
	b.Logf("单分片耗时: %v", time.Since(start))
}

func BenchmarkMultiShardBucket(b *testing.B) {
	// 多分片桶初始化
	multiShard := NewShardBucket(10, 100000)
	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(1000)

	for i := 0; i < 1000; i++ {
		go func() {
			defer wg.Done()
			client := NewMockRequestClient(multiShard)
			for j := 0; j < 100; j++ {
				client.Sub(1)
			}
		}()
	}

	wg.Wait()
	b.Logf("多分片耗时: %v", time.Since(start))
}
