package main

import (
	"math/rand"
	"sync"
)

type Bucket interface {
	Init()
	Sub(num int) bool
	Add(num int) bool
}

type RequestClient interface {
	Sub(num int) bool
	Add(num int) bool
}

type ShardBucket struct {
	shardNum int
	buckets  []Bucket
}

func NewShardBucket(mshardNum int, totalNum int) *ShardBucket {
	shardBucket := &ShardBucket{
		shardNum: mshardNum,
		buckets:  make([]Bucket, mshardNum+1),
	}
	for i, _ := range shardBucket.buckets {
		shardBucket.buckets[i] = &RedisMockBucket{}
		shardBucket.buckets[i].Init()
	}
	perBucketNum := totalNum / mshardNum
	for i := 0; i < mshardNum; i++ {
		shardBucket.buckets[i].Add(perBucketNum)
	}
	shardBucket.buckets[mshardNum].Add(totalNum % mshardNum)
	return shardBucket
}

type RedisMockBucket struct {
	mu  sync.Mutex
	num int
}

func (redisBucket *RedisMockBucket) Init() {
	redisBucket.num = 0
}

func (redisBucket *RedisMockBucket) Add(num int) bool {
	redisBucket.mu.Lock()
	defer redisBucket.mu.Unlock()
	redisBucket.num += num
	return true
}

func (redisBucket *RedisMockBucket) Sub(num int) bool {
	redisBucket.mu.Lock()
	defer redisBucket.mu.Unlock()
	if num > redisBucket.num {
		return false
	}
	redisBucket.num -= num
	return true
}

type MockRequestClient struct {
	shardBucket       *ShardBucket
	CheckedSlots      []bool
	curCheckSlotIndex int
}

func NewMockRequestClient(shardBucket *ShardBucket) MockRequestClient {
	return MockRequestClient{
		shardBucket:       shardBucket,
		CheckedSlots:      make([]bool, shardBucket.shardNum),
		curCheckSlotIndex: rand.Intn(shardBucket.shardNum),
	}
}

func (client *MockRequestClient) Add(num int) bool {
	return client.shardBucket.buckets[len(client.shardBucket.buckets)-1].Add(num)
}

// 当前不考虑num不为1的情况，否则需要考虑碎片化问题

func (client *MockRequestClient) Sub(num int) bool {
	for {
		// 尝试当前分片
		currentBucket := client.shardBucket.buckets[client.curCheckSlotIndex]
		success := currentBucket.Sub(num)
		if success {
			return true
		}

		// 标记已检查
		client.CheckedSlots[client.curCheckSlotIndex] = true

		// 寻找下一个未检查的分片
		nextIndex := -1
		for i := 0; i < len(client.CheckedSlots); i++ {
			candidate := (client.curCheckSlotIndex + 1 + i) % len(client.CheckedSlots)
			if !client.CheckedSlots[candidate] {
				nextIndex = candidate
				break
			}
		}

		if nextIndex == -1 {
			// 所有分片已试，尝试余数分片
			lastBucketIndex := len(client.shardBucket.buckets) - 1
			return client.shardBucket.buckets[lastBucketIndex].Sub(num)
		}

		client.curCheckSlotIndex = nextIndex
	}
}
