package main

import (
	"log"
	"sync"
	"time"
)

var sockNum int = 5

var batchSize int = 3

var queueLen int = 7

var waitTime int = 200

type Demo struct {
	batchChannel chan *RequestPromise
	changeLogs   []OperatorChangeLog
}
type UserRequst struct {
	OrderId int
	UserId  int
	Num     int
}

type Result struct {
	Message string
	Success bool
}
type RequestPromise struct {
	request UserRequst
	result  chan Result
}

type OperatorChangeLog struct {
	orderId      int
	count        int
	operatorType int // 0 rollback 1 commit
}

var rollbackType int = 0
var commitType int = 1

func InitOperatorChangeLog(orderId int, count int, operatorType int) OperatorChangeLog {
	return OperatorChangeLog{
		orderId:      orderId,
		count:        count,
		operatorType: operatorType,
	}
}
func InitResult(message string, success bool) Result {
	return Result{Message: message, Success: success}
}

func InitDemo() Demo {
	demo := Demo{}
	demo.batchChannel = make(chan *RequestPromise, queueLen)
	return demo
}

func (demo *Demo) Operator(userRequst UserRequst) Result {
	promise := &RequestPromise{
		request: userRequst,
		result:  make(chan Result, 1), // 缓冲通道防止阻塞
	}

	// 尝试入队，超时100ms
	select {
	case demo.batchChannel <- promise:
		// 入队成功
	case <-time.After(100 * time.Millisecond):
		return InitResult("系统繁忙", false)
	}

	// 等待结果或超时200ms
	select {
	case res := <-promise.result:
		return res
	case <-time.After(200 * time.Millisecond):
		return InitResult("等待超时", false)
	}
}

func (demo *Demo) MergeJob() {
	go func() {
		for {
			curSize := 0
			promiseArr := make([]*RequestPromise, batchSize)
			timeout := time.After(100 * time.Millisecond) // 设置200ms超时
			// 使用带标签的循环，方便在select中直接退出
		batchLoop:
			for curSize < batchSize {
				select {
				case requestPromise, ok := <-demo.batchChannel:
					if !ok {
						// 通道已关闭，直接退出循环
						break batchLoop
					}
					promiseArr[curSize] = requestPromise
					curSize++
					if requestPromise.request.UserId == 0 {
						time.Sleep(200 * time.Millisecond)
					}
					// 已收集满批次，立即返回
					if curSize == batchSize {
						break batchLoop
					}
				case <-timeout:
					// 超时触发，无论是否填满都返回已有数据
					break batchLoop
				}
			}
			count := 0
			for i := 0; i < curSize; i++ {
				count += promiseArr[i].request.Num
			}
			if count <= sockNum {
				sockNum -= count
				for i := 0; i < curSize; i++ {
					promise := promiseArr[i]
					demo.changeLogs = append(demo.changeLogs, InitOperatorChangeLog(promise.request.OrderId, promise.request.Num, commitType))
					log.Printf("===========扣减成功 %v ================\n", promise.request)
					promise.result <- InitResult("扣减成功", true)
				}
			} else {
				for i := 0; i < curSize; i++ {
					promise := promiseArr[i]
					log.Printf("===========扣减失败，库存不足 %v ================\n", promise.request)
					promise.result <- InitResult("扣减失败，库存不足", false)
				}
			}
		}
	}()
}

func (demo *Demo) Start() {
	waitGroup := sync.WaitGroup{}
	var mutex sync.Mutex = sync.Mutex{}
	var resultMap map[UserRequst]Result = make(map[UserRequst]Result)
	log.Println("===========开始请求===========")
	for i := 0; i < 10; i++ {
		waitGroup.Add(1)
		go func(i int) {
			defer waitGroup.Done()
			request := UserRequst{
				OrderId: 100 + i,
				UserId:  i,
				Num:     1,
			}
			result := demo.Operator(request)
			mutex.Lock()
			resultMap[request] = result
			mutex.Unlock()
		}(i)
	}
	waitGroup.Wait()
	log.Printf("===========开始完成 %v===========", resultMap)
	for request, result := range resultMap {
		if result.Message == "等待超时" {
			for _, operator := range demo.changeLogs {
				if operator.orderId == request.OrderId && operator.operatorType == commitType {
					// log
					log.Printf("===requst{%v} timeout rollback===\n", request)
					sockNum += request.Num
					demo.changeLogs = append(demo.changeLogs, OperatorChangeLog{orderId: request.OrderId, count: request.Num, operatorType: rollbackType})
				}
			}
		}
	}
	log.Printf("=========sockNum = {%v} ===============\n", sockNum)
}
