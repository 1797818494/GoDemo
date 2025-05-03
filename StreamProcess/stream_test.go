package main

import (
	"fmt"
	"testing"
	"time"
)

func TestStreamProcess(t *testing.T) {
	// 创建测试时间基准点
	baseTime, _ := time.Parse(time.RFC3339, "2024-01-01T10:00:00Z")

	// 初始化流处理系统（允许5分钟延迟）
	sp := NewStreamProcess(baseTime, 5*time.Minute)

	// 创建两个测试窗口
	window1 := Window{
		windowId:  "w1",
		startTime: baseTime,
		endTime:   baseTime.Add(15 * time.Minute),
	}
	window2 := Window{
		windowId:  "w2",
		startTime: baseTime.Add(15 * time.Minute),
		endTime:   baseTime.Add(30 * time.Minute),
	}
	sp.AddWindows(window1)
	sp.AddWindows(window2)

	// 测试数据集（包含正常和乱序数据）
	testCases := []struct {
		data      InputData
		expectSum int
	}{
		// 窗口1数据：10:05（正常）
		{InputData{baseTime.Add(5 * time.Minute), 1}, 0},
		// 窗口1数据：10:10（正常）
		{InputData{baseTime.Add(10 * time.Minute), 2}, 0},
		// 窗口2数据：10:20（早期数据）
		{InputData{baseTime.Add(20 * time.Minute), 3}, 0},
		// 延迟数据：09:59（应被忽略）
		{InputData{baseTime.Add(-1 * time.Minute), 99}, 0},
		// 触发点数据：10:25（将watermark推到10:20）
		{InputData{baseTime.Add(25 * time.Minute), 0}, 3}, // 应触发窗口1求和1+2=3
		// 后续数据：10:35（触发窗口2）
		{InputData{baseTime.Add(35 * time.Minute), 4}, 3}, // 触发窗口2求和3+4=7
	}

	for i, tc := range testCases {
		output := sp.processDataAndOutputReadyWindows(tc.data)
		if i == 2 {
			if len(output) != 1 || output[0] != 3 {
				t.Errorf("第%d步触发结果错误，期望 [3]，得到 %v", i+1, output)
			}
		}
		if i == 5 {
			if len(output) != 1 || output[0] != 3 {
				t.Errorf("第%d步触发结果错误，期望 [3]，得到 %v", i+1, output)
			}
		}
	}

	// 验证最终结果队列
	if fmt.Sprintf("%v", sp.ansOuputs) != "[3 3]" {
		t.Errorf("最终结果错误，期望 [3 3]，得到 %v", sp.ansOuputs)
	}
}
