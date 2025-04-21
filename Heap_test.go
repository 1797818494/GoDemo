package main

import (
	"fmt"
	"strings"
	"testing"
)

// 测试专用断言工具（比官方还严谨）
func assertEqual(t *testing.T, expected, actual interface{}, msg string) {
	t.Helper()
	if expected != actual {
		t.Fatalf("%s\n期望值: %v\n实际值: %v", msg, expected, actual)
	}
}

// 测试专用安全转换（防止panic）
func safeCast[T any](t *testing.T, val interface{}) T {
	t.Helper()
	v, ok := val.(T)
	if !ok {
		t.Fatalf("类型断言失败: %T 转 %T", val, *new(T))
	}
	return v
}

func testIntMinHeap(t *testing.T) {
	// 给定
	h := NewHeap(true)
	testData := []IntHeapItem{5, 1, -3, 8, 2}

	// 当
	for _, v := range testData {
		h.Push(v)
	}

	// 断言弹出顺序
	expected := []string{"-3", "1", "2", "5", "8"} // 严格按升序
	var results []string

	for !h.isEmpty() {
		val := safeCast[IntHeapItem](t, h.Pop())
		results = append(results, fmt.Sprint(val))
	}

	assertEqual(t, strings.Join(expected, ","), strings.Join(results, ","), "最小堆顺序异常")
}

func testStringMaxHeap(t *testing.T) {
	// 构造复杂测试数据
	testCases := []struct {
		name     string
		input    []StringHeapItem
		expected []string
	}{
		{
			name:     "常规排序",
			input:    []StringHeapItem{"z", "a", "m"},
			expected: []string{"z", "m", "a"},
		},
		{
			name:     "相同元素处理",
			input:    []StringHeapItem{"a", "a", "a"},
			expected: []string{"a", "a", "a"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHeap(false)
			for _, v := range tc.input {
				h.Push(v)
			}

			var results []string
			for !h.isEmpty() {
				val := safeCast[StringHeapItem](t, h.Pop())
				results = append(results, string(val))
			}

			assertEqual(t, strings.Join(tc.expected, ","), strings.Join(results, ","), "最大堆排序异常")
		})
	}
}

func testEmptyHeap(t *testing.T) {
	h := NewHeap(true)

	// 验证空堆弹出
	t.Run("首次弹出", func(t *testing.T) {
		assertEqual(t, true, h.Pop() == nil, "空堆未返回nil")
	})

	// 边界条件测试
	t.Run("连续弹出操作", func(t *testing.T) {
		h.Pop() // 第一次应该nil
		assertEqual(t, true, h.Pop() == nil, "多次弹出异常")
	})
}

// 性能基准测试（百万级数据压测）
func BenchmarkHeap(b *testing.B) {
	data := make([]IntHeapItem, 1e6)
	for i := range data {
		data[i] = IntHeapItem(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := NewHeap(true)
		for _, v := range data {
			h.Push(v)
		}
		for !h.isEmpty() {
			h.Pop()
		}
	}
}

func TestHeap(t *testing.T) {
	t.Run("整型最小堆全流程", func(t *testing.T) {
		// 这里可以加前置操作
		testIntMinHeap(t)
		// 这里可以加后置清理
	})

	t.Run("字符串最大堆极限操作", func(t *testing.T) {
		testStringMaxHeap(t)
	})

	t.Run("空堆异常处理", func(t *testing.T) {
		testEmptyHeap(t)
	})
}
