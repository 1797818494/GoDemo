package main

// 定义比较接口
type Comparer interface {
	Less(other Comparer) bool // 判断当前对象是否小于另一个对象
}

// 通用堆结构体
type Heap struct {
	data  []Comparer
	isMin bool // true为最小堆，false为最大堆
}

func NewHeap(isMin bool) *Heap {
	return &Heap{isMin: isMin}
}

// 获取比较结果（根据堆类型反转判断）
func (h *Heap) compare(a, b Comparer) bool {
	if h.isMin {
		return a.Less(b)
	}
	return b.Less(a) // 最大堆时取反
}

// 插入元素（多态）
func (h *Heap) Push(val Comparer) {
	h.data = append(h.data, val)
	h.siftUp(len(h.data) - 1)
}

// 弹出堆顶元素（多态返回）
func (h *Heap) Pop() Comparer {
	if len(h.data) == 0 {
		return nil
	}

	val := h.data[0]
	lastIdx := len(h.data) - 1
	h.data[0] = h.data[lastIdx]
	h.data = h.data[:lastIdx]
	h.siftDown(0)
	return val
}

// 上浮（多态比较）
func (h *Heap) siftUp(index int) {
	for index > 0 {
		parent := (index - 1) / 2
		if h.compare(h.data[index], h.data[parent]) {
			h.data[parent], h.data[index] = h.data[index], h.data[parent]
			index = parent
		} else {
			break
		}
	}
}

// 下沉（多态比较）
func (h *Heap) siftDown(index int) {
	size := len(h.data)
	for {
		left := 2*index + 1
		right := 2*index + 2
		target := index

		if left < size && h.compare(h.data[left], h.data[target]) {
			target = left
		}
		if right < size && h.compare(h.data[right], h.data[target]) {
			target = right
		}

		if target == index {
			break
		}

		h.data[index], h.data[target] = h.data[target], h.data[index]
		index = target
	}
}
func (heap *Heap) isEmpty() bool {
	return len(heap.data) == 0
}

// 实现Comparer接口的示例类型
type IntHeapItem int

func (i IntHeapItem) Less(other Comparer) bool {
	return i < other.(IntHeapItem)
}

type StringHeapItem string

func (s StringHeapItem) Less(other Comparer) bool {
	return s < other.(StringHeapItem)
}
