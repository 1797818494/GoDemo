package main

import (
	"log"
	"time"
)

type InputData struct {
	eventTime time.Time
	num       int
}

type Window struct {
	windowId  string
	startTime time.Time
	endTime   time.Time
	datas     []InputData
}

type StreamProcess struct {
	windows   []Window
	ansOuputs []int
	waterMark time.Time
	latency   time.Duration
}

func NewStreamProcess(baseTime time.Time, latency time.Duration) StreamProcess {
	return StreamProcess{
		windows:   make([]Window, 0),
		waterMark: baseTime,
		latency:   latency,
	}
}

func (sp *StreamProcess) AddWindows(window Window) {
	sp.windows = append(sp.windows, window)
}

func (sp *StreamProcess) processDataAndOutputReadyWindows(data InputData) []int {
	// 更新Watermark
	newWatermark := data.eventTime.Add(-sp.latency)
	if newWatermark.After(sp.waterMark) {
		sp.waterMark = newWatermark
	}

	// 将数据分配到符合条件的窗口中
	for i := range sp.windows {
		window := &sp.windows[i]
		//这个过程可以优化为二分查找
		if !data.eventTime.Before(window.startTime) && !window.endTime.Before(data.eventTime) {
			window.datas = append(window.datas, data)
		}
	}

	// 分离已就绪的窗口
	var readyWindows []Window
	var remainingWindows []Window
	for _, window := range sp.windows {
		log.Printf("window{%v  %v}  waterMark{%v}\n", window.startTime, window.endTime, sp.waterMark)
		if window.endTime.Before(sp.waterMark) || window.endTime.Equal(sp.waterMark) {
			readyWindows = append(readyWindows, window)
		} else {
			remainingWindows = append(remainingWindows, window)
		}
	}
	sp.windows = remainingWindows // 更新未触发的窗口列表

	// 计算就绪窗口的结果
	var outputs []int
	for _, win := range readyWindows {
		sum := 0
		for _, d := range win.datas {
			sum += d.num
		}
		outputs = append(outputs, sum)
	}

	// 保存结果并返回
	sp.ansOuputs = append(sp.ansOuputs, outputs...)
	return outputs
}
