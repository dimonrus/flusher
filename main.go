package flusher

import (
	"sync"
	"time"
)

const (
	// DefaultFlusherPackLen default count of items in one pack
	DefaultFlusherPackLen = 5000

	// DefaultParallelCountFlusher default number of async flusher
	DefaultParallelCountFlusher = 2

	// DefaultFlushPeriod every 500 millisecond
	DefaultFlushPeriod = time.Millisecond * 500
)

// FlushQueue implement functional for high load processes
type FlushQueue[T any] struct {
	// mutex
	m sync.RWMutex
	// blocks with entity
	blocks [][]*T
	// failed block
	failed failedItems[T]
	// current blocks position
	cursor int
	// count of blocks in pack
	packSize int
	// flusher callback
	flusher func(block []*T) (failed []*T)
	// stop idle
	stop chan struct{}
}

// idle idle for flush items
func (f *FlushQueue[T]) idle(workers uint8, period time.Duration) {
	ticker := time.NewTicker(period)
	// create workers
	for i := uint8(0); i < workers; i++ {
		go func() {
			for range ticker.C {
				select {
				case <-f.stop:
					return
				default:
					f.Flush()
				}
			}
		}()
	}
	return
}

// Stop idle function
func (f *FlushQueue[T]) Stop() *FlushQueue[T] {
	close(f.stop)
	return f
}

// Len get len of queue
func (f *FlushQueue[T]) Len() int {
	f.m.RLock()
	f.m.RUnlock()
	return len(f.blocks)
}

// Flusher set flusher
func (f *FlushQueue[T]) Flusher(flusher func(block []*T) (failed []*T)) *FlushQueue[T] {
	f.m.Lock()
	defer f.m.Unlock()
	f.flusher = flusher
	return f
}

// grow queue
func (f *FlushQueue[T]) grow() {
	f.blocks = append(f.blocks, make([]*T, 0))
	f.cursor = len(f.blocks) - 1
	return
}

// AddItem add item to queue
func (f *FlushQueue[T]) AddItem(item ...*T) {
	f.m.Lock()
	defer f.m.Unlock()
	f.add(item...)
	return
}

// AddItem add item to queue
func (f *FlushQueue[T]) add(item ...*T) {
	for _, t := range item {
		if len(f.blocks) == 0 || len(f.blocks[f.cursor]) >= f.packSize {
			f.grow()
		}
		f.blocks[f.cursor] = append(f.blocks[f.cursor], t)
	}
	return
}

// Reset reset all flush queue
func (f *FlushQueue[T]) Reset() {
	f.m.Lock()
	defer f.m.Unlock()
	f.blocks = f.blocks[:0]
	f.cursor = 0
	return
}

// Flush performs flush action
func (f *FlushQueue[T]) Flush() {
	f.m.Lock()
	if len(f.blocks) > 0 {
		items := f.blocks[0]
		f.blocks = f.blocks[1:]
		if f.cursor > 0 {
			f.cursor--
		}
		f.m.Unlock()
		if f.flusher != nil {
			filed := f.flusher(items)
			if len(filed) > 0 {
				f.failed.AddItem(filed...)
			}
		}
	} else {
		if f.failed.Len() > 0 {
			f.add(f.failed.Extract()...)
		}
		f.m.Unlock()
	}
	return
}

// Idle and Flush
func (f *FlushQueue[T]) Idle(workers uint8, flushPeriod time.Duration) {
	if workers == 0 {
		workers = DefaultParallelCountFlusher
	}
	if flushPeriod == 0 {
		flushPeriod = DefaultFlushPeriod
	}
	f.idle(workers, flushPeriod)
	return
}

// NewFlushQueue init new flush queue
func NewFlushQueue[T any](packSize int, flusher func(block []*T) (filed []*T)) *FlushQueue[T] {
	if packSize == 0 {
		packSize = DefaultFlusherPackLen
	}
	return &FlushQueue[T]{flusher: flusher, packSize: packSize, stop: make(chan struct{})}
}

// For failed items
type failedItems[T any] struct {
	// mutex
	m sync.RWMutex
	// failed items with entity
	items []*T
}

// Len get len of queue
func (f *failedItems[T]) Len() int {
	f.m.RLock()
	f.m.RUnlock()
	return len(f.items)
}

// AddItem add failed block
func (f *failedItems[T]) AddItem(item ...*T) *failedItems[T] {
	f.m.Lock()
	f.m.Unlock()
	f.items = append(f.items, item...)
	return f
}

// Extract get all failed items
func (f *failedItems[T]) Extract() []*T {
	f.m.Lock()
	f.m.Unlock()
	items := f.items
	f.items = f.items[:0]
	return items
}
