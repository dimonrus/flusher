package flusher

import (
	"github.com/dimonrus/porterr"
	"sync"
	"time"
)

const (
	// DefaultFlusherPackLen default count of blocks in one pack
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
	// current blocks position
	cursor int
	// count of blocks in pack
	packSize int
	// flusher callback
	flusher func(block []*T) (e porterr.IError)
	// stop idle
	stop chan struct{}
}

// idle idle for flush items
func (f *FlushQueue[T]) idle(workers uint8, period time.Duration) (e porterr.IError) {
	timer := time.NewTicker(period)
	// create workers
	for i := uint8(0); i < workers; i++ {
		go func(w uint8) {
			for range timer.C {
				select {
				case <-f.stop:
					return
				default:
					e = f.Flush()
					if e != nil {
						return
					}
				}
			}
		}(i)
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
func (f *FlushQueue[T]) Flusher(flusher func(block []*T) (e porterr.IError)) *FlushQueue[T] {
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
func (f *FlushQueue[T]) AddItem(item *T) {
	f.m.Lock()
	defer f.m.Unlock()
	if len(f.blocks) == 0 || len(f.blocks[f.cursor]) >= f.packSize {
		f.grow()
	}
	f.blocks[f.cursor] = append(f.blocks[f.cursor], item)
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
func (f *FlushQueue[T]) Flush() (e porterr.IError) {
	f.m.Lock()
	if len(f.blocks) > 0 {
		items := f.blocks[0]
		f.blocks = f.blocks[1:]
		if f.cursor > 0 {
			f.cursor--
		}
		f.m.Unlock()
		if f.flusher != nil {
			e = f.flusher(items)
		}
	} else {
		f.m.Unlock()
	}
	return
}

// Idle and Flush
func (f *FlushQueue[T]) Idle(workers uint8, flushPeriod time.Duration) porterr.IError {
	if workers == 0 {
		workers = DefaultParallelCountFlusher
	}
	if flushPeriod == 0 {
		flushPeriod = DefaultFlushPeriod
	}
	return f.idle(workers, flushPeriod)
}

// NewFlushQueue init new flush queue
func NewFlushQueue[T any](packSize int, flusher func(block []*T) (e porterr.IError)) *FlushQueue[T] {
	if packSize == 0 {
		packSize = DefaultFlusherPackLen
	}
	return &FlushQueue[T]{flusher: flusher, packSize: packSize, stop: make(chan struct{})}
}
