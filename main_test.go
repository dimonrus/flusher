package flusher

import (
	"fmt"
	"github.com/dimonrus/gohelp"
	"sync/atomic"
	"testing"
	"time"
)

// FlushItem
type FlushItem struct {
	// Name
	Name string
	// Number
	Number int
}

func (f FlushItem) String() string {
	return fmt.Sprintf("Name is: %s with number: %v", f.Name, f.Number)
}

func getTestItem() *FlushItem {
	return &FlushItem{
		Name:   gohelp.RandString(10),
		Number: gohelp.GetRndId(),
	}
}

var total int32

func testFlusher(block []*FlushItem) (failed []*FlushItem) {
	rnd := gohelp.GetRndNumber(0, 9)
	var j int
	for i, item := range block {
		if i > rnd {
			failed = append(failed, item)
		} else {
			atomic.AddInt32(&total, 1)
			j++
		}
	}
	fmt.Println(j, "items flushed", len(failed), "items failed", "success total:", atomic.LoadInt32(&total))
	return
}

func TestNewFlushQueue(t *testing.T) {
	fq := NewFlushQueue[FlushItem](10, testFlusher)
	fq.Flush()
	go fq.Idle(4, 0)
	for i := 0; i < 100; i++ {
		go func() {
			fq.AddItem(getTestItem())
		}()
	}
	for atomic.LoadInt32(&total) != 100 {
		time.Sleep(time.Millisecond * 100)
	}
	if fq.Len() != 0 {
		t.Fatal("must be 0 len")
	}
	fq.Stop()
	time.Sleep(time.Second)
	fq.AddItem(getTestItem())
	if fq.Len() != 1 {
		t.Fatal("must be a 1")
	}
	fq.Reset()
	fq.Flusher(nil)
}

func BenchmarkAdd(b *testing.B) {
	fq := NewFlushQueue[FlushItem](10, testFlusher)
	item := getTestItem()
	for i := 0; i < b.N; i++ {
		fq.AddItem(item)
	}
	b.ReportAllocs()
}

func BenchmarkFlushQueue_Flush(b *testing.B) {
	fq := NewFlushQueue[FlushItem](10, testFlusher)
	for i := 0; i < 10; i++ {
		fq.AddItem(getTestItem())
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fq.Flush()
	}
	b.ReportAllocs()
}
