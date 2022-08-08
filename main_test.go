package flusher

import (
	"fmt"
	"github.com/dimonrus/gohelp"
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

func testFlusher(block []*FlushItem) (failed []*FlushItem) {
	rnd := gohelp.GetRndNumber(0, 9)
	var j int
	for i, item := range block {
		if i > rnd {
			failed = append(failed, item)
		} else {
			j++
		}
	}
	time.Sleep(time.Second * 5)
	fmt.Println(j, "items flushed", len(failed), "items failed")
	return
}

func TestNewFlushQueue(t *testing.T) {
	fq := NewFlushQueue[FlushItem](10, testFlusher)
	fq.Flush()
	go fq.Idle(4, 0)
	for i := 0; i < 100; i++ {
		fq.AddItem(getTestItem())
	}
	time.Sleep(time.Second * 30)
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
