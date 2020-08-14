package gotimepriorityque

import (
	"container/heap"
	"testing"
)

func TestEnqueueAndDequeueOfHeap(t *testing.T) {

	pq := make(priorities, 0)
	heap.Init(&pq)

	for p := 0; p <= 100000; p++ {

		item := &orderLevel{
			priority: int64(p),
		}
		heap.Push(&pq, item)
	}

	for p := 0; p <= 100000; p++ {
		item := heap.Pop(&pq)
		if (item).(*orderLevel).priority != int64(p) {
			t.Errorf("Expected other Priority")
		}
	}
}
