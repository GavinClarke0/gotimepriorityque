package goq

import (
	"container/heap"
	"testing"
)

func TestEnqueueAndDequeueOfHeap(t *testing.T) {

	pq := make(priorityLevels, 0)
	heap.Init(&pq)

	for p := 0; p <= 50; p++ {

		item := &priorityLevel{
			priority: int64(p),
			head:     0,
			tail:     1,
		}
		heap.Push(&pq, item)
	}

	for p := 0; p <= 50; p++ {
		item := heap.Pop(&pq)
		if (item).(*priorityLevel).priority != int64(p) {
			t.Errorf("Expected other Priority")
		}
	}
}
