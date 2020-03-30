package goq

import (
	"container/heap"
	_ "sort"
)

type priorities []*orderLevel

type orderLevel struct {
	priority int64
	Index    int
}

func (pq priorities) Len() int { return len(pq) }

func (pq priorities) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority
}

func (pq priorities) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *priorities) Push(x interface{}) {
	n := len(*pq)
	item := x.(*orderLevel)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *priorities) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

func Peek(pq *priorities) interface{} {
	n := len(*pq)
	if n == 0 {
		return nil
	}
	item := (*pq)[0]
	return item
}

// update modifies the priority and value of an responseItem in the queue.
func (pq *priorities) update(item *priorityLevel, priority int64) {
	item.priority = priority
	heap.Fix(pq, item.Index)
}

func createLevelOrders(length int) *priorities {
	pq := make(priorities, length)
	heap.Init(&pq)
	return &pq
}

func (pq *priorities) getLevelList() priorities {
	return *pq
}

func (pq *priorities) getLevel(priority int64) *orderLevel {
	for i := range *pq {
		if (*pq)[i].priority == priority {
			return (*pq)[i]
		}
	}
	return nil
}
