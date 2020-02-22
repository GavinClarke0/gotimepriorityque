package goq

import (
	"container/heap"
	_ "sort"
)

type priorityLevels []*priorityLevel

func (pq priorityLevels) Len() int { return len(pq) }

func (pq priorityLevels) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority
}

func (pq priorityLevels) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *priorityLevels) Push(x interface{}) {
	n := len(*pq)
	item := x.(*priorityLevel)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *priorityLevels) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

func Peek(pq *priorityLevels) interface{} {
	n := len(*pq)
	if n == 0 {
		return nil
	}
	item := (*pq)[0]
	return item
}

// update modifies the priority and value of an responseItem in the queue.
func (pq *priorityLevels) update(item *priorityLevel, priority int64) {
	item.priority = priority
	heap.Fix(pq, item.Index)
}

func createPriorityLevels(length int) *priorityLevels {
	pq := make(priorityLevels, length)
	heap.Init(&pq)
	return &pq
}

func (pq *priorityLevels) getLevelList() priorityLevels {
	return *pq
}

func (pq *priorityLevels) getLevel(priority int64) *priorityLevel {
	for i := range *pq {
		if (*pq)[i].priority == priority {
			return (*pq)[i]
		}
	}
	return nil
}
