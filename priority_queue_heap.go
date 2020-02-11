package goq

import (
	"container/heap"
	"sort"
	"sync"
)

// TODO: use proper mutex

type priorityLevels []*priorityLevel

var mutex = &sync.Mutex{}

func (pq priorityLevels) Len() int { return len(pq) }

func (pq priorityLevels) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.

	// TODO: check that heap interface still sets index with capital name
	// TODO: see if locking is required
	return pq[i].level > pq[j].level
}

func (pq priorityLevels) Swap(i, j int) {
	mutex.Lock()

	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j

	mutex.Unlock()
}

func (pq *priorityLevels) Push(x interface{}) {
	mutex.Lock()

	n := len(*pq)
	item := x.(priorityLevel)
	item.Index = n
	*pq = append(*pq, &item)

	mutex.Unlock()
}

func (pq *priorityLevels) Pop() interface{} {

	mutex.Lock()
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	*pq = old[0 : n-1]

	mutex.Unlock()

	return item
}

func Peek(pq *priorityLevels) interface{} {

	mutex.Lock()
	n := len(*pq)
	item := (*pq)[n-1]
	mutex.Unlock()

	return item
}

// update modifies the priority and value of an responseItem in the queue.
func (pq *priorityLevels) update(item *priorityLevel, level int64) {
	item.level = level
	heap.Fix(pq, item.Index)
}

func createPriorityLevels(length int) *priorityLevels {
	pq := make(priorityLevels, length)
	return &pq
}

//TODO: figure out if should or should not return a pointer
func (pq *priorityLevels) getLevelList() priorityLevels {
	return *pq
}

// return pointer to level at index
func (pq *priorityLevels) getLevel(level int64) *priorityLevel {
	index := sort.Search((*pq).Len(), func(i int) bool { return (*pq)[i].level == level })
	if index == (*pq).Len() {
		return nil
	}
	return (*pq)[index]
}
