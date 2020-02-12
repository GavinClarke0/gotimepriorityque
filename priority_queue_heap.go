package goqueDynamicPriority

import (
	"container/heap"
	"sort"
)

type priorityLevels []*priorityLevel

func (pq priorityLevels) Len() int { return len(pq) }

func (pq priorityLevels) Less(i, j int) bool {
	return pq[i].level < pq[j].level
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
	item := old[0]
	old[0] = nil    // avoid memory leak
	item.Index = -1 // for safety
	*pq = old[1:n]

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

	// need to handle ascending or descending case
	index := sort.Search((*pq).Len(), func(i int) bool { return (*pq)[i].level >= level })
	if index == (*pq).Len() || (*pq)[index].level != level {
		return nil
	}
	return (*pq)[index]
}
