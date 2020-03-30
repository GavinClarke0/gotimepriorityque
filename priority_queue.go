package goq

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"os"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	_ "github.com/syndtr/goleveldb/leveldb/util"
)

// prefixSep is the prefix separator for each item key.
var prefixSep []byte = []byte(":")

// order defines the priority ordering of the queue.
type order int

// Defines which priority order to dequeue in.
const (
	ASC  order = iota // Set priority priority 0 as most important.
	DESC              // Set priority priority 255 as most important.
)

// priorityLevel holds the head and tail position of a priority, or the number of items currently at a single priority
type priorityLevel struct {
	priority int64
	head     uint64
	tail     uint64
	Index    int
}

// length returns the total number of items in this priority priority.
func (pl *priorityLevel) length() uint64 {
	return pl.tail - pl.head
}

// PriorityQueue is a standard FIFO (first in, first out) queue with
// priority levels.
type PriorityQueue struct {
	sync.RWMutex
	DataDir    string
	db         *leveldb.DB
	order      order
	levelOrder *priorities
	levelMap   map[int64]*priorityLevel
	curLevel   int64
	isOpen     bool
}

// OpenPriorityQueue opens a priority queue if one exists at the given
// directory. If one does not already exist, a new priority queue is
// created.
func OpenPriorityQueue(dataDir string, order order) (*PriorityQueue, error) {
	var err error

	// Create a new PriorityQueue.
	pq := &PriorityQueue{
		DataDir:    dataDir,
		db:         &leveldb.DB{},
		levelOrder: &priorities{},
		levelMap:   map[int64]*priorityLevel{},
		order:      order,
		isOpen:     false,
	}

	// Open database for the priority queue.
	pq.db, err = leveldb.OpenFile(dataDir, nil)
	if err != nil {
		return pq, err
	}

	pq.levelOrder = createLevelOrders(0)

	// Set isOpen and return.
	pq.isOpen = true
	return pq, pq.init()
}

// Enqueue adds an item to the priority queue.
func (pq *PriorityQueue) Enqueue(priority int64, value []byte) (*PriorityItem, error) {
	pq.Lock()
	defer pq.Unlock()
	// Check if queue is closed.
	if !pq.isOpen {
		return nil, ErrDBClosed
	}
	// Get the priorityLevel.

	if pq.levelMap[priority] == nil {

		pl := &priorityLevel{
			head:     0,
			tail:     0,
			priority: priority,
		}

		orderLevel := &orderLevel{
			priority: priority,
			Index:    0,
		}
		// add new priority object to heap
		heap.Push((*pq).levelOrder, orderLevel)

		//TODO: test weird pointer syntax
		(*pq).levelMap[priority] = pl

	}

	level := pq.levelMap[priority]

	// Create new PriorityItem.
	item := &PriorityItem{
		ID:       (*level).tail + 1,
		Priority: priority,
		Key:      pq.generateKey(priority, (*level).tail+1),
		Value:    value,
	}
	// Add it to the priority queue.
	if err := pq.db.Put(item.Key, item.Value, nil); err != nil {
		return nil, err
	}
	(*level).tail++

	return item, nil
}

// EnqueueString is a helper function for Enqueue that accepts a
// value as a string rather than a byte slice.
func (pq *PriorityQueue) EnqueueString(priority int64, value string) (*PriorityItem, error) {
	return pq.Enqueue(priority, []byte(value))
}

// EnqueueObject is a helper function for Enqueue that accepts any
// value type, which is then encoded into a byte slice using
// encoding/gob.
//
// Objects containing pointers with zero values will decode to nil
// when using this function. This is due to how the encoding/gob
// package works. Because of this, you should only use this function
// to encode simple types.
func (pq *PriorityQueue) EnqueueObject(priority int64, value interface{}) (*PriorityItem, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}
	return pq.Enqueue(priority, buffer.Bytes())
}

// EnqueueObjectAsJSON is a helper function for Enqueue that accepts
// any value type, which is then encoded into a JSON byte slice using
// encoding/json.
//
// Use this function to handle encoding of complex types.
func (pq *PriorityQueue) EnqueueObjectAsJSON(priority int64, value interface{}) (*PriorityItem, error) {
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return pq.Enqueue(priority, jsonBytes)
}

// Dequeue removes the next item in the priority queue and returns it.
func (pq *PriorityQueue) Dequeue() (*PriorityItem, error) {
	pq.RLock()
	defer pq.RUnlock()
	// Check if queue is closed.
	if !pq.isOpen {
		return nil, ErrDBClosed
	}
	// Try to get the next item.
	item, err := getNextItem(pq)

	if err != nil {
		return nil, err
	}
	// Remove this item from the priority queue.
	if err = pq.db.Delete(item.Key, nil); err != nil {
		return nil, err
	}
	// peak queue to determine if the current removed item causes a level to be empty
	// top level being "peaked at here should be ok as i is O(n)
	priority := Peek(pq.levelOrder)

	if priority != nil {

		level := pq.levelMap[priority.(*orderLevel).priority]
		(level).head++
		if (level).length() == 0 {
			delete(pq.levelMap, priority.(*orderLevel).priority)
			heap.Pop((*pq).levelOrder)
		}
	}
	return item, nil
}

// Peek returns the next item in the priority queue without removing it.
func (pq *PriorityQueue) Peek() (*PriorityItem, error) {
	pq.RLock()
	defer pq.RUnlock()

	// Check if queue is closed.
	if !pq.isOpen {
		return nil, ErrDBClosed
	}
	return getNextItem(pq)
}

func (pq *PriorityQueue) Length() uint64 {
	pq.RLock()
	defer pq.RUnlock()

	var length uint64
	for _, v := range pq.levelMap {

		if v != nil {
			length += v.tail - v.head
		}
	}
	return length
}

// Close closes the LevelDB database of the priority queue.
func (pq *PriorityQueue) Close() error {
	pq.Lock()
	defer pq.Unlock()
	// Check if queue is already closed.
	if !pq.isOpen {
		return nil
	}
	// Close the LevelDB database.
	if err := pq.db.Close(); err != nil {
		return err
	}
	// Reset head and tail of each priority priority
	// and set isOpen to false.
	pq.levelOrder = createLevelOrders(0)
	pq.levelMap = make(map[int64]*priorityLevel)
	pq.isOpen = false

	return nil
}

// Drop closes and deletes the LevelDB database of the priority queue.
func (pq *PriorityQueue) Drop() error {
	if err := pq.Close(); err != nil {
		return err
	}
	return os.RemoveAll(pq.DataDir)
}

// resetCurrentLevel resets the current priority priority of the queue
// so the highest priority can be found.

// getNextItem returns the next item in the priority queue, updating
// the current priority priority of the queue if necessary.
func getNextItem(pq *PriorityQueue) (*PriorityItem, error) {

	priority := Peek(pq.levelOrder)

	//TODO: clean this type casting up
	if priority == nil {
		return nil, ErrEmpty
	}

	if pq.levelMap[priority.(*orderLevel).priority] == nil {
		return nil, ErrEmpty
	}
	level := pq.levelMap[priority.(*orderLevel).priority]

	id := (*level).head + 1

	if id <= (*level).head || id > (*level).tail {
		return nil, ErrOutOfBounds
	}
	// Get item from database.
	var err error
	item := &PriorityItem{ID: id, Priority: (*level).priority, Key: pq.generateKey((*level).priority, id)}
	if item.Value, err = pq.db.Get(item.Key, nil); err != nil {
		return nil, err
	}
	return item, nil
}

// generatePrefix creates the key prefix for the given priority priority.
func (pq *PriorityQueue) generatePrefix(level int64) []byte {
	prefix := make([]byte, 9)
	binary.BigEndian.PutUint64(prefix[:8], uint64(level))
	prefix[8] = prefixSep[0]
	return prefix
}

// generateKey create a key to be used with LevelDB.
func (pq *PriorityQueue) generateKey(priority int64, id uint64) []byte {
	// prefix + key = 2 + 8 = 10
	key := make([]byte, 17)
	copy(key[0:9], pq.generatePrefix(priority))
	copy(key[9:], idToKey(id))
	return key
}

// init initializes the priority queue data.
func (pq *PriorityQueue) init() error {
	// Set starting value for curLevel.

	iter := pq.db.NewIterator(nil, nil)

	for iter.Next() {
		// Remember that the contents of the returned slice should not be modified, and
		// only valid until the
		// next call to Next.
		position := keyToID(iter.Key()[9:]) - 1
		priority := idToLevel(iter.Key()[:8])

		level := pq.levelMap[int64(priority)]

		if level == nil {
			// Create a new priorityLevel.
			orderLevel := &orderLevel{
				priority: int64(priority),
				Index:    0,
			}
			level = &priorityLevel{
				head:     0,
				tail:     0,
				priority: int64(priority),
			}

			pq.levelMap[int64(priority)] = level
			heap.Push(pq.levelOrder, orderLevel)
		}

		// if value is new head or tail set accordingly
		if position > (*level).tail {
			(*level).tail = position
		}
		if position < (*level).head {
			(*level).head = position
		}
		if iter.Error() != nil {
			return iter.Error()
		}
	}
	iter.Release()

	return nil
}
