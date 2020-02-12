package goqueDynamicPriority

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
	ASC  order = iota // Set priority level 0 as most important.
	DESC              // Set priority level 255 as most important.
)

// priorityLevel holds the head and tail position of a priority
// level within the queue.

//TODO: figure out if Index is required
type priorityLevel struct {
	level int64
	head  uint64
	tail  uint64
	Index int
}

// length returns the total number of items in this priority level.
func (pl *priorityLevel) length() uint64 {
	return pl.tail - pl.head
}

// PriorityQueue is a standard FIFO (first in, first out) queue with
// priority levels.
type PriorityQueue struct {
	sync.RWMutex
	DataDir  string
	db       *leveldb.DB
	order    order
	levels   *priorityLevels
	curLevel int64
	isOpen   bool
}

// OpenPriorityQueue opens a priority queue if one exists at the given
// directory. If one does not already exist, a new priority queue is
// created.
func OpenPriorityQueue(dataDir string, order order) (*PriorityQueue, error) {
	var err error

	// Create a new PriorityQueue.
	pq := &PriorityQueue{
		DataDir: dataDir,
		db:      &leveldb.DB{},
		levels:  &priorityLevels{},
		order:   order,
		isOpen:  false,
	}

	// Open database for the priority queue.
	pq.db, err = leveldb.OpenFile(dataDir, nil)
	if err != nil {
		return pq, err
	}

	pq.levels = createPriorityLevels(0)

	// Check if this Goque type can open the requested data directory.
	ok, err := checkGoqueType(dataDir, goquePriorityQueue)
	if err != nil {
		return pq, err
	}
	if !ok {
		return pq, ErrIncompatibleType
	}

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
	level := pq.levels.getLevel(priority)
	// create new priority object if it is not in heap
	if level == nil {
		pl := &priorityLevel{
			head:  0,
			tail:  0,
			level: priority,
		}

		// issue exists with
		heap.Push((*pq).levels, pl)
		level = pl
	}
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
	// Increment tail position.

	// idea is
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
	pq.Lock()
	defer pq.Unlock()

	// Check if queue is closed.
	if !pq.isOpen {
		return nil, ErrDBClosed
	}

	// Try to get the next item.
	item, err := pq.getNextItem()
	if err != nil {
		return nil, err
	}

	// Remove this item from the priority queue.
	if err = pq.db.Delete(item.Key, nil); err != nil {
		return nil, err
	}

	// Increment head position.
	level := pq.levels.getLevel(pq.curLevel)

	if level != nil {
		// todo: test if this actually modifies level
		(*level).head++
		if level.length() == 0 {
			heap.Pop((*pq).levels)
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

	return pq.getNextItem()
}

// Length returns the total number of items in the priority queue.
func (pq *PriorityQueue) Length() uint64 {
	pq.RLock()
	defer pq.RUnlock()

	var length uint64
	for _, v := range pq.levels.getLevelList() {
		length += (v.tail - v.head)
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
	// Reset head and tail of each priority level
	// and set isOpen to false.
	pq.levels = createPriorityLevels(0)
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

// resetCurrentLevel resets the current priority level of the queue
// so the highest level can be found.
func (pq *PriorityQueue) resetCurrentLevel() {
	pq.curLevel = (*pq).levels.getLevelList()[0].level
}

// getNextItem returns the next item in the priority queue, updating
// the current priority level of the queue if necessary.
func (pq *PriorityQueue) getNextItem() (*PriorityItem, error) {
	// If the current priority level is empty.

	level := Peek(pq.levels)
	if level == nil {
		return nil, ErrEmpty
	}

	// TODO: position 0 is null and allows length comparison
	id := level.(priorityLevel).head + 1

	if id <= level.(priorityLevel).head || id > level.(priorityLevel).tail {
		return nil, ErrOutOfBounds
	}

	// Get item from database.
	var err error
	item := &PriorityItem{ID: id, Priority: level.(priorityLevel).level, Key: pq.generateKey(level.(priorityLevel).level, id)}
	if item.Value, err = pq.db.Get(item.Key, nil); err != nil {
		return nil, err
	}

	return item, nil
}

// getItemByID returns an item, if found, for the given ID.

// generatePrefix creates the key prefix for the given priority level.
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
// TODO: this will need a drastic logic change
func (pq *PriorityQueue) init() error {
	// Set starting value for curLevel.
	pq.resetCurrentLevel()

	iter := pq.db.NewIterator(nil, nil)

	for iter.Next() {
		// Remember that the contents of the returned slice should not be modified, and
		// only valid until the
		// next call to Next.
		position := keyToID(iter.Key()[10:]) - 1
		priority := idToLevel(iter.Key()[:9])

		level := pq.levels.getLevel(int64(priority))

		if level == nil {
			// Create a new priorityLevel.
			level = &priorityLevel{
				head:  0,
				tail:  0,
				level: int64(priority),
			}
			heap.Push(pq.levels, level)
		}

		// if value is new head or tail set accordingly
		if position > (*level).tail {
			(*level).tail = position
		}
		if position < (*level).head {
			(*level).head = position
		}

		// Set priority level head to the first item.

		if iter.Error() != nil {
			return iter.Error()
		}
	}
	iter.Release()

	return nil
}
