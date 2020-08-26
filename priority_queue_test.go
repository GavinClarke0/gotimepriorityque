package gotimepriorityque

import (
	"fmt"
	"math"
	"os"
	"testing"
	"time"
)

func TestMassEnqueue(t *testing.T) {

	fmt.Println(time.Now().Unix())

	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	pq, err := OpenPriorityQueue(file, ASC)
	if err != nil {
		t.Error(err)
	}
	defer pq.Drop()

	for p := 0; p <= 10000000; p++ {
		if _, err = pq.EnqueueString(int64(p), "test"); err != nil {
			t.Error(err)
		}
	}

}

func TestEnqueueDequeSameLevel(t *testing.T) {

	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	pq, err := OpenPriorityQueue(file, ASC)
	if err != nil {
		t.Error(err)
	}
	defer pq.Drop()

	for p := 0; p <= 10000; p++ {
		if _, err = pq.EnqueueString(int64(p+1598456756), "test"); err != nil {
			t.Error(err)
		}
		if _, err = pq.EnqueueString(int64(p+1598456756), "test"); err != nil {
			t.Error(err)
		}
	}
	for p := 0; p <= 10000; p++ {
		if item, err := pq.Dequeue(); err == nil {
			if item.Priority != int64(p+1598456756) {
				t.Error("Incorrect Priority Dequeue")
			}
		}
		if item, err := pq.Dequeue(); err == nil {
			if item.Priority != int64(p+1598456756) {
				t.Error("Incorrect Priority Dequeue")
			}
		}
	}

}

func TestEnqueueDeque(t *testing.T) {

	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	pq, err := OpenPriorityQueue(file, ASC)
	if err != nil {
		t.Error(err)
	}
	defer pq.Drop()

	for p := 0; p <= 10000; p++ {
		if _, err = pq.EnqueueString(int64(p), "test"); err != nil {
			t.Error(err)
		}
	}
	for p := 0; p <= 10000; p++ {
		if item, err := pq.Dequeue(); err == nil {
			if item.Priority != int64(p) {
				t.Error("Incorrect Priority Dequeue")
			}
		}
	}
}

func TestAlternatingEnqueueDeque(t *testing.T) {

	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	pq, err := OpenPriorityQueue(file, ASC)
	if err != nil {
		t.Error(err)
	}
	defer pq.Drop()

	for p := 0; p <= 10000; p++ {
		if _, err = pq.EnqueueString(int64(p), "test"); err != nil {
			t.Error(err)
		}
		if item, err := pq.Dequeue(); err == nil {
			if item.Priority != int64(p) {
				t.Error("Incorrect Priority Dequeue")
			}
		}
	}
}

func TestPriorityQueueClose(t *testing.T) {

	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	pq, err := OpenPriorityQueue(file, ASC)
	if err != nil {
		t.Error(err)
	}
	defer pq.Drop()

	for p := 0; p <= 4; p++ {
		for i := 1; i <= 10; i++ {
			if _, err = pq.EnqueueString(int64(p), fmt.Sprintf("value for item %d", i)); err != nil {
				t.Error(err)
			}
		}
	}

	if pq.Length() != 50 {
		t.Errorf("Expected queue length of 1, got %d", pq.Length())
	}

	pq.Close()

	if _, err = pq.Dequeue(); err != ErrDBClosed {
		t.Errorf("Expected to get database closed error, got %s", err.Error())
	}

	if pq.Length() != 0 {
		fmt.Println(err)
		t.Errorf("Expected queue length of 0, got %d", pq.Length())
	}
}

func TestPriorityQueueDrop(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	pq, err := OpenPriorityQueue(file, ASC)
	if err != nil {
		t.Error(err)
	}

	if _, err = os.Stat(file); os.IsNotExist(err) {
		t.Error(err)
	}

	pq.Drop()

	if _, err = os.Stat(file); err == nil {
		t.Error("Expected directory for test database to have been deleted")
	}
}

func TestPriorityQueueEnqueue(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	pq, err := OpenPriorityQueue(file, ASC)
	if err != nil {
		t.Error(err)
	}
	defer pq.Drop()

	for p := 0; p <= 4; p++ {
		for i := 1; i <= 10; i++ {
			if _, err = pq.EnqueueString(int64(p), fmt.Sprintf("value for item %d", i)); err != nil {
				t.Error(err)
			}
		}
	}

	if pq.Length() != 50 {
		t.Errorf("Expected queue size of 50, got %d", pq.Length())
	}
}

func TestPriorityQueueDequeueAsc(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	pq, err := OpenPriorityQueue(file, ASC)
	if err != nil {
		t.Error(err)
	}
	defer pq.Drop()

	for p := 0; p <= 4; p++ {
		for i := 1; i <= 10; i++ {
			if _, err = pq.EnqueueString(int64(p), fmt.Sprintf("value for item %d", i)); err != nil {
				t.Error(err)
			}
		}
	}

	if pq.Length() != 50 {
		t.Errorf("Expected queue length of 1, got %d", pq.Length())
	}

	deqItem, err := pq.Dequeue()
	if err != nil {
		t.Error(err)
	}

	if pq.Length() != 49 {
		t.Errorf("Expected queue length of 49, got %d", pq.Length())
	}

	compStr := "value for item 1"

	if deqItem.Priority != 0 {
		t.Errorf("Expected priority priority to be 0, got %d", deqItem.Priority)
	}

	if deqItem.ToString() != compStr {
		t.Errorf("Expected string to be '%s', got '%s'", compStr, deqItem.ToString())
	}
}

/*

func TestPriorityQueueDequeueDesc(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	pq, err := OpenPriorityQueue(file, DESC)
	if err != nil {
		t.Error(err)
	}
	defer pq.Drop()

	for p := 0; p <= 4; p++ {
		for i := 1; i <= 10; i++ {
			if _, err = pq.EnqueueString(int64(p), fmt.Sprintf("value for item %d", i)); err != nil {
				t.Error(err)
			}
		}
	}

	if pq.Length() != 50 {
		t.Errorf("Expected queue length of 1, got %d", pq.Length())
	}

	deqItem, err := pq.Dequeue()
	if err != nil {
		t.Error(err)
	}

	if pq.Length() != 49 {
		t.Errorf("Expected queue length of 49, got %d", pq.Length())
	}

	compStr := "value for item 1"

	if deqItem.Priority != 4 {
		t.Errorf("Expected priority priority to be 4, got %d", deqItem.Priority)
	}

	if deqItem.ToString() != compStr {
		t.Errorf("Expected string to be '%s', got '%s'", compStr, deqItem.ToString())
	}
}
*/
func TestPriorityQueueEncodeDecodePointerJSON(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	pq, err := OpenPriorityQueue(file, DESC)
	if err != nil {
		t.Error(err)
	}
	defer pq.Drop()

	type subObject struct {
		Value *int
	}

	type object struct {
		Value     int
		SubObject subObject
	}

	val := 0
	obj := object{
		Value: 0,
		SubObject: subObject{
			Value: &val,
		},
	}

	if _, err = pq.EnqueueObjectAsJSON(0, obj); err != nil {
		t.Error(err)
	}

	item, err := pq.Dequeue()
	if err != nil {
		t.Error(err)
	}

	var itemObj object
	if err := item.ToObjectFromJSON(&itemObj); err != nil {
		t.Error(err)
	}

	if *itemObj.SubObject.Value != 0 {
		t.Errorf("Expected object subobject value to be '0', got '%v'", *itemObj.SubObject.Value)
	}
}

func TestPriorityQueuePeek(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	pq, err := OpenPriorityQueue(file, ASC)
	if err != nil {
		t.Error(err)
	}
	defer pq.Drop()

	for p := 0; p <= 4; p++ {
		for i := 1; i <= 10; i++ {
			if _, err = pq.EnqueueString(int64(p)+1581551271, fmt.Sprintf("value for item %d", i)); err != nil {
				t.Error(err)
			}
		}
	}

	compStr := "value for item 1"

	peekItem, err := pq.Peek()
	if err != nil {
		t.Error(err)
	}

	if peekItem.Priority != 0+1581551271 {
		t.Errorf("Expected priority priority to be 1581551271, got %d", peekItem.Priority)
	}

	if peekItem.ToString() != compStr {
		t.Errorf("Expected string to be '%s', got '%s'", compStr, peekItem.ToString())
	}

	if pq.Length() != 50 {
		t.Errorf("Expected queue length of 50, got %d", pq.Length())
	}
}

func TestPriorityQueueHigherPriorityAsc(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	pq, err := OpenPriorityQueue(file, ASC)
	if err != nil {
		t.Error(err)
	}
	defer pq.Drop()

	for p := 5; p <= 9; p++ {
		for i := 1; i <= 10; i++ {
			if _, err = pq.EnqueueString(int64(p), fmt.Sprintf("value for item %d", i)); err != nil {
				t.Error(err)
			}
		}
	}

	item, err := pq.Dequeue()
	if err != nil {
		t.Error(err)
	}

	if item.Priority != 5 {
		t.Errorf("Expected priority priority to be 5, got %d", item.Priority)
	}

	_, err = pq.EnqueueString(2, "value")
	if err != nil {
		t.Error(err)
	}

	higherItem, err := pq.Dequeue()
	if err != nil {
		t.Error(err)
	}

	if higherItem.Priority != 2 {
		t.Errorf("Expected priority priority to be 2, got %d", higherItem.Priority)
	}
}

/*
func TestPriorityQueueHigherPriorityDesc(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	pq, err := OpenPriorityQueue(file, DESC)
	if err != nil {
		t.Error(err)
	}
	defer pq.Drop()

	for p := 5; p <= 9; p++ {
		for i := 1; i <= 10; i++ {
			if _, err = pq.EnqueueString(int64(p), fmt.Sprintf("value for item %d", i)); err != nil {
				t.Error(err)
			}
		}
	}

	item, err := pq.Dequeue()
	if err != nil {
		t.Error(err)
	}

	if item.Priority != 9 {
		t.Errorf("Expected priority priority to be 9, got %d", item.Priority)
	}

	_, err = pq.EnqueueString(12, "value")
	if err != nil {
		t.Error(err)
	}

	higherItem, err := pq.Dequeue()
	if err != nil {
		t.Error(err)
	}

	if higherItem.Priority != 12 {
		t.Errorf("Expected priority priority to be 12, got %d", higherItem.Priority)
	}
}
*/
func TestPriorityQueueEmpty(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	pq, err := OpenPriorityQueue(file, ASC)
	if err != nil {
		t.Error(err)
	}
	defer pq.Drop()

	_, err = pq.EnqueueString(0, "value for item")
	if err != nil {
		fmt.Println(err)
		t.Error(err)
	}

	_, err = pq.Dequeue()
	if err != nil {
		fmt.Println(err)
		t.Error(err)
	}

	_, err = pq.Dequeue()
	if err != ErrEmpty {
		t.Errorf("Expected to get empty error, got %s", err.Error())
	}
}

func BenchmarkPriorityQueueEnqueue(b *testing.B) {
	// Open test database
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	pq, err := OpenPriorityQueue(file, ASC)
	if err != nil {
		b.Error(err)
	}
	defer pq.Drop()

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		_, _ = pq.EnqueueString(0, "value")
	}
}

func BenchmarkPriorityQueueDequeue(b *testing.B) {
	// Open test database
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	pq, err := OpenPriorityQueue(file, ASC)
	if err != nil {
		b.Error(err)
	}
	defer pq.Drop()

	// Fill with dummy data
	for n := 0; n < b.N; n++ {
		if _, err = pq.EnqueueString(int64(math.Mod(float64(n), 255)), "value"); err != nil {
			b.Error(err)
		}
	}

	// Start benchmark
	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		_, _ = pq.Dequeue()
	}
}
