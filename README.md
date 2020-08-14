Goq is a durable priority queue structure based off of [goque](https://github.com/beeker1121/goque) but heavily modified 
to allow 64 bit integer keys for the storing of time stamped data.

Like goque, everything is stored using the [Go port of LevelDB](https://github.com/syndtr/goleveldb), however unlike 
goque a in memory heap structure tracks queue position allowing near constant inserts and reads for a significantly larger 
range of key values. Upon start up all in memory structures are generated allowing near instant shutdown recovery. goq is 
less suitable for handling of data that cannot be stored in memory than [goque](https://github.com/beeker1121/goque) 
however it is still affective in this role just to a lesser extent in a trade off for increased priority key range. 


## Features
- Fast optimized dequeues and enqueues 
- Persistent, disk-based.
- Goroutine safe.
- Designed to work with large datasets outside of RAM/memory that are sorted by time
- Works with strings, structs and other data types

## Installation

Fetch the package from GitHub:

```sh
go get github.com/beeker1121/goque
```

Import to your project:

```go
import "github.com/GavinClarke0/gotimepriorityque"
```

## Usage

### Priority Queue

```go
	pq, err := OpenPriorityQueue(file, ASC)
	if err != nil {
		t.Error(err)
	}
	defer pq.Drop()
```

Push an item:

```go
_, err = pq.EnqueueObject(int64(p), "test")
	
```

Pop an item:

```go
item, err := pq.Dequeue()
...
fmt.Println(item.ID)         // 1
fmt.Println(item.Key)        // [0 0 0 0 0 0 0 1]
fmt.Println(item.Value)      // [105 116 101 109 32 118 97 108 117 101]
fmt.Println(item.ToString()) // item value

// Decode to object.
var obj Object
err := item.ToObject(&obj)

```

Peek the next stack item:

```go
item, err := pq.Peek()

fmt.Println(item.ID)         // 1
fmt.Println(item.Key)        // [0 0 0 0 0 0 0 1]
fmt.Println(item.Value)      // [105 116 101 109 32 118 97 108 117 101]
fmt.Println(item.ToString())
```

## Thanks To:
https://github.com/beeker1121 