package basicQueue

import (
	"sync"

	"broker/dmqp"
)

type SafeQueue struct {
	sync.RWMutex
	shards    [][]*dmqp.Message
	shardSize int
	tailIdx   int
	tail      []*dmqp.Message
	tailPos   int
	headIdx   int
	head      []*dmqp.Message
	headPos   int
	length    uint64
}

func NewSafeQueue(shardSize int) *SafeQueue {
	queue := &SafeQueue{
		shardSize: shardSize,
		shards:    [][]*dmqp.Message{make([]*dmqp.Message, shardSize)},
	}

	queue.tailIdx = 0
	queue.tail = queue.shards[queue.tailIdx]
	queue.headIdx = 0
	queue.head = queue.shards[queue.headIdx]
	return queue
}

// Push adds message into queue tail
func (queue *SafeQueue) Push(item *dmqp.Message) {
	queue.Lock()
	defer queue.Unlock()

	queue.tail[queue.tailPos] = item
	queue.tailPos++
	queue.length++

	if queue.tailPos == queue.shardSize {
		queue.tailPos = 0
		queue.tailIdx = len(queue.shards)

		buffer := make([][]*dmqp.Message, len(queue.shards)+1)
		buffer[queue.tailIdx] = make([]*dmqp.Message, queue.shardSize)
		copy(buffer, queue.shards)

		queue.shards = buffer
		queue.tail = queue.shards[queue.tailIdx]
	}
}

// PushHead adds message into queue head
func (queue *SafeQueue) PushHead(item *dmqp.Message) {
	queue.Lock()
	defer queue.Unlock()

	if queue.headPos == 0 {
		buffer := make([][]*dmqp.Message, len(queue.shards)+1)
		copy(buffer[1:], queue.shards)
		buffer[queue.headIdx] = make([]*dmqp.Message, queue.shardSize)

		queue.shards = buffer
		queue.tailIdx++
		queue.headPos = queue.shardSize
		queue.tail = queue.shards[queue.tailIdx]
		queue.head = queue.shards[queue.headIdx]
	}
	queue.length++
	queue.headPos--
	queue.head[queue.headPos] = item
}

// Pop retrieves message from head
func (queue *SafeQueue) Pop() (item *dmqp.Message) {
	queue.Lock()
	item = queue.DirtyPop()
	queue.Unlock()
	return
}

// DirtyPop retrieves message from head
// This method is not thread safe
func (queue *SafeQueue) DirtyPop() (item *dmqp.Message) {
	item, queue.head[queue.headPos] = queue.head[queue.headPos], nil
	if item == nil {
		return item
	}
	queue.headPos++
	queue.length--
	if queue.headPos == queue.shardSize {
		buffer := make([][]*dmqp.Message, len(queue.shards)-1)
		copy(buffer, queue.shards[queue.headIdx+1:])

		queue.shards = buffer

		queue.headPos = 0
		queue.tailIdx--
		queue.head = queue.shards[queue.headIdx]
	}
	return
}

func (queue *SafeQueue) Length() uint64 {
	queue.RLock()
	defer queue.RUnlock()
	return queue.length
}

// DirtyLength returns queue length
// This method is not thread safe
func (queue *SafeQueue) DirtyLength() uint64 {
	return queue.length
}

func (queue *SafeQueue) HeadItem() (res *dmqp.Message) {
	return queue.head[queue.headPos]
}

// This method is not thread safe
func (queue *SafeQueue) DirtyClear() {
	queue.shards = [][]*dmqp.Message{make([]*dmqp.Message, queue.shardSize)}
	queue.tailIdx = 0
	queue.tail = queue.shards[queue.tailIdx]
	queue.headIdx = 0
	queue.head = queue.shards[queue.headIdx]
	queue.length = 0
}

func (queue *SafeQueue) Clear() {
	queue.Lock()
	defer queue.Unlock()
	queue.DirtyClear()
}
