package basicQueue

import (
	"sync"
	"broker/dmqp"
)

//一个简单的线程安全的队列
type SimpleQueue struct {
	sqLock sync.Mutex
	data []*dmqp.Message
	queueLength uint64
	initCap uint64
}

func NewSimpleQueue(initCap uint64) *SimpleQueue {
	que := &SimpleQueue{
		queueLength : 0,
		data : make([]*dmqp.Message, 0, initCap),
		initCap : initCap,
	}
	return que
}

func (sq *SimpleQueue) Push(item *dmqp.Message){
	sq.sqLock.Lock()
	defer sq.sqLock.Unlock()

	sq.data = append(sq.data, item)
	sq.queueLength++
}

func (sq *SimpleQueue) Pop() (item *dmqp.Message) {
	sq.sqLock.Lock()
	defer sq.sqLock.Unlock()
	if sq.queueLength == 0{
		return nil
	}else{	
		item = sq.data[0]
		sq.data = sq.data[1:]
		sq.queueLength--
		return item
	}
}

func (sq *SimpleQueue) PushHead(item *dmqp.Message) {
	tmpData := make([]*dmqp.Message, sq.queueLength + 1)
	tmpData[0] = item
	copy(tmpData[1:], sq.data)
	sq.data = tmpData
}

func(sq *SimpleQueue) Length() uint64{
	sq.sqLock.Lock()
	defer sq.sqLock.Unlock()
	return sq.queueLength
}

func (sq *SimpleQueue) Clear() {
	sq.sqLock.Lock()
	defer sq.sqLock.Unlock()
	sq.data = make([]*dmqp.Message, 0, sq.initCap)
	sq.queueLength = 0
}

func (sq *SimpleQueue) HeadItem() *dmqp.Message{
	sq.sqLock.Lock()
	defer sq.sqLock.Unlock()
	if sq.queueLength == 0{
		return nil
	}else {
		return sq.data[0]
	}
}