package queue

import (
	"fmt"
	"sync"
	"bytes"
	"errors"
	"sync/atomic"

	"broker/dmqp"
	"broker/config"
	"broker/someTypes"
	"broker/cmrlimit"
	"broker/basicQueue"
	"broker/storem"
	// "mqserver/metrics"
)

type Queue struct {
	basicqLock sync.RWMutex
	// rawqs [3]basicQueue.SafeQueue      //设计了带有3种优先级的队列
	rawqs [3]basicQueue.SimpleQueue
	numOfPriority int
	name        string
	connID      uint64
	exclusive   bool
	autoDelete  bool
	durable     bool
	
	//与该队列连接的消费者相关的成员
	curCmr int
	cmrLock     sync.RWMutex
	consumers   []someTypes.Consumer
	consumeExcl bool
	call        chan struct{}
	wasConsumed bool


	actLock     sync.RWMutex
	active      bool
	delQueNow chan string
	queueLength     int64

	upLimitNum       uint64
	lastStoredMsgID        uint64
	lastMemMsgID           uint64
	wg                     *sync.WaitGroup

	queMsgStore *storem.MsgStore
}

func NewQueue(name string, connID uint64, exclusive bool, autoDelete bool, durable bool, config config.Queue, Qsp *storem.MsgStore, delQueNow chan string) *Queue {
	// var ques [3]basicQueue.SafeQueue
	var ques [3]basicQueue.SimpleQueue
	for i := 0; i < 3; i++{
		// ques[i] =  *basicQueue.NewSafeQueue(config.ShardSize)
		ques[i] =  *basicQueue.NewSimpleQueue(uint64(config.ShardSize))
	}
	return &Queue{
		rawqs:              ques,
		numOfPriority:      int(len(ques)),
		name:                   name,
		connID:                 connID,
		exclusive:              exclusive,
		autoDelete:             autoDelete,
		durable:                durable,
		call:                   make(chan struct{}, 1),
		wasConsumed:            false,
		active:                 false,
		upLimitNum:       config.MaxMessagesInRAM,
		queMsgStore:  Qsp,
		curCmr:        0,
		delQueNow:        delQueNow,
		wg:                     &sync.WaitGroup{},
	}
}


func (queue *Queue) cancelConsumers() {
	for _, cmr := range queue.consumers {
		cmr.Cancel()
	}
}

//给call发信息，进而通知消费者消费消息
func (queue *Queue) notifyCmr() {
	if !queue.active {
		return
	}
	select {
	case queue.call <- struct{}{}:
	default:
	}
}

//queue线程，接受到call信号时，让consumer消费消息
func (queue *Queue) Start() error {
	queue.actLock.Lock()
	defer queue.actLock.Unlock()

	if queue.active {
		return nil
	}

	queue.active = true
	queue.wg.Add(1)
	go func() {
		defer queue.wg.Done()
		for range queue.call {
			func() {
				queue.cmrLock.RLock()
				defer queue.cmrLock.RUnlock()
				cmrCount := len(queue.consumers)
				for i := 0; i < cmrCount; i++ {
					if !queue.active {
						return
					}
					queue.curCmr = (queue.curCmr + 1) % cmrCount
					cmr := queue.consumers[queue.curCmr]
					if cmr.Consume() {
						return
					}
				}
			}()
		}
	}()

	return nil
}

//关闭queue，等待start创建的线程的结束
func (queue *Queue) Stop() error {
	queue.actLock.Lock()
	defer queue.actLock.Unlock()

	queue.active = false
	close(queue.call)
	queue.wg.Wait()
	return nil
}

//broker重启时从数据库中加载原有的数据
func (q *Queue) LoadFromMsgStore() {
	if q.queMsgStore == nil {
		fmt.Println("queue queMsgStore is empty ptr")
	}

	numofmes := q.queMsgStore.IterateByQueWithStart(q.name, 0, q.upLimitNum, func(mes *dmqp.Message){
		pidx := mes.Header.Priority
		q.rawqs[pidx].Push(mes)
		q.lastStoredMsgID = mes.ID
		q.lastMemMsgID = mes.ID
		// fmt.Println("priority is", pidx)
	})

	q.queueLength = int64(numofmes)
}

//Push函数带有持久化的需求
func (queue *Queue) Push(message *dmqp.Message) {
	queue.actLock.Lock()
	defer queue.actLock.Unlock()

	if !queue.active {
		return
	}
	atomic.AddInt64(&queue.queueLength, 1)
	message.GenerateSeq()

	//该Add函数会更新message里ConfirmProp的信息，只有消息落盘了才能算确认，否则直接确认
	if queue.durable && message.IsDurable() {
		queue.queMsgStore.Add(message, queue.name)
		// fmt.Println("message going to db")
	}else{
		if message.ConfirmProp != nil {
			message.ConfirmProp.ConfirmNumHas++
		}
	}
	prior := int(message.Header.Priority)
	queue.rawqs[prior].Push(message)
	queue.lastMemMsgID = message.ID

	queue.notifyCmr()
}

//没有qos检查的push
func (queue *Queue) Pop() *dmqp.Message {
	return queue.PopWithLimit([]*cmrlimit.CmrLimit{})
}

//带有qos检查的push，核心是q.Inc(1, uint32(message.BodySize)，该函数check消费者的qos，
//如果满了，不允许发消息
func (queue *Queue) PopWithLimit(qosList []*cmrlimit.CmrLimit) *dmqp.Message {
	queue.actLock.RLock()
	if !queue.active {
		queue.actLock.RUnlock()
		return nil
	}
	queue.actLock.RUnlock()

	queue.basicqLock.Lock()
	var mes *dmqp.Message
	for i := 0; i < queue.numOfPriority; i++ {           //一旦遇到一个优先级较高的que中有消息的，就handle后break
		if mes = queue.rawqs[i].HeadItem(); mes == nil{
			continue
		}
		allowed := true
		for _, q := range qosList {
			if !q.IsActive() {
				continue
			}
			if !q.Inc(1, uint32(mes.BodySize)) {
				allowed = false
				break
			}
		}

		if allowed {
			// queue.rawqs[i].DirtyPop()
			queue.rawqs[i].Pop()
			atomic.AddInt64(&queue.queueLength, -1)
		} else {
			mes = nil
		}
		break
		
	}
	queue.basicqLock.Unlock()
	return mes
}

func (queue *Queue) DeleteMsgDurable(message *dmqp.Message) {
	queue.actLock.RLock()
	if !queue.active {
		queue.actLock.RUnlock()
		return
	}
	queue.actLock.RUnlock()

	if queue.durable && message.IsDurable() {
		// fmt.Println("delete msg from db")
		queue.queMsgStore.Del(message, queue.name)
	}
	return
}

//消息重新投递，因为有的consumer关闭了但是unack的消息不能凭空消失
func (q *Queue) Repush(message *dmqp.Message) {
	q.actLock.RLock()
	if !q.active {
		q.actLock.RUnlock()
		return
	}
	q.actLock.RUnlock()

	message.DeliveryCount++
	prior := int(message.Header.Priority)
	q.rawqs[prior].PushHead(message)

	if q.durable && message.IsDurable() {
		q.queMsgStore.Update(message, q.name)
	}

	atomic.AddInt64(&q.queueLength, 1)

	q.notifyCmr()
}

func (queue *Queue) Clear() (length uint64) {
	queue.basicqLock.Lock()
	defer queue.basicqLock.Unlock()
	length = uint64(atomic.LoadInt64(&queue.queueLength))
	for i := 0; i < queue.numOfPriority; i++ {
		// queue.rawqs[i].DirtyClear()
		queue.rawqs[i].Clear()
	}

	if queue.durable {
		queue.queMsgStore.PurgeQue(queue.name)
	}

	atomic.StoreInt64(&queue.queueLength, 0)
	return
}

//删除queue
func (queue *Queue) Delete(ifUnused bool, ifEmpty bool) (uint64, error) {
	queue.actLock.Lock()
	queue.cmrLock.Lock()
	queue.basicqLock.Lock()
	defer queue.actLock.Unlock()
	defer queue.cmrLock.Unlock()
	defer queue.basicqLock.Unlock()

	queue.active = false

	//unused 代表当队列中有消费者时不能删除
	if ifUnused && len(queue.consumers) != 0 {
		return 0, errors.New("queue has consumers")
	}

	//ifEmpty 代表当队列中有消息时不能删除

	if ifEmpty {
		for i := 0; i < queue.numOfPriority; i++ {
			// if queue.rawqs[i].DirtyLength() != 0 {
			// 	return 0, errors.New("queue has messages")
			// }

			if queue.rawqs[i].Length() != 0 {
				return 0, errors.New("queue has messages")
			}
		}
	}

	queue.cancelConsumers()
	length := uint64(atomic.LoadInt64(&queue.queueLength))

	//如果队列是持久话的，固定的消息也要删除
	if queue.durable {
		queue.queMsgStore.PurgeQue(queue.name)
	}

	return length, nil
}

//添加consumer，在独占模式下，一旦queue有消费者了，不能允许其他消费者介入
func (queue *Queue) AddCmr(consumer someTypes.Consumer, exclusive bool) error {
	queue.cmrLock.Lock()
	defer queue.cmrLock.Unlock()

	if !queue.active {
		return fmt.Errorf("queue is not active")
	}
	queue.wasConsumed = true

	if len(queue.consumers) != 0 && (queue.consumeExcl || exclusive) {
		return fmt.Errorf("queue is busy by %d consumers", len(queue.consumers))
	}

	if exclusive {
		queue.consumeExcl = true
	}

	queue.consumers = append(queue.consumers, consumer)

	queue.notifyCmr()
	return nil
}

//根据comserTag删除消费者，对于有自动删除功能的queue来说，如果失去了所有的consumer，就会自动删除
func (queue *Queue) DelCmr(cTag string) {
	queue.cmrLock.Lock()
	defer queue.cmrLock.Unlock()

	for i, cmr := range queue.consumers {
		if cmr.CmrTag() == cTag {
			queue.consumers = append(queue.consumers[:i], queue.consumers[i+1:]...)
			break
		}
	}
	cmrCount := len(queue.consumers)
	if cmrCount == 0 {
		queue.curCmr = 0
		queue.consumeExcl = false
	} else {
		queue.curCmr = (queue.curCmr + 1) % cmrCount
	}

	if cmrCount == 0 && queue.wasConsumed && queue.autoDelete {
		queue.delQueNow <- queue.name
	}
}


func (queue *Queue) Length() uint64 {
	return uint64(atomic.LoadInt64(&queue.queueLength))
}

func (queue *Queue) ConsumersCount() int {
	queue.cmrLock.RLock()
	defer queue.cmrLock.RUnlock()
	return len(queue.consumers)
}

func (queue *Queue) SameProp(qB *Queue) error {
	errTemplate := "inequivalent arg '%s' for queue '%s': received '%t' but current is '%t'"
	if queue.durable != qB.IsDurable() {
		return fmt.Errorf(errTemplate, "durable", queue.name, qB.IsDurable(), queue.durable)
	}
	if queue.autoDelete != qB.autoDelete {
		return fmt.Errorf(errTemplate, "autoDelete", queue.name, qB.autoDelete, queue.autoDelete)
	}
	if queue.exclusive != qB.IsExclusive() {
		return fmt.Errorf(errTemplate, "exclusive", queue.name, qB.IsExclusive(), queue.exclusive)
	}
	return nil
}

func (queue *Queue) GetName() string {
	return queue.name
}

func (queue *Queue) IsDurable() bool {
	return queue.durable
}

func (queue *Queue) IsExclusive() bool {
	return queue.exclusive
}

func (queue *Queue) IsAutoDelete() bool {
	return queue.autoDelete
}

func (queue *Queue) ConnID() uint64 {
	return queue.connID
}

func (queue *Queue) IsActive() bool {
	queue.actLock.Lock()
	defer queue.actLock.Unlock()

	return queue.active
}

func (queue *Queue) Serialize() (data []byte, err error) {
	buf := bytes.NewBuffer([]byte{})
	if err := dmqp.WriteShortstr(buf, queue.name); err != nil {
		return nil, err
	}
	var flag byte
	if queue.autoDelete {
		flag = 1
	}else {
		flag = 0
	}
	if err := dmqp.WriteOctet(buf, flag); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (q *Queue) Deserialize(data []byte) (err error) {
	buf := bytes.NewReader(data)
	if q.name, err = dmqp.ReadShortstr(buf); err != nil {
		return err
	}
	var flag byte
	if flag, err = dmqp.ReadByte(buf); err != nil {
		return err
	}
	q.autoDelete = flag > 0
	q.durable = true
	return
}

