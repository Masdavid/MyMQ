package consumer

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"broker/dmqp"
	"broker/someTypes"
	"broker/cmrlimit"
	"broker/queue"
)

const (
	started = iota
	stopped
	paused
)

var cid uint64

type Consumer struct {
	Queue       string    //对应的que的名字
	ConsumerTag string		//comsumer的tag
	noAck       bool		//ack模式的选取
	chOwnSelf     someTypes.Channel 
	queue       *queue.Queue 

	sLock  sync.RWMutex
	status      int
	limits     []*cmrlimit.CmrLimit
	comSignalCh     chan struct{}
}


func NewConsumer(queueName string, cmrTag string, noAck bool, chOwnSelf someTypes.Channel, queue *queue.Queue, limits []*cmrlimit.CmrLimit) *Consumer {
	// id := atomic.AddUint64(&cid, 1)
	if cmrTag == "" {
		cmrTag = generateTag(atomic.AddUint64(&cid, 1))
	}
	return &Consumer{
		Queue:       queueName,
		ConsumerTag: cmrTag,
		noAck:       noAck,
		chOwnSelf:     chOwnSelf,
		queue:       queue,
		limits:         limits,
		comSignalCh:     make(chan struct{}, 1),
	}
}

//如果是“”的名字，随机产生一个tag
func generateTag(id uint64) string {
	return fmt.Sprintf("%d_%d", time.Now().Unix(), id)
}

//启动comsumer线程
func (cmr *Consumer) Start() {
	cmr.status = started
	go cmr.consumingThread()
	cmr.Consume()
}

func (cmr *Consumer) consumingThread() {
	for range cmr.comSignalCh {
		cmr.getMesByCmr()
	}
}

func (cmr *Consumer) getMesByCmr() {
	var message *dmqp.Message
	cmr.sLock.RLock()
	defer cmr.sLock.RUnlock()
	if cmr.status == stopped {
		return
	}

	//允许的话，一直发布消息，否则message会变为NULL
	if cmr.noAck {
		message = cmr.queue.Pop()
	} else {
		message = cmr.queue.PopWithLimit(cmr.limits)
	}

	if message == nil {
		return
	}

	//ack模式下，发布出去一个新消息，在channel里添加一个untracked message
	dTag := cmr.chOwnSelf.NextDeliveryTag()
	if !cmr.noAck {
		cmr.chOwnSelf.AddUnackedMes(dTag, cmr.ConsumerTag, cmr.queue.GetName(), message)
	}

	//将消息用SendFullFrame函数送出去
	cmr.chOwnSelf.SendFullFrame(&dmqp.BasicDeliver{
		ConsumerTag: cmr.ConsumerTag,
		DeliveryTag: dTag,
		Redelivered: message.DeliveryCount > 1,
		Exchange:    message.Exchange,
		RoutingKey:  message.RoutingKey,
	}, message)

	cmr.giveCmSignal()
	return
}
func (cmr *Consumer) Consume() bool {
	cmr.sLock.RLock()
	defer cmr.sLock.RUnlock()
	return cmr.giveCmSignal()
}

func (cmr *Consumer) giveCmSignal() bool {
	if cmr.status == stopped || cmr.status == paused {
		return false
	}

	select {
	case cmr.comSignalCh <- struct{}{}:
		return true
	default:
		return false
	}
}

func (cmr *Consumer) Stop() {
	cmr.sLock.Lock()
	if cmr.status == stopped {
		cmr.sLock.Unlock()
		return
	}
	cmr.status = stopped
	cmr.sLock.Unlock()
	cmr.queue.DelCmr(cmr.ConsumerTag)
	close(cmr.comSignalCh)
}

func (cmr *Consumer) Cancel() {
	cmr.Stop()
	cmr.chOwnSelf.SendMethodFrame(&dmqp.BasicCancel{ConsumerTag: cmr.ConsumerTag, NoWait: true})
}

func (cmr *Consumer) CmrTag() string {
	return cmr.ConsumerTag
}

func (cmr *Consumer) GetCmrlimit() []*cmrlimit.CmrLimit {
	return cmr.limits
}