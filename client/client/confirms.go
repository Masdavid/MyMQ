package client

import (
	"sync"

	// "time"
	// "fmt"
)

type confirms struct {
	m         sync.Mutex
	listeners []chan Confirmation

	seqModeOne map[uint64]Confirmation
	published uint64
	expecting uint64
	consLock sync.Mutex
	confirmSignal map[uint64](chan bool) 

}

func newConfirms() *confirms {
	return &confirms{
		seqModeOne: map[uint64]Confirmation{},
		confirmSignal : map[uint64](chan bool) {},
		published: 0,
		expecting: 1,

	}
}
//seq中存着提前到达的confirm信息，用该函数处理连续
func (c *confirms) handleSeqOne() {
	for c.expecting <= c.published {
		seq, found := c.seqModeOne[c.expecting]
		if !found {
			return
		}
		c.confirm(seq)
	}
}

func (c *confirms) Listen(l chan Confirmation) {
	c.m.Lock()
	defer c.m.Unlock()

	c.listeners = append(c.listeners, l)
}

func (c *confirms) Publish() uint64 {
	c.m.Lock()
	defer c.m.Unlock()
	c.published++
	return c.published
}

//持久话消息的确认函数，移动滑窗
func (c *confirms) confirm(confirmation Confirmation) {
	delete(c.seqModeOne, c.expecting)
	c.expecting++
	for _, l := range c.listeners {
		l <- confirmation
	}
}

//确认函数，移动滑窗
func (c *confirms) confirmMulti(confirmation Confirmation) {
	c.expecting++
	for _, l := range c.listeners {
		l <- confirmation
	}
}

//该函数清空那些在expecting与published之间的，即已经发送的消息中
//以expecting为起点，连续得到确认的，即expectded代表expectded之前所有消息都得到确认 
func (c *confirms) One(con Confirmation) {
	c.m.Lock()
	c.consLock.Lock()
	defer c.m.Unlock()
	defer c.consLock.Unlock()

	//已经确认的消息、向通知管道中发送信息， 发送完毕后关闭并删除该管道
	dTag := con.DeliveryTag
	ch, ok := c.confirmSignal[dTag]
	if ok {
		close(ch)
	}

	//如果收到的COnfirmtag是expected，那么直接确认即可，如果不是，放入seqModeOne，代表
	//是已经confirm的，但是不是期待的（），则先存起来
	// fmt.Println("expecting:", c.expecting)
	// fmt.Println("com deliveryTag:", con.DeliveryTag)
	if c.expecting == con.DeliveryTag {
		c.confirm(con)
	}else{
		c.seqModeOne[con.DeliveryTag] = con
	} 
	c.handleSeqOne()
}

//累计确认模式，适用于非持久化的消息
func (c *confirms) Multiple(con Confirmation) {
	c.m.Lock()
	c.consLock.Lock()
	defer c.m.Unlock()
	defer c.consLock.Unlock()
	for c.expecting <= con.DeliveryTag {

		dTag := con.DeliveryTag
		ch, ok := c.confirmSignal[dTag]

		if ok {
			close(ch)
		}
		c.confirmMulti(Confirmation{c.expecting, con.Ack})
	}
}

//关闭所有管道
func (c *confirms) Close() error {
	c.m.Lock()
	defer c.m.Unlock()

	for _, l := range c.listeners {
		close(l)
	}
	for _, ch := range c.confirmSignal {
		close(ch)
	}
	c.listeners = nil
	return nil
}

func (c *confirms) getExpectedTag() uint64 {
	c.m.Lock()
	defer c.m.Unlock()
	res := c.expecting
	return res
}

func (c *confirms) getPublishTag() uint64 {
	c.m.Lock()
	defer c.m.Unlock()
	res := c.published
	return res
}
