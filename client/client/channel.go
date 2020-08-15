package client

import (
	"fmt"
	"sync"
	"time"
	"sync/atomic"
	"reflect"
)

const frameHeaderSize = 1 + 2 + 4 + 1
type Channel struct {
	closer sync.Once
	m          sync.Mutex // struct field mutex
	confirmM   sync.Mutex 
	notifyM    sync.RWMutex
	
	connection *Connection

	sars       chan message
	consumers *consumers
	id uint16
	closed int32
	noNotify bool
	closes []chan *Error
	returns []chan Return
	cancels []chan string
	confirms   *confirms
	confirming bool


	errors chan *Error
	recv func(*Channel, frame) error

	message messageWithContent
	header  *headerFrame
	body    []byte

	cslock sync.Mutex
	timeout sync.Mutex
	unConfimedStore map[uint64]*basicPublish

}

func newChannel(c *Connection, id uint16) *Channel {
	return &Channel{
		connection : c,
		id : id,
		sars : make(chan message),
		consumers:  makeConsumers(),
		confirms:   newConfirms(),
		recv:       (*Channel).recvMethod,
		errors:     make(chan *Error, 1),
		unConfimedStore : map[uint64]*basicPublish{},
		
	}
}

//打开信道
func (ch *Channel) open() error {
	return ch.SendAndResponseSync(&channelOpen{}, &channelOpenOk{})
}

func (ch *Channel) send(msg message) (err error) {
	if atomic.LoadInt32(&ch.closed) == 1 {
		return ch.sendClosed(msg)
	}

	return ch.sendOpen(msg)
}

func (ch *Channel) shutdown(e *Error) {
	ch.closer.Do(func() {
		ch.m.Lock()
		defer ch.m.Unlock()

		ch.notifyM.Lock()
		defer ch.notifyM.Unlock()

		if e != nil {
			for _, c := range ch.closes {
				c <- e
			}
		}
		atomic.StoreInt32(&ch.closed, 1)

		if e != nil {
			ch.errors <- e
		}

		ch.consumers.close()

		for _, c := range ch.closes {
			close(c)
		}

		for _, c := range ch.returns {
			close(c)
		}

		for _, c := range ch.cancels {
			close(c)
		}

		ch.closes = nil
		ch.returns = nil
		ch.cancels = nil

		if ch.confirms != nil {
			ch.confirms.Close()
		}

		close(ch.errors)
		ch.noNotify = true
	})
}

func (ch *Channel) sendClosed(msg message) (err error) {

	if _, ok := msg.(*channelCloseOk); ok {
		return ch.connection.send(&methodFrame{
			ChannelId: ch.id,
			Method:    msg,
		})
	}

	return ErrClosed
}

func (ch *Channel) Close() error {
	defer ch.connection.closeChannel(ch, nil)
	return ch.SendAndResponseSync(
		&channelClose{ReplyCode: replySuccess},
		&channelCloseOk{},
	)
}

func (ch *Channel) sendOpen(msg message) (err error) {
	//如果是一个带有内容的帧，那么将msg转为messageWithContent
	if content, ok := msg.(messageWithContent); ok {
		d, p, body := content.getContent()
		class, _ := content.id()

		var size int
		if ch.connection.config.FrameSize > 0 {
			size = ch.connection.config.FrameSize - frameHeaderSize
		} else {
			size = len(body)
		}

		//发送方法帧？
		if err = ch.connection.send(&methodFrame{
			ChannelId: ch.id,
			Method:    content,
		}); err != nil {
			return
		}

		//发送内容头帧
		if err = ch.connection.send(&headerFrame{
			ChannelId:  ch.id,
			ClassId:    class,
			Size:       uint64(len(body)),
			deliveryMode: d,
			Priority : p,
		}); err != nil {
			return
		}

		// 发送内容体帧
		for i, j := 0, size; i < len(body); i, j = j, j+size {
			if j > len(body) {
				j = len(body)
			}

			if err = ch.connection.send(&bodyFrame{
				ChannelId: ch.id,
				Body:      body[i:j],
			}); err != nil {
				return
			}
		}
	} else {
		//仅仅是只有方法的方法帧
		err = ch.connection.send(&methodFrame{
			ChannelId: ch.id,
			Method:    msg,
		})
	}

	return
}

//SendAndResponseSync函数的功能是，同步地发布req， 接收res，因为使用了同步管道sars
func (ch *Channel) SendAndResponseSync(req message, res ...message) error {
	if err := ch.send(req); err != nil {
		return err
	}
	if req.wait() {
		select {
		case e, ok := <-ch.errors:
			if ok {
				return e
			}
			return ErrClosed

		case msg := <-ch.sars:
			if msg != nil {
				for _, try := range res {
					if reflect.TypeOf(msg) == reflect.TypeOf(try) {
						vres := reflect.ValueOf(try).Elem()
						vmsg := reflect.ValueOf(msg).Elem()
						vres.Set(vmsg)
						return nil
					}
				}
				return ErrCommandInvalid
			}
			return ErrClosed
		}
	}
	return nil
}

func (ch *Channel) transition(f func(*Channel, frame) error) error {
	ch.recv = f
	return nil
}

//收到一个方法帧，判断类型
func (ch *Channel) recvMethod(f frame) error {
	switch frame := f.(type) {
	case *methodFrame:
		//如果是一个带内容的帧
		if msg, ok := frame.Method.(messageWithContent); ok {
			ch.body = make([]byte, 0)
			ch.message = msg     //携带的是方法帧里存的信息
			return ch.transition((*Channel).recvHeader)
		}

		ch.handleMethod(frame.Method) // termination state
		return ch.transition((*Channel).recvMethod)

	case *headerFrame:
		return ch.transition((*Channel).recvMethod)

	case *bodyFrame:
		return ch.transition((*Channel).recvMethod)
	}

	panic("unexpected frame type")
}

//代表接受内容头帧
func (ch *Channel) recvHeader(f frame) error {
	switch frame := f.(type) {
	case *methodFrame:
		return ch.recvMethod(f)
	case *headerFrame:
		ch.header = frame     //读取内容头帧
		if frame.Size == 0 {
			ch.message.setContent(ch.header.deliveryMode, ch.header.Priority, ch.body)
			ch.handleMethod(ch.message) // termination state
			return ch.transition((*Channel).recvMethod)
		}
		return ch.transition((*Channel).recvContent)
	case *bodyFrame:
		return ch.transition((*Channel).recvMethod)
	}

	panic("unexpected frame type")
}

//接受内容体帧
func (ch *Channel) recvContent(f frame) error {
	switch frame := f.(type) {
	case *methodFrame:
		return ch.recvMethod(f)

	case *headerFrame:
		return ch.transition((*Channel).recvMethod)

	case *bodyFrame:
		if cap(ch.body) == 0 {
			ch.body = make([]byte, 0, ch.header.Size)
		}
		ch.body = append(ch.body, frame.Body...)

		if uint64(len(ch.body)) >= ch.header.Size {
			//给ch.message设置内容，包括内容头帧中的properties信息和内容体信息
			ch.message.setContent(ch.header.deliveryMode, ch.header.Priority, ch.body)
			ch.handleMethod(ch.message) // termination state
			return ch.transition((*Channel).recvMethod)
		}

		return ch.transition((*Channel).recvContent)
	}

	panic("unexpected frame type")
}

//收到帧以后，根据处理方法的类型
func (ch *Channel) handleMethod(msg message) {
	switch m := msg.(type) {
	case *channelClose:
		//只允许发送clannelcloseok的命令
		ch.m.Lock()
		ch.send(&channelCloseOk{})
		ch.m.Unlock()
		ch.connection.closeChannel(ch, newError(m.ReplyCode, m.ReplyText))
	
	//删除消费者的命令
	case *basicCancel:
		ch.notifyM.RLock()
		for _, c := range ch.cancels {
			c <- m.ConsumerTag
		}
		ch.notifyM.RUnlock()
		ch.consumers.cancel(m.ConsumerTag)

	case *basicReturn:
		ret := newReturn(*m)
		ch.notifyM.RLock()
		for _, c := range ch.returns {
			c <- *ret
		}
		ch.notifyM.RUnlock()
	
	//收到ack代表发布消息
	case *basicAck:
		if ch.confirming {
			if m.Multiple {
				ch.confirms.Multiple(Confirmation{m.DeliveryTag, true})
			} else {
				ch.confirms.One(Confirmation{m.DeliveryTag, true})
			}
		}

	case *basicNack:
		if ch.confirming {
			if m.Multiple {
				ch.confirms.Multiple(Confirmation{m.DeliveryTag, false})
			} else {
				ch.confirms.One(Confirmation{m.DeliveryTag, false})
			}
		}

	case *basicDeliver:
		ch.consumers.send(m.ConsumerTag, newDelivery(ch, m))

	default:
		ch.sars <- msg
	}
}

//声明一个队列，用SendAndResponseSync函数 等待broker返回ok
func (ch *Channel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool) (Queue, error) {

	req := &queueDeclare{
		Queue:      name,
		Passive:    false,
		Durable:    durable,
		AutoDelete: autoDelete,
		Exclusive:  exclusive,
		NoWait:     noWait,
	}
	res := &queueDeclareOk{}

	if err := ch.SendAndResponseSync(req, res); err != nil {
		return Queue{}, err
	}

	if req.wait() {
		return Queue{
			Name:      res.Queue,
			Messages:  int(res.MessageCount),
			Consumers: int(res.ConsumerCount),
		}, nil
	}

	return Queue{Name: name}, nil
}

//发送binding交换机和队列的binding信息
func (ch *Channel) QueueBind(name, key, exchange string, noWait bool) error {

	return ch.SendAndResponseSync(
		&queueBind{
			Queue:      name,
			Exchange:   exchange,
			RoutingKey: key,
			NoWait:     noWait,
		},
		&queueBindOk{},
	)
}

func (ch *Channel) QueueUnbind(name, key, exchange string) error {
	return ch.SendAndResponseSync(
		&queueUnbind{
			Queue:      name,
			Exchange:   exchange,
			RoutingKey: key,
		},
		&queueUnbindOk{},
	)
}

//清空当前队列中还没有发送的消息，返回成功删除的消息的数量
func (ch *Channel) QueuePurge(name string, noWait bool) (int, error) {
	req := &queuePurge{
		Queue:  name,
		NoWait: noWait,
	}
	res := &queuePurgeOk{}

	err := ch.SendAndResponseSync(req, res)

	return int(res.MessageCount), err
}

func (ch *Channel) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	req := &queueDelete{
		Queue:    name,
		IfUnused: ifUnused,
		IfEmpty:  ifEmpty,
		NoWait:   noWait,
	}
	res := &queueDeleteOk{}

	err := ch.SendAndResponseSync(req, res)

	return int(res.MessageCount), err
}


//新建一个消费者，一个channel可以建立多个消费者
func (ch *Channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool) (<-chan Delivery, error) {

	if consumer == "" {
		consumer = getConsumerTag()
	}

	req := &basicConsume{
		Queue:       queue,
		ConsumerTag: consumer,
		NoLocal:     noLocal,
		NoAck:       autoAck,
		Exclusive:   exclusive,
		NoWait:      noWait,
	}
	res := &basicConsumeOk{}

	//声明一个管道，将consumer收到的消息输出出来，
	deliveries := make(chan Delivery)

	ch.consumers.add(consumer, deliveries)

	if err := ch.SendAndResponseSync(req, res); err != nil {
		ch.consumers.cancel(consumer)
		return nil, err
	}

	return (<-chan Delivery)(deliveries), nil
}

func (ch *Channel) CancelCmr(cmr  string, noWait bool) error {
	pub := &basicCancel{
		ConsumerTag : cmr,
		NoWait : noWait,
	}
	receive := &basicCancelOk{}
	if err := ch.SendAndResponseSync(pub, receive); err != nil {
		return err
	}
	if pub.wait() {
		ch.consumers.cancel(receive.ConsumerTag)
	}else{
		ch.consumers.cancel(cmr)
	}
	return nil
}

func (ch *Channel) ExchangeDeclare(name, kind string, durable, noWait bool) error {

	return ch.SendAndResponseSync(
		&exchangeDeclare{
			Exchange:   name,
			Type:       kind,
			Passive:    false,
			Durable:    durable,
			NoWait:     noWait,
		},
		&exchangeDeclareOk{},
	)
}

func (ch *Channel) ExchangeDelete(name string, ifUnused, noWait bool) error {
	return ch.SendAndResponseSync(
		&exchangeDelete{
			Exchange: name,
			IfUnused: ifUnused,
			NoWait:   noWait,
		},
		&exchangeDeleteOk{},
	)
}


func (ch *Channel) Publish(exchange, key string, mandatory, immediate bool, msg Publishing) error {
	ch.m.Lock()
	defer ch.m.Unlock()

	mes := &basicPublish{
		Exchange:   exchange,
		RoutingKey: key,
		Mandatory:  mandatory,
		Immediate:  immediate,
		Body:       msg.Body,
		DeliveryMode : msg.DeliveryMode,
		Priority : msg.Priority,
	}
	if mes.Priority > 2 || mes.Priority < 0 {
		fmt.Println("priority is illegal, please enter 0~2")
		return nil
	}

	//添加unconfirmMsg的信息
	if ch.confirming {
		curTag :=ch.confirms.Publish()
		mes.ConfirmDeliveryTag = curTag
		confirmSig := make(chan bool, 1)

		ch.confirms.consLock.Lock()
		ch.confirms.confirmSignal[curTag] = confirmSig
		ch.confirms.consLock.Unlock()
		
		ch.AddUnConfirmedMessage(curTag, mes)
	}
	ch.timeout.Lock()
	if err := ch.send(mes); err != nil {
		ch.timeout.Unlock()
		return err
	}
	ch.timeout.Unlock()
	return nil
}

//向map中添加unConfirm的消息，同时开启check
func (ch *Channel) AddUnConfirmedMessage(dTag uint64, mes *basicPublish) {
	ch.cslock.Lock()
	ch.unConfimedStore[dTag] = mes
	ch.cslock.Unlock()

	go ch.confirmCheck(dTag)
}

func (ch *Channel) deleteMesFromStore(dTag uint64) {
	ch.cslock.Lock()
	delete(ch.unConfimedStore, dTag)
	ch.cslock.Unlock()
}

func (ch *Channel) deleteSig(dTag uint64) {
	ch.confirms.consLock.Lock()
	delete(ch.confirms.confirmSignal, dTag)
	ch.confirms.consLock.Unlock()
}
//check函数 用管道传递unconfirm的消息是否confirm了，如果confirm了，直接退出check，否则重发消息
func (ch *Channel) confirmCheck(dTag uint64) error{
	//设置一个同步管道，监听是否有confirm的消息
	ticker := time.NewTicker(time.Second*5)

	ch.confirms.consLock.Lock()
	confirmSig := ch.confirms.confirmSignal[dTag]
	ch.confirms.consLock.Unlock()
	
	for {
		select {
		case <-confirmSig:
			ch.deleteMesFromStore(dTag)
			ch.deleteSig(dTag)
			ticker.Stop()
			// fmt.Println("message ",dTag, " in unCondirmStore has deleted")
			return nil
		case <- ticker.C:
			
			ch.cslock.Lock()
			mes := ch.unConfimedStore[dTag]
			ch.cslock.Unlock()

			ch.timeout.Lock()
			if err := ch.send(mes); err != nil{
				ch.timeout.Unlock()
				return err
			}
			ch.timeout.Unlock()
			fmt.Println("mes has confirmDelivery", dTag, "has republished")
		}
	}
}

func (ch *Channel) Confirm(noWait bool) error {
	if err := ch.SendAndResponseSync(
		&confirmSelect{Nowait: noWait},
		&confirmSelectOk{},
	); err != nil {
		return err
	}

	ch.confirmM.Lock()
	ch.confirming = true
	ch.confirmM.Unlock()

	return nil
}

func (ch *Channel) Ack(tag uint64, multiple bool) error {
	ch.m.Lock()
	defer ch.m.Unlock()

	return ch.send(&basicAck{
		DeliveryTag: tag,
		Multiple:    multiple,
	})
}

func (ch *Channel) Nack(tag uint64, multiple bool, requeue bool) error {
	ch.m.Lock()
	defer ch.m.Unlock()

	return ch.send(&basicNack{
		DeliveryTag: tag,
		Multiple:    multiple,
		Requeue:     requeue,
	})
}

func (ch *Channel) Reject(tag uint64, requeue bool) error {
	ch.m.Lock()
	defer ch.m.Unlock()

	return ch.send(&basicReject{
		DeliveryTag: tag,
		Requeue:     requeue,
	})
}

//为了发布者确认机制，建立一个管道，
func (ch *Channel) NotifyPublish(confirm chan Confirmation) chan Confirmation {
	ch.notifyM.Lock()
	defer ch.notifyM.Unlock()

	if ch.noNotify {
		close(confirm)
	} else {
		ch.confirms.Listen(confirm)
	}

	return confirm

}

func (ch *Channel) Qos(prefetchCount, prefetchSize int, global bool) error {
	return ch.SendAndResponseSync(
		&basicQos{
			PrefetchCount: uint16(prefetchCount),
			PrefetchSize:  uint32(prefetchSize),
			Global:        global,
		},
		&basicQosOk{},
	)
}

func (ch *Channel) GetExpAndPubConfirmTag() (exp, pub uint64){
	exp = ch.confirms.getExpectedTag()
	pub = ch.confirms.getPublishTag()
	return
}











