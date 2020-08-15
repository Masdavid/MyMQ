package server

import (
	"time"
	"sync"
	"sort"
	"bytes"
	"sync/atomic"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"broker/dmqp"
	"broker/buf"
	"broker/consumer"
	"broker/exchange"
	"broker/cmrlimit"
	"broker/queue"

	"fmt"
)

//信道，通信的一个基本单位
type Channel struct {
	active bool
	confirm bool
	id uint16
	conn *Connection
	server *Server
	incoming chan *dmqp.Frame
	outgoing chan *dmqp.Frame
	logger *log.Entry

	status int
	curMessage *dmqp.Message

	//信道管理的所有comsumer
	cmrLock sync.RWMutex
	cmrsOfChan    map[string]*consumer.Consumer   //ch管理信道上的consumer
	cmrlOfChannel *cmrlimit.CmrLimit
	cmrl *cmrlimit.CmrLimit

	//消费ack需要的成员
	deliveryTag uint64     //consume消息时给消息编号用的
	ackLock sync.Mutex
	unackMesMap map[uint64]*UnackedMessage
	
	//confirm管理需要的成员
	confirmLock sync.Mutex
	sliceConfirm []*dmqp.ConfirmProp
	csLock sync.Mutex
	confirmSequencer map[uint64]*SequenceMessage
	expectedComfirmSeq uint64
	// duConfirmCh chan bool

	bufObtained *buf.BufferPool
	closeCh chan bool
}

type UnackedMessage struct {
	cTag  string
	msg   *dmqp.Message
	queue string
}

type SequenceMessage struct {
	msg *dmqp.Message
	matchQueues map[string]bool
}

const (
	chNew = iota
	chOpen
	chClosing
	chClosed
	chDelete
)

func NewChannel(id uint16, conn *Connection) *Channel {
	c := &Channel{
		active : true,
		id : id,
		conn : conn,
		server : conn.server,
		incoming : make(chan *dmqp.Frame, 128),
		outgoing:     conn.outgoing,
		status : chNew,

		cmrsOfChan:    make(map[string]*consumer.Consumer),
		cmrlOfChannel:          cmrlimit.NewCmrLimit(0, 0),
		cmrl:  cmrlimit.NewCmrLimit(0, 0),

		unackMesMap : make(map[uint64]*UnackedMessage),
		sliceConfirm : make([]*dmqp.ConfirmProp, 0),
		closeCh : make(chan bool),
		bufObtained:   buf.NewBufferPool(0),
		confirmSequencer : make(map[uint64]*SequenceMessage),
		expectedComfirmSeq : 1,   //初始confirm期待的confirmdeliverytag id是1
		// duConfirmCh : make(chan bool, 1),
	}

	c.logger = log.WithFields(log.Fields{
		"connId": conn.id,
		"chlId":    id,
	})
	return c
}

func (ch *Channel) start() {
	if ch.id == 0 {
		go ch.connStart()
	}
	go ch.handleInput()
}

func (ch *Channel) isActive() bool {
	return ch.active
}

//关闭信道，删除信道上所有consumer，并且重新处理未ack的消息
func (ch *Channel) close() {
	ch.cmrLock.Lock()
	for _, cmr := range ch.cmrsOfChan {
		cmr.Stop()
		delete(ch.cmrsOfChan, cmr.CmrTag())
		ch.logger.WithFields(log.Fields{
			"consumerTag": cmr.CmrTag(),
		}).Info("Consumer stopped")
	}
	ch.cmrLock.Unlock()
	if ch.id > 0 {
		ch.resRejectFromCmr(0, true, true, &dmqp.BasicNack{})
	}
	ch.status = chClosed
	ch.logger.Info("Channel closed")
}


func (ch *Channel) delete() {
	ch.closeCh <- true
	ch.status = chDelete
}

func (ch *Channel) handleInput() {
	buf := bytes.NewReader([]byte{})

	for {
		select {
		case <- ch.closeCh:
			ch.close()
			return
		case f := <-ch.incoming:
			if f == nil{        //如果传进来的frame是nil，则直接返回
				return
			}
			switch f.Type {
			case dmqp.FrameMethod:
				// st1 := time.Now()

				buf.Reset(f.Payload) //方法帧的主体传进buf中
				method, err := dmqp.ReadMethod(buf)
				// fmt.Println("handleinput2")
				if err != nil {
					ch.logger.WithError(err).Error("Error on handling frame")
					ch.sendError(dmqp.ErrorStopConn(dmqp.FrameError, err.Error(), 0, 0))
					return
				}

				ch.logger.Debug("Incoming method <- " + method.Name())

				if err := ch.handleMethod(method); err != nil {
					ch.sendError(err)
				}
				// fmt.Println("method frame cost: ", time.Since(st1))
			case  dmqp.FrameHeader:
				// st2 := time.Now()

				if err := ch.handleContentHeader(f); err != nil {
					ch.sendError(err)
				}
				// fmt.Println("contentheader frame cost: ", time.Since(st2))
			case dmqp.FrameBody:
				// st3 := time.Now()
				if err := ch.handleContentBody(f); err != nil {
					ch.sendError(err)
				}
				// fmt.Println("contentbody frame cost: ", time.Since(st3))
			}


		}
	}
}

//遇到问题的话，直接发送close帧，关闭信道或者连接，从0号信道发送信息
func (ch *Channel) sendError(err *dmqp.Error) {
	ch.logger.Error(err)
	switch err.ErrorType {
	case dmqp.ErrorOnChannel:
		ch.status = chClosing
		ch.SendMethodFrame(&dmqp.ChannelClose{
			ReplyCode: err.ReplyCode,
			ReplyText: err.ReplyText,
			ClassID:   err.ClassID,
			MethodID:  err.MethodID,
		})
	case dmqp.ErrorOnConnection:
		ch := ch.conn.getChannel(0)
		if ch != nil {
			ch.SendMethodFrame(&dmqp.ConnectionClose{
				ReplyCode: err.ReplyCode,
				ReplyText: err.ReplyText,
				ClassID:   err.ClassID,
				MethodID:  err.MethodID,
			})
		}
	}
}

func (ch *Channel) sendOutput(f *dmqp.Frame){
	select {
	case <- ch.conn.ctx.Done():
		if ch.id == 0 {
			close(ch.outgoing)
		}
	case ch.outgoing <- f:
	}
}


func (ch *Channel) handleMethod(m dmqp.Method) *dmqp.Error {

	// fmt.Println("handle method")
	switch m.Cld() {
	case dmqp.ClassConnection:
		return ch.handleConnMethod(m)
	case dmqp.ClassChannel:
		return ch.handleChanMethod(m)
	case dmqp.ClassBasic:
		return ch.handelbasicMethod(m)
	case dmqp.ClassExchange:
		return ch.handleExMethod(m)
	case dmqp.ClassQueue:
		return ch.handleQueueMethod(m)
	case dmqp.ClassConfirm:
		return ch.handleConfirmMethod(m)
	}
	return nil
}

func (ch *Channel) handleContentHeader(headerFrame *dmqp.Frame) *dmqp.Error {
	reader := bytes.NewReader(headerFrame.Payload)
	var err error
	if ch.curMessage == nil {
		return dmqp.ErrorStopConn(dmqp.FrameError, "error because no curMessage", 0, 0)
	}

	if ch.curMessage.Header != nil {
		return dmqp.ErrorStopConn(dmqp.FrameError, "error because curMessage already owned contentheader", 0, 0)
	}

	if ch.curMessage.Header, err = dmqp.ReadContentHeader(reader); err != nil {
		return dmqp.ErrorStopConn(dmqp.FrameError, "error on reading contentheader", 0, 0)
	}

	return nil
}

//处理消息体帧的函数，根据message中携带的
func (ch *Channel) handleContentBody(bodyFrame *dmqp.Frame) *dmqp.Error {
	// st := time.Now()

	if ch.curMessage == nil {
		return dmqp.ErrorStopConn(dmqp.FrameError, "unexpected content body frame", 0, 0)
	}

	if ch.curMessage.Header == nil {
		return dmqp.ErrorStopConn(dmqp.FrameError, "unexpected content body frame - no header yet", 0, 0)
	}

	//向当前消息添加bodyframe帧，并且更新bodysize
	ch.curMessage.Append(bodyFrame)

	//如果不够，代表帧不全，直接return
	if ch.curMessage.BodySize < ch.curMessage.Header.BodySize {
		return nil
	}

	mg := ch.conn.GetManager()
	message := ch.curMessage
	ex := mg.GetExchange(message.Exchange)
	if ex == nil {
		ch.SendFullFrame(
			&dmqp.BasicReturn{ReplyCode: dmqp.NoRoute, 
				ReplyText: "No route", 
				Exchange: message.Exchange, 
				RoutingKey: message.RoutingKey},
			message,
		)
		//此处confirm该消息
		ch.addMesConfirm(message.ConfirmProp, message.IsDurable())

		return nil
	}
	matchedQueues := ex.GetMatchedQueues(message)
	// fmt.Println("len of queues: " ,len(matchedQueues))

	if len(matchedQueues) == 0 {
		if message.Mandatory {
			ch.SendFullFrame(
				&dmqp.BasicReturn{ReplyCode: dmqp.NoRoute, 
					ReplyText: "No route", 
					Exchange: message.Exchange, 
					RoutingKey: message.RoutingKey},
				message,
			)
		}
		fmt.Println("no route")
		ch.addMesConfirm(message.ConfirmProp, message.IsDurable())

		return nil
	}
	//expectConfirms代表消息发往几个队列，这意味这必须所有队列都成功收到消息才能正式confirm该消息
	if ch.confirm{
		message.ConfirmProp.ConfirmNumNeed = len(matchedQueues)
		if !message.IsDurable() {
			ch.handleConfirmSequence(message, matchedQueues) 
		// fmt.Println("cur expectedConfirmSeq is ", ch.expectedComfirmSeq)
			return nil
		}else{
			ch.handleConfirmAsync(message, matchedQueues)
			return nil
		}
	}

	for quename := range matchedQueues {
		qu := ch.conn.GetManager().GetQueue(quename)
		if qu == nil {
			if message.Mandatory {
				ch.SendFullFrame(
					&dmqp.BasicReturn{ReplyCode: dmqp.NoRoute, 
						ReplyText: "No route", 
						Exchange: message.Exchange, 
						RoutingKey: message.RoutingKey},
					message,
				)
			}
			return nil
		}
		qu.Push(message)
	}
	return nil
}

func (ch *Channel) handleConfirmAsync(mes *dmqp.Message, matchQues map[string]bool) *dmqp.Error {
	for quename := range matchQues {
		qu := ch.conn.GetManager().GetQueue(quename)
		if qu == nil {
			if mes.Mandatory {
				ch.SendFullFrame(
					&dmqp.BasicReturn{ReplyCode: dmqp.NoRoute, 
						ReplyText: "No route", 
						Exchange: mes.Exchange, 
						RoutingKey: mes.RoutingKey},
					mes,
				)
			}
			return nil
		}
		qu.Push(mes)
		if !qu.IsDurable() && mes.ConfirmProp.CanConfirm() {
			ch.addMesConfirm(mes.ConfirmProp, false)
		}
	}
	return nil
}

//根据传入的confrimTag与expectedComfirmSeq对比大小，确定该如何处理
func (ch *Channel) handleConfirmSequence(mes *dmqp.Message, matchQues map[string]bool) *dmqp.Error{
	
	cdTag := mes.ConfirmProp.DeliveryTag
	if cdTag < ch.expectedComfirmSeq {
		ch.addMesConfirm(mes.ConfirmProp, mes.IsDurable())
		return nil
	}

	ch.csLock.Lock()
	ch.confirmSequencer[cdTag] = &SequenceMessage{
		msg : mes,
		matchQueues : matchQues,
	}
	ch.csLock.Unlock()

	if cdTag == ch.expectedComfirmSeq{
		// fmt.Println("squence handle")
		//寻找连续的可以confirm的消息，存放与confirmSequencer中的，处理后将其从confirmSequencer中删除
		for {
			m, ok := ch.confirmSequencer[ch.expectedComfirmSeq]
			if !ok {
				return nil
			}
			// st1 := time.Now()
			confirmSuccess := false
			for quename := range m.matchQueues {
				qu := ch.conn.GetManager().GetQueue(quename)
				if qu == nil {
					if m.msg.Mandatory {
						ch.SendFullFrame(
							&dmqp.BasicReturn{ReplyCode: dmqp.NoRoute, 
								ReplyText: "No route", 
								Exchange: m.msg.Exchange, 
								RoutingKey: m.msg.RoutingKey},
							m.msg,
						)
					}
					ch.addMesConfirm(mes.ConfirmProp, m.msg.IsDurable())
					break
				}
				qu.Push(mes)
				
				if ch.confirm && m.msg.ConfirmProp.CanConfirm() && !m.msg.IsDurable() {
					ch.addMesConfirm(m.msg.ConfirmProp, m.msg.IsDurable())
					confirmSuccess = true
				}
			}
			if confirmSuccess {
				delete(ch.confirmSequencer, ch.expectedComfirmSeq)
				atomic.AddUint64(&ch.expectedComfirmSeq, 1)
			}else{
				return nil
			}
		}

	}

	return nil
}

/*------------------------------------消息发布者确认机制的函数---------------------------------------------------*/
//添加消息的confirm ack
func (ch *Channel) addMesConfirm(con *dmqp.ConfirmProp, durable bool) {
	if !ch.confirm {
		return
	}
	ch.confirmLock.Lock()
	defer ch.confirmLock.Unlock()

	if ch.status == chClosed {
		return
	}
	con.Durable = durable
	ch.sliceConfirm = append(ch.sliceConfirm, con)
}

//confirm线程，发布者确认的回复ack消息
func (ch *Channel) sendMesConfirmsThread() {
	tick := time.Tick(1 * time.Millisecond)  //每5ms发布一次confirm信息
	for range tick {
		// st := time.Now()
		if ch.status == chClosed {
			return
		}
		ch.confirmLock.Lock()
		currentConfirms := ch.sliceConfirm
		ch.sliceConfirm = make([]*dmqp.ConfirmProp, 0)
		ch.confirmLock.Unlock()


		for _, confirm := range currentConfirms {
			if confirm.Durable == true {

				ch.SendMethodFrame(&dmqp.BasicAck{
					DeliveryTag: confirm.DeliveryTag,
					Multiple:    false,
				})
			}else{
				ch.SendMethodFrame(&dmqp.BasicAck{
					DeliveryTag: confirm.DeliveryTag,
					Multiple:    true,
				})
			}
		}
	}
}

/*------------------------------------信道对各类命令的响应函数---------------------------------------------------*/
//收到Qos命令，更新消费限制
func (ch *Channel) updateCmrlimit(prefetchCount uint16, prefetchSize uint32, global bool) {
	if global {
		ch.cmrlOfChannel.Update(prefetchCount, prefetchSize)
	} else {
		ch.cmrl.Update(prefetchCount, prefetchSize)
	}
}

//收到添加consumer的命令，传入cmrlOfChannel，并且向对应的queue添加该consumer
func (ch *Channel) addCmr(method *dmqp.BasicConsume) (cmr *consumer.Consumer, err *dmqp.Error) {
	ch.cmrLock.Lock()
	defer ch.cmrLock.Unlock()

	var qu *queue.Queue
	if qu, err = ch.searchEAQue(method.Queue, method); err != nil {
		return nil, err
	}

	//cmrlOfChannel是传地址进入consumer
	var cmrl []*cmrlimit.CmrLimit
	cmrQos := ch.cmrl.Copy()
	cmrl = []*cmrlimit.CmrLimit{ch.cmrlOfChannel, cmrQos}


	cmr = consumer.NewConsumer(method.Queue, method.ConsumerTag, method.NoAck, ch, qu, cmrl)
	if _, ok := ch.cmrsOfChan[cmr.CmrTag()]; ok {
		return nil, dmqp.ErrorStopCh(dmqp.NotAllowed, 
			fmt.Sprintf("Consumer with tag '%s' already exists", cmr.CmrTag()), 
			method.Cld(), 
			method.MId(),
		)
	}

	if quErr := qu.AddCmr(cmr, method.Exclusive); quErr != nil {
		return nil, dmqp.ErrorStopCh(dmqp.AccessRefused, quErr.Error(), method.Cld(), method.MId())
	}
	ch.cmrsOfChan[cmr.CmrTag()] = cmr

	return cmr, nil
}

//删除consumer的命令
func (ch *Channel) deleteCmr(cTag string) {
	ch.cmrLock.Lock()
	defer ch.cmrLock.Unlock()
	if cmr, ok := ch.cmrsOfChan[cTag]; ok {
		cmr.Stop()
		delete(ch.cmrsOfChan, cmr.CmrTag())
	}
}

//响应消费者的ack，主要作用是将ackstore里的untracked消息删除
func (ch *Channel) ResAckFromCmr(method *dmqp.BasicAck) *dmqp.Error {
	ch.ackLock.Lock()
	defer ch.ackLock.Unlock()
	var M *UnackedMessage
	var ok bool

	//MultiPle属性代表批量删除
	if method.Multiple {
		for tag, M := range ch.unackMesMap {
			if method.DeliveryTag == 0 || tag <= method.DeliveryTag {
				ch.deleteUnackMes(M, tag)
			}
		}
		return nil
	}
	if M, ok = ch.unackMesMap[method.DeliveryTag]; !ok {
		return dmqp.ErrorStopCh(dmqp.PreconditionFailed, 
			fmt.Sprintf("Delivery tag [%d] not found", method.DeliveryTag), 
			method.Cld(),
			method.MId(),
		)
	}
	ch.deleteUnackMes(M, method.DeliveryTag)
	return nil
}
//该函数删除存在信道中的unack的消息，而且如果是持久化的消息，要从队列入手删除
func (ch *Channel) deleteUnackMes(unackMes *UnackedMessage, deliveryTag uint64) {
	delete(ch.unackMesMap, deliveryTag)
	q := ch.conn.GetManager().GetQueue(unackMes.queue)
	if q != nil {
		q.DeleteMsgDurable(unackMes.msg)
	}

	ch.decCmrLimit(unackMes)
}

//client拒绝某个消息，那么
func (ch *Channel) resRejectFromCmr(deliveryTag uint64, multiple bool, Repush bool, method dmqp.Method) *dmqp.Error {
	ch.ackLock.Lock()
	defer ch.ackLock.Unlock()

	if multiple {
		deliveryTags := make([]uint64, 0)
		for dTag := range ch.unackMesMap {
			deliveryTags = append(deliveryTags, dTag)
		}
		sort.Slice(
			deliveryTags,
			func(i, j int) bool {
				return deliveryTags[i] > deliveryTags[j]
			},
		)
		for _, tag := range deliveryTags {
			if deliveryTag == 0 || tag <= deliveryTag {
				ch.handleRejMsg(ch.unackMesMap[tag], tag, Repush)
			}
		}

		return nil
	}

	var msg *UnackedMessage
	var ok bool
	if msg, ok = ch.unackMesMap[deliveryTag]; !ok {
		return dmqp.ErrorStopCh(dmqp.PreconditionFailed, 
			fmt.Sprintf("Delivery tag [%d] not found", deliveryTag), 
			method.Cld(), 
			method.MId(),
		)
	}
	ch.handleRejMsg(msg, deliveryTag, Repush)
	return nil
}

func (ch *Channel) handleRejMsg(unackMes *UnackedMessage, deliveryTag uint64, Repush bool) {
	delete(ch.unackMesMap, deliveryTag)
	qu := ch.conn.GetManager().GetQueue(unackMes.queue)

	if qu != nil {
		if Repush {
			qu.Repush(unackMes.msg)
		} else {
			qu.DeleteMsgDurable(unackMes.msg)
		}
	} 
	ch.decCmrLimit(unackMes)
}

func (ch *Channel) decCmrLimit(unackMes *UnackedMessage) {
	ch.cmrLock.RLock()
	if cmr, ok := ch.cmrsOfChan[unackMes.cTag]; ok {
		cmr.Consume()

		for _, CmrLimit := range cmr.GetCmrlimit() {
			CmrLimit.Dec(1, uint32(unackMes.msg.BodySize))
		}
	} else {
		ch.cmrlOfChannel.Dec(1, uint32(unackMes.msg.BodySize))
	}
	ch.cmrLock.RUnlock()
}
/*---------------------------------命令响应函数结束----------------------------------------*/

//搜索已经manager中的交换机
func (ch *Channel) searchExchange(exchangeName string, method dmqp.Method) (ex *exchange.Exchange, err *dmqp.Error) {
	ex = ch.conn.GetManager().GetExchange(exchangeName)
	if ex == nil {
		return nil, dmqp.ErrorStopCh(
			dmqp.NotFound,
			fmt.Sprintf("exchange '%s' not found", exchangeName),
			method.Cld(),
			method.MId(),
		)
	}
	return ex, nil
}

//试图获取已经存在的queue
func (ch *Channel) searchEAQue(quename string, method dmqp.Method) (queue *queue.Queue, err *dmqp.Error) {
	// fmt.Println("getqueue1")
	qu := ch.conn.GetManager().GetQueue(quename)
	// fmt.Println("getqueue2")

	if qu == nil || !qu.IsActive() {
		return nil, dmqp.ErrorStopCh(
			dmqp.NotFound,
			fmt.Sprintf("queue '%s' not found", quename),
			method.Cld(),
			method.MId(),
		)
	}
	return qu, nil
}

//如果queue是独占的，代表已经被某个连接锁定了
func (ch *Channel) queueHasExclued(qu *queue.Queue, method dmqp.Method) *dmqp.Error {
	if qu == nil {
		return nil
	}
	if qu.IsExclusive() && qu.ConnID() != ch.conn.id {
		return dmqp.ErrorStopCh(
			dmqp.QueueExcluded,
			fmt.Sprintf("queue '%s' has beeb excluded by other connection", qu.GetName()),
			method.Cld(),
			method.MId(),
		)
	}
	return nil
}

/*----------------------------------对外的方法--------------------------------------------------------*/
//添加一个unack的消息，包含consumer/queue/自身id的信息
func (ch *Channel) AddUnackedMes(dTag uint64, cTag string, queue string, message *dmqp.Message) {
	ch.ackLock.Lock()
	defer ch.ackLock.Unlock()
	ch.unackMesMap[dTag] = &UnackedMessage{
		cTag:  cTag,
		msg:   message,
		queue: queue,
	}
}

//发布包含方法/内容的一帧
func (ch *Channel) SendFullFrame(method dmqp.Method, message *dmqp.Message) {
	ch.SendMethodFrame(method)

	var buf = ch.bufObtained.Get()
	dmqp.WriteContentHeader(buf, message.Header)

	payload := make([]byte, buf.Len())
	copy(payload, buf.Bytes())
	ch.bufObtained.Put(buf)

	ch.sendOutput(&dmqp.Frame{Type: byte(dmqp.FrameHeader), ChannelID: ch.id, Payload: payload, CloseAfter: false})

	for _, eachbodyframe := range message.Body {
		eachbodyframe.ChannelID = ch.id
		ch.sendOutput(eachbodyframe)
	}
}

func (ch *Channel) SendMethodFrame(method dmqp.Method) {
	var bp = ch.bufObtained.Get()
	if err := dmqp.WriteMethod(bp, method); err != nil {
		logrus.WithError(err).Error("Error")
	}
	closeAfter := method.Cld() == dmqp.ClassConnection && method.MId() == dmqp.MethodConnectionCloseOk
	ch.logger.Debug("Outgoing -> " + method.Name())
	
	//构建方法帧的payload部分
	payload := make([]byte, bp.Len())
	copy(payload, bp.Bytes())
	ch.bufObtained.Put(bp)

	//构建方法帧并且发布出去
	ch.sendOutput(&dmqp.Frame{
		Type : byte(dmqp.FrameMethod),
		ChannelID : ch.id,
		Payload : payload,
		CloseAfter : closeAfter,
		Sync : method.Sync(),
	})
}

//在当前信道上发送的消息的tag（id）
func (ch *Channel) NextDeliveryTag() uint64 {
	return atomic.AddUint64(&ch.deliveryTag, 1)
}

