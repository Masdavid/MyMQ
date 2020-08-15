package server

import (
	"broker/dmqp"
	"broker/consumer"
)

func (ch *Channel) handelbasicMethod(method dmqp.Method) *dmqp.Error {
	switch method := method.(type) {
	case *dmqp.BasicQos:
		return ch.basicQos(method)
	case *dmqp.BasicPublish:
		return ch.basicPublish(method)
	case *dmqp.BasicConsume:
		return ch.basicConsume(method)
	case *dmqp.BasicAck:
		return ch.basicAck(method)
	case *dmqp.BasicNack:
		return ch.basicNack(method)
	case *dmqp.BasicReject:
		return ch.basicReject(method)
	case *dmqp.BasicCancel:
		return ch.basicCancel(method)
	}

	return dmqp.ErrorStopConn(dmqp.NotImplemented, "unable to route basic method "+method.Name(), method.Cld(), method.MId())
}

//更新ch。qos里的prefetech 和PrefetchSize
func (ch *Channel) basicQos(method *dmqp.BasicQos) (err *dmqp.Error) {
	ch.updateCmrlimit(method.PrefetchCount, method.PrefetchSize, method.Global)
	ch.SendMethodFrame(&dmqp.BasicQosOk{})

	return nil
}

func (ch *Channel) basicAck(method *dmqp.BasicAck) (err *dmqp.Error) {
	return ch.ResAckFromCmr(method)
}

func (ch *Channel) basicNack(method *dmqp.BasicNack) (err *dmqp.Error) {
	return ch.resRejectFromCmr(method.DeliveryTag, method.Multiple, method.Repush, method)
}
func (ch *Channel) basicReject(method *dmqp.BasicReject) (err *dmqp.Error) {
	return ch.resRejectFromCmr(method.DeliveryTag, false, method.Repush, method)
}

//收到client发来的publish命令，代表client有消息要发过来。
func (ch *Channel) basicPublish(method *dmqp.BasicPublish) (err *dmqp.Error) {
	if method.Immediate {
		return dmqp.ErrorStopCh(dmqp.NotImplemented, "Immediate = true", method.Cld(), method.MId())
	}
	//先检查exchange，如果不存在，那么就返回err
	if _, err = ch.searchExchange(method.Exchange, method); err != nil {
		return err
	}
	//存在的话，将这个消息作为currentMessage，之后会使用。如果该ch是有发布者确认的ch，
	//那么会设置它的序号，使用nextConfirmDeliveryTag函数。
	ch.curMessage = dmqp.NewMessage(method)
	if ch.confirm {
		ch.curMessage.ConfirmProp = &dmqp.ConfirmProp{
			ChanID:      ch.id,
			ConnID:      ch.conn.id,
			DeliveryTag : method.ConfirmDeliveryTag,
		}
	}
	return nil
}

//consumer开启一个消费者，在ch上添加一个消费者
func (ch *Channel) basicConsume(method *dmqp.BasicConsume) (err *dmqp.Error) {
	var cmr *consumer.Consumer
	if cmr, err = ch.addCmr(method); err != nil {
		return err
	}

	if !method.NoWait {
		ch.SendMethodFrame(&dmqp.BasicConsumeOk{ConsumerTag: cmr.CmrTag()})
	}
	//同时开启消费者线程
	cmr.Start()

	return nil
}

func (ch *Channel) basicCancel(method *dmqp.BasicCancel) (err *dmqp.Error) {
	if _, ok := ch.cmrsOfChan[method.ConsumerTag]; !ok {
		return dmqp.ErrorStopCh(dmqp.NotFound, "Consumer not found", method.Cld(), method.MId())
	}
	ch.deleteCmr(method.ConsumerTag)
	ch.SendMethodFrame(&dmqp.BasicCancelOk{ConsumerTag: method.ConsumerTag})
	return nil
}