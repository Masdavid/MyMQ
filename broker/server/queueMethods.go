package server

import (
	"broker/dmqp"
	"broker/binding"
	"broker/exchange"
	"broker/queue"
	log "github.com/sirupsen/logrus"

	// "time"
	"fmt"
)

func (ch *Channel) handleQueueMethod(method dmqp.Method) *dmqp.Error {
	switch method := method.(type) {
	case *dmqp.QueueDeclare:
		return ch.buildNewQue(method)
	case *dmqp.QueueBind:
		return ch.bindQue(method)
	case *dmqp.QueueUnbind:
		return ch.unbindQue(method)
	case *dmqp.QueuePurge:
		return ch.clearQue(method)
	case *dmqp.QueueDelete:
		return ch.deleteQue(method)
	}

	return dmqp.ErrorStopConn(dmqp.NotImplemented, 
		"unable to route queue method "+method.Name(), 
		method.Cld(), 
		method.MId())
}

func (channel *Channel) buildNewQue(method *dmqp.QueueDeclare) *dmqp.Error {
	var curq *queue.Queue
	var none, hasField *dmqp.Error

	if method.Queue == "" {
		return dmqp.ErrorStopCh(
			dmqp.CommandInvalid,
			"queue name is required",
			method.Cld(),
			method.MId(),
		)
	}
	//存在检查，锁定检查
	curq, none = channel.searchEAQue(method.Queue, method)
	hasField = channel.queueHasExclued(curq, method)
	if method.Passive {
		if method.NoWait {
			return nil
		}
		if curq == nil {
			return none
		}
		if hasField != nil {
			return hasField
		}
		channel.SendMethodFrame(&dmqp.QueueDeclareOk{
			Queue:         method.Queue,
			MesNum:  uint32(curq.Length()),
			CmrNum: uint32(curq.ConsumersCount()),
		})

		return nil
	}
	nque := channel.conn.GetManager().NewQueueByMan(
		method.Queue,
		channel.conn.id,
		method.Exclusive,
		method.AutoDelete,
		method.Durable,
	)
	if curq != nil {
		if hasField != nil {
			return hasField
		}
		if err := curq.SameProp(nque); err != nil {
			return dmqp.ErrorStopCh(
				dmqp.PreconditionFailed,
				err.Error(),
				method.Cld(),
				method.MId(),
			)
		}
		channel.SendMethodFrame(&dmqp.QueueDeclareOk{
			Queue:         method.Queue,
			MesNum:  uint32(curq.Length()),
			CmrNum: uint32(curq.ConsumersCount()),
		})
		return nil
	}
	//开启queue线程
	nque.Start()
	err := channel.conn.GetManager().AddQueIntoMan(nque)
	if err != nil {
		return dmqp.ErrorStopCh(
			dmqp.PreconditionFailed,
			err.Error(),
			method.Cld(),
			method.MId(),
		)
	}
	channel.SendMethodFrame(&dmqp.QueueDeclareOk{
		Queue:         method.Queue,
		MesNum:  0,
		CmrNum: 0,
	})
	return nil
}

//绑定交换机与队列, 在channel中搜索队列，
func (channel *Channel) bindQue(method *dmqp.QueueBind) *dmqp.Error {
	var ex *exchange.Exchange
	var curq *queue.Queue
	var err *dmqp.Error

	if ex, err = channel.searchExchange(method.Exchange, method); err != nil {
		return err
	}
	//默认名字不允许绑定，因为在mamager中已经实现了
	if ex.GetName() == exDefaultName {
		return dmqp.ErrorStopCh(
			dmqp.AccessRefused,
			fmt.Sprintf("operation not permitted on the default exchange"),
			method.Cld(),
			method.MId(),
		)
	}
	if curq, err = channel.searchEAQue(method.Queue, method); err != nil {
		return err
	}
	if err = channel.queueHasExclued(curq, method); err != nil {
		return err
	}

	bind, e := binding.NewBinding(method.Queue, method.Exchange, method.RoutingKey)
	if e != nil {
		return dmqp.ErrorStopCh(
			dmqp.PreconditionFailed,
			e.Error(),
			method.Cld(),
			method.MId(),
		)
	}
	ex.AppendBinding(bind)

	//此处持久化binding， 如果交换机是持久化的，而且队列是持久化的
	if ex.IsDurable() && curq.IsDurable() {
		channel.conn.GetManager().BindIntoDb(bind)
	}
	if !method.NoWait {
		channel.SendMethodFrame(&dmqp.QueueBindOk{})
	}
	// channel.logger.Info("bind between", bind.Exchange, bind.Queue, "success")
	return nil
}


func (channel *Channel) unbindQue(method *dmqp.QueueUnbind) *dmqp.Error {
	var ex *exchange.Exchange
	var curq *queue.Queue
	var err *dmqp.Error

	if ex, err = channel.searchExchange(method.Exchange, method); err != nil {
		return err
	}

	if curq, err = channel.searchEAQue(method.Queue, method); err != nil {
		return err
	}

	if err = channel.queueHasExclued(curq, method); err != nil {
		return err
	}

	bind, e := binding.NewBinding(method.Queue, method.Exchange, method.RoutingKey)
	

	if e != nil {
		return dmqp.ErrorStopConn(
			dmqp.PreconditionFailed,
			e.Error(),
			method.Cld(),
			method.MId(),
		)
	}
	// st := time.Now()
	ex.RemoveBinding(bind)
	if ex.IsDurable() && curq.IsDurable(){
		channel.conn.GetManager().DeleteBindInDB(bind)
	}
	channel.SendMethodFrame(&dmqp.QueueUnbindOk{})
	// fmt.Println("Unbind cost:", time.Since(st))

	return nil
}

func (channel *Channel) clearQue(method *dmqp.QueuePurge) *dmqp.Error {
	var curq *queue.Queue
	var err *dmqp.Error

	if curq, err = channel.searchEAQue(method.Queue, method); err != nil {
		return err
	}
	if err = channel.queueHasExclued(curq, method); err != nil {
		return err
	}
	msgCnt := curq.Clear()
	if !method.NoWait {
		channel.SendMethodFrame(&dmqp.QueuePurgeOk{MesNum: uint32(msgCnt)})
	}
	return nil
}

func (channel *Channel) deleteQue(method *dmqp.QueueDelete) *dmqp.Error {
	var curq *queue.Queue
	var err *dmqp.Error
	if curq, err = channel.searchEAQue(method.Queue, method); err != nil {
		return err
	}
	if err = channel.queueHasExclued(curq, method); err != nil {
		return err
	}
	// st := time.Now()
	var len, errDel = channel.conn.GetManager().DeleteQueue(method.Queue, method.IfUnused, method.IfEmpty)
	if errDel != nil {
		return dmqp.ErrorStopCh(dmqp.PreconditionFailed, errDel.Error(), method.Cld(), method.MId())
	}
	// fmt.Println("deleteQueue() cost: ", time.Since(st))

	channel.SendMethodFrame(&dmqp.QueueDeleteOk{MesNum: uint32(len)})
	channel.logger.WithFields(log.Fields{
		"QueueName" : method.Queue,
	}).Info("queue has deleted")

	return nil
}
