package server

import (
	"fmt"
	"broker/dmqp"
	log "github.com/sirupsen/logrus"
	"broker/exchange"
)

func (channel *Channel) handleExMethod(method dmqp.Method) *dmqp.Error {
	switch method := method.(type) {
	case *dmqp.ExchangeDeclare:
		return channel.declareEx(method)
	case *dmqp.ExchangeDelete:
		return channel.deleteEx(method)
	}

	return dmqp.ErrorStopConn(dmqp.NotImplemented, "unable to route queue method "+method.Name(), method.Cld(), method.MId())
}

func (channel *Channel) declareEx(method *dmqp.ExchangeDeclare) *dmqp.Error {
	//如果交换机的类型不是规定的，返回一个错误
	exTypeId, err := exchange.ExIDByTypename(method.Type)
	if err != nil {
		return dmqp.ErrorStopCh(dmqp.NotImplemented, err.Error(), method.Cld(), method.MId())
	}
	//exchange名不能是默认的“”
	if method.Exchange == "" {
		return dmqp.ErrorStopCh(
			dmqp.CommandInvalid,
			"exchange name is required",
			method.Cld(),
			method.MId(),
		)
	}

	ownedEx := channel.conn.GetManager().GetExchange(method.Exchange)
	if method.Passive {
		if method.NoWait {
			return nil
		}

		if ownedEx == nil {
			return dmqp.ErrorStopCh(
				dmqp.NotFound,
				fmt.Sprintf("exchange '%s' not found", method.Exchange),
				method.Cld(),
				method.MId(),
			)
		}
		channel.SendMethodFrame(&dmqp.ExchangeDeclareOk{})
		return nil
	}

	newExchange := exchange.NewExchange(
		method.Exchange,
		exTypeId,
		method.Durable,
		false,
	)
	//禁止申请同名但是属性不同的交换机
	if ownedEx != nil {
		if err := ownedEx.SameProp(newExchange); err != nil {
			return dmqp.ErrorStopCh(
				dmqp.PreconditionFailed,
				err.Error(),
				method.Cld(),
				method.MId(),
			)
		}
		channel.SendMethodFrame(&dmqp.ExchangeDeclareOk{})
		return nil
	}

	channel.conn.GetManager().AddExIntoMan(newExchange)
	if !method.NoWait {
		channel.SendMethodFrame(&dmqp.ExchangeDeclareOk{})
	}

	return nil
}

func (ch *Channel) deleteEx(method *dmqp.ExchangeDelete) *dmqp.Error {
	var err *dmqp.Error
	if _, err = ch.searchExchange(method.Exchange, method); err != nil {
		return err
	}
	if errDel := ch.conn.GetManager().DelExFromMan(method.Exchange, method.IfUnused); err != nil{
		return dmqp.ErrorStopCh(dmqp.PreconditionFailed, errDel.Error(), method.Cld(), method.MId())
	}
	ch.SendMethodFrame(&dmqp.ExchangeDeleteOk{})
	ch.logger.WithFields(log.Fields{
		"ExName" : method.Exchange,
	}).Info("Exchange has deleted")
	return nil
	
}

