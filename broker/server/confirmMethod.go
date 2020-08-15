package server

import (
	"broker/dmqp"
)

func (channel *Channel) handleConfirmMethod(method dmqp.Method) *dmqp.Error {
	switch method := method.(type) {
	case *dmqp.ConfirmSelect:
		return channel.confirmSelect(method)
	}

	return dmqp.ErrorStopConn(dmqp.NotImplemented, 
		"unable to route channel method "+method.Name(), 
		method.Cld(), 
		method.MId())
}

func (channel *Channel) confirmSelect(method *dmqp.ConfirmSelect) (err *dmqp.Error) {
	channel.confirm = true
	go channel.sendMesConfirmsThread()
	if !method.Nowait {
		channel.SendMethodFrame(&dmqp.ConfirmSelectOk{})
	}
	return nil
}