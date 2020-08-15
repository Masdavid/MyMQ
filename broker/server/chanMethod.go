package server

import (
	"broker/dmqp"
	// "fmt"
)

func (ch *Channel) handleChanMethod(method dmqp.Method) *dmqp.Error {
	switch m:= method.(type) {
	case *dmqp.ChannelOpen:
		return ch.chanOpen(m)
	case *dmqp.ChannelClose:
		return ch.chanClose(m)
	case *dmqp.ChannelCloseOk:
		return ch.chanCloseOk(m)
	}
	return dmqp.ErrorStopConn(dmqp.NotImplemented, "unable to route channel method "+method.Name(), method.Cld(), method.MId())

}

func (ch *Channel) chanOpen(method *dmqp.ChannelOpen) (err *dmqp.Error) {
	if ch.status == chOpen {
		return dmqp.ErrorStopConn(dmqp.ChannelError, "channel already open", method.Cld(), method.MId())
	}
	ch.SendMethodFrame(&dmqp.ChannelOpenOk{})
	ch.status = chOpen
	ch.logger.Info("channel opened")
	return nil
}

func (ch *Channel) chanClose(method *dmqp.ChannelClose) (err *dmqp.Error){
	ch.status = chClosed
	ch.SendMethodFrame(&dmqp.ChannelCloseOk{})
	ch.close()
	return nil
}

func (ch *Channel) chanCloseOk(method *dmqp.ChannelCloseOk) (err *dmqp.Error) {
	ch.status = chClosed
	return nil
}



