package server

import (
	// "os"
	// "fmt"
	"broker/dmqp"
)

func (ch *Channel) connStart() {
	// ch.logger.Info("dmqp connection starting")
	method := &dmqp.ConnectionStart{}
	ch.SendMethodFrame(method)
	ch.conn.status = ConnStart
}

func (ch *Channel) handleConnMethod(method dmqp.Method) *dmqp.Error {
	switch m := method.(type) {
	case *dmqp.ConnectionStartOk:
		return ch.connStartOk(m)
	case *dmqp.ConnectionTuneOk:
		return ch.paraSync(m)
	case *dmqp.ConnectionClose:
		return ch.connClose(m)
	case *dmqp.ConnectionCloseOk:
		return ch.connCloseOk(m)
	}
	return dmqp.ErrorStopConn(dmqp.NotImplemented, "unable to route connection method", method.Cld(), method.MId())

}

func (ch *Channel) connStartOk(method *dmqp.ConnectionStartOk) *dmqp.Error {
	ch.conn.status = ConnStartOK
	// fmt.Println("recveiving ConnStartOk and sending connectionTune")
	ch.SendMethodFrame(&dmqp.ConnectionTune{
		ChannelMax : ch.conn.maxChannels,
		FrameMax:   ch.conn.maxFrameSize,
		Heartbeat:  ch.conn.heartbeatInterval,
	})
	ch.conn.status = ConnPara
	return nil
}

func (ch *Channel) paraSync(method *dmqp.ConnectionTuneOk) *dmqp.Error {
	ch.conn.status = ConnParaOK
	//client的参数与broker的参数较大的话，直接关闭connection
	if method.ChannelMax > ch.conn.maxChannels || method.FrameMax > ch.conn.maxFrameSize {
		go ch.conn.close()
		return nil
	}

	ch.conn.maxChannels = method.ChannelMax
	ch.conn.maxFrameSize = method.FrameMax

	if method.Heartbeat > 0 {
		if method.Heartbeat < ch.conn.heartbeatInterval {
			ch.conn.heartbeatInterval = method.Heartbeat
		}
		ch.conn.timeoutH = ch.conn.heartbeatInterval * 3
		go ch.conn.heartBeater()
	}

	ch.conn.Manager = ch.server.mg
	ch.logger.Info("dmqp connection open")

	return nil
}

func (ch *Channel) connClose(method *dmqp.ConnectionClose) *dmqp.Error {
	ch.logger.Infof("Connection closed by client, reason - [%d] %s", method.ReplyCode, method.ReplyText)
	// fmt.Println("recveiving ConnClose and sending ConnCloseOk")
	ch.SendMethodFrame(&dmqp.ConnectionCloseOk{})
	return nil
}

func (ch *Channel) connCloseOk(method *dmqp.ConnectionCloseOk) *dmqp.Error {
	// fmt.Println("recveiving ConnCloseOk and close connection", ch.conn.id)
	go ch.conn.close()
	return nil
}