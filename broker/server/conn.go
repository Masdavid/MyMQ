package server

import (
	"net"
	"sync"
	"time"
	"bytes"
	"sort"
	"strings"
	"context"
	"sync/atomic"
	"bufio"

	"broker/dmqp"
	log "github.com/sirupsen/logrus"

	// "fmt"
)

const (
	ConnStart = iota
	ConnStartOK
	ConnPara
	ConnParaOK
	ConnCloseOK
	ConnClosed
)

const Threshold = 1414

type Connection struct {
	id uint64
	server *Server
	netConn *net.TCPConn
	logger *log.Entry

	chanLock sync.RWMutex     //read write lock?
	channels map[uint16]*Channel

	outgoing chan *dmqp.Frame
	
	maxChannels uint16
	maxFrameSize uint32
	stLock sync.RWMutex
	status int
	
	Manager *Manager
	closeCh chan bool     //发送conn关闭的信号

	wg        *sync.WaitGroup
	ctx       context.Context
	cancelCtx context.CancelFunc

	heartbeatInterval uint16
	timeoutH  uint16
	heartbeatTimer    *time.Ticker
	newSendTime chan time.Time

}

func NewConnection(server *Server, netConn *net.TCPConn) (connection *Connection) {
	connection = &Connection{
		id : atomic.AddUint64(&server.connSeq, 1),
		server : server,
		netConn : netConn,
		channels: make(map[uint16]*Channel),
		outgoing: make(chan *dmqp.Frame, 1024),
		maxChannels:       server.config.Connection.ChannelsMax,
		maxFrameSize:      server.config.Connection.FrameMaxSize,

		closeCh : make(chan bool ,2),
		wg : &sync.WaitGroup{},
		newSendTime:    make(chan time.Time),
		heartbeatInterval: 10,
	}

	connection.logger = log.WithFields(log.Fields{
		"connectionId": connection.id,
	})

	return
}

func (conn *Connection) close() {
	conn.stLock.Lock()
	if conn.status == ConnClosed {
		conn.stLock.Unlock()
		return
	}
	if conn.heartbeatTimer != nil {
		conn.heartbeatTimer.Stop()
	}
	conn.status = ConnClosed
	conn.stLock.Unlock()

	_ = conn.netConn.Close()

	if conn.cancelCtx != nil {
		conn.cancelCtx()
	}
	conn.wg.Wait()

	channelIds := make([]int, 0)
	conn.chanLock.Lock()
	for chID := range conn.channels {
		channelIds = append(channelIds, int(chID))
	}

	//逆序排序conn管理的所有channel的ID
	sort.Sort(sort.Reverse(sort.IntSlice(channelIds)))
	for _, chID := range channelIds {
		ch := conn.channels[uint16(chID)]
		ch.delete()
		delete(conn.channels, uint16(chID))
	}
	conn.chanLock.Unlock()

	//清空队列，因为队列有独占这个属性
	conn.clearQueues()

	conn.logger.WithFields(log.Fields{
		"manager": conn.server.mgName,
		"from":  conn.netConn.RemoteAddr(),
	}).Info("Connection closed")
	conn.server.removeConn(conn.id)
	
	//异步的管道，为了表示本管道已经关闭。当server关闭该管道时，读取到该管道的值就代表已经关闭
	conn.closeCh <- true
}

//如果队列是排他的，那么conn关闭的时候，队列也会关闭
func (conn *Connection) clearQueues() {
	Manager := conn.GetManager()
	if Manager == nil {
		return
	}
	for _, queue := range Manager.GetAllQueues() {
		if queue.IsExclusive() && queue.ConnID() == conn.id {
			Manager.DeleteQueue(queue.GetName(), false, false)
		}
	}
}

//该函数是由服务器端关闭conn时调用的函数，传入一个waitGroup指针，等待conn的关闭线程完成
func (conn *Connection) srvClose(wg *sync.WaitGroup) {
	defer wg.Done()
	
	//srv要关闭conn时，由0chanel
	ch := conn.getChannel(0)
	if ch == nil {
		return
	}
	ch.SendMethodFrame(&dmqp.ConnectionClose{
		ReplyCode: dmqp.ConnectionForced,
		ReplyText: "Server shutdown",
		ClassID:   0,
		MethodID:  0,
	})
	//给客户端10s时间
	timeOut := time.After(10 * time.Second)
	select {
	case <-timeOut:
		conn.close()
		return
	case <-conn.closeCh:
		return
	}
}

func (conn *Connection) handleConnection() {
	buf := make([]byte, 4) 
	_, err := conn.netConn.Read(buf)     //连接刚建立的时候，读取头部4字节信息

	if err != nil {
		conn.logger.WithError(err).WithFields(log.Fields{
			"read buffer": buf,
		}).Error("Error on read protocol header")
		conn.close()
		return
	}

	if !bytes.Equal(buf, dmqp.DmqpHeader) {
		conn.logger.WithFields(log.Fields{
			"given":     buf,
			"supported": dmqp.DmqpHeader,
		}).Warn("Unsupported protocol")
		_, _ = conn.netConn.Write(dmqp.DmqpHeader)
		conn.close()
		return
	}

	//用于关闭线程使用haneleoutput 和handleInput
	conn.ctx, conn.cancelCtx = context.WithCancel(context.Background())

	//新链接建立时，首先创建id = 0 的信道
	ch := NewChannel(0, conn)
	conn.chanLock.Lock()
	conn.channels[ch.id] = ch
	conn.chanLock.Unlock()
	ch.start()

	conn.wg.Add(1)
	go conn.handleOutputThread()
	conn.wg.Add(1)
	go conn.handleInputThread()
}

//用于处理connection层面的输入和输出
func (conn *Connection) handleOutputThread() {
	defer func() {
		close(conn.newSendTime)
		conn.wg.Done()
		conn.close()
	}()
	var err error
	buf := bufio.NewWriterSize(conn.netConn, 128<<10)
	for {
		select {
		case <-conn.ctx.Done():
			return
		case f := <-conn.outgoing:
			if f == nil {
				return
			}
			if err = dmqp.WriteFrame(buf, f); err != nil {
				conn.logger.WithError(err).Warn("writing frame")
				return
			}

			//closeafter标志，代表？
			if f.CloseAfter {
				if err = buf.Flush(); err != nil && !conn.isClosedError(err) {
					conn.logger.WithError(err).Warn("writing frame")
				}
				return
			}

			//同步标志，作用暂时不明确
			if f.Sync {
				if err = buf.Flush(); err != nil && !conn.isClosedError(err) {
					conn.logger.WithError(err).Warn("writing frame")
					return
				}
			}else {
				if err = conn.flushbuf(buf); err != nil  && !conn.isClosedError(err) {
					conn.logger.WithError(err).Warn("writing frame")
					return
				}
			}

			select {
			case conn.newSendTime <- time.Now():
			default:
			}
		}
	}

}

//
func (conn *Connection) flushbuf(buf *bufio.Writer) (err error){
	if buf.Buffered() >= Threshold {
		if err = buf.Flush(); err != nil {
			return err
		}
	}
	if len(conn.outgoing) == 0 {
		if err = buf.Flush(); err != nil {
			return err
		}
	}
	return
}

func (conn *Connection) handleInputThread() {
	defer func() {
		conn.wg.Done()
		conn.close()
	}()

	buf := bufio.NewReaderSize(conn.netConn, 128 << 10)

	for {
		f, err := dmqp.ReadFrame(buf)
		if err != nil {
			if err.Error() != "EOF" && !conn.isClosedError(err) {
				conn.logger.WithError(err).Warn("reading frame")
			}
			return
		}

		if conn.status < ConnPara && f.ChannelID != 0 {
			conn.logger.WithError(err).Error("Frame not allowed for unTune connection")
			return
		}

		conn.chanLock.RLock()
		ch, ok := conn.channels[f.ChannelID]   //可能conn的channels里没有input信息里给定的信道编号
		conn.chanLock.RUnlock()

		if !ok {
			ch = NewChannel(f.ChannelID, conn) //如果conn没有这个信道，说明是一个client新建的信道
			conn.chanLock.Lock()
			conn.channels[f.ChannelID] = ch
			conn.chanLock.Unlock()
			ch.start()
		}

		if conn.timeoutH > 0 {      //心跳检测，如果超过一定时间读取失败，直接关闭链接conn,此处重置计时器
			if err = conn.netConn.SetReadDeadline(time.Now().Add(time.Duration(conn.timeoutH) * time.Second)); err != nil {
				conn.logger.WithError(err).Warn("reading frame")
				return
			}
		}
		// if f.Type == dmqp.FrameHeartbeat && f.ChannelID != 0 {
		// 	return
		// }
		select {
		case <-conn.ctx.Done():
			close(ch.incoming)
			return
		case ch.incoming <- f:
		}
	}
}

func (conn *Connection) isClosedError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "use of closed network connection")
}

//如果一段时间没有发送消息，broker会给client发布一个心跳帧，
func (conn *Connection) heartBeater() {
	interval := time.Duration(conn.heartbeatInterval) * time.Second
	conn.heartbeatTimer = time.NewTicker(interval)
	heartbeatFrame := &dmqp.Frame{Type: byte(dmqp.FrameHeartbeat), ChannelID: 0, Payload: []byte{}, CloseAfter: false, Sync: true}
	
	var nst time.Time
	var ok bool
	
	go func() {
		for {
			select {
			case nst, ok = <-conn.newSendTime:
				if !ok {
					return
				}
			}
		}
	}()

	for t := range conn.heartbeatTimer.C {
		if t.Sub(nst) >= interval {
			conn.outgoing <- heartbeatFrame
		}
	}
}

func (conn *Connection) getChannel(id uint16) *Channel {
	conn.chanLock.Lock()
	ch := conn.channels[id]
	conn.chanLock.Unlock()
	return ch
}

/*--------------------------------------------------对外的方法---------------------------------------------*/
func (conn *Connection) GetManager() *Manager {
	return conn.Manager
}

func (conn *Connection) GetRemoteAddr() net.Addr {
	return conn.netConn.RemoteAddr()
}

func (conn *Connection) GetChannels() map[uint16]*Channel {
	return conn.channels
}

func (conn *Connection) GetID() uint64 {
	return conn.id
}
