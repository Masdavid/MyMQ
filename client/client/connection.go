package client

import (
	"io"
	"net"
	"time"
	"sync"
	"bufio"
	"reflect"
	"sync/atomic"
	"fmt"
	// "time"
	// "strconv"
)

const (
	defaultHeartbeat  = 10*time.Second
	defaultConnTimeOut = 30*time.Second
	defaultChannelMax = (2 << 10) - 1
	maxChannelMax = (2 << 15) - 1
)

type Config struct {
	ChannelMax int
	FrameSize int
	Hp time.Duration
}

type Connection struct {
	conn io.ReadWriteCloser
	channels map[uint16]*Channel

	sendM sync.Mutex
	M sync.Mutex
	writer *writer
	rpc chan message

	chLock sync.Mutex
	config Config

	allocator *allocator // id generator valid after openTune
	closed int32
	closer sync.Once  // shutdown once
	// wg sync.WaitGroup

	noNotify bool

	closes   []chan *Error
	sends chan time.Time
	deadlines chan readDeadliner // heartbeater updates read deadlines
	errors chan *Error

}
type readDeadliner interface {
	SetReadDeadline(time.Time) error
}

func netLink(timeout time.Duration, addr string) (net.Conn, error){
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	return conn, err
}


func Dial(uri URI)(*Connection, error) {
	var err error
	var conn net.Conn

	addr := net.JoinHostPort(uri.Host, uri.Port)
	conn, err = netLink(defaultConnTimeOut, addr)
	if err != nil {
		return nil, err
	}
	config := Config{
		Hp : defaultHeartbeat,
	}
	return Open(conn, config)
}

func Open(conn io.ReadWriteCloser, config Config) (*Connection, error) {
	c := &Connection{
		conn : conn,
		writer : &writer{bufio.NewWriter(conn)},
		channels : make(map[uint16]*Channel),
		rpc : make(chan message),
		sends:   make(chan time.Time),
		deadlines: make(chan readDeadliner, 1),
		errors:    make(chan *Error, 1),
		closed : 0,

	}

	//开启读取套接字的线程
	// c.wg.Add(1)
	go c.read(conn)

	//开始建立应用层的连接
	return c, c.sendProtoHeader(config)
}

//发送协议头，连接建立第一步
func (c *Connection) sendProtoHeader(cfg Config) error {
	// time.Sleep(100*time.Second)
	
	if err := c.send(&protocolHeader{}); err != nil {
		fmt.Println("step1, sending protohead error")
		return err
	}
	// time.Sleep(100*time.Second)

	return c.recvConnStart(cfg)
}

//接受start同步信号，回复startok，此时broker会发送tune信号
func (c *Connection) recvConnStart(cfg Config) error {

	s := &connectionStart{}
	if err := c.call(nil, s); err != nil {
		fmt.Println("step2, recv connectstart_method error")
		return err
	}
	return c.handleTune(cfg)
}

//接受TUne信号，设置心跳等参数，发送TuneOk参数，完成链接的建立
func (c *Connection) handleTune(cfg Config) error {
	ok := &connectionStartOk{}
	tune := &connectionTune{}

	if err := c.call(ok, tune); err != nil {
		fmt.Println("step3, sending connectionStartOk and recv connection tune error")
		return  ErrCredentials
	}
	c.config.ChannelMax = pick(cfg.ChannelMax, int(tune.ChannelMax))
	if c.config.ChannelMax == 0 {
		c.config.ChannelMax = defaultChannelMax
	}
	c.config.ChannelMax = min(c.config.ChannelMax, maxChannelMax)
	c.config.FrameSize = pick(cfg.FrameSize, int(tune.FrameMax))
	c.config.Hp = time.Second * time.Duration(pick(
		int(cfg.Hp/time.Second),
		int(tune.Heartbeat)))

	//此时启动心跳帧的发送与接受
	go c.heartBeater(c.config.Hp, c.NotifyClose(make(chan *Error, 1)))
	
	if err := c.send(&methodFrame{
		ChannelId: 0,
		Method: &connectionTuneOk{
			ChannelMax: uint16(c.config.ChannelMax),
			FrameMax:   uint32(c.config.FrameSize),
			Heartbeat:  uint16(c.config.Hp / time.Second),
		},
	}); err != nil {
		fmt.Println("tuneOk err")
		return err
	}	
	return c.openComplete()
}

//connection连接正式建立
func (c *Connection) openComplete() error {
	if deadliner, ok := c.conn.(interface {
		SetDeadline(time.Time) error
	}); ok {
		_ = deadliner.SetDeadline(time.Time{})
	}
	c.allocator = newAllocator(1, c.config.ChannelMax)
	fmt.Println("connection established")
	// fmt.Println("framesize: ", c.config.FrameSize)
	// fmt.Println("ChannelMax: ", c.config.ChannelMax)
	// fmt.Println("heartbeat interval: ", c.config.Hp)
	return nil
}


//读取并解析来自conn的数据，将解析出来的帧送到正确的信道处理
func (c *Connection) read(r io.Reader){
	//将reader变为一个带具体缓存大小的reader
	buf := bufio.NewReader(r)

	//需要给这种reader添加方法，所以再封装了一层结构体
	frameReader := &reader{buf}
	conn, haveDeadliner := r.(readDeadliner)
	
	for {

		frame, err := frameReader.ReadFrame()
		if err != nil {
			// fmt.Println("reading frame failed", err.Error())
			c.shutdown(&Error{Code : FrameError, Reason : err.Error()})
			return
		}	
		// fmt.Println("reading one frame success")
		c.handleFrame(frame)	
		
		if haveDeadliner {
			c.deadlines <- conn
		}
		// time.Sleep(100*time.Second)

	}
}

//发送一个单帧
func (c *Connection) send(f frame) error {
	if c.IsClosed() {
		return ErrClosed
	}
	c.sendM.Lock()
	err := c.writer.WriteFrame(f)
	c.sendM.Unlock()
	if err != nil {
		fmt.Println("err on send()", err.Error())
		// shutdown could be re-entrant from signaling notify chans
		go c.shutdown(&Error{
			Code:   FrameError,
			Reason: err.Error(),
		})
	} else {
		//每次发送消息后，用sends管道传递心跳
		select {
		case c.sends <- time.Now():
		default:
		}
	}
	return err
}

func (c *Connection) handleFrame(f frame){
	if f.channel() == 0 {
		c.handleCh0(f)
	}else {
		c.handleChN(f)
	}
}

func (c *Connection) handleCh0(f frame){
	// fmt.Println("in channel 0")
	switch mf := f.(type){
	case *methodFrame:
		switch m := mf.Method.(type) {
		case *connectionClose:
			// fmt.Println("case connection close")
			c.send(&methodFrame{
				ChannelId: 0,
				Method:    &connectionCloseOk{},
			})

			c.shutdown(newError(m.ReplyCode, m.ReplyText))
		default:
			//其他相关的帧都通过rpc chan发往call函数
			// fmt.Println("other")
			c.rpc <- m

		}
	case *heartbeatFrame:
	default:
		// fmt.Println("default")
		c.closeWith(ErrUnexpectedFrame)
	}
}

func (c *Connection) handleChN(f frame) {
	c.chLock.Lock()
	ch := c.channels[f.channel()]
	c.chLock.Unlock()

	if ch != nil {
		//将消息送到对应的channel中处理
		ch.recv(ch, f)
	}else {
		c.dispatchClosed(f)
	}
}

//call函数就是为了获取并解析消息类型
func (c *Connection) call(req message, res ...message) error {
	if req != nil {
		if err := c.send(&methodFrame{ChannelId: 0, Method: req}); err != nil {
			return err
		}
	}
	select {
	case err, ok := <-c.errors:
		if !ok {
			return ErrClosed
		}
		return err
	//使用反射来确定传入的消息类型是不是给定的res类型的消息
	case msg := <- c.rpc:
		// fmt.Println("reflect")
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
}

func (c *Connection) dispatchClosed(f frame) {
	if mf, ok := f.(*methodFrame); ok {
		switch mf.Method.(type) {
		case *channelClose:
			c.send(&methodFrame{
				ChannelId: f.channel(),
				Method:    &channelCloseOk{},
			})
		case *channelCloseOk:
		default:
			c.closeWith(ErrClosed)
		}
	}
}

func (c *Connection) Close() error {
	if c.IsClosed() {
		return ErrClosed
	}

	defer c.shutdown(nil)
	return c.call(
		&connectionClose{
			ReplyCode: replySuccess,
			ReplyText: "kthxbai",
		},
		&connectionCloseOk{},
	)
}


func (c *Connection) closeWith(err *Error) error{
	if c.IsClosed() {
		return ErrClosed
	}

	defer c.shutdown(err)
	return c.call(
		&connectionClose{
			ReplyCode: uint16(err.Code),
			ReplyText: err.Reason,
		},
		&connectionCloseOk{},
	)
}

func (c *Connection) IsClosed() bool {
	return (atomic.LoadInt32(&c.closed) == 1)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func pick(client, server int) int {
	if client == 0 || server == 0 {
		return max(client, server)
	}
	return min(client, server)
}

func (c *Connection) shutdown(err *Error) {
	// fmt.Println("closes by shutdown()")
	atomic.StoreInt32(&c.closed, 1)
	c.closer.Do(func() {
		//向closes管道发送err
		if err != nil {
			for _, c := range c.closes {
				c <- err
			}
		}
		//向errors管道发送err，
		if err != nil {
			c.errors <- err
		}
		//关闭closed存管道，其中有
		for _, c := range c.closes {
			close(c)
		}

		//关闭信道
		for _, ch := range c.channels {
			ch.shutdown(err)
		}

		//关闭套接字
		c.conn.Close()
		c.channels = map[uint16]*Channel{}
		c.allocator = newAllocator(1, c.config.ChannelMax)
		c.noNotify = true
		fmt.Println("connection closed")
	})

}

func (c *Connection) heartBeater(interval time.Duration, done chan *Error){
	
	var sendTicks <-chan time.Time
	if interval > 0 {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		sendTicks = ticker.C

	}
	lastST := time.Now()
	for {
		select {
		case at, ok := <- c.sends:
			if ok {
				lastST = at
			}else {
				return
			}
		//心跳帧的驱动信号到来， 如果发现一段时间没有发送信号，就发送一帧心跳帧
		case at := <-sendTicks: 
			if at.Sub(lastST) > interval-time.Second {
				if err := c.send(&heartbeatFrame{}); err != nil {
					// send heartbeats even after close/closeOk so we
					// tick until the connection starts erroring
					return
				}
			}
		//每收到一个读取的消息，read线程会给心跳线程发送一个给conn即套接字重置deadline的信号
		case conn := <-c.deadlines:
			// When reading, reset our side of the deadline, if we've negotiated one with
			// a deadline that covers at least 2 server heartbeats
			if interval > 0 {
				conn.SetReadDeadline(time.Now().Add(3 * interval))
			}
		//结束信号，连接关闭时收到信号
		case <-done:
			return
		}
	}
}

func (c *Connection) NotifyClose(receiver chan *Error) chan *Error {
	c.M.Lock()
	defer c.M.Unlock()

	if c.noNotify {
		close(receiver)
	} else {
		c.closes = append(c.closes, receiver)
	}

	return receiver
}


//新建一个信道的接口函数
func (c *Connection) Channel() (*Channel, error) {
	return c.openChannel()
}

func (c *Connection) openChannel() (*Channel, error) {
	ch, err := c.allocateChannel()
	if err != nil {
		return nil, err
	}

	if err := ch.open(); err != nil {
		c.releaseChannel(ch.id)
		return nil, err
	}
	return ch, nil
}

//用allocate分配channel的id
func (c *Connection) allocateChannel() (*Channel, error) {
	c.M.Lock()
	defer c.M.Unlock()

	if c.IsClosed() {
		return nil, ErrClosed
	}

	id, ok := c.allocator.next()
	if !ok {
		return nil, ErrChannelMax
	}

	ch := newChannel(c, uint16(id))
	c.channels[uint16(id)] = ch

	return ch, nil
}

func (c *Connection) releaseChannel(id uint16) {
	c.M.Lock()
	defer c.M.Unlock()

	delete(c.channels, id)
	c.allocator.release(int(id))
}

func (c *Connection) closeChannel(ch *Channel, e *Error) {
	ch.shutdown(e)
	c.releaseChannel(ch.id)
}
