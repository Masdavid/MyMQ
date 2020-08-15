package server

import (
	"net"
	"sync"
	"os"
	"broker/config"
	"broker/store"
	"broker/storem"
	"broker/database"
	"broker/dmqp"
	log "github.com/sirupsen/logrus"

	// "time"
	"fmt"
)


const (
	Stopped int = iota
	Running
	Stopping
)

type Server struct {
	host string
	port string
	listener *net.TCPListener

	//信道管理
	connSeq uint64
	connLock sync.Mutex
	conns map[uint64]*Connection

	//manager管理
	config *config.Config
	mgLock sync.Mutex
	mgName string
	mg *Manager
	state int

	sst *store.BrokerStore
}

//初始化构造一个server类，conns初始是empty的, 但是mg
func NewServer(host string, port string, config *config.Config) (server *Server) {
	server = &Server {
		host : host,
		port : port,
		conns : make(map[uint64]*Connection),
		config : config,
	}
	return
}

//Start()线程， 调用listen（）后就一直阻塞
func (srv *Server) Start() {
	log.WithFields(log.Fields{
		"pid" : os.Getpid(),
	}).Info("Server Starting")
	

	go srv.listen()
	srv.state = Running
	
	srv.initSST()

	srv.initManager()

	select {}

	// time.Sleep(10*time.Second)
	// srv.Stop()
}

func (srv *Server) Stop() {
	srv.mgLock.Lock()
	defer srv.mgLock.Unlock()
	srv.state = Stopping

	//关闭监听socket
	srv.listener.Close()

	//关闭conn,多线程模式关闭
	var wg sync.WaitGroup
	srv.connLock.Lock()
	for _, conn := range srv.conns {
		wg.Add(1)
		go conn.srvClose(&wg)
	}
	srv.connLock.Unlock()

	//关闭管理者
	if srv.mg != nil {
		srv.mg.Stop()
	}
	//关闭服务器存储系统
	if srv.sst != nil {
		srv.sst.Close()
	}

	wg.Wait()

	log.WithFields(log.Fields{
		"pid" : os.Getpid(),
	}).Info("All connections closed")

}

func (srv *Server) buildDb(name string, isP bool) *database.BasicDb {
	stPath := fmt.Sprintf("%s/%s/%s", srv.config.Db.DefaultPath, srv.config.Db.Engine, name)  
	if !isP {
		stPath += ".trans"
		if err := os.RemoveAll(stPath); err != nil {
			panic(err)
		}
	}
	if err := os.MkdirAll(stPath, 0777); err != nil {
		panic(err)
	}
	log.WithFields(log.Fields{
		"path":   stPath,
		"engine": srv.config.Db.Engine,
	}).Info("Open db storage")
	
	return database.NewBasicDb(stPath)
}

func (srv *Server) initSST() {
	db := srv.buildDb("broker", true)
	srv.sst = store.NewBrokerStore(db)
}

func (srv *Server) initManager() {
	log.WithFields(log.Fields{
		"mg" : srv.config.Manager.DefaultName,
	}).Info("Initialize default manager")

	msgStoreP := storem.NewMsgStore(srv.buildDb("defaultP", true))
	
	srv.mgLock.Lock()
	defer srv.mgLock.Unlock()

	srv.mgName = srv.config.Manager.DefaultName
	srv.mg = NewManager(srv.config.Manager.DefaultName, msgStoreP, srv)

}

//监听是否有新的链接进入
func (srv *Server) listen() {
	address := srv.host + ":" + srv.port
	tcpAddr, err := net.ResolveTCPAddr("tcp4", address)
	srv.listener, err = net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"address": address,
		}).Error("Error on listener start")
		os.Exit(1)
	}

	log.WithFields(log.Fields{
		"address": address,
	}).Info("Server started")
	
	for {
		conn, err := srv.listener.AcceptTCP()
		if err != nil {
			if srv.state != Running {
				return
			}
			srv.stopWithError(err, "accept connection")			
		}

		log.WithFields(log.Fields{
			"from": conn.RemoteAddr().String(),
			"to":   conn.LocalAddr().String(),
		}).Info("accepting connection")

		conn.SetReadBuffer(srv.config.TCP.ReadBufSize)
		conn.SetWriteBuffer(srv.config.TCP.WriteBufSize)
		conn.SetNoDelay(srv.config.TCP.Nodelay)

		srv.handleConn(conn)
	}

}

func (srv *Server) stopWithError(err error, msg string) {
	log.WithError(err).Error(msg)
	srv.Stop()
	os.Exit(1)
}

func (srv *Server) handleConn(conn *net.TCPConn) {
	srv.connLock.Lock()
	defer srv.connLock.Unlock()

	connection := NewConnection(srv, conn)
	srv.conns[connection.id] = connection
	go connection.handleConnection()

}

func (srv *Server) removeConn(connID uint64) {
	srv.connLock.Lock()
	defer srv.connLock.Unlock()

	delete(srv.conns, connID)
}

func (srv *Server) getConfirmCh(cm *dmqp.ConfirmProp) *Channel {
	srv.connLock.Lock()
	defer srv.connLock.Unlock()

	conn := srv.conns[cm.ConnID]
	if conn == nil {
		return nil
	}
	return conn.getChannel(cm.ChanID)
}