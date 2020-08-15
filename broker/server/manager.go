package server

import (
	"sync"
	"errors"
	"broker/config"
	"broker/exchange"
	"broker/queue"
	"broker/binding"
	"broker/store"
	"broker/storem"
	log "github.com/sirupsen/logrus"

	"fmt"
	// "time"

)
const exDefaultName = ""

type Manager struct {
	name string

	exLock sync.RWMutex
	exchanges map[string]*exchange.Exchange

	quLock sync.RWMutex
	queues map[string]*queue.Queue

	srv *Server
	srvConfig *config.Config
	logger *log.Entry
	delQueNow chan string

	serverDbInMan  *store.BrokerStore
	msgDbInMan *storem.MsgStore
}

func NewManager(name string, msgStoreP *storem.MsgStore, srv *Server) *Manager {
	mg := &Manager{
		name : name,
		exchanges: make(map[string]*exchange.Exchange),
		queues: make(map[string]*queue.Queue),
		srvConfig : srv.config,
		srv : srv,
		serverDbInMan : srv.sst,
		delQueNow: make(chan string, 1),
		msgDbInMan : msgStoreP,
	}
	mg.logger = log.WithFields(log.Fields{
		"mg": name,
	})

	mg.buildDefaultExOfMan()
	mg.loadEx()
	mg.loadQue()
	mg.loadBind()

	//载入数据库里的消息信息， 并且启动队列
	mg.logger.Info("Load messages into queues")
	mg.loadMsgIntoQue()
	for _, q := range mg.GetAllQueues() {
		q.Start()
		mg.logger.WithFields(log.Fields{
			"name":   q.GetName(),
			"length": q.Length(),
		}).Info("Messages loaded into queue")
	}

	go mg.listenADqueue()
	go mg.listenConfirmFromDurableMes()
	mg.logger.Info("manager inited success")
	return mg
}

func (mg *Manager) loadMsgIntoQue() {
	var wg sync.WaitGroup
	for qname, q := range mg.queues {
		wg.Add(1)
		go func(name string, que *queue.Queue) {
			que.LoadFromMsgStore()
			wg.Done()
		}(qname, q)
	}
	wg.Wait()
}

func (mg *Manager) loadEx() {
	mg.logger.Info("Load exchanges from db...")
	if mg.serverDbInMan == nil {
		fmt.Println("serverDbInMan is empty")
		return
	}
	exchanges := mg.serverDbInMan.GetExChanges()
	if len(exchanges) == 0 {
		return 
	}
	for _, ex := range exchanges {
		mg.AddExIntoMan(ex)
	}
}

func (mg *Manager) loadQue() {
	mg.logger.Info("Load Queue from db...")
	queues := mg.serverDbInMan.GetQueues()
	if len(queues) == 0 {
		return
	}
	for _, q := range queues {
		//需要从manager这边重构队列
		queFromDb := mg.NewQueueByMan(q.GetName(), 0, false, q.IsAutoDelete(), q.IsDurable())
		mg.AddQueIntoMan(queFromDb)
	}
}

//从数据库中载入binding
func (mg *Manager) loadBind() {
	mg.logger.Info("Load Binding from db...")
	bindings := mg.serverDbInMan.GetBindings()
	if len(bindings) == 0 {
		return
	}
	fmt.Println("bindings num", len(bindings))
	for _, b := range bindings {
		ex := mg.getExchange(b.Exchange)
		if ex != nil{
			ex.AppendBinding(b)
		}
	}
	
}

func (mg *Manager) listenADqueue() {
	for queueName := range mg.delQueNow {
		//time.Sleep(5 * time.Second)
		mg.DeleteQueue(queueName, false, false)
	}
}

func (mg *Manager) getQueue(name string) *queue.Queue {
	res, ok := mg.queues[name]
	if !ok {
		return nil
	}else {
		return res
	}
}

func (mg *Manager) getExchange(name string) *exchange.Exchange {
	return mg.exchanges[name]
}

func (mg *Manager) getDefaultExOfMan() *exchange.Exchange {
	return mg.exchanges[exDefaultName]
}

func (mg *Manager) buildDefaultExOfMan() {
	mg.logger.Info("building default exchanges of manager...")
	systemExchange := exchange.NewExchange(exDefaultName, exchange.TypeSingle, true,  true)
	mg.AddExIntoMan(systemExchange)
}

func (mg *Manager) Stop() error {
	mg.quLock.Lock()
	mg.exLock.Lock()
	defer mg.quLock.Unlock()
	defer mg.exLock.Unlock()
	mg.logger.Info("Stop Manager")
	for _, qu := range mg.queues {
		qu.Stop()
		mg.logger.WithFields(log.Fields{
			"queueName": qu.GetName(),
		}).Info("Queue stopped")
	}

	mg.msgDbInMan.Close()
	close(mg.delQueNow)
	mg.logger.Info("Storage closed")

	return nil
}

func (mg *Manager) listenConfirmFromDurableMes(){
	ch :=  mg.msgDbInMan.GivenConfirmsCh()
	for givenMes := range ch {
		confrimOfChannel := mg.srv.getConfirmCh(givenMes.ConfirmProp)
		if confrimOfChannel == nil{
			continue
		}
		if !givenMes.ConfirmProp.CanConfirm() {
			// confrimOfChannel.duConfirmCh <- false
			continue
		}
		confrimOfChannel.addMesConfirm(givenMes.ConfirmProp, givenMes.IsDurable())
		// confrimOfChannel.duConfirmCh <- true
	}
}


/*---------------------与exchange相关的方法-------------------------*/
//根据名称获取交换机的引用
func (mg *Manager) GetExchange(name string) *exchange.Exchange {
	mg.exLock.RLock()
	defer mg.exLock.RUnlock()
	return  mg.exchanges[name]
}

//将一个新的交换机纳入到manager的管理下
func (mg *Manager) AddExIntoMan(ex *exchange.Exchange) {
	mg.exLock.Lock()
	defer mg.exLock.Unlock()
	typename, _ := exchange.ExtypeByID(ex.ExType())
	mg.logger.WithFields(log.Fields{
		"name": ex.GetName(),
		"type": typename,
	}).Info("Append exchange")
	mg.exchanges[ex.GetName()] = ex
	
	//持久化操作
	if ex.IsDurable() && !ex.IsSystem() {
		mg.serverDbInMan.AddExchange(ex)
	}
}
//删除所管理的交换机，并且删除与其有关的所有binding
func (mg *Manager) DelExFromMan(exName string, cmrFlag bool) error{
	mg.exLock.Lock()
	defer mg.exLock.Unlock()

	ex := mg.getExchange(exName)
	if ex == nil {
		return errors.New("exchange want deleted not found")
	}
	bindsTobeDel := ex.GetBindings()
	if cmrFlag && len(bindsTobeDel) > 0 {
		return errors.New("exchanges has binding with, cannot delete")
	}
	for _, dd := range bindsTobeDel {
		mg.DeleteBindInDB(dd)
	}
	//对于持久化的交换机，从db中删除
	if ex.IsDurable(){
		mg.serverDbInMan.DeleteEx(ex)
	}
	delete(mg.exchanges, exName)
	return nil
}
/*-----------------------------------------------------------------*/

/*-----------------------与queue相关的方法---------------------------*/
//根据队列的名称获取队列的引用
func (mg *Manager) GetQueue(name string) *queue.Queue {
	mg.quLock.RLock()
	defer mg.quLock.RUnlock()
	return mg.getQueue(name)
}
//获取所有manager管理的队列
func (mg *Manager) GetAllQueues() map[string]*queue.Queue {
	mg.quLock.RLock()
	defer mg.quLock.RUnlock()
	return mg.queues
}
//由manager初始化一个队列， 向队列中传入manager管理的no-sql数据库的类引用
func (mg *Manager) NewQueueByMan(name string, connID uint64, exclusive bool, autoDelete bool, durable bool) *queue.Queue {
	return queue.NewQueue(
		name,
		connID,
		exclusive,
		autoDelete,
		durable,
		mg.srvConfig.Queue,
		mg.msgDbInMan,
		mg.delQueNow,
	)
}
//将传入的队列加入到manager的管理之下，包括持久化等操作
func (mg *Manager) AddQueIntoMan(qu *queue.Queue) error {
	mg.quLock.Lock()
	defer mg.quLock.Unlock()
	mg.logger.WithFields(log.Fields{
		"queueName": qu.GetName(),
	}).Info("Append queue")

	mg.queues[qu.GetName()] = qu

	ex := mg.getDefaultExOfMan()
	bind, bindErr := binding.NewBinding(qu.GetName(), exDefaultName, qu.GetName())
	if bindErr != nil {
		return bindErr
	}
	if qu.IsDurable() {
		mg.serverDbInMan.AddQue(qu)
	}
	ex.AppendBinding(bind)
	return nil
}
//删除queue，以及所有与其相关的binding,包括在数据库中的, cmrFlag代表if有消费者不能删，msgFlag代表有消息不能删
func (mg *Manager) DeleteQueue(queueName string, cmrFlag bool, msgFlag bool) (uint64, error) {
	mg.quLock.Lock()
	defer mg.quLock.Unlock()

	qu := mg.getQueue(queueName)
	if qu == nil {
		return 0, errors.New("queue want deleted not found")
	}
	//queue自带的删除函数会删除与queue相连的consumer，而且清空队列中的持久化消息.
	var length, err = qu.Delete(cmrFlag, msgFlag)
	if err != nil {
		return 0, err
	}
	qu.Stop()
	//删除所有与该queue相关的bind
	for _, ex := range mg.exchanges {
	deletingBinds := ex.RemoveQueueBindings(queueName)
		if ex.IsDurable() && qu.IsDurable(){
			for _, dd := range deletingBinds{
				mg.DeleteBindInDB(dd)
			}
		}
	}
	if qu.IsDurable() {
		mg.serverDbInMan.DeleteQue(qu)
	}
	delete(mg.queues, queueName)
	return length, nil
}
/*---------------------------------------------------------------------------*/

//对binding的持久化操作
func (mg *Manager) BindIntoDb(b *binding.Binding){
	mg.serverDbInMan.AddBind(b)
}
//对binding的删除操作
func (mg *Manager) DeleteBindInDB(bind *binding.Binding) {
	mg.serverDbInMan.DeleteBind(bind)
}







