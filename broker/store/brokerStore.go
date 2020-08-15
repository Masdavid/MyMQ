package store

import (
	"broker/database"
	"broker/binding"
	"broker/queue"
	"broker/exchange"

	"fmt"
	// "time"
)

const (
	exPre = "ex"
	quePre = "que"
	bindPre = "bind"
)

type BrokerStore struct {
	rawdb *database.BasicDb
}

//初始化一个srv的存储类
func NewBrokerStore(db *database.BasicDb) *BrokerStore {
	return &BrokerStore{
		rawdb : db,
	}
}

func (bst *BrokerStore) Close() error {
	return bst.rawdb.Close()
}

//向数据库添加交换机
func (bst *BrokerStore) AddExchange(ex *exchange.Exchange) error {
	key := fmt.Sprintf("%s.%s", exPre, ex.GetName())
	val, err := ex.Serialize()
	if err != nil {
		return nil
	}
	return bst.rawdb.Set(key, val)
}

//从数据库删除交换机
func (bst *BrokerStore) DeleteEx(ex *exchange.Exchange) error {
	key := fmt.Sprintf("%s.%s", exPre, ex.GetName())
	return bst.rawdb.Delete(key)
}

//向数据库添加队列
func (bst *BrokerStore) AddQue(q *queue.Queue) error {
	key := fmt.Sprintf("%s.%s", quePre, q.GetName())
	val, err := q.Serialize()
	if err != nil {
		return nil
	}
	return bst.rawdb.Set(key, val)
}

//从数据库删除队列
func(bst *BrokerStore) DeleteQue(q *queue.Queue) error {
	key := fmt.Sprintf("%s.%s", quePre, q.GetName())
	return bst.rawdb.Delete(key)
}

//向数据库中添加binding
func (bst *BrokerStore) AddBind(b *binding.Binding) error {
	key := fmt.Sprintf("%s.%s", bindPre, b.GetName())
	val, err := b.Serialize()
	if err != nil {
		return nil
	}
	return bst.rawdb.Set(key, val)
}

//从数据库中删除binding
func (bst *BrokerStore) DeleteBind(b *binding.Binding) error {
	key := fmt.Sprintf("%s.%s", bindPre, b.GetName())
	return bst.rawdb.Delete(key)
}

//获取数据库中所有管理的队列
func (bst *BrokerStore) GetQueues() []*queue.Queue {
	var res []*queue.Queue
	callback := func(key []byte, val []byte) {
		q := &queue.Queue{}
		q.Deserialize(val)
		res = append(res, q)
	}
	bst.rawdb.IterateWithPre([]byte(quePre), callback)
	return res
}

//获取数据库中所有管理的交换机
func (bst *BrokerStore) GetExChanges() []*exchange.Exchange {
	var res []*exchange.Exchange
	callback := func(key []byte, val []byte) {
		q := &exchange.Exchange{}
		q.Deserialize(val)
		res = append(res, q)
	}
	bst.rawdb.IterateWithPre([]byte(exPre), callback)
	return res
}

//获取数据库中所有的binding
func (bst *BrokerStore) GetBindings() []*binding.Binding {
	var res []*binding.Binding
	callback := func(key []byte, val []byte) {
		b := &binding.Binding{}
		b.Deserialize(val)
		res = append(res, b)
	}
	bst.rawdb.IterateWithPre([]byte(bindPre), callback)
	return res
}