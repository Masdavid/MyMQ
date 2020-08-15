package storem

import (
	"sync"
	"time"
	"broker/dmqp"
	"broker/database"
	"broker/someTypes"
	"strconv"
	"strings"

	// "fmt"
	// "time"
)

type MsgStore struct {
	rawdb *database.BasicDb
	pLock sync.Mutex
	add map[string] *dmqp.Message
	update map[string] *dmqp.Message
	del map[string] *dmqp.Message

	writeSignal chan struct{}
	closeSignal chan bool

	confirm bool 
	confirmCh chan *dmqp.Message
}

func NewMsgStore(db *database.BasicDb) *MsgStore {
	res := &MsgStore{
		rawdb : db,
		writeSignal: make(chan struct{}, 7),
		closeSignal: make(chan bool),
		confirmCh : make(chan *dmqp.Message, 5000),
		add : map[string] *dmqp.Message{},
		update : map[string] *dmqp.Message{},
		del : map[string] *dmqp.Message{},
	}
	go res.WriteSignalThread()
	return res
}


func (st *MsgStore) Close() error {
	st.closeSignal <- true
	st.pLock.Lock()
	defer st.pLock.Unlock()

	return st.rawdb.Close()
}

//清空数据库中的队列里的信息
func (st *MsgStore) PurgeQue(q string) {
	pre := "msg." + q + "."
	st.rawdb.DeleteKeyWithPre([]byte(pre))
}

//获得长度
func (st *MsgStore) getLen() int {
	return len(st.add) + len(st.del) + len(st.update)
}

func (st *MsgStore) WriteSignalThread() {
	tick := time.NewTicker(5*time.Millisecond)

	go func() {
		for range tick.C {
			st.writeSignal <- struct{}{}
		}
	}()
	
	//用一个定时器给持久化程序发信号
	for range st.writeSignal {
		select {
		case <- st.closeSignal:
			return
		default:
			st.handleMessage()
		}
	}
}

//向数据库添加消息，如果内存中的操作个数大于1000, 则触发db想数据库写入信息 
func (st *MsgStore) Add(mes *dmqp.Message, q string) error {
	if st.getLen() > 1000 {
		st.writeSignal <- struct{}{}
	}
	st.pLock.Lock()
	defer st.pLock.Unlock()
	st.add[getKey(mes.ID, q)] = mes
	return nil
}	

func getKey(id uint64, queue string) string {
	return "msg." + queue + "."  + strconv.FormatInt(int64(id), 10)
}

//更新信息
func (st *MsgStore) Update(mes *dmqp.Message, q string) error {
	st.pLock.Lock()
	defer st.pLock.Unlock()

	st.update[getKey(mes.ID, q)] = mes
	return nil
}

//删除信息
func (st *MsgStore) Del(mes *dmqp.Message, q string) error {
	st.pLock.Lock()
	defer st.pLock.Unlock()

	st.del[getKey(mes.ID, q)] = mes
	return nil
}

//遍历所有信息
func (st *MsgStore) Iterate(f func(q string, mes *dmqp.Message)) {
	st.rawdb.Iterate(
		func(key []byte, val []byte) {
			queName := st.getQueOfKey(string(key))
			mes := &dmqp.Message{}
			mes.Deserialize(val)
			f(queName, mes)
		},
	)
}

//利用key获得属于这个消息所属的队列
func (st *MsgStore)getQueOfKey(key string) string {
	res := strings.Split(key, ".")
	return res[1]
}

//根据que的名字遍历消息
func (st *MsgStore) IterateByQue(q string, up uint64, f func(mes *dmqp.Message)) uint64{
	pre := "msg." + q + "."
	res := st.rawdb.IterateWithPreLimit(
		[]byte(pre),
		up,
		func(key[] byte, val []byte) {
			mes := &dmqp.Message{}
			mes.Deserialize(val)
			f(mes)
		},
	)
	return res
}

func (st *MsgStore) IterateByQueWithStart(q string, id uint64, up uint64, f func(mes *dmqp.Message)) uint64 {
	pre := "msg." + q + "."
	start := getKey(id, q)
	res := st.rawdb.IterateWithPreFrom(
		[]byte(pre),
		[]byte(start),
		up,
		func(key[] byte, val []byte) {
			mes := &dmqp.Message{}
			mes.Deserialize(val)
			f(mes)
		},
	)
	return res
}


func (st *MsgStore) clean() {
	st.add = make(map[string]*dmqp.Message)
	st.update = make(map[string]*dmqp.Message)
	st.del = make(map[string]*dmqp.Message)
}

//
func (st *MsgStore) handleMessage() {
	st.pLock.Lock()
	add := st.add
	del := st.del
	update := st.update
	st.clean()
	st.pLock.Unlock()

	batch := make([]*someTypes.Operation, 0, len(add)+len(update)+len(del))
	for key, mes := range add {
		val, _ := mes.Serialize()
		batch = append(
			batch,
			&someTypes.Operation{
				Key : key,
				Value : val,
				Op: someTypes.OpSet,
			},
		)
	}

	for key , mes := range update {
		val, _ := mes.Serialize()
		batch = append(
			batch,
			&someTypes.Operation{
				Key : key,
				Value : val,
				Op: someTypes.OpSet,
			},
		)
	}

	for key , mes := range del {
		val, _ := mes.Serialize()
		batch = append(
			batch,
			&someTypes.Operation{
				Key : key,
				Value : val,
				Op: someTypes.OpDel,
			},
		)
	}
	if err := st.rawdb.BatchHandle(batch); err != nil {
		panic(err)
	}
	
	//confirm模式下 ，对于消息的confirm在此处处理
	for _, mes := range add {
		if mes.ConfirmProp != nil && st.confirm && mes.ConfirmProp.DeliveryTag > 0 {
		mes.ConfirmProp.ConfirmNumHas++
			st.confirmCh <- mes
		}
	}

}

func (st *MsgStore) GivenConfirmsCh() chan *dmqp.Message {
	st.confirm = true
	return st.confirmCh
}



