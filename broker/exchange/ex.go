package exchange

import (
	"bytes"
	"fmt"
	"sync"

	"broker/dmqp"
	"broker/binding"
)

const (
	TypeSingle = iota + 1
	TypeBoard
)

var exNameMap = map[byte]string{
	TypeSingle:  "direct",
	TypeBoard:  "fanout",
}
var NameExMap = map[string]byte{
	"direct":  TypeSingle,
	"fanout":  TypeBoard,
}

type Exchange struct {
	Name       string
	exType     byte
	durable    bool
	system     bool
	bindLock   sync.Mutex
	bindings   []*binding.Binding
}

func NewExchange(name string, exType byte, durable bool, system bool) *Exchange {
	return &Exchange{
		Name:       name,
		exType:     exType,
		durable:    durable,
		system:     system,
	}
}

func ExtypeByID(id byte) (alias string, err error) {
	if alias, ok := exNameMap[id]; ok {
		return alias, nil
	}
	return "", fmt.Errorf("undefined exchange type '%d'", id)
}

func ExIDByTypename(alias string) (id byte, err error) {
	if id, ok := NameExMap[alias]; ok {
		return id, nil
	}
	return 0, fmt.Errorf("undefined exchange typename '%s'", alias)
}

func (ex *Exchange) GetTypeAlias() string {
	alias, _ := ExtypeByID(ex.exType)

	return alias
}

//向添加binding
func (ex *Exchange) AppendBinding(newBind *binding.Binding) {
	ex.bindLock.Lock()
	defer ex.bindLock.Unlock()

	for _, bind := range ex.bindings {
		if bind.Equal(newBind) {
			return
		}
	}
	ex.bindings = append(ex.bindings, newBind)
}

func (ex *Exchange) RemoveBinding(rmBind *binding.Binding) {
	ex.bindLock.Lock()
	defer ex.bindLock.Unlock()
	for i, bind := range ex.bindings {
		if bind.Equal(rmBind) {
			ex.bindings = append(ex.bindings[:i], ex.bindings[i+1:]...)
			return
		}
	}
}

//根据binding归属的queue进行删除
func (ex *Exchange) RemoveQueueBindings(queueName string) []*binding.Binding {
	var newBindings []*binding.Binding
	var removedBindings []*binding.Binding
	ex.bindLock.Lock()
	defer ex.bindLock.Unlock()
	for _, bind := range ex.bindings {
		if bind.GetQueue() != queueName {
			newBindings = append(newBindings, bind)
		} else {
			removedBindings = append(removedBindings, bind)
		}
	}

	ex.bindings = newBindings
	return removedBindings
}

// 根据routing或者广播方式，找到消息应当送往的queue
func (ex *Exchange) GetMatchedQueues(message *dmqp.Message) (matchedQueues map[string]bool) {
	
	matchedQueues = make(map[string]bool)
	switch ex.exType {
	case TypeSingle:
		for _, bind := range ex.bindings {
			if bind.MatchDirect(message.Exchange, message.RoutingKey) {
				matchedQueues[bind.GetQueue()] = true
				return
			}
		}
	case TypeBoard:
		for _, bind := range ex.bindings {
			if bind.MatchFanout(message.Exchange) {
				matchedQueues[bind.GetQueue()] = true
			}
		}
	}
	return
}

//对比两个交换机是否一致
func (ex *Exchange) SameProp(exB *Exchange) error {
	errTemplate := "inequivalent arg '%s' for exchange '%s': received '%s' but current is '%s'"
	if ex.exType != exB.ExType() {
		aliasA, err := ExtypeByID(ex.exType)
		if err != nil {
			return err
		}
		aliasB, err := ExtypeByID(exB.ExType())
		if err != nil {
			return err
		}
		return fmt.Errorf(errTemplate, "type", ex.Name, aliasB, aliasA)
	}
	if ex.durable != exB.IsDurable() {
		return fmt.Errorf(errTemplate, "durable", ex.Name, exB.IsDurable(), ex.durable)
	}
	return nil
}

func (ex *Exchange) GetBindings() []*binding.Binding {
	ex.bindLock.Lock()
	defer ex.bindLock.Unlock()
	return ex.bindings
}

func (ex *Exchange) IsDurable() bool {
	return ex.durable
}

func (ex *Exchange) IsSystem() bool {
	return ex.system
}


func (ex *Exchange) GetName() string {
	return ex.Name
}

func (ex *Exchange) ExType() byte {
	return ex.exType
}

func (ex *Exchange) Serialize() (data []byte, err error) {
	buf := bytes.NewBuffer([]byte{})
	if err = dmqp.WriteShortstr(buf, ex.Name); err != nil {
		return nil, err
	}
	if err = dmqp.WriteOctet(buf, ex.exType); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (ex *Exchange) Deserialize(data []byte) (err error) {
	buf := bytes.NewReader(data)
	if ex.Name, err = dmqp.ReadShortstr(buf); err != nil {
		return err
	}
	if ex.exType, err = dmqp.ReadByte(buf); err != nil {
		return err
	}
	ex.durable = true
	return
}

