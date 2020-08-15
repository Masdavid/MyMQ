package someTypes

import (
	"broker/dmqp"
)

//设计这个go接口的原因是因为，只有这样才能避免相互包含的问题
type Channel interface {
	SendFullFrame(method dmqp.Method, message *dmqp.Message)
	SendMethodFrame(method dmqp.Method)
	NextDeliveryTag() uint64
	AddUnackedMes(dTag uint64, cTag string, queue string, message *dmqp.Message)
}

type Consumer interface {
	Consume() bool
	CmrTag() string
	Cancel()
}

const OpSet = 1
const OpDel = 2
type Operation struct {
	Key   string
	Value []byte
	Op    byte
}