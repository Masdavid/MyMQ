package client

import (
	"encoding/binary"
	"io"
)

type basicQos struct {
	PrefetchSize  uint32
	PrefetchCount uint16
	Global        bool
}

func (msg *basicQos) id() (uint16, uint16) {
	return 60, 10
}

func (msg *basicQos) wait() bool {
	return true
}

func (msg *basicQos) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.PrefetchSize); err != nil {
		return
	}

	if err = binary.Write(w, binary.BigEndian, msg.PrefetchCount); err != nil {
		return
	}

	if msg.Global {
		bits |= 1 << 0
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (msg *basicQos) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.PrefetchSize); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &msg.PrefetchCount); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	msg.Global = (bits&(1<<0) > 0)

	return
}

type basicQosOk struct {
}

func (msg *basicQosOk) id() (uint16, uint16) {
	return 60, 11
}

func (msg *basicQosOk) wait() bool {
	return true
}

func (msg *basicQosOk) write(w io.Writer) (err error) {
	return
}

func (msg *basicQosOk) read(r io.Reader) (err error) {
	return
}

type basicConsume struct {
	reserved1   uint16
	Queue       string
	ConsumerTag string
	NoLocal     bool
	NoAck       bool
	Exclusive   bool
	NoWait      bool
	// Arguments   Table
}

func (msg *basicConsume) id() (uint16, uint16) {
	return 60, 20
}

func (msg *basicConsume) wait() bool {
	return true && !msg.NoWait
}

func (msg *basicConsume) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.reserved1); err != nil {
		return
	}

	if err = writeShortstr(w, msg.Queue); err != nil {
		return
	}
	if err = writeShortstr(w, msg.ConsumerTag); err != nil {
		return
	}

	if msg.NoLocal {
		bits |= 1 << 0
	}

	if msg.NoAck {
		bits |= 1 << 1
	}

	if msg.Exclusive {
		bits |= 1 << 2
	}

	if msg.NoWait {
		bits |= 1 << 3
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}
	return
}

func (msg *basicConsume) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.reserved1); err != nil {
		return
	}

	if msg.Queue, err = readShortstr(r); err != nil {
		return
	}
	if msg.ConsumerTag, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	msg.NoLocal = (bits&(1<<0) > 0)
	msg.NoAck = (bits&(1<<1) > 0)
	msg.Exclusive = (bits&(1<<2) > 0)
	msg.NoWait = (bits&(1<<3) > 0)
	return
}

type basicConsumeOk struct {
	ConsumerTag string
}

func (msg *basicConsumeOk) id() (uint16, uint16) {
	return 60, 21
}

func (msg *basicConsumeOk) wait() bool {
	return true
}

func (msg *basicConsumeOk) write(w io.Writer) (err error) {

	if err = writeShortstr(w, msg.ConsumerTag); err != nil {
		return
	}

	return
}

func (msg *basicConsumeOk) read(r io.Reader) (err error) {

	if msg.ConsumerTag, err = readShortstr(r); err != nil {
		return
	}

	return
}

type basicCancel struct {
	ConsumerTag string
	NoWait      bool
}

func (msg *basicCancel) id() (uint16, uint16) {
	return 60, 30
}

func (msg *basicCancel) wait() bool {
	return true && !msg.NoWait
}

func (msg *basicCancel) write(w io.Writer) (err error) {
	var bits byte

	if err = writeShortstr(w, msg.ConsumerTag); err != nil {
		return
	}

	if msg.NoWait {
		bits |= 1 << 0
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (msg *basicCancel) read(r io.Reader) (err error) {
	var bits byte

	if msg.ConsumerTag, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	msg.NoWait = (bits&(1<<0) > 0)

	return
}

type basicCancelOk struct {
	ConsumerTag string
}

func (msg *basicCancelOk) id() (uint16, uint16) {
	return 60, 31
}

func (msg *basicCancelOk) wait() bool {
	return true
}

func (msg *basicCancelOk) write(w io.Writer) (err error) {

	if err = writeShortstr(w, msg.ConsumerTag); err != nil {
		return
	}

	return
}

func (msg *basicCancelOk) read(r io.Reader) (err error) {

	if msg.ConsumerTag, err = readShortstr(r); err != nil {
		return
	}

	return
}

type basicPublish struct {
	reserved1  uint16
	ConfirmDeliveryTag uint64
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool

	DeliveryMode byte //内容头部分
	Priority byte

	Body       []byte //内容体部分
}

func (msg *basicPublish) id() (uint16, uint16) {
	return 60, 40
}

func (msg *basicPublish) wait() bool {
	return false
}

func (msg *basicPublish) getContent() (byte, byte, []byte) {
	return msg.DeliveryMode, msg.Priority, msg.Body
}

func (msg *basicPublish) setContent(dmMode byte, prior byte, body []byte) {
	msg.DeliveryMode, msg.Priority, msg.Body = dmMode, prior, body
}

func (msg *basicPublish) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.reserved1); err != nil {
		return
	}
	
	if err = binary.Write(w, binary.BigEndian, msg.ConfirmDeliveryTag); err != nil {
		return
	}

	if err = writeShortstr(w, msg.Exchange); err != nil {
		return
	}
	if err = writeShortstr(w, msg.RoutingKey); err != nil {
		return
	}

	if msg.Mandatory {
		bits |= 1 << 0
	}

	if msg.Immediate {
		bits |= 1 << 1
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (msg *basicPublish) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.reserved1); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &msg.ConfirmDeliveryTag); err != nil {
		return
	}

	if msg.Exchange, err = readShortstr(r); err != nil {
		return
	}
	if msg.RoutingKey, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	msg.Mandatory = (bits&(1<<0) > 0)
	msg.Immediate = (bits&(1<<1) > 0)

	return
}

type basicReturn struct {
	ReplyCode  uint16
	ReplyText  string
	Exchange   string
	RoutingKey string

	DeliveryMode byte //内容头部分
	Priority byte

	Body       []byte //内容体部分
}

func (msg *basicReturn) id() (uint16, uint16) {
	return 60, 50
}

func (msg *basicReturn) wait() bool {
	return false
}

func (msg *basicReturn) getContent() (byte, byte, []byte) {
	return msg.DeliveryMode, msg.Priority, msg.Body
}

func (msg *basicReturn) setContent(deMode byte, p byte, body []byte) {
	// msg.Properties, msg.Body = props, body
	msg.DeliveryMode, msg.Priority, msg.Body = deMode, p, body
}

func (msg *basicReturn) write(w io.Writer) (err error) {

	if err = binary.Write(w, binary.BigEndian, msg.ReplyCode); err != nil {
		return
	}

	if err = writeShortstr(w, msg.ReplyText); err != nil {
		return
	}
	if err = writeShortstr(w, msg.Exchange); err != nil {
		return
	}
	if err = writeShortstr(w, msg.RoutingKey); err != nil {
		return
	}

	return
}

func (msg *basicReturn) read(r io.Reader) (err error) {

	if err = binary.Read(r, binary.BigEndian, &msg.ReplyCode); err != nil {
		return
	}

	if msg.ReplyText, err = readShortstr(r); err != nil {
		return
	}
	if msg.Exchange, err = readShortstr(r); err != nil {
		return
	}
	if msg.RoutingKey, err = readShortstr(r); err != nil {
		return
	}

	return
}

type basicDeliver struct {
	ConsumerTag string
	DeliveryTag uint64
	Redelivered bool
	Exchange    string
	RoutingKey  string

	DeliveryMode byte //内容头部分
	Priority byte

	Body        []byte
}

func (msg *basicDeliver) id() (uint16, uint16) {
	return 60, 60
}

func (msg *basicDeliver) wait() bool {
	return false
}

func (msg *basicDeliver) getContent() (byte, byte, []byte) {
	return msg.DeliveryMode, msg.Priority, msg.Body
}

func (msg *basicDeliver) setContent(deMode byte, p byte, body []byte) {
	msg.DeliveryMode, msg.Priority, msg.Body = deMode, p, body
}

func (msg *basicDeliver) write(w io.Writer) (err error) {
	var bits byte

	if err = writeShortstr(w, msg.ConsumerTag); err != nil {
		return
	}

	if err = binary.Write(w, binary.BigEndian, msg.DeliveryTag); err != nil {
		return
	}

	if msg.Redelivered {
		bits |= 1 << 0
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	if err = writeShortstr(w, msg.Exchange); err != nil {
		return
	}
	if err = writeShortstr(w, msg.RoutingKey); err != nil {
		return
	}

	return
}

func (msg *basicDeliver) read(r io.Reader) (err error) {
	var bits byte

	if msg.ConsumerTag, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &msg.DeliveryTag); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	msg.Redelivered = (bits&(1<<0) > 0)

	if msg.Exchange, err = readShortstr(r); err != nil {
		return
	}
	if msg.RoutingKey, err = readShortstr(r); err != nil {
		return
	}

	return
}

type basicAck struct {
	DeliveryTag uint64
	Multiple    bool
}

func (msg *basicAck) id() (uint16, uint16) {
	return 60, 80
}

func (msg *basicAck) wait() bool {
	return false
}

func (msg *basicAck) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.DeliveryTag); err != nil {
		return
	}

	if msg.Multiple {
		bits |= 1 << 0
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (msg *basicAck) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.DeliveryTag); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	msg.Multiple = (bits&(1<<0) > 0)

	return
}

type basicReject struct {
	DeliveryTag uint64
	Requeue     bool
}

func (msg *basicReject) id() (uint16, uint16) {
	return 60, 90
}

func (msg *basicReject) wait() bool {
	return false
}

func (msg *basicReject) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.DeliveryTag); err != nil {
		return
	}

	if msg.Requeue {
		bits |= 1 << 0
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (msg *basicReject) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.DeliveryTag); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	msg.Requeue = (bits&(1<<0) > 0)

	return
}

type basicNack struct {
	DeliveryTag uint64
	Multiple    bool
	Requeue     bool
}

func (msg *basicNack) id() (uint16, uint16) {
	return 60, 120
}

func (msg *basicNack) wait() bool {
	return false
}

func (msg *basicNack) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.DeliveryTag); err != nil {
		return
	}

	if msg.Multiple {
		bits |= 1 << 0
	}

	if msg.Requeue {
		bits |= 1 << 1
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (msg *basicNack) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.DeliveryTag); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	msg.Multiple = (bits&(1<<0) > 0)
	msg.Requeue = (bits&(1<<1) > 0)

	return
}