package dmqp

import (
	"io"
)

type BasicQos struct {
	PrefetchSize  uint32
	PrefetchCount uint16
	Global        bool
}

func (method *BasicQos) Name() string {
	return "BasicQos"
}

func (method *BasicQos) FrameType() byte {
	return 1
}

func (method *BasicQos) Cld() uint16 {
	return 60
}

func (method *BasicQos) MId() uint16 {
	return 10
}

func (method *BasicQos) Sync() bool {
	return true
}

func (method *BasicQos) Read(reader io.Reader) (err error) {

	method.PrefetchSize, err = ReadLong(reader)
	if err != nil {
		return err
	}

	method.PrefetchCount, err = ReadShort(reader)
	if err != nil {
		return err
	}

	bits, err := ReadByte(reader)
	if err != nil {
		return err
	}

	method.Global = bits&(1<<0) != 0

	return
}

func (method *BasicQos) Write(writer io.Writer) (err error) {

	if err = WriteLong(writer, method.PrefetchSize); err != nil {
		return err
	}

	if err = WriteShort(writer, method.PrefetchCount); err != nil {
		return err
	}

	var bits byte

	if method.Global {
		bits |= 1 << 0
	}

	if err = WriteOctet(writer, bits); err != nil {
		return err
	}

	return
}

type BasicQosOk struct {
}

func (method *BasicQosOk) Name() string {
	return "BasicQosOk"
}

func (method *BasicQosOk) FrameType() byte {
	return 1
}

func (method *BasicQosOk) Cld() uint16 {
	return 60
}

func (method *BasicQosOk) MId() uint16 {
	return 11
}

func (method *BasicQosOk) Sync() bool {
	return true
}

func (method *BasicQosOk) Read(reader io.Reader) (err error) {

	return
}

func (method *BasicQosOk) Write(writer io.Writer) (err error) {

	return
}

//消费者
type BasicConsume struct {
	Reserved1   uint16
	Queue       string
	ConsumerTag string
	NoLocal     bool
	NoAck       bool
	Exclusive   bool
	NoWait      bool
}

func (method *BasicConsume) Name() string {
	return "BasicConsume"
}

func (method *BasicConsume) FrameType() byte {
	return 1
}

func (method *BasicConsume) Cld() uint16 {
	return 60
}

func (method *BasicConsume) MId() uint16 {
	return 20
}

func (method *BasicConsume) Sync() bool {
	return true
}

func (method *BasicConsume) Read(reader io.Reader) (err error) {

	method.Reserved1, err = ReadShort(reader)
	if err != nil {
		return err
	}

	method.Queue, err = ReadShortstr(reader)
	if err != nil {
		return err
	}

	method.ConsumerTag, err = ReadShortstr(reader)
	if err != nil {
		return err
	}

	bits, err := ReadByte(reader)
	if err != nil {
		return err
	}

	method.NoLocal = bits&(1<<0) != 0

	method.NoAck = bits&(1<<1) != 0

	method.Exclusive = bits&(1<<2) != 0

	method.NoWait = bits&(1<<3) != 0

	return
}

func (method *BasicConsume) Write(writer io.Writer) (err error) {

	if err = WriteShort(writer, method.Reserved1); err != nil {
		return err
	}

	if err = WriteShortstr(writer, method.Queue); err != nil {
		return err
	}

	if err = WriteShortstr(writer, method.ConsumerTag); err != nil {
		return err
	}

	var bits byte

	if method.NoLocal {
		bits |= 1 << 0
	}

	if method.NoAck {
		bits |= 1 << 1
	}

	if method.Exclusive {
		bits |= 1 << 2
	}

	if method.NoWait {
		bits |= 1 << 3
	}

	if err = WriteOctet(writer, bits); err != nil {
		return err
	}

	return
}

type BasicConsumeOk struct {
	ConsumerTag string
}

func (method *BasicConsumeOk) Name() string {
	return "BasicConsumeOk"
}

func (method *BasicConsumeOk) FrameType() byte {
	return 1
}

func (method *BasicConsumeOk) Cld() uint16 {
	return 60
}

func (method *BasicConsumeOk) MId() uint16 {
	return 21
}

func (method *BasicConsumeOk) Sync() bool {
	return true
}

func (method *BasicConsumeOk) Read(reader io.Reader) (err error) {

	method.ConsumerTag, err = ReadShortstr(reader)
	if err != nil {
		return err
	}

	return
}

func (method *BasicConsumeOk) Write(writer io.Writer) (err error) {

	if err = WriteShortstr(writer, method.ConsumerTag); err != nil {
		return err
	}

	return
}

type BasicCancel struct {
	ConsumerTag string
	NoWait      bool
}

func (method *BasicCancel) Name() string {
	return "BasicCancel"
}

func (method *BasicCancel) FrameType() byte {
	return 1
}

func (method *BasicCancel) Cld() uint16 {
	return 60
}

func (method *BasicCancel) MId() uint16 {
	return 30
}

func (method *BasicCancel) Sync() bool {
	return true
}

func (method *BasicCancel) Read(reader io.Reader) (err error) {

	method.ConsumerTag, err = ReadShortstr(reader)
	if err != nil {
		return err
	}

	bits, err := ReadByte(reader)
	if err != nil {
		return err
	}

	method.NoWait = bits&(1<<0) != 0

	return
}

func (method *BasicCancel) Write(writer io.Writer) (err error) {

	if err = WriteShortstr(writer, method.ConsumerTag); err != nil {
		return err
	}

	var bits byte

	if method.NoWait {
		bits |= 1 << 0
	}

	if err = WriteOctet(writer, bits); err != nil {
		return err
	}

	return
}

type BasicCancelOk struct {
	ConsumerTag string
}

func (method *BasicCancelOk) Name() string {
	return "BasicCancelOk"
}

func (method *BasicCancelOk) FrameType() byte {
	return 1
}

func (method *BasicCancelOk) Cld() uint16 {
	return 60
}

func (method *BasicCancelOk) MId() uint16 {
	return 31
}

func (method *BasicCancelOk) Sync() bool {
	return true
}

func (method *BasicCancelOk) Read(reader io.Reader) (err error) {

	method.ConsumerTag, err = ReadShortstr(reader)
	if err != nil {
		return err
	}

	return
}

func (method *BasicCancelOk) Write(writer io.Writer) (err error) {

	if err = WriteShortstr(writer, method.ConsumerTag); err != nil {
		return err
	}

	return
}

type BasicPublish struct {
	Reserved1  uint16
	ConfirmDeliveryTag uint64
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool
}

func (method *BasicPublish) Name() string {
	return "BasicPublish"
}

func (method *BasicPublish) FrameType() byte {
	return 1
}

func (method *BasicPublish) Cld() uint16 {
	return 60
}

func (method *BasicPublish) MId() uint16 {
	return 40
}

func (method *BasicPublish) Sync() bool {
	return false
}

func (method *BasicPublish) Read(reader io.Reader) (err error) {

	method.Reserved1, err = ReadShort(reader)
	if err != nil {
		return err
	}

	method.ConfirmDeliveryTag, err = ReadLonglong(reader)
	if err != nil {
		return err
	}

	method.Exchange, err = ReadShortstr(reader)
	if err != nil {
		return err
	}

	method.RoutingKey, err = ReadShortstr(reader)
	if err != nil {
		return err
	}

	bits, err := ReadByte(reader)
	if err != nil {
		return err
	}

	method.Mandatory = bits&(1<<0) != 0
	method.Immediate = bits&(1<<1) != 0
	return
}

// Write method from io reader
func (method *BasicPublish) Write(writer io.Writer) (err error) {

	if err = WriteShort(writer, method.Reserved1); err != nil {
		return err
	}

	if err = WriteLonglong(writer, method.ConfirmDeliveryTag); err != nil {
		return err
	}

	if err = WriteShortstr(writer, method.Exchange); err != nil {
		return err
	}

	if err = WriteShortstr(writer, method.RoutingKey); err != nil {
		return err
	}

	var bits byte

	if method.Mandatory {
		bits |= 1 << 0
	}

	if method.Immediate {
		bits |= 1 << 1
	}

	if err = WriteOctet(writer, bits); err != nil {
		return err
	}

	return
}

type BasicReturn struct {
	ReplyCode  uint16
	ReplyText  string
	Exchange   string
	RoutingKey string
}

func (method *BasicReturn) Name() string {
	return "BasicReturn"
}

func (method *BasicReturn) FrameType() byte {
	return 1
}

func (method *BasicReturn) Cld() uint16 {
	return 60
}

func (method *BasicReturn) MId() uint16 {
	return 50
}

func (method *BasicReturn) Sync() bool {
	return false
}

func (method *BasicReturn) Read(reader io.Reader) (err error) {

	method.ReplyCode, err = ReadShort(reader)
	if err != nil {
		return err
	}

	method.ReplyText, err = ReadShortstr(reader)
	if err != nil {
		return err
	}

	method.Exchange, err = ReadShortstr(reader)
	if err != nil {
		return err
	}

	method.RoutingKey, err = ReadShortstr(reader)
	if err != nil {
		return err
	}

	return
}

func (method *BasicReturn) Write(writer io.Writer) (err error) {

	if err = WriteShort(writer, method.ReplyCode); err != nil {
		return err
	}

	if err = WriteShortstr(writer, method.ReplyText); err != nil {
		return err
	}

	if err = WriteShortstr(writer, method.Exchange); err != nil {
		return err
	}

	if err = WriteShortstr(writer, method.RoutingKey); err != nil {
		return err
	}

	return
}

type BasicDeliver struct {
	ConsumerTag string
	DeliveryTag uint64
	Redelivered bool
	Exchange    string
	RoutingKey  string
}

func (method *BasicDeliver) Name() string {
	return "BasicDeliver"
}

func (method *BasicDeliver) FrameType() byte {
	return 1
}

func (method *BasicDeliver) Cld() uint16 {
	return 60
}

func (method *BasicDeliver) MId() uint16 {
	return 60
}

func (method *BasicDeliver) Sync() bool {
	return false
}

func (method *BasicDeliver) Read(reader io.Reader) (err error) {

	method.ConsumerTag, err = ReadShortstr(reader)
	if err != nil {
		return err
	}

	method.DeliveryTag, err = ReadLonglong(reader)
	if err != nil {
		return err
	}

	bits, err := ReadByte(reader)
	if err != nil {
		return err
	}

	method.Redelivered = bits&(1<<0) != 0

	method.Exchange, err = ReadShortstr(reader)
	if err != nil {
		return err
	}

	method.RoutingKey, err = ReadShortstr(reader)
	if err != nil {
		return err
	}

	return
}

func (method *BasicDeliver) Write(writer io.Writer) (err error) {

	if err = WriteShortstr(writer, method.ConsumerTag); err != nil {
		return err
	}

	if err = WriteLonglong(writer, method.DeliveryTag); err != nil {
		return err
	}

	var bits byte

	if method.Redelivered {
		bits |= 1 << 0
	}

	if err = WriteOctet(writer, bits); err != nil {
		return err
	}

	if err = WriteShortstr(writer, method.Exchange); err != nil {
		return err
	}

	if err = WriteShortstr(writer, method.RoutingKey); err != nil {
		return err
	}

	return
}

type BasicAck struct {
	DeliveryTag uint64
	Multiple    bool
}

func (method *BasicAck) Name() string {
	return "BasicAck"
}

func (method *BasicAck) FrameType() byte {
	return 1
}

func (method *BasicAck) Cld() uint16 {
	return 60
}

func (method *BasicAck) MId() uint16 {
	return 80
}

func (method *BasicAck) Sync() bool {
	return false
}

func (method *BasicAck) Read(reader io.Reader) (err error) {

	method.DeliveryTag, err = ReadLonglong(reader)
	if err != nil {
		return err
	}

	bits, err := ReadByte(reader)
	if err != nil {
		return err
	}

	method.Multiple = bits&(1<<0) != 0

	return
}

func (method *BasicAck) Write(writer io.Writer) (err error) {

	if err = WriteLonglong(writer, method.DeliveryTag); err != nil {
		return err
	}

	var bits byte

	if method.Multiple {
		bits |= 1 << 0
	}

	if err = WriteOctet(writer, bits); err != nil {
		return err
	}

	return
}


type BasicReject struct {
	DeliveryTag uint64
	Repush     bool
}

func (method *BasicReject) Name() string {
	return "BasicReject"
}

func (method *BasicReject) FrameType() byte {
	return 1
}

func (method *BasicReject) Cld() uint16 {
	return 60
}

func (method *BasicReject) MId() uint16 {
	return 90
}

func (method *BasicReject) Sync() bool {
	return false
}

func (method *BasicReject) Read(reader io.Reader) (err error) {

	method.DeliveryTag, err = ReadLonglong(reader)
	if err != nil {
		return err
	}

	bits, err := ReadByte(reader)
	if err != nil {
		return err
	}

	method.Repush = bits&(1<<0) != 0

	return
}

func (method *BasicReject) Write(writer io.Writer) (err error) {

	if err = WriteLonglong(writer, method.DeliveryTag); err != nil {
		return err
	}

	var bits byte

	if method.Repush {
		bits |= 1 << 0
	}

	if err = WriteOctet(writer, bits); err != nil {
		return err
	}

	return
}

type BasicNack struct {
	DeliveryTag uint64
	Multiple    bool
	Repush     bool
}

func (method *BasicNack) Name() string {
	return "BasicNack"
}

func (method *BasicNack) FrameType() byte {
	return 1
}

func (method *BasicNack) Cld() uint16 {
	return 60
}

func (method *BasicNack) MId() uint16 {
	return 120
}

func (method *BasicNack) Sync() bool {
	return false
}

func (method *BasicNack) Read(reader io.Reader) (err error) {

	method.DeliveryTag, err = ReadLonglong(reader)
	if err != nil {
		return err
	}

	bits, err := ReadByte(reader)
	if err != nil {
		return err
	}

	method.Multiple = bits&(1<<0) != 0
	method.Repush = bits&(1<<1) != 0
	return
}

func (method *BasicNack) Write(writer io.Writer) (err error) {

	if err = WriteLonglong(writer, method.DeliveryTag); err != nil {
		return err
	}

	var bits byte

	if method.Multiple {
		bits |= 1 << 0
	}

	if method.Repush {
		bits |= 1 << 1
	}

	if err = WriteOctet(writer, bits); err != nil {
		return err
	}

	return
}
