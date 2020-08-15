package dmqp

import (
	"io"
)

type QueueDeclare struct {
	Reserved1  uint16
	Queue      string
	Passive    bool
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	NoWait     bool
}

func (method *QueueDeclare) Name() string {
	return "QueueDeclare"
}

func (method *QueueDeclare) FrameType() byte {
	return 1
}

func (method *QueueDeclare) Cld() uint16 {
	return 50
}

func (method *QueueDeclare) MId() uint16 {
	return 10
}

func (method *QueueDeclare) Sync() bool {
	return true
}

func (method *QueueDeclare) Read(reader io.Reader) (err error) {

	method.Reserved1, err = ReadShort(reader)
	if err != nil {
		return err
	}

	method.Queue, err = ReadShortstr(reader)
	if err != nil {
		return err
	}

	bits, err := ReadByte(reader)
	if err != nil {
		return err
	}

	method.Passive = bits&(1<<0) != 0

	method.Durable = bits&(1<<1) != 0

	method.Exclusive = bits&(1<<2) != 0

	method.AutoDelete = bits&(1<<3) != 0

	method.NoWait = bits&(1<<4) != 0
	return
}

func (method *QueueDeclare) Write(writer io.Writer) (err error) {

	if err = WriteShort(writer, method.Reserved1); err != nil {
		return err
	}

	if err = WriteShortstr(writer, method.Queue); err != nil {
		return err
	}

	var bits byte

	if method.Passive {
		bits |= 1 << 0
	}

	if method.Durable {
		bits |= 1 << 1
	}

	if method.Exclusive {
		bits |= 1 << 2
	}

	if method.AutoDelete {
		bits |= 1 << 3
	}

	if method.NoWait {
		bits |= 1 << 4
	}

	if err = WriteOctet(writer, bits); err != nil {
		return err
	}
	return
}

type QueueDeclareOk struct {
	Queue         string
	MesNum  uint32
	CmrNum uint32
}

func (method *QueueDeclareOk) Name() string {
	return "QueueDeclareOk"
}

func (method *QueueDeclareOk) FrameType() byte {
	return 1
}

func (method *QueueDeclareOk) Cld() uint16 {
	return 50
}

func (method *QueueDeclareOk) MId() uint16 {
	return 11
}

func (method *QueueDeclareOk) Sync() bool {
	return true
}

func (method *QueueDeclareOk) Read(reader io.Reader) (err error) {

	method.Queue, err = ReadShortstr(reader)
	if err != nil {
		return err
	}

	method.MesNum, err = ReadLong(reader)
	if err != nil {
		return err
	}

	method.CmrNum, err = ReadLong(reader)
	if err != nil {
		return err
	}

	return
}

// Write method from io reader
func (method *QueueDeclareOk) Write(writer io.Writer) (err error) {

	if err = WriteShortstr(writer, method.Queue); err != nil {
		return err
	}

	if err = WriteLong(writer, method.MesNum); err != nil {
		return err
	}

	if err = WriteLong(writer, method.CmrNum); err != nil {
		return err
	}

	return
}

type QueueBind struct {
	Reserved1  uint16
	Queue      string
	Exchange   string
	RoutingKey string
	NoWait     bool
}

func (method *QueueBind) Name() string {
	return "QueueBind"
}

func (method *QueueBind) FrameType() byte {
	return 1
}

func (method *QueueBind) Cld() uint16 {
	return 50
}

func (method *QueueBind) MId() uint16 {
	return 20
}

func (method *QueueBind) Sync() bool {
	return true
}

func (method *QueueBind) Read(reader io.Reader) (err error) {

	method.Reserved1, err = ReadShort(reader)
	if err != nil {
		return err
	}

	method.Queue, err = ReadShortstr(reader)
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

	method.NoWait = bits&(1<<0) != 0
	return
}

func (method *QueueBind) Write(writer io.Writer) (err error) {

	if err = WriteShort(writer, method.Reserved1); err != nil {
		return err
	}

	if err = WriteShortstr(writer, method.Queue); err != nil {
		return err
	}

	if err = WriteShortstr(writer, method.Exchange); err != nil {
		return err
	}

	if err = WriteShortstr(writer, method.RoutingKey); err != nil {
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

type QueueBindOk struct {
}

func (method *QueueBindOk) Name() string {
	return "QueueBindOk"
}

func (method *QueueBindOk) FrameType() byte {
	return 1
}

func (method *QueueBindOk) Cld() uint16 {
	return 50
}

func (method *QueueBindOk) MId() uint16 {
	return 21
}

func (method *QueueBindOk) Sync() bool {
	return true
}

func (method *QueueBindOk) Read(reader io.Reader) (err error) {

	return
}

func (method *QueueBindOk) Write(writer io.Writer) (err error) {

	return
}

type QueueUnbind struct {
	Reserved1  uint16
	Queue      string
	Exchange   string
	RoutingKey string
}

func (method *QueueUnbind) Name() string {
	return "QueueUnbind"
}

func (method *QueueUnbind) FrameType() byte {
	return 1
}

func (method *QueueUnbind) Cld() uint16 {
	return 50
}

func (method *QueueUnbind) MId() uint16 {
	return 50
}

func (method *QueueUnbind) Sync() bool {
	return true
}

func (method *QueueUnbind) Read(reader io.Reader) (err error) {

	method.Reserved1, err = ReadShort(reader)
	if err != nil {
		return err
	}

	method.Queue, err = ReadShortstr(reader)
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

func (method *QueueUnbind) Write(writer io.Writer) (err error) {

	if err = WriteShort(writer, method.Reserved1); err != nil {
		return err
	}

	if err = WriteShortstr(writer, method.Queue); err != nil {
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

type QueueUnbindOk struct {
}

func (method *QueueUnbindOk) Name() string {
	return "QueueUnbindOk"
}

func (method *QueueUnbindOk) FrameType() byte {
	return 1
}

func (method *QueueUnbindOk) Cld() uint16 {
	return 50
}

func (method *QueueUnbindOk) MId() uint16 {
	return 51
}

func (method *QueueUnbindOk) Sync() bool {
	return true
}

func (method *QueueUnbindOk) Read(reader io.Reader) (err error) {

	return
}

func (method *QueueUnbindOk) Write(writer io.Writer) (err error) {

	return
}

type QueuePurge struct {
	Reserved1 uint16
	Queue     string
	NoWait    bool
}

func (method *QueuePurge) Name() string {
	return "QueuePurge"
}

func (method *QueuePurge) FrameType() byte {
	return 1
}

func (method *QueuePurge) Cld() uint16 {
	return 50
}

func (method *QueuePurge) MId() uint16 {
	return 30
}

func (method *QueuePurge) Sync() bool {
	return true
}

func (method *QueuePurge) Read(reader io.Reader) (err error) {

	method.Reserved1, err = ReadShort(reader)
	if err != nil {
		return err
	}

	method.Queue, err = ReadShortstr(reader)
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

func (method *QueuePurge) Write(writer io.Writer) (err error) {

	if err = WriteShort(writer, method.Reserved1); err != nil {
		return err
	}

	if err = WriteShortstr(writer, method.Queue); err != nil {
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

type QueuePurgeOk struct {
	MesNum uint32
}

func (method *QueuePurgeOk) Name() string {
	return "QueuePurgeOk"
}

func (method *QueuePurgeOk) FrameType() byte {
	return 1
}

func (method *QueuePurgeOk) Cld() uint16 {
	return 50
}

func (method *QueuePurgeOk) MId() uint16 {
	return 31
}

func (method *QueuePurgeOk) Sync() bool {
	return true
}

func (method *QueuePurgeOk) Read(reader io.Reader) (err error) {

	method.MesNum, err = ReadLong(reader)
	if err != nil {
		return err
	}

	return
}

func (method *QueuePurgeOk) Write(writer io.Writer) (err error) {

	if err = WriteLong(writer, method.MesNum); err != nil {
		return err
	}

	return
}

type QueueDelete struct {
	Reserved1 uint16
	Queue     string
	IfUnused  bool
	IfEmpty   bool
	NoWait    bool
}

func (method *QueueDelete) Name() string {
	return "QueueDelete"
}

func (method *QueueDelete) FrameType() byte {
	return 1
}

func (method *QueueDelete) Cld() uint16 {
	return 50
}

func (method *QueueDelete) MId() uint16 {
	return 40
}

func (method *QueueDelete) Sync() bool {
	return true
}

func (method *QueueDelete) Read(reader io.Reader) (err error) {

	method.Reserved1, err = ReadShort(reader)
	if err != nil {
		return err
	}

	method.Queue, err = ReadShortstr(reader)
	if err != nil {
		return err
	}

	bits, err := ReadByte(reader)
	if err != nil {
		return err
	}

	method.IfUnused = bits&(1<<0) != 0

	method.IfEmpty = bits&(1<<1) != 0

	method.NoWait = bits&(1<<2) != 0

	return
}

func (method *QueueDelete) Write(writer io.Writer) (err error) {

	if err = WriteShort(writer, method.Reserved1); err != nil {
		return err
	}

	if err = WriteShortstr(writer, method.Queue); err != nil {
		return err
	}

	var bits byte

	if method.IfUnused {
		bits |= 1 << 0
	}

	if method.IfEmpty {
		bits |= 1 << 1
	}

	if method.NoWait {
		bits |= 1 << 2
	}

	if err = WriteOctet(writer, bits); err != nil {
		return err
	}

	return
}

type QueueDeleteOk struct {
	MesNum uint32
}

func (method *QueueDeleteOk) Name() string {
	return "QueueDeleteOk"
}

func (method *QueueDeleteOk) FrameType() byte {
	return 1
}

func (method *QueueDeleteOk) Cld() uint16 {
	return 50
}

func (method *QueueDeleteOk) MId() uint16 {
	return 41
}

func (method *QueueDeleteOk) Sync() bool {
	return true
}

func (method *QueueDeleteOk) Read(reader io.Reader) (err error) {

	method.MesNum, err = ReadLong(reader)
	if err != nil {
		return err
	}

	return
}

func (method *QueueDeleteOk) Write(writer io.Writer) (err error) {

	if err = WriteLong(writer, method.MesNum); err != nil {
		return err
	}

	return
}
