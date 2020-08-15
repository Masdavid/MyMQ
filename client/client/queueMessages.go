package client

import (
	"encoding/binary"
	"io"
)


type queueDeclare struct {
	reserved1  uint16
	Queue      string
	Passive    bool
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	NoWait     bool
}

func (msg *queueDeclare) id() (uint16, uint16) {
	return 50, 10
}

func (msg *queueDeclare) wait() bool {
	return true && !msg.NoWait
}

func (msg *queueDeclare) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.reserved1); err != nil {
		return
	}

	if err = writeShortstr(w, msg.Queue); err != nil {
		return
	}

	if msg.Passive {
		bits |= 1 << 0
	}

	if msg.Durable {
		bits |= 1 << 1
	}

	if msg.Exclusive {
		bits |= 1 << 2
	}

	if msg.AutoDelete {
		bits |= 1 << 3
	}

	if msg.NoWait {
		bits |= 1 << 4
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}
	return
}

func (msg *queueDeclare) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.reserved1); err != nil {
		return
	}

	if msg.Queue, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	msg.Passive = (bits&(1<<0) > 0)
	msg.Durable = (bits&(1<<1) > 0)
	msg.Exclusive = (bits&(1<<2) > 0)
	msg.AutoDelete = (bits&(1<<3) > 0)
	msg.NoWait = (bits&(1<<4) > 0)
	return
}

type queueDeclareOk struct {
	Queue         string
	MessageCount  uint32
	ConsumerCount uint32
}

func (msg *queueDeclareOk) id() (uint16, uint16) {
	return 50, 11
}

func (msg *queueDeclareOk) wait() bool {
	return true
}

func (msg *queueDeclareOk) write(w io.Writer) (err error) {

	if err = writeShortstr(w, msg.Queue); err != nil {
		return
	}

	if err = binary.Write(w, binary.BigEndian, msg.MessageCount); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, msg.ConsumerCount); err != nil {
		return
	}

	return
}

func (msg *queueDeclareOk) read(r io.Reader) (err error) {

	if msg.Queue, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &msg.MessageCount); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &msg.ConsumerCount); err != nil {
		return
	}

	return
}

type queueBind struct {
	reserved1  uint16
	Queue      string
	Exchange   string
	RoutingKey string
	NoWait     bool
}

func (msg *queueBind) id() (uint16, uint16) {
	return 50, 20
}

func (msg *queueBind) wait() bool {
	return true && !msg.NoWait
}

func (msg *queueBind) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.reserved1); err != nil {
		return
	}

	if err = writeShortstr(w, msg.Queue); err != nil {
		return
	}
	if err = writeShortstr(w, msg.Exchange); err != nil {
		return
	}
	if err = writeShortstr(w, msg.RoutingKey); err != nil {
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

func (msg *queueBind) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.reserved1); err != nil {
		return
	}

	if msg.Queue, err = readShortstr(r); err != nil {
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
	msg.NoWait = (bits&(1<<0) > 0)

	return
}

type queueBindOk struct {
}

func (msg *queueBindOk) id() (uint16, uint16) {
	return 50, 21
}

func (msg *queueBindOk) wait() bool {
	return true
}

func (msg *queueBindOk) write(w io.Writer) (err error) {

	return
}

func (msg *queueBindOk) read(r io.Reader) (err error) {

	return
}

type queueUnbind struct {
	reserved1  uint16
	Queue      string
	Exchange   string
	RoutingKey string
}

func (msg *queueUnbind) id() (uint16, uint16) {
	return 50, 50
}

func (msg *queueUnbind) wait() bool {
	return true
}

func (msg *queueUnbind) write(w io.Writer) (err error) {

	if err = binary.Write(w, binary.BigEndian, msg.reserved1); err != nil {
		return
	}

	if err = writeShortstr(w, msg.Queue); err != nil {
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

func (msg *queueUnbind) read(r io.Reader) (err error) {

	if err = binary.Read(r, binary.BigEndian, &msg.reserved1); err != nil {
		return
	}

	if msg.Queue, err = readShortstr(r); err != nil {
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

type queueUnbindOk struct {
}

func (msg *queueUnbindOk) id() (uint16, uint16) {
	return 50, 51
}

func (msg *queueUnbindOk) wait() bool {
	return true
}

func (msg *queueUnbindOk) write(w io.Writer) (err error) {

	return
}

func (msg *queueUnbindOk) read(r io.Reader) (err error) {

	return
}

type queuePurge struct {
	reserved1 uint16
	Queue     string
	NoWait    bool
}

func (msg *queuePurge) id() (uint16, uint16) {
	return 50, 30
}

func (msg *queuePurge) wait() bool {
	return true && !msg.NoWait
}

func (msg *queuePurge) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.reserved1); err != nil {
		return
	}

	if err = writeShortstr(w, msg.Queue); err != nil {
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

func (msg *queuePurge) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.reserved1); err != nil {
		return
	}

	if msg.Queue, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	msg.NoWait = (bits&(1<<0) > 0)

	return
}

type queuePurgeOk struct {
	MessageCount uint32
}

func (msg *queuePurgeOk) id() (uint16, uint16) {
	return 50, 31
}

func (msg *queuePurgeOk) wait() bool {
	return true
}

func (msg *queuePurgeOk) write(w io.Writer) (err error) {

	if err = binary.Write(w, binary.BigEndian, msg.MessageCount); err != nil {
		return
	}

	return
}

func (msg *queuePurgeOk) read(r io.Reader) (err error) {

	if err = binary.Read(r, binary.BigEndian, &msg.MessageCount); err != nil {
		return
	}

	return
}

type queueDelete struct {
	reserved1 uint16
	Queue     string
	IfUnused  bool
	IfEmpty   bool
	NoWait    bool
}

func (msg *queueDelete) id() (uint16, uint16) {
	return 50, 40
}

func (msg *queueDelete) wait() bool {
	return true && !msg.NoWait
}

func (msg *queueDelete) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.reserved1); err != nil {
		return
	}

	if err = writeShortstr(w, msg.Queue); err != nil {
		return
	}

	if msg.IfUnused {
		bits |= 1 << 0
	}

	if msg.IfEmpty {
		bits |= 1 << 1
	}

	if msg.NoWait {
		bits |= 1 << 2
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (msg *queueDelete) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.reserved1); err != nil {
		return
	}

	if msg.Queue, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	msg.IfUnused = (bits&(1<<0) > 0)
	msg.IfEmpty = (bits&(1<<1) > 0)
	msg.NoWait = (bits&(1<<2) > 0)

	return
}

type queueDeleteOk struct {
	MessageCount uint32
}

func (msg *queueDeleteOk) id() (uint16, uint16) {
	return 50, 41
}

func (msg *queueDeleteOk) wait() bool {
	return true
}

func (msg *queueDeleteOk) write(w io.Writer) (err error) {

	if err = binary.Write(w, binary.BigEndian, msg.MessageCount); err != nil {
		return
	}

	return
}

func (msg *queueDeleteOk) read(r io.Reader) (err error) {

	if err = binary.Read(r, binary.BigEndian, &msg.MessageCount); err != nil {
		return
	}

	return
}

