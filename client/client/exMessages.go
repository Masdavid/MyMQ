package client

import (
	// "time"
	"encoding/binary"
	"io"
)

type exchangeDeclare struct {
	reserved1  uint16
	Exchange   string
	Type       string
	Passive    bool
	Durable    bool
	NoWait     bool
}

func (msg *exchangeDeclare) id() (uint16, uint16) {
	return 40, 10
}

func (msg *exchangeDeclare) wait() bool {
	return true && !msg.NoWait
}

func (msg *exchangeDeclare) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.reserved1); err != nil {
		return
	}

	if err = writeShortstr(w, msg.Exchange); err != nil {
		return
	}
	if err = writeShortstr(w, msg.Type); err != nil {
		return
	}

	if msg.Passive {
		bits |= 1 << 0
	}

	if msg.Durable {
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

func (msg *exchangeDeclare) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.reserved1); err != nil {
		return
	}

	if msg.Exchange, err = readShortstr(r); err != nil {
		return
	}
	if msg.Type, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	
	msg.Passive = (bits&(1<<0) > 0)
	msg.Durable = (bits&(1<<1) > 0)
	msg.NoWait = (bits&(1<<2) > 0)

	return
}

type exchangeDeclareOk struct {
}

func (msg *exchangeDeclareOk) id() (uint16, uint16) {
	return 40, 11
}

func (msg *exchangeDeclareOk) wait() bool {
	return true
}

func (msg *exchangeDeclareOk) write(w io.Writer) (err error) {

	return
}

func (msg *exchangeDeclareOk) read(r io.Reader) (err error) {

	return
}

type exchangeDelete struct {
	reserved1 uint16
	Exchange  string
	IfUnused  bool
	NoWait    bool
}

func (msg *exchangeDelete) id() (uint16, uint16) {
	return 40, 20
}

func (msg *exchangeDelete) wait() bool {
	return true && !msg.NoWait
}

func (msg *exchangeDelete) write(w io.Writer) (err error) {
	var bits byte

	if err = binary.Write(w, binary.BigEndian, msg.reserved1); err != nil {
		return
	}

	if err = writeShortstr(w, msg.Exchange); err != nil {
		return
	}

	if msg.IfUnused {
		bits |= 1 << 0
	}

	if msg.NoWait {
		bits |= 1 << 1
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (msg *exchangeDelete) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &msg.reserved1); err != nil {
		return
	}

	if msg.Exchange, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	msg.IfUnused = (bits&(1<<0) > 0)
	msg.NoWait = (bits&(1<<1) > 0)

	return
}

type exchangeDeleteOk struct {
}

func (msg *exchangeDeleteOk) id() (uint16, uint16) {
	return 40, 21
}

func (msg *exchangeDeleteOk) wait() bool {
	return true
}

func (msg *exchangeDeleteOk) write(w io.Writer) (err error) {

	return
}

func (msg *exchangeDeleteOk) read(r io.Reader) (err error) {

	return
}