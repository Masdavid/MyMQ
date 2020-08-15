package client

import (
	"io"
	"encoding/binary"
)

type connectionStart struct {
}

func (msg *connectionStart) id() (uint16, uint16) {
	return 10, 10
}

func (msg *connectionStart) wait() bool {
	return true
}

func (msg *connectionStart) write(w io.Writer) (err error) {
	return
}

func (msg *connectionStart) read(r io.Reader) (err error) {
	return
}


type connectionStartOk struct {
}

func (msg *connectionStartOk) id() (uint16, uint16) {
	return 10, 11
}

func (msg *connectionStartOk) wait() bool {
	return true
}

func (msg *connectionStartOk) write(w io.Writer) (err error) {
	return 
}

func (msg *connectionStartOk) read(r io.Reader) (err error) {
	return
}

type connectionTune struct {
	ChannelMax uint16
	FrameMax   uint32
	Heartbeat  uint16
}

func (msg *connectionTune) id() (uint16, uint16) {
	return 10, 30
}

func (msg *connectionTune) wait() bool {
	return true
}

func (msg *connectionTune) write(w io.Writer) (err error) {

	if err = binary.Write(w, binary.BigEndian, msg.ChannelMax); err != nil {
		return
	}

	if err = binary.Write(w, binary.BigEndian, msg.FrameMax); err != nil {
		return
	}

	if err = binary.Write(w, binary.BigEndian, msg.Heartbeat); err != nil {
		return
	}

	return
}

func (msg *connectionTune) read(r io.Reader) (err error) {

	if err = binary.Read(r, binary.BigEndian, &msg.ChannelMax); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &msg.FrameMax); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &msg.Heartbeat); err != nil {
		return
	}

	return
}

type connectionTuneOk struct {
	ChannelMax uint16
	FrameMax   uint32
	Heartbeat  uint16
}

func (msg *connectionTuneOk) id() (uint16, uint16) {
	return 10, 31
}

func (msg *connectionTuneOk) wait() bool {
	return true
}

func (msg *connectionTuneOk) write(w io.Writer) (err error) {

	if err = binary.Write(w, binary.BigEndian, msg.ChannelMax); err != nil {
		return
	}

	if err = binary.Write(w, binary.BigEndian, msg.FrameMax); err != nil {
		return
	}

	if err = binary.Write(w, binary.BigEndian, msg.Heartbeat); err != nil {
		return
	}

	return
}

func (msg *connectionTuneOk) read(r io.Reader) (err error) {

	if err = binary.Read(r, binary.BigEndian, &msg.ChannelMax); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &msg.FrameMax); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &msg.Heartbeat); err != nil {
		return
	}

	return
}

type connectionClose struct {
	ReplyCode uint16
	ReplyText string
	ClassId   uint16
	MethodId  uint16
}

func (msg *connectionClose) id() (uint16, uint16) {
	return 10, 50
}

func (msg *connectionClose) wait() bool {
	return true
}

func (msg *connectionClose) write(w io.Writer) (err error) {

	if err = binary.Write(w, binary.BigEndian, msg.ReplyCode); err != nil {
		return
	}

	if err = writeShortstr(w, msg.ReplyText); err != nil {
		return
	}

	if err = binary.Write(w, binary.BigEndian, msg.ClassId); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, msg.MethodId); err != nil {
		return
	}

	return
}

func (msg *connectionClose) read(r io.Reader) (err error) {

	if err = binary.Read(r, binary.BigEndian, &msg.ReplyCode); err != nil {
		return
	}

	if msg.ReplyText, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &msg.ClassId); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &msg.MethodId); err != nil {
		return
	}

	return
}

type connectionCloseOk struct {
}

func (msg *connectionCloseOk) id() (uint16, uint16) {
	return 10, 51
}

func (msg *connectionCloseOk) wait() bool {
	return true
}

func (msg *connectionCloseOk) write(w io.Writer) (err error) {

	return
}

func (msg *connectionCloseOk) read(r io.Reader) (err error) {

	return
}

type channelOpen struct {
	reserved1 string
}

func (msg *channelOpen) id() (uint16, uint16) {
	return 20, 10
}

func (msg *channelOpen) wait() bool {
	return true
}

func (msg *channelOpen) write(w io.Writer) (err error) {

	if err = writeShortstr(w, msg.reserved1); err != nil {
		return
	}

	return
}

func (msg *channelOpen) read(r io.Reader) (err error) {

	if msg.reserved1, err = readShortstr(r); err != nil {
		return
	}

	return
}

type channelOpenOk struct {
	reserved1 string
}

func (msg *channelOpenOk) id() (uint16, uint16) {
	return 20, 11
}

func (msg *channelOpenOk) wait() bool {
	return true
}

func (msg *channelOpenOk) write(w io.Writer) (err error) {

	if err = writeLongstr(w, msg.reserved1); err != nil {
		return
	}

	return
}

func (msg *channelOpenOk) read(r io.Reader) (err error) {

	if msg.reserved1, err = readLongstr(r); err != nil {
		return
	}

	return
}

type channelClose struct {
	ReplyCode uint16
	ReplyText string
	ClassId   uint16
	MethodId  uint16
}

func (msg *channelClose) id() (uint16, uint16) {
	return 20, 40
}

func (msg *channelClose) wait() bool {
	return true
}

func (msg *channelClose) write(w io.Writer) (err error) {

	if err = binary.Write(w, binary.BigEndian, msg.ReplyCode); err != nil {
		return
	}

	if err = writeShortstr(w, msg.ReplyText); err != nil {
		return
	}

	if err = binary.Write(w, binary.BigEndian, msg.ClassId); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, msg.MethodId); err != nil {
		return
	}

	return
}

func (msg *channelClose) read(r io.Reader) (err error) {

	if err = binary.Read(r, binary.BigEndian, &msg.ReplyCode); err != nil {
		return
	}

	if msg.ReplyText, err = readShortstr(r); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &msg.ClassId); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &msg.MethodId); err != nil {
		return
	}

	return
}

type channelCloseOk struct {
}

func (msg *channelCloseOk) id() (uint16, uint16) {
	return 20, 41
}

func (msg *channelCloseOk) wait() bool {
	return true
}

func (msg *channelCloseOk) write(w io.Writer) (err error) {

	return
}

func (msg *channelCloseOk) read(r io.Reader) (err error) {

	return
}