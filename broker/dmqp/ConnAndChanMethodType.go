package dmqp

import (
	// "fmt"
	"io"
	// "time"
)

type Method interface {
	Name() string
	FrameType() byte
	Cld() uint16
	MId() uint16
	Read(reader io.Reader) (err error)
	Write(writer io.Writer) (err error)
	Sync() bool
}

type ConnectionStart struct {
}

func (method *ConnectionStart) Name() string {
	return "ConnectionStart"
}

func (method *ConnectionStart) FrameType() byte {
	return 1
}

func (method *ConnectionStart) Cld() uint16 {
	return 10
}

func (method *ConnectionStart) MId() uint16 {
	return 10
}

func (method *ConnectionStart) Read(reader io.Reader) (err error) {
	return nil
}
func (method *ConnectionStart) Write(writer io.Writer) (err error) {
	return nil
}

func (method *ConnectionStart) Sync() bool {
	return true
}


type ConnectionStartOk struct {
}

func (method *ConnectionStartOk) Name() string {
	return "ConnectionStartOk"
}

func (method *ConnectionStartOk) FrameType() byte {
	return 1
}

func (method *ConnectionStartOk) Cld() uint16 {
	return 10
}

func (method *ConnectionStartOk) MId() uint16 {
	return 11
}

func (method *ConnectionStartOk) Sync() bool {
	return true
}

func (method *ConnectionStartOk) Read(reader io.Reader) (err error){
	return
}

func (method *ConnectionStartOk) Write(writer io.Writer) (err error){
	return
}



type ConnectionTune struct {
	ChannelMax uint16
	FrameMax   uint32
	Heartbeat  uint16
}

func (method *ConnectionTune) Name() string {
	return "ConnectionTune"
}

func (method *ConnectionTune) FrameType() byte {
	return 1
}

func (method *ConnectionTune) Cld() uint16 {
	return 10
}

func (method *ConnectionTune) MId() uint16 {
	return 30
}

func (method *ConnectionTune) Sync() bool {
	return true
}

func (method *ConnectionTune) Read(reader io.Reader) (err error) {

	method.ChannelMax, err = ReadShort(reader)
	if err != nil {
		return err
	}

	method.FrameMax, err = ReadLong(reader)
	if err != nil {
		return err
	}

	method.Heartbeat, err = ReadShort(reader)
	if err != nil {
		return err
	}

	return
}

// Write method from io reader
func (method *ConnectionTune) Write(writer io.Writer) (err error) {

	if err = WriteShort(writer, method.ChannelMax); err != nil {
		return err
	}

	if err = WriteLong(writer, method.FrameMax); err != nil {
		return err
	}

	if err = WriteShort(writer, method.Heartbeat); err != nil {
		return err
	}

	return
}

type ConnectionTuneOk struct {
	ChannelMax uint16
	FrameMax   uint32
	Heartbeat  uint16
}

func (method *ConnectionTuneOk) Name() string {
	return "ConnectionTuneOk"
}

func (method *ConnectionTuneOk) FrameType() byte {
	return 1
}

func (method *ConnectionTuneOk) Cld() uint16 {
	return 10
}

func (method *ConnectionTuneOk) MId() uint16 {
	return 31
}

func (method *ConnectionTuneOk) Sync() bool {
	return true
}

func (method *ConnectionTuneOk) Read(reader io.Reader) (err error) {

	method.ChannelMax, err = ReadShort(reader)
	if err != nil {
		return err
	}

	method.FrameMax, err = ReadLong(reader)
	if err != nil {
		return err
	}

	method.Heartbeat, err = ReadShort(reader)
	if err != nil {
		return err
	}

	return
}

func (method *ConnectionTuneOk) Write(writer io.Writer) (err error) {

	if err = WriteShort(writer, method.ChannelMax); err != nil {
		return err
	}

	if err = WriteLong(writer, method.FrameMax); err != nil {
		return err
	}

	if err = WriteShort(writer, method.Heartbeat); err != nil {
		return err
	}

	return
}

type ConnectionClose struct {
	ReplyCode uint16
	ReplyText string
	ClassID   uint16
	MethodID  uint16
}

func (method *ConnectionClose) Name() string {
	return "ConnectionClose"
}

func (method *ConnectionClose) FrameType() byte {
	return 1
}

func (method *ConnectionClose) Cld() uint16 {
	return 10
}

func (method *ConnectionClose) MId() uint16 {
	return 50
}

func (method *ConnectionClose) Sync() bool {
	return true
}

func (method *ConnectionClose) Read(reader io.Reader) (err error) {

	method.ReplyCode, err = ReadShort(reader)
	if err != nil {
		return err
	}

	method.ReplyText, err = ReadShortstr(reader)
	if err != nil {
		return err
	}

	method.ClassID, err = ReadShort(reader)
	if err != nil {
		return err
	}

	method.MethodID, err = ReadShort(reader)
	if err != nil {
		return err
	}

	return
}

func (method *ConnectionClose) Write(writer io.Writer) (err error) {

	if err = WriteShort(writer, method.ReplyCode); err != nil {
		return err
	}

	if err = WriteShortstr(writer, method.ReplyText); err != nil {
		return err
	}

	if err = WriteShort(writer, method.ClassID); err != nil {
		return err
	}

	if err = WriteShort(writer, method.MethodID); err != nil {
		return err
	}

	return
}

type ConnectionCloseOk struct {
}

func (method *ConnectionCloseOk) Name() string {
	return "ConnectionCloseOk"
}

func (method *ConnectionCloseOk) FrameType() byte {
	return 1
}

func (method *ConnectionCloseOk) Cld() uint16 {
	return 10
}

func (method *ConnectionCloseOk) MId() uint16 {
	return 51
}

func (method *ConnectionCloseOk) Sync() bool {
	return true
}

func (method *ConnectionCloseOk) Read(reader io.Reader) (err error) {

	return
}

func (method *ConnectionCloseOk) Write(writer io.Writer) (err error) {

	return
}


type ChannelOpen struct {
	Reserved1 string
}

func (method *ChannelOpen) Name() string {
	return "ChannelOpen"
}

func (method *ChannelOpen) FrameType() byte {
	return 1
}

func (method *ChannelOpen) Cld() uint16 {
	return 20
}

func (method *ChannelOpen) MId() uint16 {
	return 10
}

func (method *ChannelOpen) Sync() bool {
	return true
}

func (method *ChannelOpen) Read(reader io.Reader) (err error) {
	method.Reserved1, err = ReadShortstr(reader)
	if err != nil {
		return err
	}

	return
}

func (method *ChannelOpen) Write(writer io.Writer) (err error) {

	if err = WriteShortstr(writer, method.Reserved1); err != nil {
		return err
	}

	return
}

type ChannelOpenOk struct {
	Reserved1 []byte
}

func (method *ChannelOpenOk) Name() string {
	return "ChannelOpenOk"
}

func (method *ChannelOpenOk) FrameType() byte {
	return 1
}

func (method *ChannelOpenOk) Cld() uint16 {
	return 20
}

func (method *ChannelOpenOk) MId() uint16 {
	return 11
}

func (method *ChannelOpenOk) Sync() bool {
	return true
}

func (method *ChannelOpenOk) Read(reader io.Reader) (err error) {

	method.Reserved1, err = ReadLongstr(reader)
	if err != nil {
		return err
	}

	return
}

func (method *ChannelOpenOk) Write(writer io.Writer) (err error) {

	if err = WriteLongstr(writer, method.Reserved1); err != nil {
		return err
	}

	return
}

type ChannelClose struct {
	ReplyCode uint16
	ReplyText string
	ClassID   uint16
	MethodID  uint16
}

func (method *ChannelClose) Name() string {
	return "ChannelClose"
}

func (method *ChannelClose) FrameType() byte {
	return 1
}

func (method *ChannelClose) Cld() uint16 {
	return 20
}

func (method *ChannelClose) MId() uint16 {
	return 40
}

func (method *ChannelClose) Sync() bool {
	return true
}

func (method *ChannelClose) Read(reader io.Reader) (err error) {

	method.ReplyCode, err = ReadShort(reader)
	if err != nil {
		return err
	}

	method.ReplyText, err = ReadShortstr(reader)
	if err != nil {
		return err
	}

	method.ClassID, err = ReadShort(reader)
	if err != nil {
		return err
	}

	method.MethodID, err = ReadShort(reader)
	if err != nil {
		return err
	}

	return
}

func (method *ChannelClose) Write(writer io.Writer) (err error) {

	if err = WriteShort(writer, method.ReplyCode); err != nil {
		return err
	}

	if err = WriteShortstr(writer, method.ReplyText); err != nil {
		return err
	}

	if err = WriteShort(writer, method.ClassID); err != nil {
		return err
	}

	if err = WriteShort(writer, method.MethodID); err != nil {
		return err
	}

	return
}

type ChannelCloseOk struct {
}

func (method *ChannelCloseOk) Name() string {
	return "ChannelCloseOk"
}

func (method *ChannelCloseOk) FrameType() byte {
	return 1
}

func (method *ChannelCloseOk) Cld() uint16 {
	return 20
}

func (method *ChannelCloseOk) MId() uint16 {
	return 41
}

func (method *ChannelCloseOk) Sync() bool {
	return true
}

func (method *ChannelCloseOk) Read(reader io.Reader) (err error) {
	return
}

func (method *ChannelCloseOk) Write(writer io.Writer) (err error) {
	return
}











