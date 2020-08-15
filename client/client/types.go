package client

import (
	"io"
	"fmt"
	// "time"
)

//frame接口
type frame interface {
	write(io.Writer) error
	channel() uint16
}

//协议头帧
type protocolHeader struct{}

func (protocolHeader) write(w io.Writer) error {
	_, err := w.Write([]byte{'d','m','q','p'})
	return err
}

func (protocolHeader) channel() uint16 {
	panic("only valid as initial handshake")
}

type message interface {
	id() (uint16, uint16)
	wait() bool
	read(io.Reader) error
	write(io.Writer) error
}

type messageWithContent interface {
	message
	getContent() (byte, byte, []byte)
	setContent(byte, byte, []byte)
}


//方法帧
type methodFrame struct {
	ChannelId uint16
	ClassId   uint16
	MethodId  uint16
	Method    message
}

func (f *methodFrame) channel() uint16 { return f.ChannelId }

//心跳帧
type heartbeatFrame struct {
	ChannelId uint16
}

func (f *heartbeatFrame) channel() uint16 { return f.ChannelId }

//内容头帧
type headerFrame struct {
	ChannelId  uint16
	ClassId    uint16
	Size       uint64
	deliveryMode byte
	Priority byte
}

func (f *headerFrame) channel() uint16 { return f.ChannelId }

//内容体帧
type bodyFrame struct {
	ChannelId uint16
	Body      []byte
}

func (f *bodyFrame) channel() uint16 { return f.ChannelId }


type Error struct {
	Code    int    
	Reason  string 
	Server  bool   
	Recover bool  
}

func (e Error) Error() string {
	return fmt.Sprintf("Exception (%d) Reason: %q", e.Code, e.Reason)
}

func newError(code uint16, text string) *Error {
	return &Error{
		Code:    int(code),
		Reason:  text,
		Recover: isSoftExceptionCode(int(code)),
		Server:  true,
	}
}

type Queue struct {
	Name      string 
	Messages  int   
	Consumers int   
}

type Publishing struct {

	DeliveryMode    byte     // T2代表持久化
	Priority        byte     //代表优先级
	Body []byte
}

type Confirmation struct {
	DeliveryTag uint64 
	Ack         bool   
}

var (
	ErrClosed = &Error{Code: ChannelError, Reason: "channel/connection is not open"}
	ErrChannelMax = &Error{Code: ChannelError, Reason: "channel id space exhausted"}
	ErrSASL = &Error{Code: AccessRefused, Reason: "SASL could not negotiate a shared mechanism"}
	ErrCredentials = &Error{Code: AccessRefused, Reason: "username or password not allowed"}
	ErrVhost = &Error{Code: AccessRefused, Reason: "no access to this vhost"}
	ErrSyntax = &Error{Code: SyntaxError, Reason: "invalid field or value inside of a frame"}
	ErrFrame = &Error{Code: FrameError, Reason: "frame could not be parsed"}
	ErrCommandInvalid = &Error{Code: CommandInvalid, Reason: "unexpected command received"}
	ErrUnexpectedFrame = &Error{Code: UnexpectedFrame, Reason: "unexpected frame received"}
	ErrFieldType = &Error{Code: SyntaxError, Reason: "unsupported table field type"}
)

