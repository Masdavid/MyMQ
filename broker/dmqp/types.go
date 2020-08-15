package dmqp

import (
	"sync/atomic"
	"time"
	"bytes"
)

//基本帧结构
type Frame struct {
	ChannelID  uint16
	Type       byte
	CloseAfter bool
	Sync       bool
	Payload    []byte
}

//基本内容头结构
type ContentHeader struct {
	BodySize      uint64
	ClassID       uint16
	DeliveryMode    byte
	Priority		byte
}


type ConfirmProp struct {
	ChanID           uint16
	ConnID           uint64
	DeliveryTag      uint64
	ConfirmNumNeed int
	ConfirmNumHas   int
	Durable bool
}

func (meta *ConfirmProp) CanConfirm() bool {
	return meta.ConfirmNumHas == meta.ConfirmNumNeed
}

//broker里queue存放的消息类型Message
type Message struct {
	ID            uint64
	BodySize      uint64
	DeliveryCount uint32       //投递次数
	Mandatory     bool
	Immediate     bool
	Exchange      string
	RoutingKey    string
	ConfirmProp   *ConfirmProp
	Header        *ContentHeader
	Body          []*Frame
}

var msgID = uint64(time.Now().UnixNano())


//消息类型只能用publish实例化
func NewMessage(method *BasicPublish) *Message {
	return &Message{
		Exchange:      method.Exchange,
		RoutingKey:    method.RoutingKey,
		Mandatory:     method.Mandatory,
		Immediate:     method.Immediate,
		BodySize:      0,
		DeliveryCount: 0,
	}
}

func (m *Message) IsDurable() bool {
	deliveryMode := m.Header.DeliveryMode
	return deliveryMode == 2
}

func (m *Message) NoPriority() byte {
	return m.Header.Priority
}

func (m *Message) GenerateSeq() {
	if m.ID == 0 {
		m.ID = atomic.AddUint64(&msgID, 1)
	}
}

//对i消息体的添加
func (m *Message) Append(body *Frame) {
	m.Body = append(m.Body, body)
	m.BodySize += uint64(len(body.Payload))
}

func(m *Message) Serialize() (res []byte, err error) {
	buf := bytes.NewBuffer([]byte{})
	if err = WriteLonglong(buf, m.ID); err != nil {
		return nil, err
	}

	if err = WriteContentHeader(buf, m.Header); err != nil {
		return nil, err
	}
	if err = WriteShortstr(buf, m.Exchange); err != nil {
		return nil, err
	}
	if err = WriteShortstr(buf, m.RoutingKey); err != nil {
		return nil, err
	}
	for _, frame := range m.Body {
		if err = WriteFrame(buf, frame); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
	//在这复制一下

}

func (m *Message) Deserialize(res []byte) (err error) {
	buf := bytes.NewReader(res)
	if m.ID, err = ReadLonglong(buf); err != nil {
		return err
	}

	if m.Header, err = ReadContentHeader(buf); err != nil {
		return err
	}
	if m.Exchange, err = ReadShortstr(buf); err != nil {
		return err
	}
	if m.RoutingKey, err = ReadShortstr(buf); err != nil {
		return err
	}

	for m.BodySize < m.Header.BodySize {
		body, errFrame := ReadFrame(buf)
		if errFrame != nil {
			return errFrame
		}
		m.Append(body)
	}

	return nil

}


const (
	ErrorOnConnection = iota
	ErrorOnChannel
)

// Error represents dmqp-error data
type Error struct {
	ReplyCode uint16
	ReplyText string
	ClassID   uint16
	MethodID  uint16
	ErrorType int
}

// ErrorStopConn 关闭连接
func ErrorStopConn(code uint16, text string, classID uint16, methodID uint16) *Error {
	err := &Error{
		ReplyCode: code,
		ReplyText: ConstantsNameMap[code] + " - " + text,
		ClassID:   classID,
		MethodID:  methodID,
		ErrorType: ErrorOnConnection,
	}

	return err
}

// ErrorStopCh关闭信道
func ErrorStopCh(code uint16, text string, classID uint16, methodID uint16) *Error {
	err := &Error{
		ReplyCode: code,
		ReplyText: ConstantsNameMap[code] + " - " + text,
		ClassID:   classID,
		MethodID:  methodID,
		ErrorType: ErrorOnChannel,
	}

	return err
}