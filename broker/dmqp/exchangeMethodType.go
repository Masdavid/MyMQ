package dmqp
import (
	"io"
)
type ExchangeDeclare struct {
	Reserved1  uint16
	Exchange   string
	Type       string
	Passive    bool
	Durable    bool
	NoWait     bool
}

func (method *ExchangeDeclare) Name() string {
	return "ExchangeDeclare"
}

func (method *ExchangeDeclare) FrameType() byte {
	return 1
}

func (method *ExchangeDeclare) Cld() uint16 {
	return 40
}

func (method *ExchangeDeclare) MId() uint16 {
	return 10
}

func (method *ExchangeDeclare) Sync() bool {
	return true
}

func (method *ExchangeDeclare) Read(reader io.Reader) (err error) {

	method.Reserved1, err = ReadShort(reader)
	if err != nil {
		return err
	}

	method.Exchange, err = ReadShortstr(reader)
	if err != nil {
		return err
	}

	method.Type, err = ReadShortstr(reader)
	if err != nil {
		return err
	}

	bits, err := ReadByte(reader)
	if err != nil {
		return err
	}

	method.Passive = bits&(1<<0) != 0
	method.Durable = bits&(1<<1) != 0
	method.NoWait = bits&(1<<2) != 0
	return
}

func (method *ExchangeDeclare) Write(writer io.Writer) (err error) {

	if err = WriteShort(writer, method.Reserved1); err != nil {
		return err
	}

	if err = WriteShortstr(writer, method.Exchange); err != nil {
		return err
	}

	if err = WriteShortstr(writer, method.Type); err != nil {
		return err
	}

	var bits byte

	if method.Passive {
		bits |= 1 << 0
	}

	if method.Durable {
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

type ExchangeDeclareOk struct {
}

func (method *ExchangeDeclareOk) Name() string {
	return "ExchangeDeclareOk"
}

func (method *ExchangeDeclareOk) FrameType() byte {
	return 1
}

func (method *ExchangeDeclareOk) Cld() uint16 {
	return 40
}

func (method *ExchangeDeclareOk) MId() uint16 {
	return 11
}

func (method *ExchangeDeclareOk) Sync() bool {
	return true
}

func (method *ExchangeDeclareOk) Read(reader io.Reader) (err error) {

	return
}

func (method *ExchangeDeclareOk) Write(writer io.Writer) (err error) {

	return
}

type ExchangeDelete struct {
	Reserved1 uint16
	Exchange  string
	IfUnused  bool
	NoWait    bool
}

func (method *ExchangeDelete) Name() string {
	return "ExchangeDelete"
}

func (method *ExchangeDelete) FrameType() byte {
	return 1
}

func (method *ExchangeDelete) Cld() uint16 {
	return 40
}

func (method *ExchangeDelete) MId() uint16 {
	return 20
}

func (method *ExchangeDelete) Sync() bool {
	return true
}

func (method *ExchangeDelete) Read(reader io.Reader) (err error) {

	method.Reserved1, err = ReadShort(reader)
	if err != nil {
		return err
	}

	method.Exchange, err = ReadShortstr(reader)
	if err != nil {
		return err
	}

	bits, err := ReadByte(reader)
	if err != nil {
		return err
	}

	method.IfUnused = bits&(1<<0) != 0

	method.NoWait = bits&(1<<1) != 0

	return
}

func (method *ExchangeDelete) Write(writer io.Writer) (err error) {

	if err = WriteShort(writer, method.Reserved1); err != nil {
		return err
	}

	if err = WriteShortstr(writer, method.Exchange); err != nil {
		return err
	}

	var bits byte

	if method.IfUnused {
		bits |= 1 << 0
	}

	if method.NoWait {
		bits |= 1 << 1
	}

	if err = WriteOctet(writer, bits); err != nil {
		return err
	}

	return
}

type ExchangeDeleteOk struct {
}

func (method *ExchangeDeleteOk) Name() string {
	return "ExchangeDeleteOk"
}

func (method *ExchangeDeleteOk) FrameType() byte {
	return 1
}

func (method *ExchangeDeleteOk) Cld() uint16 {
	return 40
}

func (method *ExchangeDeleteOk) MId() uint16 {
	return 21
}

func (method *ExchangeDeleteOk) Sync() bool {
	return true
}

func (method *ExchangeDeleteOk) Read(reader io.Reader) (err error) {

	return
}

func (method *ExchangeDeleteOk) Write(writer io.Writer) (err error) {

	return
}


