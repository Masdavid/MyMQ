package dmqp

import(
	"io"
)

type ConfirmSelect struct {
	Nowait bool
}

func (method *ConfirmSelect) Name() string {
	return "ConfirmSelect"
}

func (method *ConfirmSelect) FrameType() byte {
	return 1
}

func (method *ConfirmSelect) Cld() uint16 {
	return 85
}

func (method *ConfirmSelect) MId() uint16 {
	return 10
}

func (method *ConfirmSelect) Sync() bool {
	return true
}

func (method *ConfirmSelect) Read(reader io.Reader) (err error) {

	bits, err := ReadByte(reader)
	if err != nil {
		return err
	}

	method.Nowait = bits&(1<<0) != 0

	return
}

func (method *ConfirmSelect) Write(writer io.Writer) (err error) {

	var bits byte

	if method.Nowait {
		bits |= 1 << 0
	}

	if err = WriteOctet(writer, bits); err != nil {
		return err
	}

	return
}

type ConfirmSelectOk struct {
}

func (method *ConfirmSelectOk) Name() string {
	return "ConfirmSelectOk"
}

func (method *ConfirmSelectOk) FrameType() byte {
	return 1
}

func (method *ConfirmSelectOk) Cld() uint16 {
	return 85
}

func (method *ConfirmSelectOk) MId() uint16 {
	return 11
}

func (method *ConfirmSelectOk) Sync() bool {
	return true
}

func (method *ConfirmSelectOk) Read(reader io.Reader) (err error) {
	return
}

func (method *ConfirmSelectOk) Write(writer io.Writer) (err error) {
	return
}

