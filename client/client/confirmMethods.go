package client
import (
	"io"
	"encoding/binary"
)
type confirmSelect struct {
	Nowait bool
}

func (msg *confirmSelect) id() (uint16, uint16) {
	return 85, 10
}

func (msg *confirmSelect) wait() bool {
	return true
}

func (msg *confirmSelect) write(w io.Writer) (err error) {
	var bits byte

	if msg.Nowait {
		bits |= 1 << 0
	}

	if err = binary.Write(w, binary.BigEndian, bits); err != nil {
		return
	}

	return
}

func (msg *confirmSelect) read(r io.Reader) (err error) {
	var bits byte

	if err = binary.Read(r, binary.BigEndian, &bits); err != nil {
		return
	}
	msg.Nowait = (bits&(1<<0) > 0)

	return
}

type confirmSelectOk struct {
}

func (msg *confirmSelectOk) id() (uint16, uint16) {
	return 85, 11
}

func (msg *confirmSelectOk) wait() bool {
	return true
}

func (msg *confirmSelectOk) write(w io.Writer) (err error) {

	return
}

func (msg *confirmSelectOk) read(r io.Reader) (err error) {

	return
}