package client

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	// "math"
	// "time"
)

type writer struct {
	w io.Writer
}

func (w *writer) WriteFrame(fr frame) (err error) {
	if err = fr.write(w.w); err != nil {
		return
	}

	if buf, ok := w.w.(*bufio.Writer); ok {
		err = buf.Flush()
	}

	return
}

//该函数写如方法帧的payload部分，是Methodframe的write方法的实现
func (f *methodFrame) write(w io.Writer) (err error) {
	var payload bytes.Buffer

	if f.Method == nil {
		return errors.New("malformed frame: missing method")
	}

	class, method := f.Method.id()

	if err = binary.Write(&payload, binary.BigEndian, class); err != nil {
		return
	}

	if err = binary.Write(&payload, binary.BigEndian, method); err != nil {
		return
	}

	if err = f.Method.write(&payload); err != nil {   //写入每个方法帧独有的部分
		return
	}
	return writeFrame(w, frameMethod, f.ChannelId, payload.Bytes())
}

func (f *heartbeatFrame) write(w io.Writer) (err error) {
	return writeFrame(w, frameHeartbeat, f.ChannelId, []byte{})
}

//该函数写如内容头帧的payload部分，是headerframe的write方法的实现
func (f *headerFrame) write(w io.Writer) (err error) {
	var payload bytes.Buffer

	if err = binary.Write(&payload, binary.BigEndian, f.ClassId); err != nil {
		return
	}
	if err = binary.Write(&payload, binary.BigEndian, f.Size); err != nil {
		return
	}
	if err = binary.Write(&payload, binary.BigEndian, f.deliveryMode); err != nil {
		return
	}
	if err = binary.Write(&payload, binary.BigEndian, f.Priority); err != nil {
		return
	}
	return writeFrame(w, frameHeader, f.ChannelId, payload.Bytes())

}

//该函数写如内容体的payload部分，是bodyframe的write方法的实现
func (f *bodyFrame) write(w io.Writer) (err error) {
	return writeFrame(w, frameBody, f.ChannelId, f.Body)
}

//writeFrame构建一个完整的帧
func writeFrame(w io.Writer, typ uint8, channel uint16, payload []byte) (err error) {
	end := []byte{frameEnd}
	size := uint(len(payload))

	//写入帧头 type，channel，size
	_, err = w.Write([]byte{
		byte(typ),
		byte((channel & 0xff00) >> 8),
		byte((channel & 0x00ff) >> 0),
		byte((size & 0xff000000) >> 24),
		byte((size & 0x00ff0000) >> 16),
		byte((size & 0x0000ff00) >> 8),
		byte((size & 0x000000ff) >> 0),
	})

	if err != nil {
		return
	}
	//写入帧的payload
	if _, err = w.Write(payload); err != nil {
		return
	}

	//写入end
	if _, err = w.Write(end); err != nil {
		return
	}

	return
}


