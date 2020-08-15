package dmqp

import (
	// "bytes"
	"encoding/binary"
	// "errors"
	"fmt"
	"io"
	// "time"
	"broker/buf"
)
var hbp = buf.NewBufferPool(14)
var DmqpHeader = []byte{'d', 'm', 'q', 'p'}
/*
0      1         3             7                  size+7 size+8
+------+---------+-------------+  +------------+  +-----------+
| type | channel |     size    |  |  payload   |  | frame-end |
+------+---------+-------------+  +------------+  +-----------+
 octet   short         long         size octets       octet
 */

//读一帧
func ReadFrame(r io.Reader) (frame *Frame, err error) {
	frame = &Frame{}
	if frame.Type, err = ReadByte(r); err != nil {
		return nil, err
	}
	if frame.ChannelID, err = ReadShort(r); err != nil {
		return nil, err
	}
	var payloadSize uint32
	if payloadSize, err = ReadLong(r); err != nil {
		return nil, err
	}

	var payload = make([]byte, payloadSize+1)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	frame.Payload = payload[0:payloadSize]
	// check frame end
	if payload[payloadSize] != FrameEnd {
		return nil, fmt.Errorf(
			"the frame-end octet MUST always be the hexadecimal value 'xCE', %x given",
			payload[payloadSize])
	}

	return frame, nil
}

// 写入一个帧，type + chanID + Payload + end
func WriteFrame(wr io.Writer, frame *Frame) (err error) {
	if err = WriteOctet(wr, frame.Type); err != nil {
		return err
	}
	if err = WriteShort(wr, frame.ChannelID); err != nil {
		return err
	}

	// size + payload
	if err = WriteLongstr(wr, frame.Payload); err != nil {
		return err
	}
	// frame end
	if err = WriteOctet(wr, FrameEnd); err != nil {
		return err
	}

	return nil
}

func writeSlice(wr io.Writer, data []byte) error {
	_, err := wr.Write(data[:])
	return err
}

func ReadByte(r io.Reader) (data byte, err error) {
	var b [1]byte
	if _, err = io.ReadFull(r, b[:]); err != nil {
		return
	}
	data = b[0]
	return
}

func WriteOctet(wr io.Writer, data byte) error {
	var b [1]byte
	b[0] = data
	return writeSlice(wr, b[:])
}

func ReadShort(r io.Reader) (data uint16, err error) {
	err = binary.Read(r, binary.BigEndian, &data)
	return
}

func WriteShort(wr io.Writer, data uint16) error {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], data)
	return writeSlice(wr, b[:])
}

func ReadLong(r io.Reader) (data uint32, err error) {
	err = binary.Read(r, binary.BigEndian, &data)
	return
}

func WriteLong(wr io.Writer, data uint32) error {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], data)
	return writeSlice(wr, b[:])
}

func ReadLonglong(r io.Reader) (data uint64, err error) {
	err = binary.Read(r, binary.BigEndian, &data)
	return
}

func WriteLonglong(wr io.Writer, data uint64) error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], data)
	return writeSlice(wr, b[:])
}

func ReadShortstr(r io.Reader) (data string, err error) {
	var length byte

	length, err = ReadByte(r)
	if err != nil {
		return "", err
	}

	strBytes := make([]byte, length)

	_, err = io.ReadFull(r, strBytes)
	if err != nil {
		return "", err
	}
	data = string(strBytes)
	return
}

func WriteShortstr(wr io.Writer, data string) error {
	if err := WriteOctet(wr, byte(len(data))); err != nil {
		return err
	}
	if _, err := wr.Write([]byte(data)); err != nil {
		return err
	}

	return nil
}

func ReadLongstr(r io.Reader) (data []byte, err error) {
	var length uint32

	length, err = ReadLong(r)
	if err != nil {
		return nil, err
	}

	data = make([]byte, length)

	_, err = io.ReadFull(r, data)
	if err != nil {
		return nil, err
	}
	return
}

func WriteLongstr(wr io.Writer, data []byte) error {
	// 写入payload的长度和payload本身
	err := WriteLong(wr, uint32(len(data)))
	if err != nil {
		return err
	}
	_, err = wr.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func ReadContentHeader(r io.Reader) (*ContentHeader, error) {
	// 12 bytes for class-id | body size | delivery mode | priority
	headerBuf := hbp.Get()
	defer hbp.Put(headerBuf)
	
	var err error
	var header [12]byte
	if _, err = io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	if _, err = headerBuf.Write(header[:]); err != nil {
		return nil, err
	}
	ch := &ContentHeader{}
	if ch.ClassID, err = ReadShort(headerBuf); err != nil {
		return nil, err
	}
	if ch.BodySize, err = ReadLonglong(headerBuf); err != nil {
		return nil, err
	}
	if ch.DeliveryMode, err = ReadByte(headerBuf); err != nil{
		return nil ,err
	}
	if ch.Priority, err = ReadByte(headerBuf); err != nil{
		return nil, err
	}
	return ch, nil
}

func WriteContentHeader(writer io.Writer, header *ContentHeader) (err error) {
	if err = WriteShort(writer, header.ClassID); err != nil {
		return err
	}
	if err = WriteLonglong(writer, header.BodySize); err != nil {
		return err
	}
	if err = WriteOctet(writer, header.DeliveryMode); err != nil {
		return err
	}
	if err = WriteOctet(writer, header.Priority); err != nil {
		return err
	}
	return
}
