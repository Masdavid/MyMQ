package client

import (
	"io"
	"fmt"
	// "time"
	"errors"
	"encoding/binary"
)

type reader struct {
	r io.Reader
}
// var cnt  = 0
func (re *reader) ReadFrame() (f frame, err error) {
	// cnt++
	// fmt.Println(cnt)
	var store [7]byte
	if _, err = io.ReadFull(re.r, store[:7]); err != nil {
		// fmt.Println("err on read head of frame")
		return
	}
	typ := uint8(store[0])
	ch := binary.BigEndian.Uint16(store[1:3])
	size := binary.BigEndian.Uint32(store[3:7])
	// fmt.Println("frametype:", typ, "framechannel:", ch, "framesize:", size)

	switch typ{
	case frameMethod:
		if f, err = re.praseMethodFrame(ch, size); err != nil {
			fmt.Println("err on praseMethodFrame")
			return
		}
	case frameHeader:
		if f, err = re.parseContentHeaderFrame(ch, size); err != nil {
			fmt.Println("err on praseContentHeaderFrame")
			return
		}
	case frameBody:
		if f, err = re.parseContentBodyFrame(ch, size); err != nil {
			return nil, err
		}
	case frameHeartbeat:
		if f, err = re.parseHeartbeatFrame(ch, size); err != nil {
			fmt.Println("err on praseeartbeatFrame")
			return
		}
	default:
		return nil, ErrFrame
	}

	//一帧的结尾必须以0Xce作为结尾，否则就是无效帧
	if _, err = io.ReadFull(re.r, store[:1]); err != nil {
		return nil, err
	}
	// fmt.Println(store[0])
	if store[0] != frameEnd {
		fmt.Println("err on wrong end frame", store[0])
		return nil, ErrFrame
	}

	return

}

func (re *reader) praseMethodFrame(ch uint16, size uint32) (f frame, err error){
	mf := &methodFrame{
		ChannelId : ch,
	}
	if err = binary.Read(re.r, binary.BigEndian, &mf.ClassId); err != nil {
		return
	}
	if err = binary.Read(re.r, binary.BigEndian, &mf.MethodId); err != nil {
		return
	}
	// fmt.Println("mf.classid", mf.ClassId, "mf.methodid", mf.MethodId)
	switch mf.ClassId {
	case 10:     //connection
		switch mf.MethodId{
		case 10:
			method := &connectionStart{}
			if err = method.read(re.r); err != nil {
				fmt.Println("err on reading connectionStart method")

				return
			}
			mf.Method = method
		case 11:
			method := &connectionStartOk{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method
		case 30:
			method := &connectionTune{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method
		case 31:
			method := &connectionTuneOk{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method
		case 50:
			method := &connectionClose{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method
		case 51:
			method := &connectionCloseOk{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method
		default:
			return nil, fmt.Errorf("nknown method %d for class %d", mf.MethodId, mf.ClassId)
		}
	case 20:    //channel
		switch mf.MethodId{
		case 10:
			method := &channelOpen{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method
		case 11 :
			method := &channelOpenOk{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method
		case 40:
			method := &channelClose{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method
		case 41:
			method := &channelCloseOk{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method
		default:
			return nil, fmt.Errorf("unknown method %d for class %d", mf.MethodId, mf.ClassId)
		}
	case 40:
		switch mf.MethodId{
			case 10: // exchange declare
			method := &exchangeDeclare{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 11: // exchange declare-ok
			method := &exchangeDeclareOk{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 20: // exchange delete
			method := &exchangeDelete{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 21: // exchange delete-ok
			method := &exchangeDeleteOk{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method
		}
	case 50:	//queue
		switch mf.MethodId {
			case 10: // queue declare
			method := &queueDeclare{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 11: // queue declare-ok
			method := &queueDeclareOk{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 20: // queue bind
			method := &queueBind{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 21: // queue bind-ok
			method := &queueBindOk{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 50: // queue unbind
			method := &queueUnbind{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 51: // queue unbind-ok
			method := &queueUnbindOk{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 30: // queue purge
			method := &queuePurge{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 31: // queue purge-ok
			method := &queuePurgeOk{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 40: // queue delete
			method := &queueDelete{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 41: // queue delete-ok
			method := &queueDeleteOk{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method
		default:
			return nil, fmt.Errorf("Bad method frame, unknown method %d for class %d", mf.MethodId, mf.ClassId)
		}
	case 60:	//basic
	switch mf.MethodId {
		case 10: // basic qos
			method := &basicQos{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 11: // basic qos-ok
			method := &basicQosOk{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 20: // basic consume
			method := &basicConsume{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 21: // basic consume-ok
			method := &basicConsumeOk{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 30: // basic cancel
			method := &basicCancel{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 31: // basic cancel-ok
			method := &basicCancelOk{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 40: // basic publish
			method := &basicPublish{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 50: // basic return
			method := &basicReturn{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 60: // basic deliver
			method := &basicDeliver{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 80: // basic ack
			method := &basicAck{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 90: // basic reject
			method := &basicReject{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 120: // basic nack
			method := &basicNack{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method
		default:
			return nil, fmt.Errorf("Bad method frame, unknown method %d for class %d", mf.MethodId, mf.ClassId)
	}
	case 85:
		switch mf.MethodId {
		case 10: // confirm select
			method := &confirmSelect{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method

		case 11: // confirm select-ok
			method := &confirmSelectOk{}
			if err = method.read(re.r); err != nil {
				return
			}
			mf.Method = method
		default:
			return nil, fmt.Errorf("Bad method frame, unknown method %d for class %d", mf.MethodId, mf.ClassId)
		}
	default:
		return nil, fmt.Errorf("Bad method frame, unknown class %d", mf.ClassId)

	}
	return mf, nil
}
//该函数读取内容头帧，frame接口
func (r *reader) parseContentHeaderFrame(channel uint16, size uint32) (frame frame, err error) {
	hf := &headerFrame {
		ChannelId : channel,
	}
	if err = binary.Read(r.r, binary.BigEndian, &hf.ClassId); err != nil {
		return
	}
	if err = binary.Read(r.r, binary.BigEndian, &hf.Size); err != nil {
		return
	}
	if err = binary.Read(r.r, binary.BigEndian, &hf.deliveryMode); err != nil {
		return
	}
	if err = binary.Read(r.r, binary.BigEndian, &hf.Priority); err != nil {
		return
	}

	return hf, nil
}


func (r *reader) parseContentBodyFrame(channel uint16, size uint32) (frame frame, err error) {
	bf := &bodyFrame{
		ChannelId: channel,
		Body:      make([]byte, size),
	}

	if _, err = io.ReadFull(r.r, bf.Body); err != nil {
		return nil, err
	}

	return bf, nil
}

var errHeartbeatPayload = errors.New("Heartbeats should not have a payload")

func (r *reader) parseHeartbeatFrame(channel uint16, size uint32) (frame frame, err error) {
	hf := &heartbeatFrame{
		ChannelId: channel,
	}
	if size > 0 {
		return nil, errHeartbeatPayload
	}
	return hf, nil
}