package dmqp

import (
	"io"
	"fmt"
	// "bytes"
)

func ReadMethod(reader io.Reader) (Method, error) {
	// fmt.Println("readMethod1")
	classID, err := ReadShort(reader)
	if err != nil {
		return nil, err
	}	
	// fmt.Println("readMethod2")

	methodID, err := ReadShort(reader)
	if err != nil {
		return nil, err
	}
	// fmt.Println("readMethod3")
	// fmt.Println("classID", classID, "methosID", methodID)

	switch classID{
	case 10:
		switch methodID{
		case 11:
			var method = &ConnectionStartOk{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 30:
			var method = &ConnectionTune{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 31:
			var method = &ConnectionTuneOk{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 50:
			var method = &ConnectionClose{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 51:
			var method = &ConnectionCloseOk{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		}
	case 20:
		switch methodID {
		case 10:
			var method = &ChannelOpen{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 11:
			var method = &ChannelOpenOk{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 40:
			var method = &ChannelClose{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 41:
			var method = &ChannelCloseOk{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		}
	case 40 :	//exchange
		switch methodID {
		case 10:
			var method = &ExchangeDeclare{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 11:
			var method = &ExchangeDeclareOk{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 20:
			var method = &ExchangeDelete{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 21:
			var method = &ExchangeDeleteOk{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		}
	case 50:
		switch methodID {
		case 10:
			var method = &QueueDeclare{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 11:
			var method = &QueueDeclareOk{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 20:
			var method = &QueueBind{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 21:
			var method = &QueueBindOk{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 50:
			var method = &QueueUnbind{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 51:
			var method = &QueueUnbindOk{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 30:
			var method = &QueuePurge{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 31:
			var method = &QueuePurgeOk{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 40:
			var method = &QueueDelete{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 41:
			var method = &QueueDeleteOk{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		}
	case 60: //basic
		switch methodID {
		case 10:
			var method = &BasicQos{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 11:
			var method = &BasicQosOk{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 20:
			var method = &BasicConsume{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 21:
			var method = &BasicConsumeOk{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 30:
			var method = &BasicCancel{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 31:
			var method = &BasicCancelOk{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 40:
			var method = &BasicPublish{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 50:
			var method = &BasicReturn{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 60:
			var method = &BasicDeliver{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 80:
			var method = &BasicAck{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 90:
			var method = &BasicReject{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 120:
			var method = &BasicNack{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		} 
	case 85:
		switch methodID {

		case 10:
			var method = &ConfirmSelect{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		case 11:
			var method = &ConfirmSelectOk{}
			if err := method.Read(reader); err != nil {
				return nil, err
			}
			return method, nil
		}

	}
	fmt.Println("no methods")
	return nil, fmt.Errorf("unknown classID and methodID: [%d. %d]", classID, methodID)

}

//对于方法帧， broker结构体里不存classID和MethodID
func WriteMethod(writer io.Writer, method Method) (err error) {
	if err = WriteShort(writer, method.Cld()); err != nil {
		return err
	}
	if err = WriteShort(writer, method.MId()); err != nil {
		return err
	}
	if err = method.Write(writer); err != nil {
		return err
	}
	return
}
