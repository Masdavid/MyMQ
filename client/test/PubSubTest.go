package test

import (
	"client/client"
	"strconv"
	"sync/atomic"

	"fmt"
)

func MultiQueueAndExDeclare(ch *client.Channel) error {
	err := ch.ExchangeDeclare(
		"pubsubEx",
		"fanout",
		false,
		false,
	)
	if err != nil {
		return err
	}
	var qunameSeq uint64
	for i:= 0; i < 5; i++ {
		var quname string = "queNo." + strconv.FormatUint(atomic.AddUint64(&qunameSeq, 1), 10)
		_, err := ch.QueueDeclare(
			quname,
			false,
			true,
			false,
			false,
		)
		if err != nil {
			return err
		}
		err = ch.QueueBind(
			quname,
			quname,
			"pubsubEx",
			false,
		)
		if err != nil {
			return err
		}
		var cmrName string = "cmrNo." + strconv.FormatUint(atomic.AddUint64(&qunameSeq, 1), 10)

		err = ch.Qos(
			1,
			0,
			false,
		)

		msgch, err := ch.Consume(
			quname,
			cmrName,
			false,
			false,
			false,
			false,
		)
		if err != nil {
			return err
		}

		go func() {
			for msg := range msgch {
				fmt.Println("delivery tag: ", msg.DeliveryTag, msg.ConsumerTag)
				msg.Ack(false)
			} 
		}()

	}
	select {}
	// return nil
}

