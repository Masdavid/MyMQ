package test

import (
	"client/client"
	"fmt"
	"time"
	"sync/atomic"
	"strconv"
)

func DeclareChannelTest(conn *client.Connection, num int) error {

	start := time.Now()

	for i := 0; i < num; i++ {
	ch1, err := conn.Channel()
	if err != nil {
		return err
	}
	ch1.Close()
	}

	fmt.Println("declare ", num, " channel costs time :", time.Since(start))

	return nil
}

func DeclareQueueTest(ch *client.Channel, num int) error {
	var qunameSeq uint64
	start := time.Now()

	for i:= 0; i < num; i++ {
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
		_, err = ch.QueueDelete(
			quname,
			true,
			true,
			false,
		)
		if err != nil {
			return err
		}
	}
	fmt.Println("declare and delete", num, "queue costs time :", time.Since(start))

	return nil
}

func DeclareQueueDurableTest(ch *client.Channel, num int) error {
	var qunameSeq uint64
	start := time.Now()

	for i:= 0; i < num; i++ {
		var quname string = "queNo." + strconv.FormatUint(atomic.AddUint64(&qunameSeq, 1), 10)
		_, err := ch.QueueDeclare(
			quname,
			true,
			true,
			false,
			false,
		)
		if err != nil {
			return err
		}
		_, err = ch.QueueDelete(
			quname,
			true,
			true,
			false,
		)
		if err != nil {
			return err
		}
	}
	fmt.Println("declare and delete", num, "queue costs time :", time.Since(start))

	return nil
}




func DeclareExchangeTest(ch *client.Channel, num int) error {
	var exnameSeq uint64
	start := time.Now()

	_, err := ch.QueueDeclare(
		"cmrtest",
		true,
		true,
		false,
		false,
	)
	if err != nil {
		return err
	}

	for i:= 0; i < num; i++{
		var exname string = "exNo." + strconv.FormatUint(atomic.AddUint64(&exnameSeq, 1), 10)
		err := ch.ExchangeDeclare(
			exname,
			"fanout",
			false,
			false,
		)
		if err != nil {
			return err
		}
		err = ch.ExchangeDelete(
			exname,
			true,
			false,
		)
		if err != nil {
			return err
		}
	}
	fmt.Println("declare and delete", num, " exchange costs time :", time.Since(start))

	return nil
}

func DeclareExchangeDurableTest(ch *client.Channel, num int) error {
	var exnameSeq uint64
	start := time.Now()

	for i:= 0; i < num; i++{
		var exname string = "exNo." + strconv.FormatUint(atomic.AddUint64(&exnameSeq, 1), 10)
		err := ch.ExchangeDeclare(
			exname,
			"fanout",
			true,
			false,
		)
		if err != nil {
			return err
		}
		err = ch.ExchangeDelete(
			exname,
			true,
			false,
		)
		if err != nil {
			return err
		}
	}
	fmt.Println("declare and delete", num, " exchange costs time :", time.Since(start))

	return nil
}

func DeclareConsumerTest(ch *client.Channel, num int) error {
	_, err := ch.QueueDeclare(
		"cmr",
		false,
		false,
		false,
		false,
	)
	if err != nil {
		return err
	}
	
	var cmrSeq uint64
	start := time.Now()

	for i := 0; i < num; i++ {
		var cmrName string = "cmrNo." + strconv.FormatUint(atomic.AddUint64(&cmrSeq, 1), 10)
		_, err := ch.Consume(
			"cmr",
			cmrName,
			false,
			false,
			false,
			false,
		)
		if err != nil {
			return err
		}
		err = ch.CancelCmr(cmrName, false)
		if err != nil {
			return err
		}
	}
	fmt.Println("declare and delete", num, " consumer costs time :", time.Since(start))
	return nil
}