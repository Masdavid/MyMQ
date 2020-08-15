package test

import (
	"client/client"
	"fmt"
	"time"
	// "math"
	// "sync/atomic"
	// "strconv"
)

func BindTest(ch *client.Channel, num int) error{
	err := ch.ExchangeDeclare(
		"exd",
		"fanout",
		false,
		false,
	)
	if err != nil {
		return err
	}

	st := time.Now()
	for i:= 0; i < num; i++ {
		err = ch.QueueBind(
			"simpleMesTest",
			"faff",
			"exd",
			false,
		)
		if err != nil {
			return err
		}
		// fmt.Println("bind success")

		err = ch.QueueUnbind(
			"simpleMesTest",
			"faff",
			"exd",
		)
		// fmt.Println("delete bind success")
	}
	fmt.Println("create and delete ", num, " bind costs time :", time.Since(st))
	return nil
}

func BindDurableTest(ch *client.Channel, num int) error{
	err := ch.ExchangeDeclare(
		"exccc",
		"fanout",
		true,
		false,
	)
	if err != nil {
		return err
	}

	st := time.Now()
	for i:= 0; i < num; i++ {
		err = ch.QueueBind(
			"simpleMesTest",
			"faff",
			"exccc",
			false,
		)
		if err != nil {
			return err
		}
		// fmt.Println("bind success")

		err = ch.QueueUnbind(
			"simpleMesTest",
			"faff",
			"exccc",
		)
		// fmt.Println("delete bind success")
	}
	fmt.Println("create and delete ", num, " bind costs time :", time.Since(st))
	return nil
}