package main

import (
	"client/client"
	"time"
	"fmt"
)

func main() {
	uri := client.URI{
		Scheme : "dmqp",
		Host: "localhost",
		Port : "9833", 
	}
	conn, err := client.Dial(uri)
	if err != nil {
		return 
	}

	ch, err := conn.Channel()
	if err != nil {
		return
	}
	msgs, err := ch.Consume(
		"simpleMesTest",
		"",
		false,
		false,
		false,
		false,
	)
	// failOnError(err, "Failed to register a consumer")
	// forever := make(chan bool)

	var st time.Time
	var target uint64 = 1000
	for m := range msgs {
		if m.DeliveryTag == 1{
			st = time.Now()
		}
		if m.DeliveryTag % 100 == 0 {
			fmt.Println("delivery tag : ", m.DeliveryTag)
		}
		m.Ack(false)
		if m.DeliveryTag == target{
			break
		}
	}
	fmt.Println("consume",target ," mes cost: ", time.Since(st))
	time.Sleep(time.Second)
	ch.Close()
	conn.Close()
	return
}