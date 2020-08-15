package main

import (
	"log"
	"time"
	// "sync"
	"fmt"
	"client/client"
)
func failOnError(err error, msg string) {
	if err != nil {
	  log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	uri := client.URI{
		Scheme : "dmqp",
		Host: "localhost",
		Port : "9833", 
	}
	conn, err := client.Dial(uri)
	failOnError(err, "Failed to connect to broker")
	defer conn.Close()

	// conn.Channel()
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	msgs, err := ch.Consume(
		"simpleMesTest",
		"",
		false,
		false,
		false,
		false,
	)
	failOnError(err, "Failed to register a consumer")
	// forever := make(chan bool)

	var st time.Time
	var target uint64 = 60
	for m := range msgs {
		if m.DeliveryTag == 1{
			st = time.Now()
		}
			fmt.Println("delivery tag:", m.DeliveryTag, "priority:", m.Priority)
		m.Ack(false)
		if m.DeliveryTag == target{
			break
		}
	}
	fmt.Println("consume",target ," mes cost: ", time.Since(st))
	time.Sleep(time.Second)
}