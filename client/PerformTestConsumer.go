package main

import (
	"log"
	"time"
	"client/client"

	"fmt"
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

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()


	// err = ch.Qos(
	// 	5,
	// 	0,
	// 	false,
	// )
	// failOnError(err, "Failed to Qos")


	msgs, err := ch.Consume(
		"simpleMesTest",
		"",
		false,
		false,
		false,
		false,
	)
	failOnError(err, "Failed to register a consumer")

	var st time.Time
	var target uint64 = 100000
	for m := range msgs {
		if m.DeliveryTag == 1{
			st = time.Now()
		}
		if m.DeliveryTag % 10000 == 0 {
			fmt.Println("delivery tag : ", m.DeliveryTag)
		}
		m.Ack(false)
		if m.DeliveryTag == target{
			break
		}
	}
	fmt.Println("consume",target ," mes cost: ", time.Since(st))
	time.Sleep(time.Second)

}