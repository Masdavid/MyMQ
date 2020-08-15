package main 

import (
	"log"
	// "time"
	// "fmt"
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
	if err != nil {
		return 
	}

	// conn.Channel()
	ch, err := conn.Channel()
	if err != nil {
		return
	}

	// q, err := ch.QueueDeclare(
	// 	"daiwei",
	// 	false,
	// 	true,
	// 	false,
	// 	false,
	// )
	// if err != nil {
	// 	return
	// }

	if err := ch.Confirm(false); err != nil {
		return
	}
	// confirms := ch.NotifyPublish(make(chan client.Confirmation, 1))

	for i := 0; i < 5; i++{
		body := "Hello World!"
		err = ch.Publish(
			"pubsubEx",
			"",
			false,
			false,
			client.Publishing{
				Body : []byte(body),
				DeliveryMode : 2,
				Priority : 1,
			})

		failOnError(err, "Failed to publish a message")
		// time.Sleep(time.Second)
	}

	select {}
}