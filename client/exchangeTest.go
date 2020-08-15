package main 

import (
	"fmt"
	"log"
	"time"
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
	ch.ExchangeDeclare(
		"delete_test_ex",
		"fanout",
		false,
		false,
	)
	time.Sleep(3*time.Second)
	ch.ExchangeDelete(
		"delete_test_ex",
		true,
		false,
	)
	fmt.Println("exchange deleted ok")
	select{}

}