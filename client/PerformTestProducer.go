package main

import (
	"client/client"
	"client/test"
	// "fmt"
	// "time"
	"log"
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
	// conn, err := client.Dial(uri)
	// if err != nil {
	// 	return 
	// }
	// ch, err := conn.Channel()
	// if err != nil {
	// 	return
	// }
	
	// test.DeclareQueueTest(ch, 100)

	// test.DeclareQueueDurableTest(ch, 10)

	// test.DeclareExchangeTest(ch, 100)

	// test.DeclareExchangeDurableTest(ch, 100)

	// test.DeclareConsumerTest(ch, 100)

	body := make([]byte, 1024)
	for id, _ := range body {
		body[id] = 'T'
	}
	// test.SimpleMessageTest(ch, body, 1000)

	// test.BindTest(ch, 1000)

	// test.PriorityMesTest(ch, body, 50)

	// test.MultiQueueAndExDeclare(ch)
	test.MultiProducer(uri, body, 20000)

	// for {
	// 	exp, pub := ch.GetExpAndPubConfirmTag()
	// 	fmt.Println("slide window", exp, " to ", pub)
	// 	time.Sleep(500*time.Millisecond)
	// }
	// return
	// select{}
	return
}
