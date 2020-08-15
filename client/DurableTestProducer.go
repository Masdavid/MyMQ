package main

import (
	"log"
	"fmt"
	"time"
	"sync"
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

	if err := ch.Confirm(false); err != nil {
		return
	}

	var num int = 1000

	conCh := ch.NotifyPublish(make(chan client.Confirmation, 1))
	var wgg sync.WaitGroup
	wgg.Add(1)
	go func () {
		defer wgg.Done()
		for cmsg := range conCh {
			// fmt.Println(cmsg.DeliveryTag)
			if cmsg.DeliveryTag == uint64(num) {
				break
			}
		}
	}()

	body := make([]byte, 1024)
	for id, _ := range body {
		body[id] = 'T'
	}

	start := time.Now()
	cnt := 0
	for{
		if cnt == num {
			break
		}
		// exp, pub := ch.GetExpAndPubConfirmTag()
		// if exp < pub && exp + 499 < pub {
		// 	// fmt.Println("slide window too big: ", exp, " to ", pub)
		// 	continue
		// }

		err = ch.Publish(
			"",
			"simpleMesTest",
			true,
			false,
			client.Publishing{
				Body : body,
				DeliveryMode : 2,
				Priority : 0,
			})
		if err != nil {
			return
		}
		cnt++
	}
	
	wgg.Wait()
	fmt.Println("publish confirm" , num, "messages costs:", time.Since(start))
	exp, pub := ch.GetExpAndPubConfirmTag()
	fmt.Println("expected:", exp, "published", pub)

	ch.Close()
	conn.Close()
}