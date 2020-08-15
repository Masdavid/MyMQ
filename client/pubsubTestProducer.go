package main

import (
	"client/client"
	"fmt"
	"sync"
	"time"
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
	// err = ch.ExchangeDeclare(
	// 	"pubsubEx",
	// 	"fanout",
	// 	true,
	// 	false,
	// )
	// if err != nil {
	// 	fmt.Println("exchange declare failed")
	// 	return 
	// }


	if err := ch.Confirm(false); err != nil {
		return
	}

	var num int = 50000
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
			"pubsubEx",
			"whatever",
			true,
			false,
			client.Publishing{
				Body : body,
				// DeliveryMode : 2,
				Priority : 0,
			})
		if err != nil {
			return
		}
		cnt++
		// time.Sleep(time.Second)
	}
	
	wgg.Wait()
	fmt.Println("publish confirm" , num, "messages costs:", time.Since(start))
	exp, pub := ch.GetExpAndPubConfirmTag()
	fmt.Println("expected:", exp, "published", pub)

	ch.Close()
	conn.Close()
}