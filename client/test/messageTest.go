package test

import (
	"client/client"
	"fmt"
	"time"
	// "math"
	// "sync/atomic"
	// "strconv"
	"sync"
)

func SimpleMessageTest(ch *client.Channel, mes []byte, num int) error {
	que, err := ch.QueueDeclare(
		"simpleMesTest",
		true,
		false,
		false,
		false,
	)
	if err != nil {
		return err
	}

	if err := ch.Confirm(false); err != nil {
		return err
	}

	conCh := ch.NotifyPublish(make(chan client.Confirmation, 1))
	var wg sync.WaitGroup
	wg.Add(1)
	go func () {
		defer wg.Done()
		for cmsg := range conCh {
			// fmt.Println(cmsg.DeliveryTag)
			if cmsg.DeliveryTag == uint64(num) {
				break
			}
		}
	}()

	start := time.Now()
	cnt := 0
	for{
		if cnt == num {
			break
		}
		// exp, pub := ch.GetExpAndPubConfirmTag()
		// if exp < pub && exp + 49 < pub {
		// 	// fmt.Println("slide window too big: ", exp, " to ", pub)
		// 	continue
		// }

		err = ch.Publish(
			"",
			que.Name,
			true,
			false,
			client.Publishing{
				Body : mes,
				DeliveryMode : 2,
				Priority : 0,
			})
		if err != nil {
			return err
		}
		cnt++
		// fmt.Println("cur cnt", cnt)

	}
	
	wg.Wait()
	fmt.Println("publish confirm ", num, " messages costs time :", time.Since(start))

	return nil
}
