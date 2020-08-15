package test

import (
	"client/client"
	"fmt"
	"time"
	// "math"
	// "sync/atomic"
	// "strconv"
)

func PriorityMesTest(ch *client.Channel, mes []byte, num int) error {
	if err := ch.Confirm(false); err != nil {
		return err
	}
	cnt := 0
	for {
		if cnt == num {
			break
		}
		err := ch.Publish(
			"",
			"simpleMesTest",
			true,
			false,
			client.Publishing{
				Body : mes,
				DeliveryMode : 1,
				Priority : 2,
			})
		if err != nil {
			return err
		}
		cnt++
	}
	fmt.Println("priority 2 has sent")
	time.Sleep(500*time.Millisecond)
	for i := 0; i < num; i++ {
		err := ch.Publish(
			"",
			"simpleMesTest",
			true,
			false,
			client.Publishing{
				Body : mes,
				DeliveryMode : 1,
				Priority : 1,
			})
		if err != nil {
			return err
		}
	}
	fmt.Println("priority 1 has sent")
	time.Sleep(500*time.Millisecond)

	for i := 0; i < num; i++ {
		err := ch.Publish(
			"",
			"simpleMesTest",
			true,
			false,
			client.Publishing{
				Body : mes,
				DeliveryMode : 1,
				Priority : 0,
			})
		if err != nil {
			return err
		}
	}
	fmt.Println("priority 0 has sent")
	time.Sleep(500*time.Millisecond)

	return nil
}