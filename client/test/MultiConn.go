package test

import (
	"client/client"
	"sync"
	"fmt"
	"time"
	"strconv"
)

var wg sync.WaitGroup

func MultiProducer(u client.URI, body []byte, num int) error {
	for i := 1; i <= 5; i++{
		wg.Add(1)
		go createConn(u, i, body, num)
	}
	wg.Wait()
	return nil
}

func MultiConsume(u client.URI, target int) error {
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go createConsumer(u, i, target)
	}
	wg.Wait()
	return nil
}

func MultiConsumeInOneConn(u client.URI, target int) error {
	conn, err := client.Dial(u)
	if err != nil {
		return err
	}
	for i := 1; i <= 2; i++ {
		ch, err := conn.Channel()
		if err != nil {
			return err
		}
		wg.Add(1)
		go startConsume(ch, i, target)
	}
	wg.Wait()
	return nil
}

func createConn(u client.URI, id int, b []byte, num int) error {
	defer wg.Done()
	conn, err := client.Dial(u)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	if err := ch.Confirm(false); err != nil {
		return err
	}

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
				Body : b,
				// DeliveryMode : 2,
				Priority : 0,
			})
		if err != nil {
			return err
		}
		cnt++
		// time.Sleep(time.Second)
	}
	
	wgg.Wait()
	fmt.Println("conn", id, "publish confirm" , num, "messages costs:", time.Since(start))
	exp, pub := ch.GetExpAndPubConfirmTag()
	fmt.Println("expected:", exp, "published", pub)

	ch.Close()
	conn.Close()

	return nil
}

func createConsumer(u client.URI, id int, target int) error {
	defer wg.Done()
	conn, err := client.Dial(u)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	var quname string = "queNo." + strconv.FormatInt(int64(id), 10)
	var cmrname string = "cmrNo." + strconv.FormatInt(int64(id), 10)
	_, err = ch.QueueDeclare(
		quname,
		false,
		true,
		false,
		false,
	)
	if err != nil {
		return err
	}
	err = ch.QueueBind(
		quname,
		quname,
		"pubsubEx",
		false,
	)
	if err != nil {
		return err
	}

	msgch, err := ch.Consume(
		quname,
		cmrname,
		false,
		false,
		false,
		false,
	)
	if err != nil {
		return err
	}
	
	var st time.Time
	// var part uint64 = uint64(target/10)
	for msg := range msgch {
		if msg.DeliveryTag == 1{
			st = time.Now()
		}
		if msg.DeliveryTag % uint64(target) == 0 {
			fmt.Println("delivertTag", msg.DeliveryTag, "of consumer:", cmrname)
		}
		msg.Ack(false)
		if msg.DeliveryTag == uint64(target){
			break
		}
	}
	fmt.Println("consumer", cmrname, "receive", target ,"message cost:", time.Since(st))
	return nil
}

func startConsume(ch *client.Channel, id int, target int) error{
	defer wg.Done()

	var quname string = "queNo." + strconv.FormatInt(int64(id), 10)
	var cmrname string = "cmrNo." + strconv.FormatInt(int64(id), 10)
	_, err := ch.QueueDeclare(
		quname,
		false,
		true,
		false,
		false,
	)
	if err != nil {
		return err
	}
	err = ch.QueueBind(
		quname,
		quname,
		"pubsubEx",
		false,
	)
	if err != nil {
		return err
	}

	msgch, err := ch.Consume(
		quname,
		cmrname,
		false,
		false,
		false,
		false,
	)
	if err != nil {
		return err
	}
	
	// var st time.Time
	var part uint64 = uint64(target/10)
	for msg := range msgch {
		// if msg.DeliveryTag == 1{
		// 	st = time.Now()
		// }
		if msg.DeliveryTag % part == 0 {
			fmt.Println("delivertTag", msg.DeliveryTag, "of consumer:", cmrname)
		}
		msg.Ack(false)
		if msg.DeliveryTag == uint64(target){
			break
		}
	}
	// fmt.Println("consumer", cmrname, "receive", target ,"message cost:", time.Since(st))
	return nil

}
