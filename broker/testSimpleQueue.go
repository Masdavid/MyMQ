package main

import (
	"broker/basicQueue"
	"broker/dmqp"
	"fmt"
	"sync"
	"time"
)

var que *basicQueue.SimpleQueue
var closeCh chan bool
var wg sync.WaitGroup

//多线程读写测试
func main() {
	que = basicQueue.NewSimpleQueue(10000)
	closeCh = make(chan bool, 10)


	for i := 0; i < 5; i++{
		wg.Add(1)
		go pushMessage()
	}

	for i := 0; i < 5; i++{
		wg.Add(1)
		go consumeMessage()
	}

	time.Sleep(5*time.Second)
	for i:= 0; i < 10; i++{
		closeCh <- true
	}
	wg.Wait()
	return
}

func pushMessage() {
	defer func() {
		fmt.Println("pushMessage closed")
		wg.Done()	
	}()
	var id uint64
	for {
		select {
		case <- closeCh:
			return
		default:
			in := &dmqp.Message{
				ID : id,
			}
			que.Push(in)

			id++
			time.Sleep(200*time.Millisecond)
		}
	}
}

func consumeMessage() {
	defer func() {
		fmt.Println("consumerMessage closed")
		wg.Done()	
	}()
	for {
		select {
		case <- closeCh:
			return
		default:
			out := que.Pop()
			if out == nil{
				continue
			}
			fmt.Println("id of message pop by queue: ", out.ID)
		}
	}
}



