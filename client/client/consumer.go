package client

import (
	"os"
	"strconv"
	"sync"
	"sync/atomic"
)

var consumerSeq uint64

const consumerTagLengthMax = 0xFF // see writeShortstr

func getConsumerTag() string {
	return generateConsumerTag(os.Args[0])
}

func generateConsumerTag(commandName string) string {
	tagPrefix := "ctag-"
	tagInfix := commandName
	tagSuffix := "-" + strconv.FormatUint(atomic.AddUint64(&consumerSeq, 1), 10)

	if len(tagPrefix)+len(tagInfix)+len(tagSuffix) > consumerTagLengthMax {
		tagInfix = "dmqp"
	}

	return tagPrefix + tagInfix + tagSuffix
}

type consumers struct {
	sync.WaitGroup               // one for buffer
	closed         chan struct{} // signal buffer
	sync.Mutex // protects below
	chans      map[string]chan *Delivery
}

func makeConsumers() *consumers {
	return &consumers{
		closed: make(chan struct{}),
		chans:  make(map[string]chan *Delivery),   //consumer的输入管道的map
	}
}

func (subs *consumers) buffer(in chan *Delivery, out chan Delivery) {
	defer close(out)
	defer subs.Done()

	var inflight = in
	var queue []*Delivery

	//等待in 管道输入信息delivery
	for delivery := range in {
		queue = append(queue, delivery)

		for len(queue) > 0 {
			select {
			case <-subs.closed:
				return

			case delivery, consuming := <-inflight:
				if consuming {
					queue = append(queue, delivery)
				} else {
					inflight = nil
				}
			//将传入信息放进queue中，并且按顺序送进out管道
			case out <- *queue[0]:
				queue = queue[1:]
			}
		}
	}
}

// 向consusmer添加一个新的通道，并且名字与发送给broker的consumer一致。
//如果有重名的，关闭之前的通道，然后新开一个线程
func (subs *consumers) add(tag string, consumer chan Delivery) {
	subs.Lock()
	defer subs.Unlock()

	if prev, found := subs.chans[tag]; found {
		close(prev)
	}

	//新建一个输入管道in，放入consumer的chans中，并且传入buffer函数中
	in := make(chan *Delivery)
	subs.chans[tag] = in

	subs.Add(1)
	go subs.buffer(in, consumer)
}

func (subs *consumers) cancel(tag string) (found bool) {
	subs.Lock()
	defer subs.Unlock()

	ch, found := subs.chans[tag]

	if found {
		delete(subs.chans, tag)
		close(ch)
	}

	return found
}

func (subs *consumers) close() {
	subs.Lock()
	defer subs.Unlock()

	close(subs.closed)

	for tag, ch := range subs.chans {
		delete(subs.chans, tag)
		close(ch)
	}

	subs.Wait()
}

//根据tag将消息送往consumer的输入管道，管道是同步的
func (subs *consumers) send(tag string, msg *Delivery) bool {
	subs.Lock()
	defer subs.Unlock()

	buffer, found := subs.chans[tag]
	if found {
		buffer <- msg
	}

	return found
}
