package client

import (
	"errors"
	// "time"
)

var errOnDelivery = errors.New("delivery not initialized")


type Acknowledger interface {
	Ack(tag uint64, multiple bool) error
	Nack(tag uint64, multiple bool, requeue bool) error
	Reject(tag uint64, requeue bool) error
}


type Delivery struct {
	Acknowledger Acknowledger // the channel from which this delivery arrived

	DeliveryMode    byte     // queue implementation use - non-persistent (1) or persistent (2)
	Priority byte

	ConsumerTag string

	MessageCount uint32

	DeliveryTag uint64
	Redelivered bool
	Exchange    string // basic.publish exchange
	RoutingKey  string // basic.publish routing key

	Body []byte
}

func newDelivery(channel *Channel, msg messageWithContent) *Delivery {
	dem, prior, body := msg.getContent()

	delivery := Delivery{
		Acknowledger: channel,

		DeliveryMode:    dem,
		Priority : prior,
		Body: body,
	}

	switch m := msg.(type) {
	case *basicDeliver:
		delivery.ConsumerTag = m.ConsumerTag
		delivery.DeliveryTag = m.DeliveryTag
		delivery.Redelivered = m.Redelivered
		delivery.Exchange = m.Exchange
		delivery.RoutingKey = m.RoutingKey
	}

	return &delivery
}

func (d Delivery) Ack(multiple bool) error {
	if d.Acknowledger == nil {
		return errOnDelivery
	}
	return d.Acknowledger.Ack(d.DeliveryTag, multiple)
}


func (d Delivery) Reject(requeue bool) error {
	if d.Acknowledger == nil {
		return errOnDelivery
	}
	return d.Acknowledger.Reject(d.DeliveryTag, requeue)
}



func (d Delivery) Nack(multiple, requeue bool) error {
	if d.Acknowledger == nil {
		return errOnDelivery
	}
	return d.Acknowledger.Nack(d.DeliveryTag, multiple, requeue)
}
