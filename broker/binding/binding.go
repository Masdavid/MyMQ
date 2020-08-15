package binding

import (
	"bytes"
	// "fmt"
	"strings"

	"broker/dmqp"
)


type Binding struct {
	Queue      string
	Exchange   string
	RoutingKey string
}

func NewBinding(queue string, exchange string, routingKey string) (*Binding, error) {
	binding := &Binding{
		Queue:      queue,
		Exchange:   exchange,
		RoutingKey: routingKey,
	}
	return binding, nil
}

func (b *Binding) MatchDirect(exchange string, routingKey string) bool {
	return b.Exchange == exchange && b.RoutingKey == routingKey
}

func (b *Binding) MatchFanout(exchange string) bool {
	return b.Exchange == exchange
}

func (b *Binding) GetExchange() string {
	return b.Exchange
}

func (b *Binding) GetRoutingKey() string {
	return b.RoutingKey
}

func (b *Binding) GetQueue() string {
	return b.Queue
}

func (b *Binding) Equal(bind *Binding) bool {
	return b.Exchange == bind.GetExchange() &&
		b.Queue == bind.GetQueue() &&
		b.RoutingKey == bind.GetRoutingKey()
}

func (b *Binding) GetName() string {
	return strings.Join(
		[]string{b.Queue, b.Exchange, b.RoutingKey},
		"_",
	)
}

func (b *Binding) Serialize() (data []byte, err error) {
	buf := bytes.NewBuffer([]byte{})
	if err = dmqp.WriteShortstr(buf, b.Queue); err != nil {
		return nil, err
	}
	if err = dmqp.WriteShortstr(buf, b.Exchange); err != nil {
		return nil, err
	}
	if err = dmqp.WriteShortstr(buf, b.RoutingKey); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (b *Binding) Deserialize(data []byte) (err error) {
	buf := bytes.NewReader(data)
	if b.Queue, err = dmqp.ReadShortstr(buf); err != nil {
		return err
	}
	if b.Exchange, err = dmqp.ReadShortstr(buf); err != nil {
		return err
	}
	if b.RoutingKey, err = dmqp.ReadShortstr(buf); err != nil {
		return err
	}
	return
} 

