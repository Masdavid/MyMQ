package client

type Return struct {
	ReplyCode  uint16 // reason
	ReplyText  string // description
	Exchange   string // basic.publish exchange
	RoutingKey string // basic.publish routing key

	DeliveryMode    byte     // queue implementation use - non-persistent (1) or persistent (2)
	Priority byte

	Body []byte
}

func newReturn(msg basicReturn) *Return {
	props, prior, body := msg.getContent()

	return &Return{
		ReplyCode:  msg.ReplyCode,
		ReplyText:  msg.ReplyText,
		Exchange:   msg.Exchange,
		RoutingKey: msg.RoutingKey,

		DeliveryMode:    props,
		Priority : prior,
		Body: body,
	}
}