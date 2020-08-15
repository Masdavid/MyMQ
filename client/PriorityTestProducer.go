package main 

import (
	"client/client"
	"client/test"
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

	body := make([]byte, 1024)
	for id, _ := range body {
		body[id] = 'T'
	}

	ch, err := conn.Channel()
	if err != nil {
		return
	}
	test.PriorityMesTest(ch, body, 20)
	ch.Close()
	conn.Close()
}