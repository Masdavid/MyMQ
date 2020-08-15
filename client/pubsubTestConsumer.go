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
	test.MultiConsume(uri, 50000)
	// test.MultiConsumeInOneConn(uri, 50000)
	return 
}