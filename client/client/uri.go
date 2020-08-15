package client

import (
	// "strings"
	"errors"
)

var errURIwp = errors.New("uri can't has whitespace")

var defaultURI = URI{
	Scheme : "dmqp",
	Host: "localhost",
	Port: "9833",
	Vhost : "/",
}

type URI struct {
	Scheme string
	Host string
	Port string
	Vhost string
}
