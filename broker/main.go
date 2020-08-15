package main

import (
	"fmt"
	"os"
	"broker/config"
	"broker/server"
)

func main() {
	cfg, err := config.CreateFromFile("config.yaml")

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	srv := server.NewServer(cfg.TCP.IP, cfg.TCP.Port, cfg)
	srv.Start()
}