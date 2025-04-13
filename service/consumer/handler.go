package main

import (
	"fmt"
	"log"

	"github.com/nats-io/nats.go/jetstream"
)

// messageHandler ...
func messageHandler() jetstream.MessageHandler {
	return func(msg jetstream.Msg) {
		if err := msg.Ack(); err != nil {
			log.Println(err)
		}

		fmt.Println(string(msg.Data()))
	}
}
