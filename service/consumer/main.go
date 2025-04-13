package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	NATSAddr  = "127.0.0.1:4222"
	NATSToken = "nats_secret_token"
)

// ICentrifugoAsyncConsumer
type ICentrifugoAsyncConsumer interface {
	OnConnect(conn *nats.Conn)
	OnDisconnect(conn *nats.Conn, err error)
	OnReconnect(conn *nats.Conn)
	OnReconnectError(conn *nats.Conn, err error)
}

func main() {
	cfg := ConsumerConfig{
		StreamName: "CENTRIFUGO",
		// CreateStream: true,
		// StreamSubjects: []string{
		// 	"API.publish",
		// 	"API.broadcast",
		// 	"API.subscribe",
		// 	"API.disconnect",
		// },
		ConsumerName:     "CF_API_PUBLISH",
		SubjectName:      "API.publish",
		ReconnectTimeout: time.Second,
		Handler:          messageHandler(),
	}

	consumer, err := NewConsumer(cfg)
	if err != nil {
		log.Fatalln(err)
	}

	connOpts := []nats.Option{
		nats.Token(NATSToken),
		nats.MaxReconnects(-1),

		nats.ConnectHandler(consumer.OnConnect),
		nats.DisconnectErrHandler(consumer.OnDisconnect),
		nats.ReconnectHandler(consumer.OnReconnect),
		nats.ReconnectErrHandler(consumer.OnReconnectError),
	}

	nc, err := nats.Connect(NATSAddr, connOpts...)
	if err != nil {
		log.Fatalln("nats connect error:", err)
	}
	defer func() {
		if err := nc.Drain(); err != nil && !errors.Is(err, nats.ErrConnectionReconnecting) {
			log.Println("nats drain error:", err)
		}
	}()

	stopCtx, stopCancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stopCancel()

	<-stopCtx.Done()
}
