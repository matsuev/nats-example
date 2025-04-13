package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/matsuev/nats-example/internal/helper"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	NATSAddr  = "127.0.0.1:4222"
	NATSToken = "nats_secret_token"
)

func main() {
	connOpts := []nats.Option{
		nats.Token(NATSToken),
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

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalln(err)
	}

	if err := helper.CreateStream(context.Background(), js, jetstream.StreamConfig{
		Name: "CENTRIFUGO",
		Subjects: []string{
			"API.publish",
		},
		Storage:   jetstream.MemoryStorage,
		Retention: jetstream.WorkQueuePolicy,
	}); err != nil {
		log.Fatalln(err)
	}

	data, _ := json.Marshal(map[string]any{
		"channel": "test",
		"data": map[string]any{
			"value": "test_value",
		},
	})

	var wg sync.WaitGroup

	ts := time.Now()

	for range 2 {
		wg.Add(1)
		go func() {
			for range 5 {
				opts := []jetstream.PublishOpt{
					// jetstream.WithMsgID("100500"),
				}

				if _, err := js.Publish(context.Background(), "API.publish", data, opts...); err != nil {
					log.Println(err)
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()

	fmt.Println(time.Since(ts))

}
