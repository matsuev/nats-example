package helper

import (
	"context"
	"errors"
	"log"

	"github.com/nats-io/nats.go/jetstream"
)

// CreateStream ...
func CreateStream(ctx context.Context, js jetstream.JetStream, cfg jetstream.StreamConfig) error {
	if _, err := js.Stream(ctx, cfg.Name); errors.Is(err, jetstream.ErrStreamNotFound) {
		if _, err := js.CreateOrUpdateStream(ctx, cfg); err != nil {
			return err
		} else {
			log.Println("stream created:", cfg.Name)
		}
	} else {
		log.Println("stream opened:", cfg.Name)
	}

	return nil
}
