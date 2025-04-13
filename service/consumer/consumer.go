package main

import (
	"context"
	"errors"
	"io"
	"log"
	"time"

	"github.com/matsuev/nats-example/internal/helper"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ConsumerConfig ...
type ConsumerConfig struct {
	StreamName       string
	StreamSubjects   []string
	ConsumerName     string
	SubjectName      string
	ReconnectTimeout time.Duration
	CreateStream     bool
	Handler          jetstream.MessageHandler
}

// Consumer ...
type Consumer struct {
	cfg     ConsumerConfig
	ctx     context.Context
	cancel  context.CancelFunc
	js      jetstream.JetStream
	cons    jetstream.Consumer
	consCtx jetstream.ConsumeContext
}

// NewConsumer ...
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	ctx, cancel := context.WithCancel(context.Background())

	return &Consumer{
		cfg:    cfg,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// OnConnect ...
func (c *Consumer) OnConnect(conn *nats.Conn) {
	logPrefix := "nats consumer: onConnect:"

	log.Println(logPrefix, "connected")

	var err error

	if c.js, err = jetstream.New(conn); err != nil {
		log.Println(logPrefix, "jetstream error:", err)
		return
	}

	if c.cfg.CreateStream {
		if err := helper.CreateStream(context.Background(), c.js, jetstream.StreamConfig{
			Name:      c.cfg.StreamName,
			Subjects:  c.cfg.StreamSubjects,
			Storage:   jetstream.MemoryStorage,
			Retention: jetstream.WorkQueuePolicy,
		}); err != nil {
			log.Println(logPrefix, "stream create error:", err)
		}
	}

	if c.consCtx == nil {
		c.createConsumer(logPrefix)
	}
}

// OnDisconnect ...
func (c *Consumer) OnDisconnect(conn *nats.Conn, err error) {
	logPrefix := "nats consumer: onDisconnect:"

	if err != nil && !errors.Is(err, io.EOF) {
		log.Println(logPrefix, "disconnect with error:", err)
	}

	if c.consCtx != nil {
		c.consCtx.Drain()
	}
}

// OnReconnect ...
func (c *Consumer) OnReconnect(conn *nats.Conn) {
	logPrefix := "nats consumer: onReconnect:"

	if c.consCtx != nil {
		<-c.consCtx.Closed()
	}

	c.createConsumer(logPrefix)
}

// OnReconnectError ...
func (c *Consumer) OnReconnectError(conn *nats.Conn, err error) {
	logPrefix := "nats consumer: onReconnect:"

	log.Println(logPrefix, "error:", err)
}

// createConsumer ...
func (c *Consumer) createConsumer(logPrefix string) {
	var err error

	if c.cons, err = c.js.CreateOrUpdateConsumer(context.Background(), c.cfg.StreamName, jetstream.ConsumerConfig{
		Name:          c.cfg.ConsumerName,
		Durable:       c.cfg.ConsumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: c.cfg.SubjectName,
	}); err != nil {
		log.Println(logPrefix, "create consumer error:", err)

		time.AfterFunc(c.cfg.ReconnectTimeout, func() {
			_ = c.js.Conn().ForceReconnect()
		})
		return
	}

	if c.consCtx, err = c.cons.Consume(c.cfg.Handler); err != nil {
		log.Println(logPrefix, "consume error:", err)
		return
	}

	log.Println(logPrefix, "consumer created:", c.cfg.ConsumerName, c.cfg.SubjectName)
}
