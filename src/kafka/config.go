package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

var writer *kafka.Writer

// Configure ... Initializes the kafka client
func Configure(kafkaBrokerUrls []string, topic string) (w *kafka.Writer, err error) {
	config := kafka.WriterConfig{
		Brokers:  kafkaBrokerUrls,
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	w = kafka.NewWriter(config)
	writer = w
	return w, nil
}

// Push .. Publishes a kafka message
func Push(parent context.Context, key, value []byte) (err error) {
	message := kafka.Message{
		Key:   key,
		Value: value,
		Time:  time.Now(),
	}
	return writer.WriteMessages(parent, message)
}
