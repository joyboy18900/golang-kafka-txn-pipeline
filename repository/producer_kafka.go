package repository

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type producerRepositoryKafka struct {
	writer *kafka.Writer
}

func NewProducerRepositoryKafka(brokers []string, topic string) ProducerRepository {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.Hash{},
	}

	return producerRepositoryKafka{writer: writer}
}

func (r producerRepositoryKafka) Publish(ctx context.Context, key string, value []byte) error {
	msg := kafka.Message{Key: []byte(key), Value: value}
	if err := r.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("publish message: %w", err)
	}

	return nil
}

func (r producerRepositoryKafka) Close() error {
	return r.writer.Close()
}
