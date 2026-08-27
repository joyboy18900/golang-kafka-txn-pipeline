package repository

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type consumerRepositoryKafka struct {
	reader *kafka.Reader
}

func NewConsumerRepositoryKafka(brokers []string, topic, groupID string, startOffset int64) ConsumerRepository {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		StartOffset:    startOffset,
		CommitInterval: 0,
	})

	return consumerRepositoryKafka{reader: reader}
}

func (r consumerRepositoryKafka) FetchMessage(ctx context.Context) (Message, error) {
	msg, err := r.reader.FetchMessage(ctx)
	if err != nil {
		return Message{}, fmt.Errorf("fetch message: %w", err)
	}

	return Message{
		Key:            msg.Key,
		Value:          msg.Value,
		Topic:          msg.Topic,
		KafkaPartition: msg.Partition,
		Offset:         msg.Offset,
	}, nil
}

func (r consumerRepositoryKafka) CommitMessage(ctx context.Context, msg Message) error {
	kafkaMsg := kafka.Message{
		Topic:     msg.Topic,
		Partition: msg.KafkaPartition,
		Offset:    msg.Offset,
	}

	if err := r.reader.CommitMessages(ctx, kafkaMsg); err != nil {
		return fmt.Errorf("commit message: %w", err)
	}

	return nil
}

func (r consumerRepositoryKafka) Close() error {
	return r.reader.Close()
}
