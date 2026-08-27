package repository

import "context"

type Message struct {
	Key            []byte
	Value          []byte
	Topic          string
	KafkaPartition int
	Offset         int64
}

//go:generate go tool mockgen -destination=../mock/mock_repository/consumer.go golang-kafka-txn-pipeline/repository ConsumerRepository
type ConsumerRepository interface {
	FetchMessage(ctx context.Context) (Message, error)
	CommitMessage(ctx context.Context, msg Message) error
	Close() error
}
