package repository

import "context"

//go:generate go tool mockgen -destination=../mock/mock_repository/producer.go golang-kafka-txn-pipeline/repository ProducerRepository
type ProducerRepository interface {
	Publish(ctx context.Context, key string, value []byte) error
	Close() error
}
