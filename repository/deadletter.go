package repository

import (
	"context"
	"time"
)

type DeadLetterEvent struct {
	ID             int64
	EventID        *string
	Topic          string
	KafkaPartition int
	KafkaOffset    int64
	Payload        []byte
	FailureReason  string
	AttemptCount   int
	FailedAt       time.Time
}

//go:generate go tool mockgen -destination=../mock/mock_repository/deadletter.go golang-kafka-txn-pipeline/repository DeadLetterRepository
type DeadLetterRepository interface {
	Insert(ctx context.Context, event DeadLetterEvent) error
	List(ctx context.Context, page, limit int) ([]DeadLetterEvent, int64, error)
}
