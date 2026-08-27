package service

import (
	"context"
	"time"

	"golang-kafka-txn-pipeline/repository"
)

type TransactionEvent struct {
	EventID     string    `json:"event_id"`
	AccountID   string    `json:"account_id"`
	Type        string    `json:"type"`
	AmountCents int64     `json:"amount_cents"`
	Currency    string    `json:"currency"`
	Status      string    `json:"status"`
	OccurredAt  time.Time `json:"occurred_at"`
}

//go:generate go tool mockgen -destination=../mock/mock_service/event.go golang-kafka-txn-pipeline/service EventService
type EventService interface {
	ProcessMessage(ctx context.Context, msg repository.Message) error
	RunConsumer(ctx context.Context) error
}
