package service

import (
	"context"
	"time"
)

type TransactionResponse struct {
	EventID     string    `json:"event_id"`
	AccountID   string    `json:"account_id"`
	Type        string    `json:"type"`
	AmountCents int64     `json:"amount_cents"`
	Currency    string    `json:"currency"`
	Status      string    `json:"status"`
	OccurredAt  time.Time `json:"occurred_at"`
}

type Pagination struct {
	Page       int   `json:"page"`
	Limit      int   `json:"limit"`
	TotalItems int64 `json:"total_items"`
	TotalPages int   `json:"total_pages"`
}

type ListTransactionsRequest struct {
	AccountID string
	Page      int
	Limit     int
}

type ListTransactionsResponse struct {
	Data       []TransactionResponse `json:"data"`
	Pagination Pagination            `json:"pagination"`
}

type AccountBalanceResponse struct {
	AccountID         string    `json:"account_id"`
	BalanceCents      int64     `json:"balance_cents"`
	AppliedEventCount int64     `json:"applied_event_count"`
	LastEventAt       time.Time `json:"last_event_at"`
}

type DeadLetterEventResponse struct {
	ID            int64     `json:"id"`
	EventID       *string   `json:"event_id"`
	Topic         string    `json:"topic"`
	FailureReason string    `json:"failure_reason"`
	AttemptCount  int       `json:"attempt_count"`
	FailedAt      time.Time `json:"failed_at"`
}

type ListDeadLetterEventsResponse struct {
	Data       []DeadLetterEventResponse `json:"data"`
	Pagination Pagination                `json:"pagination"`
}

//go:generate go tool mockgen -destination=../mock/mock_service/transaction.go golang-kafka-txn-pipeline/service TransactionService
type TransactionService interface {
	List(ctx context.Context, req ListTransactionsRequest) (*ListTransactionsResponse, error)
	GetBalance(ctx context.Context, accountID string) (*AccountBalanceResponse, error)
	ListDeadLetterEvents(ctx context.Context, page, limit int) (*ListDeadLetterEventsResponse, error)
}
