package repository

import (
	"context"
	"time"
)

type Transaction struct {
	ID          int64
	EventID     string
	AccountID   string
	Type        string
	AmountCents int64
	Currency    string
	Status      string
	OccurredAt  time.Time
	CreatedAt   time.Time
}

type ApplyEventParams struct {
	EventID     string
	AccountID   string
	Type        string
	AmountCents int64
	Currency    string
	Status      string
	OccurredAt  time.Time
}

type AccountBalance struct {
	AccountID         string
	BalanceCents      int64
	AppliedEventCount int64
	LastEventAt       time.Time
}

type ListTransactionsParams struct {
	AccountID string
	Page      int
	Limit     int
}

type ListTransactionsResult struct {
	Transactions []Transaction
	TotalItems   int64
}

//go:generate go tool mockgen -destination=../mock/mock_repository/transaction.go golang-kafka-txn-pipeline/repository TransactionRepository
type TransactionRepository interface {
	ApplyEvent(ctx context.Context, params ApplyEventParams) error
	ListTransactions(ctx context.Context, params ListTransactionsParams) (ListTransactionsResult, error)
	GetAccountBalance(ctx context.Context, accountID string) (*AccountBalance, error)
}
