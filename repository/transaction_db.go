package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type transactionRow struct {
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

func (transactionRow) TableName() string {
	return "transactions"
}

type accountBalanceRow struct {
	AccountID         string
	BalanceCents      int64
	AppliedEventCount int64
	LastEventAt       time.Time
}

func (accountBalanceRow) TableName() string {
	return "account_balances"
}

type transactionRepositoryDB struct {
	db *gorm.DB
}

func NewTransactionRepositoryDB(db *gorm.DB) TransactionRepository {
	return transactionRepositoryDB{db: db}
}

func (r transactionRepositoryDB) ApplyEvent(ctx context.Context, p ApplyEventParams) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row := transactionRow{
			EventID:     p.EventID,
			AccountID:   p.AccountID,
			Type:        p.Type,
			AmountCents: p.AmountCents,
			Currency:    p.Currency,
			Status:      p.Status,
			OccurredAt:  p.OccurredAt,
		}
		res := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "event_id"}},
			DoNothing: true,
		}).Create(&row)
		if res.Error != nil {
			return fmt.Errorf("insert transaction: %w", res.Error)
		}
		if res.RowsAffected == 0 {
			return nil
		}

		delta := p.AmountCents
		if p.Type == "debit" {
			delta = -p.AmountCents
		}

		if err := tx.Exec(`
			INSERT INTO account_balances (account_id, balance_cents, applied_event_count, last_event_at)
			VALUES (?, ?, 1, ?)
			ON CONFLICT (account_id) DO UPDATE SET
				balance_cents = account_balances.balance_cents + EXCLUDED.balance_cents,
				applied_event_count = account_balances.applied_event_count + 1,
				last_event_at = EXCLUDED.last_event_at
		`, p.AccountID, delta, p.OccurredAt).Error; err != nil {
			return fmt.Errorf("upsert account balance: %w", err)
		}

		return nil
	})
}

func (r transactionRepositoryDB) ListTransactions(ctx context.Context, params ListTransactionsParams) (ListTransactionsResult, error) {
	query := r.db.WithContext(ctx).Model(&transactionRow{})
	if params.AccountID != "" {
		query = query.Where("account_id = ?", params.AccountID)
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return ListTransactionsResult{}, fmt.Errorf("count transactions: %w", err)
	}

	offset := (params.Page - 1) * params.Limit
	var rows []transactionRow
	if err := query.Order("occurred_at DESC, id DESC").
		Offset(offset).Limit(params.Limit).
		Find(&rows).Error; err != nil {
		return ListTransactionsResult{}, fmt.Errorf("list transactions: %w", err)
	}

	transactions := make([]Transaction, len(rows))
	for i, row := range rows {
		transactions[i] = toTransaction(row)
	}

	return ListTransactionsResult{Transactions: transactions, TotalItems: total}, nil
}

func (r transactionRepositoryDB) GetAccountBalance(ctx context.Context, accountID string) (*AccountBalance, error) {
	var row accountBalanceRow
	err := r.db.WithContext(ctx).Where("account_id = ?", accountID).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get account balance: %w", err)
	}

	return &AccountBalance{
		AccountID:         row.AccountID,
		BalanceCents:      row.BalanceCents,
		AppliedEventCount: row.AppliedEventCount,
		LastEventAt:       row.LastEventAt,
	}, nil
}

func toTransaction(row transactionRow) Transaction {
	return Transaction{
		ID:          row.ID,
		EventID:     row.EventID,
		AccountID:   row.AccountID,
		Type:        row.Type,
		AmountCents: row.AmountCents,
		Currency:    row.Currency,
		Status:      row.Status,
		OccurredAt:  row.OccurredAt,
		CreatedAt:   row.CreatedAt,
	}
}
