package service

import (
	"context"

	"golang-kafka-txn-pipeline/errs"
	"golang-kafka-txn-pipeline/logs"
	"golang-kafka-txn-pipeline/repository"
)

const defaultListLimit = 20

type transactionService struct {
	txRepo  repository.TransactionRepository
	dlqRepo repository.DeadLetterRepository
}

func NewTransactionService(txRepo repository.TransactionRepository, dlqRepo repository.DeadLetterRepository) TransactionService {
	return transactionService{txRepo: txRepo, dlqRepo: dlqRepo}
}

func (s transactionService) List(ctx context.Context, req ListTransactionsRequest) (*ListTransactionsResponse, error) {
	page, limit := normalizePage(req.Page), normalizeLimit(req.Limit)

	result, err := s.txRepo.ListTransactions(ctx, repository.ListTransactionsParams{
		AccountID: req.AccountID,
		Page:      page,
		Limit:     limit,
	})
	if err != nil {
		logs.Error(err)
		return nil, errs.NewUnexpectedError()
	}

	data := make([]TransactionResponse, len(result.Transactions))
	for i, t := range result.Transactions {
		data[i] = TransactionResponse{
			EventID:     t.EventID,
			AccountID:   t.AccountID,
			Type:        t.Type,
			AmountCents: t.AmountCents,
			Currency:    t.Currency,
			Status:      t.Status,
			OccurredAt:  t.OccurredAt,
		}
	}

	return &ListTransactionsResponse{
		Data:       data,
		Pagination: buildPagination(page, limit, result.TotalItems),
	}, nil
}

func (s transactionService) GetBalance(ctx context.Context, accountID string) (*AccountBalanceResponse, error) {
	balance, err := s.txRepo.GetAccountBalance(ctx, accountID)
	if err != nil {
		logs.Error(err)
		return nil, errs.NewUnexpectedError()
	}
	if balance == nil {
		return nil, errs.NewNotFoundError("account not found")
	}

	return &AccountBalanceResponse{
		AccountID:         balance.AccountID,
		BalanceCents:      balance.BalanceCents,
		AppliedEventCount: balance.AppliedEventCount,
		LastEventAt:       balance.LastEventAt,
	}, nil
}

func (s transactionService) ListDeadLetterEvents(ctx context.Context, page, limit int) (*ListDeadLetterEventsResponse, error) {
	page, limit = normalizePage(page), normalizeLimit(limit)

	events, total, err := s.dlqRepo.List(ctx, page, limit)
	if err != nil {
		logs.Error(err)
		return nil, errs.NewUnexpectedError()
	}

	data := make([]DeadLetterEventResponse, len(events))
	for i, e := range events {
		data[i] = DeadLetterEventResponse{
			ID:            e.ID,
			EventID:       e.EventID,
			Topic:         e.Topic,
			FailureReason: e.FailureReason,
			AttemptCount:  e.AttemptCount,
			FailedAt:      e.FailedAt,
		}
	}

	return &ListDeadLetterEventsResponse{
		Data:       data,
		Pagination: buildPagination(page, limit, total),
	}, nil
}

func normalizePage(page int) int {
	if page < 1 {
		return 1
	}
	return page
}

func normalizeLimit(limit int) int {
	if limit <= 0 {
		return defaultListLimit
	}
	return limit
}

func buildPagination(page, limit int, totalItems int64) Pagination {
	totalPages := 0
	if totalItems > 0 {
		totalPages = int((totalItems + int64(limit) - 1) / int64(limit))
	}

	return Pagination{Page: page, Limit: limit, TotalItems: totalItems, TotalPages: totalPages}
}
