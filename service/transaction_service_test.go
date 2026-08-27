package service_test

import (
	"context"
	"testing"

	"golang-kafka-txn-pipeline/mock/mock_repository"
	"golang-kafka-txn-pipeline/repository"
	"golang-kafka-txn-pipeline/service"

	"go.uber.org/mock/gomock"
)

func TestTransactionService_List_TotalPages(t *testing.T) {
	tests := []struct {
		name       string
		totalItems int64
		limit      int
		wantPages  int
	}{
		{name: "zero items", totalItems: 0, limit: 20, wantPages: 0},
		{name: "exact multiple", totalItems: 40, limit: 20, wantPages: 2},
		{name: "remainder rounds up", totalItems: 41, limit: 20, wantPages: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			txRepo := mock_repository.NewMockTransactionRepository(ctrl)
			dlqRepo := mock_repository.NewMockDeadLetterRepository(ctrl)

			txRepo.EXPECT().ListTransactions(gomock.Any(), gomock.Any()).
				Return(repository.ListTransactionsResult{Transactions: nil, TotalItems: tt.totalItems}, nil)

			svc := service.NewTransactionService(txRepo, dlqRepo)
			resp, err := svc.List(context.Background(), service.ListTransactionsRequest{Page: 1, Limit: tt.limit})
			if err != nil {
				t.Fatalf("List() error = %v", err)
			}
			if resp.Pagination.TotalPages != tt.wantPages {
				t.Errorf("TotalPages = %d, want %d", resp.Pagination.TotalPages, tt.wantPages)
			}
		})
	}
}

func TestTransactionService_GetBalance_NotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	txRepo := mock_repository.NewMockTransactionRepository(ctrl)
	dlqRepo := mock_repository.NewMockDeadLetterRepository(ctrl)

	txRepo.EXPECT().GetAccountBalance(gomock.Any(), "acct-missing").Return(nil, nil)

	svc := service.NewTransactionService(txRepo, dlqRepo)
	_, err := svc.GetBalance(context.Background(), "acct-missing")
	if err == nil {
		t.Fatalf("GetBalance() error = nil, want a not-found error")
	}
}

func TestTransactionService_GetBalance_Found(t *testing.T) {
	ctrl := gomock.NewController(t)
	txRepo := mock_repository.NewMockTransactionRepository(ctrl)
	dlqRepo := mock_repository.NewMockDeadLetterRepository(ctrl)

	txRepo.EXPECT().GetAccountBalance(gomock.Any(), "acct-1").
		Return(&repository.AccountBalance{AccountID: "acct-1", BalanceCents: 500, AppliedEventCount: 2}, nil)

	svc := service.NewTransactionService(txRepo, dlqRepo)
	resp, err := svc.GetBalance(context.Background(), "acct-1")
	if err != nil {
		t.Fatalf("GetBalance() error = %v", err)
	}
	if resp.BalanceCents != 500 {
		t.Errorf("BalanceCents = %d, want 500", resp.BalanceCents)
	}
}
