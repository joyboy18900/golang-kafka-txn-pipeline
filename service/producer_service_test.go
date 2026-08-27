package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"golang-kafka-txn-pipeline/mock/mock_repository"
	"golang-kafka-txn-pipeline/service"

	"go.uber.org/mock/gomock"
)

func TestProducerService_GenerateAndPublish_ZeroRatio_AlwaysPublishesValidEvent(t *testing.T) {
	ctrl := gomock.NewController(t)
	producerRepo := mock_repository.NewMockProducerRepository(ctrl)

	var published service.TransactionEvent
	producerRepo.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, key string, value []byte) error {
		if err := json.Unmarshal(value, &published); err != nil {
			t.Fatalf("Unmarshal() error = %v", err)
		}
		if key != published.AccountID {
			t.Errorf("key = %q, want the account_id %q", key, published.AccountID)
		}
		return nil
	}).Times(20)

	svc := service.NewProducerService(producerRepo, 0)
	for i := 0; i < 20; i++ {
		if err := svc.GenerateAndPublish(context.Background()); err != nil {
			t.Fatalf("GenerateAndPublish() error = %v", err)
		}
		if published.AccountID == "" {
			t.Fatalf("AccountID is empty, want a synthetic account id")
		}
		if published.AmountCents <= 0 {
			t.Fatalf("AmountCents = %d, want a positive amount when poison_pill_ratio is 0", published.AmountCents)
		}
		if published.Type != "debit" && published.Type != "credit" {
			t.Fatalf("Type = %q, want debit or credit", published.Type)
		}
	}
}

func TestProducerService_GenerateAndPublish_FullRatio_AlwaysCorruptsPayload(t *testing.T) {
	ctrl := gomock.NewController(t)
	producerRepo := mock_repository.NewMockProducerRepository(ctrl)

	var published service.TransactionEvent
	producerRepo.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string, value []byte) error {
		return json.Unmarshal(value, &published)
	}).Times(5)

	svc := service.NewProducerService(producerRepo, 1)
	for i := 0; i < 5; i++ {
		if err := svc.GenerateAndPublish(context.Background()); err != nil {
			t.Fatalf("GenerateAndPublish() error = %v", err)
		}
		if published.AccountID == "" {
			t.Fatalf("AccountID is empty, want the partition key to stay valid even for a poison pill")
		}
		if published.AmountCents > 0 {
			t.Fatalf("AmountCents = %d, want a corrupted non-positive amount when poison_pill_ratio is 1", published.AmountCents)
		}
	}
}
