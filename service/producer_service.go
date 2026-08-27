package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"

	"golang-kafka-txn-pipeline/repository"

	"github.com/google/uuid"
)

var syntheticAccountIDs = []string{
	"acct-0", "acct-1", "acct-2", "acct-3", "acct-4",
	"acct-5", "acct-6", "acct-7", "acct-8", "acct-9",
	"acct-10", "acct-11", "acct-12", "acct-13", "acct-14",
	"acct-15", "acct-16", "acct-17", "acct-18", "acct-19",
}

type producerService struct {
	producerRepo    repository.ProducerRepository
	poisonPillRatio float64
}

func NewProducerService(producerRepo repository.ProducerRepository, poisonPillRatio float64) ProducerService {
	return producerService{producerRepo: producerRepo, poisonPillRatio: poisonPillRatio}
}

func (s producerService) GenerateAndPublish(ctx context.Context) error {
	event := TransactionEvent{
		EventID:     uuid.NewString(),
		AccountID:   syntheticAccountIDs[rand.IntN(len(syntheticAccountIDs))],
		Type:        randomTransactionType(),
		AmountCents: int64(rand.IntN(999900) + 100),
		Currency:    "USD",
		Status:      "posted",
		OccurredAt:  time.Now(),
	}

	if rand.Float64() < s.poisonPillRatio {
		event.AmountCents = -1
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	if err := s.producerRepo.Publish(ctx, event.AccountID, payload); err != nil {
		return fmt.Errorf("publish event: %w", err)
	}

	return nil
}

func randomTransactionType() string {
	if rand.IntN(2) == 0 {
		return "debit"
	}
	return "credit"
}
