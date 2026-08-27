package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"golang-kafka-txn-pipeline/logs"
	"golang-kafka-txn-pipeline/repository"
)

type eventService struct {
	consumerRepo repository.ConsumerRepository
	txRepo       repository.TransactionRepository
	dlqRepo      repository.DeadLetterRepository
	dlqProducer  repository.ProducerRepository
	maxAttempts  int
	baseBackoff  time.Duration
}

func NewEventService(
	consumerRepo repository.ConsumerRepository,
	txRepo repository.TransactionRepository,
	dlqRepo repository.DeadLetterRepository,
	dlqProducer repository.ProducerRepository,
	maxAttempts int,
	baseBackoff time.Duration,
) EventService {
	return eventService{
		consumerRepo: consumerRepo,
		txRepo:       txRepo,
		dlqRepo:      dlqRepo,
		dlqProducer:  dlqProducer,
		maxAttempts:  maxAttempts,
		baseBackoff:  baseBackoff,
	}
}

func (s eventService) RunConsumer(ctx context.Context) error {
	for {
		msg, err := s.consumerRepo.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("fetch message: %w", err)
		}

		if err := s.ProcessMessage(ctx, msg); err != nil {
			return fmt.Errorf("process message offset %d: %w", msg.Offset, err)
		}

		if err := s.consumerRepo.CommitMessage(ctx, msg); err != nil {
			return fmt.Errorf("commit message offset %d: %w", msg.Offset, err)
		}
	}
}

func (s eventService) ProcessMessage(ctx context.Context, msg repository.Message) error {
	var event TransactionEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return s.sendToDeadLetter(ctx, msg, nil, "invalid json: "+err.Error(), 0)
	}

	if err := validateTransactionEvent(event); err != nil {
		return s.sendToDeadLetter(ctx, msg, &event, err.Error(), 0)
	}

	params := repository.ApplyEventParams{
		EventID:     event.EventID,
		AccountID:   event.AccountID,
		Type:        event.Type,
		AmountCents: event.AmountCents,
		Currency:    event.Currency,
		Status:      event.Status,
		OccurredAt:  event.OccurredAt,
	}

	var lastErr error
	for attempt := 1; attempt <= s.maxAttempts; attempt++ {
		lastErr = s.txRepo.ApplyEvent(ctx, params)
		if lastErr == nil {
			return nil
		}

		logs.Error(lastErr)
		if attempt < s.maxAttempts {
			time.Sleep(s.baseBackoff * time.Duration(1<<(attempt-1)))
		}
	}

	return s.sendToDeadLetter(ctx, msg, &event, lastErr.Error(), s.maxAttempts)
}

func (s eventService) sendToDeadLetter(ctx context.Context, msg repository.Message, event *TransactionEvent, reason string, attempts int) error {
	var eventID *string
	if event != nil {
		eventID = &event.EventID
	}

	dlqEvent := repository.DeadLetterEvent{
		EventID:        eventID,
		Topic:          msg.Topic,
		KafkaPartition: msg.KafkaPartition,
		KafkaOffset:    msg.Offset,
		Payload:        msg.Value,
		FailureReason:  reason,
		AttemptCount:   attempts,
	}

	if err := s.dlqRepo.Insert(ctx, dlqEvent); err != nil {
		return fmt.Errorf("insert dead letter event: %w", err)
	}
	if err := s.dlqProducer.Publish(ctx, string(msg.Key), msg.Value); err != nil {
		return fmt.Errorf("publish dead letter event: %w", err)
	}

	return nil
}

func validateTransactionEvent(event TransactionEvent) error {
	if event.AccountID == "" {
		return errors.New("account_id is required")
	}
	if event.AmountCents <= 0 {
		return errors.New("amount_cents must be positive")
	}
	if event.Type != "debit" && event.Type != "credit" {
		return errors.New("type must be debit or credit")
	}
	return nil
}
