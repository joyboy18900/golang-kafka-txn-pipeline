package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"golang-kafka-txn-pipeline/mock/mock_repository"
	"golang-kafka-txn-pipeline/repository"
	"golang-kafka-txn-pipeline/service"

	"go.uber.org/mock/gomock"
)

func TestEventService_ProcessMessage_InvalidJSON_GoesStraightToDeadLetter(t *testing.T) {
	ctrl := gomock.NewController(t)
	txRepo := mock_repository.NewMockTransactionRepository(ctrl)
	dlqRepo := mock_repository.NewMockDeadLetterRepository(ctrl)
	dlqProducer := mock_repository.NewMockProducerRepository(ctrl)

	msg := repository.Message{Topic: "transactions.events", KafkaPartition: 0, Offset: 1, Value: []byte("not json")}

	dlqRepo.EXPECT().Insert(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, e repository.DeadLetterEvent) error {
		if e.EventID != nil {
			t.Errorf("EventID = %v, want nil for a payload that failed to unmarshal", *e.EventID)
		}
		if e.AttemptCount != 0 {
			t.Errorf("AttemptCount = %d, want 0 (no retry for invalid json)", e.AttemptCount)
		}
		return nil
	})
	dlqProducer.EXPECT().Publish(gomock.Any(), "", msg.Value).Return(nil)

	svc := service.NewEventService(nil, txRepo, dlqRepo, dlqProducer, 3, time.Millisecond)
	if err := svc.ProcessMessage(context.Background(), msg); err != nil {
		t.Fatalf("ProcessMessage() error = %v", err)
	}
}

func TestEventService_ProcessMessage_ValidationFailure_GoesStraightToDeadLetter(t *testing.T) {
	ctrl := gomock.NewController(t)
	txRepo := mock_repository.NewMockTransactionRepository(ctrl)
	dlqRepo := mock_repository.NewMockDeadLetterRepository(ctrl)
	dlqProducer := mock_repository.NewMockProducerRepository(ctrl)

	payload := []byte(`{"event_id":"evt-1","account_id":"acct-1","type":"invalid","amount_cents":-1}`)
	msg := repository.Message{Topic: "transactions.events", KafkaPartition: 0, Offset: 2, Value: payload}

	dlqRepo.EXPECT().Insert(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, e repository.DeadLetterEvent) error {
		if e.EventID == nil || *e.EventID != "evt-1" {
			t.Errorf("EventID = %v, want evt-1", e.EventID)
		}
		if e.AttemptCount != 0 {
			t.Errorf("AttemptCount = %d, want 0 (no retry for a validation failure)", e.AttemptCount)
		}
		return nil
	})
	dlqProducer.EXPECT().Publish(gomock.Any(), gomock.Any(), payload).Return(nil)

	svc := service.NewEventService(nil, txRepo, dlqRepo, dlqProducer, 3, time.Millisecond)
	if err := svc.ProcessMessage(context.Background(), msg); err != nil {
		t.Fatalf("ProcessMessage() error = %v", err)
	}
}

func TestEventService_ProcessMessage_TransientFailure_RetriesThenDeadLetters(t *testing.T) {
	ctrl := gomock.NewController(t)
	txRepo := mock_repository.NewMockTransactionRepository(ctrl)
	dlqRepo := mock_repository.NewMockDeadLetterRepository(ctrl)
	dlqProducer := mock_repository.NewMockProducerRepository(ctrl)

	payload := []byte(`{"event_id":"evt-2","account_id":"acct-1","type":"credit","amount_cents":500}`)
	msg := repository.Message{Topic: "transactions.events", KafkaPartition: 0, Offset: 3, Value: payload}

	txRepo.EXPECT().ApplyEvent(gomock.Any(), gomock.Any()).Return(errors.New("connection reset")).Times(3)
	dlqRepo.EXPECT().Insert(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, e repository.DeadLetterEvent) error {
		if e.AttemptCount != 3 {
			t.Errorf("AttemptCount = %d, want 3", e.AttemptCount)
		}
		return nil
	})
	dlqProducer.EXPECT().Publish(gomock.Any(), gomock.Any(), payload).Return(nil)

	svc := service.NewEventService(nil, txRepo, dlqRepo, dlqProducer, 3, time.Millisecond)
	if err := svc.ProcessMessage(context.Background(), msg); err != nil {
		t.Fatalf("ProcessMessage() error = %v", err)
	}
}

func TestEventService_ProcessMessage_SucceedsBeforeExhaustingRetries_NoDeadLetter(t *testing.T) {
	ctrl := gomock.NewController(t)
	txRepo := mock_repository.NewMockTransactionRepository(ctrl)
	dlqRepo := mock_repository.NewMockDeadLetterRepository(ctrl)
	dlqProducer := mock_repository.NewMockProducerRepository(ctrl)

	payload := []byte(`{"event_id":"evt-3","account_id":"acct-1","type":"debit","amount_cents":100}`)
	msg := repository.Message{Topic: "transactions.events", KafkaPartition: 0, Offset: 4, Value: payload}

	gomock.InOrder(
		txRepo.EXPECT().ApplyEvent(gomock.Any(), gomock.Any()).Return(errors.New("timeout")),
		txRepo.EXPECT().ApplyEvent(gomock.Any(), gomock.Any()).Return(nil),
	)

	svc := service.NewEventService(nil, txRepo, dlqRepo, dlqProducer, 3, time.Millisecond)
	if err := svc.ProcessMessage(context.Background(), msg); err != nil {
		t.Fatalf("ProcessMessage() error = %v", err)
	}
}
