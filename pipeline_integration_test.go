package main_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	"golang-kafka-txn-pipeline/repository"
	"golang-kafka-txn-pipeline/service"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const (
	testPostgresDSN  = "postgres://postgres:postgres@localhost:5432/golang_kafka_txn_pipeline?sslmode=disable"
	testKafkaBroker  = "localhost:29092"
	testFetchTimeout = 30 * time.Second
)

func connectTestGormDB(t *testing.T) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(postgres.Open(testPostgresDSN), &gorm.Config{})
	if err != nil {
		t.Skipf("skipping integration test: open gorm db: %v", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		t.Skipf("skipping integration test: gorm db handle: %v", err)
	}
	if err := sqlDB.PingContext(context.Background()); err != nil {
		t.Skipf("skipping integration test: postgres not reachable: %v", err)
	}

	return db
}

func connectTestKafkaBrokers(t *testing.T) []string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := kafka.DialContext(ctx, "tcp", testKafkaBroker)
	if err != nil {
		t.Skipf("skipping integration test: kafka not reachable: %v", err)
	}
	conn.Close()

	return []string{testKafkaBroker}
}

func createTestTopic(t *testing.T, brokers []string, topic string, partitions int) {
	t.Helper()

	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("Controller() error = %v", err)
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Fatalf("Dial(controller) error = %v", err)
	}
	defer controllerConn.Close()

	if err := controllerConn.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: partitions, ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateTopics() error = %v", err)
	}
}

func TestPipeline_RestartAfterUncommittedFetch_NoLossNoDuplication(t *testing.T) {
	brokers := connectTestKafkaBrokers(t)
	db := connectTestGormDB(t)

	suffix := t.Name() + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	topic := "test-transactions-" + suffix
	dlqTopic := "test-transactions-dlq-" + suffix
	groupID := "test-group-" + suffix

	createTestTopic(t, brokers, topic, 1)
	createTestTopic(t, brokers, dlqTopic, 1)

	txRepo := repository.NewTransactionRepositoryDB(db)
	deadLetterRepo := repository.NewDeadLetterRepositoryDB(db)
	dlqProducerRepo := repository.NewProducerRepositoryKafka(brokers, dlqTopic)
	defer dlqProducerRepo.Close()

	producerRepo := repository.NewProducerRepositoryKafka(brokers, topic)
	defer producerRepo.Close()

	event := service.TransactionEvent{
		EventID:     uuid.NewString(),
		AccountID:   "acct-forced-failure-test",
		Type:        "credit",
		AmountCents: 5000,
		Currency:    "USD",
		Status:      "posted",
		OccurredAt:  time.Now(),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if err := producerRepo.Publish(context.Background(), event.AccountID, payload); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	// "kill mid-batch": fetch and process, but close the reader without committing,
	// simulating a crash after processing but before the offset commit lands.
	reader1 := repository.NewConsumerRepositoryKafka(brokers, topic, groupID, kafka.FirstOffset)
	eventSvc1 := service.NewEventService(reader1, txRepo, deadLetterRepo, dlqProducerRepo, 3, time.Millisecond)

	ctx1, cancel1 := context.WithTimeout(context.Background(), testFetchTimeout)
	msg1, err := reader1.FetchMessage(ctx1)
	cancel1()
	if err != nil {
		t.Fatalf("FetchMessage() error = %v", err)
	}
	if err := eventSvc1.ProcessMessage(context.Background(), msg1); err != nil {
		t.Fatalf("ProcessMessage() error = %v", err)
	}
	if err := reader1.Close(); err != nil {
		t.Fatalf("reader1.Close() error = %v", err)
	}

	// "restart": a fresh reader against the same group ID and topic must refetch the
	// same uncommitted offset.
	reader2 := repository.NewConsumerRepositoryKafka(brokers, topic, groupID, kafka.FirstOffset)
	defer reader2.Close()
	eventSvc2 := service.NewEventService(reader2, txRepo, deadLetterRepo, dlqProducerRepo, 3, time.Millisecond)

	ctx2, cancel2 := context.WithTimeout(context.Background(), testFetchTimeout)
	defer cancel2()
	msg2, err := reader2.FetchMessage(ctx2)
	if err != nil {
		t.Fatalf("FetchMessage() after restart error = %v", err)
	}
	if msg2.Offset != msg1.Offset {
		t.Fatalf("second fetch got offset %d, want the same uncommitted offset %d", msg2.Offset, msg1.Offset)
	}
	if err := eventSvc2.ProcessMessage(context.Background(), msg2); err != nil {
		t.Fatalf("ProcessMessage() after restart error = %v", err)
	}
	if err := reader2.CommitMessage(context.Background(), msg2); err != nil {
		t.Fatalf("CommitMessage() error = %v", err)
	}

	var txCount int64
	if err := db.Table("transactions").Where("event_id = ?", event.EventID).Count(&txCount).Error; err != nil {
		t.Fatalf("count transactions error = %v", err)
	}
	if txCount != 1 {
		t.Fatalf("transactions rows for event_id = %d, want exactly 1", txCount)
	}

	balance, err := txRepo.GetAccountBalance(context.Background(), event.AccountID)
	if err != nil {
		t.Fatalf("GetAccountBalance() error = %v", err)
	}
	if balance == nil {
		t.Fatalf("GetAccountBalance() = nil, want a balance row")
	}
	if balance.BalanceCents != event.AmountCents {
		t.Fatalf("BalanceCents = %d, want %d (double-processing must not double-count)", balance.BalanceCents, event.AmountCents)
	}
	if balance.AppliedEventCount != 1 {
		t.Fatalf("AppliedEventCount = %d, want 1", balance.AppliedEventCount)
	}

	// a third reader proves the second commit actually landed: nothing left to fetch.
	reader3 := repository.NewConsumerRepositoryKafka(brokers, topic, groupID, kafka.FirstOffset)
	defer reader3.Close()

	ctx3, cancel3 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel3()
	if _, err := reader3.FetchMessage(ctx3); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected no more messages after the committed offset, got err = %v", err)
	}
}
