package repository

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type deadLetterEventRow struct {
	ID             int64
	EventID        *string
	Topic          string
	KafkaPartition int
	KafkaOffset    int64
	Payload        []byte
	FailureReason  string
	AttemptCount   int
	FailedAt       time.Time
}

func (deadLetterEventRow) TableName() string {
	return "dead_letter_events"
}

type deadLetterRepositoryDB struct {
	db *gorm.DB
}

func NewDeadLetterRepositoryDB(db *gorm.DB) DeadLetterRepository {
	return deadLetterRepositoryDB{db: db}
}

func (r deadLetterRepositoryDB) Insert(ctx context.Context, event DeadLetterEvent) error {
	row := deadLetterEventRow{
		EventID:        event.EventID,
		Topic:          event.Topic,
		KafkaPartition: event.KafkaPartition,
		KafkaOffset:    event.KafkaOffset,
		Payload:        event.Payload,
		FailureReason:  event.FailureReason,
		AttemptCount:   event.AttemptCount,
	}

	err := r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "topic"}, {Name: "kafka_partition"}, {Name: "kafka_offset"}},
		DoNothing: true,
	}).Create(&row).Error
	if err != nil {
		return fmt.Errorf("insert dead letter event: %w", err)
	}

	return nil
}

func (r deadLetterRepositoryDB) List(ctx context.Context, page, limit int) ([]DeadLetterEvent, int64, error) {
	var total int64
	if err := r.db.WithContext(ctx).Model(&deadLetterEventRow{}).Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("count dead letter events: %w", err)
	}

	offset := (page - 1) * limit
	var rows []deadLetterEventRow
	if err := r.db.WithContext(ctx).
		Order("failed_at DESC, id DESC").
		Offset(offset).Limit(limit).
		Find(&rows).Error; err != nil {
		return nil, 0, fmt.Errorf("list dead letter events: %w", err)
	}

	events := make([]DeadLetterEvent, len(rows))
	for i, row := range rows {
		events[i] = DeadLetterEvent{
			ID:             row.ID,
			EventID:        row.EventID,
			Topic:          row.Topic,
			KafkaPartition: row.KafkaPartition,
			KafkaOffset:    row.KafkaOffset,
			Payload:        row.Payload,
			FailureReason:  row.FailureReason,
			AttemptCount:   row.AttemptCount,
			FailedAt:       row.FailedAt,
		}
	}

	return events, total, nil
}
