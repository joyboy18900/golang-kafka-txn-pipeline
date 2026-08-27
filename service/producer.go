package service

import "context"

//go:generate go tool mockgen -destination=../mock/mock_service/producer.go golang-kafka-txn-pipeline/service ProducerService
type ProducerService interface {
	GenerateAndPublish(ctx context.Context) error
}
