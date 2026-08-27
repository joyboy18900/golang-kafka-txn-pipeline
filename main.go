package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang-kafka-txn-pipeline/logs"
	"golang-kafka-txn-pipeline/repository"
	"golang-kafka-txn-pipeline/service"

	"github.com/gofiber/fiber/v2"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const restartMaxBackoff = 30 * time.Second

func main() {
	initConfig()
	runMigrations()

	db := openGormDB()
	brokers := kafkaBrokers()

	mainTopic := viper.GetString("kafka.topic")
	dlqTopic := viper.GetString("kafka.dlq_topic")
	partitions := viper.GetInt("kafka.partitions")
	groupID := viper.GetString("kafka.group_id")
	consumerCount := viper.GetInt("kafka.consumer_count")

	createKafkaTopics(brokers,
		kafka.TopicConfig{Topic: mainTopic, NumPartitions: partitions, ReplicationFactor: 1},
		kafka.TopicConfig{Topic: dlqTopic, NumPartitions: 1, ReplicationFactor: 1},
	)

	transactionRepo := repository.NewTransactionRepositoryDB(db)
	deadLetterRepo := repository.NewDeadLetterRepositoryDB(db)

	dlqProducerRepo := repository.NewProducerRepositoryKafka(brokers, dlqTopic)
	defer dlqProducerRepo.Close()

	maxAttempts := viper.GetInt("retry.max_attempts")
	baseBackoff := time.Duration(viper.GetInt("retry.base_backoff_ms")) * time.Millisecond

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	consumerRepos := make([]repository.ConsumerRepository, 0, consumerCount)
	for i := 1; i <= consumerCount; i++ {
		consumerRepo := repository.NewConsumerRepositoryKafka(brokers, mainTopic, groupID, kafka.FirstOffset)
		consumerRepos = append(consumerRepos, consumerRepo)

		eventSvc := service.NewEventService(consumerRepo, transactionRepo, deadLetterRepo, dlqProducerRepo, maxAttempts, baseBackoff)

		name := "consumer-" + strconv.Itoa(i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			runConsumerLoop(ctx, name, eventSvc)
		}()
	}

	app := fiber.New()

	go func() {
		port := viper.GetString("app.port")
		logs.Info("server started on port " + port)
		if err := app.Listen(":" + port); err != nil {
			logs.Error(err)
		}
	}()

	<-ctx.Done()
	logs.Info("shutting down")

	if err := app.Shutdown(); err != nil {
		logs.Error(err)
	}
	wg.Wait()

	for _, consumerRepo := range consumerRepos {
		if err := consumerRepo.Close(); err != nil {
			logs.Error(err)
		}
	}
}

func runConsumerLoop(ctx context.Context, name string, svc service.EventService) {
	backoff := time.Second
	for {
		err := svc.RunConsumer(ctx)
		if err == nil || ctx.Err() != nil {
			return
		}

		logs.Error(name + ": " + err.Error())
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}

		if backoff < restartMaxBackoff {
			backoff *= 2
		}
	}
}

func initConfig() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if err := viper.ReadInConfig(); err != nil {
		panic(fmt.Errorf("read config: %w", err))
	}
}

func kafkaBrokers() []string {
	return strings.Split(viper.GetString("kafka.brokers"), ",")
}

func postgresDSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		viper.GetString("db.user"),
		viper.GetString("db.password"),
		viper.GetString("db.host"),
		viper.GetInt("db.port"),
		viper.GetString("db.name"),
		viper.GetString("db.sslmode"),
	)
}

func openGormDB() *gorm.DB {
	db, err := gorm.Open(postgres.Open(postgresDSN()), &gorm.Config{
		TranslateError: true,
		Logger:         gormLogger(),
	})
	if err != nil {
		panic(fmt.Errorf("open postgres: %w", err))
	}

	return db
}

func gormLogger() logger.Interface {
	return logger.New(log.New(os.Stdout, "\r\n", log.LstdFlags), logger.Config{
		SlowThreshold:             200 * time.Millisecond,
		LogLevel:                  logger.Warn,
		IgnoreRecordNotFoundError: true,
	})
}

func runMigrations() {
	dsn := strings.Replace(postgresDSN(), "postgres://", "pgx5://", 1)

	m, err := migrate.New("file://migrations", dsn)
	if err != nil {
		panic(fmt.Errorf("new migrate: %w", err))
	}
	defer m.Close()

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		panic(fmt.Errorf("migrate up: %w", err))
	}

	logs.Info("migrations up to date")
}

func createKafkaTopics(brokers []string, configs ...kafka.TopicConfig) {
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		panic(fmt.Errorf("dial kafka: %w", err))
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		panic(fmt.Errorf("get kafka controller: %w", err))
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(fmt.Errorf("dial kafka controller: %w", err))
	}
	defer controllerConn.Close()

	if err := controllerConn.CreateTopics(configs...); err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
		panic(fmt.Errorf("create kafka topics: %w", err))
	}

	logs.Info("kafka topics provisioned")
}
