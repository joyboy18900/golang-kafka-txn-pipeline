# golang-kafka-txn-pipeline

Kafka producer/consumer pipeline that processes synthetic transaction events into a
Postgres read model, with idempotent processing and a retry-then-dead-letter path on
failure.

## Run

```bash
docker-compose up
curl http://localhost:8080/transactions
```

- Partitioning: topic `transactions.events` (6 partitions) is keyed by `account_id`
  (`kafka.Hash` balancer), so all events for one account stay in order on one
  partition; that matters once non-commutative event types (reversals, holds) are
  added. The tradeoff: a single hot account can't parallelize past one partition,
  while a random key would maximize throughput but break per-account ordering.
- Retry/DLQ: `ApplyEvent` failures get 3 attempts total, with 100ms then 200ms
  backoff between them, before landing in the `transactions.events.dlq` topic and
  a `dead_letter_events` row; JSON and validation failures skip straight there.
  `producer.poison_pill_ratio` (default 0.05) deterministically exercises this path.
  `pipeline_integration_test.go` proves a consumer killed mid-batch and restarted
  causes no data loss or duplication.

The app runs pending migrations and provisions both Kafka topics on startup, then
serves on `:8080`.

## Endpoints

- `GET /health`
- `GET /transactions?account_id=&page=&limit=`
- `GET /accounts/:account_id/balance`
- `GET /dead-letter-events?page=&limit=`

## Tests

```bash
go test ./...
go generate ./...   # regenerate repository mocks
```

- `service/*_test.go`: table-driven, gomock-mocked ports.
- `pipeline_integration_test.go`: real Kafka + Postgres, skips if unreachable,
  proves no loss or duplication across a kill-mid-batch-then-restart cycle.
