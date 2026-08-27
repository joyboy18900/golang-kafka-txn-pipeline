# golang-kafka-txn-pipeline

Kafka producer/consumer pipeline that processes synthetic transaction events into a
Postgres read model, with idempotent processing and a retry-then-dead-letter path on
failure.

## Run

```bash
docker-compose up
curl http://localhost:8080/transactions
```

The app runs pending migrations and provisions both Kafka topics on startup, then
serves on `:8080`. The main topic uses `account_id` as the partition key, so
per-account event order is preserved across 6 partitions.

## Endpoints

- `GET /health`
- `GET /transactions?account_id=&page=&limit=`
- `GET /accounts/:account_id/balance`
- `GET /dead-letter-events?page=&limit=`

See `curl/flow.md` for full request/response examples.

## Tests

```bash
go test ./...
go generate ./...   # regenerate repository mocks
```

- `service/*_test.go`: table-driven, gomock-mocked ports.
- `pipeline_integration_test.go`: real Kafka + Postgres, skips if unreachable,
  proves no loss or duplication across a kill-mid-batch-then-restart cycle.
- Idempotency verified live via `ON CONFLICT (event_id) DO NOTHING`: 31
  transactions, 31 distinct `event_id`, 0 balance mismatches after
  `docker compose kill app` mid-run.
