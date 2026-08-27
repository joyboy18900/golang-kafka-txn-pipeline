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
serves on `:8080`.

| Feature | Implementation | Trade-off / Proof |
|---|---|---|
| Partition key | `account_id`, `kafka.Hash` balancer, 6 partitions | Per-account order preserved (needed once non-commutative events like reversals exist); a single hot account can't parallelize past one partition, a random key would but breaks order |
| Topic provisioning | App creates both topics on boot via `kafka.Conn.CreateTopics` | Not left to broker auto-create defaults, so the partition count above is guaranteed at runtime |
| Retry | 3 attempts total, 100ms then 200ms backoff | JSON/validation failures are non-retryable and skip straight to the DLQ |
| Dead-letter queue | `transactions.events.dlq` topic + `dead_letter_events` table | `producer.poison_pill_ratio` (default 0.05) deterministically exercises this path on every run |
| Idempotency | `ON CONFLICT (event_id) DO NOTHING` + `RowsAffected == 0` gate | Verified live: 31 transactions / 31 distinct `event_id` / 0 balance mismatches after `docker compose kill app` |
| No loss, no duplication | Offset committed only after the DB or DLQ write succeeds | `pipeline_integration_test.go`: kill-mid-batch-then-restart, passing against real Kafka + Postgres |

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
