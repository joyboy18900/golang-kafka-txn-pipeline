# Manual walkthrough

Base URL: `http://localhost:8080`

## Start the stack

```bash
docker-compose up
```

On boot the app runs pending migrations, creates both Kafka topics if they
do not exist yet, then serves on `:8080`. No request is needed to start the
pipeline - a background producer goroutine publishes a synthetic event every
`producer.interval_ms` (500ms by default), and three consumer goroutines
process them continuously.

## Topics in use

| Topic | Partitions | Written by | Read by |
|---|---|---|---|
| `transactions.events` | 6 | background producer | consumer group `txn-consumer-group` (3 consumers) |
| `transactions.events.dlq` | 1 | a consumer, after retries are exhausted | none in this codebase - kept as an audit trail; the actual read path is the `dead_letter_events` table below |

## Endpoints

### `GET /health`

```bash
curl http://localhost:8080/health
```

```json
{ "code": 200, "message": "ok", "data": null }
```

### `GET /transactions?account_id=&page=&limit=`

Lists processed transactions from Postgres. `account_id` is optional;
`page`/`limit` default per the project's pagination convention.

```bash
curl "http://localhost:8080/transactions?account_id=acct-3&page=1&limit=20"
```

```json
{
  "code": 200,
  "message": "transactions listed",
  "data": {
    "data": [
      {
        "event_id": "b1e2...",
        "account_id": "acct-3",
        "type": "credit",
        "amount_cents": 45210,
        "currency": "USD",
        "status": "posted",
        "occurred_at": "2026-08-27T10:15:03Z"
      }
    ],
    "pagination": { "page": 1, "limit": 20, "total_items": 87, "total_pages": 5 }
  }
}
```

### `GET /accounts/:account_id/balance`

Running balance for one account, derived from every successfully applied
event.

```bash
curl http://localhost:8080/accounts/acct-3/balance
```

```json
{
  "code": 200,
  "message": "balance retrieved",
  "data": {
    "account_id": "acct-3",
    "balance_cents": 128430,
    "applied_event_count": 12,
    "last_event_at": "2026-08-27T10:20:11Z"
  }
}
```

### `GET /dead-letter-events?page=&limit=`

Events that failed validation, or failed all retry attempts against
Postgres. `producer.poison_pill_ratio` (default 0.05) guarantees this path
gets exercised on every run.

```bash
curl "http://localhost:8080/dead-letter-events?page=1&limit=20"
```

```json
{
  "code": 200,
  "message": "dead letter events listed",
  "data": {
    "data": [
      {
        "id": 4,
        "event_id": "c7f0...",
        "topic": "transactions.events",
        "failure_reason": "amount_cents must be positive",
        "attempt_count": 0,
        "failed_at": "2026-08-27T10:16:40Z"
      }
    ],
    "pagination": { "page": 1, "limit": 20, "total_items": 4, "total_pages": 1 }
  }
}
```

## Proving no loss, no duplication

```bash
docker compose kill app
docker-compose up -d app
curl http://localhost:8080/transactions
```

Offsets commit only after the DB or DLQ write succeeds, so a kill mid-batch
should not lose or duplicate events. `pipeline_integration_test.go` proves
this against real Kafka and Postgres.
