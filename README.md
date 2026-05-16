# Webhook Delivery System

A production-ready, async-first webhook delivery backend built in Go. Receives webhook events from Git providers (GitHub, GitLab, Bitbucket), verifies their authenticity via HMAC-SHA256, queues them to AWS SQS (standard Queue), and delivers them reliably to registered target URLs — with retry logic, exponential backoff with jitter to avoid timing attacks and thundering herd problem, dead-letter handling, and a full delivery log in PostgreSQL.

---

## How It Works

```
Git Server (GitHub/GitLab)
        │
        │  POST /webhooks/:webhookID/events
        │  X-Hub-Signature-256: sha256=<hmac>
        ▼
┌──────────────────────┐
│   HTTP Server        │  ← Fiber
│   Verify HMAC        │
│   Enqueue to SQS     │
│   Return 200 fast    │
└──────────┬───────────┘
           │
           ▼ (async)
┌──────────────────────┐
│   SQS Queue          │  ← AWS SQS(Standard Queue)
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│   Worker Process     │  ← concurrent goroutines
│   Idempotency check  │
│   HTTP POST to target│
│   Log to Postgres    │
│   Retry on failure   │
└──────────┬───────────┘
           │ (after max retries)
           ▼
┌──────────────────────┐
│   Dead-Letter Queue  │  ← SQS DLQ
└──────────────────────┘
```

---

## Tech Stack

| Layer | Tool | Why |
|---|---|---|
| Language | Go 1.26 | Fast, small binaries, great for concurrent systems |
| HTTP Framework | [Fiber v3](https://github.com/gofiber/fiber) | Fastest Go HTTP framework, built on `fasthttp` |
| Message Queue | AWS SQS | Managed, durable, built-in DLQ, no infra to maintain |
| Database | PostgreSQL 18 | ACID guarantees for delivery log; you can't lose delivery records |
| Logger | [Zap](https://github.com/uber-go/zap) | Structured JSON logs with zero allocation hot path |
| Migrations | [golang-migrate](https://github.com/golang-migrate/migrate) | Versioned SQL migrations, CLI-driven |
| PG Driver | [pgx v5](https://github.com/jackc/pgx) | Fastest PostgreSQL driver for Go |
| AWS SDK | aws-sdk-go-v2 | Official AWS SDK, modular, context-aware |

---

## Project Structure

```
webhook-delivery/
├── cmd/
│   ├── server/main.go          # HTTP receiver — Fiber app entry point
│   └── worker/main.go          # SQS consumer — async delivery entry point
│
├── internal/
│   ├── config/config.go        # Loads all env vars into a typed Config struct
│   ├── logger/logger.go        # Zap logger singleton
│   ├── middleware/
│   │   └── signature.go        # HMAC-SHA256 verification middleware
│   ├── handler/
│   │   └── webhook.go          # Fiber route handler — verify, enqueue, 200
│   ├── queue/
│   │   └── sqs.go              # Publish, consume, delete, DLQ
│   ├── delivery/
│   │   ├── dispatcher.go       # Outbound HTTP POST + delivery log update
│   │   └── retry.go            # Exponential backoff + DLQ handoff
│   └── store/
│       ├── webhook.go          # Webhook config CRUD (postgres)
│       └── delivery_log.go     # Delivery attempt log (postgres)
│
├── migrations/
│   ├── 0001_create_webhooks.up.sql
│   ├── 0001_create_webhooks.down.sql
│   ├── 0002_create_delivery_log.up.sql
│   └── 0002_create_delivery_log.down.sql
│
├── .env
├── docker-compose.yml          # Local PostgreSQL
├── go.sum
└── go.mod
```

---

## Prerequisites

- [Go 1.22+](https://go.dev/dl/)
- [Docker](https://www.docker.com/) (for local PostgreSQL)
- [golang-migrate CLI](https://github.com/golang-migrate/migrate/tree/master/cmd/migrate)
- An AWS account with SQS queues created (see below)

---

## AWS SQS Setup

Create two standard queues in your AWS console (or via CLI):

```bash
# Main delivery queue
aws sqs create-queue --queue-name webhook-delivery-queue --region ap-south-1

# Dead-letter queue (for failed deliveries after max retries)
aws sqs create-queue --queue-name webhook-delivery-dlq --region ap-south-1
```

> **Note:** If you use FIFO queues, append `.fifo` to the names and update the `PublishWithDelay` call to include `MessageGroupId`.

---

## Local Setup

### 1. Clone the repo

```bash
git clone https://github.com/yourorg/webhook-delivery.git
cd webhook-delivery
```

### 2. Install dependencies

```bash
go mod tidy
```

### 3. Configure environment

Copy `.env` and fill in your values:

```bash
cp .env .env.local
```

```env
# Server
APP_PORT=3000

# PostgreSQL
DB_URL=postgres://postgres:password@localhost:5432/webhook_delivery?sslmode=disable

# AWS
AWS_REGION=ap-south-1
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
SQS_QUEUE_URL=https://sqs.ap-south-1.amazonaws.com/123456789012/webhook-delivery-queue
SQS_DLQ_URL=https://sqs.ap-south-1.amazonaws.com/123456789012/webhook-delivery-dlq

# Webhook HMAC secret — must match what your Git provider is configured with
WEBHOOK_SECRET=your-hmac-secret-here

# Delivery tuning
MAX_RETRY_ATTEMPTS=5
DELIVERY_TIMEOUT_SECONDS=10
WORKER_CONCURRENCY=5
```

### 4. Start PostgreSQL

```bash
make docker-up
```

### 5. Run database migrations

```bash
export DB_URL="postgres://postgres:password@localhost:5432/webhook_delivery?sslmode=disable"
make migrate-up
```

### 6. Start the HTTP server

```bash
make run-server
# Server listening on :3000
```

### 7. Start the worker (separate terminal)

```bash
make run-worker
# Worker started with 5 concurrent goroutines
```

---

## API Endpoints

### Receive a webhook event

```
POST /webhooks/:webhookID/events
```

**Headers required:**

| Header | Value |
|---|---|
| `X-Hub-Signature-256` | `sha256=<hmac-sha256 of body>` |
| `X-GitHub-Delivery` | Unique delivery UUID (idempotency key) |
| `X-GitHub-Event` | Event type e.g. `push`, `pull_request` |
| `Content-Type` | `application/json` |

**Response:**

```json
HTTP 200
{
  "status": "queued",
  "delivery_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

Returns `401` if the HMAC signature is missing or invalid.  
Returns `500` if SQS publish fails (sender should retry).

---

### Health check

```
GET /health
```

```json
HTTP 200
{ "status": "ok" }
```

---

## Database Schema

### `webhooks`

Stores registered webhook endpoints.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `target_url` | TEXT | Where to deliver the event |
| `secret` | TEXT | HMAC secret for this webhook |
| `events` | TEXT[] | Event types to listen for |
| `active` | BOOLEAN | Soft-disable without deleting |
| `created_at` | TIMESTAMPTZ | — |
| `updated_at` | TIMESTAMPTZ | — |

### `delivery_log`

One row per delivery attempt.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `delivery_id` | UUID | Idempotency key from the sender |
| `webhook_id` | UUID | FK → webhooks |
| `attempt` | INT | 1-indexed, incremented on retry |
| `status` | ENUM | `pending`, `success`, `failed` |
| `status_code` | INT | HTTP status from target (NULL on network error) |
| `duration_ms` | INT | Round-trip time |
| `error` | TEXT | Error message on failure |
| `created_at` | TIMESTAMPTZ | — |

---

## Retry Logic

Failed deliveries are retried with **exponential backoff + full jitter** to avoid thundering herd:

| Attempt | Approx. wait before next retry |
|---|---|
| 1 → 2 | ~0–5s |
| 2 → 3 | ~0–10s |
| 3 → 4 | ~0–20s |
| 4 → 5 | ~0–40s |
| 5 | → Dead-Letter Queue |

- Delays are enforced by SQS `DelaySeconds` — the worker goroutine is never blocked sleeping.
- After `MAX_RETRY_ATTEMPTS`, the message is moved to the DLQ for manual inspection or replay.
- Every attempt is logged in `delivery_log` with status, HTTP code, duration, and any error.

---

## Idempotency

SQS guarantees **at-least-once** delivery, so the same message can arrive more than once. We handle this at two levels:

1. **Fast check** — `IsDuplicate(delivery_id, attempt)` query before processing.
2. **Hard guarantee** — unique index on `(delivery_id, attempt)` in PostgreSQL. A duplicate insert fails at the DB level.

---

## Make Commands

```bash
make run-server        # Start HTTP receiver
make run-worker        # Start async worker
make build             # Build both binaries to ./bin/
make migrate-up        # Apply all pending migrations
make migrate-down      # Roll back the last migration
make docker-up         # Start local PostgreSQL via Docker
make docker-down       # Stop Docker containers
make tidy              # go mod tidy
```

---

## Environment Variables Reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `APP_PORT` | Yes | — | HTTP server port |
| `DB_URL` | Yes | — | PostgreSQL connection string |
| `AWS_REGION` | Yes | — | AWS region for SQS |
| `AWS_ACCESS_KEY_ID` | Yes | — | AWS credentials |
| `AWS_SECRET_ACCESS_KEY` | Yes | — | AWS credentials |
| `SQS_QUEUE_URL` | Yes | — | Main delivery queue URL |
| `SQS_DLQ_URL` | Yes | — | Dead-letter queue URL |
| `WEBHOOK_SECRET` | Yes | — | HMAC-SHA256 shared secret |
| `MAX_RETRY_ATTEMPTS` | No | `5` | Max delivery attempts before DLQ |
| `DELIVERY_TIMEOUT_SECONDS` | No | `10` | HTTP client timeout for outbound calls |
| `WORKER_CONCURRENCY` | No | `5` | Number of SQS consumer goroutines |