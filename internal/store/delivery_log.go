// delivery log store

// will insert and update the delivery attempt rows

package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// delivery status  enum

type DeliveryStatus string

// webhook delivery status - pending, success, failure
const (
	StatusPending DeliveryStatus = "pending"
	StatusSuccess DeliveryStatus = "success"
	StatusFailure DeliveryStatus = "failure"
)

// webhook delivery log with at least one attempt to deliver the webhook event

type DeliveryLog struct {
	ID         uuid.UUID      `json:"id"`
	DeliveryID uuid.UUID      `json:"delivery_id"` // will check for idempotency on this
	WebhookID  uuid.UUID      `json:"webhook_id"`
	Attempt    int            `json:"attempt"`
	Status     DeliveryStatus `json:"status"`
	StatusCode *int           `json:"status_code"` // it will show nil on network error
	DurationMs *int           `json:"duration_ms"`
	Error      *string        `json:"error"`
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

// delivery log store - will handle postgres query on delivery log table

type DeliveryLogStore struct {
	db *pgxpool.Pool
}

// function to create the new delivery log store

func NewDeliveryStore(db *pgxpool.Pool) *DeliveryLogStore {
	return &DeliveryLogStore{db: db}
}

// insert with initial status - pending, unique id's like delivery id and attempt we can make it idempotent, any duplicate insert will return and error

// having the delivery id and webhook id

func (d *DeliveryLogStore) Insert(ctx context.Context, deliveryID, webhookID uuid.UUID, attempt int) (*DeliveryLog, error) {

	// query for insert delivery id, webhook id , attempt and status of webhook delivery in delivery log table

	const q = `INSERT INTO delivery_log (delivery_id, webhook_id, attempt, status) VALUES ($1, $2, $3, 'pending') RETURNING id, delivery_id, webhook_id, attempt, status, status_code, duration_ms, error, created_at, updated_at
  `

	var dl DeliveryLog

	err := d.db.QueryRow(ctx, q, deliveryID, webhookID, attempt).Scan(
		&dl.ID,
		&dl.DeliveryID,
		&dl.WebhookID,
		&dl.Attempt,
		&dl.Status,
		&dl.StatusCode,
		&dl.DurationMs,
		&dl.Error,
		&dl.CreatedAt,
		&dl.UpdatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("store: insert delivery log (delivery=%s attempt=%d): %w", deliveryID, attempt, err)
	}

	return &dl, nil

}

// udpate result function for setting latest final status code, duration

func (d *DeliveryLogStore) UpdateResult(ctx context.Context, id uuid.UUID, status DeliveryStatus, statusCode *int, durationMs *int, errMsg *string) error {

	// query

	const q = `UPDATE delivery_log SET status = $2, status_code = $3, duration_ms = $4, error = $5, updated_at = now() WHERE id = $1`

	tag, err := d.db.Exec(ctx, q, id, status, statusCode, durationMs, errMsg)

	if err != nil {
		return fmt.Errorf("store: update result %s: %w", id, err)
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: update result: row %s not found", id)
	}

	return nil

}

// is duplicate function for checking if our delivery id and attempt already exists or not

func (d *DeliveryLogStore) IsDuplicate(ctx context.Context, deliveryID uuid.UUID, attempt int) (bool, error) {

	//query for checking

	const q = `SELECT EXISTS (
		  SELECT 1 FROM delivery_log WHERE delivery_id = $1 AND attempt = $2 AND status != 'pending'
	)`

	var exists bool
	if err := d.db.QueryRow(ctx, q, deliveryID, attempt).Scan(&exists); err != nil {
		return false, fmt.Errorf("store: is duplicate: %w", err)
	}

	return exists, nil
}

// to get all attempts for a given delivery id , the listbydelivery id function

// used to debug any specific delivery

func (d *DeliveryLogStore) ListByDeliveryID(ctx context.Context, deliveryID uuid.UUID) ([]*DeliveryLog, error) {

	const q = `SELECT id, delivery_id, webhook_id, attempt, status, status_code, duration_ms, error, created_at, updated_at FROM  delivery_log WHERE delivery_id = $1 ORDER BY attempt ASC`

	rows, err := d.db.Query(ctx, q, deliveryID)

	if err != nil {
		return nil, fmt.Errorf("store: list by delivery id: %w", err)
	}

	var logs []*DeliveryLog

	for rows.Next() {

		var dl DeliveryLog

		if err := rows.Scan(
			&dl.ID,
			&dl.DeliveryID,
			&dl.WebhookID,
			&dl.Attempt,
			&dl.Status,
			&dl.StatusCode,
			&dl.DurationMs,
			&dl.Error,
			&dl.CreatedAt,
			&dl.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("store: list by delivery id scan: %w", err)
		}

		logs = append(logs, &dl)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list by delivery id rows: %w", err)
	}

	return logs, nil

}
