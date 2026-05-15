// delivery log store

// will insert and update the delivery attempt rows

package store

import (
	"context"
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

	const q = ` INSERT INTO 
		
		
		`

}
