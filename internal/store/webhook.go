/*
 webhook with registered webhook endpoint
 it had a target url, which is going to be requested,


 The webhook contains three items with it:

  1.Events - can be anything
	2. Payload - in json format
	3. Target url  - webhook url endpoint
*/

package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Webhook struct {
	ID        uuid.UUID `json:"id"`
	TargetURL string    `json:"target_url"`
	Secret    string    `json:"-"` // it will never serialize to returned json response
	Events    []string  `json:"events"`
	Active    bool      `json:"active"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// we have a webhook postgres table, to take it's queries, will use pgxpool

type WebhookStore struct {
	db *pgxpool.Pool
}

// using this pool will create webhook store

func NewWebhookStore(db *pgxpool.Pool) *WebhookStore {
	return &WebhookStore{db: db}
}

// get by id - returns single webhook by primary key

// with query

func (w *WebhookStore) GetByID(ctx context.Context, id uuid.UUID) (*Webhook, error) {

	const q = `SELECT id, target_url, secret, events, active, created_at, updated_at FROM webhooks WHERE id = $1`

	// query on the row

	row := w.db.QueryRow(ctx, q, id)

	var wh Webhook

	err := row.Scan(
		&wh.ID,
		&wh.TargetURL,
		&wh.Secret,
		&wh.Events,
		&wh.Active,
		&wh.CreatedAt,
		&wh.UpdatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("store: get by id %s: %w", id, err)
	}

	return &wh, nil
}

// this function will return all the webhooks which are currently active
// list of active webhooks

func (w *WebhookStore) ListActive(ctx context.Context) ([]*Webhook, error) {

	const q = `SELECT id, target_url, secret, events, active, created_at, updated_at FROM webhooks WHERE active = true ORDER BY created_at ASC`

	rows, err := w.db.Query(ctx, q)

	if err != nil {
		return nil, fmt.Errorf("store: list active: %w", err)
	}

	var webhooks []*Webhook

	for rows.Next() {

		var wh Webhook

		if err := rows.Scan(
			&wh.ID,
			&wh.TargetURL,
			&wh.Secret,
			&wh.Events,
			&wh.Active,
			&wh.CreatedAt,
			&wh.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("store: last active scan: %w", err)
		}

		webhooks = append(webhooks, &wh)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list active rows: %w", err)
	}

	return webhooks, nil
}

// create function to insert new webhook and for return with id

func (w *WebhookStore) Create(ctx context.Context, targetURL, secret string, events []string) (*Webhook, error) {

	//query

	const q = `INSERT INTO webhooks (target_url, secret, events) VALUES ($1, $2, $3) RETURNING id, target_url, secret, events, active, created_at, updated_at`

	var wh Webhook

	err := w.db.QueryRow(ctx, q, targetURL, secret, events).Scan(
		&wh.ID,
		&wh.TargetURL,
		&wh.Secret,
		&wh.Events,
		&wh.Active,
		&wh.CreatedAt,
		&wh.UpdatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("store: create webhook: %w", err)
	}

	return &wh, nil

}

// to enable or disable the webhook without deleting it

func (w *WebhookStore) SetActive(ctx context.Context, id uuid.UUID, active bool) error {

	const q = `UPDATE webhooks SET active = $2, updated_at = now() WEHRE id = $1`

	tag, err := w.db.Exec(ctx, q, id, active)

	if err != nil {
		return fmt.Errorf("store: set active %s: %w", id, err)
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: set active: webhook %s not found", id)
	}

	return nil

}
