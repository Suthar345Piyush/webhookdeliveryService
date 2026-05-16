// dispatcher will sents a http POST request to our target URL

package delivery

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/suthar345piyush/internal/logger"
	"github.com/suthar345piyush/internal/queue"
	"github.com/suthar345piyush/internal/store"
	"go.uber.org/zap"
)

type Dispatcher struct {
	webhookStore  *store.WebhookStore
	deliveryStore *store.DeliveryLogStore
	retrier       *Retrier
	httpClient    *http.Client
}

// new dipatcher function

func NewDispatcher(
	webhookStore *store.WebhookStore,
	deliveryStore *store.DeliveryLogStore,
	retrier *Retrier,
	timeoutSeconds int,
) *Dispatcher {
	return &Dispatcher{
		webhookStore:  webhookStore,
		deliveryStore: deliveryStore,
		retrier:       retrier,
		httpClient: &http.Client{
			Timeout: time.Duration(timeoutSeconds) * time.Second,
		},
	}
}

// dispatch is an sqs message handler function

func (d *Dispatcher) Dispatch(ctx context.Context, msg queue.WebhookMessage) error {

	deliveryID, err := uuid.Parse(msg.DeliveryID)

	// invalid delivery id

	if err != nil {
		logger.Log.Error("invalid delivery_id in message - discarding", zap.String("raw", msg.DeliveryID))

		return nil

	}

	webhookID, err := uuid.Parse(msg.WebhookID)

	if err != nil {
		logger.Log.Error("invalid webhook_id in message - discarding", zap.String("raw", msg.WebhookID))

		return nil
	}

	// idempotency check - will skip if already processed (delivery_id, attempt)

	isDup, err := d.deliveryStore.IsDuplicate(ctx, deliveryID, msg.Attempt)

	if err != nil {
		logger.Log.Error("idempotency check failed", zap.String("delivery_id", msg.DeliveryID), zap.Error(err))

		return err
	}

	// if we detected the duplicate delivery id
	if isDup {
		logger.Log.Info("duplicate delivery detected - skipping", zap.String("delivery_id", msg.DeliveryID), zap.Error(err))

		return nil
	}

	// fetching the registered webhook to get the target url

	webhook, err := d.webhookStore.GetByID(ctx, webhookID)
	if err != nil {
		logger.Log.Error("webhook not found", zap.String("webhook_id", msg.WebhookID), zap.Error(err))

		return nil // webhook is deleted - now message is unrecoverable
	}

	if !webhook.Active {
		logger.Log.Info("webhook is inactive - discarding message", zap.String("webhook_id", msg.WebhookID))

		return nil
	}

	// before calling to target url, creating a delivery log row, whose status=pending

	logRow, err := d.deliveryStore.Insert(ctx, deliveryID, webhookID, msg.Attempt)

	if err != nil {
		logger.Log.Error("failed to insert delivery log row", zap.String("delivery_id", msg.DeliveryID), zap.Error(err))

		return nil

	}

	// making the http post request taking from post function

	statusCode, durationMs, callErr := d.post(ctx, webhook.TargetURL, msg.EventType, msg.Payload)

	if callErr != nil || (statusCode != nil && (*statusCode < 200 || *statusCode >= 300)) {

		errMsg := ""

		if callErr != nil {
			errMsg = callErr.Error()
		} else {
			errMsg = fmt.Sprintf("non-2xx status code: %d", *statusCode)
		}

		// updating the log row to failed

		_ = d.deliveryStore.UpdateResult(ctx, logRow.ID, store.StatusFailure, statusCode, durationMs, &errMsg)

		logger.Log.Warn("delivery_failed", zap.String("delivery_id", msg.DeliveryID), zap.String("target_url", webhook.TargetURL), zap.Int("attempt", msg.Attempt), zap.String("error", errMsg))

		return d.retrier.HandleFailure(ctx, msg)

	}

	// updating result , when succeeded

	_ = d.deliveryStore.UpdateResult(ctx, logRow.ID, store.StatusSuccess, statusCode, durationMs, nil)

	logger.Log.Info("delivery succeeded", zap.String("delivery_id", msg.DeliveryID), zap.String("target_url", webhook.TargetURL), zap.Int("attempt", msg.Attempt), zap.Int("status_code", *statusCode), zap.Int("duration_ms", *durationMs))

	return nil // deleted from sqs

}

// post function makes an POST request and returns the statuscode , duration or any errors if occurs

func (d *Dispatcher) post(ctx context.Context, targetURL, eventType string, payload []byte) (*int, *int, error) {

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(payload))

	if err != nil {
		return nil, nil, fmt.Errorf("build request: %w", err)
	}

	// headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Webhook-Event", eventType)
	req.Header.Set("User-Agent", "WebhookDelivery/1.0")

	start := time.Now()
	// Do will sent an http request and returns an http response
	resp, err := d.httpClient.Do(req)
	durationMs := int(time.Since(start).Microseconds())

	if err != nil {
		return nil, &durationMs, fmt.Errorf("HTTP POST: %w", err)
	}

	defer resp.Body.Close()

	code := resp.StatusCode
	return &code, &durationMs, nil

}
