// webhook handler will takes the webhook event(url), then push that into SQS queue,and then returns 200 response to the spot

package handler

import (
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/suthar345piyush/internal/logger"
	"github.com/suthar345piyush/internal/queue"
	"go.uber.org/zap"
)

// webhook handler will hold the dependencies, which needed to handle the incoming webhook events

type WebhookHandler struct {
	queue *queue.Client
}

// new webhook handler

func NewWebhookHandler(q *queue.Client) *WebhookHandler {
	return &WebhookHandler{queue: q}
}

// receive function, it will handle the  POST /webhooks/:webhookID/events

// signature already verified by middleware

func (w *WebhookHandler) Receive(c fiber.Ctx) error {

	// parsing the webhook id  from url path

	webhookIDStr := c.Params("webhookID")
	webhookID, err := uuid.Parse(webhookIDStr)

	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid webhook_id in path",
		})
	}

	// "X-Github-Delivery" is custom HTTP header, which sent by github in webhook POST request, and we are using this as our "idempotency" key

	deliveryIDStr := c.Get("X-Github-Delivery")

	if deliveryIDStr == "" {
		deliveryIDStr = uuid.New().String()
	}

	// validate the delivery id , to check the id is valid uuid

	if _, err := uuid.Parse(deliveryIDStr); err != nil {
		deliveryIDStr = uuid.New().String()
	}

	// now we will take event type from header (example- push , pull request, etc...)

	eventType := c.Get("X-Github-Event")

	if eventType == "" {
		eventType = "unknown"
	}

	// raw body is stored by middleware

	rawBody, _ := c.Locals("rawBody").([]byte)

	if rawBody == nil {
		rawBody = c.Body()
	}

	msg := queue.WebhookMessage{
		DeliveryID: deliveryIDStr,
		WebhookID:  webhookID.String(),
		EventType:  eventType,
		Attempt:    1,
		Payload:    rawBody,
	}

	if err := w.queue.Publish(c.Context(), msg); err != nil {
		logger.Log.Error("failed to publish webhook message to SQS", zap.String("delivery_id", deliveryIDStr), zap.String("webhook_id", webhookID.String()), zap.Error(err))

		// returns 500 so that user can retry

		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to queue event",
		})
	}

	// if every thing goes right

	logger.Log.Info("webhook event received and pushed to queue", zap.String("delivery_id", deliveryIDStr), zap.String("webhook_id", webhookID.String()), zap.String("event_type", eventType))

	// at last return 200 immediately

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status":      "queued",
		"delivery_id": deliveryIDStr,
	})

}
