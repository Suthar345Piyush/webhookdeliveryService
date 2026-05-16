// retry with exponential backoff with jitter and send request to DLQ after max attempts or retries

package delivery

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/suthar345piyush/internal/logger"
	"github.com/suthar345piyush/internal/queue"
	"go.uber.org/zap"
)

// retrier struct, used when any delivery attempt failed, either it will push back to the queue or it will send to the DLQ
type Retrier struct {
	sqsClient   *queue.Client
	maxAttempts int
}

// new retrier function

func NewRetrier(sqsClient *queue.Client, maxAttempts int) *Retrier {
	return &Retrier{
		sqsClient:   sqsClient,
		maxAttempts: maxAttempts,
	}
}

// handle failure function , when our delivery attempt failed, either push back to queue or sent the message to DLQ

func (r *Retrier) HandleFailure(ctx context.Context, msg queue.WebhookMessage) error {

	// if message attempt exceeded max attempts, the push it to DLQ

	if msg.Attempt >= r.maxAttempts {

		if err := r.sqsClient.SendToDLQ(ctx, msg); err != nil {
			logger.Log.Error("failed to send message to DLQ", zap.String("delivery_id", msg.DeliveryID), zap.Error(err))

			return fmt.Errorf("send to DLQ: %w", err)
		}

		// at last log everything like it's delivery id , webhook id, his attempts, maximum attempts

		logger.Log.Warn("max retries reached - moved to DLQ", zap.String("delivery_id", msg.DeliveryID), zap.String("webhook_id", msg.WebhookID), zap.Int("attempts", msg.Attempt), zap.Int("max_attempts", r.maxAttempts))

		return nil
	}

	// now will increment the attempt by scheduling a retry by publishing a new message

	newMsg := msg
	newMsg.Attempt = msg.Attempt + 1

	// time frame between new attempt to sent message
	//exponential backoff

	delay := backoffDelay(msg.Attempt)

	logger.Log.Info("scheduling retry", zap.String("delivery_id", msg.DeliveryID), zap.Int("new_attempt", newMsg.Attempt), zap.Duration("delay", delay))

	if err := r.publishWithDelay(ctx, newMsg, delay); err != nil {
		logger.Log.Error("failed to re-enqueue retry message", zap.String("delivery_id", newMsg.DeliveryID), zap.Error(err))

		return fmt.Errorf("re-enqueue: %w", err)
	}

	return nil

}

// exponential backoff implentation  in backoffDelay function with full jitter for a given attempt number (all are 1 indexed)

// exponential backoff formula - min(cap, base * 2^attempt-1) with random jitter in between the (0 to exponential backoff limit)

/*

the whole process runs like this:

Attempt 1 failure -> wait - 5s before attempt 2

Attempt 2 failure -> wait - 10s

Attempt 3 failure -> wait - 20s

Attempt 4 failure -> wait - 40s

Attempt 5 failure -> sent to dead letter queue (DLQ)
*/

func backoffDelay(attempt int) time.Duration {

	const (
		base    = 5 * time.Second
		capTime = 15 * time.Minute
	)

	//exponential backoff -> base * 2^attempt-1

	exp := math.Pow(2, float64(attempt-1))

	backoffLimit := time.Duration(float64(base) * exp)

	if backoffLimit > capTime {
		backoffLimit = capTime
	}

	// applying full jitter, it choose random wait time between 0 to backoffLimit

	// it prevents all clients to hit the server at the same specified wait time

	jitter := time.Duration(rand.Int63n(int64(backoffLimit) + 1))

	return jitter

}

// publish with delay function, it publishes a message to SQS with the given delay
// sqs delay seconds are 900 seconds (15 minutes)

func (r *Retrier) publishWithDelay(ctx context.Context, msg queue.WebhookMessage, delay time.Duration) error {

	delaySec := int(delay.Seconds())

	if delaySec > 900 {
		delaySec = 900
	}

	if delaySec < 0 {
		delaySec = 0
	}

	return r.sqsClient.PublishWithDelay(ctx, msg, delaySec)

}
