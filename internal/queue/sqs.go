// AWS - SQS - for publishing and consuming messages

package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	appconfig "github.com/suthar345piyush/internal/config"
	"github.com/suthar345piyush/internal/logger"
	"go.uber.org/zap"
)

// webhook message is payload that we publish to sqs, when webhook event arrives
// the worker will deserialises this to know what to deliver
// it will be json payload

type WebhookMessage struct {
	DeliveryID string `json:"delivery_id"` // X-Github-Delivery header value
	WebhookID  string `json:"webhook_id"`
	EventType  string `json:"event_type"` // push, pull req, etc..
	Attempt    int    `json:"attempt"`    // this will start with 1, increment on retry
	Payload    []byte `json:"payload"`
}

// client will wraps the aws sqs sdk with our application queue operation

type Client struct {
	svc      *sqs.Client
	queueURL string // main queue url
	dlqURL   string // dead letter queue url
}

// now function to create sqs client using env vars and config

func New(cfg *appconfig.Config) (*Client, error) {

	awsCfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion(cfg.AWSRegion),
	)

	if err != nil {
		return nil, fmt.Errorf("queue: load aws config: %w", err)
	}

	return &Client{
		svc:      sqs.NewFromConfig(awsCfg),
		queueURL: cfg.SQSQueueURL,
		dlqURL:   cfg.SQSDLQUrl,
	}, nil

}

// publish function to send message to main delivery queue

func (c *Client) Publish(ctx context.Context, msg WebhookMessage) error {

	// marshal the body (msg), serialize it

	body, err := json.Marshal(msg)

	if err != nil {
		return fmt.Errorf("queue: marshal message: %w", err)
	}

	// using standard queues

	_, err = c.svc.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(c.queueURL),
		MessageBody: aws.String(string(body)),
	})

	if err != nil {
		return fmt.Errorf("queue: send message: %w", err)
	}

	logger.Log.Info("message published to SQS",
		zap.String("delivery_id", msg.DeliveryID),
		zap.String("event_type", msg.EventType),
		zap.Int("attempt", msg.Attempt),
	)

	return nil

}

// publish message with some delay to main queue  with sqs delivery delay maximum of 900, the message will not be visible to consumer until delay over

func (c *Client) PublishWithDelay(ctx context.Context, msg WebhookMessage, delaySec int) error {

	body, err := json.Marshal(msg)

	if err != nil {
		return fmt.Errorf("queue: marshal delayed message: %w", err)
	}

	_, err = c.svc.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:     aws.String(c.queueURL),
		MessageBody:  aws.String(string(body)),
		DelaySeconds: int32(delaySec),
	})

	if err != nil {
		return fmt.Errorf("queue: send delayed message: %w", err)
	}

	logger.Log.Info("retry message publised to SQS",

		zap.String("delivery_id", msg.DeliveryID),
		zap.Int("attempt", msg.Attempt),
		zap.Int("delay_seconds", delaySec),
	)

	return nil

}

// function to send the messages after the retry limit exhausted into DLQ of SQS

func (c *Client) SendToDLQ(ctx context.Context, msg WebhookMessage) error {

	body, err := json.Marshal(msg)

	if err != nil {
		return fmt.Errorf("queue: marshal DLQ message: %w", err)
	}

	_, err = c.svc.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(c.dlqURL),
		MessageBody: aws.String(string(body)),
	})

	if err != nil {
		return fmt.Errorf("queue: send message to DLQ: %w", err)
	}

	logger.Log.Warn("message moved to DLQ - all retries exhausted",

		zap.String("delivery_id", msg.DeliveryID),
		zap.String("webhook_id", msg.WebhookID),
		zap.Int("attempt", msg.Attempt),
	)

	return nil

}

//consume function - it will read messages from sqs and call handler for each one of them
// it basically a long Poll function

func (c *Client) Consume(ctx context.Context, handler func(ctx context.Context, msg WebhookMessage) error) {

	logger.Log.Info("SQS consumer started", zap.String("queue", c.queueURL))

	// if their is an context cancellation between polls

	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("SQS consumer stopped")
			return
		default:
		}

		output, err := c.svc.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(c.queueURL),

			MaxNumberOfMessages:         10,
			WaitTimeSeconds:             20,
			VisibilityTimeout:           30,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
		})

		if err != nil {
			// context cancelled - exit safely
			if ctx.Err() != nil {
				return
			}

			logger.Log.Error("SQS ReceiveMessage failed", zap.Error(err))
			continue
		}

		for _, sqsMsg := range output.Messages {
			sqsMsg := sqsMsg

			var msg WebhookMessage

			if err := json.Unmarshal([]byte(aws.ToString(sqsMsg.Body)), &msg); err != nil {

				logger.Log.Error("failed to unmarshal SQS message body", zap.Error(err),
					zap.String("body", aws.ToString(sqsMsg.Body)),
				)

				c.deleteMessage(ctx, sqsMsg.ReceiptHandle)
				continue
			}

			if err := handler(ctx, msg); err != nil {
				logger.Log.Error("handler failed - message will become visible again", zap.Error(err), zap.String("delivery_id", msg.DeliveryID))

				continue
			}

			// handler succeeded - remove from the queue

			c.deleteMessage(ctx, sqsMsg.ReceiptHandle)

		}

	}

}

// delete message function
// receipt handle with the message which going to delete

func (c *Client) deleteMessage(ctx context.Context, receiptHandle *string) {

	_, err := c.svc.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURL),
		ReceiptHandle: receiptHandle,
	})

	if err != nil {
		logger.Log.Error("failed to delete SQS message", zap.Error(err))
	}
}
