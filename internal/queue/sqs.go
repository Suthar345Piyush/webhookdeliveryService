// AWS - SQS - for publishing and consuming messages

package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
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
