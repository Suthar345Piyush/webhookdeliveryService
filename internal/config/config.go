// config file, loading all the env vars in these

package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// config struct
type Config struct {
	AppPort string // server port

	DBURL string // database url

	// aws SQS structs

	AWSRegion          string
	AWSAccessKeyID     string
	AWSSecretAccessKey string
	SQSQueueURL        string
	SQSDLQUrl          string

	//webhook struct

	WebhookSecret string

	// delivery struct

	MaxRetryAttempts       int
	DeliveryTimeoutSeconds int
	WorkerConcurrency      int
}

// getting the env vars here

func Load() (*Config, error) {

	_ = godotenv.Load()

	cfg := &Config{
		AppPort:            getRequired("APP_PORT"),
		DBURL:              getRequired("DB_URL"),
		AWSRegion:          getRequired("AWS_REGION"),
		AWSAccessKeyID:     getRequired("AWS_ACCESS_KEY_ID"),
		AWSSecretAccessKey: getRequired("AWS_SECRET_ACCESS_KEY"),
		SQSQueueURL:        getRequired("SQS_QUEUE_URL"),
		SQSDLQUrl:          getRequired("SQS_DLQ_URL"),
		WebhookSecret:      getRequired("WEBHOOK_SECRET"),
	}

	var err error

	cfg.MaxRetryAttempts, err = getInt("MAX_RETRY_ATTEMPTS", 5)

	if err != nil {
		return nil, fmt.Errorf("config: MAX_RETRY_ATTEMPTS: %w", err)
	}

	cfg.DeliveryTimeoutSeconds, err = getInt("DELIVERY_TIMEOUT_SECONDS", 10)

	if err != nil {
		return nil, fmt.Errorf("config: DELIVERY_TIMEOUT_SECONDS: %w", err)
	}

	cfg.WorkerConcurrency, err = getInt("WORKER_CONCURRENCY", 5)

	if err != nil {
		return nil, fmt.Errorf("config: WORKER_CONCURRENCY: %w", err)
	}

	return cfg, nil

}

// required env getting function

func getRequired(key string) string {
	v := os.Getenv(key)

	if v == "" {
		panic(fmt.Sprintf("config: required env %q is not set", key))
	}
	return v
}

// get integer function, considering a default value along with

func getInt(key string, defaultVal int) (int, error) {

	v := os.Getenv(key)

	if v == "" {
		return defaultVal, nil
	}

	n, err := strconv.Atoi(v)

	if err != nil {
		return 0, fmt.Errorf("expected integer, got %q", v)
	}

	return n, nil

}
