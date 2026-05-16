// this SQS - worker(consumer) with concurrent goroutines
// it's an worker entry point

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/suthar345piyush/internal/config"
	"github.com/suthar345piyush/internal/delivery"
	"github.com/suthar345piyush/internal/logger"
	"github.com/suthar345piyush/internal/queue"
	"github.com/suthar345piyush/internal/store"
	"go.uber.org/zap"
)

func main() {

	// logger

	if err := logger.Init(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to init logger: %v\n", err)
		os.Exit(1)
	}

	defer logger.Sync()

	// config

	cfg, err := config.Load()

	if err != nil {
		logger.Log.Fatal("failed to load config", zap.Error(err))
	}

	// db - postgres

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	db, err := pgxpool.New(ctx, cfg.DBURL)
	if err != nil {
		logger.Log.Fatal("failed to connect to postgres", zap.Error(err))
	}

	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		logger.Log.Fatal("postgres ping failed", zap.Error(err))
	}

	logger.Log.Info("connected to postgres")

	// SQS client dependencies

	sqsClient, err := queue.New(cfg)

	if err != nil {
		logger.Log.Fatal("failed to create SQS client", zap.Error(err))
	}

	webhookStore := store.NewWebhookStore(db)
	deliveryStore := store.NewDeliveryStore(db)
	retrier := delivery.NewRetrier(sqsClient, cfg.MaxRetryAttempts)
	dispatcher := delivery.NewDispatcher(
		webhookStore,
		deliveryStore,
		retrier,
		cfg.DeliveryTimeoutSeconds,
	)

	// now we will start consumer go routine

	// each running on it's own consume loop, iterate on WorkerConcurrency
	// each goroutine can process upto 10 messages simultaneously
	// so the total throughput will be (WorkerConcurrency * 10 messages)

	var wg sync.WaitGroup

	for i := 0; i < cfg.WorkerConcurrency; i++ {
		wg.Add(1)

		go func(workerID int) {

			logger.Log.Info("worker goroutine started", zap.Int("worker_id", workerID))

			sqsClient.Consume(ctx, dispatcher.Dispatch)

			logger.Log.Info("worker goroutine stopped", zap.Int("worker_id", workerID))

		}(i)
	}

	// at last - graceful shutdown

	quit := make(chan os.Signal, 1)

	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	logger.Log.Info("worker started", zap.Int("concurrency", cfg.WorkerConcurrency))

	// block until signal received

	<-quit

	logger.Log.Info("shutdown signal received - stoping workers")

	cancel()

	//wait for all goroutine to complete

	wg.Wait()

	logger.Log.Info("all workers stopped - workers exiting")

}
