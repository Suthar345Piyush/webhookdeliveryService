// main server file

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/suthar345piyush/internal/config"
	"github.com/suthar345piyush/internal/handler"
	"github.com/suthar345piyush/internal/logger"
	"github.com/suthar345piyush/internal/middleware"
	"github.com/suthar345piyush/internal/queue"
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

	// db

	ctx := context.Background()

	db, err := pgxpool.New(ctx, cfg.DBURL)
	if err != nil {
		logger.Log.Fatal("failed to connect to postgres", zap.Error(err))
	}

	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		logger.Log.Fatal("postgres ping failed", zap.Error(err))
	}

	logger.Log.Info("connected to postgres")

	// AWS-SQS client

	sqsClient, err := queue.New(cfg)

	if err != nil {
		logger.Log.Fatal("failed to create SQS client", zap.Error(err))
	}

	// fiber application
	// will returns error in json format instead of normally plain format

	app := fiber.New(fiber.Config{

		ErrorHandler: func(c fiber.Ctx, err error) error {

			code := fiber.StatusInternalServerError

			if e, ok := err.(*fiber.Error); ok {
				code = e.Code
			}

			return c.Status(code).JSON(fiber.Map{"error": err.Error()})
		},

		// read the body so that signature can hash it

		ReadBufferSize: 8192, // around 8KB
	})

	// global middleware

	// it catch panics and returns 500 code , instead of crash
	app.Use(recover())

	// routes

	webhookHandler := handler.NewWebhookHandler(sqsClient)

	// post request /webhooks/:webhookID/events

	app.Post("/webhooks/:webhookID/events", middleware.VerifySignature(cfg.WebhookSecret), webhookHandler.Receive)

	// health check

	app.Get("/health", func(c fiber.Ctx) error {
		return c.Status(fiber.StatusOK).JSON(fiber.Map{"status": "ok"})
	})

	// graceful shutdown

	quit := make(chan os.Signal, 1)

	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	// to listen for the shutdown signal starting server in a goroutine

	go func() {

		addr := fmt.Sprintf(":%s", cfg.AppPort)
		logger.Log.Info("server starting", zap.String("addr", addr))

		if err := app.Listen(addr); err != nil {
			logger.Log.Fatal("server error", zap.Error(err))
		}

	}()

	<-quit

	logger.Log.Info("shutdown signal received")

	// giving inflight request up to 10 seconds

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	defer cancel()

	if err := app.ShutdownWithContext(shutdownCtx); err != nil {
		logger.Log.Error("error during server shutdown", zap.Error(err))
	}

	logger.Log.Info("server stopped")

}
