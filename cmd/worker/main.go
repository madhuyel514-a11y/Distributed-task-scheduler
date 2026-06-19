package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	internalLog "distributed-task-scheduler/internal/log"
	"distributed-task-scheduler/internal/queue"
	"distributed-task-scheduler/internal/storage"
	"distributed-task-scheduler/internal/worker"
)

func main() {
	logger := internalLog.NewStructuredLogger()
	logger.Info("system", "Starting worker process")

	rq, err := queue.NewRedisQueue(
		"localhost:6379",
		"scheduler-tasks",
	)
	if err != nil {
		log.Fatalf("failed to connect to redis: %v", err)
	}
	defer rq.Close()

	postgresStore, err := storage.NewPostgresStore(
		"localhost",
		5432,
		"postgres",
		"postgres",
		"scheduler",
	)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer postgresStore.Close()

	pool := worker.NewWorkerPool(3, rq, postgresStore, logger)
	logger.Info("system", "Worker pool created")

	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	pool.Start(ctx)
	logger.Info("system", "Workers started")

	<-ctx.Done()
	logger.Info("system", "Shutdown signal received for worker process")

	if pool.WaitWithTimeout(10 * time.Second) {
		logger.Info("system", "All workers exited cleanly")
	} else {
		logger.Info("system", "Timeout: Some workers still running")
	}
}
