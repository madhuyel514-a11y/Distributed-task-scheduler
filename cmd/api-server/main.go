package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"distributed-task-scheduler/api"
	internalLog "distributed-task-scheduler/internal/log"
	"distributed-task-scheduler/internal/queue"
	"distributed-task-scheduler/internal/scheduler"
	"distributed-task-scheduler/internal/storage"
)

func main() {
	logger := internalLog.NewStructuredLogger()
	logger.Info("system", "Starting API server process")

	rq, err := queue.NewRedisQueue(
		"localhost:6379",
		"scheduler-tasks",
	)
	if err != nil {
		log.Fatalf("failed to connect to redis: %v", err)
	}
	defer rq.Close()

	store := storage.NewMemoryStore()
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

	sched := scheduler.NewScheduler(
		rq,
		store,
		postgresStore,
		logger,
	)

	handler := &api.Handler{
		Scheduler: sched,
	}

	mux := http.NewServeMux()
	api.RegisterRoutes(mux, handler)

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	errCh := make(chan error, 1)

	go func() {
		logger.Info("system", "API server listening on :8080")
		errCh <- server.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("api server failed: %v", err)
		}
	case <-ctx.Done():
		logger.Info("system", "Shutdown signal received for API server")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("system", "", "", err)
	}

	if err := sched.Shutdown(); err != nil {
		logger.Error("system", "", "", err)
	}

	stats := sched.Stats()
	logger.Info("system", fmt.Sprintf("API server stopped: TotalSubmitted=%d, QueueSize=%d, Running=%v",
		stats["total_submitted"], stats["queue_size"], stats["is_running"]))
}
