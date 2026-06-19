package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"distributed-task-scheduler/api"
	internalLog "distributed-task-scheduler/internal/log"
	"distributed-task-scheduler/internal/queue"
	"distributed-task-scheduler/internal/scheduler"
	"distributed-task-scheduler/internal/storage"
	"distributed-task-scheduler/internal/worker"
)

// This example demonstrates a complete distributed task scheduler with concurrent workers.
//
// PROGRAM FLOW:
// 1. Initialize: Create queue, scheduler, and worker pool
// 2. Start: Launch 3 workers in separate goroutines
// 3. Submit: Add 10 tasks via scheduler (non-blocking)
// 4. Process: Workers compete for tasks from shared queue (concurrent)
// 5. Shutdown: Gracefully stop, wait for completion
//
// GOROUTINE ARCHITECTURE:
// - Main goroutine: Orchestrates setup, submission, shutdown (THIS FUNCTION)
// - Worker goroutine 1: Continuously dequeue → process → repeat
// - Worker goroutine 2: Continuously dequeue → process → repeat
// - Worker goroutine 3: Continuously dequeue → process → repeat
//
// TOTAL: 4 goroutines running concurrently
//
// EXPECTED OUTPUT:
// ✓ All workers started
// Publishing 10 tasks via scheduler...
// → Task submitted: ID=task-1, Name=job_1
// → Task submitted: ID=task-2, Name=job_2
// ...
// [Worker-1] Picked up task: ID=task-1, Name=job_1
// [Worker-2] Picked up task: ID=task-2, Name=job_2
// [Worker-3] Picked up task: ID=task-3, Name=job_3
// [Worker-1] Task task-1 completed (after finishing task-1, picks up task-4)
// ... and so on, all running concurrently

func main() {
	// ============================================================================
	// PHASE 1: SETUP - Initialize components
	// ============================================================================
	// Goroutines active: 1 (main)

	// Create a structured logger for consistent, observable logging across the system.
	// This logger provides:
	// - Timestamps (millisecond precision)
	// - Log levels (INFO, WARN, ERROR)
	// - Context (worker ID, task ID)
	// - Structured fields for parsing and monitoring
	logger := internalLog.NewStructuredLogger()

	fmt.Println("\n╔════════════════════════════════════════════════════════════╗")
	fmt.Println("║    Distributed Task Scheduler - Concurrent Execution       ║")
	fmt.Println("║    With Structured Logging for Observability               ║")
	fmt.Println("╚════════════════════════════════════════════════════════════╝\n")

	logger.Info("system", "Starting Distributed Task Scheduler")

	// Create a Redis-backed task queue shared across schedulers and workers.
	rq, err := queue.NewRedisQueue(
		"localhost:6379",
		"scheduler-tasks",
	)
	if err != nil {
		log.Fatalf("failed to connect to redis: %v", err)
	}
	defer rq.Close()
	logger.Info("system", "Redis queue created (queue: scheduler-tasks)")

	// Create the in-memory task store shared by the scheduler.
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

	// Create a worker pool with 3 workers.
	// Each worker will be launched in its own goroutine and will compete for tasks.
	// All workers read from the SAME shared queue (producer-consumer pattern).
	pool := worker.NewWorkerPool(3, rq, postgresStore, logger)
	logger.Info("system", fmt.Sprintf("Worker pool created (%d workers)", pool.Size()))

	// Create a scheduler to accept tasks from clients.
	// The scheduler:
	// - Validates input
	// - Assigns unique IDs (atomic counter)
	// - Sets status to "pending"
	// - Enqueues to the shared queue
	sched := scheduler.NewScheduler(
		rq,
		store,
		postgresStore,
		logger,
	)
	logger.Info("system", "Scheduler created")

	handler := &api.Handler{
		Scheduler: sched,
	}

	mux := http.NewServeMux()

	api.RegisterRoutes(
		mux,
		handler,
	)

	go func() {
		fmt.Println("API Server running on :8080")
		_ = http.ListenAndServe(":8080", mux)
	}()

	// ============================================================================
	// PHASE 2: START - Launch workers
	// ============================================================================
	// Goroutines active: 1 (main) + 3 (workers) = 4 total

	logger.Info("system", "Starting worker pool...")

	// Create a context for controlling all workers' lifecycle.
	// ctx is passed to all workers; calling cancel() signals shutdown to all.
	ctx, cancel := context.WithCancel(context.Background())

	// Start all workers in their own goroutines.
	// This call:
	// 1. Increments WaitGroup by 3 (one per worker)
	// 2. Launches 3 separate goroutines
	// 3. Each goroutine calls worker.Execute(ctx)
	// 4. Returns immediately (non-blocking)
	//
	// After Start() returns:
	// - Main goroutine continues
	// - 3 worker goroutines are running in background
	// - Workers are waiting on queue.Dequeue() (blocked until task available)
	pool.Start(ctx)
	logger.Info("system", "All workers started and waiting for tasks")

	// Brief pause to ensure workers are fully started and waiting.
	// This is optional but helps with predictable ordering.
	time.Sleep(100 * time.Millisecond)

	// ============================================================================
	// PHASE 3: SUBMIT - Add tasks via scheduler
	// ============================================================================
	// Goroutines active: 4 (main + 3 workers)
	// Main thread: Calling scheduler.SubmitTask()
	// Worker threads: Waiting on queue.Dequeue()

	logger.Info("system", "Publishing 10 tasks through scheduler...")

	// Create a context with timeout for submission operations.
	// If submission takes longer than 5 seconds, operations will fail.
	submitCtx, submitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer submitCancel()

	// Submit 10 tasks to the system.
	// Each task is:
	// 1. Validated by scheduler
	// 2. Assigned a unique ID (task-1, task-2, ..., task-10)
	// 3. Added to the shared queue
	// 4. Immediately available for workers to pick up
	//
	// IMPORTANT: Tasks are added to queue IMMEDIATELY.
	// They don't wait for workers to be "ready".
	// As tasks appear in queue, workers wake up from Dequeue() and process them.
	taskIDs := make([]string, 0, 10)
	for i := 1; i <= 10; i++ {
		taskName := fmt.Sprintf("job_%d", i)
		payload := []byte(fmt.Sprintf("processing batch %d with priority data", i))

		// Call scheduler's public API to submit a task.
		// This is non-blocking: returns immediately with assigned task ID.
		// Scheduler enqueues the task, which wakes up a waiting worker.
		taskID, err := sched.SubmitTask(submitCtx, taskName, payload)
		if err != nil {
			logger.Error("system", "", "", err)
			continue
		}

		// Successful submission logged by scheduler's structured logging
		taskIDs = append(taskIDs, taskID)
	}

	logger.Info("system", fmt.Sprintf("Submitted %d tasks to the queue", len(taskIDs)))

	// ============================================================================
	// ============================================================================
	// PHASE 4: PROCESSING - Workers execute tasks concurrently
	// ============================================================================
	// Goroutines active: 4 (main + 3 workers)
	// Main thread: Sleeping
	// Worker threads: Processing tasks from queue, executing in parallel
	//
	// CONCURRENCY IN ACTION:
	// Time 0.0s:  [Worker-1] Picks up Task-1, starts execution (1-3 sec)
	//             [Worker-2] Picks up Task-2, starts execution (1-3 sec)
	//             [Worker-3] Picks up Task-3, starts execution (1-3 sec)
	// Time 2.0s:  [Worker-1] Finishes Task-1, picks up Task-4
	//             [Worker-2] Still executing Task-2
	//             [Worker-3] Still executing Task-3
	// Time 3.5s:  [Worker-2] Finishes Task-2, picks up Task-5
	//             [Worker-1] Still executing Task-4
	//             [Worker-3] Still executing Task-3
	// ... and so on

	logger.Info("system", "Workers processing 10 tasks concurrently from shared queue...")

	/*
	// Let workers process tasks for 20 seconds.
	// Since each task takes 1-3 seconds and we have 3 workers:
	// - Sequential: 10 tasks × 2 sec average = 20 seconds
	// - Parallel (3 workers): ~7 seconds (10/3 ≈ 3.3 batches)
	// 20 seconds is enough for all tasks to complete.
	time.Sleep(20 * time.Second)

	logger.Info("system", "Initiating graceful shutdown...")

	// ============================================================================
	// PHASE 5: SHUTDOWN - Gracefully stop all workers
	// ============================================================================

	// Step 1: Stop scheduler from accepting new tasks.
	// Existing tasks in queue will continue processing.
	if err := sched.Shutdown(); err != nil {
		logger.Error("system", "", "", err)
	}

	// Display final statistics.
	stats := sched.Stats()
	logger.Info("system", fmt.Sprintf("Scheduler stats: TotalSubmitted=%d, QueueSize=%d, Running=%v",
		stats["total_submitted"], stats["queue_size"], stats["is_running"]))

	// Step 2: Close the queue.
	// Signals to workers that no more tasks will arrive.
	// Workers currently processing will continue; idle workers will exit after draining.
	if err := q.Close(); err != nil {
		logger.Error("system", "", "", err)
	}

	// Step 3: Cancel context.
	// This signals all workers to exit their main loops after current task.
	// Each worker:
	// 1. Checks ctx.Done() on next loop iteration
	// 2. Calls Done() on its WaitGroup (defer ensures this)
	// 3. Goroutine exits
	cancel()

	// Step 4: Wait for all workers to finish.
	// Blocks here until:
	// - All 3 workers have called Done() (via defer in Start())
	// - WaitGroup counter reaches 0
	// - OR timeout (10 seconds) is reached
	//
	// This ensures no orphaned goroutines remain.
	logger.Info("system", "Waiting for all workers to complete (max 10s)...")
	if pool.WaitWithTimeout(10 * time.Second) {
		logger.Info("system", "All workers exited cleanly")
	} else {
		logger.Info("system", "Timeout: Some workers still running")
	}

	// ============================================================================
	// FINAL REPORT
	// ============================================================================
	// Goroutines active: 1 (main only; worker goroutines all exited)

	fmt.Println("\n╔════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    EXECUTION COMPLETE                      ║")
	fmt.Println("╚════════════════════════════════════════════════════════════╝")

	logger.Info("system", fmt.Sprintf("Summary: Submitted=%d, FinalQueueSize=%d, AllProcessed=true, GracefulShutdown=true", len(taskIDs), q.Size()))
	*/

	_ = cancel
	_ = pool
	_ = taskIDs

	select {}
}
