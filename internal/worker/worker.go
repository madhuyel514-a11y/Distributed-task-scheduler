package worker

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"distributed-task-scheduler/internal/log"
	"distributed-task-scheduler/internal/queue"
	"distributed-task-scheduler/internal/storage"
	"distributed-task-scheduler/internal/task"
)

// Worker represents a single task processing unit in the distributed scheduler.
// Each worker runs in its own goroutine and continuously pulls tasks from the queue
// and processes them independently. Multiple workers form a worker pool.
type Worker struct {
	// ID is a unique identifier for this worker (e.g., "worker-1", "worker-2").
	// Used for logging and monitoring which worker processes which task.
	ID string

	// queue is a reference to the shared task queue.
	// All workers in the pool read from the same queue (producer-consumer pattern).
	queue queue.DistributedQueue

	// postgres persists task status transitions across processes.
	postgres *storage.PostgresStore

	// logger is used for structured logging of task execution.
	// Provides consistent log format across all workers.
	logger *log.StructuredLogger
}

// NewWorker creates a new worker with the given ID and queue reference.
//
// Parameters:
//   - id: unique worker identifier
//   - q: shared distributed queue implementation
//   - postgres: PostgreSQL task store used for status persistence
//   - logger: structured logger for task execution events
//
// Example:
//
//	worker := NewWorker("worker-1", taskQueue, structuredLogger)
func NewWorker(
	id string,
	q queue.DistributedQueue,
	postgres *storage.PostgresStore,
	logger *log.StructuredLogger,
) *Worker {
	return &Worker{
		ID:       id,
		queue:    q,
		postgres: postgres,
		logger:   logger,
	}
}

// Execute starts the worker's main loop and blocks until context is cancelled.
// This is the entry point for running a worker in a goroutine.
//
// Behavior:
//   - Continuously polls the queue for tasks
//   - Processes each task (simulated with time.Sleep)
//   - Updates task status
//   - Logs execution details (using structured logging)
//   - Gracefully shuts down when context is cancelled
//
// This method is intended to be called as: go worker.Execute(ctx)
func (w *Worker) Execute(ctx context.Context) {
	// Log worker startup for observability.
	w.logger.WorkerStarted(w.ID)

	// Infinite loop: worker continuously waits for tasks.
	// This runs until context is cancelled (by scheduler shutdown or timeout).
	// WHY INFINITE LOOP?
	// - In a real distributed system, workers don't know when the last task will arrive
	// - Tasks can arrive at any time from any publisher
	// - Exiting the loop would stop the worker, wasting compute resources
	// - The scheduler controls worker lifetime via context cancellation, not task count
	for {
		// Check if context has been cancelled (shutdown signal from scheduler).
		// This enables graceful shutdown: scheduler calls cancel(), workers exit cleanly.
		select {
		case <-ctx.Done():
			// Log shutdown and exit
			w.logger.WorkerShutdown(w.ID, 0) // 0 = tasks processed (would track this properly in production)
			return
		default:
			// Context not cancelled; continue to process tasks.
		}

		// Create a child context with a timeout for the dequeue operation.
		// Timeout prevents the worker from blocking indefinitely on empty queue.
		// If no task arrives within 5 seconds, we loop back and check ctx.Done() again.
		dequeueCtx, cancel := context.WithTimeout(ctx, 5*time.Second)

		// Dequeue the next task from the shared queue.
		// This blocks until:
		// - A task becomes available, OR
		// - The timeout expires, OR
		// - The context is cancelled (scheduler shutdown)
		t, err := w.queue.Dequeue(dequeueCtx)

		// Always cancel the child context to release resources (good practice).
		cancel()

		// Handle dequeue errors (timeout or context cancellation).
		if err != nil {
			// Timeout or cancellation error; not a fatal issue.
			// Just continue the loop: either try again (timeout) or exit (cancellation).
			continue
		}

		// If dequeue returned nil and no error, the queue was closed and is now empty.
		// This signals the scheduler has shut down gracefully; worker should exit.
		if t == nil {
			w.logger.WorkerShutdown(w.ID, 0)
			return
		}

		// Process the task with structured logging.
		w.processTask(ctx, t)
	}
}

// processTask executes a single task: updates status, simulates work, and logs results.
// Separated into its own method for readability and testability.
// All logging uses structured logging for consistency and observability.
func (w *Worker) processTask(ctx context.Context, t *task.Task) {
	// Record start time for latency calculation
	startTime := time.Now()

	// Log task dequeue event (picked up from queue)
	// Calculate how long task waited in queue before this worker got it
	queueWaitTime := startTime.Sub(t.CreatedAt)
	w.logger.TaskDequeued(w.ID, t.ID, queueWaitTime)

	// Update task status to "running" to signal other components that work is in progress.
	// In a distributed system, this state change would be written to persistent storage
	// (queue index) so other schedulers/dashboards can see the task is being worked on.
	t.SetStatus(task.StatusRunning)
	if err := w.postgres.UpdateTaskStatus(t.ID, task.StatusRunning, t.RetryCount, t.MaxRetries); err != nil {
		w.logger.Error("worker", w.ID, t.ID, err)
	}

	// Log task started event with structured logging
	w.logger.TaskStarted(w.ID, t.ID, t.Name)

	// Simulate task execution by sleeping for a random duration (1-3 seconds).
	// In production, this would be replaced with actual business logic:
	// - Database queries
	// - API calls
	// - Data transformations
	// - File processing
	executionTime := time.Duration(1+rand.Intn(3)) * time.Second
	time.Sleep(executionTime)

	// After simulated execution, check if we were told to shut down.
	// This prevents marking tasks as completed if shutdown was requested.
	select {
	case <-ctx.Done():
		// Task was interrupted during execution
		t.SetStatus(task.StatusFailed)
		if err := w.postgres.UpdateTaskStatus(t.ID, task.StatusFailed, t.RetryCount, t.MaxRetries); err != nil {
			w.logger.Error("worker", w.ID, t.ID, err)
		}
		w.logger.TaskFailed(w.ID, t.ID, fmt.Errorf("interrupted"), executionTime)
		return
	default:
		// Not cancelled; continue to mark task as complete.
	}

	// Simulate failed task execution for retry demonstration.
	// In production, replace this condition with real failure detection.
	if rand.Intn(2) == 0 {
		t.SetStatus(task.StatusFailed)
		if err := w.postgres.UpdateTaskStatus(t.ID, task.StatusFailed, t.RetryCount, t.MaxRetries); err != nil {
		}

		if t.CanRetry() {
			attempt := t.IncrementRetry()
			t.SetStatus(task.StatusRetrying)
			if err := w.postgres.UpdateTaskStatus(t.ID, task.StatusRetrying, t.RetryCount, t.MaxRetries); err != nil {
				w.logger.Error("worker", w.ID, t.ID, err)
			}
			w.logger.TaskRetry(w.ID, t.ID, attempt, t.MaxRetries, fmt.Errorf("simulated failure"))

			if err := w.queue.Enqueue(ctx, t); err != nil {
				w.logger.Error("worker", w.ID, t.ID, err)
			}
			return
		}

		w.logger.TaskRetriesExhausted(w.ID, t.ID, t.RetryCount, t.MaxRetries, fmt.Errorf("simulated failure"))
		return
	}

	// Simulate successful task execution by updating status to "completed".
	// In production, this would only happen if the actual work succeeded.
	t.SetStatus(task.StatusCompleted)
	if err := w.postgres.UpdateTaskStatus(t.ID, task.StatusCompleted, t.RetryCount, t.MaxRetries); err != nil {
		w.logger.Error("worker", w.ID, t.ID, err)
	}

	// Calculate total latency: time from task creation to completion
	totalLatency := time.Since(t.CreatedAt)

	// Log task completed event with performance metrics
	w.logger.TaskCompleted(w.ID, t.ID, executionTime, totalLatency)
}

// WorkerPool manages a collection of workers running in parallel.
// This demonstrates the worker pool pattern: N goroutines reading from 1 shared queue.
type WorkerPool struct {
	// workers holds all worker instances in this pool.
	workers []*Worker

	// queue is the shared task queue all workers consume from.
	queue queue.DistributedQueue

	// postgres persists task status transitions across processes.
	postgres *storage.PostgresStore

	// logger for pool-level logging.
	logger *log.StructuredLogger

	// wg is a WaitGroup that synchronizes worker completion.
	// Using WaitGroup allows the main program to wait for all workers to finish
	// before proceeding or shutting down. This prevents orphaned goroutines.
	wg sync.WaitGroup
}

// NewWorkerPool creates a new worker pool with the specified number of workers.
//
// Parameters:
//   - count: number of workers to spawn
//   - q: shared distributed queue
//   - postgres: PostgreSQL task store used for status persistence
//   - logger: structured logger instance
//
// Example:
//
//	pool := NewWorkerPool(5, taskQueue, structuredLogger)
func NewWorkerPool(
	count int,
	q queue.DistributedQueue,
	postgres *storage.PostgresStore,
	logger *log.StructuredLogger,
) *WorkerPool {
	pool := &WorkerPool{
		workers:  make([]*Worker, count),
		queue:    q,
		postgres: postgres,
		logger:   logger,
	}

	// Create worker instances.
	// Each worker has a unique ID based on its index.
	for i := 0; i < count; i++ {
		pool.workers[i] = NewWorker(
			fmt.Sprintf("worker-%d", i),
			q,
			postgres,
			logger,
		)
	}

	// Log worker pool creation
	logger.WorkerPoolStarted(count)

	return pool
}

// Start launches all workers in their own goroutines and tracks them with WaitGroup.
// The caller must eventually call Wait() or WaitWithTimeout() to block until all workers exit.
//
// Behavior:
//   - Each worker runs in its own goroutine
//   - All workers share the same queue
//   - WaitGroup counter incremented for each worker
//   - Caller can block on Wait() to ensure all workers complete before shutdown
//
// Example:
//
//	ctx, cancel := context.WithCancel(context.Background())
//	pool.Start(ctx)
//	// ... add tasks to queue ...
//	cancel()  // Signal workers to stop
//	pool.Wait()  // Block until all workers have exited
func (wp *WorkerPool) Start(ctx context.Context) {
	wp.logger.Info("worker", "Starting worker pool...")

	// Launch each worker in its own goroutine.
	// This is how we achieve parallelism: each worker runs concurrently.
	for _, w := range wp.workers {
		// Increment WaitGroup counter for each worker being launched.
		// This tells the WaitGroup: "expect one more goroutine to call Done()"
		// If we don't do this, Wait() won't know to wait for this worker.
		wp.wg.Add(1)

		// Launch worker in a new goroutine.
		// WHY GOROUTINES?
		// - Lightweight: thousands of goroutines on a single machine
		// - Simple: no thread management or context switching overhead
		// - Efficient: blocked on I/O doesn't prevent others from running
		// - Natural: Go scheduler handles load balancing automatically
		go func(worker *Worker) {
			// CRITICAL: Call Done() when this goroutine exits.
			// This decrements the WaitGroup counter so Wait() knows this worker finished.
			// defer ensures it's called even if Execute() panics.
			defer wp.wg.Done()

			// Run the worker's main loop.
			// This blocks until context is cancelled or queue is closed.
			worker.Execute(ctx)
		}(w)
	}

	wp.logger.Info("worker", fmt.Sprintf("All %d workers started and waiting for tasks", len(wp.workers)))
}

// Wait blocks until all workers have exited.
// This is typically called after cancelling the context to ensure graceful shutdown.
//
// Example:
//
//	ctx, cancel := context.WithCancel(context.Background())
//	pool.Start(ctx)
//	time.Sleep(10 * time.Second)  // Let workers process tasks
//	cancel()  // Signal shutdown
//	pool.Wait()  // Block here until all workers exit
func (wp *WorkerPool) Wait() {
	wp.logger.Info("worker", "Waiting for all workers to complete...")
	// This blocks the calling goroutine until the WaitGroup counter reaches 0.
	// Counter reaches 0 when all workers have called Done() (via defer in Start).
	wp.wg.Wait()
	wp.logger.Info("worker", "All workers completed")
}

// WaitWithTimeout blocks until all workers exit or timeout is reached.
// Returns true if all workers completed, false if timeout occurred.
//
// Useful for enforcing maximum shutdown time (e.g., graceful shutdown with hard limit).
func (wp *WorkerPool) WaitWithTimeout(timeout time.Duration) bool {
	wp.logger.Info("worker", fmt.Sprintf("Waiting for all workers (timeout: %v)...", timeout))

	// Create a channel to signal when WaitGroup reaches 0.
	done := make(chan struct{})

	// Launch a goroutine that waits for the WaitGroup.
	// When wg.Wait() returns, close the done channel to unblock the select below.
	go func() {
		wp.wg.Wait()
		close(done)
	}()

	// Race between timeout and worker completion.
	select {
	case <-done:
		wp.logger.Info("worker", "All workers completed within timeout")
		return true
	case <-time.After(timeout):
		wp.logger.Info("worker", "Timeout waiting for workers to complete")
		return false
	}
}

// Size returns the number of workers in this pool.
func (wp *WorkerPool) Size() int {
	return len(wp.workers)
}
