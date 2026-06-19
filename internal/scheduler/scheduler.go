package scheduler

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"distributed-task-scheduler/internal/log"
	"distributed-task-scheduler/internal/queue"
	"distributed-task-scheduler/internal/storage"
	"distributed-task-scheduler/internal/task"
)

// Scheduler is responsible for accepting new tasks from clients,
// assigning unique identifiers, and enqueueing them for workers to process.
//
// Separation of Concerns:
// - Scheduler: Ingestion & validation (input side)
// - Queue: Buffering & persistence (middle)
// - Workers: Execution (output side)
//
// This separation allows each layer to scale independently:
// - Multiple schedulers can feed the same queue
// - Queue can switch from in-memory to Redis without changing scheduler
// - Workers can be dynamically scaled up/down without affecting scheduler
type Scheduler struct {
	queue    queue.TaskQueue
	store    *storage.MemoryStore
	postgres *storage.PostgresStore
	logger   *log.StructuredLogger

	taskCounter int64
	mu sync.Mutex
	nextID int64
	isRunning bool
}

// NewScheduler creates a new task scheduler with the given queue.
//
// Parameters:
//   - q: task queue implementation where scheduled tasks will be enqueued
//   - store: in-memory task store used by the scheduler
//   - postgres: PostgreSQL task store used for durable persistence
//   - logger: structured logger for scheduling events
//
// Returns a new Scheduler ready to accept tasks.
//
// Example:
//
//	sched := NewScheduler(taskQueue, store, postgresStore, logger)
func NewScheduler(
	q queue.TaskQueue,
	store *storage.MemoryStore,
	postgres *storage.PostgresStore,
	logger *log.StructuredLogger,
) *Scheduler {
	scheduler := &Scheduler{
		queue:       q,
		store:       store,
		postgres:    postgres,
		logger:      logger,
		taskCounter: 0,    // Start counting from 0
		nextID:      0,    // Next ID will be 1
		isRunning:   true, // Accept tasks immediately
	}

	// Log scheduler creation
	logger.SchedulerCreated(10) // Queue capacity (hard-coded for now)

	return scheduler
}

// SubmitTask accepts a task from a client, assigns it a unique ID,
// sets its initial status, and enqueues it for processing.
//
// Parameters:
//   - ctx: context for the submit operation (allows timeout/cancellation)
//   - name: task type/purpose (e.g., "send_email", "process_payment")
//   - payload: task-specific data (arbitrary bytes)
//
// Returns:
//   - assigned task ID on success
//   - error if scheduler is closed, context cancelled, or queue full
//
// This is the public API clients call to submit work.
//
// Example:
//
//	taskID, err := scheduler.SubmitTask(ctx, "send_email", emailPayload)
//	if err != nil {
//	    log.Fatalf("Failed to submit task: %v", err)
//	}
//	log.Printf("Task accepted with ID: %s", taskID)
func (s *Scheduler) SubmitTask(ctx context.Context, name string, payload []byte) (string, error) {
	// Lock to check scheduler state and validate before creating task.
	// Critical section is minimal: just state check.
	s.mu.Lock()
	if !s.isRunning {
		s.mu.Unlock()
		return "", fmt.Errorf("scheduler is not accepting new tasks")
	}
	s.mu.Unlock()

	// Validate input to catch errors early (fail fast principle).
	// In production, add more validation:
	// - name must not be empty
	// - payload size must be within limits (e.g., max 10MB)
	// - rate limiting per client
	if name == "" {
		return "", fmt.Errorf("task name cannot be empty")
	}

	// Generate a unique ID for this task.
	// atomic.AddInt64 increments taskCounter and returns the new value.
	// Atomic means the operation is indivisible: no other goroutine can interfere.
	// This is crucial for ID uniqueness without locks.
	//
	// Why atomic instead of mutex?
	// - Atomic operations are 100x faster than mutex lock/unlock
	// - No goroutine blocking: all goroutines progress simultaneously
	// - For high-concurrency (1000s of concurrent submissions), atomic is essential
	newID := atomic.AddInt64(&s.taskCounter, 1)
	taskID := fmt.Sprintf("task-%d", newID)

	// Create a new task with the assigned ID.
	// Task is initialized in "pending" state by the constructor.
	// CreatedAt timestamp is set automatically.
	t := task.NewTask(taskID, name, payload)

	if err := s.postgres.CreateTask(t); err != nil {
		s.logger.Error("scheduler", "", taskID, err)
		return "", fmt.Errorf("failed to persist task in postgres: %w", err)
	}

	s.store.Add(t)

	// Log task creation with structured logging
	s.logger.TaskCreated(taskID, name, len(payload))

	// Enqueue the task for workers to process.
	// This is where the task enters the distributed system.
	// Context is passed through to allow caller to cancel if needed.
	// If queue is at capacity, this will block (backpressure on scheduler).
	// If context is cancelled (timeout), returns error without enqueueing.
	if err := s.queue.Enqueue(ctx, t); err != nil {
		s.logger.Error("scheduler", "", taskID, err)
		return "", fmt.Errorf("failed to enqueue task: %w", err)
	}

	// Log successful enqueue.
	// Task is now in the queue, ready for a worker to pick up.
	s.logger.TaskQueued(taskID, s.queueSize(), 10)

	return taskID, nil
}

// SubmitBatch accepts multiple tasks at once.
// More efficient than calling SubmitTask() repeatedly when submitting many tasks.
//
// Parameters:
//   - ctx: context for the entire batch operation
//   - tasks: slice of (name, payload) pairs
//
// Returns:
//   - slice of assigned IDs (in same order as input tasks)
//   - error if any task fails (stops on first error)
//
// Example:
//
//	tasks := []struct {
//	    name    string
//	    payload []byte
//	}{
//	    {"task1", []byte("data1")},
//	    {"task2", []byte("data2")},
//	}
//	ids, err := scheduler.SubmitBatch(ctx, tasks)
func (s *Scheduler) SubmitBatch(ctx context.Context, tasks []struct {
	Name    string
	Payload []byte
}) ([]string, error) {
	// Validate input to prevent invalid batch submission.
	if len(tasks) == 0 {
		return nil, fmt.Errorf("batch cannot be empty")
	}

	// Pre-allocate slice with exact capacity needed.
	// Prevents repeated allocations as we append IDs.
	// More efficient than letting slice grow dynamically.
	assignedIDs := make([]string, 0, len(tasks))

	// Process each task in the batch.
	for i, t := range tasks {
		// Submit individual task.
		// Reuse SubmitTask() logic to avoid duplication.
		// If any task fails, return error immediately (fail fast).
		id, err := s.SubmitTask(ctx, t.Name, t.Payload)
		if err != nil {
			s.logger.Error("scheduler", "", "", err)
			return nil, fmt.Errorf("batch submission failed at index %d: %w", i, err)
		}

		// Append successfully assigned ID to results.
		assignedIDs = append(assignedIDs, id)
	}

	// Log batch completion.
	s.logger.Info("scheduler", fmt.Sprintf("Batch of %d tasks submitted successfully", len(tasks)))

	return assignedIDs, nil
}

// GetQueueSize returns the current number of tasks in the queue.
// This is a snapshot and may change immediately after returning.
// Useful for monitoring and capacity planning.
//
// Example:
//
//	size := scheduler.GetQueueSize()
//	if size > 100 {
//	    log.Println("Queue backing up; consider adding more workers")
//	}
func (s *Scheduler) GetQueueSize() int {
	return s.queueSize()
}

// Shutdown gracefully stops the scheduler from accepting new tasks.
// Existing tasks in the queue will continue to be processed by workers.
//
// Returns error if scheduler was already closed.
//
// Example:
//
//	// Stop accepting new tasks
//	if err := scheduler.Shutdown(); err != nil {
//	    log.Printf("Scheduler already closed: %v", err)
//	}
//	// Queue still processes existing tasks
//	pool.Wait()  // Wait for workers to finish
func (s *Scheduler) Shutdown() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if already closed to prevent double-close.
	if !s.isRunning {
		return fmt.Errorf("scheduler already shutdown")
	}

	// Mark scheduler as closed.
	// New calls to SubmitTask() will be rejected.
	s.isRunning = false

	// Log scheduler shutdown
	totalSubmitted := int(atomic.LoadInt64(&s.taskCounter))
	s.logger.SchedulerShutdown(totalSubmitted)
	return nil
}

// Stats returns statistics about the scheduler and queue.
// Useful for monitoring, alerting, and capacity planning.
//
// Returns a map with keys: "total_submitted", "queue_size", "is_running"
//
// Example:
//
//	stats := scheduler.Stats()
//	log.Printf("Total tasks submitted: %d, Queue size: %d",
//	    stats["total_submitted"], stats["queue_size"])
func (s *Scheduler) Stats() map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	return map[string]interface{}{
		"total_submitted": atomic.LoadInt64(&s.taskCounter),
		"queue_size":      s.queueSize(),
		"is_running":      s.isRunning,
	}
}

// GetTask returns a task by ID from the scheduler's backing store.
func (s *Scheduler) GetTask(id string) (*task.Task, bool) {
	return s.store.Get(id)
}

// GetAllTasks returns all tasks from the scheduler's backing store.
func (s *Scheduler) GetAllTasks() []*task.Task {
	return s.store.GetAll()
}

func (s *Scheduler) queueSize() int {
	sizedQueue, ok := s.queue.(interface{ Size() int })
	if !ok {
		return 0
	}

	return sizedQueue.Size()
}
