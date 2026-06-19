package log

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// StructuredLogger provides consistent, structured logging across the distributed system.
// It adds timestamps, log levels, and context (worker ID, task ID) to all messages.
// This enables better debugging, monitoring, and tracing of tasks through the system.
type StructuredLogger struct {
	logger *log.Logger
	mu     sync.Mutex
}

// NewStructuredLogger creates a new structured logger writing to stdout.
// All logs include:
// - Timestamp (millisecond precision)
// - Log level (INFO, WARN, ERROR)
// - Context (worker ID, task ID)
// - Message
func NewStructuredLogger() *StructuredLogger {
	return &StructuredLogger{
		logger: log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds),
	}
}

// Event represents a structured log event with context.
type Event struct {
	Level     string                 // "INFO", "WARN", "ERROR"
	Component string                 // "scheduler", "worker", "queue"
	Message   string                 // Main log message
	Context   map[string]interface{} // Additional context (task_id, worker_id, duration, etc.)
}

// log writes a structured event to the logger in a consistent format.
// Thread-safe: uses mutex to prevent interleaved output.
// Format: [TIMESTAMP] [LEVEL] [Component] WorkerID=x TaskID=y Message
func (sl *StructuredLogger) log(event Event) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	// Build context string from map
	contextStr := ""
	for key, val := range event.Context {
		contextStr += fmt.Sprintf(" %s=%v", key, val)
	}

	// Format: [TIME] [LEVEL] [Component] WorkerID=x TaskID=y Duration=2.5s | Message
	sl.logger.Printf("[%s] [%s] %s%s | %s\n",
		time.Now().Format("15:04:05.000"),
		event.Level,
		event.Component,
		contextStr,
		event.Message,
	)
}

// ============================================================================
// TASK LIFECYCLE LOGGING
// ============================================================================

// TaskCreated logs when a task is created by the scheduler.
// Called when SubmitTask() creates a new task object (before queuing).
//
// Example:
//
//	logger.TaskCreated("task-1", "send_email", 1234)
func (sl *StructuredLogger) TaskCreated(taskID, taskName string, payloadSize int) {
	sl.log(Event{
		Level:     "INFO",
		Component: "scheduler",
		Message:   "Task created",
		Context: map[string]interface{}{
			"task_id":      taskID,
			"task_name":    taskName,
			"payload_size": payloadSize,
		},
	})
}

// TaskQueued logs when a task is added to the queue.
// Called when Enqueue() successfully adds task to the buffered channel.
//
// Example:
//
//	logger.TaskQueued("task-1", 5, 10)  // 5 tasks now in queue, capacity 10
func (sl *StructuredLogger) TaskQueued(taskID string, queueSize, queueCapacity int) {
	sl.log(Event{
		Level:     "INFO",
		Component: "queue",
		Message:   "Task enqueued",
		Context: map[string]interface{}{
			"task_id":     taskID,
			"queue_size":  queueSize,
			"queue_cap":   queueCapacity,
			"utilization": fmt.Sprintf("%.1f%%", float64(queueSize)*100/float64(queueCapacity)),
		},
	})
}

// TaskDequeued logs when a worker picks up a task from the queue.
// Called when Dequeue() returns a task to a worker.
//
// Example:
//
//	logger.TaskDequeued("worker-0", "task-1", 3*time.Second)  // waited 3 seconds
func (sl *StructuredLogger) TaskDequeued(workerID, taskID string, queueWaitTime time.Duration) {
	sl.log(Event{
		Level:     "INFO",
		Component: "worker",
		Message:   "Task picked up",
		Context: map[string]interface{}{
			"worker_id":       workerID,
			"task_id":         taskID,
			"queue_wait_time": fmt.Sprintf("%.3fs", queueWaitTime.Seconds()),
		},
	})
}

// TaskStarted logs when a worker begins executing a task.
// Called when worker moves task status to "running" and starts work.
//
// Example:
//
//	logger.TaskStarted("worker-1", "task-2", "send_email")
func (sl *StructuredLogger) TaskStarted(workerID, taskID, taskName string) {
	sl.log(Event{
		Level:     "INFO",
		Component: "worker",
		Message:   "Task execution started",
		Context: map[string]interface{}{
			"worker_id": workerID,
			"task_id":   taskID,
			"task_name": taskName,
			"status":    "running",
		},
	})
}

// TaskCompleted logs when a worker successfully completes a task.
// Called when task status moves to "completed" after execution.
// Includes latency metrics for performance analysis.
//
// Example:
//
//	logger.TaskCompleted("worker-0", "task-1", 2.5*time.Second, 0.5*time.Second)
//	// execution_time: 2.0s, total_latency: 2.5s (time from creation to completion)
func (sl *StructuredLogger) TaskCompleted(workerID, taskID string, executionTime, totalLatency time.Duration) {
	sl.log(Event{
		Level:     "INFO",
		Component: "worker",
		Message:   "Task completed successfully",
		Context: map[string]interface{}{
			"worker_id":      workerID,
			"task_id":        taskID,
			"execution_time": fmt.Sprintf("%.3fs", executionTime.Seconds()),
			"total_latency":  fmt.Sprintf("%.3fs", totalLatency.Seconds()),
			"status":         "completed",
		},
	})
}

// TaskFailed logs when a worker fails to complete a task.
// Called when task encounters an error during execution.
//
// Example:
//
//	logger.TaskFailed("worker-2", "task-5", fmt.Errorf("timeout"), 1.5*time.Second)
func (sl *StructuredLogger) TaskFailed(workerID, taskID string, err error, executionTime time.Duration) {
	sl.log(Event{
		Level:     "ERROR",
		Component: "worker",
		Message:   "Task failed",
		Context: map[string]interface{}{
			"worker_id":      workerID,
			"task_id":        taskID,
			"error":          err.Error(),
			"execution_time": fmt.Sprintf("%.3fs", executionTime.Seconds()),
			"status":         "failed",
		},
	})
}

// TaskRetry logs when a task is being retried after a failure.
// Called when the worker decides the task should be re-enqueued.
func (sl *StructuredLogger) TaskRetry(workerID, taskID string, attempt, maxRetries int, err error) {
	sl.log(Event{
		Level:     "WARN",
		Component: "worker",
		Message:   "Task retry attempt",
		Context: map[string]interface{}{
			"worker_id":   workerID,
			"task_id":     taskID,
			"attempt":     attempt,
			"max_retries": maxRetries,
			"error":       err.Error(),
			"status":      "retrying",
		},
	})
}

// TaskRetriesExhausted logs when a task reaches its retry limit and will no longer be retried.
func (sl *StructuredLogger) TaskRetriesExhausted(workerID, taskID string, attempts, maxRetries int, err error) {
	sl.log(Event{
		Level:     "ERROR",
		Component: "worker",
		Message:   "Retries exhausted",
		Context: map[string]interface{}{
			"worker_id":   workerID,
			"task_id":     taskID,
			"attempts":    attempts,
			"max_retries": maxRetries,
			"error":       err.Error(),
			"status":      "failed",
		},
	})
}

// ============================================================================
// WORKER LIFECYCLE LOGGING
// ============================================================================

// WorkerStarted logs when a worker goroutine begins execution.
// Called at the start of worker.Execute(ctx).
//
// Example:
//
//	logger.WorkerStarted("worker-0")
func (sl *StructuredLogger) WorkerStarted(workerID string) {
	sl.log(Event{
		Level:     "INFO",
		Component: "worker",
		Message:   "Worker started and waiting for tasks",
		Context: map[string]interface{}{
			"worker_id": workerID,
		},
	})
}

// WorkerShutdown logs when a worker goroutine is shutting down.
// Called when worker.Execute() returns (context cancelled or queue closed).
//
// Example:
//
//	logger.WorkerShutdown("worker-1", 15)  // processed 15 tasks before shutdown
func (sl *StructuredLogger) WorkerShutdown(workerID string, tasksProcessed int) {
	sl.log(Event{
		Level:     "INFO",
		Component: "worker",
		Message:   "Worker shutting down",
		Context: map[string]interface{}{
			"worker_id":       workerID,
			"tasks_processed": tasksProcessed,
		},
	})
}

// ============================================================================
// SYSTEM-LEVEL LOGGING
// ============================================================================

// SchedulerCreated logs scheduler initialization.
//
// Example:
//
//	logger.SchedulerCreated(1000)  // created with 1000 task capacity
func (sl *StructuredLogger) SchedulerCreated(queueCapacity int) {
	sl.log(Event{
		Level:     "INFO",
		Component: "scheduler",
		Message:   "Scheduler initialized",
		Context: map[string]interface{}{
			"queue_capacity": queueCapacity,
		},
	})
}

// SchedulerShutdown logs scheduler shutdown.
//
// Example:
//
//	logger.SchedulerShutdown(42)  // accepted 42 tasks before shutdown
func (sl *StructuredLogger) SchedulerShutdown(totalSubmitted int) {
	sl.log(Event{
		Level:     "INFO",
		Component: "scheduler",
		Message:   "Scheduler shutdown, no new tasks accepted",
		Context: map[string]interface{}{
			"total_submitted": totalSubmitted,
		},
	})
}

// WorkerPoolStarted logs when worker pool is initialized and ready.
//
// Example:
//
//	logger.WorkerPoolStarted(3)  // started 3 workers
func (sl *StructuredLogger) WorkerPoolStarted(workerCount int) {
	sl.log(Event{
		Level:     "INFO",
		Component: "worker",
		Message:   "Worker pool initialized",
		Context: map[string]interface{}{
			"worker_count": workerCount,
		},
	})
}

// QueueStats logs current queue statistics for monitoring.
// Useful for capacity planning and bottleneck detection.
//
// Example:
//
//	logger.QueueStats(25, 100, 15*time.Second)  // 25 tasks, 100 capacity, avg latency 15s
func (sl *StructuredLogger) QueueStats(currentSize, maxCapacity int, avgLatency time.Duration) {
	utilization := float64(currentSize) * 100 / float64(maxCapacity)
	sl.log(Event{
		Level:     "INFO",
		Component: "queue",
		Message:   "Queue statistics",
		Context: map[string]interface{}{
			"current_size": currentSize,
			"max_capacity": maxCapacity,
			"utilization":  fmt.Sprintf("%.1f%%", utilization),
			"avg_latency":  fmt.Sprintf("%.3fs", avgLatency.Seconds()),
		},
	})
}

// Error logs an error event with context.
// Use for exceptions, failures, and error conditions.
//
// Example:
//
//	logger.Error("queue_full", "worker-2", "task-5", fmt.Errorf("buffer overflow"))
func (sl *StructuredLogger) Error(component, workerID, taskID string, err error) {
	sl.log(Event{
		Level:     "ERROR",
		Component: component,
		Message:   err.Error(),
		Context: map[string]interface{}{
			"worker_id": workerID,
			"task_id":   taskID,
		},
	})
}

// Info logs a general information message.
// Use for system events, phase transitions, etc.
//
// Example:
//
//	logger.Info("system", "All workers completed successfully")
func (sl *StructuredLogger) Info(component, message string) {
	sl.log(Event{
		Level:     "INFO",
		Component: component,
		Message:   message,
		Context:   map[string]interface{}{},
	})
}
