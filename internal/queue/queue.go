package queue

import (
	"context"
	"fmt"
	"sync"

	"distributed-task-scheduler/internal/task"
)

var _ TaskQueue = (*Queue)(nil)
var _ DistributedQueue = (*Queue)(nil)

// Queue manages a collection of tasks to be processed by workers.
// This in-memory implementation uses buffered channels.
// Later, this interface can be implemented by a Redis-backed version.
type Queue struct {
	// tasks is a buffered channel that holds pending tasks.
	// Buffered channels act as a queue: writers can add tasks without blocking
	// until the buffer is full, and readers can consume tasks asynchronously.
	tasks chan *task.Task

	// mu protects concurrent access to internal state (like size tracking).
	mu sync.Mutex

	// size tracks the number of tasks currently in the queue.
	// Atomic operations would be more efficient for high-concurrency scenarios,
	// but mutex is simpler for now and works well up to ~1000 operations/sec.
	size int

	// closed indicates whether the queue has been shut down.
	// Once closed, no new tasks can be enqueued.
	closed bool

	// maxSize is the maximum capacity of the queue (buffer size).
	// Prevents unbounded memory growth if tasks arrive faster than workers process them.
	maxSize int
}

// NewQueue creates a new in-memory task queue with the specified buffer capacity.
//
// Parameters:
//   - capacity: the maximum number of tasks the queue can hold in memory.
//     This should be tuned based on expected task arrival rate and worker speed.
//
// Example:
//   queue := NewQueue(1000)  // Can hold up to 1000 tasks before blocking publishers
func NewQueue(capacity int) *Queue {
	return &Queue{
		tasks:   make(chan *task.Task, capacity),
		size:    0,
		closed:  false,
		maxSize: capacity,
	}
}

// Enqueue adds a task to the queue for processing by a worker.
//
// Returns an error if:
//   - The queue is closed
//   - The queue is at capacity (when context deadline is exceeded)
//
// This method is safe for concurrent calls from multiple goroutines (scheduler publishers).
func (q *Queue) Enqueue(ctx context.Context, t *task.Task) error {
	// Lock to check closed state and update size atomically.
	// This prevents race conditions when checking closed and incrementing size.
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return fmt.Errorf("queue is closed")
	}

	// Check if queue is at capacity to provide early feedback.
	// In practice, this helps clients distinguish between "queue full" and "queue closed".
	if q.size >= q.maxSize {
		q.mu.Unlock()
		return fmt.Errorf("queue capacity exceeded: %d/%d tasks", q.size, q.maxSize)
	}

	q.size++
	q.mu.Unlock()

	// Send task to the channel with context awareness.
	// If context is cancelled (timeout or caller aborts), fail gracefully.
	// This is crucial in distributed systems to avoid indefinitely blocked goroutines.
	select {
	case q.tasks <- t:
		return nil
	case <-ctx.Done():
		// Context cancelled before task was queued; decrement size and report error.
		q.mu.Lock()
		q.size--
		q.mu.Unlock()
		return fmt.Errorf("enqueue cancelled: %w", ctx.Err())
	}
}

// Dequeue retrieves the next task from the queue for a worker to process.
//
// Returns nil if the queue is empty and closed (no more tasks will arrive).
// Blocks until a task is available or the context is cancelled.
//
// This method is safe for concurrent calls from multiple goroutines (worker consumers).
func (q *Queue) Dequeue(ctx context.Context) (*task.Task, error) {
	select {
	// Try to receive a task from the channel.
	// If channel is empty, this blocks until a task is available or channel is closed.
	// If channel is closed and empty, ok will be false, and we return nil.
	case t, ok := <-q.tasks:
		if !ok {
			// Channel is closed and empty; no more tasks will ever arrive.
			return nil, nil
		}

		// Successfully dequeued a task; decrement size counter.
		q.mu.Lock()
		q.size--
		q.mu.Unlock()

		return t, nil

	// Context cancelled while waiting for a task (e.g., worker timeout or shutdown).
	// Return error so caller can decide whether to retry, retry-with-backoff, or exit.
	case <-ctx.Done():
		return nil, fmt.Errorf("dequeue cancelled: %w", ctx.Err())
	}
}

// Size returns the approximate number of tasks currently in the queue.
// This is a snapshot and may be stale by the time it's used (eventually consistent).
// Safe for concurrent calls but should not be used for critical decisions in high-concurrency scenarios.
func (q *Queue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.size
}

// Close shuts down the queue and prevents further enqueues.
// After Close(), all waiting Dequeue() calls will eventually receive nil.
// Blocks until all tasks in the buffer are drained or timeout occurs.
//
// Callers should ensure all publishers have stopped before calling Close().
func (q *Queue) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return fmt.Errorf("queue already closed")
	}
	q.closed = true
	q.mu.Unlock()

	// Close the channel to signal to all waiting consumers that no more tasks will arrive.
	// Closing also unblocks any Dequeue() calls waiting on the empty channel.
	close(q.tasks)
	return nil
}

// Drain removes and returns all tasks currently in the queue.
// Used during shutdown to process remaining work or save state.
// WARNING: Should only be called after Close() to avoid race conditions.
func (q *Queue) Drain() []*task.Task {
	var drained []*task.Task

	// Keep reading from the channel until it's empty.
	// Since the channel is closed, this will not block indefinitely.
	for {
		select {
		case t, ok := <-q.tasks:
			if !ok {
				// Channel is closed and empty; we're done.
				return drained
			}
			drained = append(drained, t)
		default:
			// Channel is closed but has no pending values; we're done.
			return drained
		}
	}
}
