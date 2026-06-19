package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"distributed-task-scheduler/internal/task"
)

var _ TaskQueue = (*RedisQueue)(nil)
var _ DistributedQueue = (*RedisQueue)(nil)

// RedisQueue implements a distributed task queue using Redis lists.
//
// Enqueue:
//
//	RPUSH queueName taskJSON
//
// Dequeue:
//
//	BLPOP queueName
//
// Multiple workers can safely consume tasks from the same Redis queue,
// even if they are running on different machines.
type RedisQueue struct {
	client    *redis.Client
	queueName string
}

// Internal structure used for JSON serialization.
type redisTaskPayload struct {
	ID         string      `json:"id"`
	Name       string      `json:"name"`
	Payload    []byte      `json:"payload"`
	Status     task.Status `json:"status"`
	RetryCount int         `json:"retry_count"`
	MaxRetries int         `json:"max_retries"`
	CreatedAt  time.Time   `json:"created_at"`
}

// NewRedisQueue creates a Redis-backed task queue.
//
// Example:
//
//	rq := NewRedisQueue(
//	    "localhost:6379",
//	    "scheduler-tasks",
//	)
func NewRedisQueue(
	addr string,
	queueName string,
) (*RedisQueue, error) {

	client := redis.NewClient(&redis.Options{
		Addr: addr,
		DB:   0,
	})

	// Verify Redis connection.
	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil,
			fmt.Errorf(
				"failed to connect to redis at %s: %w",
				addr,
				err,
			)
	}

	return &RedisQueue{
		client:    client,
		queueName: queueName,
	}, nil
}

// Enqueue serializes a task and pushes it to Redis.
//
// Redis operation:
//
// RPUSH scheduler-tasks taskJSON
func (rq *RedisQueue) Enqueue(
	ctx context.Context,
	t *task.Task,
) error {

	payload := redisTaskPayload{
		ID:         t.ID,
		Name:       t.Name,
		Payload:    t.Payload,
		Status:     t.GetStatus(),
		RetryCount: t.RetryCount,
		MaxRetries: t.MaxRetries,
		CreatedAt:  t.CreatedAt,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf(
			"marshal task %s: %w",
			t.ID,
			err,
		)
	}

	if err := rq.client.
		RPush(
			ctx,
			rq.queueName,
			data,
		).
		Err(); err != nil {

		return fmt.Errorf(
			"enqueue task %s in redis: %w",
			t.ID,
			err,
		)
	}

	return nil
}

// Dequeue blocks until a task becomes available.
//
// BLPOP allows workers to sleep without continuously
// polling Redis, reducing CPU usage.
func (rq *RedisQueue) Dequeue(
	ctx context.Context,
) (*task.Task, error) {

	result, err := rq.client.
		BLPop(
			ctx,
			0,
			rq.queueName,
		).
		Result()

	if err != nil {
		return nil,
			fmt.Errorf(
				"dequeue task from redis: %w",
				err,
			)
	}

	if len(result) != 2 {
		return nil,
			fmt.Errorf(
				"unexpected BLPOP response length: %d",
				len(result),
			)
	}

	var payload redisTaskPayload

	if err := json.Unmarshal(
		[]byte(result[1]),
		&payload,
	); err != nil {

		return nil,
			fmt.Errorf(
				"unmarshal task from redis: %w",
				err,
			)
	}

	t := task.NewTask(
		payload.ID,
		payload.Name,
		payload.Payload,
	)

	t.RetryCount = payload.RetryCount
	t.MaxRetries = payload.MaxRetries

	t.CreatedAt = payload.CreatedAt
	t.SetStatus(payload.Status)

	return t, nil
}

// Length returns the number of tasks currently
// waiting in the Redis queue.
func (rq *RedisQueue) Length(
	ctx context.Context,
) (int64, error) {

	length, err := rq.client.
		LLen(
			ctx,
			rq.queueName,
		).
		Result()

	if err != nil {
		return 0,
			fmt.Errorf(
				"get queue length: %w",
				err,
			)
	}

	return length, nil
}

// Health checks whether Redis is reachable.
func (rq *RedisQueue) Health(
	ctx context.Context,
) error {

	return rq.client.
		Ping(ctx).
		Err()
}

// Close gracefully closes the Redis client.
func (rq *RedisQueue) Close() error {
	return rq.client.Close()
}
