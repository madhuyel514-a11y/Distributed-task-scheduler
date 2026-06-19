package queue

import (
	"context"

	"distributed-task-scheduler/internal/task"
)

type ConsumerQueue interface {
	Dequeue(
		context.Context,
	) (*task.Task, error)
}