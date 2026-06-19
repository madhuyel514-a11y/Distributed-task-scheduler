package queue

import (
	"context"

	"distributed-task-scheduler/internal/task"
)

type TaskQueue interface {
	Enqueue(
		context.Context,
		*task.Task,
	) error
}
