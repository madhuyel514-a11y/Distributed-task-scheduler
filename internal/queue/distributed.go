package queue

type DistributedQueue interface {
	TaskQueue
	ConsumerQueue
}