package storage

import (
	"sync"

	"distributed-task-scheduler/internal/task"
)

// MemoryStore stores tasks in memory.
type MemoryStore struct {
	mu    sync.RWMutex
	tasks map[string]*task.Task
}

// Constructor
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		tasks: make(map[string]*task.Task),
	}
}

// Add a task
func (m *MemoryStore) Add(t *task.Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.tasks[t.ID] = t
}

// Get task by ID
func (m *MemoryStore) Get(id string) (*task.Task, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	t, ok := m.tasks[id]
	return t, ok
}

// Get all tasks
func (m *MemoryStore) GetAll() []*task.Task {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*task.Task, 0, len(m.tasks))

	for _, t := range m.tasks {
		result = append(result, t)
	}

	return result
}