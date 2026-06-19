package storage

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"

	"distributed-task-scheduler/internal/task"
)

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(
	host string,
	port int,
	user string,
	password string,
	dbname string,
) (*PostgresStore, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host,
		port,
		user,
		password,
		dbname,
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres connection: %w", err)
	}

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping postgres database: %w", err)
	}

	store := &PostgresStore{
		db: db,
	}

	if err := store.Initialize(); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("initialize postgres store: %w", err)
	}

	return store, nil
}

func (p *PostgresStore) Initialize() error {
	if p == nil || p.db == nil {
		return fmt.Errorf("postgres store is not initialized")
	}

	const createTasksTable = `
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    payload BYTEA,
    status TEXT NOT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
`

	if _, err := p.db.Exec(createTasksTable); err != nil {
		return fmt.Errorf("create tasks table: %w", err)
	}

	const alterTasksRetryColumns = `
ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS retry_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS max_retries INT NOT NULL DEFAULT 3;
`

	if _, err := p.db.Exec(alterTasksRetryColumns); err != nil {
		return fmt.Errorf("alter tasks table for retry columns: %w", err)
	}

	return nil
}

func (p *PostgresStore) CreateTask(
	t *task.Task,
) error {
	if p == nil || p.db == nil {
		return fmt.Errorf("postgres store is not initialized")
	}
	if t == nil {
		return fmt.Errorf("task is nil")
	}

	const insertTask = `
INSERT INTO tasks (id, name, payload, status, retry_count, max_retries, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
`

	if _, err := p.db.Exec(
		insertTask,
		t.ID,
		t.Name,
		t.Payload,
		t.GetStatus(),
		t.RetryCount,
		t.MaxRetries,
		t.CreatedAt,
		t.CreatedAt,
	); err != nil {
		return fmt.Errorf("insert task %s: %w", t.ID, err)
	}

	return nil
}

func (p *PostgresStore) GetTask(
	id string,
) (*task.Task, error) {
	if p == nil || p.db == nil {
		return nil, fmt.Errorf("postgres store is not initialized")
	}

	const selectTask = `
SELECT id, name, payload, status, retry_count, max_retries, created_at
FROM tasks
WHERE id = $1;
`

	var (
		taskID     string
		name       string
		payload    []byte
		status     task.Status
		retryCount int
		maxRetries int
		createdAt  time.Time
	)

	if err := p.db.QueryRow(selectTask, id).Scan(
		&taskID,
		&name,
		&payload,
		&status,
		&retryCount,
		&maxRetries,
		&createdAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("task %s not found", id)
		}
		return nil, fmt.Errorf("select task %s: %w", id, err)
	}

	t := task.NewTask(taskID, name, payload)
	t.RetryCount = retryCount
	t.MaxRetries = maxRetries
	t.CreatedAt = createdAt
	t.SetStatus(status)

	return t, nil
}

func (p *PostgresStore) GetAllTasks() (
	[]*task.Task,
	error,
) {
	if p == nil || p.db == nil {
		return nil, fmt.Errorf("postgres store is not initialized")
	}

	const selectAllTasks = `
SELECT id, name, payload, status, retry_count, max_retries, created_at
FROM tasks
ORDER BY created_at ASC;
`

	rows, err := p.db.Query(selectAllTasks)
	if err != nil {
		return nil, fmt.Errorf("select all tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]*task.Task, 0)

	for rows.Next() {
		var (
			taskID     string
			name       string
			payload    []byte
			status     task.Status
			retryCount int
			maxRetries int
			createdAt  time.Time
		)

		if err := rows.Scan(
			&taskID,
			&name,
			&payload,
			&status,
			&retryCount,
			&maxRetries,
			&createdAt,
		); err != nil {
			return nil, fmt.Errorf("scan task row: %w", err)
		}

		t := task.NewTask(taskID, name, payload)
		t.RetryCount = retryCount
		t.MaxRetries = maxRetries
		t.CreatedAt = createdAt
		t.SetStatus(status)

		tasks = append(tasks, t)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate task rows: %w", err)
	}

	return tasks, nil
}

func (p *PostgresStore) UpdateTaskStatus(
	id string,
	status task.Status,
	retryCount int,
	maxRetries int,
) error {
	if p == nil || p.db == nil {
		return fmt.Errorf("postgres store is not initialized")
	}

	const updateTaskStatus = `
UPDATE tasks
SET status = $2, retry_count = $3, max_retries = $4, updated_at = $5
WHERE id = $1;
`

	result, err := p.db.Exec(
		updateTaskStatus,
		id,
		status,
		retryCount,
		maxRetries,
		time.Now().UTC(),
	)
	if err != nil {
		return fmt.Errorf("update task %s status: %w", id, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read updated rows for task %s: %w", id, err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("task %s not found", id)
	}

	return nil
}

func (p *PostgresStore) Close() error {
	if p == nil || p.db == nil {
		return nil
	}

	return p.db.Close()
}
