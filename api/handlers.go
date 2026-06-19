package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"distributed-task-scheduler/internal/scheduler"
	"distributed-task-scheduler/internal/task"
)

type Handler struct {
	Scheduler *scheduler.Scheduler
}

type CreateTaskRequest struct {
	Name    string `json:"name"`
	Payload string `json:"payload"`
}

type CreateTaskResponse struct {
	ID string `json:"id"`
}

type TaskResponse struct {
	ID         string      `json:"id"`
	Name       string      `json:"name"`
	Payload    string      `json:"payload"`
	Status     task.Status `json:"status"`
	RetryCount int         `json:"retry_count"`
	MaxRetries int         `json:"max_retries"`
	CreatedAt  time.Time   `json:"created_at"`
}

func (h *Handler) CreateTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req CreateTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	id, err := h.Scheduler.SubmitTask(ctx, req.Name, []byte(req.Payload))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(CreateTaskResponse{ID: id})
}

func (h *Handler) GetTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	tasks := h.Scheduler.GetAllTasks()
	response := make([]TaskResponse, 0, len(tasks))

	for _, t := range tasks {
		response = append(response, toTaskResponse(t))
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (h *Handler) GetTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/tasks/")
	if id == "" || id == r.URL.Path {
		http.Error(w, "task id is required", http.StatusBadRequest)
		return
	}

	t, ok := h.Scheduler.GetTask(id)
	if !ok {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(toTaskResponse(t))
}

func toTaskResponse(t *task.Task) TaskResponse {
	return TaskResponse{
		ID:         t.ID,
		Name:       t.Name,
		Payload:    string(t.Payload),
		Status:     t.GetStatus(),
		RetryCount: t.RetryCount,
		MaxRetries: t.MaxRetries,
		CreatedAt:  t.CreatedAt,
	}
}
