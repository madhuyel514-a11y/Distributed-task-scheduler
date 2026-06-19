package api

import "net/http"

func RegisterRoutes(
	mux *http.ServeMux,
	h *Handler,
) {
	mux.HandleFunc("/tasks", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			h.CreateTask(w, r)
		case http.MethodGet:
			h.GetTasks(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/tasks/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		h.GetTask(w, r)
	})
}
