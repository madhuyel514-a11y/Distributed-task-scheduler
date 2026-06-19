# 🚀 Distributed Task Scheduler

![Go](https://img.shields.io/badge/Go-1.24-blue?logo=go)
![Redis](https://img.shields.io/badge/Redis-Message%20Broker-red?logo=redis)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-blue?logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Containerized-blue?logo=docker)

A scalable backend system for asynchronous task processing using a **distributed producer-consumer architecture**. The project demonstrates concepts such as **distributed queues, concurrency, fault tolerance, retries, persistence, and multi-process execution** using Go, Redis, and PostgreSQL.

---

# 📌 Overview

The Distributed Task Scheduler accepts tasks through REST APIs, persists task metadata in PostgreSQL, and dispatches tasks asynchronously through Redis-backed queues. Multiple worker processes consume tasks concurrently and execute them independently.

The project focuses on **distributed systems infrastructure** rather than a specific business use case. Real executors such as email delivery, image processing, report generation, or external API jobs can be plugged into the worker layer with minimal changes.

---

# ✨ Features

* Distributed producer-consumer architecture
* Asynchronous task processing using Redis
* Concurrent worker pools using Go goroutines
* Multi-process execution
* Persistent task metadata storage using PostgreSQL
* Automatic retry mechanism
* Task lifecycle management
* REST APIs for task submission and retrieval
* Structured logging
* Graceful shutdown handling
* Dockerized services
* Horizontal scaling through additional worker processes

---

# 🏗️ System Architecture

```text
                ┌──────────┐
                │ Client   │
                └────┬─────┘
                     │ HTTP
                     ▼
           ┌─────────────────┐
           │ API Server      │
           └──────┬──────────┘
                  │
        ┌─────────┴─────────┐
        │                   │
        ▼                   ▼
┌──────────────┐    ┌────────────────┐
│ PostgreSQL   │    │ Redis Queue    │
│ Task Storage │    │ Message Broker │
└──────────────┘    └──────┬─────────┘
                           │
             ┌─────────────┼─────────────┐
             ▼             ▼             ▼
         Worker-1      Worker-2      Worker-3
```

---

# ⚙️ Tech Stack

| Component        | Technology                |
| ---------------- | ------------------------- |
| Language         | Go                        |
| Message Broker   | Redis                     |
| Database         | PostgreSQL                |
| Containerization | Docker                    |
| APIs             | REST                      |
| Concurrency      | Goroutines                |
| Synchronization  | Channels and Worker Pools |
| Logging          | Structured Logging        |

---

# 📂 Project Structure

```text
distributed-task-scheduler/
│
├── cmd/
│   ├── api-server/
│   ├── worker/
│   └── scheduler/
│
├── internal/
│   ├── api/
│   ├── queue/
│   ├── scheduler/
│   ├── storage/
│   ├── task/
│   ├── worker/
│   └── log/
│
├── docker-compose.yml
├── go.mod
├── go.sum
├── README.md
└── .gitignore
```

---

# 🔄 How It Works

## Step 1: Client Submits Task

```text
Client
   ↓
POST /tasks
```

Example:

```json
{
  "name": "retry_test",
  "payload": "hello"
}
```

---

## Step 2: API Server Creates Task

The API server:

* Generates a unique task ID
* Creates a task object
* Initializes task metadata
* Stores task information

---

## Step 3: Persist Task in PostgreSQL

Task metadata is stored in PostgreSQL:

* id
* name
* payload
* status
* retry_count
* max_retries
* timestamps

---

## Step 4: Publish Task to Redis

The scheduler pushes the task into Redis:

```text
Redis Queue
     ↓
task-41
task-42
task-43
```

Redis acts as a message broker between producers and workers.

---

## Step 5: Workers Consume Tasks

Multiple workers continuously listen to the queue:

```text
Worker-1
Worker-2
Worker-3
```

Each worker pulls tasks independently and processes them concurrently.

---

## Step 6: Task Completion

Workers update task status:

```text
pending
↓
running
↓
completed
```

If execution fails:

```text
pending
↓
running
↓
retrying
↓
completed
```

or

```text
pending
↓
running
↓
retrying
↓
failed
```

---

# 🔁 Retry Mechanism

The system automatically retries failed tasks.

Example:

```text
Task-30
Attempt 1 → Failed
Attempt 2 → Failed
Attempt 3 → Completed
```

Another example:

```text
Task-40
Attempt 1 → Failed
Attempt 2 → Failed
Attempt 3 → Failed
Final Status → failed
```

Retry metadata stored in PostgreSQL:

```text
retry_count
max_retries
```

---

# 📡 REST APIs

## Create Task

### Request

```http
POST /tasks
```

Body:

```json
{
  "name": "retry_test",
  "payload": "hello"
}
```

### Response

```json
{
  "id": "task-42"
}
```

---

## Get All Tasks

### Request

```http
GET /tasks
```

### Response

```json
[
  {
    "id": "task-42",
    "name": "retry_test",
    "status": "completed",
    "retry_count": 2,
    "max_retries": 3
  }
]
```

---

## Get Task By ID

### Request

```http
GET /tasks/task-42
```

### Response

```json
{
  "id": "task-42",
  "name": "retry_test",
  "status": "completed",
  "retry_count": 2,
  "max_retries": 3
}
```

---

# 🐳 Setup and Installation

## Clone Repository

```bash
git clone https://github.com/your-username/distributed-task-scheduler.git
cd distributed-task-scheduler
```

---

## Start Redis and PostgreSQL

```bash
docker compose up -d
```

Verify:

```bash
docker ps
```

---

## Install Dependencies

```bash
go mod tidy
```

---

## Start API Server

Terminal 1:

```bash
go run ./cmd/api-server
```

---

## Start Workers

Terminal 2:

```bash
go run ./cmd/worker
```

Terminal 3:

```bash
go run ./cmd/worker
```

Terminal 4:

```bash
go run ./cmd/worker
```

All workers consume tasks from the same Redis queue concurrently.

---

# 🧪 Testing the System

Create a task:

```powershell
Invoke-RestMethod `
-Uri "http://localhost:8080/tasks" `
-Method POST `
-ContentType "application/json" `
-Body '{"name":"retry_test","payload":"hello"}'
```

Create multiple tasks:

```powershell
1..20 | % {
    Invoke-RestMethod `
    -Uri "http://localhost:8080/tasks" `
    -Method POST `
    -ContentType "application/json" `
    -Body '{"name":"retry_test","payload":"hello"}'
}
```

---

# 🔍 Verify Redis

Open Redis CLI:

```bash
docker exec -it scheduler-redis redis-cli
```

Check queue size:

```bash
LLEN scheduler-tasks
```

View queued tasks:

```bash
LRANGE scheduler-tasks 0 -1
```

---

# 🗄️ Verify PostgreSQL

Open PostgreSQL:

```bash
docker exec -it scheduler-postgres psql -U postgres -d scheduler
```

View tables:

```sql
\dt
```

View schema:

```sql
\d tasks
```

View task data:

```sql
SELECT id,
       status,
       retry_count,
       max_retries
FROM tasks;
```

---

# 📊 Key Achievements

* Built a distributed producer-consumer architecture using Go and Redis.
* Implemented concurrent task execution using goroutines and worker pools.
* Persisted task metadata and lifecycle state in PostgreSQL.
* Designed automatic retry handling with configurable retry limits.
* Demonstrated horizontal scaling by running multiple worker processes consuming from a shared queue.
* Added structured logging and REST APIs for observability and debugging.

---

# 🧠 Distributed Systems Concepts Demonstrated

* Producer-Consumer Architecture
* Distributed Task Queues
* Asynchronous Processing
* Concurrent Worker Pools
* Goroutines and Synchronization
* Multi-process Communication
* Message Brokers
* Fault Tolerance
* Retry Mechanisms
* Persistent Storage
* Horizontal Scaling
* Separation of Concerns
* Graceful Shutdown

---

# 🔮 Future Improvements

* Health endpoint (`/health`)
* Metrics endpoint (`/metrics`)
* Dead Letter Queue (DLQ)
* Priority queues
* Task scheduling with delays and cron expressions
* Authentication and authorization
* Monitoring dashboards
* Kubernetes deployment
* Real task executors:

  * Email delivery
  * Image processing
  * Report generation
  * External API jobs
  * AI inference tasks
