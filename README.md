# Real-Time Contest LeaderStack

A highly scalable, real-time leaderboard service built with **FastAPI**, **Redis Sorted Sets**, and **Server-Sent Events (SSE)**. 
Designed to support concurrent contests, high-throughput score submissions, and deterministic tie-breaking.

---

## 🎯 Original Assignment Mapping

This project was built to satisfy a core technical assignment. It has since been extended to support multiple concurrent contests, but all original constraints remain strictly satisfied:

1. **Build a leaderboard where scores update instantly:** Satisfied via Server-Sent Events (SSE) pushing updates in real-time.
2. **API to submit/update score:** Satisfied via `POST /score` (and `POST /contests/{id}/score`).
3. **Use Redis Sorted Sets:** Satisfied. The core ranking engine uses `ZADD`, `ZREVRANGE`, `ZREVRANK`, etc.
4. **Fetch top-N leaderboard:** Satisfied via `GET /leaderboard?limit=10`.
5. **Fetch participant's current rank:** Satisfied via `GET /rank/{participant_id}`.
6. **Dockerize app + Redis:** Satisfied via `docker-compose.yml`, which containerizes the API, Redis, and a frontend Nginx server.

---

## 🏆 Contest-Aware Architecture

The system supports multiple distinct contests running simultaneously. 

### Contest Lifecycle
Each contest has a defined `startTime` and `endTime`:
- 🟡 **Upcoming:** API rejects submissions (`400 Bad Request`). Board can be viewed.
- 🟢 **Live:** API accepts valid submissions subject to rate limits and best-score rules.
- 🔴 **Ended:** API rejects new submissions (`400 Bad Request`). Board is frozen and readable.

### Backward Compatibility
If no `contest_id` is provided to an API endpoint, it automatically defaults to the `global` contest to preserve backward compatibility with legacy clients and the original assignment specification.

---

## 🚀 Deploy on Render

This project is configured for one-click deployment on [Render](https://render.com). It uses a `render.yaml` blueprint to automatically provision both the FastAPI application and a managed Redis instance.

### How to deploy:
1. Push this repository to your GitHub/GitLab/Bitbucket account.
2. Log into the [Render Dashboard](https://dashboard.render.com).
3. Click **New +** and select **Blueprint**.
4. Connect your repository. Render will automatically detect the `render.yaml` file.
5. Review the plan (it will create a Web Service and a Redis instance on the Free tier by default) and click **Apply**.

### Under the Hood:
- **`PORT` Injection**: Render dynamically injects a `PORT` environment variable. The Dockerfile `CMD` is configured to automatically bind `uvicorn` to this port.
- **Redis Connection**: The blueprint automatically injects the `REDIS_URL` connection string from the newly provisioned Redis service into the web service's environment variables. The API is programmed to prioritize `REDIS_URL` if available.
- **Health Checks**: Render automatically pings the `/health` endpoint to ensure zero-downtime rollouts.

> [!WARNING]
> The `render.yaml` defaults to Render's Free tier for Redis. Free tier Redis instances **do not persist data across restarts** and will evict keys if memory limits are reached. For a production leaderboard, you should upgrade the Redis instance in the Render dashboard to a paid plan with persistence enabled.

---

## 🏃 Running the Project Locally

The entire stack (Redis, FastAPI Backend, Nginx Frontend) is containerized.

```bash
docker compose up --build
```

- **Frontend Dashboard:** [http://localhost:5500](http://localhost:5500)
- **API Documentation (Swagger):** [http://localhost:8000/docs](http://localhost:8000/docs)
- **Redis Node:** `localhost:6379`

Demo data is automatically seeded into the `global`, `contest-live`, and `contest-ended` boards on startup if the database is empty.

---

## ⚖️ Business Rules & Tie-Breaking

### Best-Score Only
A participant's score can only increase. If a submission is lower than or equal to their current best score, the API rejects it with a `400 Bad Request`.

### Tie-Breaking (Earlier Wins)
To resolve ties dynamically without a secondary sort pass, scores are stored in Redis as floats (Composite Scores).
`composite_score = integer_score + (1.0 / timestamp_ms)`
- The fractional addition is infinitely small (e.g., `+ 0.0000000000001`).
- An earlier submission has a smaller timestamp, yielding a slightly *larger* fraction.
- Thus, the earlier submitter ranks higher. The API strips the fraction when returning data.

---

## 🔑 Redis Key Structure

Data is heavily partitioned to ensure contest isolation:

| Key Pattern | Data Type | Purpose |
|---|---|---|
| `contests:list` | Set | Tracks all created contest IDs |
| `contest:{contest}:meta` | Hash | Contest settings (name, expectedUsers, startTime, endTime, maxScore) |
| `leaderboard:{contest}:all` | Sorted Set | All-time rankings |
| `leaderboard:{contest}:daily:{date}` | Sorted Set | Daily rankings (48h TTL) |
| `leaderboard:{contest}:weekly:{week}` | Sorted Set | Weekly rankings (10d TTL) |
| `activity:{contest}:log` | List | Last 100 score changes |
| `metrics:{contest}` | Hash | Counters (Total, Accepted, Rejected) |
| `rate:{contest}:{participant}` | String | Rate-limiting (60s TTL) |

---

## 🔌 API Reference

*(All endpoints prefixed with `/contests/{contest_id}` can also be called without the prefix to default to the `global` contest)*

### Core
- `GET /contests` — List all contests and their lifecycle status
- `POST /contests/{id}/score` — Submit a score (Rate limited: 10/min)
- `GET /contests/{id}/leaderboard` — Get top participants
- `GET /contests/{id}/rank/{participant_id}` — Get specific participant rank
- `GET /contests/{id}/participants/{participant_id}/neighbors` — Get opponents immediately above/below

### Observability
- `GET /events` — SSE stream emitting live `score_update` events across all contests
- `GET /contests/{id}/activity` — Return the recent activity log
- `GET /contests/{id}/metrics` — Return contest-specific submission counters
- `GET /health` — Liveness and Redis readiness probe

---

## 🛡️ Reliability & Edge Cases (Defensive Hardening)

This project has undergone a comprehensive defensive hardening pass to ensure data integrity and stability under extreme load:

### 1. Redis Transaction Safety (Atomicity)
- **Problem:** Submitting scores previously required multiple round-trips to Redis (check score, add score, rank). This allowed concurrent requests to corrupt the rank movement log or bypass best-score rules.
- **Solution:** Score submissions are now fully encapsulated inside a **Lua script** (`EVALSHA`). The best-score check, `ZADD`, rank calculation, and metric increments execute completely atomically inside Redis. Race conditions are mathematically impossible.

### 2. Distributed Rate Limiting
- A globally applied `RateLimit` dependency enforces sliding-window rate limits across all routes:
  - `POST /score`: 5 requests per 10s per participant ID.
  - `POST /contests`: 3 requests per 60s per IP (Admin limits).
  - `GET /*` (leaderboard, metrics): 60 requests per 60s per IP.
  - Returns `429 Too Many Requests` when limits are exceeded.

### 3. Frontend Resilience & Self-Healing
- **Fetch Timeout:** `apiFetch` uses an `AbortController` to strictly timeout hanging requests after 8 seconds, preventing stuck loading spinners.
- **Exponential Backoff:** The Server-Sent Events (SSE) `EventSource` wraps reconnects in an exponential backoff algorithm (up to 30s) to prevent a failing server from being hammered by reconnect spikes.
- **Graceful Degradation:** When the backend/Redis fails, the UI disables submission inputs, defaults to safe fallback states (`—` or `0`), and displays clear "Offline" or "Reconnecting" badges without throwing console exceptions.

### 4. Dynamic Validation Guardrails
- The `ScoreRequest` model removes hardcoded constraints and dynamically validates incoming scores against the active contest's specific `maxScore` metadata.
- Prevents partial contest creation, timezone mismatches, and prevents non-live contests from accepting data (`400 Bad Request`).

---

## 📡 External System Integration

The Leaderboard can seamlessly ingest real-time events from external sources (e.g., Online Judges, Quiz Engines, Sensors) alongside manual admin updates.

### Ingestion Endpoint
`POST /contests/{contest_id}/ingest-score-event`

**Payload Schema:**
```json
{
  "eventId": "judge-submission-9281",
  "participantId": "alice",
  "source": "judge",
  "scoreMode": "delta",
  "scoreValue": 100,
  "timestamp": "2026-07-17T17:20:00Z"
}
```

### Modes & Idempotency
- **Modes:** Supports `absolute` (overwrites if higher) and `delta` (adds to current score). Both strictly enforce contest rules (`maxScore` limits and "no-negative-deltas").
- **Idempotency:** Every event requires a unique `eventId`. The API guarantees mathematically atomic deduplication via Redis (`SET event:dedupe:{id} COMPLETED NX EX 86400`). Duplicate webhook deliveries or upstream retries are safely caught and rejected without corrupting the leaderboard, metrics, or activity logs.
- **Source Tracking:** The `source` field determines the UI badge in the admin activity log (`admin`, `judge`, `sensor`, `system`, `organizer`).

### 🚀 Future Scale: Redis Streams
Currently, the ingestion endpoint directly executes a Lua script for real-time processing. 
If traffic scales massively, the `process_score_event()` abstraction layer is designed to be easily swapped to an asynchronous Event-Driven Architecture. Instead of executing the score logic synchronously, the API would simply run `XADD stream:score_events` to append to a Redis Stream, and a separate fleet of Consumer Groups (`XREADGROUP`) would process the queue idempotently at their own pace.
