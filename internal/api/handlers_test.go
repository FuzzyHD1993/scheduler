package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"

	"scheduler/internal/api"
	metricspkg "scheduler/internal/metrics"
	"scheduler/internal/queue"
)

// fakeQueue is an in-memory api.JobQueue with programmable behavior.
type fakeQueue struct {
	jobs            map[int64]*queue.Job
	nextID          int64
	maxPayloadCount int

	createErr error
	getErr    error
	retryErr  error
}

func newFakeQueue() *fakeQueue {
	return &fakeQueue{jobs: map[int64]*queue.Job{}, nextID: 1}
}

func (q *fakeQueue) CreateJob(_ context.Context, jobType string, payload json.RawMessage, maxAttempts int) (*queue.Job, error) {
	if q.createErr != nil {
		return nil, q.createErr
	}
	j := &queue.Job{
		ID:          q.nextID,
		Type:        jobType,
		Payload:     payload,
		Status:      queue.StatusPending,
		Attempts:    0,
		MaxAttempts: maxAttempts,
		RunAt:       time.Now(),
	}
	q.nextID++
	q.jobs[j.ID] = j
	return j, nil
}

func (q *fakeQueue) GetJob(_ context.Context, id int64) (*queue.Job, error) {
	if q.getErr != nil {
		return nil, q.getErr
	}
	j, ok := q.jobs[id]
	if !ok {
		return nil, queue.ErrJobNotFound
	}
	return j, nil
}

func (q *fakeQueue) ListJobs(_ context.Context, status string, limit, offset int) ([]*queue.Job, error) {
	var out []*queue.Job
	for _, j := range q.jobs {
		if status == "" || j.Status == queue.Status(status) {
			out = append(out, j)
		}
	}
	// Return at most `limit` jobs, honoring offset by skipping.
	skipped := 0
	var res []*queue.Job
	for _, j := range out {
		if skipped < offset {
			skipped++
			continue
		}
		res = append(res, j)
		if len(res) >= limit {
			break
		}
	}
	return res, nil
}

func (q *fakeQueue) RetryJob(_ context.Context, id int64) (*queue.Job, error) {
	if q.retryErr != nil {
		return nil, q.retryErr
	}
	j, ok := q.jobs[id]
	if !ok {
		return nil, queue.ErrJobNotFound
	}
	if j.Status != queue.StatusDead {
		return nil, queue.ErrJobNotDead
	}
	j.Status = queue.StatusPending
	j.Attempts = 0
	return j, nil
}

func (q *fakeQueue) CountPending(context.Context) (int64, error) {
	return int64(len(q.jobs)), nil
}

type fakePinger struct{ err error }

func (f fakePinger) Ping(ctx context.Context) error {
	if f.err != nil {
		return f.err
	}
	return ctx.Err()
}

// newTestApp builds an app around a fake queue with per-test knobs.
func newTestApp(t *testing.T, q *fakeQueue, mutate func(*api.Deps)) *fiber.App {
	t.Helper()
	deps := api.Deps{
		Queue:           q,
		Postgres:        fakePinger{},
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		MetricsHandler:  metricspkg.New().Handler(),
		MaxPayloadBytes: 1 << 20,
		JobTypes:        []string{"http", "noop"},
		RateLimit:       100,
		RateWindow:      time.Second,
	}
	if mutate != nil {
		mutate(&deps)
	}
	return api.New(deps)
}

func doJSON(t *testing.T, app *fiber.App, method, path string, body []byte) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, "http://test"+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("test request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp, data
}

func TestCreateJobSuccess(t *testing.T) {
	q := newFakeQueue()
	app := newTestApp(t, q, nil)

	resp, data := doJSON(t, app, "POST", "/jobs",
		[]byte(`{"type":"http","payload":{"url":"https://example.com","method":"POST"},"max_attempts":5}`))
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, body = %s, want 201", resp.StatusCode, data)
	}
	var got struct {
		Job *queue.Job `json:"job"`
	}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Job == nil || got.Job.Type != "http" || got.Job.MaxAttempts != 5 || got.Job.ID != 1 {
		t.Fatalf("bad created job: %+v", got.Job)
	}
	if len(q.jobs) != 1 {
		t.Fatalf("jobs stored = %d, want 1", len(q.jobs))
	}
}

func TestCreateJobUnknownType(t *testing.T) {
	app := newTestApp(t, newFakeQueue(), nil)
	resp, data := doJSON(t, app, "POST", "/jobs", []byte(`{"type":"bogus"}`))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s, want 400", resp.StatusCode, data)
	}
	if !strings.Contains(string(data), "unknown job type") {
		t.Fatalf("body = %s, want unknown-type error", data)
	}
}

func TestCreateJobPayloadTooLarge(t *testing.T) {
	app := newTestApp(t, newFakeQueue(), nil)
	big := fmt.Sprintf(`{"type":"noop","payload":{"blob":"%s"}}`, strings.Repeat("x", 1<<20))
	resp, data := doJSON(t, app, "POST", "/jobs", []byte(big))
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, body = %s, want 413", resp.StatusCode, data)
	}
}

func TestCreateJobMaxAttemptsOutOfRange(t *testing.T) {
	app := newTestApp(t, newFakeQueue(), nil)
	for _, n := range []int{0, 101, -3} {
		body := fmt.Sprintf(`{"type":"noop","max_attempts":%d}`, n)
		resp, data := doJSON(t, app, "POST", "/jobs", []byte(body))
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("max_attempts=%d: status = %d, body = %s, want 400", n, resp.StatusCode, data)
		}
	}
}

func TestCreateJobHTTPWithoutURL(t *testing.T) {
	app := newTestApp(t, newFakeQueue(), nil)
	resp, data := doJSON(t, app, "POST", "/jobs", []byte(`{"type":"http","payload":{}}`))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s, want 400", resp.StatusCode, data)
	}
}

func TestCreateJobDefaultMaxAttempts(t *testing.T) {
	q := newFakeQueue()
	app := newTestApp(t, q, nil)
	resp, data := doJSON(t, app, "POST", "/jobs", []byte(`{"type":"noop"}`))
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, body = %s, want 201", resp.StatusCode, data)
	}
	if q.jobs[1] == nil || q.jobs[1].MaxAttempts != 3 {
		t.Fatalf("default max_attempts not applied: %+v", q.jobs[1])
	}
}

func TestGetJob(t *testing.T) {
	q := newFakeQueue()
	app := newTestApp(t, q, nil)
	if _, err := q.CreateJob(context.Background(), "noop", nil, 3); err != nil {
		t.Fatalf("seed job: %v", err)
	}

	resp, data := doJSON(t, app, "GET", "/jobs/1", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s, want 200", resp.StatusCode, data)
	}
	if !strings.Contains(string(data), `"job"`) {
		t.Fatalf("body = %s", data)
	}

	// 404 for missing jobs.
	resp, data = doJSON(t, app, "GET", "/jobs/999", nil)
	if resp.StatusCode != http.StatusNotFound ||
		!strings.Contains(string(data), `"job not found"`) {
		t.Fatalf("status = %d, body = %s, want 404", resp.StatusCode, data)
	}

	// 400 for a non-numeric id.
	resp, data = doJSON(t, app, "GET", "/jobs/abc", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s, want 400", resp.StatusCode, data)
	}
}

func TestListJobsValidation(t *testing.T) {
	q := newFakeQueue()
	app := newTestApp(t, q, nil)

	resp, _ := doJSON(t, app, "GET", "/jobs?status=weird", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid status: status = %d, want 400", resp.StatusCode)
	}

	// Valid listing shapes.
	resp, data := doJSON(t, app, "GET", "/jobs", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list: status = %d, want 200", resp.StatusCode)
	}
	var got struct {
		Count  int `json:"count"`
		Limit  int `json:"limit"`
		Offset int `json:"offset"`
	}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal list: %v", err)
	}
	if got.Limit != 50 || got.Offset != 0 {
		t.Fatalf("default limit/offset = %d/%d, want 50/0", got.Limit, got.Offset)
	}

	resp, data = doJSON(t, app, "GET", "/jobs?limit=500", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list big limit: status = %d, want 200", resp.StatusCode)
	}
	data2 := map[string]any{}
	if err := json.Unmarshal(data, &data2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if int(data2["limit"].(float64)) != 100 {
		t.Fatalf("limit clamp broken: %v", data2["limit"])
	}

	resp, _ = doJSON(t, app, "GET", "/jobs?offset=-1", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("negative offset: status = %d, want 400", resp.StatusCode)
	}
	resp, _ = doJSON(t, app, "GET", "/jobs?limit=abc", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad limit: status = %d, want 400", resp.StatusCode)
	}
}

func TestRetryJob(t *testing.T) {
	q := newFakeQueue()
	app := newTestApp(t, q, nil)
	dead := &queue.Job{ID: 7, Type: "noop", Status: queue.StatusDead, RunAt: time.Now()}
	q.jobs[7] = dead

	resp, data := doJSON(t, app, "POST", "/jobs/7/retry", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("retry dead: status = %d, body = %s, want 200", resp.StatusCode, data)
	}

	// 409 for a non-dead job.
	q.jobs[7].Status = queue.StatusPending
	resp, _ = doJSON(t, app, "POST", "/jobs/7/retry", nil)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("retry pending: status = %d, want 409", resp.StatusCode)
	}

	// 404 for a missing job.
	resp, data = doJSON(t, app, "POST", "/jobs/8/retry", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("retry missing: status = %d, body = %s, want 404", resp.StatusCode, data)
	}
}

func TestHealth(t *testing.T) {
	q := newFakeQueue()

	// All ok.
	app := newTestApp(t, q, nil)
	resp, data := doJSON(t, app, "GET", "/health", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health: status = %d, body = %s, want 200", resp.StatusCode, data)
	}
	m := map[string]any{}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal health: %v", err)
	}
	if m["status"] != "ok" {
		t.Fatalf("health status = %v", m["status"])
	}
	checks := m["checks"].(map[string]any)
	if checks["redis"] != "disabled" {
		t.Fatalf("redis check = %v, want disabled", checks["redis"])
	}

	// Postgres down -> degraded 503, error details stay in the logs.
	app = newTestApp(t, q, func(d *api.Deps) {
		d.Postgres = fakePinger{err: errors.New("connection refused")}
	})
	resp, data = doJSON(t, app, "GET", "/health", nil)
	if resp.StatusCode != http.StatusServiceUnavailable ||
		!strings.Contains(string(data), "degraded") {
		t.Fatalf("degraded: status = %d, body = %s, want 503", resp.StatusCode, data)
	}
	var boom struct {
		Checks map[string]any `json:"checks"`
	}
	if err := json.Unmarshal(data, &boom); err != nil {
		t.Fatalf("unmarshal degraded body: %v", err)
	}
	if boom.Checks["postgres"] != "error" {
		t.Fatalf("postgres check = %v, want 'error' without details", boom.Checks["postgres"])
	}
	if strings.Contains(string(data), "connection refused") {
		t.Fatalf("response leaks internal error text: %s", data)
	}
}

func TestRateLimit(t *testing.T) {
	q := newFakeQueue()
	app := newTestApp(t, q, func(d *api.Deps) {
		d.RateLimit = 2
		d.RateWindow = time.Second
	})

	resp, data := doJSON(t, app, "GET", "/health", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health before jobs: status = %d, want 200", resp.StatusCode)
	}

	// Fly two requests under the limit, then expect 429.
	for i := 0; i < 2; i++ {
		resp, data = doJSON(t, app, "POST", "/jobs", []byte(`{"type":"noop"}`))
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("request %d: status = %d, body = %s, want 201", i, resp.StatusCode, data)
		}
	}
	resp, data = doJSON(t, app, "POST", "/jobs", []byte(`{"type":"noop"}`))
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("third request: status = %d, body = %s, want 429", resp.StatusCode, data)
	}
	if !strings.Contains(string(data), `"rate limit exceeded"`) {
		t.Fatalf("body = %s, want rate limit error", data)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	app := newTestApp(t, newFakeQueue(), nil)
	resp, data := doJSON(t, app, "GET", "/metrics", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(string(data), "jobs_pending") {
		t.Fatalf("metrics body missing jobs_pending:\n%.300s", data)
	}
}

func TestInternalErrorDoesNotLeakDetails(t *testing.T) {
	// A panic inside a handler (nil Queue here) surfaces through the
	// recover middleware into the error handler, which must answer a
	// generic JSON error, not the panic text.
	app := newTestApp(t, nil, func(d *api.Deps) { d.Queue = nil })
	resp, data := doJSON(t, app, "GET", "/jobs?status=pending", nil)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
	if strings.Contains(string(data), "panic") || strings.Contains(string(data), "nil pointer") {
		t.Fatalf("internal error text leaked: %s", data)
	}
	lines := strings.Split(string(data), "\n")
	if len(lines) != 1 || !strings.Contains(lines[0], `"internal server error"`) {
		t.Fatalf("expected generic JSON error body, got %s", data)
	}
}

func TestJSONErrorHandler(t *testing.T) {
	app := newTestApp(t, newFakeQueue(), nil)
	resp, data := doJSON(t, app, "GET", "/definitely-not-a-route", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	if !strings.Contains(string(data), `"error"`) {
		t.Fatalf("body = %s, want JSON error shape", data)
	}
}
