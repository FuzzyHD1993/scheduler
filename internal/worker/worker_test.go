package worker_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"scheduler/internal/queue"
	"scheduler/internal/worker"
)

// Backoff configuration used across the worker tests.
const (
	backoffTestBase = 2 * time.Second
	backoffTestMax  = 5 * time.Minute
)

// fakeQueue records every interaction and hands out a fixed set of
// claimed jobs from an ordered list.
type fakeQueue struct {
	mu sync.Mutex

	pendingJobs []*queue.Job  // drained by ClaimJob in order
	claimErr    error         // returned by every ClaimJob when set
	completeErr error         // CompleteJob failure; no successful outcome recorded
	failErr     error         // FailJob failure; no successful outcome recorded
	failStatus  queue.Status  // FailJob answer, defaults to pending
	outcome     chan struct{} // closed once a job is completed or failed

	completeIDs []int64
	releaseIDs  []int64
	failCalls   []failCall
}

type failCall struct {
	id      int64
	errMsg  string
	retryAt time.Time
	at      time.Time // when FailJob was called
}

func (q *fakeQueue) ClaimJob(context.Context, string) (*queue.Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.claimErr != nil {
		return nil, q.claimErr
	}
	if len(q.pendingJobs) == 0 {
		return nil, nil
	}
	job := q.pendingJobs[0]
	q.pendingJobs = q.pendingJobs[1:]
	return job, nil
}

func (q *fakeQueue) CompleteJob(_ context.Context, id int64, _ string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.completeErr != nil {
		return q.completeErr
	}
	q.completeIDs = append(q.completeIDs, id)
	q.signalOutcome()
	return nil
}

// signalOutcome unblocks runQueue as soon as a terminal answer exists.
// Callers must hold q.mu.
func (q *fakeQueue) signalOutcome() {
	if q.outcome != nil {
		select {
		case <-q.outcome:
		default:
			close(q.outcome)
		}
	}
}

func (q *fakeQueue) FailJob(_ context.Context, id int64, _ string, errMsg string, retryAt time.Time) (queue.Status, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.failErr != nil {
		return "", q.failErr
	}
	q.failCalls = append(q.failCalls, failCall{id: id, errMsg: errMsg, retryAt: retryAt, at: time.Now()})
	q.signalOutcome()
	if q.failStatus == "" {
		return queue.StatusPending, nil
	}
	return q.failStatus, nil
}

func (q *fakeQueue) ReleaseJob(_ context.Context, id int64, _ string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.releaseIDs = append(q.releaseIDs, id)
	return nil
}

func (q *fakeQueue) lastFail() failCall {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.failCalls) == 0 {
		return failCall{}
	}
	return q.failCalls[len(q.failCalls)-1]
}

func (q *fakeQueue) released() []int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]int64(nil), q.releaseIDs...)
}

func (q *fakeQueue) completed() []int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]int64(nil), q.completeIDs...)
}

// fakeMetrics records processed/failed increments.
type fakeMetrics struct {
	mu        sync.Mutex
	processed map[string]int
	failed    int
}

func (m *fakeMetrics) IncProcessed(status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.processed == nil {
		m.processed = map[string]int{}
	}
	m.processed[status]++
}

func (m *fakeMetrics) IncFailed() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failed++
}

func (m *fakeMetrics) processedFor(status string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.processed[status]
}

func (m *fakeMetrics) failedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failed
}

func newJob(typ, payload string, attempts int) *queue.Job {
	return &queue.Job{
		ID:          42,
		Type:        typ,
		Payload:     []byte(payload),
		Status:      queue.StatusPending,
		Attempts:    attempts,
		MaxAttempts: 3,
		RunAt:       time.Now(),
	}
}

// runQueue starts a worker to drain q, waits until a job outcome is
// recorded, stops the worker and returns it.
func runQueue(t *testing.T, q *fakeQueue, m *fakeMetrics, jobTimeout time.Duration) *worker.Worker {
	t.Helper()
	w := &worker.Worker{
		ID:           "test-worker",
		Queue:        q,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:      m,
		HTTPClient:   &http.Client{Timeout: time.Minute}, // per-request ctx enforces the job deadline
		JobTimeout:   jobTimeout,
		BackoffBase:  backoffTestBase,
		BackoffMax:   backoffTestMax,
		PollInterval: 5 * time.Millisecond,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()
	outcome := make(chan struct{})
	q.mu.Lock()
	q.outcome = outcome
	q.mu.Unlock()
	select {
	case <-outcome:
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker made no progress in time")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop in time")
	}
	return w
}

func TestNoopJobCompletes(t *testing.T) {
	q := &fakeQueue{pendingJobs: []*queue.Job{newJob("noop", "", 0)}}
	m := &fakeMetrics{}
	runQueue(t, q, m, time.Second)

	if got := q.completed(); len(got) != 1 || got[0] != 42 {
		t.Fatalf("completed = %v, want [42]", got)
	}
	if m.processedFor("completed") != 1 {
		t.Fatalf("processed[completed] = %d, want 1", m.processedFor("completed"))
	}
	if m.failedCount() != 0 {
		t.Fatalf("failed = %d, want 0", m.failedCount())
	}
}

func TestHTTPJobSuccess(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	payload := fmt.Sprintf(`{"url":%q,"method":"POST"}`, srv.URL+"/hook")
	q := &fakeQueue{pendingJobs: []*queue.Job{newJob("http", payload, 0)}}
	m := &fakeMetrics{}
	runQueue(t, q, m, time.Second)

	if len(q.completed()) != 1 {
		t.Fatalf("job did not complete: fail = %+v", q.lastFail())
	}
	if gotPath != "/hook" {
		t.Fatalf("path = %q", gotPath)
	}
	if m.processedFor("completed") != 1 {
		t.Fatalf("processed[completed] = %d, want 1", m.processedFor("completed"))
	}
}

func TestHTTPJobServerErrorRetries(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	payload := fmt.Sprintf(`{"url":%q}`, srv.URL)
	q := &fakeQueue{pendingJobs: []*queue.Job{newJob("http", payload, 0)}}
	m := &fakeMetrics{}
	runQueue(t, q, m, time.Second)

	if len(q.completed()) != 0 {
		t.Fatal("job completed on 500")
	}
	fail := q.lastFail()
	if fail.id != 42 {
		t.Fatalf("failed id = %d, want 42", fail.id)
	}
	if !strings.Contains(fail.errMsg, "500") {
		t.Fatalf("error %q does not mention the status", fail.errMsg)
	}
	// retryAt should approximate the backoff for attempt 1: base with
	// ±20% jitter, measured from the moment FailJob was called.
	delay := fail.retryAt.Sub(fail.at)
	if delay < backoffTestBase*8/10-time.Second || delay > backoffTestBase*12/10+time.Second {
		t.Fatalf("retry delay %v outside backoff jitter range", delay)
	}
	if m.failedCount() != 1 {
		t.Fatalf("failed = %d, want 1", m.failedCount())
	}
	if m.processedFor("failed") != 1 {
		t.Fatalf("processed[failed] = %d, want 1 (metrics map pending retries to failed)", m.processedFor("failed"))
	}
}

func TestHTTPJobDeadWhenAttemptsExhausted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	payload := fmt.Sprintf(`{"url":%q}`, srv.URL)
	q := &fakeQueue{
		pendingJobs: []*queue.Job{newJob("http", payload, 2)},
		failStatus:  queue.StatusDead, // FailJob answered the final status
	}
	m := &fakeMetrics{}
	runQueue(t, q, m, time.Second)

	if m.processedFor("dead") != 1 {
		t.Fatalf("processed[dead] = %d, want 1", m.processedFor("dead"))
	}
	if m.failedCount() != 1 {
		t.Fatalf("failed = %d, want 1", m.failedCount())
	}
	if m.processedFor("failed") != 0 {
		t.Fatalf("processed[failed] = %d, want 0 for a dead outcome", m.processedFor("failed"))
	}
	if m.processedFor("pending") != 0 {
		t.Fatalf("processed[pending] = %d, want 0: pending must not appear in metrics", m.processedFor("pending"))
	}
}

func TestHTTPJobTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Hang until the context deadline; never respond.
		<-r.Context().Done()
	}))
	defer srv.Close()

	payload := fmt.Sprintf(`{"url":%q}`, srv.URL)
	q := &fakeQueue{pendingJobs: []*queue.Job{newJob("http", payload, 0)}}
	m := &fakeMetrics{}
	runQueue(t, q, m, 100*time.Millisecond)

	fail := q.lastFail()
	if !strings.Contains(fail.errMsg, "context deadline exceeded") {
		t.Fatalf("expected deadline exceeded in error, got %q", fail.errMsg)
	}
	if m.failedCount() != 1 {
		t.Fatalf("failed = %d, want 1", m.failedCount())
	}
}

func TestMalformedPayloadIsOrdinaryError(t *testing.T) {
	q := &fakeQueue{pendingJobs: []*queue.Job{newJob("http", `{"url":`, 0)}}
	m := &fakeMetrics{}
	runQueue(t, q, m, time.Second)

	if m.failedCount() != 1 {
		t.Fatalf("failed = %d, want 1", m.failedCount())
	}
	fail := q.lastFail()
	if fail.id != 42 || fail.errMsg == "" {
		t.Fatalf("expected FailJob for malformed payload, got %+v", fail)
	}
}

func TestInvalidURLIsOrdinaryError(t *testing.T) {
	q := &fakeQueue{pendingJobs: []*queue.Job{newJob("http", `{"url":"http://"}`, 0)}}
	m := &fakeMetrics{}
	runQueue(t, q, m, time.Second)

	if m.failedCount() != 1 || len(q.released()) != 0 {
		t.Fatalf("invalid url should fail, not release: failed=%d released=%v",
			m.failedCount(), q.released())
	}
}

func TestUnknownJobTypeFails(t *testing.T) {
	q := &fakeQueue{pendingJobs: []*queue.Job{newJob("bogus", "{}", 0)}}
	m := &fakeMetrics{}
	runQueue(t, q, m, time.Second)

	fail := q.lastFail()
	if fail.errMsg == "" {
		t.Fatal("expected an execution error for unknown type")
	}
	if m.failedCount() != 1 {
		t.Fatalf("failed = %d, want 1", m.failedCount())
	}
}

func TestShutdownReleasesClaimedJob(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer srv.Close()

	payload := fmt.Sprintf(`{"url":%q}`, srv.URL)
	q := &fakeQueue{pendingJobs: []*queue.Job{newJob("http", payload, 0)}}
	m := &fakeMetrics{}
	w := &worker.Worker{
		ID:           "test-worker",
		Queue:        q,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:      m,
		HTTPClient:   &http.Client{Timeout: 5 * time.Second},
		JobTimeout:   5 * time.Second,
		BackoffBase:  backoffTestBase,
		BackoffMax:   backoffTestMax,
		PollInterval: 5 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	// Wait until the job has been claimed (pending list drained).
	deadline := time.Now().Add(2 * time.Second)
	for {
		q.mu.Lock()
		claimed := len(q.pendingJobs) == 0
		q.mu.Unlock()
		if claimed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("job was never claimed")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop in time")
	}

	rel := q.released()
	if len(rel) != 1 || rel[0] != 42 {
		t.Fatalf("released = %v, want [42]", rel)
	}
	if len(q.completed()) != 0 {
		t.Fatal("completed the job it should have released")
	}
	if m.failedCount() != 0 {
		t.Fatalf("shutdown treated as failure: failed = %d", m.failedCount())
	}
}

func TestClaimErrorSleepsAndExitsOnCancel(t *testing.T) {
	q := &fakeQueue{claimErr: errors.New("db unavailable")}
	w := &worker.Worker{
		ID:           "test-worker",
		Queue:        q,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:      &fakeMetrics{},
		JobTimeout:   time.Second,
		PollInterval: 5 * time.Millisecond,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop after ctx deadline")
	}
}

func TestBackoff(t *testing.T) {
	base := backoffTestBase
	max := backoffTestMax

	// Attempt 1: base with ±20% jitter.
	d := worker.Backoff(1, base, max)
	if d < base*8/10 || d > base*12/10 {
		t.Fatalf("attempt 1 backoff %v outside jitter range", d)
	}
	// Sampling the jitter range a few times should exhibit variance.
	sawDifferent := false
	first := worker.Backoff(1, base, max)
	for i := 0; i < 20; i++ {
		if worker.Backoff(1, base, max) != first {
			sawDifferent = true
			break
		}
	}
	if !sawDifferent {
		t.Fatal("jitter produced no variance across calls")
	}

	// Attempt 4: base * 8 = 16s ±20%.
	d = worker.Backoff(4, base, max)
	if d < 16*time.Second*8/10 || d > 16*time.Second*12/10 {
		t.Fatalf("attempt 4 backoff %v outside jitter range", d)
	}

	// Huge attempt: never overflows and is capped at max. Jitter may
	// lower the value below the cap but never raise it above.
	d = worker.Backoff(100, base, max)
	if d > max || d < max*8/10 {
		t.Fatalf("attempt 100 backoff %v outside capped jitter range [%.0fs, %.0fs]",
			d, max.Seconds()*0.8, max.Seconds())
	}

	// Degenerate input: attempt <= 1 still yields a positive delay.
	if d = worker.Backoff(0, base, max); d <= 0 {
		t.Fatalf("attempt 0 returned %v, want a positive delay", d)
	}

	// Zero base falls back to the default base.
	if d = worker.Backoff(1, 0, max); d <= 0 {
		t.Fatalf("zero base returned %v, want a positive delay", d)
	}

	// Max smaller than base clamps to base.
	if d = worker.Backoff(7, base, base/2); d < base*8/10 || d > base*12/10 {
		t.Fatalf("max < base gave %v, want ~base", d)
	}
}

// runBudget runs a worker for a fixed wall-clock budget and waits for
// it to stop. Unlike runQueue it does not wait for a job outcome, so
// it also works when CompleteJob/FailJob errors suppress outcomes.
func runBudget(t *testing.T, q *fakeQueue, m *fakeMetrics, jobTimeout, budget time.Duration) {
	t.Helper()
	w := &worker.Worker{
		ID:           "test-worker",
		Queue:        q,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:      m,
		HTTPClient:   &http.Client{Timeout: time.Minute},
		JobTimeout:   jobTimeout,
		BackoffBase:  backoffTestBase,
		BackoffMax:   backoffTestMax,
		PollInterval: 5 * time.Millisecond,
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(budget + 5*time.Second):
		t.Fatal("worker did not stop in time")
	}
}

func TestCompleteJobErrorIsNotCountedAsSuccess(t *testing.T) {
	q := &fakeQueue{
		pendingJobs: []*queue.Job{newJob("noop", "", 0), newJob("noop", "", 0)},
		completeErr: errors.New("db gone"),
	}
	m := &fakeMetrics{}
	runBudget(t, q, m, time.Second, 300*time.Millisecond)

	if m.processedFor("completed") != 0 {
		t.Fatalf("processed[completed] = %d, want 0: completion failed", m.processedFor("completed"))
	}
	if m.failedCount() != 0 {
		t.Fatalf("failed = %d, want 0: job did not fail", m.failedCount())
	}
}

func TestFailJobErrorIsNotCountedAsProcessed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	payload := fmt.Sprintf(`{"url":%q}`, srv.URL)
	q := &fakeQueue{
		pendingJobs: []*queue.Job{newJob("http", payload, 0), newJob("http", payload, 0)},
		failErr:     errors.New("db gone"),
	}
	m := &fakeMetrics{}
	runBudget(t, q, m, time.Second, 300*time.Millisecond)

	if m.failedCount() != 0 {
		t.Fatalf("failed = %d, want 0: FailJob did not record anything", m.failedCount())
	}
	if m.processedFor("failed") != 0 || m.processedFor("dead") != 0 {
		t.Fatalf("processed statuses incremented despite FailJob error: %+v", m.processed)
	}
}

func TestInvalidUTF8ResponseBodyIsSanitized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte{0xff, 0xfe, 'x'})
	}))
	defer srv.Close()

	payload := fmt.Sprintf(`{"url":%q}`, srv.URL)
	q := &fakeQueue{pendingJobs: []*queue.Job{newJob("http", payload, 0)}}
	m := &fakeMetrics{}
	runQueue(t, q, m, time.Second)

	fail := q.lastFail()
	if fail.errMsg == "" {
		t.Fatal("expected an error message with the response body")
	}
	if !utf8.ValidString(fail.errMsg) {
		t.Fatalf("errMsg is not valid UTF-8: %q", fail.errMsg)
	}
	if !strings.Contains(fail.errMsg, "\uFFFD") {
		t.Fatalf("expected replacement characters in %q", fail.errMsg)
	}
}
