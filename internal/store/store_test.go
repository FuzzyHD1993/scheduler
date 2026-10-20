package store_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"scheduler/internal/queue"
	"scheduler/internal/store"
	"scheduler/migrations"
)

// testDSN reads the integration database URL or skips the tests.
func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set; skipping integration tests")
	}
	return dsn
}

// TestNewFailsFastOnUnreachableDB pins down P1-1: pgxpool is lazy, so
// store.New must explicitly ping and return an error (not a working
// pool) when the database is unreachable. Deliberately does not
// require TEST_DATABASE_URL and never skips.
func TestNewFailsFastOnUnreachableDB(t *testing.T) {
	dsn := "postgres://u:p@127.0.0.1:1/db?sslmode=disable"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	st, err := store.New(ctx, dsn)
	elapsed := time.Since(start)
	if err == nil {
		st.Close()
		t.Fatal("store.New succeeded against an unreachable database, want error")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("store.New took %v, want a fast failure", elapsed)
	}
	if !strings.Contains(err.Error(), "ping db") {
		t.Fatalf("error %v should mention the failed ping", err)
	}
}

// setup migrates the schema and resets the jobs table for each test.
func setup(t *testing.T) (*pgxpool.Pool, *queue.Queue) {
	t.Helper()
	ctx := context.Background()
	pool := connect(t, ctx)
	t.Cleanup(pool.Close)

	if err := store.Migrate(ctx, pool, migrations.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := pool.Exec(ctx, "TRUNCATE jobs RESTART IDENTITY"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return pool, queue.NewQueue(pool)
}

// connect opens a plain pool with retries so a freshly started test
// database does not make the suite flaky.
func connect(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	dsn := testDSN(t)
	deadline := time.Now().Add(10 * time.Second)
	for {
		pool, err := pgxpool.New(ctx, dsn)
		if err == nil {
			if err := pool.Ping(ctx); err == nil {
				return pool
			}
			pool.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("connect to test database: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// ageLock pushes the job's lock back by d seconds, as if the owning
// worker died d seconds ago.
func ageLock(t *testing.T, pool *pgxpool.Pool, id int64, d time.Duration) {
	t.Helper()
	if _, err := pool.Exec(t.Context(),
		"UPDATE jobs SET locked_at = now() - $2 * interval '1 second' WHERE id = $1",
		id, d.Seconds()); err != nil {
		t.Fatalf("age lock: %v", err)
	}
}

// jobRow reads a job row directly, bypassing the queue methods under test.
func jobRow(t *testing.T, pool *pgxpool.Pool, id int64) (*queue.Job, error) {
	t.Helper()
	j := &queue.Job{}
	err := pool.QueryRow(t.Context(),
		"SELECT id, type, payload, status, attempts, max_attempts, run_at, locked_by, locked_at, last_error, created_at, updated_at FROM jobs WHERE id = $1",
		id).Scan(
		&j.ID, &j.Type, &j.Payload, &j.Status, &j.Attempts, &j.MaxAttempts,
		&j.RunAt, &j.LockedBy, &j.LockedAt, &j.LastError, &j.CreatedAt, &j.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, queue.ErrJobNotFound
		}
		return nil, err
	}
	return j, nil
}

func TestCreateClaimComplete(t *testing.T) {
	pool, q := setup(t)
	ctx := context.Background()

	job, err := q.CreateJob(ctx, "noop", []byte(`{"x":1}`), 3)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if job.ID == 0 || job.Status != queue.StatusPending || job.Attempts != 0 {
		t.Fatalf("unexpected new job: %+v", job)
	}

	claimed, err := q.ClaimJob(ctx, "worker-1")
	if err != nil {
		t.Fatalf("ClaimJob: %v", err)
	}
	if claimed == nil {
		t.Fatal("ClaimJob returned no job")
	}
	if claimed.ID != job.ID || claimed.Status != queue.StatusRunning {
		t.Fatalf("claimed job wrong: %+v", claimed)
	}
	if claimed.LockedBy == nil || *claimed.LockedBy != "worker-1" {
		t.Fatalf("locked_by not set: %+v", claimed.LockedBy)
	}
	if claimed.LockedAt == nil {
		t.Fatal("locked_at not set")
	}

	if err := q.CompleteJob(ctx, job.ID, "worker-1"); err != nil {
		t.Fatalf("CompleteJob: %v", err)
	}

	row, err := jobRow(t, pool, job.ID)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if row.Status != queue.StatusCompleted {
		t.Fatalf("status = %q, want completed", row.Status)
	}
	if row.LockedBy != nil || row.LockedAt != nil {
		t.Fatalf("lock not cleared: locked_by=%v locked_at=%v", row.LockedBy, row.LockedAt)
	}
}

func TestReclaimStaleJobs(t *testing.T) {
	pool, q := setup(t)
	ctx := context.Background()

	job, err := q.CreateJob(ctx, "noop", nil, 3)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := q.ClaimJob(ctx, "worker-1"); err != nil {
		t.Fatalf("ClaimJob: %v", err)
	}

	ageLock(t, pool, job.ID, 10*time.Minute)

	n, err := q.ReclaimStaleJobs(ctx, 30*time.Second)
	if err != nil {
		t.Fatalf("ReclaimStaleJobs: %v", err)
	}
	if n != 1 {
		t.Fatalf("reclaimed = %d, want 1", n)
	}

	row, err := jobRow(t, pool, job.ID)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if row.Status != queue.StatusPending {
		t.Fatalf("status = %q, want pending", row.Status)
	}
	if row.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", row.Attempts)
	}
	if row.LastError == nil || *row.LastError != queue.StaleReclaimError {
		t.Fatalf("last_error = %v, want %q", row.LastError, queue.StaleReclaimError)
	}

	// Second run finds nothing left to reclaim.
	n, err = q.ReclaimStaleJobs(ctx, 30*time.Second)
	if err != nil {
		t.Fatalf("ReclaimStaleJobs again: %v", err)
	}
	if n != 0 {
		t.Fatalf("second reclaim = %d, want 0", n)
	}
}

func TestFencing(t *testing.T) {
	pool, q := setup(t)
	ctx := context.Background()

	job, err := q.CreateJob(ctx, "noop", nil, 3)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := q.ClaimJob(ctx, "worker-A"); err != nil {
		t.Fatalf("ClaimJob A: %v", err)
	}

	// Simulate a crashed worker whose lock was reclaimed by the reaper.
	ageLock(t, pool, job.ID, 10*time.Minute)
	if _, err := q.ReclaimStaleJobs(ctx, 30*time.Second); err != nil {
		t.Fatalf("ReclaimStaleJobs: %v", err)
	}

	claimed, err := q.ClaimJob(ctx, "worker-B")
	if err != nil {
		t.Fatalf("ClaimJob B: %v", err)
	}
	if claimed == nil || claimed.ID != job.ID {
		t.Fatalf("worker B did not get the job: %+v", claimed)
	}

	err = q.CompleteJob(ctx, job.ID, "worker-A")
	if !errors.Is(err, queue.ErrJobNotOwned) {
		t.Fatalf("stale worker complete: err = %v, want ErrJobNotOwned", err)
	}
	if err := q.CompleteJob(ctx, job.ID, "worker-B"); err != nil {
		t.Fatalf("new owner complete: %v", err)
	}
}

func TestFailRetryDeadLifecycle(t *testing.T) {
	pool, q := setup(t)
	ctx := context.Background()

	job, err := q.CreateJob(ctx, "noop", nil, 2)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	claimed, err := q.ClaimJob(ctx, "worker-1")
	if err != nil || claimed == nil {
		t.Fatalf("ClaimJob: %v, %v", claimed, err)
	}

	// First failure with attempts < max: back to pending, run_at in future.
	retryAt := time.Now().Add(30 * time.Second)
	status, err := q.FailJob(ctx, job.ID, "worker-1", "boom", retryAt)
	if err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	if status != queue.StatusPending {
		t.Fatalf("status = %q, want pending", status)
	}

	row, err := jobRow(t, pool, job.ID)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if row.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", row.Attempts)
	}
	if row.Status != queue.StatusPending {
		t.Fatalf("row status = %q, want pending", row.Status)
	}
	if !row.RunAt.After(time.Now()) {
		t.Fatalf("run_at = %v, want in the future", row.RunAt)
	}
	if row.LastError == nil || *row.LastError != "boom" {
		t.Fatalf("last_error = %v, want 'boom'", row.LastError)
	}

	// Second claim and final failure: attempts reach max -> dead.
	// The job is scheduled in the future; simulate the retry becoming due.
	if _, err := pool.Exec(ctx,
		"UPDATE jobs SET run_at = now() WHERE id = $1", job.ID); err != nil {
		t.Fatalf("reschedule for retry: %v", err)
	}
	claimed2, err := q.ClaimJob(ctx, "worker-2")
	if err != nil || claimed2 == nil {
		t.Fatalf("ClaimJob: %v, %v", claimed2, err)
	}
	status, err = q.FailJob(ctx, job.ID, "worker-2", "still broken", retryAt)
	if err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	if status != queue.StatusDead {
		t.Fatalf("status = %q, want dead", status)
	}

	// The job can no longer be owned/failed by anyone.
	if _, err := q.FailJob(ctx, job.ID, "worker-2", "again", retryAt); !errors.Is(err, queue.ErrJobNotOwned) {
		t.Fatalf("fail dead job: err = %v, want ErrJobNotOwned", err)
	}

	// RetryJob on dead resets attempts.
	revived, err := q.RetryJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("RetryJob: %v", err)
	}
	if revived.Status != queue.StatusPending || revived.Attempts != 0 {
		t.Fatalf("retry result: status=%q attempts=%d", revived.Status, revived.Attempts)
	}
	if revived.LastError != nil {
		t.Fatalf("last_error not cleared: %v", revived.LastError)
	}

	// Retrying a pending job is rejected.
	if _, err := q.RetryJob(ctx, job.ID); !errors.Is(err, queue.ErrJobNotDead) {
		t.Fatalf("RetryJob pending: err = %v, want ErrJobNotDead", err)
	}

	// Unknown job id: not found.
	if _, err := q.RetryJob(ctx, 123456); !errors.Is(err, queue.ErrJobNotFound) {
		t.Fatalf("RetryJob unknown: err = %v, want ErrJobNotFound", err)
	}
}

func TestSwarmClaims(t *testing.T) {
	_, q := setup(t)
	ctx := context.Background()

	const total = 100
	const swarm = 10
	for i := 0; i < total; i++ {
		if _, err := q.CreateJob(ctx, "noop", nil, 1); err != nil {
			t.Fatalf("CreateJob: %v", err)
		}
	}

	var mu sync.Mutex
	var claimed []int64
	var wg sync.WaitGroup
	for w := 0; w < swarm; w++ {
		wg.Add(1)
		go func(workerID string) {
			defer wg.Done()
			for {
				job, err := q.ClaimJob(ctx, workerID)
				if err != nil {
					t.Errorf("ClaimJob: %v", err)
					return
				}
				if job == nil {
					return
				}
				if err := q.CompleteJob(ctx, job.ID, workerID); err != nil {
					t.Errorf("CompleteJob: %v", err)
					return
				}
				mu.Lock()
				claimed = append(claimed, job.ID)
				mu.Unlock()
			}
		}(fmt.Sprintf("swarm-%d", w))
	}
	wg.Wait()

	if len(claimed) != total {
		t.Fatalf("claimed %d jobs, want %d", len(claimed), total)
	}
	seen := make(map[int64]struct{}, total)
	for _, id := range claimed {
		if _, dup := seen[id]; dup {
			t.Fatalf("job %d was claimed twice", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != total {
		t.Fatalf("claimed %d unique ids, want %d (missing %v)",
			len(seen), total, missingIDs(total, seen))
	}
}

func missingIDs(total int, seen map[int64]struct{}) []int64 {
	var missing []int64
	for id := int64(1); id <= int64(total); id++ {
		if _, ok := seen[id]; !ok {
			missing = append(missing, id)
		}
	}
	if len(missing) > 5 {
		return missing[:5]
	}
	return missing
}
