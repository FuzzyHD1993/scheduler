package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// StaleReclaimError is the fixed error message applied to dead jobs
// whose worker crashed mid-run.
const StaleReclaimError = "lock expired: worker crashed or timed out"

// lastErrorLimit bounds the persisted error text.
const lastErrorLimit = 2048

// jobColumns is the shared projection for full job rows.
const jobColumns = `id, type, payload, status, attempts, max_attempts,
	run_at, locked_by, locked_at, last_error, created_at, updated_at`

// Queue is a PostgreSQL-backed job queue.
type Queue struct {
	pool *pgxpool.Pool
}

// NewQueue builds a Queue on top of the given pool.
func NewQueue(pool *pgxpool.Pool) *Queue {
	return &Queue{pool: pool}
}

// CreateJob inserts a new pending job and returns it. A nil or empty
// payload is stored as {}; a non-positive maxAttempts falls back to
// DefaultMaxAttempts.
func (q *Queue) CreateJob(ctx context.Context, jobType string, payload json.RawMessage, maxAttempts int) (*Job, error) {
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxAttempts
	}

	j := &Job{Type: jobType, Payload: payload, MaxAttempts: maxAttempts}
	err := q.pool.QueryRow(ctx, `INSERT INTO jobs (type, payload, max_attempts)
		VALUES ($1, $2, $3) RETURNING `+jobColumns,
		jobType, payload, maxAttempts).Scan(
		&j.ID, &j.Type, &j.Payload, &j.Status, &j.Attempts, &j.MaxAttempts,
		&j.RunAt, &j.LockedBy, &j.LockedAt, &j.LastError, &j.CreatedAt, &j.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("queue: create job: %w", err)
	}
	return j, nil
}

// GetJob returns a job by id, or ErrJobNotFound.
func (q *Queue) GetJob(ctx context.Context, id int64) (*Job, error) {
	j := &Job{}
	err := q.pool.QueryRow(ctx, "SELECT "+jobColumns+" FROM jobs WHERE id = $1", id).Scan(
		&j.ID, &j.Type, &j.Payload, &j.Status, &j.Attempts, &j.MaxAttempts,
		&j.RunAt, &j.LockedBy, &j.LockedAt, &j.LastError, &j.CreatedAt, &j.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrJobNotFound
		}
		return nil, fmt.Errorf("queue: get job: %w", err)
	}
	return j, nil
}

// ListJobs returns jobs in descending id order. An empty status means
// all statuses; limit and offset must be applied by the caller.
func (q *Queue) ListJobs(ctx context.Context, status string, limit, offset int) ([]*Job, error) {
	var (
		rows pgx.Rows
		err  error
	)
	if status == "" {
		rows, err = q.pool.Query(ctx,
			"SELECT "+jobColumns+" FROM jobs ORDER BY id DESC LIMIT $1 OFFSET $2", limit, offset)
	} else {
		rows, err = q.pool.Query(ctx,
			"SELECT "+jobColumns+" FROM jobs WHERE status = $1 ORDER BY id DESC LIMIT $2 OFFSET $3",
			status, limit, offset)
	}
	if err != nil {
		return nil, fmt.Errorf("queue: list jobs: %w", err)
	}
	defer rows.Close()

	jobs := make([]*Job, 0, max(limit, 0))
	for rows.Next() {
		j := &Job{}
		if err := rows.Scan(
			&j.ID, &j.Type, &j.Payload, &j.Status, &j.Attempts, &j.MaxAttempts,
			&j.RunAt, &j.LockedBy, &j.LockedAt, &j.LastError, &j.CreatedAt, &j.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("queue: list jobs scan: %w", err)
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// CountPending returns the number of pending jobs.
func (q *Queue) CountPending(ctx context.Context) (int64, error) {
	var n int64
	if err := q.pool.QueryRow(ctx,
		"SELECT count(*) FROM jobs WHERE status = 'pending'").Scan(&n); err != nil {
		return 0, fmt.Errorf("queue: count pending: %w", err)
	}
	return n, nil
}

// ClaimJob atomically claims the next pending, due job for the given
// worker. It returns (nil, nil) when no job is available. The
// SELECT ... FOR UPDATE SKIP LOCKED CTE guarantees two workers never
// claim the same row.
func (q *Queue) ClaimJob(ctx context.Context, workerID string) (*Job, error) {
	j := &Job{}
	err := q.pool.QueryRow(ctx, `WITH claimed AS (
			SELECT id FROM jobs
			WHERE status = 'pending' AND run_at <= now()
			ORDER BY run_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE jobs j
		SET status = 'running', locked_by = $1, locked_at = now(), updated_at = now()
		FROM claimed c
		WHERE j.id = c.id
		RETURNING j.id, j.type, j.payload, j.status, j.attempts, j.max_attempts,
			j.run_at, j.locked_by, j.locked_at, j.last_error, j.created_at, j.updated_at`,
		workerID).Scan(
		&j.ID, &j.Type, &j.Payload, &j.Status, &j.Attempts, &j.MaxAttempts,
		&j.RunAt, &j.LockedBy, &j.LockedAt, &j.LastError, &j.CreatedAt, &j.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("queue: claim job: %w", err)
	}
	return j, nil
}

// CompleteJob marks a running job completed. The update only matches
// rows still owned by workerID, so a stale worker cannot fence a job
// that has been reclaimed; zero affected rows yield ErrJobNotOwned.
func (q *Queue) CompleteJob(ctx context.Context, id int64, workerID string) error {
	tag, err := q.pool.Exec(ctx, `UPDATE jobs
		SET status = 'completed', locked_by = NULL, locked_at = NULL, updated_at = now()
		WHERE id = $1 AND status = 'running' AND locked_by = $2`, id, workerID)
	if err != nil {
		return fmt.Errorf("queue: complete job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrJobNotOwned
	}
	return nil
}

// FailJob records a failed attempt atomically: it increments
// attempts, marks the job dead when the attempt budget is exhausted,
// otherwise reschedules it at retryAt, clears the ownership lock and
// stores a bounded error message. It returns the new status.
func (q *Queue) FailJob(ctx context.Context, id int64, workerID, errMsg string, retryAt time.Time) (Status, error) {
	msg := truncateError(errMsg, lastErrorLimit)
	var status Status
	err := q.pool.QueryRow(ctx, `UPDATE jobs
		SET attempts = attempts + 1,
			status = CASE WHEN attempts + 1 >= max_attempts THEN 'dead' ELSE 'pending' END,
			run_at = CASE WHEN attempts + 1 >= max_attempts THEN run_at ELSE $3 END,
			locked_by = NULL,
			locked_at = NULL,
			last_error = $2,
			updated_at = now()
		WHERE id = $1 AND status = 'running' AND locked_by = $4
		RETURNING status`, id, msg, retryAt, workerID).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrJobNotOwned
		}
		return "", fmt.Errorf("queue: fail job: %w", err)
	}
	return status, nil
}

// ReleaseJob returns a running job to the pending queue without
// incrementing attempts, for graceful shutdown. Only the current owner
// may release it.
func (q *Queue) ReleaseJob(ctx context.Context, id int64, workerID string) error {
	tag, err := q.pool.Exec(ctx, `UPDATE jobs
		SET status = 'pending', run_at = now(),
			locked_by = NULL, locked_at = NULL, updated_at = now()
		WHERE id = $1 AND status = 'running' AND locked_by = $2`, id, workerID)
	if err != nil {
		return fmt.Errorf("queue: release job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrJobNotOwned
	}
	return nil
}

// ReclaimStaleJobs requeues running jobs whose ownership lock expired
// (worker crashed or timed out). Each reclaimed job consumes one
// attempt and goes dead once the budget is exhausted. Returns the
// number of reclaimed jobs.
func (q *Queue) ReclaimStaleJobs(ctx context.Context, timeout time.Duration) (int64, error) {
	tag, err := q.pool.Exec(ctx, `UPDATE jobs
		SET attempts = attempts + 1,
			status = CASE WHEN attempts + 1 >= max_attempts THEN 'dead' ELSE 'pending' END,
			locked_by = NULL,
			locked_at = NULL,
			last_error = $2,
			run_at = now(),
			updated_at = now()
		WHERE status = 'running' AND locked_at < now() - $1::double precision * interval '1 second'`,
		timeout.Seconds(), StaleReclaimError)
	if err != nil {
		return 0, fmt.Errorf("queue: reclaim stale jobs: %w", err)
	}
	return tag.RowsAffected(), nil
}

// RetryJob manually requeues a dead job with a fresh attempt budget.
// Returns ErrJobNotFound for unknown ids and ErrJobNotDead when the
// job is not in dead state.
func (q *Queue) RetryJob(ctx context.Context, id int64) (*Job, error) {
	var status Status
	err := q.pool.QueryRow(ctx, "SELECT status FROM jobs WHERE id = $1", id).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrJobNotFound
		}
		return nil, fmt.Errorf("queue: retry job lookup: %w", err)
	}
	if status != StatusDead {
		return nil, ErrJobNotDead
	}

	j := &Job{}
	err = q.pool.QueryRow(ctx, `UPDATE jobs
		SET status = 'pending', attempts = 0, run_at = now(),
			locked_by = NULL, locked_at = NULL, last_error = NULL, updated_at = now()
		WHERE id = $1 AND status = 'dead'
		RETURNING `+jobColumns, id).Scan(
		&j.ID, &j.Type, &j.Payload, &j.Status, &j.Attempts, &j.MaxAttempts,
		&j.RunAt, &j.LockedBy, &j.LockedAt, &j.LastError, &j.CreatedAt, &j.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Lost a race: the job is no longer dead.
			return nil, ErrJobNotDead
		}
		return nil, fmt.Errorf("queue: retry job: %w", err)
	}
	return j, nil
}
