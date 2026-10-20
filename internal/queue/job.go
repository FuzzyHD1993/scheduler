// Package queue implements a PostgreSQL-backed job queue with atomic
// claiming (FOR UPDATE SKIP LOCKED) and fencing for completed,
// failed and released jobs.
package queue

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"
)

// Status is the lifecycle state of a job.
type Status string

// Job statuses. 'failed' is reserved by the schema and never set by
// the runtime lifecycle.
const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusDead      Status = "dead"
)

// Sentinel errors returned by the queue.
var (
	// ErrJobNotFound is returned when a job id does not exist.
	ErrJobNotFound = errors.New("queue: job not found")
	// ErrJobNotOwned is returned when the caller is not the current
	// owner of the job (fencing token mismatch).
	ErrJobNotOwned = errors.New("queue: job not owned by worker")
	// ErrJobNotDead is returned when retrying a job that is not dead.
	ErrJobNotDead = errors.New("queue: job is not dead")
)

// DefaultMaxAttempts is used when a caller passes a non-positive value.
const DefaultMaxAttempts = 3

// Job is the domain representation of a queued job. Pointer fields map
// to nullable database columns.
type Job struct {
	ID          int64           `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Status      Status          `json:"status"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	RunAt       time.Time       `json:"run_at"`
	LockedBy    *string         `json:"locked_by"`
	LockedAt    *time.Time      `json:"locked_at"`
	LastError   *string         `json:"last_error"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// truncateError makes an error message safe for a TEXT column:
// invalid UTF-8 bytes are replaced with U+FFFD, then the result is
// cut to at most limit bytes on a rune boundary.
func truncateError(msg string, limit int) string {
	if !utf8.ValidString(msg) {
		msg = strings.ToValidUTF8(msg, "\uFFFD")
	}
	if len(msg) <= limit {
		return msg
	}
	cut := msg[:limit]
	for len(cut) > 0 && !utf8.ValidString(cut) {
		cut = cut[:len(cut)-1]
	}
	return cut
}
