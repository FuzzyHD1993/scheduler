// Package worker executes claimed jobs with retries, exponential
// backoff and graceful shutdown semantics.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"scheduler/internal/queue"
)

// JobQueue is the consumer-side view of the queue used by the worker.
// Declared here (not in queue) so tests can inject fakes.
type JobQueue interface {
	ClaimJob(ctx context.Context, workerID string) (*queue.Job, error)
	CompleteJob(ctx context.Context, id int64, workerID string) error
	FailJob(ctx context.Context, id int64, workerID, errMsg string, retryAt time.Time) (queue.Status, error)
	ReleaseJob(ctx context.Context, id int64, workerID string) error
}

// Metrics is the metrics sink the worker reports to.
type Metrics interface {
	IncProcessed(status string)
	IncFailed()
}

// Defaults injected when the corresponding Worker field is empty.
const (
	DefaultJobTimeout   = 10 * time.Second
	DefaultBackoffBase  = 2 * time.Second
	DefaultBackoffMax   = 5 * time.Minute
	DefaultPollInterval = 500 * time.Millisecond

	// maxRespBodyBytes bounds how much of an HTTP response body is read.
	maxRespBodyBytes = 64 << 10 // 64 KiB

	// lastErrorBodyLimit bounds the response-body snippet stored in errors.
	lastErrorBodyLimit = 512

	// releaseTimeout bounds releasing jobs to the queue on shutdown.
	releaseTimeout = 5 * time.Second

	// postActionTimeout bounds CompleteJob/FailJob after execution so
	// shutdown ordering cannot stall them forever.
	postActionTimeout = 5 * time.Second
)

// AllowedHTTPMethods is the set of methods an http job may request.
var AllowedHTTPMethods = map[string]struct{}{
	http.MethodGet:    {},
	http.MethodPost:   {},
	http.MethodPut:    {},
	http.MethodPatch:  {},
	http.MethodDelete: {},
}

// httpPayload is the schema of an http job payload.
type httpPayload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}

// Worker polls the queue and executes jobs.
type Worker struct {
	ID           string
	Queue        JobQueue
	HTTPClient   *http.Client
	Logger       *slog.Logger
	Metrics      Metrics
	JobTimeout   time.Duration
	BackoffBase  time.Duration
	BackoffMax   time.Duration
	PollInterval time.Duration
}

// Run claims and executes jobs until ctx is cancelled. Jobs claimed
// when the parent context is cancelled are released back to the queue
// without consuming an attempt.
func (w *Worker) Run(ctx context.Context) {
	logger := w.logger()
	for ctx.Err() == nil {
		job, err := w.Queue.ClaimJob(ctx, w.ID)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logger.Error("claim job failed", "worker", w.ID, "error", err)
			if !sleepCtx(ctx, w.pollInterval()) {
				return
			}
			continue
		}
		if job == nil {
			if !sleepCtx(ctx, w.pollInterval()) {
				return
			}
			continue
		}

		started := time.Now()
		jobCtx, cancelJob := context.WithTimeout(ctx, w.jobTimeout())
		execErr := w.Execute(jobCtx, job)
		cancelJob()

		switch {
		case execErr == nil:
			// Finish through a short detached context so a shutdown
			// racing the completion still records the job as done.
			completeCtx, cancelComplete := context.WithTimeout(context.WithoutCancel(ctx), postActionTimeout)
			err := w.Queue.CompleteJob(completeCtx, job.ID, w.ID)
			cancelComplete()
			switch {
			case err == nil:
				w.Metrics.IncProcessed(string(queue.StatusCompleted))
				logger.Info("job completed",
					"worker", w.ID, "job_id", job.ID, "type", job.Type,
					"duration", time.Since(started))
			case errors.Is(err, queue.ErrJobNotOwned):
				// Fencing conflict: another worker reclaimed the job.
				logger.Warn("complete job not owned, job was reclaimed",
					"worker", w.ID, "job_id", job.ID, "error", err)
			default:
				logger.Error("complete job failed",
					"worker", w.ID, "job_id", job.ID, "error", err)
			}
		case ctx.Err() != nil:
			// Shutdown, not a job failure: hand the job back untouched.
			releaseCtx, cancelRelease := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
			err := w.Queue.ReleaseJob(releaseCtx, job.ID, w.ID)
			cancelRelease()
			switch {
			case err == nil:
			case errors.Is(err, queue.ErrJobNotOwned):
				logger.Warn("release job not owned, job was reclaimed",
					"worker", w.ID, "job_id", job.ID, "error", err)
			default:
				logger.Error("release job on shutdown failed",
					"worker", w.ID, "job_id", job.ID, "error", err)
			}
			return
		default:
			attempts := job.Attempts + 1
			retryAt := time.Now().Add(Backoff(attempts, w.backoffBase(), w.backoffMax()))
			failCtx, cancelFail := context.WithTimeout(context.WithoutCancel(ctx), postActionTimeout)
			status, err := w.Queue.FailJob(failCtx, job.ID, w.ID, execErr.Error(), retryAt)
			cancelFail()
			if err != nil {
				if errors.Is(err, queue.ErrJobNotOwned) {
					logger.Warn("fail job not owned, job was reclaimed",
						"worker", w.ID, "job_id", job.ID, "error", err)
				} else {
					logger.Error("fail job failed",
						"worker", w.ID, "job_id", job.ID, "error", err)
				}
				continue
			}
			// jobs_processed_total only documents completed|failed|dead;
			// the retryable 'pending' outcome maps to 'failed'.
			metricStatus := queue.StatusFailed
			if status == queue.StatusDead {
				metricStatus = queue.StatusDead
			}
			w.Metrics.IncFailed()
			w.Metrics.IncProcessed(string(metricStatus))
			logger.Warn("job failed",
				"worker", w.ID, "job_id", job.ID, "type", job.Type,
				"attempt", attempts, "status", status,
				"duration", time.Since(started), "error", execErr.Error())
		}
	}
}

// Execute runs one job. It returns an error when the job failed and
// should be retried. Unknown job types are ordinary execution errors
// so they go through the common retry path.
func (w *Worker) Execute(ctx context.Context, job *queue.Job) error {
	httpClient := w.httpClient()
	switch job.Type {
	case "noop":
		return nil
	case "http":
		return w.executeHTTP(ctx, httpClient, job)
	default:
		return fmt.Errorf("unknown job type %q", job.Type)
	}
}

// executeHTTP performs the HTTP request described by the payload.
// Payload validation errors and unsuccessful responses are reported
// as errors; secrets and headers are never logged.
func (w *Worker) executeHTTP(ctx context.Context, httpClient *http.Client, job *queue.Job) error {
	var p httpPayload
	if err := json.Unmarshal(job.Payload, &p); err != nil {
		return fmt.Errorf("malformed http payload: %w", err)
	}

	u, err := url.Parse(strings.TrimSpace(p.URL))
	if err != nil || u.Scheme == "" {
		return errors.New("http payload has invalid url")
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errors.New("http payload requires url with http/https scheme and host")
	}

	method := strings.ToUpper(strings.TrimSpace(p.Method))
	if method == "" {
		method = http.MethodPost
	}
	if _, ok := AllowedHTTPMethods[method]; !ok {
		return fmt.Errorf("http method %q not allowed", method)
	}

	var bodyReader io.Reader
	if p.Body != "" {
		bodyReader = strings.NewReader(p.Body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bodyReader)
	if err != nil {
		return fmt.Errorf("build http request: %w", err)
	}
	for k, v := range p.Headers {
		// Values are passed through; header keys and values never reach logs.
		req.Header.Set(k, v)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	// Read at most the limit; the body is closed via defer.
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxRespBodyBytes))

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("http request got status %d: %s",
			resp.StatusCode, truncateBytes(string(snippet), lastErrorBodyLimit))
	}
	return nil
}

// Backoff computes the retry delay for attempt n (1-based) with
// exponential growth base*2^(n-1), capped at max and jittered ±20%.
func Backoff(attempt int, base, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if base <= 0 {
		base = DefaultBackoffBase
	}
	if max < base {
		max = base
	}

	d := base
	for i := 1; i < attempt; i++ {
		if d > max/2 {
			d = max
			break
		}
		d *= 2
	}

	// ±20% jitter, clamped to the cap.
	factor := 0.8 + 0.4*rand.Float64()
	jittered := time.Duration(float64(d) * factor)
	if jittered > max {
		jittered = max
	}
	return jittered
}

// truncateBytes makes a string safe for a TEXT column: invalid UTF-8
// bytes are replaced with U+FFFD, then the result is cut to at most
// limit bytes on a rune boundary.
func truncateBytes(s string, limit int) string {
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "\uFFFD")
	}
	if len(s) <= limit {
		return s
	}
	cut := s[:limit]
	for len(cut) > 0 && !utf8.ValidString(cut) {
		cut = cut[:len(cut)-1]
	}
	return cut
}

// sleepCtx waits for d or until ctx is done; returns false when ctx
// ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// pollInterval returns the poll interval with a small asymmetry
// breaker (±10%) so workers do not lockstep on an empty queue.
func (w *Worker) pollInterval() time.Duration {
	base := w.PollInterval
	if base <= 0 {
		base = DefaultPollInterval
	}
	j := base / 5
	if j < 1 {
		j = 1
	}
	return base - j/2 + time.Duration(rand.Int64N(int64(j)))
}

func (w *Worker) jobTimeout() time.Duration {
	if w.JobTimeout <= 0 {
		return DefaultJobTimeout
	}
	return w.JobTimeout
}

func (w *Worker) backoffBase() time.Duration {
	if w.BackoffBase <= 0 {
		return DefaultBackoffBase
	}
	return w.BackoffBase
}

func (w *Worker) backoffMax() time.Duration {
	if w.BackoffMax <= 0 {
		return DefaultBackoffMax
	}
	return w.BackoffMax
}

func (w *Worker) httpClient() *http.Client {
	if w.HTTPClient != nil {
		return w.HTTPClient
	}
	return &http.Client{Timeout: w.jobTimeout()}
}

func (w *Worker) logger() *slog.Logger {
	if w.Logger != nil {
		return w.Logger
	}
	return slog.Default()
}
