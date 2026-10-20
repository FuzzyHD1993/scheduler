package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/redis/go-redis/v9"

	"scheduler/internal/queue"
)

// JobQueue is the API-side view of the job queue, narrowed for the
// control plane so tests can inject fakes.
type JobQueue interface {
	CreateJob(ctx context.Context, jobType string, payload json.RawMessage, maxAttempts int) (*queue.Job, error)
	GetJob(ctx context.Context, id int64) (*queue.Job, error)
	ListJobs(ctx context.Context, status string, limit, offset int) ([]*queue.Job, error)
	RetryJob(ctx context.Context, id int64) (*queue.Job, error)
	CountPending(ctx context.Context) (int64, error)
}

// NewRedisPinger adapts a go-redis client to the RedisPinger
// interface.
func NewRedisPinger(c *redis.Client) RedisPinger {
	return redisPinger{c: c}
}

type redisPinger struct {
	c *redis.Client
}

func (r redisPinger) Ping(ctx context.Context) error {
	return r.c.Ping(ctx).Err()
}

// Default limits for job creation and listing.
const (
	defaultMaxAttempts = 3
	minMaxAttempts     = 1
	maxMaxAttempts     = 100

	defaultListLimit = 50
	maxListLimit     = 100

	// healthTimeout bounds each dependency check in /health.
	healthTimeout = 2 * time.Second
)

// reqContext returns a context for the request, carrying its
// cancellation signal into downstream I/O.
func reqContext(c fiber.Ctx) context.Context {
	return c.RequestCtx()
}

// createJob handles POST /jobs.
func createJob(c fiber.Ctx, deps Deps) error {
	var req struct {
		Type        string          `json:"type"`
		Payload     json.RawMessage `json:"payload"`
		MaxAttempts *int            `json:"max_attempts"`
	}
	if err := json.Unmarshal(c.Body(), &req); err != nil {
		return respondError(c, fiber.StatusBadRequest, "invalid JSON body")
	}

	if len(req.Payload) > deps.MaxPayloadBytes {
		return respondError(c, fiber.StatusRequestEntityTooLarge, "payload too large")
	}

	if req.Type == "" {
		return respondError(c, fiber.StatusBadRequest, "type is required")
	}
	if !allowedType(deps, req.Type) {
		return respondError(c, fiber.StatusBadRequest, "unknown job type")
	}

	maxAttempts := defaultMaxAttempts
	if req.MaxAttempts != nil {
		maxAttempts = *req.MaxAttempts
		if maxAttempts < minMaxAttempts || maxAttempts > maxMaxAttempts {
			return respondError(c, fiber.StatusBadRequest, "max_attempts must be between 1 and 100")
		}
	}

	payload := req.Payload
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}

	if req.Type == "http" {
		if err := validateHTTPPayload(payload); err != nil {
			return respondError(c, fiber.StatusBadRequest, err.Error())
		}
	}

	job, err := deps.Queue.CreateJob(reqContext(c), req.Type, payload, maxAttempts)
	if err != nil {
		return respondError(c, fiber.StatusInternalServerError, "could not create job")
	}
	return c.Status(fiber.StatusCreated).JSON(fiber.Map{"job": job})
}

// validateHTTPPayload checks the stored payload against the http job
// requirements: valid JSON with an http/https url carrying a host.
func validateHTTPPayload(payload []byte) error {
	var p struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return errors.New("http payload must be a valid JSON object")
	}
	u, err := url.Parse(strings.TrimSpace(p.URL))
	if err != nil || u.Scheme == "" {
		return errors.New("http payload requires a valid url")
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errors.New("http payload requires url with http/https scheme and host")
	}
	return nil
}

func allowedType(deps Deps, t string) bool {
	for _, allowed := range deps.JobTypes {
		if allowed == t {
			return true
		}
	}
	return false
}

// getJob handles GET /jobs/:id.
func getJob(c fiber.Ctx, deps Deps) error {
	id, ok := parseID(c.Params("id"))
	if !ok {
		return respondError(c, fiber.StatusBadRequest, "invalid job id")
	}
	job, err := deps.Queue.GetJob(reqContext(c), id)
	if err != nil {
		if errors.Is(err, queue.ErrJobNotFound) {
			return respondError(c, fiber.StatusNotFound, "job not found")
		}
		return respondError(c, fiber.StatusInternalServerError, "could not get job")
	}
	return c.Status(fiber.StatusOK).JSON(fiber.Map{"job": job})
}

// listJobs handles GET /jobs?status=&limit=&offset=.
func listJobs(c fiber.Ctx, deps Deps) error {
	status := c.Query("status")
	if status != "" && !statusOK(status) {
		return respondError(c, fiber.StatusBadRequest, "invalid status")
	}

	limit := defaultListLimit
	if raw := c.Query("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return respondError(c, fiber.StatusBadRequest, "invalid limit")
		}
		if n < 0 {
			return respondError(c, fiber.StatusBadRequest, "limit must be non-negative")
		}
		limit = n
	}
	offset := 0
	if raw := c.Query("offset"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return respondError(c, fiber.StatusBadRequest, "invalid offset")
		}
		if n < 0 {
			return respondError(c, fiber.StatusBadRequest, "offset must be non-negative")
		}
		offset = n
	}
	limit = min(limit, maxListLimit)

	jobs, err := deps.Queue.ListJobs(reqContext(c), status, limit, offset)
	if err != nil {
		return respondError(c, fiber.StatusInternalServerError, "could not list jobs")
	}
	if jobs == nil {
		jobs = []*queue.Job{}
	}
	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"jobs":   jobs,
		"count":  len(jobs),
		"limit":  limit,
		"offset": offset,
	})
}

// retryJob handles POST /jobs/:id/retry.
func retryJob(c fiber.Ctx, deps Deps) error {
	id, ok := parseID(c.Params("id"))
	if !ok {
		return respondError(c, fiber.StatusBadRequest, "invalid job id")
	}
	job, err := deps.Queue.RetryJob(reqContext(c), id)
	switch {
	case err == nil:
		return c.Status(fiber.StatusOK).JSON(fiber.Map{"job": job})
	case errors.Is(err, queue.ErrJobNotFound):
		return respondError(c, fiber.StatusNotFound, "job not found")
	case errors.Is(err, queue.ErrJobNotDead):
		return respondError(c, fiber.StatusConflict, "job is not dead")
	default:
		return respondError(c, fiber.StatusInternalServerError, "could not retry job")
	}
}

// health handles GET /health with PostgreSQL and Redis checks.
// Error details never reach the response body; they are logged only.
func health(c fiber.Ctx, deps Deps) error {
	checks := make(map[string]string, 2)
	ok := true

	if deps.Postgres != nil {
		ctx, cancel := context.WithTimeout(reqContext(c), healthTimeout)
		if err := deps.Postgres.Ping(ctx); err != nil {
			logger(deps.Logger).Error("health check failed", "component", "postgres", "error", err)
			checks["postgres"] = "error"
			ok = false
		} else {
			checks["postgres"] = "ok"
		}
		cancel()
	}

	if deps.Redis != nil {
		ctx, cancel := context.WithTimeout(reqContext(c), healthTimeout)
		if err := deps.Redis.Ping(ctx); err != nil {
			logger(deps.Logger).Error("health check failed", "component", "redis", "error", err)
			checks["redis"] = "error"
			ok = false
		} else {
			checks["redis"] = "ok"
		}
		cancel()
	} else {
		checks["redis"] = "disabled"
	}

	body := fiber.Map{"checks": checks}
	if ok {
		body["status"] = "ok"
		return c.Status(fiber.StatusOK).JSON(body)
	}
	body["status"] = "degraded"
	return c.Status(fiber.StatusServiceUnavailable).JSON(body)
}

func respondError(c fiber.Ctx, code int, msg string) error {
	return c.Status(code).JSON(fiber.Map{"error": msg})
}
