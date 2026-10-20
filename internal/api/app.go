// Package api implements the HTTP control plane of the scheduler on
// Fiber v3.
package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/adaptor"
	"github.com/gofiber/fiber/v3/middleware/limiter"
	"github.com/gofiber/fiber/v3/middleware/recover"

	"scheduler/internal/queue"
)

// PostgresPinger checks PostgreSQL connectivity.
type PostgresPinger interface {
	Ping(ctx context.Context) error
}

// RedisPinger checks Redis connectivity. The real client is adapted
// with NewRedisPinger; tests use fakes.
type RedisPinger interface {
	Ping(ctx context.Context) error
}

// Deps wires the API to its dependencies. All collaborators are
// interfaces so tests can run without a database.
type Deps struct {
	Queue           JobQueue
	Postgres        PostgresPinger
	Redis           RedisPinger
	Logger          *slog.Logger
	MetricsHandler  http.Handler
	MaxPayloadBytes int
	JobTypes        []string
	RateLimit       int
	RateWindow      time.Duration
}

// New builds the Fiber application with all middleware and routes.
func New(deps Deps) *fiber.App {
	log := logger(deps.Logger)
	cfg := fiber.Config{
		ErrorHandler: func(c fiber.Ctx, err error) error {
			return httpErrorHandler(c, err, log)
		},
		BodyLimit:    bodyLimit(deps.MaxPayloadBytes),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
		// Fiber v3 does not print startup messages by default, so no
		// DisableStartupMessage knob exists.
	}
	app := fiber.New(cfg)
	app.Use(recover.New())

	app.Use(func(c fiber.Ctx) error {
		start := time.Now()
		err := c.Next()
		logger(deps.Logger).Info("request",
			"method", c.Method(),
			"path", c.Path(),
			"status", c.Response().StatusCode(),
			"duration", time.Since(start),
		)
		return err
	})

	if deps.MetricsHandler != nil {
		app.Get("/metrics", adaptor.HTTPHandler(deps.MetricsHandler))
	}
	app.Get("/health", func(c fiber.Ctx) error { return health(c, deps) })

	// Rate limiting applies to jobs management endpoints only.
	if deps.RateLimit > 0 {
		app.Use(rateLimiter(deps))
	}

	app.Post("/jobs", func(c fiber.Ctx) error { return createJob(c, deps) })
	app.Get("/jobs/:id", func(c fiber.Ctx) error { return getJob(c, deps) })
	app.Get("/jobs", func(c fiber.Ctx) error { return listJobs(c, deps) })
	app.Post("/jobs/:id/retry", func(c fiber.Ctx) error { return retryJob(c, deps) })

	return app
}

// rateLimiter returns the rate-limit middleware for /jobs* routes.
func rateLimiter(deps Deps) fiber.Handler {
	window := deps.RateWindow
	if window <= 0 {
		window = time.Second
	}
	maxVal := deps.RateLimit
	cfg := limiter.Config{
		Max:        maxVal,
		Expiration: window,
		Next: func(c fiber.Ctx) bool {
			return !strings.HasPrefix(c.Path(), "/jobs")
		},
		LimitReached: func(c fiber.Ctx) error {
			return c.Status(fiber.StatusTooManyRequests).
				JSON(fiber.Map{"error": "rate limit exceeded"})
		},
	}
	return limiter.New(cfg)
}

func logger(l *slog.Logger) *slog.Logger {
	if l == nil {
		return slog.Default()
	}
	return l
}

// bodyLimit sizes the request-body limit with headroom above the
// max payload.
func bodyLimit(maxPayload int) int {
	if maxPayload <= 0 {
		return 256 << 10
	}
	return 2*maxPayload + 64<<10
}

// httpErrorHandler renders returned errors as JSON without leaking
// internals: *fiber.Error keeps its message; anything else is a
// generic "internal server error" with the real cause logged.
func httpErrorHandler(c fiber.Ctx, err error, log *slog.Logger) error {
	code := fiber.StatusInternalServerError
	msg := "internal server error"
	var fiberErr *fiber.Error
	if errors.As(err, &fiberErr) {
		code = fiberErr.Code
		msg = fiberErr.Message
	}
	if code >= fiber.StatusInternalServerError {
		log.Error("http handler error", "method", c.Method(),
			"path", c.Path(), "status", code, "error", err)
	}
	return c.Status(code).JSON(fiber.Map{"error": msg})
}

// validJobStatuses is the closed set accepted for listing.
var validJobStatuses = map[string]struct{}{
	string(queue.StatusPending):   {},
	string(queue.StatusRunning):   {},
	string(queue.StatusCompleted): {},
	string(queue.StatusFailed):    {},
	string(queue.StatusDead):      {},
}

func statusOK(s string) bool {
	_, ok := validJobStatuses[s]
	return ok
}

func parseID(raw string) (int64, bool) {
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id < 1 {
		return 0, false
	}
	return id, true
}
