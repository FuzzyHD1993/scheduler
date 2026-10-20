// Command scheduler is the entry point of the distributed job
// scheduler: API, workers and maintenance loops.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/gofiber/fiber/v3"

	"scheduler/internal/api"
	"scheduler/internal/config"
	"scheduler/internal/metrics"
	"scheduler/internal/queue"
	"scheduler/internal/store"
	"scheduler/internal/worker"
	"scheduler/migrations"
)

// version is overridden at build time with -ldflags "-X main.version=...".
var version = "dev"

// connectTotalTimeout bounds all Postgres connect attempts.
const (
	connectTotalTimeout   = 30 * time.Second
	connectAttemptTimeout = 3 * time.Second
	connectRetryInterval  = time.Second
)

// connectPostgres connects with per-attempt timeouts and log-paced
// retries for up to 30 seconds, returning a ready pool. The retry
// pause honours ctx cancellation so a SIGTERM stops the loop early.
func connectPostgres(ctx context.Context, logger *slog.Logger, dsn string) (*store.Store, error) {
	deadline := time.Now().Add(connectTotalTimeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, fmt.Errorf("postgres not reachable within %s", connectTotalTimeout)
		}
		attemptTimeout := min(remaining, connectAttemptTimeout)
		attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		st, err := store.New(attemptCtx, dsn)
		cancel()
		if err == nil {
			return st, nil
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("postgres connect cancelled: %w", ctx.Err())
		}
		logger.Warn("postgres connect failed, retrying",
			"error", err,
			"retry_in", connectRetryInterval.String(),
			"time_left", remaining.Truncate(time.Millisecond).String())
		if time.Until(deadline) <= connectRetryInterval {
			return nil, fmt.Errorf("postgres not reachable within %s: %w", connectTotalTimeout, err)
		}
		if !sleepCtx(ctx, connectRetryInterval) {
			return nil, ctx.Err()
		}
	}
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

func newLogger(levelName string) (*slog.Logger, error) {
	level, err := config.LogLevel(levelName)
	if err != nil {
		return nil, err
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})), nil
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}

func main() {
	var showVersion bool
	flag.BoolVar(&showVersion, "version", false, "print version and exit")
	flag.Parse()

	if showVersion {
		fmt.Println(version)
		return
	}

	cfg, err := config.Load()
	if err != nil {
		fatal("config", err)
	}
	logger, err := newLogger(cfg.LogLevel)
	if err != nil {
		fatal("config", err)
	}
	logger.Info("starting scheduler", "version", version, "workers", cfg.WorkerCount)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := connectPostgres(ctx, logger, cfg.DatabaseURL)
	if err != nil {
		logger.Error("postgres connect failed", "error", err)
		os.Exit(1)
	}
	defer st.Close()
	pool := st.Pool()

	if cfg.AutoMigrate {
		if err := store.Migrate(ctx, pool, migrations.FS); err != nil {
			logger.Error("migrations failed", "error", err)
			os.Exit(1)
		}
		logger.Info("migrations applied")
	}

	m := metrics.New()
	redisClient := connectRedis(cfg.RedisAddr)
	var redisPinger api.RedisPinger
	if redisClient != nil {
		redisPinger = api.NewRedisPinger(redisClient)
	}

	workerID := cfg.WorkerIDPrefix
	if workerID == "" {
		workerID = hostname()
	}
	jobQueue := queue.NewQueue(pool)

	app := api.New(api.Deps{
		Queue:           jobQueue,
		Postgres:        st,
		Redis:           redisPinger,
		Logger:          logger,
		MetricsHandler:  m.Handler(),
		MaxPayloadBytes: cfg.MaxPayloadBytes,
		JobTypes:        cfg.JobTypes,
		RateLimit:       cfg.RateLimit,
		RateWindow:      cfg.RateWindow,
	})

	// Background maintenance loops.
	var bgWG sync.WaitGroup
	reaperCtx, cancelReaper := context.WithCancel(ctx)
	bgWG.Add(1)
	go runReaper(reaperCtx, logger, jobQueue, cfg.StaleTimeout, &bgWG)

	pendingCtx, cancelPending := context.WithCancel(ctx)
	bgWG.Add(1)
	go runPendingUpdater(pendingCtx, logger, jobQueue, m, &bgWG)

	// HTTP server. Unexpected Listen errors must fail the process.
	httpErrCh := make(chan error, 1)
	var httpWG sync.WaitGroup
	httpWG.Add(1)
	go func() {
		defer httpWG.Done()
		logger.Info("http listening", "addr", cfg.HTTPAddr)
		err := app.Listen(cfg.HTTPAddr, fiber.ListenConfig{DisableStartupMessage: true})
		switch {
		case err == nil,
			errors.Is(err, http.ErrServerClosed),
			errors.Is(err, context.Canceled):
			// Graceful shutdown path: not a failure.
		default:
			httpErrCh <- err
		}
	}()

	// Workers, each with its own instance so their IDs are distinct.
	workerCtx, cancelWorkers := context.WithCancel(context.Background())
	var workerWG sync.WaitGroup
	for i := 1; i <= cfg.WorkerCount; i++ {
		w := worker.Worker{
			ID:           fmt.Sprintf("%s-w%d", workerID, i),
			Queue:        jobQueue,
			Logger:       logger,
			Metrics:      m,
			HTTPClient:   &http.Client{Timeout: cfg.JobTimeout},
			JobTimeout:   cfg.JobTimeout,
			BackoffBase:  cfg.BackoffBase,
			BackoffMax:   cfg.BackoffMax,
			PollInterval: cfg.PollInterval,
		}
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			w.Run(workerCtx)
		}()
	}

	// Wait for SIGTERM or for a fatal HTTP failure.
	exitCode := 0
	select {
	case <-ctx.Done():
		logger.Info("shutdown initiated")
	case err := <-httpErrCh:
		logger.Error("http server failed", "error", err)
		exitCode = 1
		stop()
	}

	// Stop workers first so they do not claim new jobs while the API
	// drains; claimed jobs are released through their shutdown path.
	cancelWorkers()

	// Drain in-flight HTTP requests.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelShutdown()
	if err := app.ShutdownWithContext(shutdownCtx); err != nil && !errors.Is(err, fiber.ErrNotRunning) {
		logger.Error("http shutdown", "error", err)
	}
	httpWG.Wait()

	// Wait for workers to release and finish.
	workersDone := make(chan struct{})
	go func() {
		workerWG.Wait()
		close(workersDone)
	}()
	select {
	case <-workersDone:
	case <-time.After(cfg.ShutdownTimeout):
		logger.Warn("workers did not stop within shutdown timeout")
	}

	cancelReaper()
	cancelPending()
	bgWG.Wait()

	if redisClient != nil {
		if err := redisClient.Close(); err != nil {
			logger.Error("redis close", "error", err)
		}
	}
	st.Close()
	logger.Info("shutdown complete")
	if exitCode != 0 {
		os.Exit(exitCode)
	}
}

// fatal logs to a bare stdout JSON logger and exits; used only before
// the real logger exists.
func fatal(what string, err error) {
	slog.New(slog.NewJSONHandler(os.Stdout, nil)).Error(what, "error", err)
	os.Exit(1)
}

func runReaper(ctx context.Context, logger *slog.Logger, jobQueue *queue.Queue, staleTimeout time.Duration, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := jobQueue.ReclaimStaleJobs(ctx, staleTimeout)
			if err != nil {
				logger.Error("reclaim stale jobs failed", "error", err)
				continue
			}
			if n > 0 {
				logger.Warn("reclaimed stale jobs", "count", n)
			}
		}
	}
}

func runPendingUpdater(ctx context.Context, logger *slog.Logger, jobQueue *queue.Queue, m *metrics.Metrics, wg *sync.WaitGroup) {
	defer wg.Done()
	updater := func() {
		n, err := jobQueue.CountPending(ctx)
		if err != nil {
			logger.Error("count pending failed", "error", err)
			return
		}
		m.SetPending(float64(n))
	}
	updater()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			updater()
		}
	}
}

func connectRedis(addr string) *redis.Client {
	if addr == "" {
		return nil
	}
	return redis.NewClient(&redis.Options{Addr: addr})
}
