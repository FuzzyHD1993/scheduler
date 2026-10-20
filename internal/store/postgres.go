// Package store owns PostgreSQL connectivity for the scheduler.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store wraps a pgx connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// New connects to PostgreSQL and builds the connection pool with
// conservative pool settings suitable for the scheduler workload.
func New(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("store: parse dsn: %w", err)
	}
	cfg.MaxConns = 10
	cfg.MinConns = 1
	cfg.MaxConnLifetime = time.Hour
	cfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("store: create pool: %w", err)
	}
	// The pool is created lazily: without an explicit ping the caller
	// would see success even with a dead database.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping db: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Pool exposes the underlying connection pool.
func (s *Store) Pool() *pgxpool.Pool {
	return s.pool
}

// Ping verifies database connectivity.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close closes the underlying pool.
func (s *Store) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}
