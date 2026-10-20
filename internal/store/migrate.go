package store

import (
	"context"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// migrationLockID is an arbitrary but fixed advisory lock key so that
// only one scheduler instance applies migrations at a time.
const migrationLockID int64 = 742_199_001

// Migrate applies every *.sql file found in fsys (sorted by filename)
// that has not been applied yet. Each file runs inside its own
// transaction. The run is idempotent: re-running it does nothing once
// all versions are recorded in schema_migrations.
func Migrate(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("migrate: acquire connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockID); err != nil {
		return fmt.Errorf("migrate: acquire advisory lock: %w", err)
	}
	defer func() {
		// Best effort: the lock is session-scoped and dies with the
		// connection, so a failed unlock can only leave a lock on a
		// connection the pool is about to recycle anyway.
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_, _ = conn.Exec(unlockCtx, "SELECT pg_advisory_unlock($1)", migrationLockID)
	}()

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version TEXT PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return fmt.Errorf("migrate: create schema_migrations: %w", err)
	}

	versions, err := appliedVersions(ctx, conn)
	if err != nil {
		return err
	}

	migrations, err := listMigrations(fsys)
	if err != nil {
		return err
	}

	for _, name := range migrations {
		if _, ok := versions[name]; ok {
			continue
		}
		if err := applyMigration(ctx, conn, name, fsys); err != nil {
			return err
		}
	}
	return nil
}

// appliedVersions loads all versions recorded so far into a set.
func appliedVersions(ctx context.Context, conn *pgxpool.Conn) (map[string]struct{}, error) {
	rows, err := conn.Query(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return nil, fmt.Errorf("migrate: read applied versions: %w", err)
	}
	defer rows.Close()

	versionSet := make(map[string]struct{})
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("migrate: scan version: %w", err)
		}
		versionSet[version] = struct{}{}
	}
	return versionSet, rows.Err()
}

// listMigrations collects and sorts all *.sql files in fsys.
func listMigrations(fsys fs.FS) ([]string, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("migrate: read filesystem: %w", err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() || !fs.ValidPath(e.Name()) {
			continue
		}
		if path.Ext(e.Name()) != ".sql" {
			continue
		}
		names = append(names, e.Name())
	}
	slices.Sort(names)
	return names, nil
}

// applyMigration runs one SQL file fully (it may contain many
// statements) in its own transaction and records the version. The file
// is executed through the simple query protocol, which supports
// multi-statement strings.
func applyMigration(ctx context.Context, conn *pgxpool.Conn, name string, fsys fs.FS) error {
	sqlBytes, err := fs.ReadFile(fsys, name)
	if err != nil {
		return fmt.Errorf("migrate: read %s: %w", name, err)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("migrate: begin %s: %w", name, err)
	}

	if _, err := tx.Exec(ctx, string(sqlBytes), pgx.QueryExecModeSimpleProtocol); err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("migrate: apply %s: %w", name, err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", name); err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("migrate: record %s: %w", name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("migrate: commit %s: %w", name, err)
	}
	return nil
}
