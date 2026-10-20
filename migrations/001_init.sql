-- Initial schema for the distributed job scheduler.
CREATE TABLE IF NOT EXISTS jobs (
    id           BIGSERIAL PRIMARY KEY,
    type         VARCHAR(64) NOT NULL,
    payload      JSONB NOT NULL DEFAULT '{}'::jsonb,
    status       VARCHAR(16) NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'running', 'completed', 'failed', 'dead')),
    attempts     INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
    run_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by    VARCHAR(128),
    locked_at    TIMESTAMPTZ,
    last_error   TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_run_at ON jobs (status, run_at);
CREATE INDEX IF NOT EXISTS idx_jobs_locked ON jobs (locked_by) WHERE locked_by IS NOT NULL;
