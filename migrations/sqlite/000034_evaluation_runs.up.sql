CREATE TABLE evaluation_runs (
    task_id TEXT PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    dataset_id TEXT NOT NULL,
    status INTEGER NOT NULL,
    detail_json TEXT NOT NULL,
    started_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_evaluation_runs_tenant_started
    ON evaluation_runs (tenant_id, started_at DESC);
