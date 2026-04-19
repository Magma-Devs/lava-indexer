package state

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Run is a single execution lifetime of the indexer process. Each process
// start inserts one row; batch commits heartbeat into it; shutdown (or the
// next startup, if shutdown was ungraceful) marks it ended.
//
// Aggregating across runs gives you "total time spent indexing" and "total
// blocks ever indexed", which answers "how long did it take to index
// everything" even across crashes and planned restarts.
type Run struct {
	ID            int64      `json:"id"`
	StartedAt     time.Time  `json:"started_at"`
	LastHeartbeat time.Time  `json:"last_heartbeat"`
	EndedAt       *time.Time `json:"ended_at,omitempty"`
	BlocksIndexed int64      `json:"blocks_indexed"`
	RowsWritten   int64      `json:"rows_written"`
	EndReason     string     `json:"end_reason,omitempty"`
}

// ensureRunsTableTx creates the table on first use. Called from Ensure
// inside the schema-bring-up transaction so the runs table comes up with
// the rest of the state schema atomically.
func (s *State) ensureRunsTableTx(ctx context.Context, tx pgx.Tx) error {
	sql := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %[1]s.indexer_runs (
		  id             BIGSERIAL   PRIMARY KEY,
		  started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
		  last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now(),
		  ended_at       TIMESTAMPTZ,
		  blocks_indexed BIGINT      NOT NULL DEFAULT 0,
		  rows_written   BIGINT      NOT NULL DEFAULT 0,
		  end_reason     TEXT
		);
		CREATE INDEX IF NOT EXISTS idx_indexer_runs_started
		  ON %[1]s.indexer_runs (started_at DESC);
	`, s.schema)
	_, err := tx.Exec(ctx, sql)
	return err
}

// StartRun closes any previously-running row (interrupted crash, etc.) with
// its last-heartbeat as a proxy for ended_at, then opens a fresh one and
// returns its id.
func (s *State) StartRun(ctx context.Context) (int64, error) {
	// Close stale rows.
	if _, err := s.pool.Exec(ctx, fmt.Sprintf(
		`UPDATE %s.indexer_runs
		 SET ended_at = last_heartbeat, end_reason = 'interrupted'
		 WHERE ended_at IS NULL`, s.schema)); err != nil {
		return 0, fmt.Errorf("close stale runs: %w", err)
	}
	var id int64
	err := s.pool.QueryRow(ctx, fmt.Sprintf(
		`INSERT INTO %s.indexer_runs DEFAULT VALUES RETURNING id`, s.schema)).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert run: %w", err)
	}
	return id, nil
}

// Heartbeat records that the current run is still alive and has indexed
// some more blocks/rows. Called inside a batch transaction (so it commits
// atomically with the row writes and range merge).
func (s *State) Heartbeat(ctx context.Context, tx pgx.Tx, runID, blocks, rows int64) error {
	_, err := tx.Exec(ctx, fmt.Sprintf(
		`UPDATE %s.indexer_runs
		 SET last_heartbeat = now(),
		     blocks_indexed = blocks_indexed + $2,
		     rows_written   = rows_written   + $3
		 WHERE id = $1`, s.schema), runID, blocks, rows)
	return err
}

// EndRun marks the run complete with a human-readable reason. Safe to call
// multiple times; idempotent.
func (s *State) EndRun(ctx context.Context, runID int64, reason string) error {
	_, err := s.pool.Exec(ctx, fmt.Sprintf(
		`UPDATE %s.indexer_runs
		 SET ended_at = COALESCE(ended_at, now()),
		     end_reason = COALESCE(end_reason, $2)
		 WHERE id = $1`, s.schema), runID, reason)
	return err
}

// RecentRuns returns the most recent N runs, newest first.
func (s *State) RecentRuns(ctx context.Context, limit int) ([]Run, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		`SELECT id, started_at, last_heartbeat, ended_at,
		        blocks_indexed, rows_written,
		        COALESCE(end_reason, '')
		 FROM %s.indexer_runs
		 ORDER BY started_at DESC LIMIT $1`, s.schema), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		var r Run
		if err := rows.Scan(&r.ID, &r.StartedAt, &r.LastHeartbeat, &r.EndedAt,
			&r.BlocksIndexed, &r.RowsWritten, &r.EndReason); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Totals aggregates across every run. Returns (blocks, rows, cumulative
// wall-clock seconds).
func (s *State) Totals(ctx context.Context) (blocks, rows int64, seconds float64, firstStarted *time.Time, err error) {
	var ts *time.Time
	row := s.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT
		   COALESCE(SUM(blocks_indexed), 0)::bigint,
		   COALESCE(SUM(rows_written), 0)::bigint,
		   COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(ended_at, last_heartbeat) - started_at))), 0)::float8,
		   MIN(started_at)
		 FROM %s.indexer_runs`, s.schema))
	if err = row.Scan(&blocks, &rows, &seconds, &ts); err != nil {
		return
	}
	firstStarted = ts
	return
}
