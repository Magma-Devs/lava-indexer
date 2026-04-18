// Package state tracks which block heights have been indexed by which
// handler. Coverage is stored as non-overlapping [from, to] ranges, keyed by
// handler name. This lets the indexer:
//
//   - Re-run with a different requested window and only fetch the uncovered
//     gap (e.g. "this week do blocks 1M–2M, next week do 0–1M").
//   - Add new handlers later that only cover the blocks seen after they were
//     registered, without affecting existing handlers' coverage.
//   - Survive abrupt shutdowns: row writes and range-merges share one tx per
//     batch, so a crashed process either persists both or neither.
package state

import (
	"context"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Range is an inclusive [From, To] block-height range.
type Range struct {
	From int64
	To   int64
}

type State struct {
	pool   *pgxpool.Pool
	schema string
}

func New(pool *pgxpool.Pool, schema string) *State {
	return &State{pool: pool, schema: schema}
}

func (s *State) Schema() string { return s.schema }

// Ensure creates the schema + state tables if missing. Idempotent.
func (s *State) Ensure(ctx context.Context) error {
	sql := fmt.Sprintf(`
		CREATE SCHEMA IF NOT EXISTS %[1]s;
		CREATE TABLE IF NOT EXISTS %[1]s.indexer_ranges (
		  handler     TEXT        NOT NULL,
		  from_height BIGINT      NOT NULL,
		  to_height   BIGINT      NOT NULL,
		  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
		  PRIMARY KEY (handler, from_height),
		  CHECK (to_height >= from_height)
		);
		CREATE INDEX IF NOT EXISTS idx_indexer_ranges_handler_to
		  ON %[1]s.indexer_ranges (handler, to_height);
	`, s.schema)
	if _, err := s.pool.Exec(ctx, sql); err != nil {
		return err
	}
	if err := s.ensureRunsTable(ctx); err != nil {
		return err
	}
	return s.ensureFailuresTable(ctx)
}

// ensureFailuresTable creates the dead-letter table for batches that failed
// all retries. Skipped heights stay out of indexer_ranges, so the next
// invocation picks them up — the row here records the reason for any
// operator who wants to look later.
func (s *State) ensureFailuresTable(ctx context.Context) error {
	sql := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %[1]s.indexer_failures (
		  handler       TEXT        NOT NULL,
		  height        BIGINT      NOT NULL,
		  reason        TEXT        NOT NULL,
		  retries       INT         NOT NULL DEFAULT 0,
		  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		  last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
		  PRIMARY KEY (handler, height)
		);`, s.schema)
	_, err := s.pool.Exec(ctx, sql)
	return err
}

// RecordFailure upserts a dead-letter entry for a height that exhausted its
// retry budget. Safe to call repeatedly — each call bumps retries and
// last_seen_at without changing first_seen_at.
func (s *State) RecordFailure(ctx context.Context, handler string, height int64, reason string) error {
	_, err := s.pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.indexer_failures (handler, height, reason, retries)
		VALUES ($1, $2, $3, 1)
		ON CONFLICT (handler, height) DO UPDATE SET
		  reason       = EXCLUDED.reason,
		  retries      = %s.indexer_failures.retries + 1,
		  last_seen_at = now()`, s.schema, s.schema),
		handler, height, reason)
	return err
}

// FailureCount returns (total_dead_letter_rows, max_retries) per handler.
// Used by the UI to surface "there are holes and they've been tried N times".
func (s *State) FailureCount(ctx context.Context, handler string) (count int64, maxRetries int, err error) {
	row := s.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT COUNT(*), COALESCE(MAX(retries), 0)
		 FROM %s.indexer_failures WHERE handler = $1`, s.schema), handler)
	err = row.Scan(&count, &maxRetries)
	return
}

// FailureRanges coalesces individual dead-letter heights into contiguous
// ranges so the UI can paint them as red bars on the timeline instead of
// 1-pixel spikes.
func (s *State) FailureRanges(ctx context.Context, handler string) ([]Range, error) {
	// Classic "gaps and islands" pattern: consecutive heights share
	// (height − row_number()) and collapse to one group.
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		WITH numbered AS (
		  SELECT height, ROW_NUMBER() OVER (ORDER BY height) AS rn
		  FROM %s.indexer_failures
		  WHERE handler = $1
		)
		SELECT MIN(height), MAX(height)
		FROM numbered
		GROUP BY (height - rn)
		ORDER BY 1`, s.schema), handler)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRanges(rows)
}

// Ranges returns ranges for a single handler, sorted by From.
func (s *State) Ranges(ctx context.Context, handler string) ([]Range, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		`SELECT from_height, to_height FROM %s.indexer_ranges
		 WHERE handler = $1 ORDER BY from_height`, s.schema), handler)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRanges(rows)
}

// AllRanges returns every handler's ranges as a map — used by the UI to show
// per-handler progress side-by-side.
func (s *State) AllRanges(ctx context.Context) (map[string][]Range, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		`SELECT handler, from_height, to_height FROM %s.indexer_ranges
		 ORDER BY handler, from_height`, s.schema))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string][]Range)
	for rows.Next() {
		var h string
		var r Range
		if err := rows.Scan(&h, &r.From, &r.To); err != nil {
			return nil, err
		}
		out[h] = append(out[h], r)
	}
	return out, rows.Err()
}

// Gaps returns the subranges of [start, end] NOT covered by the handler's
// ranges. Used by the pipeline to plan fetch work.
func (s *State) Gaps(ctx context.Context, handler string, start, end int64) ([]Range, error) {
	if end < start {
		return nil, nil
	}
	ranges, err := s.Ranges(ctx, handler)
	if err != nil {
		return nil, err
	}
	return computeGaps(ranges, start, end), nil
}

// UnionGaps returns heights in [start, end] where AT LEAST ONE handler needs
// coverage. Useful for the fetcher: we fetch the superset, then each batch
// only calls Persist on handlers that actually need that specific range.
func (s *State) UnionGaps(ctx context.Context, handlers []string, start, end int64) ([]Range, error) {
	if len(handlers) == 0 || end < start {
		return nil, nil
	}
	var union []Range
	for _, h := range handlers {
		g, err := s.Gaps(ctx, h, start, end)
		if err != nil {
			return nil, err
		}
		union = mergeRanges(append(union, g...))
	}
	return union, nil
}

// HandlerNeedsRange reports whether [from, to] is entirely or partially
// missing from the handler's coverage.
func (s *State) HandlerNeedsRange(ctx context.Context, handler string, from, to int64) (bool, error) {
	gaps, err := s.Gaps(ctx, handler, from, to)
	if err != nil {
		return false, err
	}
	return len(gaps) > 0, nil
}

// RecordRange merges [from, to] into the handler's ranges inside the given
// transaction. Call with the same tx that wrote the block rows so both
// commit atomically.
//
// A per-handler advisory lock serialises concurrent range writers: without
// it, two workers writing adjacent batches would each fail to see the
// other's pending insert and produce two touching-but-unmerged rows.
// The lock is transaction-scoped, so it releases on commit/rollback and
// has no cross-tx cost.
func (s *State) RecordRange(ctx context.Context, tx pgx.Tx, handler string, from, to int64) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1)::bigint)`, handler); err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}
	rows, err := tx.Query(ctx, fmt.Sprintf(
		`SELECT from_height, to_height FROM %s.indexer_ranges
		 WHERE handler = $1 AND from_height <= $2 + 1 AND to_height + 1 >= $3
		 FOR UPDATE`, s.schema), handler, to, from)
	if err != nil {
		return fmt.Errorf("lock touching: %w", err)
	}
	var touching []int64
	newFrom, newTo := from, to
	for rows.Next() {
		var f, t int64
		if err := rows.Scan(&f, &t); err != nil {
			rows.Close()
			return err
		}
		if f < newFrom {
			newFrom = f
		}
		if t > newTo {
			newTo = t
		}
		touching = append(touching, f)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(touching) > 0 {
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			`DELETE FROM %s.indexer_ranges WHERE handler = $1 AND from_height = ANY($2::bigint[])`,
			s.schema), handler, touching); err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s.indexer_ranges (handler, from_height, to_height) VALUES ($1, $2, $3)`,
		s.schema), handler, newFrom, newTo)
	return err
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func scanRanges(rows pgx.Rows) ([]Range, error) {
	var out []Range
	for rows.Next() {
		var r Range
		if err := rows.Scan(&r.From, &r.To); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func computeGaps(ranges []Range, start, end int64) []Range {
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].From < ranges[j].From })
	var gaps []Range
	cursor := start
	for _, r := range ranges {
		if r.To < cursor {
			continue
		}
		if r.From > end {
			break
		}
		if cursor < r.From {
			top := r.From - 1
			if top > end {
				top = end
			}
			gaps = append(gaps, Range{From: cursor, To: top})
		}
		if r.To+1 > cursor {
			cursor = r.To + 1
		}
	}
	if cursor <= end {
		gaps = append(gaps, Range{From: cursor, To: end})
	}
	return gaps
}

// mergeRanges sorts and coalesces touching/overlapping ranges into a minimal
// non-overlapping sequence.
func mergeRanges(rs []Range) []Range {
	if len(rs) == 0 {
		return nil
	}
	sort.Slice(rs, func(i, j int) bool { return rs[i].From < rs[j].From })
	out := []Range{rs[0]}
	for _, r := range rs[1:] {
		last := &out[len(out)-1]
		if r.From <= last.To+1 {
			if r.To > last.To {
				last.To = r.To
			}
		} else {
			out = append(out, r)
		}
	}
	return out
}
