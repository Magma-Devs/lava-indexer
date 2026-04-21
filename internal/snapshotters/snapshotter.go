// Package snapshotters is the extensibility seam for indexers that are
// NOT event-driven: jobs that pull chain state at specific heights and
// persist a snapshot of it. Parallel in spirit to internal/events but
// distinct in shape:
//
//   - Events fire whenever they happen; snapshotters fire on a calendar
//     (or any other caller-supplied schedule).
//   - Events are consumed out of the fetched block stream; snapshotters
//     make their own chain RPC calls for whatever data they need.
//   - Events commit alongside indexer_ranges; snapshotters own their own
//     table (one row per snapshot date) and don't touch indexer_ranges.
//
// A concrete example: provider_rewards queries Lava's estimated rewards
// endpoint on the 17th of every month at 15:00 UTC, for every registered
// provider, and stores one row per (date, provider, spec, source, denom).
// See internal/snapshotters/provider_rewards for the implementation.
package snapshotters

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Snapshotter is the interface a concrete snapshotter implements. Mirrors
// events.Handler in spirit — a stable name, DDL it owns, and a per-target
// write path that runs inside a pgx.Tx supplied by the registry.
//
// Lifecycle per registry tick:
//
//   1. BlocksDue returns the still-needed snapshot targets (usually bounded
//      to what the calendar has already produced and the DB hasn't covered).
//   2. For each target the registry opens a fresh pgx.Tx, calls Snapshot,
//      commits on success / rolls back on error. Failures are logged and
//      the next target still runs.
//
// Snapshotter implementations MUST be concurrency-safe for sequential
// invocations on one instance — the registry does not fan out snapshots
// in parallel, but it does re-invoke per tick without re-constructing.
type Snapshotter interface {
	// Name is the stable identifier used in log lines and the /api/snapshotters
	// response. Changing it looks like a brand-new snapshotter with no
	// coverage — pick once and keep it.
	Name() string

	// DDL returns the SQL statements that create this snapshotter's tables
	// and indexes. Idempotent — use IF NOT EXISTS. Run once at startup
	// inside a single tx (same pattern as events.Handler).
	DDL() []string

	// BlocksDue returns the set of (date, block) targets that still need a
	// snapshot. Implementations typically query their own snapshots table
	// to subtract already-covered dates. The registry runs Snapshot for
	// each returned target in turn.
	BlocksDue(ctx context.Context, pool *pgxpool.Pool) ([]SnapshotTarget, error)

	// Snapshot persists one snapshot. Runs inside the passed pgx.Tx so
	// either every write lands atomically or none does — killing the
	// process mid-snapshot leaves no partial rows.
	Snapshot(ctx context.Context, tx pgx.Tx, target SnapshotTarget) error
}

// SnapshotTarget is one (logical date, chain block, chain time) tuple
// BlocksDue emits and Snapshot consumes. SnapshotDate is the *logical*
// date the snapshot represents (e.g. 2025-01-17 00:00 UTC) — separate
// from BlockTime, which is the actual on-chain timestamp of the block
// chosen to represent that date.
type SnapshotTarget struct {
	BlockHeight  int64
	SnapshotDate time.Time
	BlockTime    time.Time
}
