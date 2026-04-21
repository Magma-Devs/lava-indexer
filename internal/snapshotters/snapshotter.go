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

	"github.com/jackc/pgx/v5/pgxpool"
)

// Snapshotter is the interface a concrete snapshotter implements. Mirrors
// events.Handler in spirit — a stable name, DDL it owns, and a per-target
// write path that owns its own tx boundary.
//
// Lifecycle per registry tick:
//
//   1. BlocksDue returns the still-needed snapshot targets (usually bounded
//      to what the calendar has already produced and the DB hasn't covered).
//   2. For each target the registry calls Snapshot, which does its own
//      HTTP fan-out (outside any tx) and opens short transactions for
//      the DB writes. Failures are logged and the next target still runs.
//
// Why the snapshotter owns the tx boundary instead of the registry:
// a snapshot's chain fan-out can take several minutes (200+ providers,
// up to 10 retries each, 60s HTTP timeout). Wrapping that in a single
// pgx.Tx pinned a pool connection in `idle in transaction` for that
// whole window, starving the status handler, dead-letter sweep, and
// autovacuum. Pushing tx ownership into the concrete snapshotter lets
// it keep the fan-out outside any tx and open short tx(s) only for
// the actual DB writes.
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

	// Snapshot persists one snapshot. The snapshotter owns its own tx
	// boundary: chain fan-out happens outside any tx; short transactions
	// are opened only for the DB writes. Atomicity is preserved inside
	// those short tx(s); the function as a whole is not tx-atomic from
	// fetch to insert (a crash mid-fetch leaves no partial rows because
	// no rows have been written yet; a crash mid-write rolls back the
	// write tx).
	Snapshot(ctx context.Context, pool *pgxpool.Pool, target SnapshotTarget) error

	// Status returns the current coverage view for this snapshotter as
	// surfaced on /api/snapshotters and the dashboard card. Each
	// snapshotter owns its own query against its own schema — the web
	// layer only iterates and serialises. Kept in the base interface so
	// web can stay generic; implementations may return an empty Status
	// if they have no concept of expected/covered dates.
	Status(ctx context.Context, pool *pgxpool.Pool) (Status, error)
}

// Status is the generic coverage view returned by Snapshotter.Status.
// Rendered as /api/snapshotters entries and the dashboard timeline.
// Dates are ISO-formatted (YYYY-MM-DD); Failed carries per-date error
// strings for the UI's expandable errors block.
type Status struct {
	Expected []string
	Covered  []string
	Missing  []string
	Failed   []FailedDate
}

// FailedDate pairs a snapshot date with the error message recorded for
// it. Snapshotters that don't persist failed-status rows return an
// empty slice.
type FailedDate struct {
	Date  string
	Error string
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
