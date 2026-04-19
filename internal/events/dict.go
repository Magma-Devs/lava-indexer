package events

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Dict is a string→int32 mapping backed by a Postgres table of the form
//
//	CREATE TABLE <table> (
//	  id    INT  GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
//	  <col> TEXT NOT NULL UNIQUE
//	);
//
// It lets high-cardinality repeated TEXT values (provider addresses, chain
// names) be stored as 4-byte INT references in fact tables, saving ~25–30%
// of disk over the naive design for our row shape.
//
// Cache discipline (commit-only): the in-memory cache is populated ONLY
// from values observed as already-committed in the database — either via
// startup Warmup or via the SELECT fallback inside IDs. Values returned
// from INSERT…RETURNING (i.e. inserted by the *current* uncommitted tx)
// are NEVER cached, because a rollback would leave the cache holding an
// id for a row that doesn't exist — exactly the FK-race that bit an
// earlier always-cache implementation. New values pay one extra
// round-trip on the *next* batch that needs them; from then on they're
// cached. With a chain-cardinality of ~100 providers/chains and dozens
// of batches/sec, steady-state hit rate is ~99% and IDs is a no-op for
// the hot path.
type Dict struct {
	Schema string // e.g. "app"
	Table  string // e.g. "providers"
	Column string // e.g. "addr"

	mu    sync.RWMutex
	cache map[string]int32
}

func NewDict(schema, table, col string) *Dict {
	return &Dict{Schema: schema, Table: table, Column: col, cache: map[string]int32{}}
}

// Warmup loads every committed (id, value) pair from the dict table into
// the in-memory cache. Intended to run once at startup, after the table's
// DDL has been applied and before the first IDs call. Subsequent IDs
// calls then short-circuit on cache hits instead of round-tripping to
// Postgres for every dictionary lookup.
func (d *Dict) Warmup(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx,
		fmt.Sprintf(`SELECT id, %[3]s FROM %[1]s.%[2]s`, d.Schema, d.Table, d.Column))
	if err != nil {
		return fmt.Errorf("%s.%s warmup: %w", d.Schema, d.Table, err)
	}
	defer rows.Close()
	loaded := make(map[string]int32, 256)
	for rows.Next() {
		var id int32
		var v string
		if err := rows.Scan(&id, &v); err != nil {
			return err
		}
		loaded[v] = id
	}
	if err := rows.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	for k, v := range loaded {
		d.cache[k] = v
	}
	d.mu.Unlock()
	return nil
}

func (d *Dict) DDL() string {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %[1]s.%[2]s (
		  id     INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		  %[3]s  TEXT NOT NULL UNIQUE
		);`, d.Schema, d.Table, d.Column)
}

// IDs returns id for every value in `values`, inserting new ones as
// needed. Must be called with an active transaction so that dict writes
// commit (or roll back) atomically with the caller's batch.
//
// Implemented as two queries (INSERT … DO NOTHING, then SELECT for the
// conflicting rows) rather than one INSERT … DO UPDATE … RETURNING. The
// one-round-trip form sounds nicer, but DO UPDATE acquires a row-level
// exclusive lock on every conflicting row and holds it until the caller's
// tx commits. Under N parallel workers each inserting the same ~60
// dictionary rows in a batch tx that also contains a heavy COPY, every
// worker serialised on those locks for the full batch duration — ~30 s
// under fresh-backfill load, pushing pg_stat_activity to 100% active and
// starving any other query (the /api/status handler, the dead-letter
// sweep) of a pool connection.
//
// DO NOTHING takes the lock only for the brief conflict check and
// releases on the spot; we then fetch the existing ids with a plain
// read. MVCC means concurrent tx's inserts we couldn't see still surface
// via the fallback SELECT once they commit — i.e. correctness is
// preserved while the lock contention is eliminated.
//
// Cache: cache hits short-circuit before any DB call. Misses go through
// the INSERT/SELECT path; only SELECT-returning rows (already-committed
// values from other tx's) are added back to the cache. INSERT-returning
// rows belong to *this* uncommitted tx, so caching them risks the
// rollback-FK race that the all-tx-cache version of this code suffered
// from.
func (d *Dict) IDs(ctx context.Context, tx pgx.Tx, values []string) (map[string]int32, error) {
	if len(values) == 0 {
		return nil, nil
	}

	// Dedup before round-tripping. unnest+DISTINCT would dedup server-side
	// too, but trimming the arg array first keeps the wire smaller and
	// makes the fallback `missing` calculation straightforward.
	seen := make(map[string]struct{}, len(values))
	dedup := make([]string, 0, len(values))
	for _, v := range values {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		dedup = append(dedup, v)
	}

	out := make(map[string]int32, len(dedup))

	// Cache lookup: anything already cached is a committed id from a
	// previous tx (Warmup or SELECT-fallback) and safe to use.
	misses := dedup[:0:0]
	d.mu.RLock()
	for _, v := range dedup {
		if id, ok := d.cache[v]; ok {
			out[v] = id
		} else {
			misses = append(misses, v)
		}
	}
	d.mu.RUnlock()
	if len(misses) == 0 {
		return out, nil
	}

	// Step 1: INSERT anything new. ON CONFLICT DO NOTHING means a conflict
	// skips without grabbing the conflicting row's lock past the check.
	// Returned rows are the ones WE inserted — NOT cached, since this tx
	// may roll back.
	insSQL := fmt.Sprintf(`
		INSERT INTO %[1]s.%[2]s (%[3]s)
		SELECT DISTINCT v FROM unnest($1::text[]) AS t(v)
		ON CONFLICT (%[3]s) DO NOTHING
		RETURNING id, %[3]s`, d.Schema, d.Table, d.Column)
	insRows, err := tx.Query(ctx, insSQL, misses)
	if err != nil {
		return nil, fmt.Errorf("%s.%s insert: %w", d.Schema, d.Table, err)
	}
	for insRows.Next() {
		var id int32
		var v string
		if err := insRows.Scan(&id, &v); err != nil {
			insRows.Close()
			return nil, err
		}
		out[v] = id
	}
	insRows.Close()
	if err := insRows.Err(); err != nil {
		return nil, err
	}

	// Step 2: anything still missing was skipped by DO NOTHING — either
	// because it already existed (common) or because a concurrent tx just
	// committed its insert (rare, benign). Fetch their ids with a plain
	// read; PG's unique-index conflict resolution guarantees the committed
	// row is visible to this transaction by the time Step 1 returned.
	stillMissing := misses[:0]
	for _, v := range misses {
		if _, ok := out[v]; !ok {
			stillMissing = append(stillMissing, v)
		}
	}
	if len(stillMissing) == 0 {
		return out, nil
	}
	selSQL := fmt.Sprintf(
		`SELECT id, %[3]s FROM %[1]s.%[2]s WHERE %[3]s = ANY($1::text[])`,
		d.Schema, d.Table, d.Column)
	selRows, err := tx.Query(ctx, selSQL, stillMissing)
	if err != nil {
		return nil, fmt.Errorf("%s.%s select-existing: %w", d.Schema, d.Table, err)
	}
	defer selRows.Close()
	committed := make(map[string]int32, len(stillMissing))
	for selRows.Next() {
		var id int32
		var v string
		if err := selRows.Scan(&id, &v); err != nil {
			return nil, err
		}
		out[v] = id
		committed[v] = id
	}
	if err := selRows.Err(); err != nil {
		return nil, err
	}
	// Step-2 hits are already-committed rows from other tx's — safe to
	// cache. Step-1 (INSERT-returning) results are NOT cached.
	if len(committed) > 0 {
		d.mu.Lock()
		for k, v := range committed {
			d.cache[k] = v
		}
		d.mu.Unlock()
	}
	return out, nil
}
