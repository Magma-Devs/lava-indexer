package events

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
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
// Each IDs call goes to Postgres — no in-memory cache. An earlier version
// cached ids in-process, but under parallel workers that produced FK
// violations: worker A would insert a row and cache its id before A's tx
// committed, worker B would grab the id from cache, then A would roll back
// and B's COPY into the fact table would reference a non-existent id. The
// round-trip is cheap (N=107 unique values across the entire chain's
// history for our workload) and the correctness win dwarfs the saving.
type Dict struct {
	Schema string // e.g. "app"
	Table  string // e.g. "providers"
	Column string // e.g. "addr"
}

func NewDict(schema, table, col string) *Dict {
	return &Dict{Schema: schema, Table: table, Column: col}
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

	// Step 1: INSERT anything new. ON CONFLICT DO NOTHING means a conflict
	// skips without grabbing the conflicting row's lock past the check.
	// Returned rows are the ones WE inserted.
	insSQL := fmt.Sprintf(`
		INSERT INTO %[1]s.%[2]s (%[3]s)
		SELECT DISTINCT v FROM unnest($1::text[]) AS t(v)
		ON CONFLICT (%[3]s) DO NOTHING
		RETURNING id, %[3]s`, d.Schema, d.Table, d.Column)
	insRows, err := tx.Query(ctx, insSQL, dedup)
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
	missing := dedup[:0]
	for _, v := range dedup {
		if _, ok := out[v]; !ok {
			missing = append(missing, v)
		}
	}
	if len(missing) == 0 {
		return out, nil
	}
	selSQL := fmt.Sprintf(
		`SELECT id, %[3]s FROM %[1]s.%[2]s WHERE %[3]s = ANY($1::text[])`,
		d.Schema, d.Table, d.Column)
	selRows, err := tx.Query(ctx, selSQL, missing)
	if err != nil {
		return nil, fmt.Errorf("%s.%s select-existing: %w", d.Schema, d.Table, err)
	}
	defer selRows.Close()
	for selRows.Next() {
		var id int32
		var v string
		if err := selRows.Scan(&id, &v); err != nil {
			return nil, err
		}
		out[v] = id
	}
	return out, selRows.Err()
}
