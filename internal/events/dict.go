package events

import (
	"context"
	"fmt"
	"sync"

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
// The cache is populated lazily from successful UPSERTs and lives for the
// lifetime of the process. It is safe for concurrent use.
type Dict struct {
	Schema string // e.g. "app"
	Table  string // e.g. "providers"
	Column string // e.g. "addr"

	mu    sync.RWMutex
	cache map[string]int32
}

func NewDict(schema, table, col string) *Dict {
	return &Dict{Schema: schema, Table: table, Column: col, cache: make(map[string]int32)}
}

func (d *Dict) DDL() string {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %[1]s.%[2]s (
		  id     INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		  %[3]s  TEXT NOT NULL UNIQUE
		);`, d.Schema, d.Table, d.Column)
}

// IDs returns id for every value in `values`, inserting new ones via a single
// bulk UPSERT. Must be called with an active transaction — the writes commit
// atomically with the caller's batch.
func (d *Dict) IDs(ctx context.Context, tx pgx.Tx, values []string) (map[string]int32, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make(map[string]int32, len(values))
	var missing []string

	// Fast path: read-lock the cache for hits.
	d.mu.RLock()
	for _, v := range values {
		if id, ok := d.cache[v]; ok {
			out[v] = id
		} else {
			missing = append(missing, v)
		}
	}
	d.mu.RUnlock()

	if len(missing) == 0 {
		return out, nil
	}

	// Dedup missing before round-tripping.
	seen := make(map[string]struct{}, len(missing))
	dedup := missing[:0]
	for _, v := range missing {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		dedup = append(dedup, v)
	}

	// ON CONFLICT DO UPDATE ... RETURNING is the trick to get the id back for
	// both newly-inserted and already-existing rows in one round-trip.
	sql := fmt.Sprintf(`
		INSERT INTO %[1]s.%[2]s (%[3]s)
		SELECT DISTINCT v FROM unnest($1::text[]) AS t(v)
		ON CONFLICT (%[3]s) DO UPDATE SET %[3]s = EXCLUDED.%[3]s
		RETURNING id, %[3]s`, d.Schema, d.Table, d.Column)
	rows, err := tx.Query(ctx, sql, dedup)
	if err != nil {
		return nil, fmt.Errorf("%s.%s upsert: %w", d.Schema, d.Table, err)
	}
	defer rows.Close()

	d.mu.Lock()
	for rows.Next() {
		var id int32
		var v string
		if err := rows.Scan(&id, &v); err != nil {
			d.mu.Unlock()
			return nil, err
		}
		d.cache[v] = id
		out[v] = id
	}
	d.mu.Unlock()
	return out, rows.Err()
}
