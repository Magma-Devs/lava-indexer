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

// IDs returns id for every value in `values`, inserting new ones via a
// single bulk UPSERT. Must be called with an active transaction so that
// dict writes commit (or roll back) atomically with the caller's batch.
func (d *Dict) IDs(ctx context.Context, tx pgx.Tx, values []string) (map[string]int32, error) {
	if len(values) == 0 {
		return nil, nil
	}

	// Dedup before round-tripping. unnest+DISTINCT would handle this
	// server-side, but trimming the arg array first keeps the wire smaller.
	seen := make(map[string]struct{}, len(values))
	dedup := make([]string, 0, len(values))
	for _, v := range values {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		dedup = append(dedup, v)
	}

	// ON CONFLICT DO UPDATE ... RETURNING returns the id for BOTH new and
	// pre-existing rows in one round-trip. The DO UPDATE is a no-op write
	// whose sole purpose is to trigger RETURNING; without it, ON CONFLICT
	// DO NOTHING would skip RETURNING for the conflict rows.
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

	out := make(map[string]int32, len(dedup))
	for rows.Next() {
		var id int32
		var v string
		if err := rows.Scan(&id, &v); err != nil {
			return nil, err
		}
		out[v] = id
	}
	return out, rows.Err()
}
