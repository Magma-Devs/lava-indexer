package state

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// schemaCacheTTL bounds how often SchemaInfo actually queries pg_catalog.
// The schema doesn't change inside a single runtime, so a short cache
// turns repeated UI polls (default 3 s tick × multiple browser tabs)
// into a single sub-millisecond map read.
const schemaCacheTTL = 30 * time.Second

type schemaCache struct {
	mu     sync.Mutex
	at     time.Time
	result []SchemaTable
}

// SchemaTable is a data-dictionary view of a single table in the indexer's
// schema: columns, primary key, indexes, plus storage stats pulled from
// pg_class / pg_stat_user_tables. Used by the web UI's Schema tab.
type SchemaTable struct {
	Name         string
	Columns      []SchemaColumn
	PrimaryKey   []string
	Indexes      []SchemaIndex
	RowsApprox   int64
	SizeBytes    int64
	LastAnalyzed *time.Time
}

// SchemaColumn describes one column on a table. DataType is the format_type
// rendering (e.g. "bigint", "timestamp with time zone") — matches what
// operators see in psql \d output.
type SchemaColumn struct {
	Name       string
	DataType   string
	Nullable   bool
	Default    string
	IsPrimary  bool
	References string // e.g. "providers(id)" for FKs; empty otherwise
}

// SchemaIndex is one index definition. Columns is the ordered list of
// indexed expressions (attribute names for plain b-tree indexes, raw
// pg_get_indexdef output for functional ones).
type SchemaIndex struct {
	Name      string
	Columns   []string
	Unique    bool
	SizeBytes int64
}

// SchemaInfo reads the indexer's schema metadata out of pg_catalog and
// returns one SchemaTable per `r`-relkind table in the schema, alphabetically
// sorted. Read-only — safe to call on a hot system.
//
// Result is cached for schemaCacheTTL (~30 s) because the underlying
// pg_get_indexdef + pg_total_relation_size calls aren't free (~40 ms of
// pg-side CPU per call once the schema grows past a few dozen tables) and
// the UI polls this on every dashboard tick. The schema doesn't mutate
// inside a runtime, so cache invalidation is purely time-based.
//
// pg_catalog queries use a bound `$1` for the schema name even though the
// rest of this package interpolates `s.schema` into table paths. The
// difference: a schema name used as a table-path prefix can't be a bind
// parameter (it's part of the SQL identifier), while a schema name used as
// a VALUE in a WHERE clause absolutely can — and should — be parameterised.
func (s *State) SchemaInfo(ctx context.Context) ([]SchemaTable, error) {
	s.schemaCache.mu.Lock()
	if time.Since(s.schemaCache.at) < schemaCacheTTL && s.schemaCache.result != nil {
		out := s.schemaCache.result
		s.schemaCache.mu.Unlock()
		return out, nil
	}
	s.schemaCache.mu.Unlock()

	// 1. Tables + sizes + row counts. One row per table.
	tables, order, err := s.schemaTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("tables: %w", err)
	}
	if len(order) == 0 {
		return nil, nil
	}

	// 2. Columns. Grouped by table name.
	cols, err := s.schemaColumns(ctx)
	if err != nil {
		return nil, fmt.Errorf("columns: %w", err)
	}

	// 3. Primary keys + indexes. PK attribute list pulled from the primary
	// index; other indexes appear under their own entry.
	pks, idxs, err := s.schemaIndexes(ctx)
	if err != nil {
		return nil, fmt.Errorf("indexes: %w", err)
	}

	// 4. Foreign keys. Best-effort; if pg_get_constraintdef returns something
	// we can't parse, we fall through with the raw def.
	fks, err := s.schemaForeignKeys(ctx)
	if err != nil {
		return nil, fmt.Errorf("foreign keys: %w", err)
	}

	out := make([]SchemaTable, 0, len(order))
	for _, name := range order {
		t := tables[name]
		t.Columns = cols[name]
		t.PrimaryKey = pks[name]
		t.Indexes = idxs[name]

		// Flag PK columns and attach FK references.
		pkSet := make(map[string]struct{}, len(t.PrimaryKey))
		for _, c := range t.PrimaryKey {
			pkSet[c] = struct{}{}
		}
		for i := range t.Columns {
			if _, ok := pkSet[t.Columns[i].Name]; ok {
				t.Columns[i].IsPrimary = true
			}
			if ref, ok := fks[fkKey{table: name, col: t.Columns[i].Name}]; ok {
				t.Columns[i].References = ref
			}
		}
		out = append(out, t)
	}
	s.schemaCache.mu.Lock()
	s.schemaCache.at = time.Now()
	s.schemaCache.result = out
	s.schemaCache.mu.Unlock()
	return out, nil
}

func (s *State) schemaTables(ctx context.Context) (map[string]SchemaTable, []string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT c.relname,
		       pg_total_relation_size(c.oid)::bigint,
		       COALESCE(st.n_live_tup, 0)::bigint,
		       st.last_analyze
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_stat_user_tables st ON st.relid = c.oid
		WHERE n.nspname = $1 AND c.relkind = 'r'
		ORDER BY c.relname`, s.schema)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	byName := make(map[string]SchemaTable)
	var order []string
	for rows.Next() {
		var t SchemaTable
		var last *time.Time
		if err := rows.Scan(&t.Name, &t.SizeBytes, &t.RowsApprox, &last); err != nil {
			return nil, nil, err
		}
		t.LastAnalyzed = last
		byName[t.Name] = t
		order = append(order, t.Name)
	}
	return byName, order, rows.Err()
}

func (s *State) schemaColumns(ctx context.Context) (map[string][]SchemaColumn, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT c.relname,
		       a.attname,
		       format_type(a.atttypid, a.atttypmod),
		       NOT a.attnotnull,
		       COALESCE(pg_get_expr(ad.adbin, ad.adrelid), '')
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
		WHERE n.nspname = $1 AND c.relkind = 'r'
		  AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY c.relname, a.attnum`, s.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string][]SchemaColumn)
	for rows.Next() {
		var table string
		var col SchemaColumn
		if err := rows.Scan(&table, &col.Name, &col.DataType, &col.Nullable, &col.Default); err != nil {
			return nil, err
		}
		out[table] = append(out[table], col)
	}
	return out, rows.Err()
}

func (s *State) schemaIndexes(ctx context.Context) (map[string][]string, map[string][]SchemaIndex, error) {
	// One row per index. indkey → array of attribute-name strings via
	// pg_get_indexdef(indexrelid, k+1, true), which honours expression
	// indexes the same way psql does.
	rows, err := s.pool.Query(ctx, `
		SELECT c.relname,
		       i.relname,
		       idx.indisprimary,
		       idx.indisunique,
		       pg_relation_size(i.oid)::bigint,
		       array(
		         SELECT pg_get_indexdef(idx.indexrelid, k + 1, true)
		         FROM generate_subscripts(idx.indkey, 1) AS k
		       )
		FROM pg_index idx
		JOIN pg_class c ON c.oid = idx.indrelid
		JOIN pg_class i ON i.oid = idx.indexrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relkind = 'r'
		ORDER BY c.relname, i.relname`, s.schema)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	pks := make(map[string][]string)
	idxs := make(map[string][]SchemaIndex)
	for rows.Next() {
		var table string
		var idx SchemaIndex
		var isPrimary bool
		if err := rows.Scan(&table, &idx.Name, &isPrimary, &idx.Unique, &idx.SizeBytes, &idx.Columns); err != nil {
			return nil, nil, err
		}
		if isPrimary {
			pks[table] = append([]string(nil), idx.Columns...)
		}
		idxs[table] = append(idxs[table], idx)
	}
	return pks, idxs, rows.Err()
}

// fkKey identifies a foreign-key-bearing column for the references lookup.
type fkKey struct {
	table string
	col   string
}

func (s *State) schemaForeignKeys(ctx context.Context) (map[fkKey]string, error) {
	// conkey is a smallint[] of the constrained columns (ordered). confrelid
	// is the referenced table's pg_class.oid; confkey is the matching list
	// of referenced columns. Unnest both arrays in parallel so multi-column
	// FKs resolve to one entry per local column.
	rows, err := s.pool.Query(ctx, `
		SELECT lrel.relname    AS local_table,
		       la.attname      AS local_col,
		       frel.relname    AS ref_table,
		       fa.attname      AS ref_col
		FROM pg_constraint ct
		JOIN pg_class lrel ON lrel.oid = ct.conrelid
		JOIN pg_class frel ON frel.oid = ct.confrelid
		JOIN pg_namespace n ON n.oid = ct.connamespace
		JOIN LATERAL unnest(ct.conkey, ct.confkey) WITH ORDINALITY AS k(lattnum, fattnum, ord) ON true
		JOIN pg_attribute la ON la.attrelid = ct.conrelid  AND la.attnum = k.lattnum
		JOIN pg_attribute fa ON fa.attrelid = ct.confrelid AND fa.attnum = k.fattnum
		WHERE n.nspname = $1 AND ct.contype = 'f'`, s.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[fkKey]string)
	for rows.Next() {
		var localTbl, localCol, refTbl, refCol string
		if err := rows.Scan(&localTbl, &localCol, &refTbl, &refCol); err != nil {
			return nil, err
		}
		out[fkKey{table: localTbl, col: localCol}] = fmt.Sprintf("%s(%s)", refTbl, refCol)
	}
	return out, rows.Err()
}
