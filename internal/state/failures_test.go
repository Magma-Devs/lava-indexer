package state

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testPool dials the DB described by TEST_DATABASE_URL and returns a
// State scoped to a unique schema per-test so parallel tests don't
// collide and teardown is one DROP SCHEMA. Skips the test when no
// TEST_DATABASE_URL is set — these tests need real Postgres for the
// upsert/CASE logic.
func testPool(t *testing.T) (*State, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping state integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	schema := fmt.Sprintf("test_failures_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
		pool.Close()
	})
	st := New(pool, schema)
	if err := st.Ensure(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	return st, pool
}

func TestPopFailuresExcludesPermanent(t *testing.T) {
	st, _ := testPool(t)
	ctx := context.Background()

	// A mix of retrying + permanent rows. Only the two retrying ones
	// should come back, in height-desc order.
	if err := st.RecordFailure(ctx, "h1", 100, "transient", 0); err != nil {
		t.Fatalf("record retrying 100: %v", err)
	}
	if err := st.RecordFailure(ctx, "h1", 200, "transient", 0); err != nil {
		t.Fatalf("record retrying 200: %v", err)
	}
	if err := st.RecordPermanentFailure(ctx, "h1", 150, "pruned"); err != nil {
		t.Fatalf("record permanent 150: %v", err)
	}
	if err := st.RecordPermanentFailure(ctx, "h1", 250, "no endpoint covers"); err != nil {
		t.Fatalf("record permanent 250: %v", err)
	}

	got, err := st.PopFailures(ctx, "h1", 10)
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	// DELETE ... RETURNING doesn't guarantee row order; we only care
	// about the set-equality: retrying heights IN, permanent heights OUT.
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	want := []int64{100, 200}
	if len(got) != len(want) {
		t.Fatalf("pop got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pop[%d] = %d, want %d", i, got[i], want[i])
		}
	}

	// Permanent rows remain after the pop — PopFailures is supposed to
	// leave them as observability breadcrumbs.
	retrying, permanent, _, err := st.FailureCount(ctx, "h1")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if retrying != 0 {
		t.Fatalf("retrying count after pop = %d, want 0", retrying)
	}
	if permanent != 2 {
		t.Fatalf("permanent count after pop = %d, want 2", permanent)
	}
}

func TestRecordFailureTransitionsToPermanentAtCap(t *testing.T) {
	st, pool := testPool(t)
	ctx := context.Background()

	// Cap at 3: the third call should flip the row to permanent in the
	// same upsert. Before the third call, retries should be growing
	// without permanent=true.
	const cap = 3
	for i := 1; i <= cap-1; i++ {
		if err := st.RecordFailure(ctx, "h2", 42, fmt.Sprintf("try %d", i), cap); err != nil {
			t.Fatalf("record try %d: %v", i, err)
		}
		var retries int
		var permanent bool
		var reachedAt *time.Time
		q := fmt.Sprintf(`SELECT retries, permanent, max_retries_reached_at
		                  FROM %s.indexer_failures WHERE handler=$1 AND height=$2`, st.Schema())
		if err := pool.QueryRow(ctx, q, "h2", int64(42)).Scan(&retries, &permanent, &reachedAt); err != nil {
			t.Fatalf("select: %v", err)
		}
		if retries != i {
			t.Fatalf("after try %d: retries = %d, want %d", i, retries, i)
		}
		if permanent {
			t.Fatalf("after try %d: permanent unexpectedly true", i)
		}
		if reachedAt != nil {
			t.Fatalf("after try %d: max_retries_reached_at unexpectedly set", i)
		}
	}

	if err := st.RecordFailure(ctx, "h2", 42, "final", cap); err != nil {
		t.Fatalf("record final: %v", err)
	}
	var retries int
	var permanent bool
	var reachedAt *time.Time
	q := fmt.Sprintf(`SELECT retries, permanent, max_retries_reached_at
	                  FROM %s.indexer_failures WHERE handler=$1 AND height=$2`, st.Schema())
	if err := pool.QueryRow(ctx, q, "h2", int64(42)).Scan(&retries, &permanent, &reachedAt); err != nil {
		t.Fatalf("select final: %v", err)
	}
	if retries != cap {
		t.Fatalf("final retries = %d, want %d", retries, cap)
	}
	if !permanent {
		t.Fatalf("final permanent = false, want true at cap")
	}
	if reachedAt == nil {
		t.Fatalf("final max_retries_reached_at = nil, want timestamp")
	}

	// Subsequent PopFailures must not return this height.
	got, err := st.PopFailures(ctx, "h2", 10)
	if err != nil {
		t.Fatalf("pop after cap: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("pop after cap returned %v, want []", got)
	}
}

func TestRecordFailureNoCapNeverFlipsPermanent(t *testing.T) {
	st, pool := testPool(t)
	ctx := context.Background()

	// maxRetries=0 preserves the old behaviour — no cap, never flips.
	for i := 0; i < 10; i++ {
		if err := st.RecordFailure(ctx, "h3", 7, "nope", 0); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	var retries int
	var permanent bool
	q := fmt.Sprintf(`SELECT retries, permanent
	                  FROM %s.indexer_failures WHERE handler=$1 AND height=$2`, st.Schema())
	if err := pool.QueryRow(ctx, q, "h3", int64(7)).Scan(&retries, &permanent); err != nil {
		t.Fatalf("select: %v", err)
	}
	if retries != 10 {
		t.Fatalf("retries = %d, want 10", retries)
	}
	if permanent {
		t.Fatalf("permanent = true with no cap configured")
	}
}

func TestEnsureFailuresTableIsAdditiveMigration(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { pool.Close() })

	schema := fmt.Sprintf("test_migrate_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
	})

	// Simulate an existing deployment: create the old-shape table
	// (no permanent / max_retries_reached_at) with a row in it, then
	// let Ensure run the additive migration and verify the row survived
	// and the new columns are addressable.
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schema)); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.indexer_failures (
		  handler       TEXT        NOT NULL,
		  height        BIGINT      NOT NULL,
		  reason        TEXT        NOT NULL,
		  retries       INT         NOT NULL DEFAULT 0,
		  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		  last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
		  PRIMARY KEY (handler, height)
		)`, schema)); err != nil {
		t.Fatalf("create legacy table: %v", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s.indexer_failures (handler, height, reason, retries) VALUES ($1, $2, $3, $4)`, schema),
		"legacy", int64(999), "stale", 1); err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}

	st := New(pool, schema)
	if err := st.Ensure(ctx); err != nil {
		t.Fatalf("ensure (migration): %v", err)
	}

	// Legacy row still there, defaults on new columns.
	var retries int
	var permanent bool
	var reachedAt *time.Time
	q := fmt.Sprintf(`SELECT retries, permanent, max_retries_reached_at
	                  FROM %s.indexer_failures WHERE handler=$1 AND height=$2`, schema)
	if err := pool.QueryRow(ctx, q, "legacy", int64(999)).Scan(&retries, &permanent, &reachedAt); err != nil {
		t.Fatalf("select legacy: %v", err)
	}
	if retries != 1 {
		t.Fatalf("legacy row retries = %d, want 1", retries)
	}
	if permanent {
		t.Fatalf("legacy row permanent = true after migration, want default false")
	}
	if reachedAt != nil {
		t.Fatalf("legacy row max_retries_reached_at = %v, want nil", reachedAt)
	}
}

func TestRecordPermanentFailureFlagsOnInsertAndUpdate(t *testing.T) {
	st, pool := testPool(t)
	ctx := context.Background()

	// Insert-path.
	if err := st.RecordPermanentFailure(ctx, "h4", 11, "no endpoint covers heights 11-11"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var permanent bool
	var reachedAt *time.Time
	q := fmt.Sprintf(`SELECT permanent, max_retries_reached_at
	                  FROM %s.indexer_failures WHERE handler=$1 AND height=$2`, st.Schema())
	if err := pool.QueryRow(ctx, q, "h4", int64(11)).Scan(&permanent, &reachedAt); err != nil {
		t.Fatalf("select insert: %v", err)
	}
	if !permanent {
		t.Fatalf("permanent = false on fresh insert, want true")
	}
	if reachedAt == nil {
		t.Fatalf("max_retries_reached_at = nil on fresh insert, want timestamp")
	}

	// Update-path: an existing transient row gets upgraded in place.
	if err := st.RecordFailure(ctx, "h4", 22, "transient", 0); err != nil {
		t.Fatalf("seed transient: %v", err)
	}
	if err := st.RecordPermanentFailure(ctx, "h4", 22, "upgraded"); err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	if err := pool.QueryRow(ctx, q, "h4", int64(22)).Scan(&permanent, &reachedAt); err != nil {
		t.Fatalf("select upgrade: %v", err)
	}
	if !permanent {
		t.Fatalf("upgraded row permanent = false, want true")
	}
	if reachedAt == nil {
		t.Fatalf("upgraded row max_retries_reached_at = nil, want timestamp")
	}
}
