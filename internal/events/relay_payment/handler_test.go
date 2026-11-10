package relay_payment

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/magma-devs/lava-indexer/internal/events"
	"github.com/magma-devs/lava-indexer/internal/rpc"
)

// testHandler dials TEST_DATABASE_URL and returns a Handler on a fresh
// schema with its DDL applied. Skips without TEST_DATABASE_URL.
func testHandler(t *testing.T) (*Handler, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping relay_payment integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	schema := fmt.Sprintf("test_relay_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
		pool.Close()
	})
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	h := New(schema)
	for _, ddl := range h.DDL() {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			t.Fatalf("ddl: %v", err)
		}
	}
	return h, pool
}

// paymentEvents builds one lava_relay_payment event per block, each carrying
// `providers` provider entries.
func paymentEvents(heights []int64, providers int) []events.HandledEvent {
	var out []events.HandledEvent
	for _, height := range heights {
		attrs := map[string]string{}
		for i := 0; i < providers; i++ {
			n := fmt.Sprint(i)
			attrs["provider."+n] = fmt.Sprintf("lava@provider%d", i)
			attrs["chainID."+n] = "ETH1"
			attrs["CU."+n] = "10"
			attrs["relayNumber."+n] = "3"
		}
		blk := &rpc.Block{Height: height, Time: time.Unix(1_700_000_000+height, 0).UTC()}
		out = append(out, events.HandledEvent{
			Block: blk,
			Event: rpc.Event{
				TxHash:   strings.Repeat(fmt.Sprintf("%02X", height%256), 32),
				Type:     EventType,
				Attrs:    attrs,
				EventIdx: 0,
			},
		})
	}
	return out
}

func persist(t *testing.T, ctx context.Context, h *Handler, pool *pgxpool.Pool, batches ...[]events.HandledEvent) {
	t.Helper()
	err := pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		for _, es := range batches {
			if err := h.Persist(ctx, tx, es); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("persist: %v", err)
	}
}

func countRows(t *testing.T, ctx context.Context, h *Handler, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s.relay_payments", h.schema)).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

// Persisting a block whose rows already exist is a no-op, not a duplicate-key
// error that aborts the writer's tx (MAG-5084).
func TestPersistIsIdempotent(t *testing.T) {
	h, pool := testHandler(t)
	ctx := context.Background()
	es := paymentEvents([]int64{100, 101}, 3)

	persist(t, ctx, h, pool, es)
	if got := countRows(t, ctx, h, pool); got != 6 {
		t.Fatalf("rows after first persist = %d, want 6", got)
	}
	persist(t, ctx, h, pool, es)
	if got := countRows(t, ctx, h, pool); got != 6 {
		t.Fatalf("rows after re-persist = %d, want 6", got)
	}
}

// A re-processed block that rides with a new block in the same tx must not
// stop the new block's rows from landing.
func TestPersistMixedExistingAndNewBlocks(t *testing.T) {
	h, pool := testHandler(t)
	ctx := context.Background()
	persist(t, ctx, h, pool, paymentEvents([]int64{100}, 2))

	persist(t, ctx, h, pool, paymentEvents([]int64{100, 200}, 2))
	if got := countRows(t, ctx, h, pool); got != 4 {
		t.Fatalf("rows = %d, want 4", got)
	}
}

// The writer aggregates several jobs into one tx, so Persist runs several
// times per tx. Each call must insert only its own rows and leave the stage
// empty for the next call.
func TestPersistSeveralCallsInOneTx(t *testing.T) {
	h, pool := testHandler(t)
	ctx := context.Background()
	err := pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		for _, es := range [][]events.HandledEvent{
			paymentEvents([]int64{300}, 2),
			paymentEvents([]int64{301}, 1),
			paymentEvents([]int64{300}, 2),
		} {
			if err := h.Persist(ctx, tx, es); err != nil {
				return err
			}
			var staged int
			if err := tx.QueryRow(ctx, "SELECT count(*) FROM "+stageTable).Scan(&staged); err != nil {
				return err
			}
			if staged != 0 {
				t.Errorf("%d rows left in %s after Persist", staged, stageTable)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("persist: %v", err)
	}
	if got := countRows(t, ctx, h, pool); got != 3 {
		t.Fatalf("rows = %d, want 3", got)
	}
}
