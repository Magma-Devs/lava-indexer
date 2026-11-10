package state

import (
	"context"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSubtractHeights(t *testing.T) {
	cases := []struct {
		name    string
		rs      []Range
		heights []int64
		want    []Range
	}{
		{"no heights", []Range{{1, 10}}, nil, []Range{{1, 10}}},
		{"middle", []Range{{1, 10}}, []int64{5}, []Range{{1, 4}, {6, 10}}},
		{"edges", []Range{{1, 10}}, []int64{1, 10}, []Range{{2, 9}}},
		{"whole range", []Range{{5, 5}}, []int64{5}, []Range{}},
		{"adjacent heights", []Range{{1, 10}}, []int64{4, 5, 6}, []Range{{1, 3}, {7, 10}}},
		{"outside", []Range{{10, 20}}, []int64{1, 30}, []Range{{10, 20}}},
		{"across ranges", []Range{{1, 5}, {10, 15}}, []int64{3, 7, 10, 15}, []Range{{1, 2}, {4, 5}, {11, 14}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := subtractHeights(c.rs, c.heights)
			if len(got) == 0 && len(c.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("subtractHeights(%v, %v) = %v, want %v", c.rs, c.heights, got, c.want)
			}
		})
	}
}

func recordRange(t *testing.T, ctx context.Context, st *State, pool *pgxpool.Pool, handler string, from, to int64) {
	t.Helper()
	err := pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		return st.RecordRange(ctx, tx, handler, from, to)
	})
	if err != nil {
		t.Fatalf("record range %d-%d: %v", from, to, err)
	}
}

func coverage(t *testing.T, ctx context.Context, st *State, handler string) []Range {
	t.Helper()
	rs, err := st.Ranges(ctx, handler)
	if err != nil {
		t.Fatalf("ranges: %v", err)
	}
	return mergeRanges(rs)
}

// A range committed by a writer while the compactor is between its read and
// its swap must survive. Before MAG-5084 the swap deleted every row of the
// handler, so this range vanished while its event rows stayed committed.
func TestCompactRangesKeepsRangeCommittedDuringCompaction(t *testing.T) {
	st, pool := testPool(t)
	ctx := context.Background()
	recordRange(t, ctx, st, pool, "h1", 1, 10)
	recordRange(t, ctx, st, pool, "h1", 11, 20)

	merged, err := st.compactRanges(ctx, "h1", func() {
		recordRange(t, ctx, st, pool, "h1", 30, 40)
	})
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if merged != 1 {
		t.Fatalf("merged = %d, want 1", merged)
	}
	want := []Range{{1, 20}, {30, 40}}
	if got := coverage(t, ctx, st, "h1"); !reflect.DeepEqual(got, want) {
		t.Fatalf("coverage = %v, want %v", got, want)
	}
}

// A writer extending a row the compactor already read (RecordRange's ON
// CONFLICT … GREATEST) must not lose the extension.
func TestCompactRangesKeepsRangeExtendedDuringCompaction(t *testing.T) {
	st, pool := testPool(t)
	ctx := context.Background()
	recordRange(t, ctx, st, pool, "h1", 1, 10)
	recordRange(t, ctx, st, pool, "h1", 11, 20)

	if _, err := st.compactRanges(ctx, "h1", func() {
		recordRange(t, ctx, st, pool, "h1", 11, 25)
	}); err != nil {
		t.Fatalf("compact: %v", err)
	}
	want := []Range{{1, 25}}
	if got := coverage(t, ctx, st, "h1"); !reflect.DeepEqual(got, want) {
		t.Fatalf("coverage = %v, want %v", got, want)
	}

	// The next tick collapses whatever the race left behind.
	if _, err := st.CompactRanges(ctx, "h1"); err != nil {
		t.Fatalf("second compact: %v", err)
	}
	rs, err := st.Ranges(ctx, "h1")
	if err != nil {
		t.Fatalf("ranges: %v", err)
	}
	if !reflect.DeepEqual(rs, want) {
		t.Fatalf("rows after second compact = %v, want %v", rs, want)
	}
}

// Permanent dead-letters are not gaps for the producer; transient ones are.
func TestUnionGapsSkipsPermanentFailures(t *testing.T) {
	st, pool := testPool(t)
	ctx := context.Background()
	recordRange(t, ctx, st, pool, "h1", 1, 10)
	if err := st.RecordPermanentFailure(ctx, "h1", 12, "no endpoint covers"); err != nil {
		t.Fatalf("record permanent: %v", err)
	}
	if err := st.RecordFailure(ctx, "h1", 14, "transient", 0); err != nil {
		t.Fatalf("record transient: %v", err)
	}

	got, err := st.UnionGaps(ctx, []string{"h1"}, 1, 20)
	if err != nil {
		t.Fatalf("union gaps: %v", err)
	}
	want := []Range{{11, 11}, {13, 20}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("union gaps = %v, want %v", got, want)
	}

	// Gaps (the UI's view) still reports the permanent height as uncovered.
	gaps, err := st.Gaps(ctx, "h1", 1, 20)
	if err != nil {
		t.Fatalf("gaps: %v", err)
	}
	if want := []Range{{11, 20}}; !reflect.DeepEqual(gaps, want) {
		t.Fatalf("gaps = %v, want %v", gaps, want)
	}
}

// A permanent failure for one handler must not hide the height from another
// handler that still needs it.
func TestUnionGapsPermanentIsPerHandler(t *testing.T) {
	st, pool := testPool(t)
	ctx := context.Background()
	recordRange(t, ctx, st, pool, "h1", 1, 10)
	recordRange(t, ctx, st, pool, "h2", 1, 10)
	if err := st.RecordPermanentFailure(ctx, "h1", 11, "pruned"); err != nil {
		t.Fatalf("record permanent: %v", err)
	}

	got, err := st.UnionGaps(ctx, []string{"h1", "h2"}, 1, 11)
	if err != nil {
		t.Fatalf("union gaps: %v", err)
	}
	if want := []Range{{11, 11}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("union gaps = %v, want %v", got, want)
	}
}
