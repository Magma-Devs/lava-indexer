package snapshotters

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// fakeSnap is a Snapshotter that records what BlocksDue and Snapshot
// were called with, plus a pluggable set of targets and an optional
// per-target error.
type fakeSnap struct {
	name      string
	targets   []SnapshotTarget
	dueErr    error
	snapErr   map[int64]error // keyed on BlockHeight
	snapshots atomic.Int32
	dueCalls  atomic.Int32

	mu    sync.Mutex
	seen  []int64 // BlockHeights actually attempted
	onRun func()
}

func (f *fakeSnap) Name() string     { return f.name }
func (f *fakeSnap) DDL() []string    { return nil }

func (f *fakeSnap) BlocksDue(_ context.Context, _ *pgxpool.Pool) ([]SnapshotTarget, error) {
	f.dueCalls.Add(1)
	if f.onRun != nil {
		f.onRun()
	}
	return f.targets, f.dueErr
}

func (f *fakeSnap) Snapshot(_ context.Context, _ pgx.Tx, target SnapshotTarget) error {
	// In unit tests we never call this through the registry's real
	// pgx.Tx path — we only reach here if someone exercised Snapshot
	// directly. The registry path calls pgx.BeginTxFunc, which needs a
	// real pool. Registry-level tests use runOne via RunLoop and rely
	// on BlocksDue returning an empty list instead.
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seen = append(f.seen, target.BlockHeight)
	f.snapshots.Add(1)
	if err, ok := f.snapErr[target.BlockHeight]; ok {
		return err
	}
	return nil
}

// TestRegistry_TickRunsAllSnapshotters verifies that RunLoop calls
// BlocksDue on every registered snapshotter when the ticker fires.
// Uses a very short interval so the test is fast; relies on the
// "run-once-immediately" contract to observe the first tick.
func TestRegistry_TickRunsAllSnapshotters(t *testing.T) {
	a := &fakeSnap{name: "a"}
	b := &fakeSnap{name: "b"}
	r := NewRegistry()
	r.Register(a)
	r.Register(b)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		_ = r.RunLoop(ctx, nil, 50*time.Millisecond)
		close(done)
	}()
	<-done

	if got := a.dueCalls.Load(); got < 1 {
		t.Fatalf("a.BlocksDue calls = %d, want ≥ 1", got)
	}
	if got := b.dueCalls.Load(); got < 1 {
		t.Fatalf("b.BlocksDue calls = %d, want ≥ 1", got)
	}
}

// TestRegistry_EmptyRegistryIdles confirms RunLoop with no snapshotters
// doesn't panic and returns cleanly on ctx cancel.
func TestRegistry_EmptyRegistryIdles(t *testing.T) {
	r := NewRegistry()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := r.RunLoop(ctx, nil, 10*time.Millisecond); err != nil {
		t.Fatalf("RunLoop returned error on empty registry: %v", err)
	}
}

// TestRegistry_LastRunAtAdvances verifies the per-tick last_run_at
// bookkeeping surfaces in LastRunAt().
func TestRegistry_LastRunAtAdvances(t *testing.T) {
	a := &fakeSnap{name: "a"}
	r := NewRegistry()
	r.Register(a)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	before := time.Now().Add(-1 * time.Second)
	done := make(chan struct{})
	go func() {
		_ = r.RunLoop(ctx, nil, 50*time.Millisecond)
		close(done)
	}()
	<-done

	got := r.LastRunAt()
	if got.Before(before) {
		t.Fatalf("LastRunAt %s is before test start %s", got, before)
	}
}

// TestRegistry_OneFailureDoesNotStarveOthers verifies that a BlocksDue
// error on the first snapshotter doesn't prevent the second from running.
func TestRegistry_OneFailureDoesNotStarveOthers(t *testing.T) {
	a := &fakeSnap{name: "a", dueErr: errors.New("boom")}
	b := &fakeSnap{name: "b"}
	r := NewRegistry()
	r.Register(a)
	r.Register(b)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = r.RunLoop(ctx, nil, 40*time.Millisecond)
		close(done)
	}()
	<-done

	if a.dueCalls.Load() < 1 {
		t.Fatal("a never called")
	}
	if b.dueCalls.Load() < 1 {
		t.Fatal("b never called even though a's error should not starve it")
	}
}
