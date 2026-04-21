package snapshotters

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Registry holds the set of registered snapshotters and runs them on a
// timer. Mirrors events.Registry in shape, but stores its own run
// metadata (last/next tick) so the web UI can surface them.
type Registry struct {
	snapshotters []Snapshotter

	mu          sync.RWMutex
	lastRunAt   time.Time
	nextRunAt   time.Time
	lastTickErr error
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{}
}

// Register adds a snapshotter. Not safe to call concurrently with
// RunLoop — call at startup.
func (r *Registry) Register(s Snapshotter) {
	r.snapshotters = append(r.snapshotters, s)
}

// All returns the registered snapshotters in registration order. Useful
// for DDL application and the /api/snapshotters enumeration.
func (r *Registry) All() []Snapshotter {
	out := make([]Snapshotter, len(r.snapshotters))
	copy(out, r.snapshotters)
	return out
}

// LastRunAt returns the time the most-recent tick started. Zero until
// the first tick fires.
func (r *Registry) LastRunAt() time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastRunAt
}

// NextRunAt returns the scheduled time of the next tick. Zero when the
// registry hasn't been started yet.
func (r *Registry) NextRunAt() time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nextRunAt
}

// RunLoop runs each registered snapshotter's BlocksDue + Snapshot on
// interval (default 10m). It runs once immediately on startup — that's
// the backfill path, so a fresh deploy catches up without waiting a
// full interval for the first tick.
//
// Per tick: for each snapshotter, walk its BlocksDue list and run
// Snapshot inside a fresh pgx.Tx per target. A failure on one target
// logs and continues to the next — we never let one bad snapshot starve
// the others, and permanent failures are recorded into the
// snapshotter's own table (not here) so operators can see them.
//
// A small jittered sleep between targets keeps the chain RPC from
// bursting when many dates are due at once (fresh deploys after a long
// gap land here).
//
// RunLoop returns nil on ctx cancellation and never returns a non-nil
// error — all per-snapshotter errors are logged. This matches the
// errgroup.Go() usage in main.go: the pipeline's fatal path triggers
// ctx cancel, snapshotters shut down cleanly, nobody takes the blame.
func (r *Registry) RunLoop(ctx context.Context, pool *pgxpool.Pool, interval time.Duration) error {
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	if len(r.snapshotters) == 0 {
		slog.Info("snapshotter registry: no snapshotters registered, idle loop")
		<-ctx.Done()
		return nil
	}

	// Run once immediately so a fresh deploy catches up before the first
	// interval. The ticker fires again at t=interval, not t=0.
	r.runTick(ctx, pool)

	t := time.NewTicker(interval)
	defer t.Stop()
	r.setNextRunAt(time.Now().Add(interval))

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			r.runTick(ctx, pool)
			r.setNextRunAt(time.Now().Add(interval))
		}
	}
}

// runTick does one pass across every registered snapshotter.
func (r *Registry) runTick(ctx context.Context, pool *pgxpool.Pool) {
	now := time.Now()
	r.mu.Lock()
	r.lastRunAt = now
	r.lastTickErr = nil
	r.mu.Unlock()

	slog.Info("snapshotter tick", "snapshotters", len(r.snapshotters))

	for _, s := range r.snapshotters {
		if ctx.Err() != nil {
			return
		}
		r.runOne(ctx, pool, s)
	}
}

// runOne runs BlocksDue for a single snapshotter and then dispatches
// each returned target to its own pgx.Tx. Target failures don't abort
// the remaining targets — we log and continue so one bad snapshot can't
// starve the rest of the queue.
func (r *Registry) runOne(ctx context.Context, pool *pgxpool.Pool, s Snapshotter) {
	targets, err := s.BlocksDue(ctx, pool)
	if err != nil {
		slog.Error("snapshotter BlocksDue failed",
			"snapshotter", s.Name(), "err", err)
		return
	}
	if len(targets) == 0 {
		slog.Debug("snapshotter up to date", "snapshotter", s.Name())
		return
	}
	slog.Info("snapshotter targets due",
		"snapshotter", s.Name(), "count", len(targets))

	for i, tgt := range targets {
		if ctx.Err() != nil {
			return
		}
		// Small jittered sleep between targets so fresh-deploy catch-ups
		// don't burst the chain RPC. Skip on the first target so a
		// steady-state single-snapshot-per-tick call doesn't pay latency.
		if i > 0 {
			jitter := time.Duration(rand.Int63n(int64(2 * time.Second)))
			select {
			case <-ctx.Done():
				return
			case <-time.After(500*time.Millisecond + jitter):
			}
		}

		date := tgt.SnapshotDate.UTC().Format("2006-01-02")
		slog.Info("snapshot started",
			"snapshotter", s.Name(),
			"date", date,
			"block", tgt.BlockHeight)

		start := time.Now()
		err := pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			return s.Snapshot(ctx, tx, tgt)
		})
		dur := time.Since(start)
		if err != nil {
			// Context cancellation isn't an error worth reporting from
			// the snapshotter's side — the outer shutdown is the cause.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			slog.Error("snapshot failed",
				"snapshotter", s.Name(),
				"date", date,
				"block", tgt.BlockHeight,
				"duration", dur,
				"err", err)
			r.mu.Lock()
			r.lastTickErr = fmt.Errorf("%s date=%s block=%d: %w", s.Name(), date, tgt.BlockHeight, err)
			r.mu.Unlock()
			continue
		}
		slog.Info("snapshot ok",
			"snapshotter", s.Name(),
			"date", date,
			"block", tgt.BlockHeight,
			"duration", dur)
	}
}

func (r *Registry) setNextRunAt(t time.Time) {
	r.mu.Lock()
	r.nextRunAt = t
	r.mu.Unlock()
}
