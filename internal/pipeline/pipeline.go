// Package pipeline orchestrates the height-dispatcher → fetchers → writer
// flow. It is range-aware: on startup it reads existing ranges from the
// state table and only fetches the gaps of the requested window. When
// end_height is 0 it runs forever, polling the tip and extending the
// covered range.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/magma-devs/lava-indexer/internal/config"
	"github.com/magma-devs/lava-indexer/internal/events"
	"github.com/magma-devs/lava-indexer/internal/rpc"
	"github.com/magma-devs/lava-indexer/internal/state"
	"golang.org/x/sync/errgroup"
)

type Pipeline struct {
	cfg      *config.Config
	pool     *pgxpool.Pool
	client   rpc.Client
	state    *state.State
	registry *events.Registry

	totalBlocks atomic.Int64
	totalRows   atomic.Int64
	rate        *RateTracker

	runID     int64     // 0 until Run() assigns one
	runStart  time.Time // wall-clock at StartRun

	sizer *AdaptiveSizer // nil when all knobs are explicit
}

// WithSizer wires an AdaptiveSizer so the pipeline consults it for every
// batch instead of using the static config value. Optional.
func (p *Pipeline) WithSizer(s *AdaptiveSizer) *Pipeline {
	p.sizer = s
	return p
}

// Stats is a snapshot of pipeline progress. Exposed for the /api/status
// endpoint — safe to call from any goroutine.
type Stats struct {
	TotalBlocks  int64     // blocks indexed since process start
	TotalRows    int64     // rows accumulated (events, not DB rows strictly)
	BlocksPerSec float64   // short-window rate
	RunID        int64     // DB id of the current run row
	RunStart     time.Time // wall-clock time this run started
}

func New(cfg *config.Config, pool *pgxpool.Pool, client rpc.Client, st *state.State, reg *events.Registry) *Pipeline {
	return &Pipeline{
		cfg: cfg, pool: pool, client: client, state: st, registry: reg,
		rate: NewRateTracker(60 * time.Second),
	}
}

// Stats returns a snapshot of the current indexing progress.
func (p *Pipeline) Stats() Stats {
	return Stats{
		TotalBlocks:  p.totalBlocks.Load(),
		TotalRows:    p.totalRows.Load(),
		BlocksPerSec: p.rate.Rate(),
		RunID:        p.runID,
		RunStart:     p.runStart,
	}
}

// RunID returns the current run's DB id (0 before Run starts). Exposed so
// main.go can call state.EndRun on shutdown.
func (p *Pipeline) RunID() int64 { return p.runID }

// Run executes the pipeline until the requested window is complete (when
// end_height > 0) or the context is cancelled (when end_height == 0,
// i.e. tip-following mode).
func (p *Pipeline) Run(ctx context.Context) error {
	cfg := p.cfg.Indexer

	// Open a fresh run row — atomic counters get written into it by every
	// batch commit, giving us persistent "blocks indexed this run" that
	// survives across restarts.
	runID, err := p.state.StartRun(ctx)
	if err != nil {
		return fmt.Errorf("start run: %w", err)
	}
	p.runID = runID
	p.runStart = time.Now()
	slog.Info("run started", "run_id", runID)

	// Initial tip — also defines the effective end when end_height == 0.
	tip, err := p.client.Tip(ctx)
	if err != nil {
		return fmt.Errorf("initial tip: %w", err)
	}
	end := cfg.EndHeight
	effectiveEnd := end
	if end == 0 {
		effectiveEnd = tip - cfg.TipConfirmations
	}
	if effectiveEnd < cfg.StartHeight {
		slog.Info("nothing to do yet", "start", cfg.StartHeight, "tip", tip, "confirmations", cfg.TipConfirmations)
	}

	// Seed: backfill any gaps in [start, effectiveEnd].
	if effectiveEnd >= cfg.StartHeight {
		if err := p.runWindow(ctx, cfg.StartHeight, effectiveEnd); err != nil {
			return err
		}
	}

	// Done if the user asked for a bounded window.
	if end > 0 {
		slog.Info("bounded window complete", "blocks_indexed", p.totalBlocks.Load(), "rows_written", p.totalRows.Load())
		return nil
	}

	// Tip-following loop: poll until cancelled.
	slog.Info("switching to tip-follower", "poll_interval", cfg.TipPollInterval)
	t := time.NewTicker(cfg.TipPollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			newTip, err := p.client.Tip(ctx)
			if err != nil {
				slog.Warn("tip poll failed", "err", err)
				continue
			}
			target := newTip - cfg.TipConfirmations
			if target < cfg.StartHeight {
				continue
			}
			// Drop down to the highest covered height+1 as the new start of
			// the gap search, or cfg.StartHeight if no coverage yet.
			if err := p.runWindow(ctx, cfg.StartHeight, target); err != nil {
				return err
			}
		}
	}
}

// runWindow fetches and writes only the heights in [start, end] NOT covered
// by at least one handler. The per-batch processing then picks which
// handlers still need it.
//
// Multiple gaps (e.g. one from an earlier start-height change + one from
// the current tail) are processed in PARALLEL, not sequentially: the
// single producer interleaves batches round-robin across every gap, so
// workers are never stuck waiting on gap A to finish before touching gap
// B. Also naturally balances load across endpoints when different gaps
// have different eligibility (e.g. old heights = archive only, recent =
// both).
func (p *Pipeline) runWindow(ctx context.Context, start, end int64) error {
	names := make([]string, 0, len(p.registry.All()))
	for _, h := range p.registry.All() {
		names = append(names, h.Name())
	}
	gaps, err := p.state.UnionGaps(ctx, names, start, end)
	if err != nil {
		return fmt.Errorf("compute gaps: %w", err)
	}
	if len(gaps) == 0 {
		slog.Info("window fully covered by prior runs, nothing to fetch", "start", start, "end", end)
		return nil
	}
	total := int64(0)
	for _, g := range gaps {
		total += g.To - g.From + 1
	}
	slog.Info("indexing window",
		"start", start, "end", end,
		"gap_blocks", total, "gap_count", len(gaps))

	return p.indexGaps(ctx, gaps)
}

// indexGaps fans out fetch work over N workers across ANY number of
// disjoint gaps in parallel. Batches from each gap are interleaved
// round-robin so workers never queue behind one gap's tail before
// touching another. Each worker still commits its own batch atomically
// (rows + per-handler range merge) in a single Postgres tx.
func (p *Pipeline) indexGaps(ctx context.Context, gaps []state.Range) error {
	cfg := p.cfg.Indexer
	batchOf := func() int64 {
		if p.sizer != nil {
			return int64(p.sizer.BatchSize())
		}
		return int64(cfg.FetchBatchSize)
	}

	type job struct {
		from, to int64
	}
	jobs := make(chan job, cfg.QueueDepth)

	g, ctx := errgroup.WithContext(ctx)

	// Single producer, tip-first: on every emit we pick the gap whose
	// current cursor is HIGHEST — i.e. closest to the chain tip — and
	// dispatch the next batch from there. Consequences:
	//   - With a historical-backfill gap (e.g. [6mo-ago, pruning-horizon])
	//     plus a live tip-extension gap, the live gap always wins. The
	//     backfill only gets worker attention once the tip gap is caught
	//     up, so the observable "latest indexed height" tracks the chain
	//     head even while history is still being filled.
	//   - When tip-follower extends the top gap on each poll, those new
	//     heights immediately jump to the front of the queue; they don't
	//     sit behind half a million backfill blocks.
	//   - Within a single gap the behaviour is unchanged: bottom-up walk
	//     from gap.From to gap.To.
	g.Go(func() error {
		defer close(jobs)
		cursors := make([]int64, len(gaps))
		for i := range gaps {
			cursors[i] = gaps[i].From
		}
		for {
			best := -1
			for i := range gaps {
				if cursors[i] > gaps[i].To {
					continue
				}
				if best < 0 || cursors[i] > cursors[best] {
					best = i
				}
			}
			if best < 0 {
				return nil
			}
			b := batchOf()
			to := cursors[best] + b - 1
			if to > gaps[best].To {
				to = gaps[best].To
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case jobs <- job{from: cursors[best], to: to}:
			}
			cursors[best] = to + 1
		}
	})

	// Consumers.
	start := time.Now()
	var progressBlocks atomic.Int64
	for i := 0; i < cfg.FetchWorkers; i++ {
		g.Go(func() error {
			for j := range jobs {
				if err := p.processWithSplit(ctx, j.from, j.to); err != nil {
					return fmt.Errorf("batch %d-%d: %w", j.from, j.to, err)
				}
				n := j.to - j.from + 1
				done := progressBlocks.Add(n)
				total := p.totalBlocks.Add(n)
				p.rate.Record(total)
				// Coarse progress log every ~1k blocks.
				if done%1000 < int64(batchOf()) {
					elapsed := time.Since(start).Seconds()
					bps := float64(done) / elapsed
					slog.Info("progress",
						"gaps", len(gaps),
						"blocks_done", done,
						"bps", fmt.Sprintf("%.1f", bps),
						"rows_total", p.totalRows.Load(),
					)
				}
			}
			return nil
		})
	}
	return g.Wait()
}

// processWithSplit runs processBatch. On a terminal failure it bisects the
// range and retries each half, continuing down to size 1. Any size-1 range
// that STILL fails is recorded in indexer_failures and skipped — its height
// stays absent from indexer_ranges so the next indexer run picks it up
// automatically. This turns a corrupt-block-on-one-node failure from a
// process-killing error into an observable hole in the data.
func (p *Pipeline) processWithSplit(ctx context.Context, from, to int64) error {
	err := p.processBatch(ctx, from, to)
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if from == to {
		// single block and still failing → dead-letter and skip.
		reason := truncate(err.Error(), 250)
		for _, h := range p.registry.All() {
			if rerr := p.state.RecordFailure(ctx, h.Name(), from, reason); rerr != nil {
				slog.Warn("could not record dead-letter", "height", from, "err", rerr)
			}
		}
		slog.Error("giving up on block after all retries, recorded in dead-letter",
			"height", from, "err", reason)
		return nil // swallow — pipeline continues
	}
	mid := from + (to-from)/2
	slog.Warn("batch failed, bisecting and retrying halves",
		"from", from, "to", to, "mid", mid, "err", err)
	if e1 := p.processWithSplit(ctx, from, mid); e1 != nil {
		return e1
	}
	return p.processWithSplit(ctx, mid+1, to)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func (p *Pipeline) processBatch(ctx context.Context, from, to int64) error {
	heights := make([]int64, 0, to-from+1)
	for h := from; h <= to; h++ {
		heights = append(heights, h)
	}

	// Fetch w/ retries for transient network errors.
	blocks, err := fetchWithRetry(ctx, p.client, heights, 4)
	if err != nil {
		return fmt.Errorf("fetch: %w", err)
	}

	dispatched := p.registry.Dispatch(blocks)
	// Count rows for progress log (only rough — counts events, not DB rows).
	for _, evs := range dispatched {
		p.totalRows.Add(int64(len(evs)))
	}

	// One tx per batch: all handler writes + per-handler range merges +
	// run-stats heartbeat commit together. If the process dies mid-batch,
	// neither the rows, the range update, NOR the counter advance lands,
	// so state stays consistent and the batch is re-fetched on next run.
	var rowsThisBatch int64
	for _, evs := range dispatched {
		rowsThisBatch += int64(len(evs))
	}
	blocksThisBatch := to - from + 1
	return pgx.BeginTxFunc(ctx, p.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		for _, handler := range p.registry.All() {
			needs, err := p.state.HandlerNeedsRange(ctx, handler.Name(), from, to)
			if err != nil {
				return err
			}
			if !needs {
				continue
			}
			if err := handler.Persist(ctx, tx, dispatched[handler]); err != nil {
				return fmt.Errorf("%s persist: %w", handler.Name(), err)
			}
			if err := p.state.RecordRange(ctx, tx, handler.Name(), from, to); err != nil {
				return fmt.Errorf("%s record range: %w", handler.Name(), err)
			}
		}
		if p.runID != 0 {
			if err := p.state.Heartbeat(ctx, tx, p.runID, blocksThisBatch, rowsThisBatch); err != nil {
				return fmt.Errorf("heartbeat: %w", err)
			}
		}
		return nil
	})
}

func fetchWithRetry(ctx context.Context, c rpc.Client, heights []int64, attempts int) ([]*rpc.Block, error) {
	var lastErr error
	backoff := 200 * time.Millisecond
	for i := 0; i < attempts; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
		}
		blocks, err := c.FetchBlocks(ctx, heights)
		if err == nil {
			return blocks, nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		lastErr = err
		slog.Warn("fetch failed, retrying", "attempt", i+1, "err", err)
	}
	return nil, lastErr
}
