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
	"sort"
	"sync"
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

	// Resolved runtime knobs. main.go computes these (mixing operator
	// values, adaptive sizer output, and per-endpoint probe results) and
	// passes them in via WithRuntimeKnobs so the pipeline never reaches
	// back into cfg for them. Treating *config.Config as input-only after
	// Load means anyone reading the config (logs, /api/status, future
	// "show effective config" handler) sees what the operator actually
	// typed, not main's resolution.
	workers  int
	batchMax int

	totalBlocks atomic.Int64
	totalRows   atomic.Int64
	rate        *RateTracker

	runID    int64     // 0 until Run() assigns one
	runStart time.Time // wall-clock at StartRun

	sizer *AdaptiveSizer // nil when all knobs are explicit
}

// WithSizer wires an AdaptiveSizer so the pipeline consults it for every
// batch instead of using the static config value. Optional.
func (p *Pipeline) WithSizer(s *AdaptiveSizer) *Pipeline {
	p.sizer = s
	return p
}

// WithRuntimeKnobs overrides the worker and batch-size values the
// pipeline uses at run time. Set by main.go after the adaptive sizer
// resolves them, so the pipeline doesn't have to re-discover them and
// cfg.Indexer keeps reflecting what the operator actually configured.
func (p *Pipeline) WithRuntimeKnobs(workers, batchMax int) *Pipeline {
	if workers > 0 {
		p.workers = workers
	}
	if batchMax > 0 {
		p.batchMax = batchMax
	}
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
		// Default to operator-supplied values. main.go overrides via
		// WithRuntimeKnobs once it's resolved adaptive ones.
		workers:  cfg.Indexer.FetchWorkers,
		batchMax: cfg.Indexer.FetchBatchSize,
		rate:     NewRateTracker(60 * time.Second),
	}
}

// Workers returns the resolved fetch-worker count.
func (p *Pipeline) Workers() int { return p.workers }

// BatchMax returns the resolved per-fetch batch-size cap.
func (p *Pipeline) BatchMax() int { return p.batchMax }

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
	// Slice any gap that crosses an endpoint's Earliest horizon into
	// sub-gaps — otherwise the producer walks a single giant gap bottom-up
	// and the tier's only eligible endpoint (usually the archive) does all
	// the work while pruning endpoints sit idle until the cursor crosses
	// their horizon. Splitting lets indexGaps' interleave producer dispatch
	// across tiers in parallel from block zero of the backfill.
	if multi, ok := p.client.(*rpc.MultiClient); ok {
		gaps = splitByEndpointCoverage(gaps, multi.Endpoints())
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

// splitByEndpointCoverage slices every gap at each endpoint's Earliest
// boundary so gaps above a pruning endpoint's horizon become distinct
// work units. The result preserves order, non-overlap, and total covered
// heights; only the gap count changes. Endpoints whose Earliest is 0 or
// which are Disabled do not contribute boundaries.
func splitByEndpointCoverage(gaps []state.Range, eps []*rpc.Endpoint) []state.Range {
	horizons := map[int64]struct{}{}
	for _, ep := range eps {
		if ep.Disabled || ep.Earliest <= 1 {
			continue
		}
		horizons[ep.Earliest] = struct{}{}
	}
	if len(horizons) == 0 {
		return gaps
	}
	sorted := make([]int64, 0, len(horizons))
	for h := range horizons {
		sorted = append(sorted, h)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	out := make([]state.Range, 0, len(gaps)+len(sorted))
	for _, g := range gaps {
		cursor := g.From
		for _, h := range sorted {
			if h <= cursor || h > g.To {
				continue
			}
			out = append(out, state.Range{From: cursor, To: h - 1})
			cursor = h
		}
		out = append(out, state.Range{From: cursor, To: g.To})
	}
	return out
}

// indexGaps fans out fetch+persist work over two decoupled worker pools:
//
//   - FETCH pool (N = p.workers): pulls jobs off the producer channel, calls
//     MultiClient.FetchBlocks, parses events, bisects on fetch errors. On
//     success it hands the parsed batch to the writer pool via writeCh and
//     moves on — no DB work is held by a fetcher.
//   - WRITE pool (M = writerCount, bounded by pg pool): pulls writeJobs off
//     writeCh, opens a single tx per batch, persists each handler's events,
//     merges indexer_ranges, heartbeats run stats, commits.
//
// Why the split: previously each worker fetched AND persisted in one go,
// so a slow persist blocked the fetch it was paired with. Under heavy
// write load, fetchers sat idle behind COPYs. Splitting lets fetch run
// at the upstream ceiling while write runs at the DB ceiling — whichever
// is slower becomes the bottleneck, and neither one gates the other.
//
// Bisect policy: stays in the fetcher (unchanged). Persist errors are NOT
// bisected — they're recorded as failures for the dead-letter sweep,
// because persist errors are rarely "this specific block is poison" (FKs
// are dict races, pool exhaustion is a pool problem, etc.).
func (p *Pipeline) indexGaps(ctx context.Context, gaps []state.Range) error {
	cfg := p.cfg.Indexer
	batchOf := func() int64 {
		// Static batch size wins. The sizer may still exist (because the
		// operator left fetch_workers adaptive) but its batch state is
		// only consulted when the operator explicitly chose adaptive for
		// batch too — fetch_batch_size <= 0 in config.
		if cfg.FetchBatchSize > 0 {
			return int64(cfg.FetchBatchSize)
		}
		if p.sizer != nil {
			return int64(p.sizer.BatchSize())
		}
		return int64(p.batchMax)
	}

	type job struct {
		from, to int64
	}
	jobs := make(chan job, cfg.QueueDepth)

	// Writer pool size: pool_size - 4 so we reserve a handful of
	// connections for the status handler, the dead-letter sweep's pop
	// query, HandlerNeedsRange in fetchers, and the pg_cron heartbeat —
	// but hand the rest to writers since the DB write path is usually
	// the bottleneck on a large-table backfill. Minimum 8 even on small
	// pools. Previously pool_size/2, which left half the pool idle
	// while writers stacked up on writeCh send.
	writerCount := p.cfg.Database.PoolSize - 4
	if writerCount < 8 {
		writerCount = 8
	}
	// Small buffer for rate smoothing without unbounded memory. Each
	// writeJob holds the parsed events for its batch.
	writeCh := make(chan writeJob, 2*writerCount)

	g, ctx := errgroup.WithContext(ctx)

	// Single producer, tip-first with interleave: most batches go to the
	// gap with the HIGHEST cursor (closest to tip), but every Nth batch
	// goes to the LOWEST-cursor gap instead, so a big historical gap
	// can't starve behind thousands of tip-close fragments.
	//
	// Why not pure tip-first: if gaps are highly fragmented near tip
	// (dead-letter holes × thousands) the tip-first preference keeps
	// rotating through those fragments for hours while a single large
	// bottom gap gets zero cycles. Interleave every N batches lets the
	// bottom gap drain in parallel at ~(1/N) of the throughput without
	// losing the "tip tracks head" property.
	//
	// BackfillInterleave: 0 or 1 → pure tip-first. Default 5 → ~80/20.
	// Within a single gap the walk is still bottom-up From → To.
	g.Go(func() error {
		defer close(jobs)
		cursors := make([]int64, len(gaps))
		for i := range gaps {
			cursors[i] = gaps[i].From
		}
		interleave := cfg.BackfillInterleave
		batchIdx := 0
		for {
			// Every `interleave`-th batch, pick the LOWEST-cursor gap
			// (oldest work) instead of the highest.
			pickLowest := interleave > 1 && (batchIdx%interleave) == interleave-1
			best := -1
			for i := range gaps {
				if cursors[i] > gaps[i].To {
					continue
				}
				if best < 0 {
					best = i
					continue
				}
				if pickLowest {
					if cursors[i] < cursors[best] {
						best = i
					}
				} else {
					if cursors[i] > cursors[best] {
						best = i
					}
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
			batchIdx++
		}
	})

	// Dead-letter retry sweep: the pipeline's processWithSplit records any
	// size-1 block that exhausts its retries into app.indexer_failures and
	// moves on. Historically those heights stayed "failed" until the process
	// restarted — no good when a multi-day backfill briefly saturates the
	// endpoints and racks up thousands of transient holes.
	//
	// This goroutine periodically pops the newest-first batch of failures,
	// deletes their rows, and runs them back through processWithSplit. If
	// the retry succeeds the height lands in indexer_ranges (so it stops
	// being a gap); if it fails again, processWithSplit re-inserts the
	// row with retries+1 and it gets another chance on the next sweep.
	//
	// Runs in parallel with the main tip-first producer — uses the same
	// pgxpool + processWithSplit, so it shares the AIMD budget and advisory
	// locks naturally. No separate worker pool needed; dead-letter retry
	// is rarely the bottleneck.
	if cfg.FailureRetryInterval > 0 && cfg.FailureRetryBatch > 0 {
		g.Go(func() error {
			ticker := time.NewTicker(cfg.FailureRetryInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-ticker.C:
				}
				// Cap each sweep at one FailureRetryInterval so the next
				// tick can't fire on top of an unfinished sweep. Without
				// this, FailureRetryBatch=200 × ~500 ms/retry = 100 s of
				// work would overlap a 60 s tick — sweeps stack, share
				// the per-handler advisory lock with the producer, and
				// degrade the entire indexer's commit loop. Heights left
				// over from this tick stay in indexer_failures and the
				// next tick picks them up.
				sweepDeadline := time.Now().Add(cfg.FailureRetryInterval)
				overran := false
				for _, handler := range p.registry.All() {
					if overran {
						break
					}
					heights, err := p.state.PopFailures(ctx, handler.Name(), cfg.FailureRetryBatch)
					if err != nil {
						slog.Warn("dead-letter sweep: pop failed", "handler", handler.Name(), "err", err)
						continue
					}
					if len(heights) == 0 {
						continue
					}
					slog.Info("dead-letter sweep: retrying heights",
						"handler", handler.Name(), "count", len(heights))
					for i, h := range heights {
						if ctx.Err() != nil {
							return nil
						}
						if time.Now().After(sweepDeadline) {
							slog.Warn("dead-letter sweep: time budget exhausted; deferring remainder",
								"handler", handler.Name(),
								"completed", i, "remaining", len(heights)-i)
							overran = true
							break
						}
						// Swallow errors: processWithSplit only returns ctx
						// errors; real failures re-insert into indexer_failures.
						_ = p.processWithSplit(ctx, writeCh, h, h)
					}
				}
			}
		})
	}

	// Fetchers. Track completion with a WaitGroup so we can close writeCh
	// after the last fetcher exits — writers drain, then exit.
	var fetchWg sync.WaitGroup
	for i := 0; i < p.workers; i++ {
		fetchWg.Add(1)
		g.Go(func() error {
			defer fetchWg.Done()
			for j := range jobs {
				if err := p.processWithSplit(ctx, writeCh, j.from, j.to); err != nil {
					return fmt.Errorf("batch %d-%d: %w", j.from, j.to, err)
				}
			}
			return nil
		})
	}
	g.Go(func() error {
		fetchWg.Wait()
		close(writeCh)
		return nil
	})

	// Writers. Each drains up to writeAggregateMax writeJobs (or waits
	// writeAggregateWait) before opening a tx, and commits the whole
	// batch as one — amortising per-tx overhead (BEGIN, advisory no-op,
	// Heartbeat, COMMIT) across many batches. Progress log records
	// COMMITTED blocks (honest rate reporting).
	start := time.Now()
	var progressBlocks atomic.Int64
	const (
		writeAggregateMax  = 32                    // max writeJobs per tx
		writeAggregateWait = 50 * time.Millisecond // wait for more jobs to arrive before flushing
	)
	for i := 0; i < writerCount; i++ {
		g.Go(func() error {
			for {
				// Block on first job.
				w, ok := <-writeCh
				if !ok {
					return nil
				}
				batch := []writeJob{w}
				// Drain more without blocking indefinitely — small time
				// budget lets sparse arrivals through while dense bursts
				// aggregate.
				timer := time.NewTimer(writeAggregateWait)
			drain:
				for len(batch) < writeAggregateMax {
					select {
					case w, ok := <-writeCh:
						if !ok {
							// channel closed — flush what we have and exit after
							timer.Stop()
							break drain
						}
						batch = append(batch, w)
					case <-timer.C:
						break drain
					}
				}
				timer.Stop()

				if err := p.persistJobs(ctx, batch); err != nil {
					// Whole-tx failure → record every job's range as dead-letter.
					reason := safeFailureReason(err)
					slog.Error("persist failed; batch recorded as dead-letter",
						"count", len(batch), "err", reason)
					for _, j := range batch {
						p.recordBatchFailure(ctx, j.from, j.to, reason)
					}
					continue
				}
				// Success: update progress counters summed across the batch.
				var n int64
				for _, j := range batch {
					n += j.blocksThisBatch
				}
				done := progressBlocks.Add(n)
				total := p.totalBlocks.Add(n)
				p.rate.Record(total)
				if done%1000 < int64(batchOf()) {
					elapsed := time.Since(start).Seconds()
					bps := float64(done) / elapsed
					slog.Info("progress",
						"gaps", len(gaps),
						"blocks_done", done,
						"bps", fmt.Sprintf("%.1f", bps),
						"rows_total", p.totalRows.Load(),
						"agg", len(batch),
					)
				}
			}
		})
	}

	// Range compactor: with the advisory lock gone, writers INSERT raw
	// ranges and fragmentation accumulates. A periodic tick collapses
	// touching ranges per handler via range_agg so indexer_ranges stays
	// tight and HandlerNeedsRange's bounded-overlap scan stays fast.
	g.Go(func() error {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
			for _, h := range p.registry.All() {
				reduced, err := p.state.CompactRanges(ctx, h.Name())
				if err != nil {
					slog.Warn("range compactor failed", "handler", h.Name(), "err", err)
					continue
				}
				if reduced > 0 {
					slog.Info("range compactor", "handler", h.Name(), "rows_merged", reduced)
				}
			}
		}
	})

	return g.Wait()
}

// writeJob is the handoff between fetch workers and writer workers. Holds
// the parsed events plus enough context for the writer to decide which
// handlers still need this batch and to update indexer_ranges atomically.
type writeJob struct {
	from, to        int64
	dispatched      map[events.Handler][]events.HandledEvent
	needs           map[string]bool // handler.Name() → still-needed
	rowsThisBatch   int64
	blocksThisBatch int64
}

// processWithSplit fetches [from, to] and hands the parsed events off to
// the writer pool via writeCh. On a transient fetch failure it bisects
// and recurses; on a permanent fetch failure it records every height as
// permanent in one shot (no bisect, no wait); on the size-1 still-failing
// case it records a dead-letter for the sweep to retry.
//
// Returns nil on success and on recorded-failure (pipeline keeps moving).
// Only returns a non-nil error on ctx cancellation.
//
// Persist errors are NOT handled here — they surface in the writer, which
// records the batch as failure via recordBatchFailure.
func (p *Pipeline) processWithSplit(ctx context.Context, writeCh chan<- writeJob, from, to int64) error {
	wj, err := p.fetchBatch(ctx, from, to)
	if err == nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case writeCh <- wj:
		}
		return nil
	}
	// A per-request HTTP timeout (Client.Timeout firing on a slow upstream)
	// wraps context.Canceled too, so errors.Is(err, context.Canceled) returns
	// true for BOTH "we were shut down" and "one request took too long". The
	// distinguishing signal is the OUTER ctx — if our own context is still
	// live, the error is a per-request timeout and should fall through to
	// bisect/dead-letter like any other transient fetch failure. Only when
	// our own ctx is cancelled do we propagate, which is the clean-shutdown
	// path.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if isPermanentFetchError(err) {
		reason := safeFailureReason(err)
		for h := from; h <= to; h++ {
			for _, handler := range p.registry.All() {
				if rerr := p.state.RecordPermanentFailure(ctx, handler.Name(), h, reason); rerr != nil {
					slog.Warn("could not record permanent dead-letter", "height", h, "err", rerr)
				}
			}
		}
		slog.Error("permanent fetch failure, skipping bisect and recording range as dead-letter",
			"from", from, "to", to, "err", reason)
		return nil
	}
	if from == to {
		reason := safeFailureReason(err)
		maxRetries := p.cfg.Indexer.FailureMaxRetries
		for _, h := range p.registry.All() {
			if rerr := p.state.RecordFailure(ctx, h.Name(), from, reason, maxRetries); rerr != nil {
				slog.Warn("could not record dead-letter", "height", from, "err", rerr)
			}
		}
		slog.Error("giving up on block after all retries, recorded in dead-letter",
			"height", from, "err", reason)
		return nil
	}
	mid := from + (to-from)/2
	slog.Warn("batch failed, bisecting and retrying halves",
		"from", from, "to", to, "mid", mid, "err", err)
	if e1 := p.processWithSplit(ctx, writeCh, from, mid); e1 != nil {
		return e1
	}
	return p.processWithSplit(ctx, writeCh, mid+1, to)
}

// recordBatchFailure records every height in [from, to] as a transient
// failure — used when persist throws on a batch that fetched
// successfully. The dead-letter sweep will re-fetch and re-persist these.
func (p *Pipeline) recordBatchFailure(ctx context.Context, from, to int64, reason string) {
	maxRetries := p.cfg.Indexer.FailureMaxRetries
	for h := from; h <= to; h++ {
		for _, handler := range p.registry.All() {
			if rerr := p.state.RecordFailure(ctx, handler.Name(), h, reason, maxRetries); rerr != nil {
				slog.Warn("could not record persist failure", "height", h, "err", rerr)
			}
		}
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// safeFailureReason returns a dead-letter-safe one-line reason string
// for err. Typed RPC errors that carry URLs or upstream response bodies
// (rpc.HeightPrunedError, rpc.HTTPStatusError) are stripped down to
// status-only forms — operators put bearer tokens in URL paths and
// providers echo query strings in error bodies, and indexer_failures.reason
// is surfaced unauthenticated on /api/status. Generic errors fall through
// to the truncated err.Error() and should not contain credentials.
func safeFailureReason(err error) string {
	if err == nil {
		return ""
	}
	var hp *rpc.HeightPrunedError
	if errors.As(err, &hp) {
		return fmt.Sprintf("height %d pruned (http %d)", hp.Height, hp.Status)
	}
	var hs *rpc.HTTPStatusError
	if errors.As(err, &hs) {
		return fmt.Sprintf("http %d", hs.Status)
	}
	var nec *rpc.NoEndpointCoversError
	if errors.As(err, &nec) {
		return fmt.Sprintf("no endpoint covers heights %d-%d", nec.MinHeight, nec.MaxHeight)
	}
	return truncate(err.Error(), 250)
}

// fetchBatch pulls blocks from MultiClient, dispatches events to handlers,
// and computes which handlers still need this range. Returns a writeJob
// the caller can hand off to the writer pool. Does NOT touch the DB for
// writes — only the HandlerNeedsRange read, which uses its own brief
// pool connection outside any tx.
func (p *Pipeline) fetchBatch(ctx context.Context, from, to int64) (writeJob, error) {
	heights := make([]int64, 0, to-from+1)
	for h := from; h <= to; h++ {
		heights = append(heights, h)
	}

	// MultiClient.FetchBlocks is the authoritative failover loop.
	blocks, err := p.client.FetchBlocks(ctx, heights)
	if err != nil {
		return writeJob{}, fmt.Errorf("fetch: %w", err)
	}

	dispatched := p.registry.Dispatch(blocks)
	var rowsThisBatch int64
	for _, evs := range dispatched {
		rowsThisBatch += int64(len(evs))
	}
	p.totalRows.Add(rowsThisBatch)

	// HandlerNeedsRange is read-only and MUST run outside any tx —
	// calling it inside BeginTxFunc pool-deadlocks at
	// workers > pool_size.
	needs := make(map[string]bool, len(p.registry.All()))
	for _, h := range p.registry.All() {
		n, err := p.state.HandlerNeedsRange(ctx, h.Name(), from, to)
		if err != nil {
			return writeJob{}, fmt.Errorf("needs-range: %w", err)
		}
		needs[h.Name()] = n
	}

	return writeJob{
		from:            from,
		to:              to,
		dispatched:      dispatched,
		needs:           needs,
		rowsThisBatch:   rowsThisBatch,
		blocksThisBatch: to - from + 1,
	}, nil
}

// persistJobs runs the writer half of the pipeline for N batches in a
// single tx. For each job: for each handler that still needs this batch,
// call Persist (skipped entirely when the dispatched event slice for that
// handler is empty — the common case for historical-empty blocks) and
// RecordRange. One Heartbeat sums all jobs. Returns an error when the tx
// fails; the caller records every job's range as dead-letter.
//
// Aggregating N jobs per tx amortises BEGIN / Heartbeat / COMMIT overhead
// across those jobs. At batch=50 and agg=16 that's 800 blocks per tx —
// one COMMIT instead of sixteen for the same amount of work.
func (p *Pipeline) persistJobs(ctx context.Context, jobs []writeJob) error {
	return pgx.BeginTxFunc(ctx, p.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		var totalBlocks, totalRows int64
		for _, w := range jobs {
			for _, handler := range p.registry.All() {
				if !w.needs[handler.Name()] {
					continue
				}
				evs := w.dispatched[handler]
				// Empty-event optimisation: handler.Persist is a no-op
				// when there are no events, but iterating + calling it
				// still costs interface dispatch + tx bookkeeping. Skip
				// directly to RecordRange so the range still gets marked
				// covered (next producer pass won't re-fetch).
				if len(evs) > 0 {
					if err := handler.Persist(ctx, tx, evs); err != nil {
						return fmt.Errorf("%s persist: %w", handler.Name(), err)
					}
				}
				if err := p.state.RecordRange(ctx, tx, handler.Name(), w.from, w.to); err != nil {
					return fmt.Errorf("%s record range: %w", handler.Name(), err)
				}
			}
			totalBlocks += w.blocksThisBatch
			totalRows += w.rowsThisBatch
		}
		if p.runID != 0 {
			if err := p.state.Heartbeat(ctx, tx, p.runID, totalBlocks, totalRows); err != nil {
				return fmt.Errorf("heartbeat: %w", err)
			}
		}
		return nil
	})
}

