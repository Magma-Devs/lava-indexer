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
				for _, handler := range p.registry.All() {
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
					for _, h := range heights {
						if ctx.Err() != nil {
							return nil
						}
						// Swallow errors: processWithSplit only returns ctx
						// errors; real failures re-insert into indexer_failures.
						_ = p.processWithSplit(ctx, h, h)
					}
				}
			}
		})
	}

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

// processWithSplit runs processBatch. On a terminal transient failure it
// bisects the range and retries each half, continuing down to size 1. A
// size-1 range that STILL fails is recorded in indexer_failures and
// skipped — its height stays absent from indexer_ranges so the next
// indexer run picks it up automatically.
//
// Permanent failures short-circuit the bisect: if no endpoint covers the
// range or the node has pruned the heights, splitting the range can't
// change that — every sub-range will hit the same error. In that case
// every height in [from, to] is recorded as permanent in one shot and
// the function returns without bisecting.
func (p *Pipeline) processWithSplit(ctx context.Context, from, to int64) error {
	err := p.processBatch(ctx, from, to)
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if isPermanentFetchError(err) {
		reason := truncate(err.Error(), 250)
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
		// Single block and still failing transiently → dead-letter for the
		// sweep to retry. RecordFailure flips the row to permanent when
		// retries hit the configured cap so the sweep eventually stops.
		reason := truncate(err.Error(), 250)
		maxRetries := p.cfg.Indexer.FailureMaxRetries
		for _, h := range p.registry.All() {
			if rerr := p.state.RecordFailure(ctx, h.Name(), from, reason, maxRetries); rerr != nil {
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

	// Determine which handlers need this batch BEFORE acquiring a tx.
	// HandlerNeedsRange issues a pool query; running it inside BeginTxFunc
	// means every worker holds one pool connection for its tx while trying
	// to acquire a second one for the gap read. Under N workers > pool_size
	// that self-deadlocks — every connection is held by a tx waiting on a
	// gap-read connection that will never come (observed at pool_size=16,
	// workers=64). The result was thousands of "idle in transaction" rows
	// in pg_stat_activity and /api/status timing out on its own pool acquire.
	// The read is idempotent and safe to run without a tx — a concurrent
	// writer's changes between now and tx-begin are handled by the advisory
	// lock + merge semantics in RecordRange.
	needs := make(map[string]bool, len(p.registry.All()))
	anyNeeds := false
	for _, h := range p.registry.All() {
		n, err := p.state.HandlerNeedsRange(ctx, h.Name(), from, to)
		if err != nil {
			return fmt.Errorf("needs-range: %w", err)
		}
		needs[h.Name()] = n
		if n {
			anyNeeds = true
		}
	}

	var rowsThisBatch int64
	for _, evs := range dispatched {
		rowsThisBatch += int64(len(evs))
	}
	blocksThisBatch := to - from + 1

	// If no handler needs this batch, still commit a heartbeat so the run's
	// progress counters advance and the dashboard shows the batch as work
	// done (fetched but nothing to write). Skip the per-handler section.
	return pgx.BeginTxFunc(ctx, p.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if anyNeeds {
			for _, handler := range p.registry.All() {
				if !needs[handler.Name()] {
					continue
				}
				if err := handler.Persist(ctx, tx, dispatched[handler]); err != nil {
					return fmt.Errorf("%s persist: %w", handler.Name(), err)
				}
				if err := p.state.RecordRange(ctx, tx, handler.Name(), from, to); err != nil {
					return fmt.Errorf("%s record range: %w", handler.Name(), err)
				}
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
