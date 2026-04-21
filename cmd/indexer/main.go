package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/magma-devs/lava-indexer/internal/aggregates"
	"github.com/magma-devs/lava-indexer/internal/config"
	"github.com/magma-devs/lava-indexer/internal/events"
	"github.com/magma-devs/lava-indexer/internal/events/relay_payment"
	"github.com/magma-devs/lava-indexer/internal/pipeline"
	"github.com/magma-devs/lava-indexer/internal/rpc"
	"github.com/magma-devs/lava-indexer/internal/snapshotters"
	"github.com/magma-devs/lava-indexer/internal/snapshotters/provider_rewards"
	"github.com/magma-devs/lava-indexer/internal/state"
	"github.com/magma-devs/lava-indexer/internal/web"
	_ "go.uber.org/automaxprocs" // auto-tunes GOMAXPROCS from cgroup CPU quota
	"golang.org/x/sync/errgroup"
)

func main() {
	var cfgPath string
	var benchmark bool
	flag.StringVar(&cfgPath, "config", "config.yml", "path to config.yml (optional — env vars can supply everything)")
	flag.BoolVar(&benchmark, "benchmark", false, "probe each endpoint's concurrency/throughput curve and exit without indexing")
	flag.Parse()

	cfg, err := config.Load(cfgPath)
	if err != nil {
		// Hard-code JSON here since we haven't installed a handler yet.
		_, _ = fmt.Fprintf(os.Stderr, `{"time":"%s","level":"ERROR","msg":"load config","err":"%s"}`+"\n",
			time.Now().UTC().Format(time.RFC3339Nano), err.Error())
		os.Exit(1)
	}
	slog.SetDefault(newLogger(cfg.Log))

	// Auto-set GOMEMLIMIT to 90% of the container's cgroup memory so the Go
	// GC paces itself before the kernel OOM-kills us. Silently no-ops
	// outside a cgroup (e.g. bare metal). automaxprocs does the CPU side
	// via its init() import above.
	if _, err := memlimit.SetGoMemLimitWithOpts(
		memlimit.WithRatio(0.9),
		memlimit.WithProvider(memlimit.FromCgroupHybrid),
	); err != nil {
		slog.Info("memlimit: not running in a memory-capped cgroup", "info", err.Error())
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client, err := newClient(cfg)
	if err != nil {
		fatal("build rpc client", err)
	}
	defer client.Close()

	if err := client.Probe(ctx, cfg.Network.ChainID); err != nil {
		fatal("probe endpoints", err)
	}
	// Resolve chain genesis once (initial_height + genesis_time) for the
	// dashboard's coverage percent and timeline axis. Non-fatal on failure;
	// MultiClient.Genesis() falls back to 1.
	client.FetchGenesis(ctx)

	// Start the metrics recorder so the per-endpoint history ring gets
	// populated every 3s for the web UI's sparklines.
	client.StartMetricsRecorder(ctx, 3*time.Second)

	// Adaptive sizer (optional): takes over fetch_workers and/or
	// fetch_batch_size when either is set to 0. It ticks every 10s,
	// reads per-endpoint signals (error rate + p50/p99), and:
	//   - shrinks batch ×0.5 on hot signals, grows +1 on calm signals
	//   - sizes worker cap to min(Σ budgets × 2, CPU × 8, mem / 2 MiB)
	// Explicit positive values in config bypass the sizer entirely.
	//
	// Resolved values are kept in local variables and passed into the
	// pipeline / pool via constructors — cfg.Indexer is never mutated, so
	// /api/status and any future "effective config" surface still show
	// what the operator typed.
	workersAdaptive := cfg.Indexer.FetchWorkers <= 0
	batchAdaptive := cfg.Indexer.FetchBatchSize <= 0
	resolvedBatchMax := cfg.Indexer.FetchBatchSize
	resolvedWorkers := cfg.Indexer.FetchWorkers
	var sizer *pipeline.AdaptiveSizer
	if workersAdaptive || batchAdaptive {
		// Pick bounds: if the user set a positive batch size, treat it
		// as the cap; otherwise use a sensible default.
		batchMax := cfg.Indexer.FetchBatchSize
		if batchMax <= 0 {
			batchMax = 20
		}
		sizer = pipeline.NewAdaptiveSizer(1, batchMax, 256, 10*time.Second, multiClientSignals{client})
		sizer.Start(ctx)
		if batchAdaptive {
			resolvedBatchMax = batchMax
		}
	}
	if workersAdaptive {
		workers := 16
		if sizer != nil && sizer.Workers() > 0 {
			workers = sizer.Workers()
		} else {
			var sum int
			for _, ep := range client.Endpoints() {
				if !ep.Disabled {
					sum += 64
				}
			}
			if sum > 0 {
				workers = sum
			}
		}
		slog.Info("adaptive fetch_workers resolved", "workers", workers)
		resolvedWorkers = workers
	}

	// Pool is built AFTER fetch_workers is resolved so newPool can size
	// MaxConns to the actual worker count (+ web/aggregates headroom).
	// With pool_size=8 vs adaptive workers≈100, a stale default starves
	// every worker past the 8th and silently caps throughput at ~8% of
	// what the operator configured.
	pool, err := newPool(ctx, cfg, resolvedWorkers)
	if err != nil {
		fatal("connect postgres", err)
	}
	defer pool.Close()

	if benchmark {
		runBenchmark(ctx, client)
		return
	}

	if cfg.Indexer.EarliestMode {
		minE := int64(0)
		for _, ep := range client.Endpoints() {
			if ep.Disabled || ep.Earliest <= 0 {
				continue
			}
			if minE == 0 || ep.Earliest < minE {
				minE = ep.Earliest
			}
		}
		if minE == 0 {
			fatal("earliest mode", fmt.Errorf("no healthy endpoint reported an earliest block"))
		}
		slog.Info("earliest mode: resolved start_height from endpoint probe",
			"resolved", minE, "configured", cfg.Indexer.StartHeight)
		cfg.Indexer.StartHeight = minE
	}
	if err := client.ValidateRange(cfg.Indexer.StartHeight, cfg.Indexer.EndHeight); err != nil {
		fatal("validate start height", err)
	}

	st := state.New(pool, cfg.Database.Schema)
	if err := st.Ensure(ctx); err != nil {
		fatal("ensure state table", err)
	}

	// Register handlers. Add more here as you add event types.
	// cfg.Indexer.Handlers selects a subset; "all" (or empty) means every one.
	reg := events.NewRegistry()
	register := func(h events.Handler) {
		if cfg.Indexer.WantsHandler(h.Name()) {
			reg.Register(h)
			slog.Info("handler registered", "name", h.Name())
		} else {
			slog.Info("handler skipped by config", "name", h.Name())
		}
	}
	register(relay_payment.New(cfg.Database.Schema))

	// Apply every handler's DDL before indexing starts. Each handler's
	// DDL slice runs inside one tx so a partial failure (network blip,
	// lock contention, syntax mismatch on a future PG version) leaves the
	// DB at the pre-bring-up state instead of half-migrated. CREATE
	// TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS are
	// transaction-safe in PG.
	for _, h := range reg.All() {
		ddls := h.DDL()
		err := pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			for _, ddl := range ddls {
				if _, err := tx.Exec(ctx, ddl); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			fatal("apply handler DDL", fmt.Errorf("%s: %w", h.Name(), err))
		}
	}

	// Warm any handler dictionaries so steady-state IDs() lookups hit
	// the in-process cache instead of round-tripping to PG every batch.
	for _, h := range reg.All() {
		w, ok := h.(events.Warmer)
		if !ok {
			continue
		}
		if err := w.Warmup(ctx, pool); err != nil {
			slog.Warn("handler warmup failed (non-fatal)", "name", h.Name(), "err", err)
		}
	}

	// Register snapshotters. Snapshotters are the periodic, calendar-
	// driven ingestion seam (parallel to events.Handler for event-
	// driven). Selection mirrors the handler pattern:
	// cfg.Snapshotters.Handlers selects a subset; "all" (or empty) means
	// every one. Snapshotters not selected are never constructed —
	// no DDL, no goroutine, nothing in /api/snapshotters.
	snapReg := snapshotters.NewRegistry()
	registerSnapshotter := func(s snapshotters.Snapshotter) {
		if cfg.Snapshotters.WantsSnapshotter(s.Name()) {
			snapReg.Register(s)
			slog.Info("snapshotter registered", "name", s.Name())
		} else {
			slog.Info("snapshotter skipped by config", "name", s.Name())
		}
	}
	// Prefer the operator-configured snapshot_anchor over the chain's
	// /genesis timestamp — needed for forked / renamed chains where
	// /genesis still reports the upstream origin (e.g. testnet-2 still
	// reporting testnet-1's 2022-12-26 genesis when the operational
	// start was 2023-08-17).
	snapAnchor := cfg.Snapshotters.ProviderRewards.ParsedSnapshotAnchor()
	if snapAnchor.IsZero() {
		snapAnchor = client.GenesisTime()
	}
	registerSnapshotter(provider_rewards.New(provider_rewards.Config{
		Schema:        cfg.Database.Schema,
		EarliestDate:  cfg.Snapshotters.ProviderRewards.ParsedEarliestDate(),
		Concurrency:   cfg.Snapshotters.ProviderRewards.Concurrency,
		RESTURL:       resolveSnapshotterRESTURL(cfg),
		RESTHeaders:   resolveSnapshotterRESTHeaders(cfg),
		GenesisHeight: client.Genesis(),
		GenesisTime:   snapAnchor,
	}))
	// Apply snapshotter DDL, same one-tx-per-snapshotter pattern as
	// event handlers. Each snapshotter owns its tables so a partial DDL
	// failure rolls back cleanly.
	for _, s := range snapReg.All() {
		ddls := s.DDL()
		err := pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			for _, ddl := range ddls {
				if _, err := tx.Exec(ctx, ddl); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			fatal("apply snapshotter DDL", fmt.Errorf("%s: %w", s.Name(), err))
		}
	}
	// Snapshotters that implement events.Warmer get the same warm-up
	// treatment as handlers — pre-loads dict caches so the first tick
	// doesn't pay per-provider round-trips.
	for _, s := range snapReg.All() {
		w, ok := s.(events.Warmer)
		if !ok {
			continue
		}
		if err := w.Warmup(ctx, pool); err != nil {
			slog.Warn("snapshotter warmup failed (non-fatal)", "name", s.Name(), "err", err)
		}
	}

	// Apply user-defined aggregates + schedule refresh jobs.
	if err := aggregates.Apply(ctx, pool, cfg.AggregatesDir); err != nil {
		slog.Warn("aggregates apply failed (non-fatal)", "err", err)
	}

	// Optional: drop state for heights outside the current window. Used
	// when the operator has narrowed start_height (or dropped earliest_mode)
	// after a previous run populated things below the new window. One-shot
	// and idempotent — if there's nothing to prune the counts are 0.
	if cfg.Indexer.PruneOutsideWindow && !cfg.Indexer.EarliestMode {
		pruneEnd := cfg.Indexer.EndHeight
		if pruneEnd == 0 {
			if tip, err := client.Tip(ctx); err == nil {
				pruneEnd = tip
			}
		}
		if pruneEnd > 0 {
			for _, h := range reg.All() {
				rr, ff, err := st.PruneOutsideWindow(ctx, h.Name(), cfg.Indexer.StartHeight, pruneEnd)
				if err != nil {
					slog.Warn("prune outside window failed", "handler", h.Name(), "err", err)
					continue
				}
				if rr > 0 || ff > 0 {
					slog.Info("pruned state outside window",
						"handler", h.Name(),
						"start", cfg.Indexer.StartHeight, "end", pruneEnd,
						"ranges_deleted", rr, "failures_deleted", ff)
				}
			}
		}
	}

	pipe := pipeline.New(cfg, pool, client, st, reg).
		WithRuntimeKnobs(resolvedWorkers, resolvedBatchMax)
	if sizer != nil {
		pipe = pipe.WithSizer(sizer)
	}
	slog.Info("indexer starting",
		"chain", cfg.Network.ChainID,
		"endpoints", len(cfg.Network.Endpoints),
		"start", cfg.Indexer.StartHeight,
		"end", cfg.Indexer.EndHeight,
		"workers", resolvedWorkers,
		"fetch_batch", resolvedBatchMax,
	)

	// Run the pipeline and web UI side-by-side. Either exiting with an error
	// cancels the sibling. ctx cancellation (SIGINT/SIGTERM) triggers a
	// graceful shutdown of both.
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		err := pipe.Run(gctx)
		if err != nil && !isContextDone(err) {
			return err
		}
		return nil
	})
	// Snapshotters run on their own tick; RunLoop never returns a fatal
	// error (per-tick failures are logged + continue), so this goroutine
	// only exits when ctx is cancelled by sibling shutdown.
	if len(snapReg.All()) > 0 {
		g.Go(func() error {
			return snapReg.RunLoop(gctx, pool, cfg.Snapshotters.CheckInterval)
		})
	}
	if cfg.Web.Enabled {
		srv := &web.Server{
			ChainID:           cfg.Network.ChainID,
			State:             st,
			Client:          client,
			Registry:        reg,
			Snapshotters:    snapReg,
			Pool:            pool,
			Stats:           pipelineStatsAdapter{pipe},
			Start:           cfg.Indexer.StartHeight,
			End:             cfg.Indexer.EndHeight,
			GraphQLEnabled:  cfg.GraphQL.Enabled,
			GraphQLUpstream: cfg.GraphQL.Upstream,
		}
		g.Go(func() error { return srv.ListenAndServe(gctx, cfg.Web.Addr) })
	}
	waitErr := g.Wait()
	// Close out the run row so aggregate stats reflect this run's end time.
	if pipe.RunID() != 0 {
		reason := "graceful"
		if waitErr != nil && !isContextDone(waitErr) {
			reason = "error"
		}
		endCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := st.EndRun(endCtx, pipe.RunID(), reason); err != nil {
			slog.Warn("failed to mark run ended", "err", err)
		}
		cancel()
	}
	if waitErr != nil && !isContextDone(waitErr) {
		fatal("indexer", waitErr)
	}
	slog.Info("shutdown")
}

// webPoolHeadroom reserves connections on top of fetch_workers for the
// embedded web UI's /api/status (~14 round-trips per request) plus
// aggregates and ad-hoc queries. Without headroom a busy pipeline
// makes the dashboard unresponsive and aggregate refreshes block.
const webPoolHeadroom = 4

func newPool(ctx context.Context, cfg *config.Config, resolvedWorkers int) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.Database.ConnString())
	if err != nil {
		return nil, redactSecret(err, cfg.Database.Password)
	}
	// Derive MaxConns to fit the resolved fetch_workers. Each worker holds
	// a connection for the full per-batch tx, so anything below
	// fetch_workers is a hard concurrency ceiling regardless of pool_size.
	want := resolvedWorkers + webPoolHeadroom
	max := cfg.Database.PoolSize
	if max < want {
		if max > 0 {
			slog.Warn("pool_size too small for fetch_workers; growing",
				"configured_pool_size", max,
				"fetch_workers", resolvedWorkers,
				"resolved_pool_size", want)
		}
		max = want
	}
	pc.MaxConns = int32(max)
	return pgxpool.NewWithConfig(ctx, pc)
}

func newClient(cfg *config.Config) (*rpc.MultiClient, error) {
	eps := make([]*rpc.Endpoint, 0, len(cfg.Network.Endpoints))
	for _, e := range cfg.Network.Endpoints {
		var c rpc.Client
		switch e.Kind {
		case "rpc":
			c = rpc.NewRPC(e.URL, e.Headers)
		case "rest":
			c = rpc.NewREST(e.URL, e.Headers)
		default:
			return nil, fmt.Errorf("unknown endpoint kind %q for %s", e.Kind, e.URL)
		}
		eps = append(eps, &rpc.Endpoint{URL: e.URL, Kind: e.Kind, Client: c})
	}
	return rpc.NewMulti(eps), nil
}

func isContextDone(err error) bool {
	return err == context.Canceled || err == context.DeadlineExceeded
}

// redactSecret strips a known secret string from an error message before
// it's logged. pgxpool.ParseConfig errors include the full DSN — and
// therefore the password — in their messages; once that error reaches
// slog.Error("connect postgres", "err", err) the password lands in the
// JSON log stream where shippers (CloudWatch, Loki, Datadog) index it
// for full-text search forever. Triggered by any malformed DB_PORT or
// DB_HOST env var, which is a common operational mistake.
func redactSecret(err error, secret string) error {
	if err == nil || secret == "" {
		return err
	}
	msg := strings.ReplaceAll(err.Error(), secret, "***")
	// Also catch percent-encoded variants since ConnString URL-escapes
	// the password — pgx may report either form depending on the parse
	// failure mode.
	if escaped := url.QueryEscape(secret); escaped != secret {
		msg = strings.ReplaceAll(msg, escaped, "***")
	}
	return errors.New(msg)
}

// pipelineStatsAdapter converts pipeline.Stats to web.Stats to keep the two
// packages import-cycle-free.
type pipelineStatsAdapter struct{ p *pipeline.Pipeline }

func (a pipelineStatsAdapter) Stats() web.Stats {
	s := a.p.Stats()
	return web.Stats{
		TotalBlocks:  s.TotalBlocks,
		TotalRows:    s.TotalRows,
		BlocksPerSec: s.BlocksPerSec,
		RunID:        s.RunID,
		RunStart:     s.RunStart,
	}
}

func fatal(what string, err error) {
	slog.Error(what, "err", err)
	os.Exit(1)
}

// resolveSnapshotterRESTURL returns the REST URL the provider_rewards
// snapshotter should hit. Explicit override wins; otherwise fall back
// to the first kind=rest endpoint in the main config so a typical
// single-config deployment doesn't need a duplicated URL.
func resolveSnapshotterRESTURL(cfg *config.Config) string {
	if u := strings.TrimSpace(cfg.Snapshotters.ProviderRewards.RESTURL); u != "" {
		return u
	}
	for _, e := range cfg.Network.Endpoints {
		if e.Kind == "rest" {
			return e.URL
		}
	}
	return ""
}

// resolveSnapshotterRESTHeaders picks the headers to attach to
// snapshotter-specific requests. If the snapshotter has its own headers
// in config, those win (so an operator can target an archive-mode
// upstream differently from the main one). Otherwise fall back to the
// first REST endpoint's headers.
func resolveSnapshotterRESTHeaders(cfg *config.Config) map[string]string {
	if len(cfg.Snapshotters.ProviderRewards.RESTHeaders) > 0 {
		out := make(map[string]string, len(cfg.Snapshotters.ProviderRewards.RESTHeaders))
		for k, v := range cfg.Snapshotters.ProviderRewards.RESTHeaders {
			out[k] = v
		}
		return out
	}
	for _, e := range cfg.Network.Endpoints {
		if e.Kind == "rest" {
			out := make(map[string]string, len(e.Headers))
			for k, v := range e.Headers {
				out[k] = v
			}
			return out
		}
	}
	return nil
}

// multiClientSignals bridges rpc.MultiClient into pipeline.SignalSource.
type multiClientSignals struct{ c *rpc.MultiClient }

func (m multiClientSignals) EndpointSignals() []pipeline.EndpointSignal {
	eps := m.c.Endpoints()
	out := make([]pipeline.EndpointSignal, 0, len(eps))
	for _, ep := range eps {
		if ep.Disabled {
			continue
		}
		mm := ep.Metrics()
		er := 0.0
		if mm.TotalRequests > 0 {
			er = float64(mm.TotalErrors) / float64(mm.TotalRequests)
		}
		out = append(out, pipeline.EndpointSignal{
			URL:             ep.URL,
			RecentErrorRate: er,
			LatencyP50Ms:    mm.LatencyP50Ms,
			LatencyP99Ms:    mm.LatencyP99Ms,
		})
	}
	return out
}

// newLogger picks a text or JSON slog handler based on config. JSON is the
// default — CloudWatch, Loki, Datadog, GCP etc. all index JSON log lines
// natively, so the same binary is observable anywhere without extra
// shippers.
func newLogger(cfg config.Log) *slog.Logger {
	level := slog.LevelInfo
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	switch cfg.Format {
	case "text":
		h = slog.NewTextHandler(os.Stderr, opts)
	default: // "json" (and whatever else — safe fallback)
		h = slog.NewJSONHandler(os.Stderr, opts)
	}
	return slog.New(h)
}

// runBenchmark sweeps each endpoint at 1→128 concurrency and prints a
// per-endpoint curve. The "knee" row — where req/s plateaus while p99
// climbs — tells you the node's sustainable ceiling.
func runBenchmark(ctx context.Context, client *rpc.MultiClient) {
	levels := []int{1, 2, 4, 8, 16, 32, 64, 128}
	results := rpc.RunBenchmark(ctx, client.Endpoints(), levels, 60)

	for url, curve := range results {
		fmt.Println()
		fmt.Printf("=== %s ===\n", url)
		fmt.Printf("%-13s %-10s %-10s %-8s %-8s %s\n", "concurrency", "req/s", "p50", "p99", "errors", "recommendation")
		var bestRps float64
		for _, r := range curve {
			note := ""
			if r.RequestsPerSec > bestRps*1.10 {
				bestRps = r.RequestsPerSec
				note = "↗ gaining"
			} else if r.RequestsPerSec > bestRps*0.95 {
				note = "≈ flat"
			} else {
				note = "↘ knee likely passed"
			}
			fmt.Printf("%-13d %-10.1f %-10s %-8s %-8d %s\n",
				r.Concurrency, r.RequestsPerSec,
				fmtMs(r.LatencyP50), fmtMs(r.LatencyP99), r.Errors, note)
		}
	}
	if len(results) == 0 {
		fmt.Println("no endpoints to benchmark")
	}
}

func fmtMs(d time.Duration) string {
	ms := float64(d) / float64(time.Millisecond)
	return fmt.Sprintf("%.0fms", ms)
}
