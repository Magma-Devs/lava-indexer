// Package web serves the built-in progress UI and a JSON status API.
//
// Endpoints:
//
//	GET /              → SPA (HTML + JS served from embedded assets)
//	GET /api/status    → JSON — chain, endpoints, handlers, per-handler ranges
//	GET /healthz       → simple 200 OK
//
// The UI polls /api/status every few seconds and renders a per-handler
// timeline from genesis to tip, highlighting covered ranges and gaps.
package web

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/magma-devs/lava-indexer/internal/events"
	"github.com/magma-devs/lava-indexer/internal/rpc"
	"github.com/magma-devs/lava-indexer/internal/state"
)

// StatsProvider lets us pull progress numbers from the pipeline without an
// import cycle. Pipeline.Stats satisfies this.
type StatsProvider interface {
	Stats() Stats
}

// Stats mirrors pipeline.Stats — duplicated here to keep the web package
// free of a pipeline import.
type Stats struct {
	TotalBlocks  int64
	TotalRows    int64
	BlocksPerSec float64
	RunID        int64
	RunStart     time.Time
}

//go:embed assets
var assetsFS embed.FS

// Server bundles the small HTTP surface into one dependency-injectable struct.
type Server struct {
	ChainID         string
	State           *state.State
	Client          *rpc.MultiClient
	Registry        *events.Registry
	Pool            *pgxpool.Pool
	Stats           StatsProvider // optional — enables blocks/sec in the response
	Start           int64
	End             int64
	GraphQLEnabled  bool
	GraphQLUpstream string

	// dbSizeCache memoises pg_total_relation_size for the schema. The
	// underlying SUM(pg_total_relation_size) is the heaviest query in
	// handleStatus on a multi-hundred-million-row table — it walks every
	// relation and TOAST chunk, and under checkpoint/autovacuum pressure
	// it can spike to multiple seconds, blocking the dashboard. The size
	// changes by maybe a few MB per second under normal indexing load, so
	// a 30 s cache is invisible to operators while removing the spike.
	dbSizeCache struct {
		mu    sync.Mutex
		at    time.Time
		bytes int64
	}
}

const dbSizeCacheTTL = 30 * time.Second

func (s *Server) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200); _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/history", s.handleHistory)

	// Optional reverse-proxy to PostGraphile (or whatever GraphQL server).
	// When enabled, /graphql and /graphiql on this port proxy to the
	// upstream, so the embedded UI can iframe /graphiql without CORS.
	if s.GraphQLEnabled && s.GraphQLUpstream != "" {
		if target, err := parseProxyUpstream(s.GraphQLUpstream); err == nil {
			proxy := httputil.NewSingleHostReverseProxy(target)
			mux.Handle("/graphql", proxy)
			mux.Handle("/graphql/", proxy)
			mux.Handle("/graphiql", proxy)
			mux.Handle("/graphiql/", proxy)
		} else {
			slog.Warn("graphql.upstream rejected, skipping proxy", "upstream", s.GraphQLUpstream, "err", err)
		}
	}
	mux.HandleFunc("/api/ui-config", s.handleUIConfig)

	sub, err := fs.Sub(assetsFS, "assets")
	if err != nil {
		panic(err)
	}
	mux.Handle("/", http.FileServer(http.FS(sub)))
	return mux
}

// parseProxyUpstream parses and validates the GraphQL reverse-proxy
// upstream URL. Reject anything that isn't http/https and anything that
// resolves outside loopback or RFC1918/ULA private space — without this
// the indexer becomes a SSRF proxy: a misconfigured GRAPHQL_UPSTREAM
// could be aimed at the AWS metadata service or an arbitrary attacker
// host, and any unauthenticated client of the indexer's web port could
// proxy through it.
func parseProxyUpstream(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("scheme must be http or https, got %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("missing host")
	}
	host := u.Hostname()
	// Resolve the host. ResolveIPAddr returns a single IP — fine for the
	// validation gate; we're checking that no public IP is reachable, so
	// any single resolved address tells us whether the host points at a
	// safe range. For multi-A-record hostnames Go uses LookupIP elsewhere
	// in the stack, but the proxy only needs to know if the *target* is
	// allowed to be reached.
	ips, err := net.LookupIP(host)
	if err != nil {
		return nil, fmt.Errorf("resolve %q: %w", host, err)
	}
	for _, ip := range ips {
		if !isPrivateOrLoopback(ip) {
			return nil, fmt.Errorf("upstream host %q resolves to public IP %s — refusing to proxy",
				host, ip)
		}
	}
	return u, nil
}

func isPrivateOrLoopback(ip net.IP) bool {
	// Allow loopback + RFC1918/ULA. Explicitly DENY link-local — the
	// IPv4 link-local range 169.254.0.0/16 includes the cloud metadata
	// service (169.254.169.254 on AWS/GCP/Azure), which is the canonical
	// SSRF target.
	return ip.IsLoopback() || ip.IsPrivate()
}

// handleUIConfig exposes tiny bits of config the UI needs to know at
// runtime — specifically whether the GraphQL tab should show. Keeps the
// embedded HTML static (no templating).
func (s *Server) handleUIConfig(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("content-type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"graphql_enabled": s.GraphQLEnabled,
	})
}

func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           s.Mux(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	slog.Info("web UI listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// /api/status
// ---------------------------------------------------------------------------

type StatusResponse struct {
	ChainID          string           `json:"chain_id"`
	ChainGenesis     int64            `json:"chain_genesis"`               // always 1 by Cosmos convention
	ChainGenesisTime *time.Time       `json:"chain_genesis_time,omitempty"`// set iff an endpoint reports earliest=1 (archive)
	TipTime          *time.Time       `json:"tip_time,omitempty"`          // wall-clock time at current tip
	Start            int64            `json:"start_height"`                // user-configured indexing window
	End              int64            `json:"end_height"`
	Tip              int64            `json:"tip"`
	BlocksPerSec     float64          `json:"blocks_per_sec"`              // short-window rate, 0 if not tracking yet
	GeneratedAt      time.Time        `json:"generated_at"`
	Endpoints        []EndpointStatus `json:"endpoints"`
	Handlers         []HandlerStatus  `json:"handlers"`
	CurrentRun       *RunSummary      `json:"current_run,omitempty"`
	Stats            *LifetimeStats   `json:"lifetime_stats,omitempty"`
	RecentRuns       []RunSummary     `json:"recent_runs,omitempty"`

	// Total on-disk footprint of the indexer schema: tables + materialised
	// views + their indexes + TOAST. Useful as a sanity check while the
	// backfill grows.
	DBSizeBytes int64 `json:"db_size_bytes"`
}

type RunSummary struct {
	ID            int64      `json:"id"`
	StartedAt     time.Time  `json:"started_at"`
	LastHeartbeat time.Time  `json:"last_heartbeat"`
	EndedAt       *time.Time `json:"ended_at,omitempty"`
	DurationSec   float64    `json:"duration_sec"`
	BlocksIndexed int64      `json:"blocks_indexed"`
	RowsWritten   int64      `json:"rows_written"`
	EndReason     string     `json:"end_reason,omitempty"`
}

// LifetimeStats aggregates across every run that has ever existed for this
// schema — survives process restarts.
type LifetimeStats struct {
	FirstStartedAt  *time.Time `json:"first_started_at,omitempty"`
	TotalSeconds    float64    `json:"total_seconds"`
	TotalBlocks     int64      `json:"total_blocks"`
	TotalRows       int64      `json:"total_rows"`
	TotalRuns       int        `json:"total_runs"`
}

type EndpointStatus struct {
	URL          string     `json:"url"`
	Kind         string     `json:"kind"`
	Earliest     int64      `json:"earliest"`
	EarliestTime *time.Time `json:"earliest_time,omitempty"`
	Latest       int64      `json:"latest"`
	LatestTime   *time.Time `json:"latest_time,omitempty"`
	Disabled     bool       `json:"disabled"`
	// Reason is the short human-readable explanation for Disabled — set
	// by MultiClient.Probe when an endpoint fails its probe or fails
	// chain_id validation. Empty when the endpoint is healthy.
	Reason  string               `json:"reason,omitempty"`
	Metrics *EndpointMetricsJSON `json:"metrics,omitempty"`
}

type EndpointMetricsJSON struct {
	InFlight       int64   `json:"in_flight"`
	MaxInFlight    int64   `json:"max_in_flight"`
	TotalRequests  int64   `json:"total_requests"`
	TotalErrors    int64   `json:"total_errors"`
	RequestsPerSec float64 `json:"requests_per_sec"`
	LatencyP50Ms   float64 `json:"latency_p50_ms"`
	LatencyP95Ms   float64 `json:"latency_p95_ms"`
	LatencyP99Ms   float64 `json:"latency_p99_ms"`
	HistoryURL     string  `json:"history_url,omitempty"` // pointer to /api/history?url=...
}

type HandlerStatus struct {
	Name         string           `json:"name"`
	EventTypes   []string         `json:"event_types"`
	Ranges       []RangeJSON      `json:"ranges"`
	// Gaps is "never-tried" heights only: uncovered AND not currently in
	// the dead-letter pool. FailedRanges holds the dead-letter side so
	// the UI can report them separately with different semantics.
	Gaps         []RangeJSON      `json:"gaps"`
	WindowPct    float64          `json:"window_pct"` // % of configured [start, end∨tip]
	ChainPct     float64          `json:"chain_pct"`  // % of full chain [1, tip]
	CoveredStart int64            `json:"covered_start"`
	CoveredEnd   int64            `json:"covered_end"`
	DataFrom     *time.Time       `json:"data_from,omitempty"` // min(timestamp) in handler data
	DataTo       *time.Time       `json:"data_to,omitempty"`   // max(timestamp) in handler data
	RowCounts    map[string]int64 `json:"row_counts"`

	// Dead-letter: heights that exhausted their retry budget. Still gaps
	// in indexer_ranges, so a future sweep (or restart) picks them up.
	// FailureCount sums both slices; the individual fields let the UI
	// distinguish a churning retry pool from one the pipeline has
	// permanently given up on.
	FailureCount     int64       `json:"failure_count"`
	RetryingCount    int64       `json:"retrying_count"`
	PermanentCount   int64       `json:"permanent_count"`
	MaxRetries       int         `json:"max_retries"`
	FailedRanges     []RangeJSON `json:"failed_ranges,omitempty"`
}

type RangeJSON struct {
	From int64 `json:"from"`
	To   int64 `json:"to"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	tip, _ := s.Client.Tip(ctx)
	all, err := s.State.AllRanges(ctx)
	if err != nil {
		http.Error(w, "state query: "+err.Error(), http.StatusInternalServerError)
		return
	}

	resp := StatusResponse{
		ChainID:      s.ChainID,
		ChainGenesis: 1,
		Start:        s.Start,
		End:          s.End,
		Tip:          tip,
		GeneratedAt:  time.Now(),
	}
	if s.Stats != nil {
		resp.BlocksPerSec = s.Stats.Stats().BlocksPerSec
	}

	// Current run: the row in indexer_runs whose id matches pipeline's runID.
	if s.Stats != nil {
		cur := s.Stats.Stats()
		if cur.RunID != 0 {
			runs, err := s.State.RecentRuns(ctx, 10)
			if err == nil {
				out := make([]RunSummary, 0, len(runs))
				for _, r := range runs {
					out = append(out, summariseRun(r))
				}
				resp.RecentRuns = out
				for _, r := range runs {
					if r.ID == cur.RunID {
						sum := summariseRun(r)
						resp.CurrentRun = &sum
						break
					}
				}
			}
		}
	}
	// Total schema footprint (tables + MVs + indexes + TOAST). Cached for
	// dbSizeCacheTTL — see Server.dbSizeCache godoc for rationale.
	resp.DBSizeBytes = s.cachedDBSize(ctx)

	if blocks, rows, secs, firstStarted, err := s.State.Totals(ctx); err == nil {
		resp.Stats = &LifetimeStats{
			FirstStartedAt: firstStarted,
			TotalSeconds:   secs,
			TotalBlocks:    blocks,
			TotalRows:      rows,
			TotalRuns:      len(resp.RecentRuns),
		}
	}
	var tipTime *time.Time
	var genesisTime *time.Time
	for _, ep := range s.Client.Endpoints() {
		es := EndpointStatus{
			URL: ep.URL, Kind: ep.Kind,
			Earliest: ep.Earliest, Latest: ep.Latest,
			Disabled: ep.Disabled,
			Reason:   ep.Reason,
		}
		if !ep.EarliestTime.IsZero() {
			t := ep.EarliestTime
			es.EarliestTime = &t
			// If an endpoint serves from block 1, its earliest_time is the
			// chain's genesis time.
			if ep.Earliest == 1 && (genesisTime == nil || t.Before(*genesisTime)) {
				genesisTime = &t
			}
		}
		if !ep.LatestTime.IsZero() {
			t := ep.LatestTime
			es.LatestTime = &t
			if tipTime == nil || t.After(*tipTime) {
				tipTime = &t
			}
		}
		mm := ep.Metrics()
		es.Metrics = &EndpointMetricsJSON{
			InFlight:       mm.InFlight,
			MaxInFlight:    mm.MaxInFlight,
			TotalRequests:  mm.TotalRequests,
			TotalErrors:    mm.TotalErrors,
			RequestsPerSec: mm.RequestsPerSec,
			LatencyP50Ms:   mm.LatencyP50Ms,
			LatencyP95Ms:   mm.LatencyP95Ms,
			LatencyP99Ms:   mm.LatencyP99Ms,
		}
		resp.Endpoints = append(resp.Endpoints, es)
	}
	resp.TipTime = tipTime
	resp.ChainGenesisTime = genesisTime

	windowEnd := s.End
	if windowEnd == 0 {
		windowEnd = tip
	}

	for _, h := range s.Registry.All() {
		hs := HandlerStatus{Name: h.Name(), EventTypes: h.EventTypes()}
		for _, rg := range all[h.Name()] {
			hs.Ranges = append(hs.Ranges, RangeJSON{From: rg.From, To: rg.To})
		}
		// Coverage % within the configured window [start, windowEnd] and the
		// full chain [1, tip] — the UI renders both.
		hs.WindowPct = coverageRatio(hs.Ranges, s.Start, windowEnd) * 100
		if tip > 0 {
			hs.ChainPct = coverageRatio(hs.Ranges, 1, tip) * 100
		}
		if len(hs.Ranges) > 0 {
			hs.CoveredStart = hs.Ranges[0].From
			hs.CoveredEnd = hs.Ranges[len(hs.Ranges)-1].To
		}
		// Gaps = uncovered AND not already dead-lettered, within [start, windowEnd].
		// The UI shows dead-lettered heights separately via FailedRanges.
		gaps, err := s.State.Gaps(ctx, h.Name(), s.Start, windowEnd)
		if err == nil {
			failed, ferr := s.State.FailureHeights(ctx, h.Name(), s.Start, windowEnd)
			if ferr != nil {
				// Non-fatal: if we can't read failures, fall back to raw gaps.
				for _, rg := range gaps {
					hs.Gaps = append(hs.Gaps, RangeJSON{From: rg.From, To: rg.To})
				}
			} else {
				for _, rg := range subtractHeightsFromRanges(gaps, failed) {
					hs.Gaps = append(hs.Gaps, RangeJSON{From: rg.From, To: rg.To})
				}
			}
		}
		hs.RowCounts = s.rowCountsFor(ctx, h.Name())
		if from, to, ok := s.dataTimeRangeFor(ctx, h.Name()); ok {
			hs.DataFrom = &from
			hs.DataTo = &to
		}
		if retrying, permanent, maxR, err := s.State.FailureCount(ctx, h.Name()); err == nil {
			hs.RetryingCount = retrying
			hs.PermanentCount = permanent
			hs.FailureCount = retrying + permanent
			hs.MaxRetries = maxR
		}
		if frs, err := s.State.FailureRanges(ctx, h.Name()); err == nil {
			for _, r := range frs {
				hs.FailedRanges = append(hs.FailedRanges, RangeJSON{From: r.From, To: r.To})
			}
		}
		resp.Handlers = append(resp.Handlers, hs)
	}

	w.Header().Set("content-type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(resp)
}

// rowCountsFor returns a row count per table owned by the handler. Small
// lookup tables (providers, chains) get a live COUNT(*) because it's
// trivially cheap and `pg_class.reltuples` is -1 for never-analyzed tables.
// Large fact tables use reltuples (an estimate kept fresh by autovacuum)
// clamped to 0 so the UI never shows negative numbers.
func (s *Server) rowCountsFor(ctx context.Context, handler string) map[string]int64 {
	type tableInfo struct {
		name string
		big  bool // true = use reltuples estimate, false = live COUNT(*)
	}
	tablesByHandler := map[string][]tableInfo{
		"lava_relay_payment": {
			{"relay_payments", true},
			{"providers", false},
			{"chains", false},
		},
	}
	schema := s.State.Schema()
	out := make(map[string]int64)
	for _, t := range tablesByHandler[handler] {
		var n int64
		var err error
		fq := fmt.Sprintf("%s.%s", schema, t.name)
		if t.big {
			err = s.Pool.QueryRow(ctx, `
				SELECT GREATEST(c.reltuples, 0)::bigint
				FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
				WHERE n.nspname = $1 AND c.relname = $2`,
				schema, t.name).Scan(&n)
		} else {
			err = s.Pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s`, fq)).Scan(&n)
		}
		if err == nil {
			out[fq] = n
		}
	}
	return out
}

// cachedDBSize returns the schema's pg_total_relation_size, recomputed at
// most once every dbSizeCacheTTL. On query failure (timeout etc.) it
// returns the last cached value so the dashboard keeps showing data
// instead of a zero — the size is observational, not load-bearing.
func (s *Server) cachedDBSize(ctx context.Context) int64 {
	s.dbSizeCache.mu.Lock()
	if time.Since(s.dbSizeCache.at) < dbSizeCacheTTL && s.dbSizeCache.bytes > 0 {
		out := s.dbSizeCache.bytes
		s.dbSizeCache.mu.Unlock()
		return out
	}
	s.dbSizeCache.mu.Unlock()

	var size int64
	if err := s.Pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(pg_total_relation_size(c.oid)), 0)::bigint
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relkind IN ('r','m','i','t')
	`, s.State.Schema()).Scan(&size); err != nil {
		// Fall through to whatever's cached. Empty cache → return 0; UI
		// renders "—" or similar.
		s.dbSizeCache.mu.Lock()
		out := s.dbSizeCache.bytes
		s.dbSizeCache.mu.Unlock()
		return out
	}

	s.dbSizeCache.mu.Lock()
	s.dbSizeCache.at = time.Now()
	s.dbSizeCache.bytes = size
	s.dbSizeCache.mu.Unlock()
	return size
}

// handleHistory serves time-series metrics for a single endpoint. Clients
// pass ?idx=<endpoint-index> to disambiguate when the same URL appears
// more than once (e.g. one with, one without an archive-mode header).
// ?url= is still accepted as a fallback for older callers but is
// ambiguous when URLs repeat.
func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	eps := s.Client.Endpoints()
	if idxStr := r.URL.Query().Get("idx"); idxStr != "" {
		idx, err := strconv.Atoi(idxStr)
		if err != nil || idx < 0 || idx >= len(eps) {
			http.Error(w, "idx out of range", http.StatusBadRequest)
			return
		}
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(eps[idx].MetricHistory())
		return
	}
	url := r.URL.Query().Get("url")
	if url == "" {
		http.Error(w, "missing idx or url query param", http.StatusBadRequest)
		return
	}
	for _, ep := range eps {
		if ep.URL == url {
			w.Header().Set("content-type", "application/json")
			_ = json.NewEncoder(w).Encode(ep.MetricHistory())
			return
		}
	}
	http.Error(w, "endpoint not found", http.StatusNotFound)
}

// summariseRun converts a state.Run into the UI-friendly wire type.
func summariseRun(r state.Run) RunSummary {
	end := r.LastHeartbeat
	if r.EndedAt != nil {
		end = *r.EndedAt
	}
	return RunSummary{
		ID:            r.ID,
		StartedAt:     r.StartedAt,
		LastHeartbeat: r.LastHeartbeat,
		EndedAt:       r.EndedAt,
		DurationSec:   end.Sub(r.StartedAt).Seconds(),
		BlocksIndexed: r.BlocksIndexed,
		RowsWritten:   r.RowsWritten,
		EndReason:     r.EndReason,
	}
}

// dataTimeRangeFor returns (min, max) of the timestamp column for this
// handler's primary fact table. Both lookups use the composite PK
// ordered by block_height so each is a single index hit even on billions
// of rows. Returns ok=false if the handler has no mapped table or the
// table is empty.
func (s *Server) dataTimeRangeFor(ctx context.Context, handler string) (time.Time, time.Time, bool) {
	tablesByHandler := map[string]string{
		"lava_relay_payment": "relay_payments",
	}
	t, ok := tablesByHandler[handler]
	if !ok {
		return time.Time{}, time.Time{}, false
	}
	var from, to time.Time
	fq := fmt.Sprintf("%s.%s", s.State.Schema(), t)
	if err := s.Pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT timestamp FROM %s ORDER BY block_height ASC LIMIT 1`, fq)).Scan(&from); err != nil {
		return time.Time{}, time.Time{}, false
	}
	if err := s.Pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT timestamp FROM %s ORDER BY block_height DESC LIMIT 1`, fq)).Scan(&to); err != nil {
		return time.Time{}, time.Time{}, false
	}
	return from, to, true
}

// coverageRatio returns the fraction of [from, to] that is covered by ranges.
// 0 if to < from.
// subtractHeightsFromRanges splits each input range at every height in
// `heights`, returning the sub-ranges that remain after the heights are
// removed. `heights` must be sorted ascending (PopFailures / FailureHeights
// both produce sorted output). Used by the status handler to return
// "never-tried" gaps: raw gaps minus dead-lettered heights.
func subtractHeightsFromRanges(ranges []state.Range, heights []int64) []state.Range {
	if len(heights) == 0 || len(ranges) == 0 {
		return ranges
	}
	out := make([]state.Range, 0, len(ranges))
	h := 0
	for _, rg := range ranges {
		// Advance h past heights below this range — they're irrelevant.
		for h < len(heights) && heights[h] < rg.From {
			h++
		}
		cur := rg.From
		for h < len(heights) && heights[h] <= rg.To {
			if heights[h] > cur {
				out = append(out, state.Range{From: cur, To: heights[h] - 1})
			}
			cur = heights[h] + 1
			h++
		}
		if cur <= rg.To {
			out = append(out, state.Range{From: cur, To: rg.To})
		}
	}
	return out
}

func coverageRatio(ranges []RangeJSON, from, to int64) float64 {
	if to < from {
		return 0
	}
	covered := int64(0)
	for _, rg := range ranges {
		f := rg.From
		t := rg.To
		if f < from {
			f = from
		}
		if t > to {
			t = to
		}
		if t >= f {
			covered += t - f + 1
		}
	}
	total := to - from + 1
	if total == 0 {
		return 0
	}
	return float64(covered) / float64(total)
}
