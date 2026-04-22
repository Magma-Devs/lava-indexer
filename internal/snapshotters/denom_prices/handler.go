// Package denom_prices implements a Snapshotter that records CoinGecko
// historical USD prices for every known denom (see app.denom_metadata)
// at the same genesis-anchored monthly cadence as provider_rewards.
//
// Pricing sources are hydrated at startup via the DDL seed in
// provider_rewards: each row in app.denom_metadata carries a
// `coingecko_id` operators can edit post-deploy (UPDATE in place). This
// snapshotter reads that overlay, calls CoinGecko's
// `/coins/{id}/history?date=DD-MM-YYYY` endpoint for each denom that
// has a coingecko_id and isn't suppressed, and writes one row per
// (snapshot_date, denom) into app.denom_prices.
//
// After a successful write the snapshotter refreshes
// app.priced_rewards (MV that joins provider_rewards × denom_metadata ×
// denom_prices) concurrently so the pricing view stays live without a
// table-level lock.
package denom_prices

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/magma-devs/lava-indexer/internal/rpc"
	"github.com/magma-devs/lava-indexer/internal/snapshotters"
	"github.com/magma-devs/lava-indexer/internal/snapshotters/provider_rewards"
)

// Name is the stable identifier for this snapshotter. Matches the
// Snapshotter.Name() contract — used in log lines and on /api/snapshotters.
const Name = "denom_prices"

// coingeckoMaxAttempts caps per-call retries. Matches pricing.ts
// (COINGECKO_MAX_ATTEMPTS=5) so the Go snapshotter has the same
// retry budget as the TS reference implementation.
const coingeckoMaxAttempts = 5

// coingeckoMaxBackoff caps the inter-attempt sleep. 60s matches
// pricing.ts (COINGECKO_MAX_BACKOFF_MS).
const coingeckoMaxBackoff = 60 * time.Second

// lavaCoingeckoID is the CoinGecko id for LAVA. The snapshotter treats
// the LAVA price as load-bearing (if it fails, the whole date is
// marked failed) so it's fetched first and a failure short-circuits
// the rest of the per-date fan-out.
const lavaCoingeckoID = "lava-network"

// Config is the subset of cfg.Snapshotters.DenomPrices this package
// needs. Passed explicitly (rather than reaching into the global
// Config) so the snapshotter is straightforward to test — see
// handler_test.go.
type Config struct {
	Schema           string
	EarliestDate     time.Time
	CoingeckoAPIURL  string            // base URL of the CoinGecko API (v3)
	CoingeckoHeaders map[string]string // extra headers (e.g. x-cg-pro-api-key)
	// RESTCaller drives the block-time lookup for BlocksDue. Reuse the
	// same caller wired for provider_rewards so the binary-search
	// benefits from the shared HTTP pool and archive routing.
	RESTCaller provider_rewards.RESTCaller
	// GenesisHeight is the hint for the BlocksDue binary-search lower bound.
	GenesisHeight int64
	// GenesisTime anchors the monthly cadence — same shape as the
	// provider_rewards anchor.
	GenesisTime time.Time
}

// New returns a Handler wired with the default HTTP CoinGecko client.
// Tests use NewWithCaller to swap in a mock.
func New(cfg Config) *Handler {
	return NewWithCaller(cfg, NewHTTPCaller(cfg.CoingeckoAPIURL, cfg.CoingeckoHeaders))
}

// NewWithCaller is the test-friendly constructor. Accepts any
// CoingeckoCaller so unit tests can exercise response classification
// without a live HTTP server.
func NewWithCaller(cfg Config, caller CoingeckoCaller) *Handler {
	if cfg.Schema == "" {
		cfg.Schema = "app"
	}
	return &Handler{
		cfg:   cfg,
		gecko: caller,
	}
}

// Handler is the Snapshotter implementation for denom_prices.
type Handler struct {
	cfg   Config
	gecko CoingeckoCaller

	// blockCache memoises (date → block height) across runs — matches
	// the provider_rewards cache shape so BlocksDue doesn't re-do the
	// binary search on a rerun.
	blockCache struct {
		sync.Mutex
		m map[string]cacheEntry
	}
}

type cacheEntry struct {
	blockHeight int64
	blockTime   time.Time
}

// Name returns the stable identifier.
func (h *Handler) Name() string { return Name }

// RESTURL exposes the CoinGecko API base URL so the dashboard can
// render it alongside the rest of the configuration-dependent state.
func (h *Handler) RESTURL() string { return h.cfg.CoingeckoAPIURL }

// DDL returns the SQL statements that create the tables owned by this
// snapshotter. The denom_metadata seed rows live in provider_rewards'
// DDL (so that snapshotter bootstraps the dict standalone); here we
// own only the denom_price_snapshots + denom_prices + priced_rewards
// tables/MV.
func (h *Handler) DDL() []string {
	return []string{
		fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %[1]s.denom_price_snapshots (
			  snapshot_date DATE        PRIMARY KEY,
			  block_time    TIMESTAMPTZ NOT NULL,
			  snapshot_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
			  denom_count   INT         NOT NULL,
			  status        TEXT        NOT NULL DEFAULT 'ok',
			  error         TEXT
			);`, h.cfg.Schema),
		fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %[1]s.denom_prices (
			  snapshot_date DATE            NOT NULL
			    REFERENCES %[1]s.denom_price_snapshots(snapshot_date) ON DELETE CASCADE,
			  denom_id      INT             NOT NULL REFERENCES %[1]s.denoms(id),
			  price_usd     NUMERIC(40, 18) NOT NULL,
			  coingecko_id  TEXT,
			  PRIMARY KEY (snapshot_date, denom_id)
			);`, h.cfg.Schema),
		fmt.Sprintf(`
			CREATE INDEX IF NOT EXISTS idx_denom_prices_denom_date
			  ON %[1]s.denom_prices (denom_id, snapshot_date);`, h.cfg.Schema),
		// priced_rewards is an MV joining provider_rewards × denom_metadata ×
		// denom_prices for dashboard-friendly reads. Drop + recreate keeps
		// schema migrations idempotent without tracking view versions.
		// CONCURRENTLY refresh depends on the UNIQUE INDEX defined below —
		// without it REFRESH MATERIALIZED VIEW CONCURRENTLY errors.
		fmt.Sprintf(`DROP MATERIALIZED VIEW IF EXISTS %[1]s.priced_rewards CASCADE;`, h.cfg.Schema),
		fmt.Sprintf(`
			CREATE MATERIALIZED VIEW %[1]s.priced_rewards AS
			SELECT
			  ps.snapshot_date,
			  ps.block_height,
			  ps.block_time,
			  p.addr                                       AS provider,
			  c.name                                       AS spec,
			  pr.source_kind                               AS source_kind,
			  pr.source_denom                              AS source_denom,
			  d.denom                                      AS resolved_denom,
			  dm.base_denom                                AS display_denom,
			  pr.amount                                    AS raw_amount,
			  (pr.amount::numeric / dm.factor::numeric)    AS display_amount,
			  dp.price_usd                                 AS price_usd,
			  ((pr.amount::numeric / dm.factor::numeric) * dp.price_usd) AS value_usd
			FROM %[1]s.provider_rewards pr
			JOIN %[1]s.provider_rewards_snapshots ps ON ps.block_height = pr.block_height
			JOIN %[1]s.providers p                   ON p.id = pr.provider_id
			JOIN %[1]s.chains    c                   ON c.id = pr.spec_id
			JOIN %[1]s.denoms    d                   ON d.id = pr.denom_id
			LEFT JOIN %[1]s.denom_metadata dm        ON dm.denom_id = pr.denom_id
			LEFT JOIN %[1]s.denom_prices   dp        ON dp.snapshot_date = ps.snapshot_date
			                                      AND dp.denom_id      = pr.denom_id
			WHERE ps.status = 'ok' AND COALESCE(dm.suppress, FALSE) = FALSE;`, h.cfg.Schema),
		fmt.Sprintf(`
			CREATE UNIQUE INDEX IF NOT EXISTS uniq_priced_rewards
			  ON %[1]s.priced_rewards
			  (block_height, provider, spec, source_kind, source_denom);`, h.cfg.Schema),
	}
}

// BlocksDue computes the set of (slot, block) targets that still need
// a price snapshot. Uses the same genesis-anchored cadence as
// provider_rewards (ExpectedDates) — both snapshotters share the 17th-
// of-each-month slots. Dates already present in denom_price_snapshots
// with status IN ('ok', 'failed') are subtracted.
func (h *Handler) BlocksDue(ctx context.Context, pool *pgxpool.Pool) ([]snapshotters.SnapshotTarget, error) {
	slots := provider_rewards.ExpectedDates(h.cfg.GenesisTime, h.cfg.EarliestDate, time.Now().UTC())
	if len(slots) == 0 {
		return nil, nil
	}

	covered, err := h.coveredDates(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("query covered dates: %w", err)
	}

	targets := make([]snapshotters.SnapshotTarget, 0, len(slots))
	for _, slot := range slots {
		if _, ok := covered[slot.Format("2006-01-02")]; ok {
			continue
		}
		target, cached, err := h.blockForSlot(ctx, slot)
		if err != nil {
			// Non-fatal for a single slot — the chain may be temporarily
			// unreachable. Log and keep going so one bad date doesn't
			// starve the others.
			slog.Warn("resolve block for snapshot slot failed",
				"snapshotter", Name, "date", slot.Format("2006-01-02"), "err", err)
			continue
		}
		if !cached {
			slog.Info("resolved block for snapshot slot",
				"snapshotter", Name,
				"date", slot.Format("2006-01-02"),
				"block", target.BlockHeight,
				"block_time", target.BlockTime)
		}
		targets = append(targets, target)
	}
	return targets, nil
}

func (h *Handler) coveredDates(ctx context.Context, pool *pgxpool.Pool) (map[string]struct{}, error) {
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT snapshot_date FROM %s.denom_price_snapshots
		WHERE status IN ('ok', 'failed')`, h.cfg.Schema))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]struct{})
	for rows.Next() {
		var d time.Time
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		out[d.UTC().Format("2006-01-02")] = struct{}{}
	}
	return out, rows.Err()
}

func (h *Handler) blockForSlot(ctx context.Context, slot time.Time) (snapshotters.SnapshotTarget, bool, error) {
	key := slot.Format("2006-01-02")
	h.blockCache.Lock()
	if h.blockCache.m == nil {
		h.blockCache.m = make(map[string]cacheEntry)
	}
	if e, ok := h.blockCache.m[key]; ok {
		h.blockCache.Unlock()
		return snapshotters.SnapshotTarget{
			BlockHeight:  e.blockHeight,
			SnapshotDate: slot,
			// block_time is the scheduled slot (the monthly-17th 15:00
			// UTC anchor), not the resolved header time. The row's
			// logical date is what downstream consumers care about;
			// the actual block height is already recorded for audit.
			BlockTime: slot,
		}, true, nil
	}
	h.blockCache.Unlock()

	// Skip block resolution entirely when no RESTCaller is wired (test
	// path). In production RESTCaller is always non-nil; tests
	// construct Handler with a mock that only answers CoinGecko calls.
	if h.cfg.RESTCaller == nil {
		return snapshotters.SnapshotTarget{
			SnapshotDate: slot,
			BlockTime:    slot,
		}, false, nil
	}

	target := slot.UTC()
	low := h.cfg.GenesisHeight
	if low < 1 {
		low = 1
	}
	blk, _, err := snapshotters.FindBlockAtTime(ctx, h.cfg.RESTCaller, target, low)
	if err != nil {
		return snapshotters.SnapshotTarget{}, false, err
	}
	h.blockCache.Lock()
	h.blockCache.m[key] = cacheEntry{blockHeight: blk, blockTime: slot}
	h.blockCache.Unlock()
	return snapshotters.SnapshotTarget{
		BlockHeight:  blk,
		SnapshotDate: slot,
		BlockTime:    slot,
	}, false, nil
}

// Status returns the current coverage view for /api/snapshotters and
// the dashboard card. Same semantics as provider_rewards.Status —
// expected from cfg.EarliestDate + genesis cadence, partitioned by
// the status column on each row.
func (h *Handler) Status(ctx context.Context, pool *pgxpool.Pool) (snapshotters.Status, error) {
	type row struct {
		date   time.Time
		status string
		errMsg string
	}
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT snapshot_date, status, COALESCE(error, '')
		FROM %s.denom_price_snapshots
		ORDER BY snapshot_date`, h.cfg.Schema))
	if err != nil {
		// Most likely cause: DDL hasn't been applied yet. Return empty
		// so the UI can still render "no snapshots yet".
		return snapshotters.Status{}, nil
	}
	defer rows.Close()
	seen := make(map[string]row)
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.date, &r.status, &r.errMsg); err != nil {
			return snapshotters.Status{}, err
		}
		seen[r.date.UTC().Format("2006-01-02")] = r
	}
	if err := rows.Err(); err != nil {
		return snapshotters.Status{}, err
	}

	expected := provider_rewards.ExpectedDates(h.cfg.GenesisTime, h.cfg.EarliestDate, time.Now().UTC())
	out := snapshotters.Status{
		Expected: make([]string, 0, len(expected)),
		Covered:  make([]string, 0, len(expected)),
		Missing:  make([]string, 0, len(expected)),
		Blocks:   make(map[string]int64),
	}
	for _, d := range expected {
		key := d.Format("2006-01-02")
		out.Expected = append(out.Expected, key)
		r, ok := seen[key]
		switch {
		case !ok:
			out.Missing = append(out.Missing, key)
		case r.status == "ok":
			out.Covered = append(out.Covered, key)
		case r.status == "failed":
			out.Failed = append(out.Failed, snapshotters.FailedDate{Date: key, Error: r.errMsg})
		default:
			out.Failed = append(out.Failed, snapshotters.FailedDate{
				Date:  key,
				Error: fmt.Sprintf("status=%s %s", r.status, r.errMsg),
			})
		}
	}
	return out, nil
}

// denomRow bundles one row of denom_metadata joined with denoms.
// denomID is the FK into denom_prices; coingeckoID is the CoinGecko
// identifier used to fetch the USD price.
type denomRow struct {
	denomID     int32
	coingeckoID string
}

// Snapshot performs one date's price fetch + write cycle. The
// CoinGecko calls are SEQUENTIAL per-date — CoinGecko's free tier
// rate-limits concurrent callers aggressively and the TS reference
// implementation (pricing.ts) also serialises. LAVA goes first
// because the dashboard and downstream aggregates treat its price as
// load-bearing; if LAVA fails the whole date is marked 'failed' and
// the other denoms don't bother running.
func (h *Handler) Snapshot(ctx context.Context, pool *pgxpool.Pool, target snapshotters.SnapshotTarget) error {
	// Only fetch prices for denoms that ACTUALLY APPEAR in
	// provider_rewards at this block. Fetching every mapped denom
	// regardless of relevance burns CoinGecko free-tier quota on 20+
	// useless calls per date (Lava rewards are usually 1-3 distinct
	// denoms). Cuts a 27-date backfill from ~650 to ~40 calls.
	denoms, err := h.selectDenomsForBlock(ctx, pool, target.BlockHeight)
	if err != nil {
		return fmt.Errorf("select denoms: %w", err)
	}
	if len(denoms) == 0 {
		// No reward rows for this date (e.g. pre-provider-rewards
		// block, or every provider returned empty) → no prices to
		// fetch. Record a zero-count row so BlocksDue subtracts this
		// date and move on.
		return h.persistAndRefresh(ctx, pool, target, nil)
	}

	// Order LAVA first — it's the load-bearing denom. If LAVA fails
	// after retries we persist status='failed' and abort the rest of
	// the date. When the date's rewards don't include ulava at all,
	// LAVA is simply not in the list and no special handling kicks in.
	ordered := orderLavaFirst(denoms)

	date := target.SnapshotDate.UTC()
	prices := make([]priceRow, 0, len(ordered))

	for _, d := range ordered {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		price, err := h.gecko.PriceAt(ctx, d.coingeckoID, date)
		if err != nil {
			// LAVA failure is load-bearing — abort the date.
			if d.coingeckoID == lavaCoingeckoID {
				errMsg := fmt.Sprintf("LAVA price fetch failed: %v", err)
				slog.Warn("denom_prices: LAVA fetch failed, marking date failed",
					"snapshotter", Name,
					"date", date.Format("2006-01-02"),
					"err", err)
				return pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
					return h.insertSnapshotRow(ctx, tx, target, 0, "failed", errMsg)
				})
			}
			// Non-LAVA failure: log + skip. The date still succeeds
			// at the snapshot level — downstream JOINs on denom_prices
			// simply won't have a row for this denom on this date.
			slog.Warn("denom_prices: non-LAVA fetch failed, skipping denom",
				"snapshotter", Name,
				"date", date.Format("2006-01-02"),
				"coingecko_id", d.coingeckoID,
				"err", err)
			continue
		}
		prices = append(prices, priceRow{
			denomID:     d.denomID,
			priceUSD:    price,
			coingeckoID: d.coingeckoID,
		})
	}

	return h.persistAndRefresh(ctx, pool, target, prices)
}

// persistAndRefresh commits the snapshot row + per-denom price rows in
// one tx, then refreshes the pricing MV. Shared between the
// zero-denom fast path and the normal Snapshot flow.
func (h *Handler) persistAndRefresh(ctx context.Context, pool *pgxpool.Pool, target snapshotters.SnapshotTarget, prices []priceRow) error {
	date := target.SnapshotDate.UTC()
	copyCols := []string{"snapshot_date", "denom_id", "price_usd", "coingecko_id"}
	rows := make([][]any, 0, len(prices))
	for _, p := range prices {
		priceDec, err := parseDecimal(p.priceUSD)
		if err != nil {
			return fmt.Errorf("parse price %q for %s: %w", p.priceUSD, p.coingeckoID, err)
		}
		rows = append(rows, []any{
			date, p.denomID, priceDec, p.coingeckoID,
		})
	}

	err := pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := h.insertSnapshotRow(ctx, tx, target, len(rows), "ok", ""); err != nil {
			return fmt.Errorf("insert snapshot row: %w", err)
		}
		if len(rows) == 0 {
			return nil
		}
		_, err := tx.CopyFrom(
			ctx,
			pgx.Identifier{h.cfg.Schema, "denom_prices"},
			copyCols,
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			return fmt.Errorf("COPY denom_prices: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	if _, err := pool.Exec(ctx,
		fmt.Sprintf(`REFRESH MATERIALIZED VIEW CONCURRENTLY %s.priced_rewards`, h.cfg.Schema),
	); err != nil {
		slog.Warn("denom_prices: MV refresh failed (non-fatal)",
			"snapshotter", Name, "err", err)
	}
	return nil
}

// priceRow is one (denom, date, price) tuple collected before the
// write tx.
type priceRow struct {
	denomID     int32
	priceUSD    string // decimal string to preserve NUMERIC(40,18) precision
	coingeckoID string
}

// selectDenomsForBlock returns the coingecko-mapped, non-suppressed
// denoms that appear in app.provider_rewards AT THIS block_height.
// Pricing a denom that never shows up in rewards at this date is
// wasted CoinGecko budget — on mainnet the 24-row seed means 20+
// useless per-date lookups otherwise.
func (h *Handler) selectDenomsForBlock(ctx context.Context, pool *pgxpool.Pool, blockHeight int64) ([]denomRow, error) {
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT DISTINCT dm.denom_id, dm.coingecko_id
		FROM %[1]s.provider_rewards pr
		JOIN %[1]s.denom_metadata dm ON dm.denom_id = pr.denom_id
		WHERE pr.block_height = $1
		  AND dm.coingecko_id IS NOT NULL
		  AND dm.suppress = FALSE
		ORDER BY dm.denom_id`, h.cfg.Schema), blockHeight)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]denomRow, 0, 8)
	for rows.Next() {
		var d denomRow
		if err := rows.Scan(&d.denomID, &d.coingeckoID); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// selectDenoms returns the coingecko-mapped, non-suppressed denoms
// from app.denom_metadata. Retained for reference but no longer used
// by Snapshot — the per-block variant above is much cheaper.
func (h *Handler) selectDenoms(ctx context.Context, pool *pgxpool.Pool) ([]denomRow, error) {
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT dm.denom_id, dm.coingecko_id
		FROM %[1]s.denom_metadata dm
		WHERE dm.coingecko_id IS NOT NULL
		  AND dm.suppress = FALSE
		ORDER BY dm.denom_id`, h.cfg.Schema))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]denomRow, 0, 32)
	for rows.Next() {
		var d denomRow
		if err := rows.Scan(&d.denomID, &d.coingeckoID); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// orderLavaFirst returns denoms with LAVA (if present) at position 0.
// Preserves relative order of the rest. Lets Snapshot short-circuit
// on a LAVA failure without iterating every denom first.
func orderLavaFirst(denoms []denomRow) []denomRow {
	out := make([]denomRow, 0, len(denoms))
	var rest []denomRow
	for _, d := range denoms {
		if d.coingeckoID == lavaCoingeckoID {
			out = append(out, d)
			continue
		}
		rest = append(rest, d)
	}
	return append(out, rest...)
}

func (h *Handler) insertSnapshotRow(ctx context.Context, tx pgx.Tx, target snapshotters.SnapshotTarget, denomCount int, status, errMsg string) error {
	var errVal any
	if errMsg != "" {
		errVal = errMsg
	}
	// block_time = scheduled slot; snapshot_date = UTC calendar date of slot.
	_, err := tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.denom_price_snapshots
		  (snapshot_date, block_time, denom_count, status, error)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (snapshot_date) DO UPDATE SET
		  block_time   = EXCLUDED.block_time,
		  snapshot_at  = now(),
		  denom_count  = EXCLUDED.denom_count,
		  status       = EXCLUDED.status,
		  error        = EXCLUDED.error`, h.cfg.Schema),
		target.SnapshotDate.UTC(), target.BlockTime, denomCount, status, errVal)
	return err
}

// parseDecimal turns a decimal string into a value pgx can bind to a
// numeric(40, 18) column. pgx/v5 accepts plain strings for numeric
// bindings, but we validate the shape first so a garbage input
// surfaces as a Go error instead of a pgx parse error at COPY time.
func parseDecimal(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "0", nil
	}
	// Fast-path: pure integer.
	if !strings.ContainsRune(s, '.') {
		if _, ok := new(big.Int).SetString(s, 10); !ok {
			return "", fmt.Errorf("not a decimal: %q", s)
		}
		return s, nil
	}
	// Decimal — split and validate both sides.
	dot := strings.IndexByte(s, '.')
	intPart := s[:dot]
	fracPart := s[dot+1:]
	if intPart == "" || intPart == "-" {
		intPart += "0"
	}
	if _, ok := new(big.Int).SetString(intPart, 10); !ok {
		return "", fmt.Errorf("not a decimal: %q", s)
	}
	for _, r := range fracPart {
		if r < '0' || r > '9' {
			return "", fmt.Errorf("not a decimal: %q", s)
		}
	}
	return intPart + "." + fracPart, nil
}

// ---------------------------------------------------------------------------
// CoinGecko caller
// ---------------------------------------------------------------------------

// CoingeckoCaller is the interface this snapshotter uses to fetch
// historical USD prices. HTTPCaller is the production implementation;
// tests substitute a mock to exercise error classification without a
// live HTTP server.
type CoingeckoCaller interface {
	// PriceAt returns the USD price for `coingeckoID` on `date`. The
	// returned price is a decimal string — zero ("0") is a legitimate
	// "coin not listed / no data at this date" answer and NOT an error.
	// Transient failures (429 after retries, network blip) return an
	// error the caller treats as retry-next-tick.
	PriceAt(ctx context.Context, coingeckoID string, date time.Time) (string, error)
}

// HTTPCaller is the production CoingeckoCaller. Calls
// /coins/{id}/history?date=DD-MM-YYYY via rpc.SharedTransport so the
// snapshotter shares the process-wide HTTP/2 + TLS session cache.
type HTTPCaller struct {
	baseURL string
	headers map[string]string
	http    *http.Client
}

// NewHTTPCaller builds an HTTPCaller pointed at `baseURL` (typically
// https://api.coingecko.com/api/v3 for the free tier, or the Pro URL).
// Extra headers are attached verbatim — useful for the Pro API's
// x-cg-pro-api-key.
func NewHTTPCaller(baseURL string, headers map[string]string) *HTTPCaller {
	return &HTTPCaller{
		baseURL: strings.TrimRight(baseURL, "/"),
		headers: headers,
		http: &http.Client{
			Transport: rpc.SharedTransport,
			Timeout:   30 * time.Second,
		},
	}
}

// PriceAt hits /coins/{id}/history?date=DD-MM-YYYY&localization=false.
// Retries 429 responses (Retry-After honored if present, else
// 2^attempt * 1s with a 60s cap) for up to coingeckoMaxAttempts.
// Treats 404 and 200-with-no-market_data as a zero price (legitimate
// "coin not listed / no data at this date" answer). Other non-2xx
// statuses surface as errors after retries exhaust.
func (c *HTTPCaller) PriceAt(ctx context.Context, coingeckoID string, date time.Time) (string, error) {
	dateParam := formatDateForCoingecko(date)
	// url.PathEscape defends the id segment against any weirdness; the
	// existing denom_metadata seed uses plain ASCII slugs but an
	// operator-added coingecko_id could theoretically contain special
	// characters.
	path := "/coins/" + url.PathEscape(coingeckoID) + "/history"
	q := url.Values{}
	q.Set("date", dateParam)
	q.Set("localization", "false")
	full := c.baseURL + path + "?" + q.Encode()

	for attempt := 1; attempt <= coingeckoMaxAttempts; attempt++ {
		body, status, retryAfter, err := c.doGET(ctx, full)
		if err != nil {
			// Network-level error — back off and retry if we have
			// attempts left.
			if attempt == coingeckoMaxAttempts {
				return "", fmt.Errorf("coingecko %s: %w", coingeckoID, err)
			}
			if !backoffSleep(ctx, attempt, 0) {
				return "", ctx.Err()
			}
			continue
		}
		if status == http.StatusNotFound {
			// Coin not listed at this date — legitimate zero.
			return "0", nil
		}
		if status == http.StatusTooManyRequests {
			if attempt == coingeckoMaxAttempts {
				return "", fmt.Errorf("coingecko %s: %w after %d attempts", coingeckoID, ErrRateLimited, attempt)
			}
			if !backoffSleep(ctx, attempt, retryAfter) {
				return "", ctx.Err()
			}
			continue
		}
		if status < 200 || status >= 300 {
			return "", fmt.Errorf("coingecko %s: http %d", coingeckoID, status)
		}
		return parseHistoryBody(body)
	}
	return "", fmt.Errorf("coingecko %s: exhausted %d attempts", coingeckoID, coingeckoMaxAttempts)
}

// parseHistoryBody extracts the USD price from a /coins/{id}/history
// 200 response. Returns "0" if market_data is missing or if the USD
// current_price isn't present — both legitimate "no data" responses
// per pricing.ts.
func parseHistoryBody(body []byte) (string, error) {
	if len(strings.TrimSpace(string(body))) == 0 {
		// Empty body — treat as no data rather than a hard error; the
		// operator sees a zero row and next tick can re-fetch.
		return "0", nil
	}
	// Decode the USD price as a json.Number so we don't lose precision
	// to float64 — the Postgres numeric(40,18) column can hold far
	// more digits than a float can round-trip, and we want to preserve
	// whatever CoinGecko returned.
	var parsed struct {
		MarketData *struct {
			CurrentPrice map[string]json.Number `json:"current_price"`
		} `json:"market_data"`
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&parsed); err != nil {
		return "", fmt.Errorf("decode history body: %w", err)
	}
	if parsed.MarketData == nil {
		return "0", nil
	}
	raw, ok := parsed.MarketData.CurrentPrice["usd"]
	if !ok {
		return "0", nil
	}
	s := raw.String()
	if s == "" {
		return "0", nil
	}
	return s, nil
}

// maxCoingeckoBodyBytes caps the response body we read per call. The
// /coins/{id}/history response is a few KB; 1 MiB is generous but
// still stops a compromised upstream from OOMing the process.
const maxCoingeckoBodyBytes = 1 << 20

// doGET runs one GET against `url`. Returns the body, status, and the
// retry-after header (0 if absent/unparseable).
func (c *HTTPCaller) doGET(ctx context.Context, fullURL string) ([]byte, int, time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, 0, 0, err
	}
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, 0, err
	}
	defer resp.Body.Close()
	// Always drain the body before returning so the connection goes
	// back to the keep-alive pool. Saves a TCP+TLS handshake on every
	// retry.
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxCoingeckoBodyBytes))
	if err != nil {
		return nil, resp.StatusCode, 0, fmt.Errorf("read body: %w", err)
	}
	var ra time.Duration
	if v := resp.Header.Get("Retry-After"); v != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n > 0 {
			ra = time.Duration(n) * time.Second
		}
	}
	return body, resp.StatusCode, ra, nil
}

// backoffSleep sleeps before the next attempt. If retryAfter is
// non-zero it wins (honoring the server's cooldown); otherwise
// 2^attempt * 1s, capped at coingeckoMaxBackoff — matches pricing.ts.
// Returns false if ctx was cancelled during the sleep.
func backoffSleep(ctx context.Context, attempt int, retryAfter time.Duration) bool {
	var d time.Duration
	if retryAfter > 0 {
		d = retryAfter
	} else {
		// 2^attempt * 1s — attempt starts at 1 so this is 2s, 4s, 8s, …
		d = time.Duration(1<<uint(attempt)) * time.Second
	}
	if d > coingeckoMaxBackoff {
		d = coingeckoMaxBackoff
	}
	tmr := time.NewTimer(d)
	defer tmr.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-tmr.C:
		return true
	}
}

// formatDateForCoingecko turns a Go time.Time into the DD-MM-YYYY
// string the CoinGecko /coins/{id}/history endpoint expects. UTC —
// the snapshot date is always a UTC calendar day.
func formatDateForCoingecko(t time.Time) string {
	t = t.UTC()
	return fmt.Sprintf("%02d-%02d-%04d", t.Day(), int(t.Month()), t.Year())
}

// ---------------------------------------------------------------------------
// Exported sentinels / helpers for tests
// ---------------------------------------------------------------------------

// ErrRateLimited is returned by HTTPCaller.PriceAt when CoinGecko
// returns 429 for every attempt. Exported so tests (and callers that
// want to classify at a glance) can assert on it.
var ErrRateLimited = errors.New("coingecko rate-limited")
