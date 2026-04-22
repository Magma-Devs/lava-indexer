// Package supply implements a Snapshotter that records the chain's
// total `ulava` supply at the same genesis-anchored monthly cadence as
// provider_rewards and denom_prices.
//
// Each snapshot captures one row per (snapshot_date, block_height,
// total_supply). Downstream consumers (e.g. info's /burn-rate endpoint)
// read the table via GraphQL and compute burn = prev_supply - curr_supply
// per month — LAVA has a constant issuance schedule, so month-over-month
// supply deltas expose the effective burn rate.
//
// Shape mirrors denom_prices:
//   - BlocksDue emits the due monthly slots (shared cadence with
//     provider_rewards.ExpectedDates), subtracts already-covered dates
//     from app.supply_snapshots, and binary-searches each slot's block
//     height.
//   - Snapshot hits /cosmos/bank/v1beta1/supply at the target block with
//     `x-cosmos-block-height` pinning, finds the `ulava` entry, and
//     persists the amount as NUMERIC(40, 0).
//
// The only NEW chain call this snapshotter makes is the bank supply
// endpoint — block-time resolution reuses the provider_rewards.RESTCaller
// wired in main.go so both snapshotters share the archive-backed HTTP
// pool + TLS session cache.
package supply

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
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
const Name = "supply"

// lavaDenom is the base denom the chain uses for native LAVA. The
// /cosmos/bank/v1beta1/supply response lists every minted denom; we
// select this one and record its amount.
const lavaDenom = "ulava"

// Config is the subset of cfg.Snapshotters.Supply this package needs.
// Passed explicitly (rather than reaching into the global Config) so the
// snapshotter is straightforward to test — see handler_test.go.
type Config struct {
	Schema       string
	EarliestDate time.Time
	RESTURL      string            // base URL of the REST endpoint
	RESTHeaders  map[string]string // extra headers (e.g. lava-extension: archive)
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

// SupplyCaller is the interface this snapshotter uses to fetch the
// chain's total supply at a pinned block height. HTTPCaller is the
// production implementation; tests substitute a mock.
type SupplyCaller interface {
	// TotalSupply returns one (denom → amount-string) map for
	// blockHeight. Amount strings are base-unit integers ("ulava", not
	// "lava"). Transient failures (5xx, network blip, decode error)
	// return an error the caller persists as status='failed'.
	TotalSupply(ctx context.Context, blockHeight int64) (map[string]string, error)
}

// New returns a Handler wired with the default HTTP bank client. Tests
// use NewWithCaller to swap in a mock.
func New(cfg Config) *Handler {
	return NewWithCaller(cfg, NewHTTPCaller(cfg.RESTURL, cfg.RESTHeaders))
}

// NewWithCaller is the test-friendly constructor. Accepts any
// SupplyCaller so unit tests can exercise response classification
// without a live HTTP server.
func NewWithCaller(cfg Config, caller SupplyCaller) *Handler {
	if cfg.Schema == "" {
		cfg.Schema = "app"
	}
	return &Handler{
		cfg:    cfg,
		caller: caller,
	}
}

// Handler is the Snapshotter implementation for supply.
type Handler struct {
	cfg    Config
	caller SupplyCaller

	// blockCache memoises (date → block height) across runs — matches
	// the provider_rewards / denom_prices cache shape so a re-invoked
	// BlocksDue doesn't re-do the binary search.
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

// RESTURL exposes the base REST endpoint so the dashboard can render it
// alongside the rest of the configuration-dependent state.
func (h *Handler) RESTURL() string { return h.cfg.RESTURL }

// DDL returns the SQL statements that create the supply_snapshots
// table. Idempotent — CREATE IF NOT EXISTS everywhere. No DROP CASCADE:
// the schema is stable from the first deploy, and a DROP here would
// wipe every operator's accumulated monthly data on every restart.
func (h *Handler) DDL() []string {
	return []string{
		fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %[1]s.supply_snapshots (
			  snapshot_date DATE            PRIMARY KEY,
			  block_height  BIGINT          NOT NULL,
			  block_time    TIMESTAMPTZ     NOT NULL,
			  snapshot_at   TIMESTAMPTZ     NOT NULL DEFAULT now(),
			  total_supply  NUMERIC(40, 0)  NOT NULL,
			  status        TEXT            NOT NULL DEFAULT 'ok',
			  error         TEXT
			);`, h.cfg.Schema),
		fmt.Sprintf(`
			CREATE INDEX IF NOT EXISTS idx_supply_snapshots_block_height
			  ON %[1]s.supply_snapshots (block_height);`, h.cfg.Schema),
	}
}

// BlocksDue computes the set of (slot, block) targets that still need a
// supply snapshot. Uses the shared genesis-anchored cadence
// (provider_rewards.ExpectedDates) so all three snapshotters agree on
// which dates are due. Dates already present in supply_snapshots with
// status IN ('ok', 'failed') are subtracted — operators delete a failed
// row to retry it.
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

// coveredDates returns the set of snapshot dates already recorded as
// ok OR failed. Failed dates are treated as covered so BlocksDue doesn't
// re-retry them forever — operators who want to retry delete the row
// manually and the next tick picks it up.
func (h *Handler) coveredDates(ctx context.Context, pool *pgxpool.Pool) (map[string]struct{}, error) {
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT snapshot_date FROM %s.supply_snapshots
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
			BlockTime:    e.blockTime,
		}, true, nil
	}
	h.blockCache.Unlock()

	// Skip block resolution entirely when no RESTCaller is wired (test
	// path). In production RESTCaller is always non-nil; tests
	// construct Handler with a mock that only answers supply calls.
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
	blk, blkTime, err := snapshotters.FindBlockAtTime(ctx, h.cfg.RESTCaller, target, low)
	if err != nil {
		return snapshotters.SnapshotTarget{}, false, err
	}
	h.blockCache.Lock()
	h.blockCache.m[key] = cacheEntry{blockHeight: blk, blockTime: blkTime}
	h.blockCache.Unlock()
	return snapshotters.SnapshotTarget{
		BlockHeight:  blk,
		SnapshotDate: slot,
		BlockTime:    blkTime,
	}, false, nil
}

// Status returns the current coverage view for /api/snapshotters and the
// dashboard card. Same semantics as provider_rewards / denom_prices —
// expected from cfg.EarliestDate + genesis cadence, partitioned by the
// status column on each row.
func (h *Handler) Status(ctx context.Context, pool *pgxpool.Pool) (snapshotters.Status, error) {
	type row struct {
		date   time.Time
		block  int64
		status string
		errMsg string
	}
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT snapshot_date, block_height, status, COALESCE(error, '')
		FROM %s.supply_snapshots
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
		if err := rows.Scan(&r.date, &r.block, &r.status, &r.errMsg); err != nil {
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
		Blocks:   make(map[string]int64, len(seen)),
	}
	for _, d := range expected {
		key := d.Format("2006-01-02")
		out.Expected = append(out.Expected, key)
		r, ok := seen[key]
		if ok {
			out.Blocks[key] = r.block
		}
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

// Snapshot fetches the total supply at `target.BlockHeight`, finds the
// ulava entry, and writes one row into supply_snapshots. Short tx — a
// single INSERT. Errors from the chain (5xx, decode) persist as
// status='failed' so BlocksDue skips the date on subsequent ticks;
// operators delete the failed row to retry. A response that omits ulava
// entirely is also status='failed' with a descriptive error so operators
// notice the misconfiguration rather than silently getting zero.
func (h *Handler) Snapshot(ctx context.Context, pool *pgxpool.Pool, target snapshotters.SnapshotTarget) error {
	supply, err := h.caller.TotalSupply(ctx, target.BlockHeight)
	if err != nil {
		errMsg := fmt.Sprintf("fetch supply: %v", err)
		slog.Warn("supply: fetch failed, marking date failed",
			"snapshotter", Name,
			"date", target.SnapshotDate.UTC().Format("2006-01-02"),
			"block", target.BlockHeight,
			"err", err)
		return pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			return h.insertSnapshotRow(ctx, tx, target, nil, "failed", errMsg)
		})
	}

	amount, ok := supply[lavaDenom]
	if !ok {
		errMsg := fmt.Sprintf("%s missing from supply response (denoms=%d)", lavaDenom, len(supply))
		slog.Warn("supply: ulava missing from response, marking date failed",
			"snapshotter", Name,
			"date", target.SnapshotDate.UTC().Format("2006-01-02"),
			"block", target.BlockHeight)
		return pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			return h.insertSnapshotRow(ctx, tx, target, nil, "failed", errMsg)
		})
	}

	total, ok := new(big.Int).SetString(strings.TrimSpace(amount), 10)
	if !ok {
		errMsg := fmt.Sprintf("parse %s amount %q", lavaDenom, amount)
		slog.Warn("supply: ulava amount not a base-10 integer",
			"snapshotter", Name,
			"date", target.SnapshotDate.UTC().Format("2006-01-02"),
			"block", target.BlockHeight,
			"amount", amount)
		return pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			return h.insertSnapshotRow(ctx, tx, target, nil, "failed", errMsg)
		})
	}

	return pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		return h.insertSnapshotRow(ctx, tx, target, total, "ok", "")
	})
}

func (h *Handler) insertSnapshotRow(ctx context.Context, tx pgx.Tx, target snapshotters.SnapshotTarget, totalSupply *big.Int, status, errMsg string) error {
	var errVal any
	if errMsg != "" {
		errVal = errMsg
	}
	// NUMERIC(40, 0) accepts pgx's big.Int binding directly. For the
	// failure path we persist 0 — the row's status='failed' is the
	// authoritative signal, not the amount. A nil binding would
	// violate the NOT NULL constraint on total_supply.
	supplyVal := totalSupply
	if supplyVal == nil {
		supplyVal = big.NewInt(0)
	}
	_, err := tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.supply_snapshots
		  (snapshot_date, block_height, block_time, total_supply, status, error)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (snapshot_date) DO UPDATE SET
		  block_height = EXCLUDED.block_height,
		  block_time   = EXCLUDED.block_time,
		  snapshot_at  = now(),
		  total_supply = EXCLUDED.total_supply,
		  status       = EXCLUDED.status,
		  error        = EXCLUDED.error`, h.cfg.Schema),
		target.SnapshotDate.UTC(),
		target.BlockHeight,
		target.BlockTime,
		supplyVal,
		status,
		errVal)
	return err
}

// ---------------------------------------------------------------------------
// HTTP caller (production wiring)
// ---------------------------------------------------------------------------

// HTTPCaller is the production SupplyCaller. Calls
// /cosmos/bank/v1beta1/supply via rpc.SharedTransport so the snapshotter
// shares the process-wide HTTP/2 + TLS session cache with the rest of
// the indexer's HTTP work.
type HTTPCaller struct {
	baseURL string
	headers map[string]string
	http    *http.Client
}

// NewHTTPCaller builds an HTTPCaller pointed at `baseURL` (typically the
// chain's LCD REST endpoint). Pass the archive-backed REST URL; the
// caller assumes the endpoint can answer historical heights via the
// `x-cosmos-block-height` header.
//
// Uses rpc.SharedTransport so the snapshotter participates in the same
// HTTP/2 + TLS-session-cache + high-idle-conns connection pool as the
// rest of the indexer.
func NewHTTPCaller(baseURL string, headers map[string]string) *HTTPCaller {
	return &HTTPCaller{
		baseURL: strings.TrimRight(baseURL, "/"),
		headers: headers,
		http: &http.Client{
			Transport: rpc.SharedTransport,
			Timeout:   60 * time.Second,
		},
	}
}

// maxSupplyBodyBytes caps the response body we read per call. The
// /cosmos/bank/v1beta1/supply response lists every minted denom — Lava
// mainnet typically has <32 entries, a few KB total. 4 MiB is generous
// headroom that still bounds a compromised upstream trying to OOM the
// process.
const maxSupplyBodyBytes = 4 << 20

// supplyResp mirrors the Cosmos bank module's /supply JSON shape. The
// chain returns `{"supply":[{"denom":"…","amount":"…"}, ...]}` with
// optional pagination metadata we don't need for a per-height snapshot.
type supplyResp struct {
	Supply []supplyCoin `json:"supply"`
}

type supplyCoin struct {
	Denom  string `json:"denom"`
	Amount string `json:"amount"`
}

// TotalSupply hits /cosmos/bank/v1beta1/supply?pagination.limit=10000
// pinned at blockHeight. Returns a (denom → amount-string) map so
// callers can look up the denom of interest without scanning the slice.
// pagination.limit=10000 is a belt-and-braces guard against a paginated
// response silently omitting ulava if the default page is small — Lava
// mainnet has <32 denoms today, but a chain with many IBC hashes could
// grow past an unspecified default.
func (c *HTTPCaller) TotalSupply(ctx context.Context, blockHeight int64) (map[string]string, error) {
	path := "/cosmos/bank/v1beta1/supply?pagination.limit=10000"
	body, err := c.doGET(ctx, path, blockHeight)
	if err != nil {
		return nil, err
	}
	var parsed supplyResp
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("decode supply body: %w", err)
	}
	out := make(map[string]string, len(parsed.Supply))
	for _, coin := range parsed.Supply {
		out[coin.Denom] = coin.Amount
	}
	return out, nil
}

// doGET runs one GET with the configured headers and a pinned block-
// height header. Reads the body via io.LimitReader + ReadAll before
// inspecting status — draining to EOF even on non-2xx is required for
// Go's net/http to return the connection to the keep-alive pool.
func (c *HTTPCaller) doGET(ctx context.Context, path string, blockHeight int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	if blockHeight > 0 {
		req.Header.Set("x-cosmos-block-height", strconv.FormatInt(blockHeight, 10))
	}
	req.Header.Set("accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxSupplyBodyBytes))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET %s: http %d", path, resp.StatusCode)
	}
	return body, nil
}

// ---------------------------------------------------------------------------
// Exported sentinels for tests
// ---------------------------------------------------------------------------

// ErrUlavaMissing is returned by TotalSupply when a technically-valid
// response omits the ulava entry. Exported so tests and callers that
// want to classify at a glance can assert on it.
//
// Currently unused at the caller level (Snapshot does the absence check
// directly against the returned map, to keep the interface contract
// simple), but retained as a sentinel for future refactors that might
// move the check into the caller.
var ErrUlavaMissing = errors.New("ulava missing from supply response")
