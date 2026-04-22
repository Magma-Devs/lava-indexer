// Package provider_rewards implements a Snapshotter that records the
// Lava chain's estimated_provider_rewards for every registered provider
// at a fixed monthly cadence (the 17th of each month at 15:00 UTC).
//
// Why monthly-17th@15:00: Lava's distribution epoch boundaries land on
// that calendar slot, so snapshotting there gives a consistent
// view of accrued-but-not-yet-claimed rewards. The REST call we make
// pins the block height via the `x-cosmos-block-height` header, so the
// answer is deterministic across re-runs — no drift if the snapshotter
// retries a target hours later.
package provider_rewards

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/magma-devs/lava-indexer/internal/events"
	"github.com/magma-devs/lava-indexer/internal/rpc"
	"github.com/magma-devs/lava-indexer/internal/snapshotters"
	"golang.org/x/sync/errgroup"
)

// Name returned by the Snapshotter interface. Stable identifier — used
// in log lines and on /api/snapshotters.
const Name = "provider_rewards"

// SourceKind is the small-int encoding of the reward source label
// ("Boost", "Pools", "Subscription") we get from the chain. Matches the
// `source_kind` SMALLINT column so the DB stays compact.
type SourceKind int16

const (
	SourceBoost        SourceKind = 0
	SourcePools        SourceKind = 1
	SourceSubscription SourceKind = 2
	// SourceTotal is the aggregate row synthesised from the chain's
	// `total[]` field when `info[]` comes back empty. Observed shape:
	// {"info":[], "total":[{"denom":"ulava","amount":"…"}]} — the
	// chain returns no source-breakdown for the simulated-delegation
	// query we send but still reports an aggregate reward figure.
	// Stored with spec="" (no breakdown available) so downstream
	// consumers can differentiate from source-attributed rows.
	SourceTotal SourceKind = 3
)

// Config is the subset of cfg.Snapshotters.ProviderRewards this package
// needs. Passed explicitly (rather than reaching into the global Config)
// so the snapshotter is straightforward to test — see handler_test.go.
type Config struct {
	Schema       string
	EarliestDate time.Time
	Concurrency  int
	RESTURL      string            // base URL of the REST endpoint
	RESTHeaders  map[string]string // extra headers (e.g. lava-extension: archive)
	// GenesisHeight is the hint for the BlocksDue binary-search lower bound.
	GenesisHeight int64
	// GenesisTime is the chain's genesis timestamp. Used as the anchor
	// for monthly snapshot dates: slots are genesis+1mo, genesis+2mo,
	// etc., preserving the genesis hour/minute/second. Lava mainnet's
	// genesis at 2024-01-17T15:00:00Z produces the monthly-17th@15:00
	// UTC cadence the operator expects.
	GenesisTime time.Time
}

// New returns a Handler wired with a concrete RESTCaller. The RESTCaller
// is what the Snapshot method uses to talk to the chain — tests can
// swap in a mock by calling NewWithCaller directly.
func New(cfg Config) *Handler {
	return NewWithCaller(cfg, NewHTTPCaller(cfg.RESTURL, cfg.RESTHeaders))
}

// NewWithCaller is the test-friendly constructor. Accepts any RESTCaller
// so unit tests can exercise response classification without a live HTTP
// server.
func NewWithCaller(cfg Config, caller RESTCaller) *Handler {
	if cfg.Schema == "" {
		cfg.Schema = "app"
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 25
	}
	return &Handler{
		cfg:       cfg,
		caller:    caller,
		providers: events.NewDict(cfg.Schema, "providers", "addr"),
		chains:    events.NewDict(cfg.Schema, "chains", "name"),
		denoms:    events.NewDict(cfg.Schema, "denoms", "denom"),
	}
}

// Handler is the Snapshotter implementation for provider_rewards.
type Handler struct {
	cfg       Config
	caller    RESTCaller
	providers *events.Dict
	chains    *events.Dict
	// denoms maps RESOLVED denom strings (e.g. "ulava", "uatom") onto
	// int32 FKs. IBC hashes are resolved via the caller's ResolveIBC
	// method BEFORE being looked up here — the dict only ever holds
	// canonical microdenoms, never `ibc/<hash>` entries.
	denoms *events.Dict

	// blockCache memoises (date → block height) across runs so a
	// re-invoked BlocksDue after a DB blip doesn't re-do the binary
	// search from scratch. Keyed by the date's UTC RFC3339 string to
	// avoid time.Time map-key sharp edges.
	blockCache struct {
		sync.Mutex
		m map[string]cacheEntry
	}

	// suppressCache caches the set of RAW denoms whose
	// denom_metadata.suppress column is TRUE. Checked on every
	// per-provider parse so noisy test denoms don't leak into
	// provider_rewards. Refreshed once per snapshot tick — a new
	// entry via operator UPDATE takes effect on the next tick.
	suppressCache struct {
		sync.Mutex
		// loadedAt is zero until the cache has been populated at least
		// once; callers rebuild it on every Snapshot() entry.
		loadedAt time.Time
		set      map[string]struct{}
	}
}

type cacheEntry struct {
	blockHeight int64
	blockTime   time.Time
}

// Name returns the stable identifier.
func (h *Handler) Name() string { return Name }

// RESTURL exposes the base REST endpoint this snapshotter is pointed
// at, so the web UI can render it alongside the rest of the
// configuration-dependent state. Empty when the operator left RESTURL
// blank and the resolver's fallback didn't find a rest-kind endpoint.
func (h *Handler) RESTURL() string { return h.cfg.RESTURL }

// seedDenomMetadata returns the idempotent INSERT statements that seed
// app.denoms + app.denom_metadata with the known microdenom →
// base_denom mapping. Kept alongside the DDL so provider_rewards can
// bootstrap the denom dict standalone — the denom_prices snapshotter
// depends on this data but shouldn't re-seed it (single source of
// truth).
//
// The raw set mirrors pricing.ts DENOM_CONVERSIONS + DENOM_COINGECKO_ID
// verbatim (see /home/bob/projects/info/apps/api/src/rpc/pricing.ts).
// New denoms are added here once; operators can override
// coingecko_id / suppress in the DB with a targeted UPDATE — no
// redeploy needed.
func seedDenomMetadata(schema string) []string {
	type seedEntry struct {
		raw, base, factor, cgid string
	}
	entries := []seedEntry{
		{"ulava", "lava", "1000000", "lava-network"},
		{"uatom", "atom", "1000000", "cosmos"},
		{"uosmo", "osmo", "1000000", "osmosis"},
		{"ujuno", "juno", "1000000", "juno-network"},
		{"ustars", "stars", "1000000", "stargaze"},
		{"uakt", "akt", "1000000", "akash-network"},
		{"uhuahua", "huahua", "1000000", "chihuahua-token"},
		{"uevmos", "evmos", "1000000000000000000", "evmos"},
		{"inj", "inj", "1000000000000000000", "injective-protocol"},
		{"aevmos", "evmos", "1000000000000000000", "evmos"},
		{"basecro", "cro", "100000000", "crypto-com-chain"},
		{"uscrt", "scrt", "1000000", "secret"},
		{"uiris", "iris", "1000000", "iris-network"},
		{"uregen", "regen", "1000000", "regen"},
		{"uion", "ion", "1000000", "ion"},
		{"nanolike", "like", "1000000000", "likecoin"},
		{"uaxl", "axl", "1000000", "axelar"},
		{"uband", "band", "1000000", "band-protocol"},
		{"ubld", "bld", "1000000", "agoric"},
		{"ucmdx", "cmdx", "1000000", "comdex"},
		{"ucre", "cre", "1000000", "crescent-network"},
		{"uxprt", "xprt", "1000000", "persistence"},
		{"uusdc", "usdc", "1000000", "usd-coin"},
		{"unit-move", "move", "10000000", "movement"},
	}
	out := make([]string, 0, 2*len(entries)+2)
	for _, e := range entries {
		out = append(out, fmt.Sprintf(
			`INSERT INTO %[1]s.denoms (denom) VALUES ('%[2]s') ON CONFLICT DO NOTHING;`,
			schema, e.raw))
		out = append(out, fmt.Sprintf(
			`INSERT INTO %[1]s.denom_metadata (denom_id, base_denom, factor, coingecko_id)
			 SELECT id, '%[3]s', %[4]s, '%[5]s' FROM %[1]s.denoms WHERE denom = '%[2]s'
			 ON CONFLICT (denom_id) DO UPDATE
			   SET base_denom = EXCLUDED.base_denom,
			       factor = EXCLUDED.factor,
			       coingecko_id = EXCLUDED.coingecko_id;`,
			schema, e.raw, e.base, e.factor, e.cgid))
	}
	// Test-denom blacklist — observed on Lava mainnet as a rogue
	// ibc/... hash whose `total[]` entries appeared in the rewards
	// simulation. Flagging suppress=TRUE keeps it out of
	// priced_rewards while still letting operators audit what came
	// through via a direct query on app.provider_rewards.
	const testDenom = "ibc/E3FCBEDDBAC500B1BAB90395C7D1E4F33D9B9ECFE82A16ED7D7D141A0152323F"
	out = append(out, fmt.Sprintf(
		`INSERT INTO %[1]s.denoms (denom) VALUES ('%[2]s') ON CONFLICT DO NOTHING;`,
		schema, testDenom))
	out = append(out, fmt.Sprintf(
		`INSERT INTO %[1]s.denom_metadata (denom_id, base_denom, factor, suppress)
		 SELECT id, 'test', 1, TRUE FROM %[1]s.denoms WHERE denom = '%[2]s'
		 ON CONFLICT (denom_id) DO NOTHING;`,
		schema, testDenom))
	return out
}

// DDL returns the SQL statements that create the tables owned by this
// snapshotter, PLUS idempotent CREATE-IF-NOT-EXISTS for the `providers`,
// `chains`, and `denoms` dict tables we FK into. The dict tables are
// conceptually shared with other handlers, but including them here
// (CREATE IF NOT EXISTS) lets the snapshotter start standalone.
//
//   - providers / chains / denoms — dict tables.
//   - denom_metadata — operator-editable overlay (base_denom, factor,
//     coingecko_id, suppress) keyed on denom_id.
//   - provider_rewards_snapshots is one row per (date, block) — the
//     snapshotter's "coverage table". Unique on snapshot_date so a retry
//     of the same date safely upserts.
//   - provider_rewards is the fact table; the composite PK keys on
//     (block_height, provider, spec, source, denom_id, source_denom) so
//     a single tuple lands once per block while preserving the RAW
//     (pre-resolution) denom observed on-chain for audit.
//
// The schema is wiped + recreated (DROP TABLE IF EXISTS) because no
// prod data exists yet and the column layout is changing. Safe to run
// against an empty DB; catastrophic against populated prod — operators
// must re-run the snapshotter to refill.
func (h *Handler) DDL() []string {
	stmts := []string{
		h.providers.DDL(),
		h.chains.DDL(),
		h.denoms.DDL(),
		// denom_metadata: operator-editable overlay keyed on denom_id.
		// Separate from denoms (the pure Dict) so the Dict pattern stays
		// consistent with providers / chains.
		fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %[1]s.denom_metadata (
			  denom_id      INT PRIMARY KEY REFERENCES %[1]s.denoms(id),
			  base_denom    TEXT           NOT NULL,
			  factor        NUMERIC(40, 0) NOT NULL DEFAULT 1000000,
			  coingecko_id  TEXT,
			  suppress      BOOL           NOT NULL DEFAULT FALSE,
			  first_seen_at TIMESTAMPTZ    NOT NULL DEFAULT now(),
			  last_seen_at  TIMESTAMPTZ    NOT NULL DEFAULT now()
			);`, h.cfg.Schema),
	}
	// Seed the known microdenoms + blacklist. Appended after the
	// create-table statements so the INSERTs have targets.
	stmts = append(stmts, seedDenomMetadata(h.cfg.Schema)...)
	// DROP + recreate the fact tables — no prod data to preserve.
	// CASCADE handles the priced_rewards MV defined in the
	// denom_prices snapshotter's DDL.
	stmts = append(stmts,
		fmt.Sprintf(`DROP TABLE IF EXISTS %[1]s.provider_rewards CASCADE;`, h.cfg.Schema),
		fmt.Sprintf(`DROP TABLE IF EXISTS %[1]s.provider_rewards_snapshots CASCADE;`, h.cfg.Schema),
		// Leftover from the previous priced_denoms design — harmless
		// if the table never existed.
		fmt.Sprintf(`DROP TABLE IF EXISTS %[1]s.priced_denoms CASCADE;`, h.cfg.Schema),
		fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %[1]s.provider_rewards_snapshots (
			  block_height   BIGINT       PRIMARY KEY,
			  snapshot_date  DATE         NOT NULL,
			  block_time     TIMESTAMPTZ  NOT NULL,
			  snapshot_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
			  provider_count INT          NOT NULL,
			  status         TEXT         NOT NULL DEFAULT 'ok',
			  error          TEXT
			);`, h.cfg.Schema),
		fmt.Sprintf(`
			CREATE UNIQUE INDEX IF NOT EXISTS uniq_provider_rewards_snapshots_date
			  ON %[1]s.provider_rewards_snapshots (snapshot_date);`, h.cfg.Schema),
		fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %[1]s.provider_rewards (
			  block_height BIGINT         NOT NULL
			    REFERENCES %[1]s.provider_rewards_snapshots(block_height) ON DELETE CASCADE,
			  provider_id  INT            NOT NULL REFERENCES %[1]s.providers(id),
			  spec_id      INT            NOT NULL REFERENCES %[1]s.chains(id),
			  source_kind  SMALLINT       NOT NULL,
			  denom_id     INT            NOT NULL REFERENCES %[1]s.denoms(id),
			  source_denom TEXT           NOT NULL,
			  amount       NUMERIC(40, 0) NOT NULL,
			  PRIMARY KEY (block_height, provider_id, spec_id, source_kind, denom_id, source_denom)
			);`, h.cfg.Schema),
		fmt.Sprintf(`
			CREATE INDEX IF NOT EXISTS idx_provider_rewards_provider_block
			  ON %[1]s.provider_rewards (provider_id, block_height);`, h.cfg.Schema),
	)
	return stmts
}

// Warmup pre-loads the providers, chains, and denoms dictionary caches.
// Non-fatal on error — the Snapshot path tolerates a cold cache.
func (h *Handler) Warmup(ctx context.Context, pool *pgxpool.Pool) error {
	if err := h.providers.Warmup(ctx, pool); err != nil {
		return err
	}
	if err := h.chains.Warmup(ctx, pool); err != nil {
		return err
	}
	return h.denoms.Warmup(ctx, pool)
}

// BlocksDue computes the set of (slot, block) targets that still need
// a snapshot. Slots are genesis-anchored monthly steps (see
// ExpectedDates). Slots whose timestamp is strictly in the past are
// eligible; future slots are skipped until the cadence catches up.
// Dates already present in provider_rewards_snapshots with status='ok'
// or 'failed' are subtracted.
//
// For each still-needed slot we binary-search the chain for the block
// whose timestamp is closest to the slot's wall-clock and cache the
// (date → block) mapping so a rerun of BlocksDue doesn't pay the
// search cost a second time.
func (h *Handler) BlocksDue(ctx context.Context, pool *pgxpool.Pool) ([]snapshotters.SnapshotTarget, error) {
	slots := ExpectedDates(h.cfg.GenesisTime, h.cfg.EarliestDate, time.Now().UTC())
	if len(slots) == 0 {
		return nil, nil
	}

	// Subtract dates already covered successfully.
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
// ok OR failed. Failed dates are treated as covered so BlocksDue
// doesn't re-retry them forever — operators who want to retry a
// failed date delete the row manually and the next tick picks it up.
func (h *Handler) coveredDates(ctx context.Context, pool *pgxpool.Pool) (map[string]struct{}, error) {
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT snapshot_date FROM %s.provider_rewards_snapshots
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
	// Cache key = calendar date; the slot itself is deterministic from
	// the genesis anchor (one slot per month) so a per-date key is
	// sufficient and matches how the DB stores snapshot_date.
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

	target := slot.UTC()
	low := h.cfg.GenesisHeight
	if low < 1 {
		low = 1
	}
	blk, blkTime, err := snapshotters.FindBlockAtTime(ctx, h.caller, target, low)
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

// Status returns the current coverage view for /api/snapshotters and
// the dashboard card. Uses h.cfg.EarliestDate as the authoritative
// "expected from" date (the config is the operator's declaration of
// what should be snapshotted). Partitions into Covered/Missing/Failed
// by reading each row's status column. Returns an empty Status when
// the snapshots table hasn't been created yet (pre-DDL or
// configuration skip).
func (h *Handler) Status(ctx context.Context, pool *pgxpool.Pool) (snapshotters.Status, error) {
	type row struct {
		date   time.Time
		block  int64
		status string
		errMsg string
	}
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT snapshot_date, block_height, status, COALESCE(error, '')
		FROM %s.provider_rewards_snapshots
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

	expected := ExpectedDates(h.cfg.GenesisTime, h.cfg.EarliestDate, time.Now().UTC())
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
			// 'partial' or any future state we haven't enumerated —
			// surface it as failed-with-status so operators notice.
			out.Failed = append(out.Failed, snapshotters.FailedDate{
				Date:  key,
				Error: fmt.Sprintf("status=%s %s", r.status, r.errMsg),
			})
		}
	}
	return out, nil
}

// ExpectedDates returns the monthly snapshot slots anchored on the
// chain's genesis timestamp. Slot N = genesis + N months, preserving
// the genesis day-of-month and wall-clock time. Slots earlier than
// earliestDate are filtered out (operator floor); slots in the future
// (genesis+N > now) are not yet eligible.
//
// For Lava mainnet (genesis 2024-01-17T15:00:00Z) this produces
// 2024-02-17T15:00:00Z, 2024-03-17T15:00:00Z, … — the same cadence
// a hardcoded monthly-17th@15:00 would, but correct for any chain
// without hardcoded magic numbers.
//
// Exported for tests, for the Status() path, AND for the sibling
// denom_prices snapshotter which shares this cadence.
func ExpectedDates(genesis, earliest, now time.Time) []time.Time {
	if genesis.IsZero() {
		return nil
	}
	g := genesis.UTC()
	e := earliest.UTC()
	var out []time.Time
	for n := 1; ; n++ {
		slot := g.AddDate(0, n, 0)
		if slot.After(now) {
			return out
		}
		if slot.Before(e) {
			continue
		}
		out = append(out, slot)
	}
}

// minSuccessFraction is the upper bound on the fraction of providers
// that returned a REAL fetch failure (not no-rewards, not state-
// pruned) before we mark a snapshot as status='failed'. Defined
// indirectly: if failed/total > (1 - minSuccessFraction), fail.
//
// Chosen at 0.8 (so up to 20% real failures tolerated) because most
// per-provider errors on historical dates are archive-state
// unavailability (outcomeStatePruned) or chain-authoritative "not
// staked" (outcomeNoRewards) — genuine 5xx / decode failures are
// rare once those are removed from the failure count.
const minSuccessFraction = 0.8

// Snapshot runs one full snapshot for `target`. The HTTP fan-out
// happens OUTSIDE any transaction — critical because a snapshot can
// take several minutes at 25-way concurrency × 10 retries. Per-date
// flow:
//
//  1. Select providers (no tx).
//  2. Fan out per-provider EstimatedRewards calls (no tx). Carries
//     RAW denoms back.
//  3. Refresh suppressed-denom cache (one pool read).
//  4. Collect distinct raw denoms; for each `ibc/<hash>` call
//     ResolveIBC; build raw→resolved map. ibc/ entries are never
//     written to app.denoms.
//  5. Short tx: providers.IDs + chains.IDs + denoms.IDs(RESOLVED).
//  6. Short write tx: insertSnapshotRow + CopyFrom.
//
// Per-provider failures are tolerated up to (1 - minSuccessFraction)
// of the provider set. Above that threshold the snapshot row is
// persisted with status='failed' so BlocksDue doesn't re-retry
// indefinitely, and the UI surfaces the partial-coverage state.
// Below the threshold (i.e. most providers succeeded), the snapshot
// is persisted as 'ok' with only the successful providers' rows and
// a log line listing the failed providers for operator investigation.
func (h *Handler) Snapshot(ctx context.Context, pool *pgxpool.Pool, target snapshotters.SnapshotTarget) error {
	// 1. Enumerate known providers (pool query, no tx — a one-shot read
	// doesn't need tx isolation). We use the committed app.providers set
	// so coverage tracks whatever handlers the operator has run so far —
	// in practice that's lava_relay_payment.
	providers, err := h.selectProviders(ctx, pool)
	if err != nil {
		return fmt.Errorf("select providers: %w", err)
	}
	if len(providers) == 0 {
		// Empty snapshot is legitimate — no providers means no rows to
		// write, and the snapshots row records provider_count=0. Open a
		// short tx just for the upsert.
		return pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			return h.insertSnapshotRow(ctx, tx, target, 0, "ok", "")
		})
	}

	// Refresh the suppress cache once per tick. Operators UPDATE
	// denom_metadata.suppress out-of-band to blacklist a denom; the
	// next tick picks the change up.
	if err := h.refreshSuppressCache(ctx, pool); err != nil {
		slog.Warn("snapshot: load suppress cache failed (continuing with stale)",
			"snapshotter", Name, "err", err)
	}

	// 2. Fetch estimated rewards for every provider in a concurrency-
	// bounded errgroup — OUTSIDE any tx. Per-provider errors are
	// tolerated; only ctx cancellation aborts the fan-out.
	results, err := h.fetchAll(ctx, providers, target.BlockHeight)
	if err != nil {
		return fmt.Errorf("fetch rewards: %w", err)
	}

	// 2a. Partition by outcome. Only real fetch failures count toward
	// the threshold; chain-authoritative "no rewards" and archive-
	// state-pruned responses are accepted at face value.
	var fetched, noRewards, statePruned, failed int
	var firstFailReason error
	for _, r := range results {
		switch r.outcome {
		case outcomeFetched:
			fetched++
		case outcomeNoRewards:
			noRewards++
		case outcomeStatePruned:
			statePruned++
		case outcomeFailed:
			failed++
			if firstFailReason == nil {
				firstFailReason = r.reason
			}
		}
	}
	slog.Info("snapshot fetch complete",
		"snapshotter", Name,
		"date", target.SnapshotDate.UTC().Format("2006-01-02"),
		"block", target.BlockHeight,
		"total", len(providers),
		"fetched", fetched,
		"no_rewards", noRewards,
		"state_pruned", statePruned,
		"failed", failed)
	if failed > 0 {
		slog.Warn("snapshot had real fetch failures",
			"snapshotter", Name,
			"date", target.SnapshotDate.UTC().Format("2006-01-02"),
			"failed", failed,
			"first_err", firstFailReason)
	}
	// Threshold applies ONLY to real fetch failures against total.
	// A date where the archive pruned state for most providers is
	// still a legitimate snapshot of the subset we DID get — we
	// wouldn't want to re-try it indefinitely because retries won't
	// materialise new state.
	if len(providers) > 0 && float64(failed)/float64(len(providers)) > (1-minSuccessFraction) {
		errMsg := fmt.Sprintf("%d/%d providers failed to fetch: %v",
			failed, len(providers), firstFailReason)
		return pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			return h.insertSnapshotRow(ctx, tx, target, fetched, "failed", errMsg)
		})
	}

	// 3. Drop suppressed denoms before resolution — runs off the raw
	// denom so an operator blacklist for `ibc/<hash>` works even if
	// the trace lookup fails.
	h.dropSuppressed(results)

	// 4. Collect unique raw denoms, resolve IBC hashes. `rawToResolved`
	// maps every raw denom we saw to its canonical form; the denom
	// dict IDs are looked up on the RESOLVED values so `ibc/…` entries
	// never land in app.denoms.
	rawToResolved, err := h.resolveDenoms(ctx, results)
	if err != nil {
		// IBC resolution failure is tolerated with a warning — the
		// caller's fetchAll already handled transient HTTP issues
		// generally, so a hard error here means resolveDenoms hit a
		// bug. Persist what we have as 'failed' rather than crash
		// the snapshotter.
		errMsg := fmt.Sprintf("resolve denoms: %v", err)
		return pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			return h.insertSnapshotRow(ctx, tx, target, fetched, "failed", errMsg)
		})
	}

	// 5. Resolve provider + spec + denom IDs in their own short tx.
	// Dict caches are commit-only, so these writes commit safely
	// independent of the final write tx. Only outcomeFetched rows
	// contribute entries; noRewards/failed/suppressed providers don't.
	provSet := make(map[string]struct{})
	specSet := make(map[string]struct{})
	denomSet := make(map[string]struct{})
	for _, r := range results {
		if r.outcome != outcomeFetched {
			continue
		}
		provSet[r.provider] = struct{}{}
		for _, entry := range r.entries {
			specSet[entry.Spec] = struct{}{}
			for _, amt := range entry.Amounts {
				resolved := rawToResolved[amt.Denom]
				if resolved == "" {
					// Resolution failed (e.g. ibc/ 5xx that exhausted
					// retries). Skip the entry; it will be retried on
					// the next snapshot.
					continue
				}
				denomSet[resolved] = struct{}{}
			}
		}
	}
	provList := collectKeys(provSet)
	specList := collectKeys(specSet)
	denomList := collectKeys(denomSet)

	var provIDs, specIDs, denomIDs map[string]int32
	err = pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		var e error
		provIDs, e = h.providers.IDs(ctx, tx, provList)
		if e != nil {
			return fmt.Errorf("provider ids: %w", e)
		}
		specIDs, e = h.chains.IDs(ctx, tx, specList)
		if e != nil {
			return fmt.Errorf("spec ids: %w", e)
		}
		denomIDs, e = h.denoms.IDs(ctx, tx, denomList)
		if e != nil {
			return fmt.Errorf("denom ids: %w", e)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// 6. Build the fact rows in memory (no DB, no tx). Every row is
	// unique by construction (composite PK); dedup as a safety net.
	const (
		iBlockHeight = iota
		iProviderID
		iSpecID
		iSourceKind
		iDenomID
		iSourceDenom
		iAmount
	)
	type rowKey struct {
		providerID  int32
		specID      int32
		sourceKind  int16
		denomID     int32
		sourceDenom string
	}
	seen := make(map[rowKey]struct{})
	rows := make([][]any, 0, 256)
	provWithRewards := 0
	for _, r := range results {
		if r.outcome != outcomeFetched {
			continue
		}
		if len(r.entries) > 0 {
			provWithRewards++
		}
		provID, ok := provIDs[r.provider]
		if !ok {
			return fmt.Errorf("provider id missing for %s", r.provider)
		}
		for _, entry := range r.entries {
			specID, ok := specIDs[entry.Spec]
			if !ok {
				return fmt.Errorf("spec id missing for %s", entry.Spec)
			}
			for _, amt := range entry.Amounts {
				resolved := rawToResolved[amt.Denom]
				if resolved == "" {
					continue
				}
				denomID, ok := denomIDs[resolved]
				if !ok {
					return fmt.Errorf("denom id missing for resolved=%s (raw=%s)", resolved, amt.Denom)
				}
				amount, ok := new(big.Int).SetString(amt.Amount, 10)
				if !ok {
					return fmt.Errorf("parse amount %q for %s/%s", amt.Amount, r.provider, entry.Spec)
				}
				k := rowKey{
					providerID:  provID,
					specID:      specID,
					sourceKind:  int16(entry.SourceKind),
					denomID:     denomID,
					sourceDenom: amt.Denom,
				}
				if _, dup := seen[k]; dup {
					continue
				}
				seen[k] = struct{}{}
				row := make([]any, 7)
				row[iBlockHeight] = target.BlockHeight
				row[iProviderID] = provID
				row[iSpecID] = specID
				row[iSourceKind] = int16(entry.SourceKind)
				row[iDenomID] = denomID
				row[iSourceDenom] = amt.Denom
				row[iAmount] = amount
				rows = append(rows, row)
			}
		}
	}
	copyCols := []string{"block_height", "provider_id", "spec_id", "source_kind", "denom_id", "source_denom", "amount"}

	// 7. Open the write tx: insert the snapshots row (parent FK) then
	// CopyFrom the fact rows. Short tx — a few milliseconds of DB work,
	// no HTTP inside. On failure the whole write tx rolls back and the
	// next tick re-tries the date.
	return pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := h.insertSnapshotRow(ctx, tx, target, provWithRewards, "ok", ""); err != nil {
			return fmt.Errorf("insert snapshot row: %w", err)
		}
		if len(rows) == 0 {
			return nil
		}
		_, err := tx.CopyFrom(
			ctx,
			pgx.Identifier{h.cfg.Schema, "provider_rewards"},
			copyCols,
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			return fmt.Errorf("COPY provider_rewards: %w", err)
		}
		return nil
	})
}

// collectKeys is a tiny helper for deterministic list order out of a
// set. Used for dict ID lookups — the underlying IDs() call dedups
// internally, but providing a deduped slice keeps the wire message
// smaller.
func collectKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	return out
}

// refreshSuppressCache loads the set of RAW denoms whose
// denom_metadata.suppress column is TRUE. Joined across denom_metadata
// + denoms so the result keys off the raw on-chain form (what
// per-provider fetches surface, before IBC resolution). Runs once per
// Snapshot() call.
func (h *Handler) refreshSuppressCache(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT d.denom
		FROM %[1]s.denom_metadata dm
		JOIN %[1]s.denoms d ON d.id = dm.denom_id
		WHERE dm.suppress = TRUE`, h.cfg.Schema))
	if err != nil {
		return err
	}
	defer rows.Close()
	set := make(map[string]struct{})
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return err
		}
		set[s] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	h.suppressCache.Lock()
	h.suppressCache.set = set
	h.suppressCache.loadedAt = time.Now()
	h.suppressCache.Unlock()
	return nil
}

// dropSuppressed mutates `results` in-place, stripping any Amount
// entries whose RAW denom is in the suppress cache. Operates before
// IBC resolution so an operator can blacklist an `ibc/<hash>` without
// needing the trace lookup to succeed first.
func (h *Handler) dropSuppressed(results []fetchResult) {
	h.suppressCache.Lock()
	set := h.suppressCache.set
	h.suppressCache.Unlock()
	if len(set) == 0 {
		return
	}
	for i := range results {
		if results[i].outcome != outcomeFetched {
			continue
		}
		for j := range results[i].entries {
			filtered := results[i].entries[j].Amounts[:0]
			for _, amt := range results[i].entries[j].Amounts {
				if _, bad := set[amt.Denom]; bad {
					continue
				}
				filtered = append(filtered, amt)
			}
			results[i].entries[j].Amounts = filtered
		}
	}
}

// resolveDenoms builds a raw→resolved map for every distinct denom in
// `results`. Non-IBC denoms pass through unchanged (their on-chain form
// IS the dict key). `ibc/<hash>` entries are resolved via the caller's
// ResolveIBC — permanent failures (404) map to "" (caller skips the
// row); transient failures return an error and abort the snapshot.
func (h *Handler) resolveDenoms(ctx context.Context, results []fetchResult) (map[string]string, error) {
	raws := make(map[string]struct{})
	for _, r := range results {
		if r.outcome != outcomeFetched {
			continue
		}
		for _, entry := range r.entries {
			for _, amt := range entry.Amounts {
				raws[amt.Denom] = struct{}{}
			}
		}
	}
	out := make(map[string]string, len(raws))
	for raw := range raws {
		if !strings.HasPrefix(raw, "ibc/") {
			out[raw] = raw
			continue
		}
		hash := strings.TrimPrefix(raw, "ibc/")
		if hash == "" {
			// Malformed — skip (empty-string outcome means the caller
			// drops this amt from the row set).
			out[raw] = ""
			continue
		}
		base, err := h.caller.ResolveIBC(ctx, hash)
		if err != nil {
			if errors.Is(err, errIBCNotFound) {
				// Permanent — record empty so the caller skips. We
				// don't want to abort the whole snapshot for a
				// malformed hash.
				slog.Warn("ibc trace not found, dropping amount",
					"snapshotter", Name, "raw", raw)
				out[raw] = ""
				continue
			}
			// Transient — abort so the next tick retries.
			return nil, fmt.Errorf("resolve ibc %s: %w", raw, err)
		}
		if base == "" {
			out[raw] = ""
			continue
		}
		out[raw] = base
	}
	return out, nil
}

func (h *Handler) selectProviders(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	rows, err := pool.Query(ctx, fmt.Sprintf(`SELECT addr FROM %s.providers ORDER BY id`, h.cfg.Schema))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0, 256)
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func (h *Handler) insertSnapshotRow(ctx context.Context, tx pgx.Tx, target snapshotters.SnapshotTarget, providerCount int, status, errMsg string) error {
	var errVal any
	if errMsg != "" {
		errVal = errMsg
	}
	_, err := tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.provider_rewards_snapshots
		  (block_height, snapshot_date, block_time, provider_count, status, error)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (block_height) DO UPDATE SET
		  snapshot_date  = EXCLUDED.snapshot_date,
		  block_time     = EXCLUDED.block_time,
		  snapshot_at    = now(),
		  provider_count = EXCLUDED.provider_count,
		  status         = EXCLUDED.status,
		  error          = EXCLUDED.error`, h.cfg.Schema),
		target.BlockHeight, target.SnapshotDate, target.BlockTime,
		providerCount, status, errVal)
	return err
}

// providerOutcome distinguishes four shapes the per-provider fetch
// can take. Only outcomeFailed counts toward the snapshot-level
// threshold — the others are chain- or archive-state answers we
// accept at face value.
type providerOutcome int

const (
	outcomeFetched     providerOutcome = iota // got a clean response (possibly empty info[])
	outcomeNoRewards                          // chain said "no rewards for this provider at this block"
	outcomeStatePruned                        // archive state for this height pruned on every retried replica
	outcomeFailed                             // real fetch failure — HTTP 5xx / decode / retry exhausted
)

// fetchResult bundles one provider's parsed chain response. When
// outcome == outcomeFetched, entries is the (possibly empty) reward
// list. When outcome == outcomeNoRewards or outcomeFailed, reason
// carries the underlying message for logs; entries is empty.
type fetchResult struct {
	provider string
	outcome  providerOutcome
	entries  []RewardEntry
	reason   error // wrapped chain message or HTTP error, for logs
}

// fetchAll returns one entry per provider in the same order. Successful
// calls set entries (possibly empty — "no accrued rewards" is a valid
// response). Failed calls set err. A single ctx cancellation (e.g.
// shutdown) terminates the whole fan-out and returns ctx.Err().
//
// Per-provider errors DO NOT cancel siblings — unlike first-error-wins,
// the real Lava LB returns 501 / empty-body for a consistent ~1–5% of
// queries regardless of retries, and aborting the whole snapshot on
// one bad provider means no date ever completes. The caller decides
// whether enough providers succeeded to persist the snapshot.
func (h *Handler) fetchAll(ctx context.Context, providers []string, blockHeight int64) ([]fetchResult, error) {
	g, gctx := errgroup.WithContext(ctx)
	workers := h.cfg.Concurrency
	if workers <= 0 {
		workers = 25
	}
	g.SetLimit(workers)

	out := make([]fetchResult, len(providers))
	for i, addr := range providers {
		g.Go(func() error {
			if gctx.Err() != nil {
				return nil
			}
			entries, err := h.caller.EstimatedRewards(gctx, addr, blockHeight)
			switch {
			case err == nil:
				out[i] = fetchResult{provider: addr, outcome: outcomeFetched, entries: entries}
			case errors.Is(err, errNoRewards):
				out[i] = fetchResult{provider: addr, outcome: outcomeNoRewards, reason: err}
			case errors.Is(err, errStatePruned):
				out[i] = fetchResult{provider: addr, outcome: outcomeStatePruned, reason: err}
			default:
				out[i] = fetchResult{provider: addr, outcome: outcomeFailed, reason: err}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// REST caller + response parsing
// ---------------------------------------------------------------------------

// RESTCaller is the interface a snapshotter uses to talk to the chain.
// The production implementation (HTTPCaller) hits the Lava LCD; tests
// substitute a mock.
type RESTCaller interface {
	// EstimatedRewards returns the parsed `info` entries for `addr` at
	// `blockHeight`. An empty slice means "no accrued rewards"; an error
	// means the call exhausted retries.
	EstimatedRewards(ctx context.Context, addr string, blockHeight int64) ([]RewardEntry, error)

	// BlockTime / Tip satisfy snapshotters.BlockTimeLookup so the same
	// caller can drive the binary search used by BlocksDue.
	BlockTime(ctx context.Context, height int64) (time.Time, error)
	Tip(ctx context.Context) (int64, error)

	// ResolveIBC returns the base_denom for an IBC hash (the trailing
	// portion of an `ibc/<hash>` denom string). 404 responses surface
	// as errIBCNotFound (permanent) so the caller can drop just that
	// amount; 5xx / network errors surface as other errors (transient).
	ResolveIBC(ctx context.Context, hash string) (string, error)
}

// RewardEntry is one parsed `info[]` element from the estimated-rewards
// response. Spec + SourceKind come from splitting the chain's `source`
// field on ": " (e.g. "Boost: ETH1" → SourceBoost + "ETH1").
type RewardEntry struct {
	SourceKind SourceKind
	Spec       string
	Amounts    []RewardAmount
}

// RewardAmount is one (denom, amount) pair inside a RewardEntry.
type RewardAmount struct {
	Denom  string
	Amount string // base-unit integer as returned; parsed as big.Int on insert
}

// estimatedRewardsResp mirrors the chain's JSON shape so decoding stays
// declarative. Most observed responses populate `total[]` (aggregate
// reward for the simulated delegation) with `info[]` empty; the spec
// originally assumed `info[]` would carry the source breakdown but in
// practice the chain only populates it for specific query shapes. We
// parse both and prefer `info[]` when present, falling back to a
// single synthesised entry from `total[]` so the snapshot captures
// the reward amount rather than silently dropping it.
type estimatedRewardsResp struct {
	Info    []rawInfo       `json:"info"`
	Total   []rawCoinAmount `json:"total"`
	Success *bool           `json:"success,omitempty"`
	Message string          `json:"message,omitempty"`
}

type rawInfo struct {
	Source string          `json:"source"`
	Amount []rawCoinAmount `json:"amount"`
}

type rawCoinAmount struct {
	Denom  string `json:"denom"`
	Amount string `json:"amount"`
}

// parseSource splits "Boost: ETH1" into (SourceBoost, "ETH1"). Unknown
// labels default to SourceSubscription, which is the category the chain
// uses for anything that isn't explicitly boost/pool.
func parseSource(s string) (SourceKind, string) {
	idx := strings.Index(s, ": ")
	if idx < 0 {
		return SourceSubscription, s
	}
	label := s[:idx]
	spec := s[idx+2:]
	switch label {
	case "Boost":
		return SourceBoost, spec
	case "Pools":
		return SourcePools, spec
	case "Subscription":
		return SourceSubscription, spec
	default:
		return SourceSubscription, spec
	}
}

// ParseEstimatedRewards decodes the chain's raw JSON body and returns
// either the reward entries or a typed error indicating the
// application-level failure mode (pruning replica, no accrued rewards,
// opaque upstream failure). Exported for tests.
func ParseEstimatedRewards(body []byte) ([]RewardEntry, error) {
	// Empty body with 200 OK — gateway hiccup. Observed in the wild
	// on Lava's public REST LB; treat as pruned-retry so the fresh
	// connection / replica rotation has a shot.
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, fmt.Errorf("%w: empty body", errRetryPruned)
	}
	var r estimatedRewardsResp
	if err := json.Unmarshal(body, &r); err != nil {
		// Truncated / malformed JSON is almost always a gateway issue.
		// Classify as pruned-retry so a fresh connection has a chance.
		return nil, fmt.Errorf("%w: decode: %v", errRetryPruned, err)
	}
	if r.Success != nil && !*r.Success {
		return nil, classifyChainMessage(r.Message)
	}
	if len(r.Info) > 0 {
		entries := make([]RewardEntry, 0, len(r.Info))
		for _, raw := range r.Info {
			kind, spec := parseSource(raw.Source)
			amounts, err := parseAmounts(raw.Amount, spec)
			if err != nil {
				return nil, err
			}
			entries = append(entries, RewardEntry{SourceKind: kind, Spec: spec, Amounts: amounts})
		}
		return entries, nil
	}
	// info[] empty — fall back to total[] if present. A single entry
	// with SourceTotal captures the aggregate so downstream consumers
	// at least know the per-provider reward figure, even without the
	// source breakdown the chain withheld for this query shape.
	if len(r.Total) > 0 {
		amounts, err := parseAmounts(r.Total, "")
		if err != nil {
			return nil, err
		}
		return []RewardEntry{{SourceKind: SourceTotal, Spec: "", Amounts: amounts}}, nil
	}
	return nil, nil
}

// parseAmounts cleans the chain's decimal-string amounts down to
// base-unit integers. Extracted so both info[]- and total[]-shaped
// responses reuse the same logic.
func parseAmounts(raw []rawCoinAmount, spec string) ([]RewardAmount, error) {
	out := make([]RewardAmount, 0, len(raw))
	for _, a := range raw {
		cleaned, ok := cleanIntegerString(a.Amount)
		if !ok {
			return nil, fmt.Errorf("non-integer amount %q (spec=%s denom=%s)", a.Amount, spec, a.Denom)
		}
		out = append(out, RewardAmount{Denom: a.Denom, Amount: cleaned})
	}
	return out, nil
}

// cleanIntegerString accepts either a plain integer string or a decimal
// with all-zero fractional digits. Returns the integer part.
func cleanIntegerString(s string) (string, bool) {
	// Handle pure integers first — fastest path.
	if !strings.ContainsRune(s, '.') {
		// Validate digits only (may start with '-').
		idx := 0
		if len(s) > 0 && s[0] == '-' {
			idx = 1
		}
		if idx == len(s) {
			return "", false
		}
		for i := idx; i < len(s); i++ {
			if s[i] < '0' || s[i] > '9' {
				return "", false
			}
		}
		return s, true
	}
	dot := strings.IndexByte(s, '.')
	intPart := s[:dot]
	frac := s[dot+1:]
	for _, r := range frac {
		if r != '0' {
			return "", false
		}
	}
	// Empty int part (e.g. ".000") — treat as zero.
	if intPart == "" || intPart == "-" {
		return "0", true
	}
	return intPart, true
}

// ---------------------------------------------------------------------------
// Error classification
// ---------------------------------------------------------------------------

// errRetryPruned indicates the endpoint looks like a pruning replica
// (or the handler isn't registered at the queried height). Retry on a
// fresh connection — Lava's LB rotates between replicas, so a second
// try often lands on one that can answer.
var errRetryPruned = errors.New("replica pruned; retry")

// errNoRewards indicates the chain authoritatively returned "no
// rewards to compute for this provider at this block" — the provider
// wasn't staked, the delegation state isn't present, rewards already
// distributed, etc. NOT an error at the snapshotter level; treated as
// an empty info[]. The underlying chain message is carried in the
// wrapped error so logs show the specific reason per provider.
//
// We classify ALL unrecognised success:false responses as no-rewards
// rather than "transient retry", because success:false from a working
// replica is a deterministic answer — retrying 3× won't change it and
// only makes "not staked at this height" look like a real fetch
// failure to the operator.
var errNoRewards = errors.New("chain reports no rewards for provider")

// errStatePruned indicates we exhausted pruned-retry attempts and
// every replica we hit had state pruned at the queried height. Not a
// fetch failure on our side — it's archive unavailability. The REST
// gateway's LB rotates through replicas with different pruning depths,
// and for old dates no subset of replicas reliably holds state. These
// providers are neither "fetched" nor a real failure to investigate;
// they're just "state unavailable" for this (provider, height) pair.
var errStatePruned = errors.New("state pruned at queried height")

// errIBCNotFound is a permanent 404 from the IBC /denom_traces
// endpoint. Caller drops the amount rather than aborting the snapshot.
var errIBCNotFound = errors.New("ibc trace 404 (permanent)")

func classifyChainMessage(msg string) error {
	low := strings.ToLower(msg)
	// Patterns that indicate the REPLICA (not the chain state) is the
	// problem — the handler isn't registered, or the replica is at a
	// different chain version. Retry on a fresh connection.
	switch {
	case strings.Contains(low, "version does not exist"),
		strings.Contains(low, "version mismatch"),
		strings.Contains(low, "no commit info found"),
		strings.Contains(low, "not implemented"):
		return fmt.Errorf("%w: %s", errRetryPruned, msg)
	}
	// Every other success:false is a chain-authoritative "nothing to
	// compute for this (provider, block) pair" — provider not staked,
	// delegation absent, rewards already distributed, etc. Treat as
	// empty rewards; the caller can log the specific message so
	// operators can audit why a provider showed empty.
	return fmt.Errorf("%w: %s", errNoRewards, msg)
}

// ---------------------------------------------------------------------------
// HTTP caller (production wiring)
// ---------------------------------------------------------------------------

// maxPrunedRetries caps the per-provider retries when the replica looks
// pruned. 10 is aggressive — pruning replicas usually rotate inside 2-3
// seconds on Lava's public gateway — but the worst-case payoff is a few
// seconds per stuck provider, bounded.
const maxPrunedRetries = 10

// HTTPCaller is the production RESTCaller. Calls the chain via a plain
// http.Client sharing keep-alive + TLS session cache with the rest of
// the indexer's HTTP work.
type HTTPCaller struct {
	baseURL string
	headers map[string]string
	http    *http.Client

	// ibcCache memoises denom_traces lookups. IBC traces are immutable
	// once issued, so a resolved hash never changes; we keep the
	// cache forever. Population is lazy — the first query for each
	// hash pays one round-trip, every subsequent one is a map hit.
	ibcCache struct {
		sync.RWMutex
		m map[string]string
	}
}

// NewHTTPCaller builds an HTTPCaller. Pass the archive-backed REST URL;
// the caller assumes the endpoint can answer historical heights via the
// `x-cosmos-block-height` header.
//
// Uses rpc.SharedTransport so the snapshotter participates in the same
// HTTP/2 + TLS-session-cache + high-idle-conns connection pool as the
// rest of the indexer. Without this, http.DefaultTransport's
// MaxIdleConnsPerHost=2 would force 23 of 25 concurrent workers to pay
// a fresh TCP+TLS handshake per request (~50-150 ms each), compounded
// on every pruned-replica retry (501 is the expected signal, not the
// exception).
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

// estimatedRewardsDelegatorAmount is the amount_delegator path
// segment on the EstimatedProviderRewards REST endpoint. 10^10 ulava
// (= 10 LAVA) — per operator guidance, this is the amount downstream
// consumers expect; a trivially small amount like 1ulava returns
// technically-valid-but-not-useful estimates.
const estimatedRewardsDelegatorAmount = "10000000000ulava"

// EstimatedRewards hits the Lava subscription module's estimated-
// rewards endpoint pinned at blockHeight. The gRPC method takes two
// args (provider, amount_delegator); the REST gateway exposes it as a
// two-segment path: /{provider}/{amount_delegator}. We pass
// 10000000000ulava (10 LAVA) so the chain returns the estimated
// per-provider breakdown against that delegation amount.
//
// Retries pruned-replica responses up to maxPrunedRetries with
// jittered exponential backoff. Also treats HTTP 501/404 the same way:
// Lava's public gateway load-balances across replicas, and the
// EstimatedProviderRewards handler isn't always registered on every
// replica — a fresh connection usually hits a working one.
func (c *HTTPCaller) EstimatedRewards(ctx context.Context, addr string, blockHeight int64) ([]RewardEntry, error) {
	// url.PathEscape defends against any bech32 weirdness sneaking into
	// the addr segment. Cosmos addresses don't contain path-special
	// characters in practice, but escaping eliminates the injection
	// class outright.
	path := "/lavanet/lava/subscription/estimated_provider_rewards/" + url.PathEscape(addr) + "/" + estimatedRewardsDelegatorAmount
	for attempt := 0; ; attempt++ {
		body, err := c.doGET(ctx, path, blockHeight)
		if err != nil {
			// Treat 404/501 as pruned-replica: the handler isn't
			// registered on the replica we just hit; a fresh
			// connection usually rotates to one that has it. If
			// retries exhaust, treat as state-pruned (archive
			// unavailable for this height) rather than a real
			// fetch failure.
			var hs *httpStatusErr
			if errors.As(err, &hs) && (hs.code == 501 || hs.code == 404) {
				if attempt+1 >= maxPrunedRetries {
					return nil, fmt.Errorf("%w: %v (attempts=%d)", errStatePruned, err, attempt+1)
				}
				if !backoffSleep(ctx, attempt) {
					return nil, ctx.Err()
				}
				continue
			}
			return nil, err
		}
		entries, perr := ParseEstimatedRewards(body)
		if perr == nil {
			return entries, nil
		}
		// Chain authoritatively said "no rewards for this provider at
		// this block". Not an error — return empty entries so the
		// caller counts this as a successful-but-empty fetch, not a
		// failure to fetch. The wrapped message is logged at the
		// caller if the operator wants to see it.
		if errors.Is(perr, errNoRewards) {
			return nil, perr
		}
		// errRetryPruned retries up to maxPrunedRetries; anything else
		// from ParseEstimatedRewards (shouldn't happen with the current
		// classifier) is terminal.
		if !errors.Is(perr, errRetryPruned) {
			return nil, perr
		}
		if attempt+1 >= maxPrunedRetries {
			// Every replica we tried had state pruned at this height.
			// Not a fetch failure we can act on — the archive just
			// doesn't hold state that old across the LB. Surface a
			// typed errStatePruned so fetchAll routes to
			// outcomeStatePruned instead of outcomeFailed.
			return nil, fmt.Errorf("%w: %v (attempts=%d)", errStatePruned, perr, attempt+1)
		}
		if !backoffSleep(ctx, attempt) {
			return nil, ctx.Err()
		}
	}
}

// BlockTime reads block/{height}'s header.time. Used by the binary
// search in BlocksDue — not by Snapshot.
func (c *HTTPCaller) BlockTime(ctx context.Context, height int64) (time.Time, error) {
	path := fmt.Sprintf("/cosmos/base/tendermint/v1beta1/blocks/%d", height)
	body, err := c.doGET(ctx, path, 0)
	if err != nil {
		return time.Time{}, err
	}
	var resp struct {
		Block struct {
			Header struct {
				Time time.Time `json:"time"`
			} `json:"header"`
		} `json:"block"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return time.Time{}, fmt.Errorf("decode block h=%d: %w", height, err)
	}
	return resp.Block.Header.Time, nil
}

// Tip returns the chain's current tip. Used by the binary search.
func (c *HTTPCaller) Tip(ctx context.Context) (int64, error) {
	path := "/cosmos/base/tendermint/v1beta1/blocks/latest"
	body, err := c.doGET(ctx, path, 0)
	if err != nil {
		return 0, err
	}
	var resp struct {
		Block struct {
			Header struct {
				Height string `json:"height"`
			} `json:"header"`
		} `json:"block"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, fmt.Errorf("decode latest: %w", err)
	}
	h, err := strconv.ParseInt(resp.Block.Header.Height, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse tip height %q: %w", resp.Block.Header.Height, err)
	}
	return h, nil
}

// ResolveIBC looks up an IBC hash's `base_denom` via
// /ibc/apps/transfer/v1/denom_traces/{hash}. Cached in-process
// forever (IBC traces are immutable). A 404 surfaces as
// errIBCNotFound (permanent — caller drops the row). Other 5xx /
// network errors return a plain error (transient — caller retries).
func (c *HTTPCaller) ResolveIBC(ctx context.Context, hash string) (string, error) {
	if hash == "" {
		return "", errors.New("ibc hash empty")
	}
	c.ibcCache.RLock()
	if c.ibcCache.m != nil {
		if b, ok := c.ibcCache.m[hash]; ok {
			c.ibcCache.RUnlock()
			return b, nil
		}
	}
	c.ibcCache.RUnlock()

	path := "/ibc/apps/transfer/v1/denom_traces/" + url.PathEscape(hash)
	body, err := c.doGET(ctx, path, 0)
	if err != nil {
		var hs *httpStatusErr
		if errors.As(err, &hs) && hs.code == 404 {
			return "", fmt.Errorf("%w: hash=%s", errIBCNotFound, hash)
		}
		return "", fmt.Errorf("ibc trace %s: %w", hash, err)
	}
	var parsed struct {
		DenomTrace struct {
			BaseDenom string `json:"base_denom"`
		} `json:"denom_trace"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", fmt.Errorf("ibc trace %s decode: %w", hash, err)
	}
	base := strings.TrimSpace(parsed.DenomTrace.BaseDenom)
	if base == "" {
		return "", fmt.Errorf("ibc trace %s: empty base_denom", hash)
	}
	c.ibcCache.Lock()
	if c.ibcCache.m == nil {
		c.ibcCache.m = make(map[string]string, 64)
	}
	c.ibcCache.m[hash] = base
	c.ibcCache.Unlock()
	return base, nil
}

// maxBodyBytes caps the response body we read per call. Estimated-
// rewards responses are a few KB per provider; 16 MiB is generous
// headroom that still bounds a compromised/broken upstream trying to
// OOM the process via an unbounded body.
const maxBodyBytes = 16 << 20

// doGET runs one GET with the configured headers and (optionally) a
// pinned block-height header. Reads the body via io.LimitReader + ReadAll
// before inspecting status — draining to EOF even on non-2xx is
// required for Go's net/http to return the connection to the
// keep-alive pool; skipping the drain forces a TCP+TLS handshake on
// every retry, compounded over the pruned-replica 501s the retry loop
// actually expects.
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
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &httpStatusErr{path: path, code: resp.StatusCode}
	}
	return body, nil
}

// httpStatusErr carries the upstream HTTP status so EstimatedRewards
// can route 501/404 into the pruned-retry loop instead of giving up.
type httpStatusErr struct {
	path string
	code int
}

func (e *httpStatusErr) Error() string {
	return fmt.Sprintf("GET %s: http %d", e.path, e.code)
}

// backoffSleep sleeps a random duration in [0, 250ms × 2^attempt]
// (capped at 10s) — AWS-style full jitter. Full jitter (not ±50%) is
// critical when 25 concurrent workers all hit the same pruned replica:
// narrow jitter bands clump their retries back onto the upstream in
// the same ~125ms window, defeating the load-balancing purpose.
// Returns false if ctx was cancelled during the sleep.
func backoffSleep(ctx context.Context, attempt int) bool {
	base := 250 * time.Millisecond
	d := base << attempt
	if d > 10*time.Second {
		d = 10 * time.Second
	}
	d = time.Duration(rand.Int63n(int64(d) + 1))
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
