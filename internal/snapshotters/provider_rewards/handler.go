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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"math/rand"
	"net/http"
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
)

// Config is the subset of cfg.Snapshotters.ProviderRewards this package
// needs. Passed explicitly (rather than reaching into the global Config)
// so the snapshotter is straightforward to test — see handler_test.go.
type Config struct {
	Schema        string
	EarliestDate  time.Time
	Concurrency   int
	RESTURL       string            // base URL of the REST endpoint
	RESTHeaders   map[string]string // extra headers (e.g. lava-extension: archive)
	GenesisHeight int64             // hint for the binary-search lower bound
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
	}
}

// Handler is the Snapshotter implementation for provider_rewards.
type Handler struct {
	cfg       Config
	caller    RESTCaller
	providers *events.Dict
	chains    *events.Dict

	// blockCache memoises (date → block height) across runs so a
	// re-invoked BlocksDue after a DB blip doesn't re-do the binary
	// search from scratch. Keyed by the date's UTC RFC3339 string to
	// avoid time.Time map-key sharp edges.
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

// RESTURL exposes the base REST endpoint this snapshotter is pointed
// at, so the web UI can render it alongside the rest of the
// configuration-dependent state. Empty when the operator left RESTURL
// blank and the resolver's fallback didn't find a rest-kind endpoint.
func (h *Handler) RESTURL() string { return h.cfg.RESTURL }

// DDL returns the SQL statements that create the two tables owned by
// this snapshotter, PLUS idempotent CREATE-IF-NOT-EXISTS for the
// `providers` and `chains` dict tables we FK into. The dict tables
// are conceptually shared with `lava_relay_payment`, but including
// them here (CREATE IF NOT EXISTS) lets the snapshotter start
// standalone — without this, selecting `provider_rewards` but not
// `lava_relay_payment` fails at startup because the FK targets don't
// exist. Idempotent by design.
//
// - providers / chains — dict tables; shared with lava_relay_payment.
// - provider_rewards_snapshots is one row per (date, block) — the
//   snapshotter's "coverage table". Unique on snapshot_date so a retry
//   of the same date safely upserts.
// - provider_rewards is the fact table; the composite PK ensures a
//   single (provider, spec, source, denom) tuple per block.
func (h *Handler) DDL() []string {
	return []string{
		h.providers.DDL(),
		h.chains.DDL(),
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
			  denom        TEXT           NOT NULL,
			  amount       NUMERIC(40, 0) NOT NULL,
			  PRIMARY KEY (block_height, provider_id, spec_id, source_kind, denom)
			);`, h.cfg.Schema),
		fmt.Sprintf(`
			CREATE INDEX IF NOT EXISTS idx_provider_rewards_provider_block
			  ON %[1]s.provider_rewards (provider_id, block_height);`, h.cfg.Schema),
	}
}

// Warmup pre-loads the providers and chains dictionary caches.
// Non-fatal on error — the Snapshot path tolerates a cold cache.
func (h *Handler) Warmup(ctx context.Context, pool *pgxpool.Pool) error {
	if err := h.providers.Warmup(ctx, pool); err != nil {
		return err
	}
	return h.chains.Warmup(ctx, pool)
}

// BlocksDue computes the set of (date, block) targets that still need a
// snapshot. The date set is every 17th of the month from EarliestDate
// up to today, filtered to dates whose 15:00 UTC slot is strictly in the
// past (we can't snapshot a future block). Dates already present in
// provider_rewards_snapshots with status='ok' are subtracted.
//
// For each still-needed date we binary-search the chain for the block
// whose timestamp is closest to {date}T15:00:00Z and cache the
// (date → block) mapping so a rerun of BlocksDue doesn't pay the search
// cost a second time.
func (h *Handler) BlocksDue(ctx context.Context, pool *pgxpool.Pool) ([]snapshotters.SnapshotTarget, error) {
	dates := ExpectedDates(h.cfg.EarliestDate, time.Now().UTC())
	if len(dates) == 0 {
		return nil, nil
	}

	// Subtract dates already covered successfully.
	covered, err := h.coveredDates(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("query covered dates: %w", err)
	}

	targets := make([]snapshotters.SnapshotTarget, 0, len(dates))
	for _, d := range dates {
		if _, ok := covered[d.Format("2006-01-02")]; ok {
			continue
		}
		target, cached, err := h.blockForDate(ctx, d)
		if err != nil {
			// Non-fatal for a single date — the chain may be temporarily
			// unreachable. Log and keep going so one bad date doesn't
			// starve the others.
			slog.Warn("resolve block for snapshot date failed",
				"snapshotter", Name, "date", d.Format("2006-01-02"), "err", err)
			continue
		}
		if !cached {
			slog.Info("resolved block for snapshot date",
				"snapshotter", Name,
				"date", d.Format("2006-01-02"),
				"block", target.BlockHeight,
				"block_time", target.BlockTime)
		}
		targets = append(targets, target)
	}
	return targets, nil
}

func (h *Handler) coveredDates(ctx context.Context, pool *pgxpool.Pool) (map[string]struct{}, error) {
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT snapshot_date FROM %s.provider_rewards_snapshots
		WHERE status = 'ok'`, h.cfg.Schema))
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

func (h *Handler) blockForDate(ctx context.Context, date time.Time) (snapshotters.SnapshotTarget, bool, error) {
	key := date.Format("2006-01-02")
	h.blockCache.Lock()
	if h.blockCache.m == nil {
		h.blockCache.m = make(map[string]cacheEntry)
	}
	if e, ok := h.blockCache.m[key]; ok {
		h.blockCache.Unlock()
		return snapshotters.SnapshotTarget{
			BlockHeight:  e.blockHeight,
			SnapshotDate: date,
			BlockTime:    e.blockTime,
		}, true, nil
	}
	h.blockCache.Unlock()

	target := time.Date(date.Year(), date.Month(), date.Day(), 15, 0, 0, 0, time.UTC)
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
		SnapshotDate: date,
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
		status string
		errMsg string
	}
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT snapshot_date, status, COALESCE(error, '')
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
		if err := rows.Scan(&r.date, &r.status, &r.errMsg); err != nil {
			return snapshotters.Status{}, err
		}
		seen[r.date.UTC().Format("2006-01-02")] = r
	}
	if err := rows.Err(); err != nil {
		return snapshotters.Status{}, err
	}

	expected := ExpectedDates(h.cfg.EarliestDate, time.Now().UTC())
	out := snapshotters.Status{
		Expected: make([]string, 0, len(expected)),
		Covered:  make([]string, 0, len(expected)),
		Missing:  make([]string, 0, len(expected)),
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

// ExpectedDates returns every monthly-17th between earliest and now
// (inclusive on earliest, exclusive on dates whose 15:00 UTC slot is in
// the future). Exported for tests.
func ExpectedDates(earliest, now time.Time) []time.Time {
	earliest = time.Date(earliest.Year(), earliest.Month(), 17, 0, 0, 0, 0, time.UTC)
	var out []time.Time
	cur := earliest
	for {
		slot := time.Date(cur.Year(), cur.Month(), 17, 15, 0, 0, 0, time.UTC)
		if slot.After(now) {
			return out
		}
		out = append(out, time.Date(cur.Year(), cur.Month(), 17, 0, 0, 0, 0, time.UTC))
		// Advance to the 17th of the next month. Using AddDate(0, 1, 0)
		// on a day-17 date stays on day 17 across every month length
		// (no Feb-29 / Feb-30 weirdness since 17 ≤ 28).
		cur = cur.AddDate(0, 1, 0)
	}
}

// Snapshot runs one full snapshot for `target`. The HTTP fan-out
// happens OUTSIDE any transaction — critical because a snapshot can
// take several minutes at 25-way concurrency × 10 retries. We then
// open two short transactions: one to resolve dict IDs (providers +
// chains), one to write the snapshot row + fact rows.
//
// If any provider's chain call exhausts retries, Snapshot returns an
// error without writing anything — next tick re-tries the date from
// scratch, giving a consistent view at the cost of repeating some
// chain reads. "Partial" mode is deliberately not implemented.
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

	// 2. Fetch estimated rewards for every provider in a concurrency-
	// bounded errgroup — OUTSIDE any tx. First error cancels siblings
	// so a doomed snapshot doesn't keep 24 more HTTP loops running.
	results, err := h.fetchAll(ctx, providers, target.BlockHeight)
	if err != nil {
		return fmt.Errorf("fetch rewards: %w", err)
	}

	// 3. Resolve provider + spec IDs in their own short tx. Dict caches
	// are commit-only, so these writes commit safely independent of the
	// final write tx — a rollback in step 4 leaves the dict rows in
	// place, which is harmless (the composite PK on provider_rewards
	// prevents dup-inserting fact rows, and dict rows are idempotent by
	// design).
	provSet := make(map[string]struct{})
	specSet := make(map[string]struct{})
	for _, r := range results {
		provSet[r.provider] = struct{}{}
		for _, entry := range r.entries {
			specSet[entry.Spec] = struct{}{}
		}
	}
	provList := make([]string, 0, len(provSet))
	for p := range provSet {
		provList = append(provList, p)
	}
	specList := make([]string, 0, len(specSet))
	for s := range specSet {
		specList = append(specList, s)
	}
	var provIDs, specIDs map[string]int32
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
		return nil
	})
	if err != nil {
		return err
	}

	// 4. Build the fact rows in memory (no DB, no tx). Every row is
	// unique by construction (composite PK); dedup as a safety net.
	const (
		iBlockHeight = iota
		iProviderID
		iSpecID
		iSourceKind
		iDenom
		iAmount
	)
	type rowKey struct {
		providerID int32
		specID     int32
		sourceKind int16
		denom      string
	}
	seen := make(map[rowKey]struct{})
	rows := make([][]any, 0, 256)
	provWithRewards := 0
	for _, r := range results {
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
				amount, ok := new(big.Int).SetString(amt.Amount, 10)
				if !ok {
					return fmt.Errorf("parse amount %q for %s/%s", amt.Amount, r.provider, entry.Spec)
				}
				k := rowKey{
					providerID: provID,
					specID:     specID,
					sourceKind: int16(entry.SourceKind),
					denom:      amt.Denom,
				}
				if _, dup := seen[k]; dup {
					continue
				}
				seen[k] = struct{}{}
				row := make([]any, 6)
				row[iBlockHeight] = target.BlockHeight
				row[iProviderID] = provID
				row[iSpecID] = specID
				row[iSourceKind] = int16(entry.SourceKind)
				row[iDenom] = amt.Denom
				row[iAmount] = amount
				rows = append(rows, row)
			}
		}
	}
	copyCols := []string{"block_height", "provider_id", "spec_id", "source_kind", "denom", "amount"}

	// 5. Open the write tx: insert the snapshots row (parent FK) then
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

// fetchResult bundles one provider's parsed chain response.
type fetchResult struct {
	provider string
	entries  []RewardEntry
}

func (h *Handler) fetchAll(ctx context.Context, providers []string, blockHeight int64) ([]fetchResult, error) {
	// errgroup.WithContext gives us first-error-cancels-siblings semantics:
	// when any worker's EstimatedRewards call fails, the derived ctx is
	// cancelled and the other 24 in-flight HTTP loops bail out of their
	// retry sleeps + active requests promptly, instead of running the
	// remaining ~2000 retries for a snapshot that's already going to
	// roll back.
	g, gctx := errgroup.WithContext(ctx)
	workers := h.cfg.Concurrency
	if workers <= 0 {
		workers = 25
	}
	g.SetLimit(workers)

	out := make([]fetchResult, len(providers))
	for i, addr := range providers {
		g.Go(func() error {
			entries, err := h.caller.EstimatedRewards(gctx, addr, blockHeight)
			if err != nil {
				return fmt.Errorf("provider %s: %w", addr, err)
			}
			out[i] = fetchResult{provider: addr, entries: entries}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
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
// declarative. On success the `info` array is populated; on the
// application-level error path the `success` flag is false and the
// `message` field carries the reason.
type estimatedRewardsResp struct {
	Info    []rawInfo `json:"info"`
	Success *bool     `json:"success,omitempty"`
	Message string    `json:"message,omitempty"`
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
	var r estimatedRewardsResp
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if r.Success != nil && !*r.Success {
		return nil, classifyChainMessage(r.Message)
	}
	entries := make([]RewardEntry, 0, len(r.Info))
	for _, raw := range r.Info {
		kind, spec := parseSource(raw.Source)
		amounts := make([]RewardAmount, 0, len(raw.Amount))
		for _, a := range raw.Amount {
			// The chain returns amount as a decimal string like
			// "30737293.000000000000000000". Strip the fractional zeros
			// so we can fit it into a NUMERIC(40, 0) — every entry we've
			// observed has all-zero fractional part (it's a base-unit
			// integer dressed in fixed-decimal notation).
			cleaned, ok := cleanIntegerString(a.Amount)
			if !ok {
				return nil, fmt.Errorf("non-integer amount %q (spec=%s denom=%s)", a.Amount, spec, a.Denom)
			}
			amounts = append(amounts, RewardAmount{Denom: a.Denom, Amount: cleaned})
		}
		entries = append(entries, RewardEntry{SourceKind: kind, Spec: spec, Amounts: amounts})
	}
	return entries, nil
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
// for this block height. Retry on a fresh connection — some gateways
// rotate between nodes, so a second try often succeeds.
var errRetryPruned = errors.New("replica pruned; retry")

// errRetryTransient indicates an opaque success:false response. Retry
// conservatively (fewer attempts than pruned-replica).
var errRetryTransient = errors.New("chain returned success=false; retry")

// errNoClaimableRewards indicates the application-level "nothing to
// distribute" state. Treated as an empty info[] — NOT an error at the
// snapshotter level.
var errNoClaimableRewards = errors.New("no claimable rewards after distribution")

func classifyChainMessage(msg string) error {
	low := strings.ToLower(msg)
	switch {
	case strings.Contains(low, "version does not exist"),
		strings.Contains(low, "version mismatch"),
		strings.Contains(low, "no commit info found"),
		// "Not Implemented" (grpc code 12) surfaces when Lava's public
		// gateway load-balances to a replica that doesn't have the
		// EstimatedProviderRewards handler registered at this height —
		// another replica will. Retry on a fresh connection.
		strings.Contains(low, "not implemented"):
		return fmt.Errorf("%w: %s", errRetryPruned, msg)
	case strings.Contains(low, "cannot get claimable rewards after distribution"):
		return errNoClaimableRewards
	default:
		return fmt.Errorf("%w: %s", errRetryTransient, msg)
	}
}

// ---------------------------------------------------------------------------
// HTTP caller (production wiring)
// ---------------------------------------------------------------------------

// maxPrunedRetries caps the per-provider retries when the replica looks
// pruned. 10 is aggressive — pruning replicas usually rotate inside 2-3
// seconds on Lava's public gateway — but the worst-case payoff is a few
// seconds per stuck provider, bounded.
const maxPrunedRetries = 10

// maxTransientRetries caps retries for opaque success:false. Lower than
// pruned since the error is non-specific: if the chain is consistently
// returning "success: false" for this provider, hammering won't help.
const maxTransientRetries = 3

// HTTPCaller is the production RESTCaller. Calls the chain via a plain
// http.Client sharing keep-alive + TLS session cache with the rest of
// the indexer's HTTP work.
type HTTPCaller struct {
	baseURL string
	headers map[string]string
	http    *http.Client
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

// EstimatedRewards hits the Lava pairing module's estimated-rewards
// endpoint pinned at blockHeight. The gRPC method takes two args
// (provider, amount_delegator); the REST gateway exposes it as a
// two-segment path. We always pass "1ulava" as the delegator amount —
// the minimum valid coin — so the chain returns the estimated
// per-provider breakdown regardless of any real delegation.
//
// Retries pruned-replica responses up to maxPrunedRetries with
// jittered exponential backoff. Also treats HTTP 501/404 the same way:
// Lava's public gateway load-balances across replicas, and the
// EstimatedProviderRewards handler isn't always registered on every
// replica — a fresh connection usually hits a working one.
func (c *HTTPCaller) EstimatedRewards(ctx context.Context, addr string, blockHeight int64) ([]RewardEntry, error) {
	path := fmt.Sprintf("/lavanet/lava/pairing/estimated_provider_rewards/%s/1ulava", addr)
	for attempt := 0; ; attempt++ {
		body, err := c.doGET(ctx, path, blockHeight)
		if err != nil {
			// Treat 404/501 as pruned-replica: the handler isn't
			// registered on the replica we just hit; a fresh
			// connection usually rotates to one that has it.
			var hs *httpStatusErr
			if errors.As(err, &hs) && (hs.code == 501 || hs.code == 404) {
				if attempt+1 >= maxPrunedRetries {
					return nil, fmt.Errorf("provider %s: %w (attempts=%d)", addr, err, attempt+1)
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
		if errors.Is(perr, errNoClaimableRewards) {
			return nil, nil
		}
		var limit int
		switch {
		case errors.Is(perr, errRetryPruned):
			limit = maxPrunedRetries
		case errors.Is(perr, errRetryTransient):
			limit = maxTransientRetries
		default:
			return nil, perr
		}
		if attempt+1 >= limit {
			return nil, fmt.Errorf("provider %s: %w (attempts=%d)", addr, perr, attempt+1)
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
	var h int64
	for _, r := range resp.Block.Header.Height {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("non-numeric tip height %q", resp.Block.Header.Height)
		}
		h = h*10 + int64(r-'0')
	}
	return h, nil
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
		req.Header.Set("x-cosmos-block-height", fmt.Sprintf("%d", blockHeight))
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

// backoffSleep sleeps 250ms × 2^attempt with ±50% jitter, capped at 10s.
// Returns false if ctx was cancelled during the sleep.
func backoffSleep(ctx context.Context, attempt int) bool {
	base := 250 * time.Millisecond
	d := base << attempt
	if d > 10*time.Second {
		d = 10 * time.Second
	}
	jitter := time.Duration(rand.Int63n(int64(d)/2 + 1))
	d = d/2 + jitter
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
