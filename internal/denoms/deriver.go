package denoms

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DDL returns the idempotent CREATE statements for the priced_denoms
// table plus a seed of known base_denom → coingecko_id mappings.
// Applied once at startup by RunDeriver.
//
// Seed philosophy: operators add new mappings by running a single
// INSERT ... ON CONFLICT DO UPDATE against the DB — no redeploy.
// coingecko_id is nullable so the deriver can insert a newly-seen
// denom without knowing its CoinGecko identifier; operators fill the
// mapping later via SQL.
func DDL(schema string) []string {
	return []string{
		fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %[1]s.priced_denoms (
			  base_denom    TEXT PRIMARY KEY,
			  coingecko_id  TEXT,
			  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			  last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now()
			);`, schema),
		fmt.Sprintf(`
			CREATE INDEX IF NOT EXISTS idx_priced_denoms_needs_coingecko
			  ON %[1]s.priced_denoms (base_denom)
			  WHERE coingecko_id IS NULL;`, schema),
		fmt.Sprintf(`
			INSERT INTO %[1]s.priced_denoms (base_denom, coingecko_id) VALUES
			  ('lava',   'lava-network'),
			  ('atom',   'cosmos'),
			  ('osmo',   'osmosis'),
			  ('juno',   'juno-network'),
			  ('stars',  'stargaze'),
			  ('akt',    'akash-network'),
			  ('huahua', 'chihuahua-token'),
			  ('evmos',  'evmos'),
			  ('inj',    'injective-protocol'),
			  ('cro',    'crypto-com-chain'),
			  ('scrt',   'secret'),
			  ('iris',   'iris-network'),
			  ('regen',  'regen'),
			  ('ion',    'ion'),
			  ('like',   'likecoin'),
			  ('axl',    'axelar'),
			  ('band',   'band-protocol'),
			  ('bld',    'agoric'),
			  ('cmdx',   'comdex'),
			  ('cre',    'crescent-network'),
			  ('xprt',   'persistence'),
			  ('usdc',   'usd-coin'),
			  ('move',   'movement')
			ON CONFLICT (base_denom) DO UPDATE
			  SET coingecko_id = EXCLUDED.coingecko_id;`, schema),
	}
}

// DeriverConfig is the subset of cfg.Denoms.Deriver this package needs.
type DeriverConfig struct {
	Schema         string
	Interval       time.Duration
	ResolveTimeout time.Duration
	RESTURL        string
	RESTHeaders    map[string]string
}

// Deriver owns the ticker + cache state across ticks. Exposed on
// top-level so the web layer can pull its Status() snapshot for the
// dashboard card without re-querying the DB.
type Deriver struct {
	cfg      DeriverConfig
	pool     *pgxpool.Pool
	resolver *Resolver

	mu             sync.RWMutex
	lastTickAt     time.Time
	lastTickErr    error
	known          map[string]struct{} // raw denoms already processed this process lifetime
	mappedCount    int
	unmappedCount  int
	unmappedList   []string // sorted, capped
}

// New constructs a Deriver. Call ApplyDDL once at startup, then Run.
func New(pool *pgxpool.Pool, cfg DeriverConfig) *Deriver {
	if cfg.Schema == "" {
		cfg.Schema = "app"
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Minute
	}
	return &Deriver{
		cfg:      cfg,
		pool:     pool,
		resolver: NewResolver(cfg.RESTURL, cfg.RESTHeaders, cfg.ResolveTimeout),
		known:    make(map[string]struct{}, 128),
	}
}

// ApplyDDL runs the package's DDL + seed against pool. Idempotent.
// Call once at startup, before Run.
func (d *Deriver) ApplyDDL(ctx context.Context) error {
	for _, stmt := range DDL(d.cfg.Schema) {
		if _, err := d.pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("priced_denoms ddl: %w", err)
		}
	}
	return nil
}

// Run ticks on d.cfg.Interval, running one derivation pass per tick.
// Returns nil on ctx cancel. Errors from individual ticks are logged
// and folded into Status() — they never propagate up because losing
// the deriver would cascade via errgroup into the whole indexer.
func (d *Deriver) Run(ctx context.Context) error {
	// One immediate pass so a fresh deploy populates priced_denoms
	// without waiting a full interval.
	d.runTick(ctx)

	t := time.NewTicker(d.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			d.runTick(ctx)
		}
	}
}

// runTick does one SELECT DISTINCT + resolve + upsert pass.
func (d *Deriver) runTick(ctx context.Context) {
	now := time.Now().UTC()

	raws, err := d.distinctRawDenoms(ctx)
	if err != nil {
		slog.Warn("denoms deriver: read failed", "err", err)
		d.recordTickResult(now, err)
		return
	}

	var newRaws []string
	d.mu.RLock()
	for _, r := range raws {
		if _, seen := d.known[r]; !seen {
			newRaws = append(newRaws, r)
		}
	}
	d.mu.RUnlock()

	if len(newRaws) == 0 {
		// Fast path: nothing new this tick. Touch last_seen_at for
		// every already-cached denom so operators can still spot
		// stale denoms, but skip the per-denom resolve+upsert roundtrips.
		if err := d.touchLastSeen(ctx, raws); err != nil {
			slog.Warn("denoms deriver: touch last_seen failed", "err", err)
		}
		d.refreshCounts(ctx, now, nil)
		return
	}

	var resolveErrs []error
	for _, raw := range newRaws {
		rctx, cancel := context.WithTimeout(ctx, d.cfg.ResolveTimeout)
		base, transient, err := d.resolver.ResolveBaseDenom(rctx, raw)
		cancel()
		if err != nil {
			if transient {
				slog.Warn("denoms: transient resolve failure; will retry next tick",
					"raw", raw, "err", err)
				continue
			}
			slog.Warn("denoms: permanent resolve failure; skipping",
				"raw", raw, "err", err)
			resolveErrs = append(resolveErrs, err)
			continue
		}
		if base == "" {
			continue
		}
		if err := d.upsert(ctx, base); err != nil {
			slog.Warn("denoms: upsert failed", "raw", raw, "base", base, "err", err)
			continue
		}
		// Only mark known after successful DB upsert — a failed upsert
		// means we should retry on the next tick.
		d.mu.Lock()
		d.known[raw] = struct{}{}
		d.mu.Unlock()
	}

	// Still touch last_seen for raws that were already known.
	if err := d.touchLastSeen(ctx, raws); err != nil {
		slog.Warn("denoms deriver: touch last_seen failed", "err", err)
	}

	var combined error
	if len(resolveErrs) > 0 {
		combined = fmt.Errorf("%d raw denom(s) failed to resolve", len(resolveErrs))
	}
	d.refreshCounts(ctx, now, combined)
}

// distinctRawDenoms returns every distinct denom currently in
// provider_rewards. The set is typically small (tens).
func (d *Deriver) distinctRawDenoms(ctx context.Context) ([]string, error) {
	rows, err := d.pool.Query(ctx, fmt.Sprintf(
		`SELECT DISTINCT denom FROM %s.provider_rewards`, d.cfg.Schema))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0, 16)
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out, rows.Err()
}

// upsert writes a new base_denom (if missing) or bumps last_seen_at
// (if present). coingecko_id is left at whatever the seed or a prior
// operator UPDATE set.
func (d *Deriver) upsert(ctx context.Context, base string) error {
	_, err := d.pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %[1]s.priced_denoms (base_denom)
		VALUES ($1)
		ON CONFLICT (base_denom) DO UPDATE
		  SET last_seen_at = now()`, d.cfg.Schema), base)
	if err != nil {
		return err
	}
	// Warn when we land a denom without a coingecko_id so operators
	// see the actionable signal in logs (matching the `coingecko_id IS
	// NULL` row).
	var mapped bool
	row := d.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT coingecko_id IS NOT NULL FROM %s.priced_denoms WHERE base_denom = $1`,
		d.cfg.Schema), base)
	if err := row.Scan(&mapped); err == nil && !mapped {
		slog.Warn("denoms: new base denom has no coingecko_id mapping",
			"base_denom", base,
			"fix", fmt.Sprintf("UPDATE %s.priced_denoms SET coingecko_id = '…' WHERE base_denom = '%s';",
				d.cfg.Schema, base))
	}
	return nil
}

// touchLastSeen bumps last_seen_at for every base denom that appears
// in the current raw-denom set (after resolution). For the fast path
// we don't have the resolved bases handy, so this version cheats:
// it only bumps last_seen for bases we ALREADY know about by running
// a single UPDATE keyed on the raw denoms re-passed through the
// resolver's cache. Non-cacheable denoms are skipped (they'll bump
// on the next new-denom pass).
func (d *Deriver) touchLastSeen(ctx context.Context, raws []string) error {
	if len(raws) == 0 {
		return nil
	}
	// Use the resolver cache exclusively here — touch is non-critical,
	// so we skip IBC denoms whose trace hasn't been resolved yet and
	// avoid making network calls from the fast path.
	bases := make(map[string]struct{}, len(raws))
	for _, r := range raws {
		low := strings.ToLower(strings.TrimSpace(r))
		if strings.HasPrefix(low, "ibc/") {
			hash := r[len("ibc/"):]
			b := d.resolver.cachedBase(hash)
			if b == "" {
				continue
			}
			bases[stripMicroPrefix(b)] = struct{}{}
			continue
		}
		bases[stripMicroPrefix(low)] = struct{}{}
	}
	if len(bases) == 0 {
		return nil
	}
	list := make([]string, 0, len(bases))
	for b := range bases {
		list = append(list, b)
	}
	_, err := d.pool.Exec(ctx, fmt.Sprintf(`
		UPDATE %s.priced_denoms SET last_seen_at = now()
		WHERE base_denom = ANY($1::text[])`, d.cfg.Schema), list)
	return err
}

// Status is the Deriver's snapshot for the dashboard card.
type Status struct {
	Mapped     int       `json:"mapped"`
	Unmapped   int       `json:"unmapped"`
	Unresolved []string  `json:"unresolved"`
	LastRunAt  time.Time `json:"last_run_at,omitempty"`
	LastErr    string    `json:"last_err,omitempty"`
}

// Status returns the current in-memory view for /api/denoms. Safe to
// call from any goroutine.
func (d *Deriver) Status() Status {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := Status{
		Mapped:    d.mappedCount,
		Unmapped:  d.unmappedCount,
		LastRunAt: d.lastTickAt,
	}
	if d.lastTickErr != nil {
		out.LastErr = d.lastTickErr.Error()
	}
	if len(d.unmappedList) > 0 {
		cp := make([]string, len(d.unmappedList))
		copy(cp, d.unmappedList)
		out.Unresolved = cp
	}
	return out
}

// refreshCounts rebuilds the in-memory status counts from the DB.
// Cheap (two queries against a small table) and we only do it once
// per tick.
func (d *Deriver) refreshCounts(ctx context.Context, tickAt time.Time, tickErr error) {
	var mapped, unmapped int
	if err := d.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT
		   COUNT(*) FILTER (WHERE coingecko_id IS NOT NULL),
		   COUNT(*) FILTER (WHERE coingecko_id IS NULL)
		 FROM %s.priced_denoms`, d.cfg.Schema)).Scan(&mapped, &unmapped); err != nil {
		slog.Warn("denoms deriver: count refresh failed", "err", err)
		d.recordTickResult(tickAt, errors.Join(tickErr, err))
		return
	}
	var unmappedList []string
	if unmapped > 0 {
		rows, err := d.pool.Query(ctx, fmt.Sprintf(
			`SELECT base_denom FROM %s.priced_denoms
			 WHERE coingecko_id IS NULL
			 ORDER BY first_seen_at DESC LIMIT 50`, d.cfg.Schema))
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var s string
				if scanErr := rows.Scan(&s); scanErr == nil {
					unmappedList = append(unmappedList, s)
				}
			}
		}
	}
	d.mu.Lock()
	d.mappedCount = mapped
	d.unmappedCount = unmapped
	d.unmappedList = unmappedList
	d.lastTickAt = tickAt
	d.lastTickErr = tickErr
	d.mu.Unlock()
}

// recordTickResult stamps last_tick metadata when the tick failed
// before it could call refreshCounts.
func (d *Deriver) recordTickResult(tickAt time.Time, err error) {
	d.mu.Lock()
	d.lastTickAt = tickAt
	d.lastTickErr = err
	d.mu.Unlock()
}

// cachedBase exposes the resolver's IBC cache to the fast-path
// last_seen_at UPDATE, avoiding a network call from a non-critical
// code path.
func (r *Resolver) cachedBase(hash string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cache[hash]
}
