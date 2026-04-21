# Implementation summary: provider_rewards snapshotter

Added a new `Snapshotter` seam (`internal/snapshotters`) parallel to the
existing event-driven `Handler` and wired up a concrete
`provider_rewards` snapshotter that captures
`estimated_provider_rewards` for every registered provider at
monthly-17th 15:00-UTC block heights.

## Files added

- `internal/snapshotters/snapshotter.go` — `Snapshotter` interface +
  `SnapshotTarget`.
- `internal/snapshotters/registry.go` — `Registry` with `RunLoop`
  (tick-on-interval, runs once immediately on startup, per-target
  pgx.Tx, one-failure-doesn't-starve-others).
- `internal/snapshotters/block_time.go` — generic binary search for
  "block closest to timestamp T".
- `internal/snapshotters/provider_rewards/handler.go` — the concrete
  snapshotter: DDL, BlocksDue, Snapshot, HTTP caller with
  retry-on-pruned-replica, source-label parser, amount cleaner.
- `internal/snapshotters/registry_test.go`,
  `internal/snapshotters/block_time_test.go`,
  `internal/snapshotters/provider_rewards/handler_test.go`,
  `internal/snapshotters/provider_rewards/http_caller_test.go` — unit
  tests for all the response-classification branches, date math, binary
  search convergence, one-failure-doesn't-starve-others behaviour.

## Files modified

- `internal/config/config.go` — added `Snapshotters` section,
  env-overrides, earliest-date validation, `ParsedEarliestDate()`
  helper.
- `cmd/indexer/main.go` — registration (gated on
  `cfg.Snapshotters.ProviderRewards.Enabled`), DDL apply, dict warmup,
  RunLoop goroutine under errgroup, `resolveSnapshotterRESTURL` +
  `resolveSnapshotterRESTHeaders` helpers (fall back to first
  `kind: rest` endpoint).
- `internal/web/server.go` — `Snapshotters` field on `Server`,
  `/api/snapshotters` endpoint, `SnapshotterStatus` + failed-date
  shapes.
- `internal/web/assets/index.html` — new "Snapshotters" UI card with a
  dot-per-date timeline (green covered / red failed / grey missing),
  expandable errors list, last-run / next-run meta, isolated keyed DOM
  cache for in-place updates. Section is hidden when
  `/api/snapshotters` returns `[]`.
- `config.example.yml` — documented new `snapshotters:` block.
- `README.md` — new "Snapshotters" section describing the seam, the
  concrete provider_rewards snapshotter, and how to add another.

## Key design decisions

- **Dedicated seam, not reuse of `events.Handler`.** Snapshotters have
  a fundamentally different shape: calendar-driven, their own RPC
  calls, their own coverage table (not `indexer_ranges`). Shoehorning
  would have muddied both interfaces.
- **Dict reuse.** `provider_rewards` upserts into `app.providers` and
  `app.chains` via the existing `events.Dict` helper so IDs align with
  whatever `lava_relay_payment` has already written — no duplicated
  dictionary tables.
- **All-or-nothing snapshot transactions.** The spec had a "partial"
  status in the DDL, but the implementation only writes `ok` or
  `failed`. A partial snapshot creates data-quality ambiguity for
  downstream consumers, and the registry re-runs failed dates from
  scratch on the next tick. The `partial` value is reserved in the
  column (TEXT DEFAULT 'ok') for future use; the UI's
  `SnapshotterFailedDate` path treats any non-`ok`/non-`failed` status
  as a flagged failed entry so anything we add later surfaces.
- **HTTP caller is small and self-contained.** Rather than adapting
  the main `rpc.RESTClient` (which is specifically for block /
  tx-events fetching), I wrote a minimal caller that satisfies both
  `RESTCaller` and `BlockTimeLookup`. This avoids dragging the full
  MultiClient+routing code into the snapshotter path — the snapshotter
  only needs one archive-backed endpoint, not routing + failover.
- **Web Server.SnapshotterSchema field.** The server's status handler
  knows `app` by default (via `state.State.Schema()`) but the
  snapshotter status is keyed on the same schema; I added a separate
  field rather than plumbing `state.State` into a second role, to keep
  the dependency direction clean.

## Divergences from spec

- **Partial status not emitted.** The spec lists `'ok' | 'partial' |
  'failed'` but the current code only writes `ok` / `failed`. See the
  design-decisions note above — schema still supports the value, just
  no code path produces it today.
- **API `expected` set starts at the earliest observed DB row**, not
  `cfg.EarliestDate`, because the web server doesn't hold a reference
  to the snapshotter's parsed config. This is correct in
  steady state (once the first snapshot lands, `expected` matches)
  and gracefully shows "no snapshots yet" on a fresh deploy. If
  operators want to see the configured cadence before the first tick,
  we could plumb the config through — left open as a follow-up.
- **No "retry failed" button** — explicitly optional per spec; not
  implemented.
