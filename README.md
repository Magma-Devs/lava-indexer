# lava-indexer

Lightweight Go indexer for Lava Network. Reads blocks from a Tendermint/CometBFT
RPC (or Cosmos REST/LCD), extracts the events you care about, and writes them
to Postgres with a space-efficient normalised schema. Exposes a GraphQL API
over the same schema via PostGraphile.

Designed for high-throughput backfills and live tip-follow on the same
process: per-batch `CopyFrom` writes, adaptive fetch workers, dictionary-
compressed repeated TEXT columns (provider addresses, chain names), and
self-sizing to whatever cgroup CPU/memory limits the container is given.

## What's indexed

Out of the box: **`lava_relay_payment`** events only. Every other Lava event
is dropped at ingest — consistent with the design decision that the MV is
the real read surface and per-event records would just bloat disk.

Adding a new event type is a new file and one line of registration (see
[§ Adding a new event](#adding-a-new-event)).

## Layout

```
cmd/indexer/                    main.go (wires the pipeline)
internal/config/                YAML config loading + env overrides
internal/rpc/                   Tendermint RPC + Cosmos REST + multi-endpoint
internal/events/                Handler interface, Registry, Dict helper
internal/events/relay_payment/  lava_relay_payment handler
internal/state/                 per-handler indexer_ranges (range-aware resume)
internal/aggregates/            apply aggregates/*.sql + pg_cron scheduling
internal/pipeline/              dispatcher + fetchers + atomic-commit writer
internal/web/                   built-in progress UI (SVG timeline + JSON API)
aggregates/                     user-defined materialised views / rollups
  001_mv_relay_daily.sql        example MV
  aggregates.yml                pg_cron schedules
docker/                         postgres.Dockerfile (postgres:16 + pg_cron)
Dockerfile                      indexer binary
docker-compose.yml              dev stack: postgres + indexer + graphql
docker-compose.prod.yml         prod: indexer only, bring your own postgres
config.example.yml              copy to config.yml and edit
```

## Running

```bash
cp config.example.yml config.yml
cp .env.example .env
# edit as needed — LAVA_RPC_ENDPOINT, start/end heights, etc.
docker compose up -d --build
```

- **Postgres** on `127.0.0.1:5432`
- **GraphQL** on `http://localhost:3000/graphql` (GraphiQL at `/graphiql`)
- **Indexer** container logs the progress via `docker compose logs -f indexer`

Without Docker (useful for dev):

```bash
go build -o bin/indexer ./cmd/indexer
./bin/indexer -config config.yml
```

## Crash safety

Every fetch-batch commits in **one** `pgx.Tx` that wraps:
1. Every handler's `CopyFrom` into its tables.
2. The per-handler range-merge into `app.indexer_ranges`.

If the process dies mid-batch (SIGKILL, panic, container restart, DB
connectivity blip, anything), the transaction rolls back and **neither the
row writes NOR the range update persist**. On the next run, `Gaps` still
sees that height range as missing and refetches the whole batch. You can't
end up with rows written but the range advanced (or vice versa) — so
"already indexed" means "the rows are in Postgres".

## Range-aware resume

State is kept in `app.indexer_ranges` as a list of non-overlapping inclusive
`[from_height, to_height]` ranges. On every run, the indexer subtracts your
configured `[start_height, end_height]` window from the existing ranges and
only fetches the gaps. So you can:

- **Week 1**: start with `start_height: 4311086`, `end_height: 0` → index
  from 6 months ago to tip, then follow tip.
- **Week 2**: change `start_height: 3500000`, `end_height: 4311085` → only
  the new `[3500000, 4311085]` gap is fetched; previously-indexed heights
  are skipped entirely.

Ranges are merged as gaps close, so the state table stays small (typically 1
row once you're caught up).

Row writes and range-merges commit in one transaction per batch — if the
indexer is killed mid-batch, nothing is written and the whole batch is
refetched on next run.

### Tip-first parallel gap filling

When you rerun with a lower `start_height`, the missing window is split into
disjoint gaps: the _old_ gap between the new start and the prior earliest,
plus every gap between previously-indexed ranges (including dead-letter
holes) and the tip. All gaps are worked in parallel by a shared pool, but
the producer is **tip-first**: on every emit it picks the gap whose cursor
is currently highest — i.e. closest to the chain tip — and dispatches the
next batch from there. Consequences:

- With a historical-backfill gap (e.g. `[6mo-ago, pruning-horizon]`) plus
  a live tip-extension gap, the live gap always wins. The observable
  "latest indexed height" tracks the chain head even while history is
  still filling in.
- When the tip-follower extends the top gap on each poll, those new
  heights immediately jump to the front of the queue; they never sit
  behind half a million backfill blocks.
- Within a single gap the walk is bottom-up, `gap.From → gap.To`.

Each gap is also sliced at every endpoint's `Earliest` coverage horizon
so a tier with more eligible endpoints doesn't get starved behind a tier
only the archive can serve. Example: with `lava-archive (earliest=1)`,
`rpc1 (earliest=4M)`, and `rpc2 (earliest=4.5M)`, a backfill from block 1
runs as three tiers (`[1, 4M-1]` archive-only, `[4M, 4.5M-1]` + `[4.5M,
tip]` each with more eligible endpoints) and the routing layer fans work
across them in parallel.

#### Fetch / persist are decoupled

A worker isn't one-request-one-commit. The pipeline splits into two pools:

```
producer → jobs → N fetchers → writeCh → M writers → DB
```

- **N fetchers** (`fetch_workers`) pull batches, fetch via
  `MultiClient.FetchBlocks`, parse events, and enqueue a `writeJob` to
  `writeCh`. Fetch failures bisect (same as before); the fetcher never
  holds a DB tx.
- **M writers** (`pool_size - 4`) drain up to 32 `writeJob`s per 50 ms
  window and commit them in a single Postgres tx — amortising the
  BEGIN/Heartbeat/COMMIT overhead across many batches. Persist failures
  record the batch as dead-letter; the sweep re-fetches.

This lets fetch run up to the upstream ceiling and write run up to the DB
ceiling independently — whichever is slower becomes the bottleneck, not
both. No more "fast upstream gated on slow commit" or vice versa.

Writers to `indexer_ranges` are **lock-free**: each writer does an
`INSERT … ON CONFLICT DO UPDATE` and exits. A background compactor ticks
every 30 s and runs `range_agg(int8range(...))` to collapse overlapping
or touching raw ranges into the minimum-row representation. Writers never
block on each other; fragmentation is bounded by one compaction cycle.

## Space-efficient schema

Row shape chosen to minimise disk for billion-row workloads:

| column | type | why |
|---|---|---|
| `block_height` | `BIGINT` | 8B; future-proof |
| `tx_hash` | `BYTEA` (32B) | half the size of hex TEXT, SHA-256 fits exactly |
| `event_idx` | `INT` | block-scoped event counter |
| `record_idx` | `SMALLINT` | provider index within a relay_payment event |
| `timestamp` | `TIMESTAMPTZ` | 8B |
| `provider_id` | `INT` → `app.providers(id)` | dictionary — 40-char address → 4B |
| `chain_id` | `INT` → `app.chains(id)` | dictionary — short repeated TEXT → 4B |
| `cu`, `relay_number` | `BIGINT`, `INT` | |
| `qos_*`, `ex_qos_*` | `REAL` (4B) | half the size of DOUBLE, perceptibly lossless for QoS fractions |

Primary key is composite `(block_height, tx_hash, event_idx, record_idx)` —
no synthetic TEXT id column. Secondary index is a single BRIN over
`(block_height, timestamp)` — tiny, fast for range scans.

Rough density: ~100 bytes/row on a dense Lava block = ~180 GB for ~2B rows.

## How much load are we pushing at each node?

With the defaults (`fetch_workers: 0`, `fetch_batch_size: 50`) the
indexer sizes fetch_workers to the host (CPU × 8, memory / 2 MiB, hard
max from config) and keeps **up to `fetch_workers` HTTP POSTs in flight
total**, distributed across endpoints by live routing (see below). Each
POST carries 100 JSON-RPC method calls (block + block_results × 50
heights).

There is no per-endpoint concurrency cap. The process-wide
`fetch_workers` count is the only ceiling. Historically the indexer ran
an AIMD controller that capped each endpoint's in-flight count based on
latency and errors — useful on shared public RPCs where good-citizen
throttling matters, over-conservative on owned infra. The cap is gone;
routing distributes load by speed instead.

The UI's Endpoints tab shows live per-endpoint load and sparkline history
for rps / p50 / p99 / in-flight:

```
✓ [RPC] https://rpc1        earliest …  latest …
  in-flight 7 / peak 128   req/s 48.3   p50/p95/p99 65/110/210ms   errors 0/4812
```

The only feedback signals wired in today are (a) routing score to bias
load away from slow or erroring endpoints, and (b) the adaptive sizer's
shrink path on sustained 3%+ error rate (shrinks batch size ×0.5). Tail
latency alone no longer triggers any reaction — slow-but-healthy is
different from broken, and routing handles the "slow" case naturally.

## Endpoint routing

Every `FetchBlocks` call ranks eligible endpoints and tries them in
score order (lowest wins):

```
score = p50_ms × (1 + in_flight) × (1 + errRate × 3)
```

Three signals, each independent:

- `p50_ms` — how fast this endpoint has been completing requests in the
  recent window. An endpoint that hasn't been sampled yet gets
  `p50_ms = 100` so new endpoints get exploration traffic rather than
  starving.
- `(1 + in_flight)` — dampens piling onto an already-queued endpoint.
  Grows linearly with outstanding requests, so a saturating endpoint
  naturally sheds new work to an idle peer.
- `(1 + errRate × 3)` — dampens piling onto a broken endpoint. Errors
  typically complete fast (429 / 5xx fast-fail), so `p50_ms` stays low
  even when the endpoint is returning failures — without the error
  multiplier a broken endpoint would keep winning routing and
  self-reinforce. A 50 % error rate makes the score 2.5× worse;
  100 % makes it 4× worse.

Failover is first-class: on `*ErrRateLimited` (429) or `*ErrServerError`
(5xx) the client returns immediately and `MultiClient` tries the next
candidate in score order. No in-place retry against a node that's
actively failing — wastes latency and doesn't help.

There is no "saturation wait": without a per-endpoint cap, saturation
isn't a routing-level state. If all eligible endpoints are genuinely
failing, `FetchBlocks` returns `"all N eligible endpoints failed"` and
the pipeline bisects (multi-block batch) or dead-letters (size 1).

## Retry, failover, and dead-letter

Each layer of the request path does ONE distinct job, with deliberately
narrow retry budgets so a single bad block doesn't amplify into hundreds
of HTTP calls.

### Layer map (a request's full lifecycle)

```
runWindow          (gap detection + coverage-tier slicing)
  └── indexGaps    (single producer; tip-first with interleave)
        ├── N fetchers (FIFO from jobs chan)
        │     └── processWithSplit  (BISECT on fetch failure)
        │           └── fetchBatch
        │                 └── MultiClient.FetchBlocks   ◄── FAILOVER by score
        │                       └── retryableCall       ◄── PER-NODE recovery only
        │                             └── http.Client.Do (shared Transport, HTTP/2)
        │     └── writeCh send (hand off parsed events to writers)
        └── M writers (drain up to 32 writeJobs per tx)
              └── persistJobs
                    └── handler.Persist + RecordRange + Heartbeat (all one tx)
```

### What each layer is allowed to retry

| layer | retries on | budget | why |
|---|---|---|---|
| `retryableCall` (per-node HTTP) | network err only (TCP reset, EOF, TLS, broken pipe, i/o timeout) | **1 in-place retry** (`httpRetryNetwork`) | covers the keep-alive-closed race; if the second attempt fails the node is genuinely sick |
| `retryableCall` | `5xx` | **0** — returns `*ErrServerError` immediately | sick nodes don't recover in 200 ms; failover is faster than waiting |
| `retryableCall` | `429` | **0** — returns `*ErrRateLimited` immediately | the node is telling us to back off; retrying it is counterproductive |
| `retryableCall` | `4xx ≠ 429` | **0** — returns `*HTTPStatusError` (or `*HeightPrunedError`) | request is malformed or the height is gone; no retry can fix it |
| `MultiClient.FetchBlocks` | every error type above | **N eligible endpoints**, walked in score order (see § Endpoint routing) | a different node may be healthy; this is the authoritative "I tried everyone" loop |
| `processWithSplit` | multi-block batch failure (transient) | **bisect halves until size 1** | isolate one bad block from an otherwise-healthy batch |
| Dead-letter sweep | size-1 failures still in `indexer_failures` | **`failure_max_retries` × 60 s ticks** (default 3) | slow-time recovery for whole-infra outages; flips `permanent=true` once exhausted |
| Permanent classifier (`HeightPrunedError`, `NoEndpointCoversError`) | nothing | **0** — short-circuits the bisect, records every height as `permanent=true` in one shot | retrying a pruned height across all endpoints is the same answer every time |

### Why we don't retry the whole `MultiClient` call

There is no outer retry wrapper. `MultiClient.FetchBlocks` already
exhausts every eligible endpoint before returning, so re-running it
just re-hits the same exhausted set. The pipeline gains parallelism
through workers + bisect and slow-time recovery through the dead-letter
sweep — neither needs an immediate retry of the failover loop.

### Worst-case amplification

For a single permanently-bad block hitting fully-saturated infra:

```
1 block × log₂(batch_size) bisect levels × N endpoints × (1 same-node retry on net err)
       × failure_max_retries dead-letter cycles
```

With `batch_size=50, N=3, failure_max_retries=3`: `~6 × 3 × 2 × 3 ≈ 108`
HTTP requests across the block's entire lifetime. Healthy blocks are
one request.

### Failover semantics — switch nodes, don't wait

Both 429 (`*ErrRateLimited`) and 5xx (`*ErrServerError`) are returned
**immediately** from the HTTP layer with no in-place retry, so
MultiClient's failover loop tries a different node on the next
iteration. The previous shape retried both error classes 4 times
against the same node before failing over, which delayed recovery by
seconds against a node that was already telling us it couldn't help.

Also, **per-request HTTP timeouts are classified correctly** now: Go's
`http.Client.Timeout` firing wraps `context.Canceled`, which used to
collide with the shutdown-canceled branch and propagate a fatal error
out of `processWithSplit`, crashing the whole indexer. The pipeline
now checks `ctx.Err()` on the OUTER context to distinguish "we're
shutting down" from "one request took too long" — the latter falls
through to bisect / dead-letter like any other transient failure.

The fast-fail rule is: **any error class that won't recover in ~200 ms
is somebody else's problem** — failover, bisect, or the dead-letter
sweep, depending on the error class.

### Finding the capacity of a node

Run the one-shot sweep (probes 1 → 128 concurrency and prints a curve per
endpoint, then exits):

```bash
./bin/indexer -config config.yml -benchmark
```

Output looks like:

```
=== https://rpc1 ===
concurrency   req/s      p50       p99     errors  recommendation
1             24.8       40ms      55ms    0       ↗ gaining
4             94.2       42ms      60ms    0       ↗ gaining
16            310.5      51ms      120ms   0       ↗ gaining
32            420.1      76ms      280ms   0       ≈ flat
64            438.9      146ms     890ms   0       ≈ flat
128           390.4      328ms     2100ms  7       ↘ knee likely passed
```

The "≈ flat" rows are where you're at the node's ceiling. In the owned-
infra model there's no per-endpoint cap to pin, but the curve still
informs `fetch_workers`: pick a process-wide value whose share per
endpoint (roughly `fetch_workers / healthy_endpoints`) sits on the flat
plateau for the slowest endpoint.

## Performance knobs

All in `config.yml` under `indexer:` unless noted:

- `fetch_workers` — parallel fetchers. Each worker holds one in-flight
  HTTP request at a time; total in-flight across all endpoints is bounded
  by this. **`0` = adaptive** (sized at startup to `min(CPU×8, mem/2 MiB,
  hard_max)`; `hard_max` defaults to 256). 16-32 is a reasonable fixed
  value on a single-endpoint config; 64-128 when you have owned infra
  with multiple endpoints to spread across.
- `fetch_batch_size` — heights per JSON-RPC batch POST. 50 = 100 methods
  per request (block + block_results × 50). **`0` = adaptive** (sizer
  starts at 20, shrinks ×0.5 when the endpoint error rate exceeds 3 %).
  Raise toward 100-200 on fast owned archive nodes; the wire savings
  compound. Drop if your upstream starts returning 5xx at that size.
- `write_batch_rows` — rows per `CopyFrom`. Higher = fewer tx commits,
  more memory.
- `queue_depth` — bound on the producer→fetcher channel. Backpressure.
- `tip_confirmations` — how many blocks below head to stay at (reorg safety).
  Set to 0 only if you know the chain is reorg-free.
- `database.pool_size` — Postgres connection-pool upper bound.
  **Writer count is derived as `pool_size - 4`** (reserving 4 connections
  for the status handler, dead-letter sweep, and fetcher's
  `HandlerNeedsRange` probe). Increase alongside `fetch_workers` when
  throughput is DB-bound; watch `pg_stat_activity` for saturation
  (`active` count near `pool_size`). Target `max_connections - 10` on the
  Postgres side so graphile + internals fit.

## Adding a new event

1. Create a package `internal/events/<your_event>/handler.go` that
   implements `events.Handler`:
   ```go
   type Handler struct { /* … */ }

   func (h *Handler) EventTypes() []string { return []string{"lava_delegator_reward"} }
   func (h *Handler) DDL() []string        { return []string{/* CREATE TABLE ... */} }
   func (h *Handler) Persist(ctx, tx, events) error { /* parse + CopyFrom */ }
   ```
2. Register it in `cmd/indexer/main.go`:
   ```go
   reg.Register(delegator_reward.New(cfg.Database.Schema))
   ```
3. `docker compose up -d --build` — DDL runs at startup, new events start
   indexing on the next batch.

The handler gets a fresh `pgx.Tx` for each batch. Writing goes into that tx,
so your rows land atomically with the range-merge.

## Snapshotters

Handlers react to events that appear in the block stream. Some jobs
don't fit that shape: they pull state from the chain on a **calendar
schedule** (monthly, weekly) and record what the chain answered at that
moment. For those we have a parallel seam: `internal/snapshotters`.

A snapshotter implements:

```go
type Snapshotter interface {
    Name() string
    DDL() []string
    BlocksDue(ctx, pool) ([]SnapshotTarget, error)
    Snapshot(ctx, tx, target) error
}
```

The registry runs `BlocksDue` on every tick (default 10 min) and
dispatches `Snapshot` inside a fresh `pgx.Tx` per target. A failure on
one target logs and continues — one bad snapshot never starves the
others. DDL applies at startup, same as handler DDL.

### Built-in: `provider_rewards`

Records Lava's `estimated_provider_rewards` at the 17th of every month,
15:00 UTC. For each snapshot date we binary-search the chain for the
block closest to that timestamp, then fetch estimated rewards for every
provider in `app.providers` at that block height. Results go into:

- `app.provider_rewards_snapshots` — one row per (date, block),
  `status` column is `ok` / `failed`.
- `app.provider_rewards` — fact rows, `(block_height, provider_id,
  spec_id, source_kind, denom)` composite PK, amounts stored as
  `NUMERIC(40, 0)` (no precision loss vs. the chain's decimal strings).

Operator setup:

```yaml
snapshotters:
  handlers: ["all"]       # same shape as indexer.handlers; [] also means all
  check_interval: 10m
  provider_rewards:
    earliest_date: "2025-01-17"
    concurrency: 25
    rest_url: ""          # optional; falls back to the first REST endpoint
    rest_headers: {}      # optional; falls back to that endpoint's headers
```

The REST endpoint **must be archive-backed** — estimated-rewards queries
historical heights (18+ months in some cases). If your main REST
endpoint is pruning, set `rest_url` to a dedicated archive node.

Progress is visible in the web UI's "Snapshotters" card (one dot per
expected date — green covered / red failed / grey missing) and via
`GET /api/snapshotters`.

Env overrides: `SNAPSHOTTERS_HANDLERS` (comma-separated — `all` /
`provider_rewards`), `SNAPSHOTTERS_CHECK_INTERVAL`,
`PROVIDER_REWARDS_EARLIEST_DATE`, `PROVIDER_REWARDS_CONCURRENCY`,
`PROVIDER_REWARDS_REST_URL`.

### Adding a new snapshotter

1. Create a package under `internal/snapshotters/<your_snap>/` that
   implements `snapshotters.Snapshotter`.
2. Add a `cfg.Snapshotters.YourSnap` section to `internal/config/config.go`
   with whatever schedule/target knobs you need — **no** `enabled` flag;
   selection is done via `snapshotters.handlers` to match the handler pattern.
3. Register it unconditionally in `cmd/indexer/main.go` via the
   `registerSnapshotter` helper — it consults `cfg.Snapshotters.WantsSnapshotter`
   and skips anything not in the list.
4. DDL applies at startup; RunLoop runs `BlocksDue` + `Snapshot` on
   every tick.

## Priced denoms deriver

`internal/denoms` keeps `app.priced_denoms` populated from whatever
raw denoms land in `app.provider_rewards`. One goroutine, 5 min
ticker, idempotent on every pass.

- **What it does.** `SELECT DISTINCT denom FROM app.provider_rewards`,
  resolve each raw denom to a base form (`ulava` → `lava`,
  `ibc/<hash>` → IBC trace lookup → base, otherwise lowercase-trimmed),
  upsert into `app.priced_denoms`. New denoms land with
  `coingecko_id = NULL`; existing ones just bump `last_seen_at`.
- **Seeded mappings.** 23 known Cosmos tokens ship with
  `base_denom → coingecko_id` applied on startup (`ON CONFLICT DO
  UPDATE`). Adding a new mapping later is one SQL statement — no
  redeploy.
- **Adding a coingecko_id for a new denom.**

  ```sql
  UPDATE app.priced_denoms
     SET coingecko_id = 'some-new-token'
   WHERE base_denom = 'snt';
  ```

- **`coingecko_id IS NULL` is a monitoring hook.** The deriver logs a
  WARN every time it lands a new base denom without a mapping, and
  the dashboard's **Priced denoms** card renders an "N unmapped"
  expandable list for a direct ops view. Alert on
  `SELECT COUNT(*) FROM app.priced_denoms WHERE coingecko_id IS NULL`
  if you care about price coverage.
- **Disable.** `denoms.deriver.enabled: false` (or
  `DENOMS_DERIVER_ENABLED=false`) skips DDL entirely and runs no
  goroutine.

## Aggregates: MVs and rollups

Every `*.sql` file under `aggregates/` is executed at startup in filename
order. Make statements idempotent (`IF NOT EXISTS`).

`aggregates.yml` declares pg_cron refresh schedules:

```yaml
refresh:
  mv_relay_daily:
    sql: "REFRESH MATERIALIZED VIEW CONCURRENTLY app.mv_relay_daily"
    schedule: "*/5 * * * *"
```

If pg_cron isn't installed, DDL still runs and scheduling is skipped with a
warning — refresh manually via `SELECT ...`.

For **incremental rollup tables**: create a `TABLE` in a SQL file, then in
a handler's `Persist`, UPSERT the delta into the rollup in the same
transaction that writes the raw rows. Since every row-write is already
in-tx, the rollup stays consistent.

## Configuring endpoints

`network.endpoints` is a list — mix and match as many as you want. The
MultiClient probes each on startup, records `(earliest, latest)`, and only
routes a batch to endpoints whose window covers it. A pruning private
node + a public archive fallback just works out of the box.

Each entry has:

- `kind: rpc` (recommended) — Tendermint JSON-RPC 2.0. One HTTP POST
  batches `block` + `block_results` for N heights. Captures every event
  including begin/end_block.
- `kind: rest` — Cosmos LCD. No batch; one request per block. Only
  captures tx-scoped events (fine for `lava_relay_payment`, drops
  begin/end_block silently). Typically SLOWER than RPC — the LCD wraps
  RPC under the hood.
- `url` — base URL.
- `headers` (optional) — arbitrary HTTP headers sent on every request
  to this endpoint. See § Per-endpoint headers.

### Per-endpoint headers

Many hosted RPC providers route on headers, and almost all paid ones
auth on them. Examples:

```yaml
endpoints:
  - kind: rpc
    url: https://lava.tendermintrpc.lava.build:443
    headers:
      lava-extension: archive           # Lava public gateway: serve from archive nodes
  - kind: rest
    url: https://rpc.my-paid-provider.com
    headers:
      Authorization: "Bearer eyJhbGciOi..."
      x-api-key: "sk_live_..."
```

Values pass through verbatim. The client always sets `Content-Type` /
`Accept` itself (last-write wins), so a typo in config can't break the
wire format.

### `earliest_mode`

Instead of hard-coding `start_height`, set `earliest_mode: true` and the
indexer resolves the start block at runtime to the **minimum earliest
block** across healthy endpoints. Useful when your fleet changes over
time (pruning horizon advances) and you just want to cover as much
history as the current fleet allows.

### Selecting which handlers run

`indexer.handlers` is a whitelist of handler names (or `["all"]` /
empty = every compiled-in handler). Unlisted handlers are not
registered, so they also don't record coverage in `indexer_ranges` —
good for experimenting with a new event type on a subset of the chain
without touching others. Also settable via `INDEXER_HANDLERS=name1,name2`.

## Progress UI

The indexer's own HTTP listener (default `:8080`) serves:

- `GET /` — single-page UI. Overview tab shows stat cards, a compact
  per-endpoint list (kind-coloured badge, health, coverage), and
  per-handler coverage timelines (green bars = indexed, red bars =
  dead-lettered heights). Endpoints tab shows per-endpoint AIMD
  budget / in-flight / rps / p50-p95-p99 / errors with Grafana-style
  sparkline history. GraphQL tab iframes GraphiQL (reverse-proxied,
  same origin — no CORS dance).
- `GET /api/status` — JSON consumed by the UI; also useful for CI /
  external dashboards.
- `GET /healthz` — liveness probe (200 OK, always).

UI updates are done **in-place** on a keyed DOM (Map<url, refs> / the
handler equivalent) — no full re-render on poll, no sparkline flicker.

Disable via `web.enabled: false` in config or `WEB_ENABLED=false`.
Disable just the GraphQL tab with `graphql.enabled: false`.

## Production deployment (external Postgres)

For deployments where you bring your own managed Postgres, use
`docker-compose.prod.yml` — indexer only, no bundled DB. Everything is
overridable via env:

```bash
DB_HOST=my-postgres.internal \
DB_USER=info_indexer \
DB_PASSWORD=... \
DB_SSLMODE=require \
LAVA_RPC_ENDPOINTS=https://rpc1,https://rpc2 \
docker compose -f docker-compose.prod.yml up -d
```

### Resource-aware by default

The binary sizes itself to whatever cgroup limits the container is given,
so you can't OOM it by giving it less RAM than expected:

- `go.uber.org/automaxprocs` — `GOMAXPROCS` is set at init to match the
  cgroup CPU quota. No more Go defaulting to the host's physical core
  count when the container is capped at 1 vCPU.
- `github.com/KimMachineGun/automemlimit` — `GOMEMLIMIT` is set to 90% of
  the cgroup memory so Go's GC paces itself before the kernel OOM-kills
  the container.

Both are safe outside a cgroup (log a one-line note, otherwise no-op).

### Health & shutdown

- `GET /healthz` returns 200 OK — point your load balancer / platform
  health check at it.
- SIGTERM triggers a graceful shutdown. In-flight batches either finish
  committing their tx or roll back cleanly. No partial writes.

### Secrets

Inject `DB_PASSWORD` from whatever secret store your platform provides
(Secrets Manager, Vault, SOPS-decrypted env, etc). Never commit it to
`config.yml` or the compose file.

## GraphQL

PostGraphile auto-generates the API from the Postgres schema. Examples:

```graphql
# Daily totals over the last N days
query {
  mvRelayDailies(
    filter: { date: { greaterThanOrEqualTo: "2026-01-01" } }
    orderBy: DATE_ASC
  ) {
    nodes { date chainId provider cu relays }
  }
}

# Aggregates grouped by chain
query {
  mvRelayDailies {
    groupedAggregates(groupBy: CHAIN_ID) {
      keys
      sum { cu relays }
    }
  }
}
```

Querying the raw `relayPayments` table directly works too, but prefer the MV
for anything over a small range — the raw table has no secondary indexes by
design.

## CI / CD

`.github/workflows/` ships four pipelines:

- **`ci.yml`** — runs on every push to `main` / `mainnet` / `testnet` and
  every PR targeting `main`. `go mod download` → `go vet ./...` →
  `go build ./...` → `go test ./...`. Concurrency-grouped on the ref
  with `cancel-in-progress: true`, so outdated PR pushes are cancelled.
- **`build.yml`** — runs on push to `main`. Assumes an OIDC role in
  `secrets.AWS_ROLE_ARN` with access to an ECR repo named
  `lava-indexer`. Skips the Docker build if an image tagged with the
  current SHA already exists, otherwise builds via
  `docker/build-push-action@v6` with GHA cache (`type=gha`) and pushes
  `:<sha>` plus `:latest`.
- **`publish.yml`** — manual (`workflow_dispatch`). Same image-build
  logic as `build.yml`, for ad-hoc re-publishes without a push.
- **`deploy.yml`** — manual, takes an `environment` (`mainnet` /
  `testnet`) and a `tag` input (default `latest`). Pulls the current
  ECS task definition for `lava-indexer-<env>-indexer`, rewrites its
  container image to point at the chosen ECR tag, registers a new
  task-def revision, and `update-service`s the `indexer` service to
  roll onto it.

All AWS actions use OIDC (no long-lived keys): the IAM role assumed by
the workflow is passed via `secrets.AWS_ROLE_ARN`, and permissions on
each job include `id-token: write` so GitHub can exchange a JWT with
AWS STS.

## License

TODO
