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

Concurrent writers to `indexer_ranges` take a
`pg_advisory_xact_lock(hashtext(handler))` before merging, so two workers
converging on touching-but-unmerged ranges can't fragment the state.

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

With the defaults (`fetch_workers: 0`, `fetch_batch_size: 0`, both adaptive)
the indexer sizes itself to the fleet and breathes with the nodes — see
§ Adaptive concurrency below. With explicit values
(`fetch_workers: 16`, `fetch_batch_size: 10`) it keeps up to **16
concurrent HTTP POSTs in flight**, each carrying 20 JSON-RPC method calls
(block + block_results × 10 heights). Requests are routed across endpoints
by live headroom, so a saturating node naturally sheds load to a quieter
peer.

The UI's Endpoints tab shows live per-endpoint load and sparkline history
for rps / p99 / AIMD budget / in-flight:

```
✓ [RPC] https://rpc1        earliest …  latest …
  budget 42   in-flight 7 / peak 12   req/s 48.3   p50/p95/p99 65/110/210ms   errors 0/4812
```

If p99 climbs past ~8× p50 the AIMD controller pulls the budget back
automatically; you only need to intervene if the controller is stuck at
the minimum (= node is too slow to use) or the error rate is persistently
> 1 % (= node is unhealthy).

## Adaptive concurrency (AIMD)

Each endpoint has its own **additive-increase / multiplicative-decrease**
controller gating how many requests the MultiClient will keep in flight
against it. Why AIMD: different nodes have different capacity — a premium
private RPC and a public gateway don't deserve the same worker budget,
and node health varies minute-to-minute (upstream load, GC pauses,
restarts). A static `fetch_workers` number is always wrong for _some_
endpoint.

The tick (every 3s) reads live metrics (rolling p50/p99 latency, error
rate, utilisation) and moves the per-endpoint `budget`:

- **grow** (+2) when p99 < 3× p50, utilisation ≥ 70 %, errors < 1 %
- **shrink** (× 0.75) when p99 > 8× p50 or error rate ≥ 5 %
- clamped to `[min, max]` (defaults 4 and 64)
- the first 20 requests after startup count as **warmup** and can't
  trigger shrinks, so cold TLS/TCP handshakes don't collapse the budget

Routing is **headroom-aware**: before dispatching a batch, eligible
endpoints are sorted by `budget − in_flight` descending, so the least
loaded node takes new work first and one slow peer can't monopolise the
queue.

When `fetch_workers: 0` the process picks `Σ endpoint.max_concurrency`
workers on startup — plenty for every endpoint to hit its AIMD ceiling
in parallel. Extra workers just backpressure gracefully instead of
over-loading a node.

## Retry, failover, and dead-letter

Each layer of the request path does ONE distinct job, with deliberately
narrow retry budgets so a single bad block doesn't amplify into hundreds
of HTTP calls.

### Layer map (a request's full lifecycle)

```
runWindow          (gap detection + tier slicing)
  └── indexGaps    (single producer; tip-first with interleave)
        └── worker (N goroutines, FIFO from jobs channel)
              └── processWithSplit  (BISECT — recurses halves on failure)
                    └── processBatch (fetch + write tx)
                          └── MultiClient.FetchBlocks   ◄── FAILOVER across endpoints
                                └── retryableCall       ◄── PER-NODE recovery only
                                      └── http.Client.Do
```

### What each layer is allowed to retry

| layer | retries on | budget | why |
|---|---|---|---|
| `retryableCall` (per-node HTTP) | network err only (TCP reset, EOF, TLS, broken pipe, i/o timeout) | **1 in-place retry** (`httpRetryNetwork`) | covers the keep-alive-closed race; if the second attempt fails the node is genuinely sick |
| `retryableCall` | `5xx` | **0** — returns `*ErrServerError` immediately | sick nodes don't recover in 200 ms; failover is faster than waiting |
| `retryableCall` | `429` | **0** — returns `*ErrRateLimited` immediately | the node is telling us to back off; retrying it is counterproductive |
| `retryableCall` | `4xx ≠ 429` | **0** — returns `*HTTPStatusError` (or `*HeightPrunedError`) | request is malformed or the height is gone; no retry can fix it |
| `MultiClient.FetchBlocks` | every error type above | **N eligible endpoints**, walked in headroom-sorted order | a different node may be healthy; this is the authoritative "I tried everyone" loop |
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

With `batch_size=20, N=2, failure_max_retries=3`: `5 × 2 × 2 × 3 = 60`
HTTP requests across the block's entire lifetime. Healthy blocks are
one request.

### Failover semantics — switch nodes, don't wait

Both 429 (`*ErrRateLimited`) and 5xx (`*ErrServerError`) are returned
**immediately** from the HTTP layer with no in-place retry, so
MultiClient's failover loop tries a different node on the next
iteration. The previous shape retried both error classes 4 times
against the same node before failing over, which delayed recovery by
seconds and burned the AIMD budget on a node that was already telling
us it couldn't help.

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

The "≈ flat" rows are where you're at the node's ceiling. Pick the lowest
`fetch_workers` value that sits on the flat plateau for headroom.

## Performance knobs

All in `config.yml` under `indexer:`:

- `fetch_workers` — parallel RPC fetchers. Each worker holds one in-flight
  HTTP request at a time. **`0` = adaptive** (sized at startup to the sum
  of per-endpoint AIMD ceilings, and naturally clamped by CPU / memory
  cgroup limits). 16 is a reasonable fixed value for a dedicated box.
- `fetch_batch_size` — blocks per JSON-RPC batch POST. 10 = one HTTP round-trip
  fetches 20 methods (block + block_results for 10 heights). **`0` =
  adaptive** (AdaptiveSizer shrinks × 0.5 on failure, grows +1 on
  sustained success). Raise on a fast RPC, lower on a flaky one.
- `write_batch_rows` — rows per `CopyFrom`. Higher = fewer tx commits,
  more memory.
- `queue_depth` — bound on the fetcher→writer channel. Backpressure.
- `tip_confirmations` — how many blocks below head to stay at (reorg safety).
  Set to 0 only if you know the chain is reorg-free.

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
