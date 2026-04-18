# Go performance audit — what to look for

Concrete perf issues a senior Go reviewer would flag in a throughput-
sensitive service (indexer, pipeline, data plane). Micro-optimisations
without evidence (e.g. "use strings.Builder here") belong at `low`
severity. Observable-impact issues belong higher.

## Hot-path allocations

- **Per-request allocations in a ≥ 1k rps path**: struct literals,
  slice copies, map creations. The compiler's escape analysis
  (`go build -gcflags='-m'`) shows which allocations escape to heap.
  Treat `&T{}` in a loop as a finding unless the lifetime crosses the
  loop.
- **Repeated `[]byte(s)` / `string(b)` conversion**: these allocate.
  In a log or metric path, they add up fast.
- **Logger format args**: `slog.Info(msg, "key", expensiveCall())`
  evaluates `expensiveCall()` even when the log level filters the
  message. Use `slog.LogAttrs` with `slog.Any` lazily, or guard with
  `slog.Enabled`.
- **JSON encoding**: `json.Marshal` re-compiles struct metadata on
  first call. For hot paths, `encoding/json`'s reuse via
  `json.NewEncoder(w).Encode(v)` is marginally better; for real
  performance, see `bytedance/sonic` or `goccy/go-json`.

## Concurrency cost

- **Unbuffered channels with slow consumers**: each send blocks. Under
  load, sender goroutines pile up. Check every `chan T` for buffer
  size and consumer velocity.
- **Goroutine-per-request**: fine on small services, a disaster at
  10k rps. Count goroutines via `runtime.NumGoroutine()` in steady
  state; unexpected growth = leak.
- **Mutex contention**: a single global mutex on a hot data structure
  is a scaling limit. `sync.RWMutex` where reads dominate; shard the
  structure otherwise.
- **`sync.Pool` misuse**: pools only help if the allocations are
  large AND frequent. For small allocations (< 100 bytes), the
  pool overhead exceeds the savings. For allocations held longer
  than one request, pools leak.
- **atomic vs mutex**: `atomic.Int64.Add` is vastly cheaper than
  `mu.Lock(); x++; mu.Unlock()` for a counter. Flag counters that
  use a mutex unnecessarily.

## pgx / Postgres

- **`CopyFrom` for bulk**: inserting 10k+ rows via `INSERT` in a
  loop is an order-of-magnitude loss vs. `pgx.CopyFrom`. Any batch
  insert of > 1000 rows using exec/plain insert is a finding.
- **Pool sizing**: `pgxpool.MaxConns` should exceed concurrent
  callers. If you have 48 indexer workers and a pool of 8, 40
  workers block on pool acquisition permanently.
- **`SELECT count(*)` on big tables**: unbounded scan; use
  `pg_class.reltuples` for estimates or partial counts.
- **N+1 queries**: a `for` loop that runs `pool.QueryRow` per iteration
  is a classic miss. Batch via `= ANY($1)` or a JOIN.
- **Wide `SELECT *`** with TOAST columns pulls unused blobs across
  the wire. Select explicit columns.
- **Prepared statements**: pgx auto-prepares on repeated queries;
  constructing the SQL with `fmt.Sprintf` for schema prefix defeats
  caching. Use placeholders where possible.
- **Transaction held too long**: a `tx` that does HTTP fetches or
  CPU-heavy work between `Begin` and `Commit` holds row locks for
  seconds. Separate I/O from the transaction.
- **Long-running `LISTEN` / cursors**: watch for cursors not closed
  in all paths.

## HTTP client

- **Default transport reuse**: `http.Client{Transport: nil}` reuses
  `http.DefaultTransport`, which is shared across the process. Fine,
  but tuning `MaxIdleConnsPerHost` per-client requires a custom
  transport.
- **Keep-alive disabled**: `DisableKeepAlives: true` destroys
  connection reuse. Should almost never be set unless debugging.
- **Connection reuse**: failure to `io.Copy(io.Discard, resp.Body);
  resp.Body.Close()` on every response (even errors) breaks
  connection reuse. The pool drops the connection.
- **Retry amplification**: retries × concurrent callers × endpoints
  can explode outbound request rate. Check for exponential backoff
  and cap on total attempts.

## Resource leaks

- **Goroutine leaks on cancel**: a goroutine that reads from a
  channel but has no `ctx.Done()` case will live forever if the
  sender never closes the channel. Check every `for` over a channel
  in a goroutine for a `select` with ctx.
- **Timer leaks**: `time.NewTicker(...)` must be `Stop()`'d. `time.After`
  in a loop can stack up. Use `time.NewTimer` with `Reset` in loops.
- **File handle leaks**: every `os.Open` needs a matching `Close`,
  idiomatically via `defer`. Watch for loops that open files without
  closing them inside the iteration.

## Indexer-specific perf

- **Batch size vs RTT**: larger RPC batches amortise the HTTP
  round-trip but make a single failure more expensive (whole batch
  retried). Sweet spot is usually 10-50. `fetch_batch_size: 1` is a
  throughput killer.
- **AIMD budget collapse**: the adaptive controller can flatten to
  min under sustained noise. Check the shrink thresholds aren't
  triggered by normal jitter.
- **Dead-letter amplification**: if a failure rate > 1% causes every
  failed height to be re-queued on every sweep, that's a compounding
  load multiplier. Bound the sweep frequency and retry count.
- **Gap fragmentation**: 14k small ranges in `indexer_ranges` turns
  every `Gaps()` query into an expensive scan. Merging ranges under
  an advisory lock helps; check the merge is actually running.

## Memory

- **Huge allocations**: allocating a slice of `make([]T, largeN)`
  when `largeN` comes from user input is a DoS vector.
- **GOMEMLIMIT**: `KimMachineGun/automemlimit` sets this from cgroup;
  without it, the Go GC will try to use the full host memory and
  OOM-kill inside a capped container.
- **GOMAXPROCS**: `go.uber.org/automaxprocs` scales to cgroup CPU
  quota; missing this in a container gives you host-scale
  parallelism that the scheduler can't deliver.
- **Slice growth**: `append(s, v)` on a cold slice doubles capacity
  each growth. Pre-size slices when the final length is known.
