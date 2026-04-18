# Go architecture review — what to look for

Checklist applied when reviewing the lava-indexer codebase (or any Go
service of similar shape). Emphasis on issues a senior Go engineer
would flag in a code review; ignore advice that's only meaningful in
enterprise Java or other ecosystems.

## Module boundaries

- **Package cohesion**: every file in a package should describe the
  same concept. A `state` package mixing DB schema management with HTTP
  clients is a smell — split it.
- **Dependency direction**: packages should form a DAG. Import cycles
  are a compile error, but one-way chains that look like cycles
  (A → B, then anyone edits B to need a helper from A) are silent
  architecture decay. Check `go list -m -deps ./... | grep cycle`.
- **Public surface**: exported identifiers (`PascalCase`) are a public
  contract. If a package exports 40 names but only 3 are used from
  outside, the other 37 are accidental surface.

## Dependency injection

- Constructors take dependencies as arguments; do NOT have constructors
  reach into globals (`os.Getenv`, `http.DefaultClient`).
- Interfaces should be declared by the consumer, not the provider. A
  handler that needs "something that can fetch a block" declares its
  own minimal `BlockFetcher` interface; the RPC package returns a
  concrete `*RPCClient` that satisfies it.
- **Test seams**: can the code be tested without spinning up Postgres?
  If every unit test requires Docker, something too concrete is in the
  wrong place.

## Concurrency architecture

- **Ownership**: for every goroutine, who owns its lifetime? Who closes
  the channels it reads from? If the answer isn't obvious at review
  time, it's a bug waiting to happen under cancellation.
- **Fan-out/fan-in via `errgroup`**: check that every `g.Go(...)` call
  has a clear exit condition, and that `g.Wait()` is actually reached
  on all paths (including `ctx` cancel).
- **Context propagation**: every blocking call (HTTP, DB, sleep, channel
  send to unbuffered channel) should have a `ctx` path. Missing `ctx`
  = potential leak on cancel.

## State and persistence

- **Transactions**: a single DB transaction should be a semantic unit
  of work. If writes + range-merges are in separate transactions,
  they can desynchronise on crash. Look for "write X and then record
  that X was written" patterns and confirm both are in the same `tx`.
- **Schema changes in code**: `CREATE TABLE IF NOT EXISTS` in
  application startup is OK for small projects. For anything serious,
  dedicated migrations with versioning (goose, sqlmigrate, atlas) are
  expected. Flag the trade-off if you see growing DDL-in-code.
- **Idempotency**: if a handler can be invoked twice for the same
  input (retry, at-least-once delivery), its write operations must be
  idempotent (ON CONFLICT DO ...).

## Error handling architecture

- **Error types as API**: if a caller needs to `errors.Is` or
  `errors.As` to distinguish error classes, those classes must be
  exported types. `ErrRateLimited` as a sentinel or struct is OK; an
  unexported `errTransient` that's handled differently from an
  unexported `errPermanent` is a bug in waiting.
- **Error wrapping**: `fmt.Errorf("context: %w", err)` preserves the
  chain; `fmt.Errorf("context: %v", err)` drops it. The latter is fine
  only if the caller explicitly doesn't care about root-cause tagging.
- **Panic vs error**: panic is for "this program is broken"; error is
  for "this operation failed". Panic in a production request path is
  a regression.

## Module boundaries specific to indexers

- **Fetch / parse / persist**: these are three distinct responsibilities
  that should live in three different packages so they can be tested
  independently and mixed (e.g. fake RPC + real DB).
- **Handler registry vs pipeline**: the pipeline owns scheduling and
  concurrency; handlers own domain semantics. The registry is the
  boundary between the two. A handler that reaches into pipeline
  internals is a leak.
- **State table**: coverage tracking (`indexer_ranges`) should be
  blind to what any specific handler does with the data. If the
  state package imports the relay_payment handler, that's wrong.

## Testability red flags

- Functions that take concrete types where an interface would do
  ("hard to mock").
- Functions that construct their own dependencies (`http.DefaultClient`,
  `time.Now()`, `os.Hostname()`).
- `init()` functions that touch globals or the filesystem.
- Missing tests for the "hot" path — the exact code path users hit.

## Code you'd expect in a serious Go service

- `internal/` for anything not intended as public API.
- `cmd/` for binaries, one per binary.
- `pkg/` for intentional public library code (optional — many services
  skip this entirely).
- No top-level `utils/` package. If a helper is general enough to be
  "utility", it belongs in the concrete package that uses it until it
  has 2-3 real consumers.
