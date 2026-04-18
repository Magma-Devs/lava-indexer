---
name: performance-analysis
description: Analyze Go code for performance bottlenecks — hot-path allocations, goroutine leaks, channel buffering, mutex contention, pgx / Postgres query patterns (N+1, missing CopyFrom, pool sizing), HTTP client reuse, retry amplification, memory growth, and indexer-specific issues (batch sizing, AIMD tuning, dead-letter amplification, range fragmentation). Invoked when the performance-analyst agent reviews code, a diff, or a PR; use this for "performance review", "find bottlenecks", "profile this", "slow", "scaling issue", "memory leak", "goroutine leak", "DB query pattern", "pool sizing", etc. Triggered implicitly when the code-review or code-development orchestrator fans out to its performance role.
---

# Performance analysis

You are analysing Go code for performance issues. Output is a single
markdown file of findings.

## Scope

Review the files/diff passed to you. If no specific scope is given, default
to `git diff main...HEAD -- '*.go'`. For whole-repo reviews, start with the
pipeline's hot path (`internal/pipeline/*.go`, `internal/rpc/*.go`,
`internal/state/*.go` for this codebase).

## Methodology

Walk the code in this order:

1. **Identify the hot paths** from the entry point. For lava-indexer:
   `pipeline.Run → runWindow → indexGaps → processWithSplit →
   processBatch → handler.Persist`. Code outside the hot path gets
   lower priority.
2. **Grep for expensive-in-a-loop patterns**:
   - `append` without pre-sized slice in a known-N loop
   - `json.Marshal` per iteration (consider streaming)
   - `pool.QueryRow` inside a `for` over a slice (N+1)
   - `fmt.Sprintf` in a ≥ 1k rps path
3. **pgx specifics**: pool size vs concurrent callers, CopyFrom for
   bulk, transaction duration vs I/O inside tx.
4. **Concurrency cost**: channel buffer sizes, goroutine lifecycles,
   mutex vs atomic.
5. **HTTP client reuse**: retries × concurrency amplification,
   connection pool exhaustion, body drain-and-close.
6. **Resource leaks**: timers, tickers, file handles, goroutines on
   cancel.

Load `references/go-performance-patterns.md` for the detailed checklist
plus indexer-specific considerations (batch size, AIMD, dead-letter).

## Output

Write findings to `_workspace/performance_findings.md` using the
finding schema.

- Severity depends on blast radius: a perf bug in cold code is
  `low`-`medium`. Same bug in the hot path is `high` or `critical`.
- Quantify impact when you can: "10k allocations per batch ≈ 1 MB/s
  of garbage at 200 bps". Rough numbers with reasoning beat vague
  "slow".
- "Could benchmark this" is not a finding by itself — state the
  concern, let the reader decide whether to benchmark.

## What performance analysis is NOT

- "This code is ugly" → style-reviewer.
- "This code is architecturally wrong" → architect-reviewer. (An
  architecturally-bad module may also perform badly; flag the perf
  consequence, let architect-reviewer flag the structural root
  cause.)
- "This code is unsafe" → security-auditor.
