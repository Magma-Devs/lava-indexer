# Severity calibration

How to assign severity without inflating or deflating. Applies to every
reviewer; the synthesiser uses the same ladder to resort merged findings.

## The question to ask

For every finding, the reviewer answers in order:

1. **Is there a concrete failure mode?** (e.g. "a user submits input X and it
   causes Y"). If the answer is "well, in theory…" or "it would be cleaner
   if…" — it's at most `low`. Probably `info`.

2. **What's the blast radius?** One user? All users? The whole DB? The
   whole cluster? Larger blast radius pushes up the ladder.

3. **Is the trigger realistic?** "An attacker crafts a 2 GB payload" is
   realistic on a public API; "an internal tool sends malformed YAML" is
   less so. Unrealistic triggers cap severity at `medium` even if the
   damage would be large.

4. **Is it reachable?** Dead code with a bug is `info`. Live-path code
   with a latent bug is `high`. A bug that fires on every request is
   `critical` if the damage is large enough.

## Concrete examples from this codebase's domain

| Finding | Severity | Why |
|---|---|---|
| `RecordFailure` interpolates `handler` into SQL via `fmt.Sprintf` | `critical` if `handler` is user-derived; `info` if it's a compile-time constant | Blast radius is DB; trigger is "any path feeding user input into handler" |
| `pgxpool` sized to 8 but 48 concurrent workers attempt transactions | `high` | Correctness: workers block on pool acquisition, tx backlog grows unbounded |
| Missing `ctx` in `retryableCall`'s inner `time.Sleep` | `high` | Under ctx cancel, goroutine sleeps for full jitter window before exiting — resource leak on shutdown |
| Handler registration reads a map iteration order for DDL ordering | `medium` | Non-deterministic ordering; a second handler with dependencies would break |
| `slog.Info` in hot path allocates on every call even with level = warn | `medium` | Measurable perf cost on 200bps backfill |
| Named return `err error` unused in `func` with single return statement | `low` | Style nit, no real cost |
| `// TODO` on a completed feature | `info` | Housekeeping |

## When to down-rank

- **Transient correctness:** a retry loop already covers the failure →
  drop by one level (the real fix is the retry loop, not this site).
- **Framework guarantee:** pgx parameterises `$1` always → don't flag
  the `$1`-bound query as SQL-injectable.
- **Compile-time constants:** string interpolation of a literal into SQL
  is not SQL injection; it's a style preference.

## When to up-rank

- **Observed in production:** the code committed fixes an incident →
  any regression of the same pattern is `high` minimum.
- **Concurrent path:** a bug that only fires under concurrency usually
  goes up one level because the trigger is guaranteed in real load.
- **Silent failure:** a bug that doesn't log or raise an error goes up
  one level — it's harder to detect, which extends blast radius.
