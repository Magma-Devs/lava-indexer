# chain_id mismatch → runtime disable (was: fail-fast abort)

## What changed from the previous version

The previous implementer built chain_id validation as a fail-fast: any
mismatch or "didn't report" caused Probe to return an aggregated error
and the process to exit. The user revised the requirement: treat
mismatch the same way as a transient probe failure — flag the endpoint
as `Disabled` with a reason string, log WARN, continue. Startup only
aborts when ZERO endpoints are left healthy, preserving the existing
fail-fast for "nothing to index from".

## Files touched

- `internal/rpc/multi.go`
  - Added `Reason string` to `Endpoint`. Set whenever `Disabled = true`;
    zero-value on healthy endpoints.
  - `Probe(ctx, expectedChainID)` rewritten: no more `mismatches` slice,
    no more aggregate error. All four disable paths (probe error,
    empty Network under validation, mismatch, and the pre-existing
    probe failure) now set both `Disabled` and `Reason`. Only returns
    an error when zero endpoints remain healthy.
  - Dropped the `strings` import (no longer building a joined error).
  - Probe-failure path now carries `"probe failed: <err>"` as Reason —
    previously the `✗` on the dashboard was unexplained.
  - Log lines changed for chain_id issues: WARN instead of silently
    feeding an aggregate error, with the same slog key=value style as
    the rest of the file.

- `internal/web/server.go`
  - Added `Reason string \`json:"reason,omitempty"\`` to
    `EndpointStatus`. Populated from `ep.Reason` in `handleStatus`.

- `internal/web/assets/index.html`
  - Two small CSS classes: `.ep-reason` (muted inline line under the
    URL on the compact overview row) and `.ep-reason-banner` (red
    banner on the detail card). These are the minimum needed — reusing
    the existing `.bad` colour would have been too noisy on the compact
    list and too quiet on the detail card.
  - Overview row: extra `<span class="ep-reason">` appended inside
    each `.ep` grid; shown when `e.disabled && e.reason`, hidden
    otherwise.
  - Detail card: banner placed directly under the head so it's the
    first thing an operator sees on a broken endpoint. Hidden on
    healthy endpoints.

- `internal/rpc/multi_test.go` (rewrite)
  - Renamed the mismatch test from `ChainIDMismatch` → `ChainIDMismatchDisables`
    to reflect the new semantic.
  - Existing assertions flipped from "error mentions bad endpoint" to
    "Probe returns nil; bad endpoint Disabled=true with Reason
    containing expected + advertised chain_ids; good endpoints
    untouched".
  - New `AllMismatchFailsFast` test: three endpoints all mismatched
    must still return "no healthy endpoints" — preserves the
    fail-fast when there's literally nothing to index from.
  - New `FailureSetsReason` test: confirms probe failures (dial
    error) now populate Reason, not just Disabled.
  - `EndpointDidNotReport` and `EmptyExpectedSkips` tests updated to
    match the new "disable, don't error" semantics.
  - Added a `newFailingStubEndpoint` helper and a `findEndpoint`
    helper so tests re-look up by URL instead of assuming slice
    order.

## Files deliberately untouched

- `internal/rpc/rpc.go`, `internal/rpc/rest.go`,
  `internal/rpc/rpc_test.go`, `internal/rpc/rest_test.go` — the
  `node_info.network` parsing the previous implementer added is
  unchanged. The tests there exercise the Probe parsing, which the
  new semantics don't affect.
- `cmd/indexer/main.go` — the caller already passes
  `cfg.Network.ChainID` and handles the "no healthy endpoints" error;
  no code change needed since that's now the only error Probe returns.
- AIMD / adaptive path — untouched per the spec.

## Test coverage

Runs green under `go build ./...`, `go vet ./...`, and
`go test ./internal/rpc/... ./internal/web/...`:

- `TestMultiClient_Probe_ChainIDMatch` — happy path, no Disabled, no
  Reason.
- `TestMultiClient_Probe_ChainIDMismatchDisables` — one mismatched
  endpoint is Disabled with a Reason naming both chain_ids; good
  endpoints stay healthy; Probe returns nil.
- `TestMultiClient_Probe_AllMismatchFailsFast` — every endpoint
  mismatches → Probe returns "no healthy endpoints".
- `TestMultiClient_Probe_EndpointDidNotReport` — empty Network under
  validation: Disabled=true, Reason mentions "did not report".
- `TestMultiClient_Probe_EmptyExpectedSkips` — empty expectedChainID
  skips validation; no endpoint Disabled.
- `TestMultiClient_Probe_FailureSetsReason` — network-error probe
  disables endpoint AND populates Reason with `"probe failed: ..."`.
- Pre-existing RPC / REST Probe parsing tests still pass unchanged.

## Design notes

- **Reason format is a stable short string, not structured.** The
  dashboard consumes it verbatim. If a future caller wants to
  distinguish "mismatch" from "did not report" programmatically it
  can look at `Disabled` + substring — same ergonomics we already use
  elsewhere — but the primary consumer is an operator reading a
  banner, so a human-readable string wins over a typed enum here.
- **No re-enable / recheck.** An endpoint disabled at startup stays
  disabled for the process lifetime. Matches the pre-existing
  probe-failure semantics — operators fix config and restart, rather
  than hot-recovery.
- **Logging key names match the existing style** (`url`, `kind`,
  `expected`, `advertised`). No changes to log format.
- **`any` sentinel moved after chain_id validation.** Previously
  `any = true` ran before the mismatch check, which was harmless
  under fail-fast (the whole thing aborted anyway). In the new
  semantics, a mismatched endpoint must NOT count toward "we have a
  healthy endpoint", so the flag is set only after the endpoint
  survives all checks.
