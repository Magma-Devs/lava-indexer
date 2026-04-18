---
name: security-audit
description: Audit Go code for security vulnerabilities — SQL injection (pgx), secret leakage (logs/errors), missing input validation, TLS and crypto misuse, authorization gaps, unbounded reads, container/deployment hardening, and supply-chain risk. Invoked when the security-auditor agent reviews code, a diff, or a PR; use this for any request about "security review", "vulnerability scan", "OWASP", "audit for secrets", "check for SQL injection", "authentication", "authorization", "crypto review", or similar. Triggered implicitly when the code-review or code-development orchestrator fans out to its security role.
---

# Security audit

You are performing a security audit of Go code. Output is a single
markdown file of findings, formatted per the finding schema.

## Scope

Review the files/diff passed to you. If no specific scope is given, default
to `git diff main...HEAD -- '*.go' '*.yml' '*.yaml' 'Dockerfile*'`. Security
findings in YAML/Dockerfile are in scope because that's where secrets and
auth leaks tend to hide.

## Methodology

Walk the code in this order:

1. **Grep for known-bad patterns first** (cheap, high-signal):
   - `fmt.Sprintf` near `pool.Exec` / `pool.Query` / `tx.Exec` → potential SQL injection
   - `InsecureSkipVerify` → TLS bypass
   - `slog.` or `log.` lines that reference `Password`, `Token`, `Secret`,
     `Authorization`, `Key` → secret leakage
   - `math/rand` used for tokens/IDs/nonces → weak randomness
   - `io.ReadAll(resp.Body)` without `LimitReader` → DoS vector
   - Missing `http.Client{Timeout: ...}` / `http.Server{ReadHeaderTimeout: ...}`
2. **Read the changed files for authz + validation**. Every HTTP handler
   that mutates state needs authz. Every deserialisation path from
   external input needs bounds.
3. **Dockerfile pass**: USER directive, pinned digests, exposed ports,
   secret env vars.
4. **Config files pass**: plaintext credentials, example configs that
   ship with real values.

Load `references/go-security-checklist.md` for the full list and for
examples calibrated to this codebase (pgx, embedded web UI, Docker
deployment, lava-indexer specifics).

## Output

Write findings to `_workspace/security_findings.md` using the schema
in `finding-synthesis/references/finding-schema.md`.

- Severity: `critical` is reserved for exploitable-now issues. Apply
  the severity guide strictly.
- Include the exact trigger (`an attacker submits X ...`) in the
  description. A "theoretical" security finding without a trigger
  downgrades one level.
- If you find nothing, write the header with `Total findings: 0`.

## What security audit is NOT

- Style (go to style-reviewer) — even if the style is "ugly security
  code".
- Performance (go to performance-analyst) — DoS via slowloris or bomb
  is security; general slowness is performance.
- Architecture (go to architect-reviewer) — unless the architecture
  IS the vuln (authz scattered everywhere, secret config not
  centralised).
