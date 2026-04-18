# Finding schema

Every reviewer writes findings in this exact markdown format to
`_workspace/{category}_findings.md`. The synthesiser merges them by
parsing the structure below, so deviations break merging.

## One finding per block

```markdown
### [SEVERITY] Title
- **Location:** `path/to/file.go:LINE` (or `path/to/file.go:LINE-LINE` for a range, or `repo-wide` if cross-cutting)
- **Category:** architecture | security | performance | style
- **Confidence:** high | medium | low
- **Tags:** comma,separated (optional — e.g. `sql-injection, pgx`)

Description: 1-4 sentences stating the concrete problem. Reference
specific code by quoting the relevant expression inline, not a
paragraph block. State the concrete failure mode (what breaks, when,
for whom) — not a lecture.

**Suggestion:** 1-3 sentences on the fix, or "(see recommendations)"
if the fix needs more than that.

---
```

## Header at the top of each file

Each reviewer's output file starts with exactly:

```markdown
# {CATEGORY} review findings

- **Scope:** {what was reviewed — paths, diff range, etc.}
- **Reviewer:** {agent name}
- **Reviewed at:** {ISO-8601 UTC}
- **Total findings:** {N}

---
```

Then the findings list follows.

## Severity ladder (strict definitions)

| Level | Threshold | Examples |
|---|---|---|
| `critical` | Exploitable now, or will cause data loss / outage on the current code path | SQL injection, plaintext secrets in logs, goroutine leak that eats memory unbounded |
| `high` | Correctness or security bug with real blast radius, reachable in production | Missing `ctx` propagation that leaks resources, race condition under load, auth bypass behind a realistic input |
| `medium` | Real technical debt with measurable consequence; a senior reviewer would block on this | Missing retry on 5xx, allocation in a hot loop that doubles latency, tight coupling that will hurt the next feature |
| `low` | Nit with a defensible fix but no real consequence | Comment style, named-return over- or under-use, import grouping |
| `info` | Observation only — no action required | "This module uses pattern X; future handlers should follow the same shape" |

**Rule:** reviewers bias UP only when the blast radius is genuinely there.
Do not label a style nit `high` because it annoys you. Do not label an
outage-inducing bug `medium` to be polite.

## Confidence rubric

| Level | Meaning |
|---|---|
| `high` | The reviewer has verified the claim by reading the code. A reader could reproduce the problem from the description alone. |
| `medium` | The pattern strongly suggests the problem; some context (tests, runtime behaviour) would confirm. |
| `low` | Gut feeling or pattern-matching from other codebases. Reader should verify before acting. |

Use `low` liberally when you're not sure — don't manufacture false confidence.

## What NOT to do

- **Don't paste code blocks.** The final report stays readable when findings
  quote expressions inline with backticks. Multi-line code blocks
  balloon the report. Refer to file:line instead.
- **Don't report the same issue under multiple categories.** Pick the
  category that fits best. If it's genuinely multi-category, note it in
  the description; the synthesiser will catch cross-cutting cases.
- **Don't report absence of a concern.** "I looked and found nothing"
  goes in the header as `Total findings: 0`, not as a finding with
  severity `info`.
- **Don't stack `critical` findings.** If you have more than 3 criticals,
  either they're actually `high` or the codebase really is on fire.
  Either way, re-evaluate before submitting.
