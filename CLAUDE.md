# lava-indexer Claude harness

This repo has a `.claude/` configuration that provides a parallel
code-review team and a combined development-plus-review workflow.
When a new session starts, this file is always loaded — it tells you
(Claude) how to use the harness and when to defer to it.

## Harness: code development + review

**Goal:** produce high-quality Go code for the lava-indexer (a
Tendermint/CometBFT block indexer for the Lava Network written in Go)
by combining implementation and four-domain parallel review into a
single workflow.

**Agent team:**

| Agent | Role |
|---|---|
| `architect-reviewer` | Module boundaries, dependency direction, testability, concurrency ownership, transaction discipline |
| `security-auditor` | SQL injection (pgx), secret leakage, authz, input validation, TLS/crypto, container hardening |
| `performance-analyst` | Hot-path allocations, goroutine leaks, pgx patterns (pool size, CopyFrom, N+1), retry amplification, indexer-specific concerns |
| `style-reviewer` | Go idioms, naming, comments, project conventions (matched against existing code) |
| `report-synthesizer` | Lead — merges the four reviewers' findings into one prioritised report |
| `implementer` | Writes/modifies code for the dev workflow; used by `code-development` only |

**Skills:**

| Skill | Used by | Purpose |
|---|---|---|
| `code-review` | Main entry (review existing code) | Orchestrator — scope resolution, fan-out to reviewers, synth |
| `code-development` | Main entry (write new code) | Orchestrator — implement, then run the review team on the diff |
| `architecture-review` | architect-reviewer | Methodology + Go architecture checklist |
| `security-audit` | security-auditor | Methodology + Go/pgx/Docker security checklist |
| `performance-analysis` | performance-analyst | Methodology + Go/pgx performance patterns + indexer specifics |
| `style-review` | style-reviewer | Methodology + Go style + project conventions |
| `finding-synthesis` | report-synthesizer | Finding schema + severity ladder + merge rules |

**Execution rules:**

- For any code-review request ("review this", "audit this", "review
  the PR", "check the diff", "run the review team on X") — invoke the
  `code-review` skill. It handles team formation, fan-out,
  synthesis, and presentation.
- For any development request that involves writing new code
  ("implement X", "add a handler for Y", "fix this bug and review") —
  invoke the `code-development` skill. It runs `implementer` first,
  then `code-review` on the diff.
- All agents must be dispatched with `model: "opus"`.
- Reviewers fan out **in parallel** — all four `TaskCreate` calls go
  in a single message.
- Intermediate artifacts: `_workspace/` at the repo root. Finding
  files are `_workspace/{category}_findings.md`; consolidated report
  is `_workspace/report.md`. Previous runs move to
  `_workspace_prev_{timestamp}/` when a new run starts.
- Simple questions ("what does this function do?", "is this test
  passing?") should be answered directly without invoking the
  harness. The harness is for full reviews and feature work.

**Directory structure:**

```
CLAUDE.md                                (this file)
.claude/
├── agents/
│   ├── architect-reviewer.md
│   ├── security-auditor.md
│   ├── performance-analyst.md
│   ├── style-reviewer.md
│   ├── report-synthesizer.md
│   └── implementer.md
└── skills/
    ├── code-review/
    │   ├── SKILL.md                     (orchestrator)
    │   └── references/
    │       └── scope-resolution.md
    ├── code-development/
    │   └── SKILL.md                     (orchestrator, wraps code-review)
    ├── architecture-review/
    │   ├── SKILL.md
    │   └── references/
    │       └── go-architecture-patterns.md
    ├── security-audit/
    │   ├── SKILL.md
    │   └── references/
    │       └── go-security-checklist.md
    ├── performance-analysis/
    │   ├── SKILL.md
    │   └── references/
    │       └── go-performance-patterns.md
    ├── style-review/
    │   ├── SKILL.md
    │   └── references/
    │       └── go-style-and-project-conventions.md
    └── finding-synthesis/
        ├── SKILL.md
        └── references/
            ├── finding-schema.md
            └── severity-guide.md
```

**Key decisions baked in:**

- **Fan-out / fan-in pattern.** Four reviewers are independent — they
  don't message each other. Cross-cutting concerns are detected by
  the synthesiser, not by inter-reviewer chatter.
- **Severity is strict.** `critical` for exploitable-now / outage-
  causing; `high` for real bugs with blast radius; `medium` for debt
  with consequences; `low` for nits; `info` for observations. The
  severity guide (`.claude/skills/finding-synthesis/references/severity-guide.md`)
  is the source of truth. Reviewers and synth both apply it.
- **Findings are markdown, not JSON.** Easier to write, easier to
  render, easier to merge. The schema is enforced structurally
  (headers + bullets) rather than by a JSON parser.
- **Implementer stages, never commits.** The user decides when to
  commit. Same for pushes and PRs.

**Variables this harness expects in the env:**

- None. The skills work off `git` + the repo. No AWS, no API keys.

**Variables deliberately NOT used:**

- `LLM_TEMPERATURE` — opus is used with default settings.
- External HTTP calls — reviewers are offline; they only read the
  repo and the reference docs.

## Variation: when NOT to use the harness

- Quick questions about existing code ("what does X do", "where is Y
  set", "is there a test for Z"). Use the `Explore` agent or direct
  grep/read.
- Infrastructure work. The harness reviews Go code; terraform and
  compose changes use `Explore` + direct review.
- Urgent hotfix where review is a blocker. Skip review for the fix;
  run the harness on the follow-up.

## Changelog

| Date | Change | Target | Reason |
|---|---|---|---|
| 2026-04-18 | Initial harness | entire `.claude/` | User requested parallel review team with synthesis |
