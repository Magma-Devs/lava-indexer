---
name: code-review
description: Orchestrate a comprehensive parallel code review — architecture, security, performance, and style — then merge all findings into a single prioritised report. Invoke for any request phrased as "review this code", "review this PR", "review the diff", "audit this", "check my changes", "run the review team on X", "code review", "PR review"; also for iterative requests like "review again", "redo the review", "only the security part", "re-run the review after I changed X". The orchestrator handles scope resolution (files vs diff vs PR), spawns the reviewer team, writes _workspace/{category}_findings.md files, runs the synthesiser, and presents _workspace/report.md.
---

# Code review orchestrator

You coordinate a 5-agent review team (architect-reviewer, security-
auditor, performance-analyst, style-reviewer, report-synthesizer) and
produce a single prioritised report.

## Phase 0 — Context check (first thing, every run)

Before spawning anything, check what's already in `_workspace/`:

- **`_workspace/` missing** → **initial run**. Create
  `_workspace/`.
- **`_workspace/report.md` exists + user asks to re-run** → **new run**.
  Move current `_workspace/` to `_workspace_prev_{timestamp}/` so the
  previous report is preserved for comparison. Create a fresh
  `_workspace/`.
- **`_workspace/report.md` exists + user asks for a subset** (e.g. "only
  the security part", "re-run performance") → **partial re-run**.
  Delete only the specific reviewer's findings file; leave the rest.
  Run only the requested reviewer + synthesiser.

Log the decision: `initial run`, `new run (preserved prev)`, or
`partial re-run ({reviewer})`.

## Phase 1 — Scope resolution

Ask (or infer) what to review:

| User says | Scope |
|---|---|
| "review the diff" / "my changes" | `git diff main...HEAD` (and include `git status --porcelain` if dirty) |
| "review the PR" | `git diff {base}...HEAD` where `base` comes from `gh pr view --json baseRefName` |
| "review pipeline.go" (specific file(s)) | those files, whole file |
| "review this" with no context | ask; don't default to the whole repo |
| "review the whole repo" | all `*.go` files; warn that this is slow and expensive |

Write the resolved scope to `_workspace/scope.txt` so every reviewer
reads the same thing.

## Phase 2 — Team formation

Build the team:

```
TeamCreate(
  team_name = "code-review",
  members = [
    {name: "architect-reviewer",   subagent_type: "architect-reviewer",   model: "opus"},
    {name: "security-auditor",     subagent_type: "security-auditor",     model: "opus"},
    {name: "performance-analyst",  subagent_type: "performance-analyst",  model: "opus"},
    {name: "style-reviewer",       subagent_type: "style-reviewer",       model: "opus"},
    {name: "report-synthesizer",   subagent_type: "report-synthesizer",   model: "opus"},
  ]
)
```

For a partial re-run, include only the requested reviewer + the
synthesiser.

## Phase 3 — Fan-out

Dispatch the four reviewers in PARALLEL (all `TaskCreate` in a single
message so they run concurrently):

Each reviewer gets:

- **Input**: `_workspace/scope.txt`
- **Output**: `_workspace/{category}_findings.md`
- **Instruction**: "Review the scope in `_workspace/scope.txt` using
  your {category} skill. Write findings to `_workspace/{category}_findings.md`
  following the finding schema. Hit your own domain only — the
  synthesiser handles cross-cutting."

Four reviewers produce four files. They do NOT talk to each other.

## Phase 4 — Synthesis

Once all four reviewers report complete (via task status, not message
polling), dispatch the synthesiser:

- **Input**: the four `_workspace/{category}_findings.md` files
- **Output**: `_workspace/report.md`
- **Instruction**: "Read the four findings files in `_workspace/`,
  dedup, re-rank by severity, and write the consolidated report to
  `_workspace/report.md` per the finding-synthesis skill."

## Phase 5 — Presentation

Return to the user:

1. A short preamble: scope, counts by severity, top 1-2 issues by name.
2. The full content of `_workspace/report.md` inline.
3. A one-line hint: "Full reviewer outputs at `_workspace/*_findings.md`.
   Re-run a single reviewer by name (e.g. 'rerun the security review')."

## Error handling

| Problem | Action |
|---|---|
| A reviewer fails / times out | Retry once. If still failing, proceed to synthesis without that file; the synthesiser's header will flag the missing reviewer. |
| All four reviewers find nothing | Synthesiser writes a one-paragraph report confirming clean review. Return that, don't fabricate findings. |
| User provides scope that's empty (no files, no diff) | Stop and ask for clarification. Don't run reviewers on nothing. |
| Reviewer writes malformed markdown (schema break) | Synthesiser fixes obvious format issues; re-ranks. Flag as "Reviewer X output had format issues, best-effort merged." |

## Test scenarios

**Normal:** User says "review the diff from main". Scope resolves to `git
diff main...HEAD`. Four reviewers run in parallel, produce findings.
Synthesiser merges. Report returned. Expected output: `_workspace/report.md`
with severity-ordered findings.

**Re-run single reviewer:** User says "rerun the security review". Phase 0
detects partial re-run. Only security-auditor + synthesiser are
dispatched. Existing architecture/performance/style findings are
reused. New `_workspace/report.md` is written.

**Empty diff:** User says "review my changes" on a clean working tree.
Phase 1 resolves scope to empty. Orchestrator stops and asks whether to
expand scope to the last N commits or to specific files.

## What this orchestrator doesn't do

- Fix the findings. Presenting the report is the end of the workflow.
  If the user says "fix the critical findings", that's a separate
  request handled by `code-development` (which wraps this orchestrator).
- Gate commits. No pre-commit hook integration. The user decides when
  to run review and when to commit.
