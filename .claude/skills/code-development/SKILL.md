---
name: code-development
description: End-to-end code development workflow — implement a feature or fix, then automatically run the full review team (architecture, security, performance, style) against the diff and produce a consolidated report. Invoke for requests like "implement X", "add a feature that does Y", "build Z", "write code to handle W", "fix this bug" (when implementation is required, not just analysis), or any request that combines writing code with quality gates. Also triggers on "develop X with review", "implement and audit X". Wraps the code-review orchestrator — so the review component runs the same way.
---

# Code development orchestrator

Writes new code (or modifies existing), then runs the same parallel
review workflow as `code-review`, against the diff the implementer just
produced.

## Workflow

### Phase 0 — Spec

Confirm the requirement. State back to the user in 1-3 sentences what
you understood. If the requirement is ambiguous ("add caching"),
extract the decisions that have to be made and ask ONCE up front.
Don't start coding on a muddy spec.

### Phase 1 — Implementation

Dispatch the implementer agent:

```
Agent(
  subagent_type = "implementer",
  model = "opus",
  prompt = "Implement: {spec}. Constraints: match existing project conventions
           (see /home/bob/projects/lava-indexer/.claude/skills/style-review/references/go-style-and-project-conventions.md).
           Produce git-friendly commits — one feature per commit. When done,
           write a one-paragraph summary of what changed to _workspace/impl_summary.md."
)
```

Wait for completion. The implementer operates directly on the working
tree (not a worktree) unless the user specifies otherwise.

### Phase 2 — Review

Invoke the `code-review` skill with scope = `git diff --staged` (plus
untracked if present). This runs the four reviewers + synthesiser
identically to a standalone review.

The review report goes to `_workspace/report.md`.

### Phase 3 — Report

Present to the user, in this order:

1. The implementer's summary (`_workspace/impl_summary.md`).
2. A one-line review result: "{N} critical, {M} high, … — see below".
3. The full `_workspace/report.md` inline.
4. A decision prompt: "Ship as-is? Fix the critical/high findings and re-review? Something else?"

## Iteration

If the user says "fix the criticals and re-review":

1. Re-dispatch the implementer with the report as input.
   Instruction: "Address every critical and high finding in
   `_workspace/report.md`. Leave medium/low for the next iteration
   unless they're trivial."
2. Re-run Phase 2 (`code-review` on the new diff).
3. Re-present.

Cap at 3 iterations. After 3, return control to the user with a
summary of what was fixed vs. still open.

## Error handling

| Problem | Action |
|---|---|
| Implementer fails to produce working code (build errors) | Return the build error to the user; don't run review on non-compiling code. |
| Implementer changes nothing (user asked for a no-op fix) | Skip review; return "no changes made". |
| Review finds critical findings that implementer introduced | That's expected on first pass — surface the finding, offer to iterate. |
| User rejects the iteration and wants to stop | Stop. Leave `_workspace/` intact for inspection. |

## Test scenarios

**Normal:** User says "add a new handler for lava_delegator_reward events."
Phase 0 confirms the event shape. Phase 1 implementer writes
`internal/events/delegator_reward/handler.go`, registers in main.go,
adds DDL. Phase 2 review runs on the diff. Phase 3 presents combined
report.

**Iterative:** After initial implementation, review flags a `critical`
SQL-injection finding. User says "fix the criticals". Implementer
re-runs with the report, patches the issue. Review re-runs, confirms
the finding is gone.

**Spec rejection:** User says "add authentication". Phase 0 catches
missing decisions (token shape? auth scope? etc.) and asks before
touching any code.

## What this doesn't do

- Commit. Implementer stages; the user commits.
- Push to remote. The user pushes after approving.
- Open a PR. The user does that via `gh` or the UI after committing.
- Deploy. Deployment is handled by devops; this stays at the repo
  level.
