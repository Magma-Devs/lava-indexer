---
name: architecture-review
description: Review Go code for architectural soundness — module boundaries, dependency direction, interface placement, testability, concurrency ownership, transactional correctness, and state-management patterns. Invoked when the architect-reviewer agent audits code, a diff, or a PR; use this whenever a request mentions "architecture review", "design review", "module boundaries", "layering", "clean architecture", "dependency direction", "testability", "coupling", or asks the architect to look at specific files. Triggered implicitly when the code-review or code-development orchestrator fans out to its architect role.
---

# Architecture review

You are performing architecture review for a Go service. Your output is
a single markdown file of findings, formatted per the finding schema.

## Scope

Review the files/diff passed to you. If no specific scope is given, default to
`git diff main...HEAD -- '*.go'`. For whole-repo reviews, focus on package
boundaries first (`go list -m -deps ./...`) before opening individual files.

## Methodology

Walk the codebase in this order:

1. **Package graph**: run `go list -deps ./...` (or read imports). Sketch
   the dependency DAG mentally. Flag any cycle, near-cycle, or unexpected
   cross-package reach.
2. **Module boundaries**: for each changed package, ask "does every file
   belong to one responsibility?" and "is the public surface small?"
3. **Concurrency ownership**: for every `go func()` or `g.Go(...)`, identify
   who closes the channels, who cancels the context, who owns the
   lifecycle. Missing answer = finding.
4. **Dependency injection**: constructors and test seams. Globals, init()
   side-effects, DefaultClient / time.Now() reaches.
5. **State / persistence**: are writes + coverage tracking in the same tx?
   Is idempotency handled? DDL discipline?
6. **Error handling shape**: exported error types used correctly, error
   wrapping preserved, panic vs error discipline.

Load `references/go-architecture-patterns.md` before starting — it contains
the detailed checklist and the project-specific context.

## Output

Write findings to `_workspace/architecture_findings.md` using the schema
in `finding-synthesis/references/finding-schema.md`.

- Severity calibrated per `finding-synthesis/references/severity-guide.md`.
- If you find nothing, still write the header with `Total findings: 0`
  — the synthesiser needs the file to exist.
- If you identify cross-cutting concerns (e.g. "this package should
  split"), note them once at high severity; don't repeat per-file.

## What architecture review is NOT

- Style (go to style-reviewer).
- Performance (go to performance-analyst) — unless the perf issue is
  structural (e.g. "this whole layer should not exist").
- Security (go to security-auditor) — unless the security issue is
  architectural (e.g. "authz is scattered across handlers").

Stay in lane. The synthesiser handles cross-domain findings.
