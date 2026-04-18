---
name: style-review
description: Review Go code for style, idioms, naming, comments, and project conventions — including error handling style, context propagation, logging conventions, package comments, YAML tag consistency, and tool-caught issues that slipped through (gofmt, go vet, staticcheck). Invoked when the style-reviewer agent reviews code, a diff, or a PR; use this for "style review", "code style", "conventions", "idioms", "naming review", "clean up the comments", "gofmt check", etc. Triggered implicitly when the code-review or code-development orchestrator fans out to its style role.
---

# Style review

You are reviewing Go code for style and idiom adherence. Output is a
single markdown file of findings. This reviewer has the LOWEST bar for
severity: findings here are almost all `low` or `info` — the only time
style rises is when it indicates a latent bug (shadowed `err`, missed
`defer Close`, mismatched YAML tag).

## Scope

Review the files/diff passed to you. If no specific scope is given,
default to `git diff main...HEAD -- '*.go' '*.yml' '*.yaml'`. Style
review of non-Go YAML/config is in scope because those are routinely
out of sync with Go struct tags.

## Methodology

1. **Tool findings first**: run (mentally, or describe what the tools
   would find) `gofmt -l ./...`, `go vet ./...`. If these find
   anything, single `medium` finding "CI does not run gofmt/go vet"
   — don't list every instance.
2. **Naming conventions**: acronym casing, package-qualified names,
   getter shape, short/long var names.
3. **Error handling shape**: wrap-with-context, don't log-and-return,
   sentinel vs typed errors.
4. **Comments**: doc comments on exported names, WHY not WHAT, TODO
   hygiene.
5. **Project conventions**: slog field style, ctx as first param,
   DDL idempotency, YAML tag snake_case. See the reference for the
   full list inferred from this codebase.
6. **Tests**: shape, subtests, table-driven, t.Helper(), t.Cleanup.

Load `references/go-style-and-project-conventions.md` for the
checklist + project-specific conventions.

## Output

Write findings to `_workspace/style_findings.md` using the finding schema.

- 90% of findings should be `low` or `info`.
- Don't report every instance of a repeated pattern. One finding
  "`Url` is used in 6 places; should be `URL` per Go convention" —
  not 6 separate findings.
- If you find nothing, write header with `Total findings: 0`. Silence
  is a legitimate outcome for style review.

## What style review is NOT

- Architecture. "This package has too many responsibilities" is
  architectural, not stylistic, even if the package name is ugly.
- Security. "This comment leaks the internal hostname format" is
  security.
- Performance. "This comment-heavy loop is slow" — the comments
  aren't the problem; the loop is.
