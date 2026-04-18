# Scope resolution reference

How the code-review orchestrator turns a user request into a concrete
list of files + lines for the reviewers to examine.

## Supported scope types

```
1. Whole repo:          every *.go file under the module root
2. Diff vs branch:      `git diff {ref}...HEAD` — committed changes since ref
3. Diff vs working:     `git diff` + `git status --porcelain` — including dirty
4. PR diff:             `gh pr view {n} --json baseRefName`, diff against that
5. Specific files:      user names files explicitly
6. Specific functions:  user names symbols; resolve to file:line via grep
```

## Commands to build the scope

Write these to `_workspace/scope.txt` in this layout — one scope per
reviewer run, so all four reviewers see the same thing:

```
MODE: {whole-repo | diff-branch | diff-working | pr | files | symbols}
BASE: {ref or 'working'}
TIMESTAMP: {ISO-8601 UTC}

FILES:
{path1}
{path2}
...

DIFF:
{raw git diff output, if applicable — up to 50k lines; bigger scopes fall back to file list only}
```

## Heuristics

- **Default when ambiguous**: if the user says "review this" without a
  subject and there's a dirty working tree, use `diff-working`. If
  the working tree is clean, use `diff-branch` vs `main`. If the repo
  has no `main`, ask.
- **Large scope warning**: if the scope exceeds 10k LOC of Go code,
  warn the user before dispatching — the review will be slow and may
  miss details by saturating context windows. Suggest a smaller scope.
- **Out-of-scope filtering**: exclude `vendor/`, `.git/`, generated
  files (look for `// Code generated ... DO NOT EDIT.` headers),
  test fixtures.

## Examples

**User:** "review the security-audit changes"
→ `gh pr view --json files -q '.files[].path'` finds the file list for
   the current branch's PR; scope = those files in HEAD.

**User:** "review pipeline.go and state.go"
→ Scope = `internal/pipeline/pipeline.go`, `internal/state/state.go`,
   whole files.

**User:** "review my changes"
→ On a dirty tree: `git diff` + `git diff --cached` + the list of
   untracked .go files (`git ls-files --others --exclude-standard`).
   On a clean tree: `git diff main...HEAD`.

**User:** "review `processWithSplit`"
→ `grep -n "func .*processWithSplit" internal/pipeline/*.go` → file:line.
   Scope = that function, ±20 lines of context.
