# Go style + project conventions

Style findings should stay in the `low` / `info` band unless they point
at a real bug. The goal is consistency across the codebase, not
adherence to any one author's preference.

## Pre-checked by tools — don't flag manually

These are caught by `gofmt`, `go vet`, `staticcheck`. If they slip
through (missing tool in CI), that itself is a finding; individual
instances are not.

- Whitespace, import grouping, indent (`gofmt`).
- Shadowed variables, unreachable code, struct tag typos (`go vet`).
- Unused results, deprecated API usage, error-shadowing patterns
  (`staticcheck`).
- Package comment style (`golint`, deprecated but still useful).

## Error handling idioms

- **`if err != nil { return err }` chains**: idiomatic, don't "improve"
  them.
- **Wrap with context**: `fmt.Errorf("operation X: %w", err)` — the
  message is what the caller was trying to do, NOT what the error was
  (it'll have that).
- **Don't log AND return**: pick one. The outer caller logs; the inner
  function returns. Double-logging the same error is noise.
- **Sentinel errors**: exported `var ErrNotFound = errors.New(...)` is
  fine. Unexported sentinels for internal control flow are too.
- **Error types**: a struct type implementing `Error()` is overkill
  unless callers need structured fields (e.g. `RetryAfter time.Duration`).
- **`errors.Is` / `errors.As`**: use these for type assertions on
  errors. Direct `==` only on untyped sentinels.

## Naming

- **Acronyms are uppercase**: `URL`, `HTTP`, `ID`, `JSON`. `Url`, `Http`,
  `Id`, `Json` are flagged.
- **Package-qualified names**: `rpc.Client`, not `rpc.RPCClient`. The
  package name is already context.
- **Getters**: `Name()`, not `GetName()`.
- **Short var names for short scopes**: `for i, r := range ranges`, not
  `for index, rng := range ranges`.
- **Long var names for package-level or long scopes**: `totalBlocks`,
  not `tb`.
- **Receivers**: one or two letters, consistent across the type. Don't
  mix `func (c *Client)` and `func (cl *Client)`.

## Comments

- **Doc comments**: exported identifiers (any `PascalCase` name) must
  have a doc comment that starts with the identifier name. `golint`
  catches this. Unexported names don't need them unless the behaviour
  is subtle.
- **Comments explain WHY, not WHAT**: `// i++ // increment i` is noise.
  `// i++ // skip the start-of-block header` is useful.
- **Multi-line comments**: use `//` for every line, not `/* */` (the
  latter is reserved for build tags and generated code).
- **TODO/FIXME**: include an owner and ideally an issue link. Drive-by
  TODOs that nobody will ever address are technical debt by a prettier
  name.

## Project-specific conventions (inferred from lava-indexer)

These come from reading `cmd/indexer/main.go`, `internal/pipeline/*`,
`internal/state/state.go`. New code should match.

- **Package comments**: each `internal/` package has a package-level
  `// Package X ...` comment that explains responsibility. New
  packages should do the same.
- **Logging**: uses `log/slog` exclusively. No `fmt.Println` in
  production code. Field-style: `slog.Info("msg", "key", value)`,
  not `slog.Info("msg: " + value)`.
- **Context**: every blocking function takes `ctx context.Context` as
  the first parameter. No exceptions.
- **Pool passed through**: `*pgxpool.Pool` is passed as a constructor
  arg; no package-level globals.
- **Defer for cleanup**: `defer rows.Close()` directly after the
  `rows, err := …`; `defer cancel()` after `WithTimeout`.
- **Config validation in the loader**: `config.Load()` returns an
  error for invalid configs; callers trust the returned `*Config`.
- **SQL style**: multi-line `pgx` queries are raw strings with leading
  indentation matching the Go source; placeholders are `$1, $2` in
  numeric order.
- **Handler DDL**: each handler provides its own `DDL() []string`
  which is run once at startup. DDL is idempotent (`IF NOT EXISTS`).

## YAML / config

- YAML tags on exported fields match snake_case of the field name:
  `FetchWorkers int \`yaml:"fetch_workers"\``.
- Defaults in a dedicated `defaults()` function; overrides in
  `applyEnvOverrides()`. Don't mix defaults into `validate()`.

## Tests

- `_test.go` files live next to the file they test.
- Table-driven tests: `tests := []struct{ name string; ...  }{...}` —
  matches the Go community idiom.
- Subtests: `t.Run(tt.name, func(t *testing.T) { ... })`.
- Helpers call `t.Helper()` so test failures report the caller's line.
- Setup/teardown via `t.Cleanup(func() { ... })`, not `defer`.

## Anti-patterns that deserve a finding

- `interface{}` / `any` in function signatures when a concrete type
  would do.
- `panic()` in library code (outside of `init()` assertions for
  invariants).
- `_ = err` swallowing errors without a comment explaining why.
- Global mutable state (`var Cache = map[string]...`). Each request
  should get what it needs through its call chain.
- Large switches on error types — a sign the code wants proper error
  hierarchy or subtyping.
