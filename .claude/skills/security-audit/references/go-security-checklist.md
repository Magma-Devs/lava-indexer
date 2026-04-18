# Go security audit — what to look for

Focus on concrete, exploitable issues a security-conscious senior reviewer
would flag in a Go service. Generic OWASP bullet-points are filtered here
to the ones that matter for this stack (Go + pgx + HTTP + Docker).

## SQL: pgx specifics

- **Parameterised queries only**: `pool.Exec(ctx, "... $1 ...", val)` is
  safe. `pool.Exec(ctx, fmt.Sprintf("... %s ...", val))` is not, full
  stop. `fmt.Sprintf` is ONLY OK for values that are compile-time
  constants (e.g. schema/table names from `var` declarations, not from
  user input, config, or another column).
- **Identifier interpolation**: schema names and table names can't be
  parameterised in SQL. If they're interpolated, confirm they come
  from a closed set (an enum, a compile-time constant, a hard-coded
  whitelist). `fmt.Sprintf("%s.indexer_ranges", s.schema)` is safe iff
  `s.schema` is from trusted config.
- **Array/IN clauses**: use `= ANY($1)` with a slice, not
  `IN (` + joined strings + `)`.
- **Advisory locks**: `pg_advisory_xact_lock(hashtext($1))` is safe
  even with string input because `hashtext` returns an int — the
  string never reaches the engine's parser.

## Secrets handling

- **No secrets in logs**: `slog.Info(..., "password", cfg.Password)`
  is an immediate finding. Check every log call that references the
  DB config, tokens, or headers.
- **No secrets in errors**: `fmt.Errorf("connect to %s failed", dsn)`
  leaks the full DSN including password into error strings that may
  be logged, returned to clients, or tracked.
- **Config precedence**: secrets should come from env or secret
  manager, never from a committed file. `config.yml` shipping with
  a real password even in `example` form is a finding.
- **Header injection**: if user input flows into an HTTP request
  header, it can break out via CR/LF. Check every place
  `req.Header.Set` is called with non-literal input.

## HTTP client / server

- **TLS verification**: `&http.Transport{TLSClientConfig:
  &tls.Config{InsecureSkipVerify: true}}` is a critical finding
  unless behind an explicit dev-mode flag.
- **Timeout everywhere**: an `http.Client` without `Timeout` or a
  `&http.Server` without `ReadHeaderTimeout` / `ReadTimeout` is a
  slow-loris invitation.
- **Unbounded body reads**: `io.ReadAll(resp.Body)` without a ceiling
  is a memory-exhaustion vector. `io.LimitReader` first.
- **Request smuggling via proxies**: if there's a reverse proxy hop,
  check `Host` headers are not trusted, and CORS is scoped.
- **Authorization checks**: every HTTP handler that modifies state
  should verify authz. "Internal only, reachable via VPC CIDR" is
  not authz, it's defence-in-depth — add both.

## Input validation at boundaries

- **Deserialisation**: `json.Unmarshal`, `yaml.Unmarshal` on untrusted
  input should be bounded and type-constrained. A struct with no
  validation after Unmarshal can accept attacker-controlled values
  for fields downstream code assumes are sane.
- **Integer overflow**: `int64` conversions from untrusted sources
  need bounds checks. Block heights as untrusted strings parsed via
  `strconv.ParseInt` should be checked.
- **Path traversal**: any `os.Open(userInput)` or
  `filepath.Join(base, userInput)` where `userInput` can contain `..`
  is a finding.

## Concurrency security

- **Race conditions with security impact**: if auth state is checked
  then used (TOCTOU), and the check/use gap spans goroutines, an
  attacker can race past authz. Mutex the check+use together.
- **Shared HTTP client state**: putting per-request headers on a
  shared `http.Client` via `DefaultTransport` leaks them across
  requests.

## Crypto

- **Algorithm choice**: flag MD5, SHA1 (except git compat), DES, RC4,
  ECB. SHA-256 for hashing, AES-GCM for auth-enc, TLS 1.2+ for
  transport.
- **Fixed nonce/IV**: a constant nonce with a counter-mode cipher is
  catastrophic.
- **Key management**: keys hard-coded or in env without rotation
  mechanism get flagged as architectural, not cryptographic.
- **Randomness**: `math/rand` for anything security-sensitive is a
  critical finding. `crypto/rand` for tokens, nonces, passwords.

## Supply chain

- **Pinned versions**: `go.mod` is pinned via `go.sum`, good. Check
  for replace directives pointing at forks without review.
- **Dependency permissions**: a new dependency that reads the
  filesystem, exec's subprocesses, or opens network sockets deserves
  a line item.
- **Docker base image**: `FROM alpine` without a pinned digest drifts
  silently. `FROM golang:1.26-alpine@sha256:...` is ideal.

## Container / deployment

- **Running as root**: `USER 0` (or no USER directive) in a
  production Dockerfile. `USER indexer` with a non-root uid is what's
  expected.
- **Capabilities**: `cap_add` / `privileged: true` in compose without
  a documented reason is a finding.
- **Secrets in env**: env vars are visible to `docker inspect` and
  often leak via logs. File-mounted secrets (read at startup) are
  better; the trade-off should be conscious, not accidental.

## Lava-indexer specific

- **Headers config**: per-endpoint custom headers (e.g. `lava-extension:
  archive`, `Authorization: Bearer`) should be treated as secrets if
  they carry auth tokens — same rules as DB credentials.
- **DB migrations**: `CREATE EXTENSION pg_cron` requires superuser on
  non-RDS installs. Confirm the deployment has that privilege or has
  a clearer failure mode.
- **Dead-letter table**: `app.indexer_failures` stores raw error
  strings (`reason` column). Check these don't include DB passwords
  from a connection error or a stack trace that reveals internals.
