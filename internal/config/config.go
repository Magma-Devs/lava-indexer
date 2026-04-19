package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Network       Network  `yaml:"network"`
	Indexer       Indexer  `yaml:"indexer"`
	Database      Database `yaml:"database"`
	Web           Web      `yaml:"web"`
	Log           Log      `yaml:"log"`
	GraphQL       GraphQL  `yaml:"graphql"`
	AggregatesDir string   `yaml:"aggregates_dir"`
}

// Web configures the built-in progress UI + status JSON API.
type Web struct {
	Enabled bool   `yaml:"enabled"`
	Addr    string `yaml:"addr"` // e.g. ":8080"
}

// Log configures the slog handler. Use "json" for CloudWatch / Loki / any
// structured log consumer; "text" for human-readable terminal output.
type Log struct {
	Format string `yaml:"format"` // "json" | "text"
	Level  string `yaml:"level"`  // "debug" | "info" | "warn" | "error"
}

// GraphQL wires a reverse-proxy to an upstream PostGraphile (or equivalent)
// so the same origin that serves the indexer UI also serves /graphql and
// /graphiql. That means the UI can embed GraphiQL in an iframe without
// CORS gymnastics and the user only has to expose one port.
type GraphQL struct {
	Enabled  bool   `yaml:"enabled"`
	Upstream string `yaml:"upstream"` // e.g. "http://graphql:5000"
}

type Network struct {
	ChainID   string     `yaml:"chain_id"`
	Endpoints []Endpoint `yaml:"endpoints"`
}

type Endpoint struct {
	Kind string `yaml:"kind"` // "rpc" | "rest"
	URL  string `yaml:"url"`
	// Headers attached to every HTTP request this endpoint makes. Useful for
	// routing hints some providers expect (e.g. Lava's `lava-extension:
	// archive` flag that asks a gateway to serve from archive nodes) or
	// auth tokens. Keys are case-insensitive (http.Header canonicalises).
	// Ignored keys: Content-Type / Accept are always overwritten by the
	// client to match the protocol, so the request can't be misformatted
	// from config.
	Headers map[string]string `yaml:"headers,omitempty"`
}

type Indexer struct {
	StartHeight int64 `yaml:"start_height"`
	EndHeight   int64 `yaml:"end_height"`
	// EarliestMode overrides StartHeight: on startup the indexer uses the
	// lowest block served by any healthy endpoint. Useful when you want to
	// cover as much history as your endpoint fleet allows without hard-coding.
	EarliestMode bool `yaml:"earliest_mode"`
	// Handlers selects which event handlers to register. Use "all" (or leave
	// empty) to enable every compiled-in handler. Otherwise list handler
	// names (e.g. ["lava_relay_payment"]). Handlers not listed are not
	// registered, so they also don't record coverage in indexer_ranges.
	Handlers         []string      `yaml:"handlers"`
	TipConfirmations int64         `yaml:"tip_confirmations"`
	// FetchWorkers: positive number = explicit worker count; 0 = adaptive.
	// In adaptive mode the process spawns `Σ endpoint.max_concurrency`
	// workers at startup, so every endpoint can hit its ceiling in
	// parallel. The AIMD controller then gates actual concurrent calls
	// per endpoint, so over-provisioning workers just backpressures
	// gracefully instead of overloading a node.
	FetchWorkers     int           `yaml:"fetch_workers"`
	FetchBatchSize   int           `yaml:"fetch_batch_size"`
	WriteBatchRows   int           `yaml:"write_batch_rows"`
	QueueDepth       int           `yaml:"queue_depth"`
	TipPollInterval  time.Duration `yaml:"tip_poll_interval"`

	// PruneOutsideWindow, if true, deletes any indexer_ranges and
	// indexer_failures entries for heights outside [StartHeight, EndHeight|tip]
	// on startup. Useful when the operator narrowed the window after a
	// previous (wider) run and wants the stale state dropped so gap math
	// reflects the new window. One-shot per startup; safe to leave on.
	PruneOutsideWindow bool `yaml:"prune_outside_window"`

	// FailureRetryInterval controls how often the pipeline sweeps the
	// dead-letter table and re-queues failed heights during an active
	// run. 0 disables in-run retry (old behaviour: dead-letters wait
	// until a restart or until tip-follow re-picks them up). Default 60s
	// catches transient saturation failures without hammering the DB.
	FailureRetryInterval time.Duration `yaml:"failure_retry_interval"`

	// FailureRetryBatch is the max number of failed heights retried per
	// sweep. Smaller = gentler on the RPC endpoints, larger = faster
	// drain. Default 200.
	FailureRetryBatch int `yaml:"failure_retry_batch"`

	// FailureMaxRetries caps how many times a transient dead-letter entry
	// is retried before the pipeline flips it to permanent and stops
	// cycling it through the retry sweep. Without a cap the sweep can
	// churn on the same heights indefinitely (e.g. 10k "no endpoint
	// covers" rows rotating forever). Default 5.
	FailureMaxRetries int `yaml:"failure_max_retries"`

	// BackfillInterleave controls how strictly tip-first the gap scheduler
	// is. Every Nth batch goes to the OLDEST-cursor gap instead of the
	// highest, so a large backfill gap can't starve behind tip-close
	// fragments. 0 or 1 disables the interleave (pure tip-first — good
	// when you only ever have one gap). Default 5 → 80/20 split in
	// favour of tip-close work.
	BackfillInterleave int `yaml:"backfill_interleave"`
}

// WantsHandler reports whether `name` should be registered given the
// handlers config. Empty list or ["all"] means "every handler".
func (i Indexer) WantsHandler(name string) bool {
	if len(i.Handlers) == 0 {
		return true
	}
	for _, h := range i.Handlers {
		if strings.EqualFold(h, "all") || strings.EqualFold(h, name) {
			return true
		}
	}
	return false
}

type Database struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	Schema   string `yaml:"schema"`
	PoolSize int    `yaml:"pool_size"`
	SSLMode  string `yaml:"ssl_mode"` // disable | require | verify-full
}

// Load reads a config file (YAML) and overlays environment variable overrides.
// The config file is OPTIONAL: if path is "" or the file doesn't exist, the
// loader runs with pure defaults + env overrides. This makes the binary
// deploy-friendly in containerised setups where config commonly comes from
// the orchestrator's env + a secret mount.
func Load(path string) (*Config, error) {
	cfg := defaults()
	if path != "" {
		if raw, err := os.ReadFile(path); err == nil {
			if err := yaml.Unmarshal(raw, cfg); err != nil {
				return nil, fmt.Errorf("parse %s: %w", path, err)
			}
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
	}
	applyEnvOverrides(cfg)
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func defaults() *Config {
	return &Config{
		Indexer: Indexer{
			StartHeight:          1,
			EndHeight:            0,
			TipConfirmations:     0, // Cosmos/CometBFT has deterministic finality
			FetchWorkers:         16,
			// Bumped from 10 to 50. With the fetch/persist split and the
			// uncapped per-endpoint routing, roundtrip overhead matters more
			// than queue depth; 50 heights per JSON-RPC batch cuts per-
			// request overhead ~5× on the wire. Tendermint batch responses at
			// this size are still <2 MB, comfortably within any upstream's
			// buffers.
			FetchBatchSize:       50,
			WriteBatchRows:       20000,
			QueueDepth:           200,
			TipPollInterval:      3 * time.Second,
			FailureRetryInterval: 60 * time.Second,
			FailureRetryBatch:    200,
			FailureMaxRetries:    5,
			BackfillInterleave:   5,
		},
		Database: Database{
			Host: "localhost",
			Port: 5432,
			User: "postgres",
			// Password intentionally has no default — operator must set
			// it via config or DB_PASSWORD env. A baked-in default would
			// silently ship as a known credential.
			Database: "postgres",
			Schema:   "app",
			PoolSize: 8,
			SSLMode:  "disable",
		},
		Web: Web{
			Enabled: true,
			Addr:    ":8080",
		},
		Log: Log{
			Format: "json",
			Level:  "info",
		},
		GraphQL: GraphQL{
			// Off by default. When enabled, the indexer reverse-proxies
			// /graphql + /graphiql to the upstream Postgraphile container,
			// which exposes the entire app schema unauthenticated. Operators
			// must opt-in (graphql.enabled: true or GRAPHQL_ENABLED=true)
			// after deciding how to gate access — typically via a reverse
			// proxy with auth, or by binding the indexer to loopback.
			Enabled:  false,
			Upstream: "http://graphql:5000",
		},
		AggregatesDir: "./aggregates",
	}
}

// applyEnvOverrides is the container-first config layer. Every important
// knob has an env-var override so orchestrators can drive behaviour without
// mutating the baked-in config.yml.
//
// Networking:
//
//	LAVA_CHAIN_ID               overrides network.chain_id
//	LAVA_RPC_ENDPOINT           overrides the first endpoint's URL
//	LAVA_RPC_ENDPOINTS          comma-separated URLs (kind=rpc each). Wins
//	                            over LAVA_RPC_ENDPOINT and the file list.
//
// Indexer:
//
//	INDEXER_START_HEIGHT        int64
//	INDEXER_END_HEIGHT          int64 (0 = follow tip)
//	INDEXER_FETCH_WORKERS       int
//	INDEXER_FETCH_BATCH_SIZE    int
//	INDEXER_TIP_CONFIRMATIONS   int64
//
// Database:
//
//	DB_HOST DB_PORT DB_USER DB_PASSWORD DB_DATABASE DB_SCHEMA
//	DB_SSLMODE (disable | require | verify-ca | verify-full)
//	DB_POOL_SIZE
//
// Web UI:
//
//	WEB_ADDR                    e.g. ":8080"  /  "0.0.0.0:8080"
//	WEB_ENABLED                 "true" | "false"
func applyEnvOverrides(cfg *Config) {
	// Network
	if v := os.Getenv("LAVA_CHAIN_ID"); v != "" {
		cfg.Network.ChainID = v
	}
	if v := os.Getenv("LAVA_RPC_ENDPOINTS"); v != "" {
		cfg.Network.Endpoints = cfg.Network.Endpoints[:0]
		for _, u := range strings.Split(v, ",") {
			u = strings.TrimSpace(u)
			if u == "" {
				continue
			}
			cfg.Network.Endpoints = append(cfg.Network.Endpoints, Endpoint{Kind: "rpc", URL: u})
		}
	} else if v := os.Getenv("LAVA_RPC_ENDPOINT"); v != "" {
		if len(cfg.Network.Endpoints) == 0 {
			cfg.Network.Endpoints = append(cfg.Network.Endpoints, Endpoint{Kind: "rpc", URL: v})
		} else {
			cfg.Network.Endpoints[0].URL = v
		}
	}

	// Indexer
	if v, ok := envInt64("INDEXER_START_HEIGHT"); ok {
		cfg.Indexer.StartHeight = v
	}
	if v, ok := envInt64("INDEXER_END_HEIGHT"); ok {
		cfg.Indexer.EndHeight = v
	}
	if v := os.Getenv("INDEXER_EARLIEST_MODE"); v != "" {
		cfg.Indexer.EarliestMode = strings.EqualFold(v, "true") || v == "1"
	}
	if v := os.Getenv("INDEXER_HANDLERS"); v != "" {
		var out []string
		for _, h := range strings.Split(v, ",") {
			h = strings.TrimSpace(h)
			if h != "" {
				out = append(out, h)
			}
		}
		cfg.Indexer.Handlers = out
	}
	if v, ok := envInt("INDEXER_FETCH_WORKERS"); ok {
		cfg.Indexer.FetchWorkers = v
	}
	if v, ok := envInt("INDEXER_FETCH_BATCH_SIZE"); ok {
		cfg.Indexer.FetchBatchSize = v
	}
	if v, ok := envInt64("INDEXER_TIP_CONFIRMATIONS"); ok {
		cfg.Indexer.TipConfirmations = v
	}

	// Database
	if v := os.Getenv("DB_HOST"); v != "" {
		cfg.Database.Host = v
	}
	if v, ok := envInt("DB_PORT"); ok {
		cfg.Database.Port = v
	}
	if v := os.Getenv("DB_USER"); v != "" {
		cfg.Database.User = v
	}
	if v := os.Getenv("DB_PASSWORD"); v != "" {
		cfg.Database.Password = v
	}
	if v := os.Getenv("DB_DATABASE"); v != "" {
		cfg.Database.Database = v
	}
	if v := os.Getenv("DB_SCHEMA"); v != "" {
		cfg.Database.Schema = v
	}
	if v := os.Getenv("DB_SSLMODE"); v != "" {
		cfg.Database.SSLMode = v
	}
	if v, ok := envInt("DB_POOL_SIZE"); ok {
		cfg.Database.PoolSize = v
	}

	// Web
	if v := os.Getenv("WEB_ADDR"); v != "" {
		cfg.Web.Addr = v
	}
	if v := os.Getenv("WEB_ENABLED"); v != "" {
		cfg.Web.Enabled = strings.EqualFold(v, "true") || v == "1"
	}

	// Log
	if v := os.Getenv("LOG_FORMAT"); v != "" {
		cfg.Log.Format = strings.ToLower(v)
	}
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		cfg.Log.Level = strings.ToLower(v)
	}

	// GraphQL
	if v := os.Getenv("GRAPHQL_ENABLED"); v != "" {
		cfg.GraphQL.Enabled = strings.EqualFold(v, "true") || v == "1"
	}
	if v := os.Getenv("GRAPHQL_UPSTREAM"); v != "" {
		cfg.GraphQL.Upstream = v
	}
}

func envInt(name string) (int, bool) {
	v := os.Getenv(name)
	if v == "" {
		return 0, false
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, false
	}
	return n, true
}

func envInt64(name string) (int64, bool) {
	v := os.Getenv(name)
	if v == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func (c *Config) validate() error {
	if len(c.Network.Endpoints) == 0 {
		return fmt.Errorf("network.endpoints must have at least one entry")
	}
	for i, e := range c.Network.Endpoints {
		if e.Kind != "rpc" && e.Kind != "rest" {
			return fmt.Errorf("endpoints[%d].kind must be 'rpc' or 'rest' (got %q)", i, e.Kind)
		}
		if e.URL == "" {
			return fmt.Errorf("endpoints[%d].url is empty", i)
		}
	}
	// FetchWorkers 0 is the "adaptive" sentinel; main.go resolves it after
	// probing endpoint max-concurrency. Any positive value is honoured.
	if c.Indexer.FetchWorkers < 0 {
		return fmt.Errorf("indexer.fetch_workers must be >= 0 (0 = adaptive)")
	}
	// 0 = adaptive; main.go wires the sizer if either fetch_workers or
	// fetch_batch_size is zero.
	if c.Indexer.FetchBatchSize < 0 {
		return fmt.Errorf("indexer.fetch_batch_size must be >= 0 (0 = adaptive)")
	}
	// start_height < 1 is valid iff earliest_mode resolves it later.
	if c.Indexer.StartHeight < 1 && !c.Indexer.EarliestMode {
		c.Indexer.StartHeight = 1
	}
	if c.Database.Schema == "" {
		c.Database.Schema = "app"
	}
	return nil
}

// ConnString returns a libpq-compatible postgres URL with user/password
// properly URL-escaped via url.UserPassword. Passwords containing `@`,
// `?`, `/`, `#`, `%` are common in cloud-managed-DB credentials (RDS,
// Cloud SQL) — naive %s interpolation corrupts the URL parse and can
// silently downgrade SSL when, e.g., a password ends with `?sslmode=`.
func (d Database) ConnString() string {
	ssl := d.SSLMode
	if ssl == "" {
		ssl = "disable"
	}
	q := url.Values{}
	q.Set("sslmode", ssl)
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(d.User, d.Password),
		Host:     fmt.Sprintf("%s:%d", d.Host, d.Port),
		Path:     "/" + d.Database,
		RawQuery: q.Encode(),
	}
	return u.String()
}
