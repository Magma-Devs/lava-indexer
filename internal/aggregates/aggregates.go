// Package aggregates applies user-defined SQL files (CREATE MATERIALIZED
// VIEW / CREATE TABLE / rollups) on startup and wires up pg_cron refresh
// schedules from aggregates.yml.
//
// Conventions
//
//   - Every *.sql file in the aggregates directory is executed in filename
//     order. Use numeric prefixes (001_, 010_, …) for ordering.
//   - Every statement must be idempotent (CREATE ... IF NOT EXISTS or
//     equivalent). The loader runs them every time the indexer starts.
//   - aggregates.yml in the same directory (optional) declares pg_cron jobs:
//
//         refresh:
//           mv_relay_daily:
//             sql: "REFRESH MATERIALIZED VIEW CONCURRENTLY app.mv_relay_daily"
//             schedule: "*/5 * * * *"
//
//     Jobs are re-scheduled on every startup so edits take effect without a
//     DB reset.
package aggregates

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Refresh map[string]RefreshSpec `yaml:"refresh"`
}

type RefreshSpec struct {
	SQL      string `yaml:"sql"`
	Schedule string `yaml:"schedule"` // pg_cron cron expression
}

// Apply runs every *.sql file in dir in filename order, then schedules any
// pg_cron jobs declared in aggregates.yml. pg_cron is optional — if the
// extension isn't installed, cron scheduling is logged and skipped.
func Apply(ctx context.Context, pool *pgxpool.Pool, dir string) error {
	if dir == "" {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Warn("aggregates dir does not exist, skipping", "dir", dir)
			return nil
		}
		return fmt.Errorf("read aggregates dir: %w", err)
	}

	// 1. Apply *.sql in filename order.
	var sqlFiles []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		sqlFiles = append(sqlFiles, filepath.Join(dir, e.Name()))
	}
	sort.Strings(sqlFiles)
	for _, f := range sqlFiles {
		body, err := os.ReadFile(f)
		if err != nil {
			return fmt.Errorf("read %s: %w", f, err)
		}
		if len(strings.TrimSpace(string(body))) == 0 {
			continue
		}
		slog.Info("applying aggregate", "file", filepath.Base(f))
		// Bound each SQL apply to 30 s. On startup, an in-progress
		// REFRESH MATERIALIZED VIEW CONCURRENTLY can hold an exclusive
		// lock that blocks our idempotent CREATE. Instead of stalling
		// the whole process (web UI + pipeline are downstream of this),
		// log and move on — the aggregate already exists.
		if err := applyWithTimeout(ctx, pool, string(body), 30*time.Second); err != nil {
			slog.Warn("aggregate apply timed out or failed, continuing",
				"file", filepath.Base(f), "err", err)
		}
	}

	// 2. Schedule pg_cron refresh jobs from aggregates.yml.
	cfgPath := filepath.Join(dir, "aggregates.yml")
	cfgBytes, err := os.ReadFile(cfgPath)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("read %s: %w", cfgPath, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(cfgBytes, &cfg); err != nil {
		return fmt.Errorf("parse aggregates.yml: %w", err)
	}
	if len(cfg.Refresh) == 0 {
		return nil
	}
	return scheduleCron(ctx, pool, cfg.Refresh)
}

// applyWithTimeout runs a SQL apply inside a scoped context so it can't
// wedge startup forever if Postgres is holding locks.
func applyWithTimeout(ctx context.Context, pool *pgxpool.Pool, sqlText string, timeout time.Duration) error {
	c, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	// lock_timeout is a belt-and-suspenders so we get a clean error from
	// Postgres if a lock is contended rather than sitting in lock wait.
	if _, err := pool.Exec(c, "SET LOCAL lock_timeout = '15s'"); err != nil {
		// SET LOCAL only works inside a transaction; fall through if the
		// server rejects it outside of one. pgxpool runs exec at autocommit
		// level, so SET LOCAL is best-effort here.
		_ = err
	}
	_, err := pool.Exec(c, sqlText)
	return err
}

func scheduleCron(ctx context.Context, pool *pgxpool.Pool, jobs map[string]RefreshSpec) error {
	// Probe pg_cron availability — install is optional.
	if _, err := pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS pg_cron`); err != nil {
		slog.Warn("pg_cron not available, skipping refresh schedules", "err", err)
		return nil
	}

	for name, spec := range jobs {
		if spec.SQL == "" || spec.Schedule == "" {
			slog.Warn("aggregate refresh job missing sql or schedule, skipping", "name", name)
			continue
		}
		// Un-schedule existing job with this name (idempotent edit-in-place).
		// cron.unschedule raises if the job doesn't exist, so we wrap it.
		unsched := fmt.Sprintf(
			`DO $$ BEGIN PERFORM cron.unschedule(%[1]s); EXCEPTION WHEN OTHERS THEN NULL; END $$`,
			pgLiteral(name))
		if _, err := pool.Exec(ctx, unsched); err != nil {
			return fmt.Errorf("unschedule %s: %w", name, err)
		}
		if _, err := pool.Exec(ctx, `SELECT cron.schedule($1, $2, $3)`, name, spec.Schedule, spec.SQL); err != nil {
			return fmt.Errorf("schedule %s: %w", name, err)
		}
		slog.Info("scheduled refresh", "name", name, "cron", spec.Schedule)
	}
	return nil
}

func pgLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
