-- Runs on first-time Postgres init via docker-entrypoint-initdb.d.
-- Creates extensions the indexer and aggregates rely on.

CREATE EXTENSION IF NOT EXISTS pg_cron;
CREATE EXTENSION IF NOT EXISTS btree_gist;
