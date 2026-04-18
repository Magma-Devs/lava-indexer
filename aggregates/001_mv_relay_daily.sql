-- Daily per-(chain, provider) aggregates. Mirrors the prior lava-indexer MV
-- but reads from the normalised relay_payments + providers + chains tables.
--
-- Size strategy: the raw table is narrow (INT ids instead of TEXT strings),
-- the MV is O(N_days * N_chains * N_providers) which is tiny compared to the
-- raw event stream, and the MV carries pre-joined TEXT so query-time joins
-- are not needed.

-- Every column name matters — the info app's GraphQL queries reference them
-- by the camelCase form PostGraphile derives from snake_case. In particular,
-- `/provider-rewards` in the consumer needs both:
--   - WEIGHTED QoS sums (qos_*_w paired with qos_weight), for time-window
--     averages weighted by relay volume; and
--   - DELTA-COMPATIBLE per-row QoS sums (qos_*_sum, qos_count, qos_cu, and
--     the excellence variants), so it can compute UNweighted averages
--     across an arbitrary date range by re-summing the daily rows. These
--     are the "qosSyncSum", "qosCount", "qosCu" fields the query pulls.
CREATE MATERIALIZED VIEW IF NOT EXISTS app.mv_relay_daily AS
SELECT
  (r.timestamp AT TIME ZONE 'UTC')::date                                    AS date,
  c.name                                                                    AS chain_id,
  p.addr                                                                    AS provider,

  -- Totals
  SUM(r.cu)::numeric                                                        AS cu,
  SUM(r.relay_number)::numeric                                              AS relays,

  -- Weighted QoS (numerator = metric × relay_number, denominator = qos_weight)
  SUM(CASE WHEN r.qos_sync    IS NOT NULL
           THEN r.qos_sync::float8    * r.relay_number::float8 END)         AS qos_sync_w,
  SUM(CASE WHEN r.qos_avail   IS NOT NULL
           THEN r.qos_avail::float8   * r.relay_number::float8 END)         AS qos_avail_w,
  SUM(CASE WHEN r.qos_latency IS NOT NULL
           THEN r.qos_latency::float8 * r.relay_number::float8 END)         AS qos_latency_w,
  SUM(CASE WHEN r.qos_sync    IS NOT NULL
           THEN r.relay_number ELSE 0 END)::numeric                         AS qos_weight,

  -- Delta-compatible unweighted QoS — only rows where ALL three core QoS
  -- metrics exist AND are > 0 AND cu > 0 count. Two sums per metric
  -- (qos_*_sum for numerator, qos_count / qos_cu for denominators) so the
  -- API can recompute averages at arbitrary aggregation granularity.
  SUM(CASE WHEN r.qos_sync > 0 AND r.qos_avail > 0 AND r.qos_latency > 0
                AND r.cu > 0 THEN r.cu ELSE 0 END)::numeric                 AS qos_cu,
  SUM(CASE WHEN r.qos_sync > 0 AND r.qos_avail > 0 AND r.qos_latency > 0
                AND r.cu > 0 THEN r.qos_sync    END)                        AS qos_sync_sum,
  SUM(CASE WHEN r.qos_sync > 0 AND r.qos_avail > 0 AND r.qos_latency > 0
                AND r.cu > 0 THEN r.qos_avail   END)                        AS qos_avail_sum,
  SUM(CASE WHEN r.qos_sync > 0 AND r.qos_avail > 0 AND r.qos_latency > 0
                AND r.cu > 0 THEN r.qos_latency END)                        AS qos_latency_sum,
  COUNT(CASE WHEN r.qos_sync > 0 AND r.qos_avail > 0 AND r.qos_latency > 0
                  AND r.cu > 0 THEN 1 END)::numeric                         AS qos_count,

  -- Weighted excellence QoS
  SUM(CASE WHEN r.ex_qos_sync    IS NOT NULL
           THEN r.ex_qos_sync::float8    * r.relay_number::float8 END)      AS ex_qos_sync_w,
  SUM(CASE WHEN r.ex_qos_avail   IS NOT NULL
           THEN r.ex_qos_avail::float8   * r.relay_number::float8 END)      AS ex_qos_avail_w,
  SUM(CASE WHEN r.ex_qos_latency IS NOT NULL
           THEN r.ex_qos_latency::float8 * r.relay_number::float8 END)      AS ex_qos_latency_w,
  SUM(CASE WHEN r.ex_qos_sync    IS NOT NULL
           THEN r.relay_number ELSE 0 END)::numeric                         AS ex_qos_weight,

  -- Delta-compatible unweighted excellence QoS (same filter as core)
  SUM(CASE WHEN r.ex_qos_sync > 0 AND r.ex_qos_avail > 0 AND r.ex_qos_latency > 0
                AND r.cu > 0 THEN r.ex_qos_sync    END)                     AS ex_qos_sync_sum,
  SUM(CASE WHEN r.ex_qos_sync > 0 AND r.ex_qos_avail > 0 AND r.ex_qos_latency > 0
                AND r.cu > 0 THEN r.ex_qos_avail   END)                     AS ex_qos_avail_sum,
  SUM(CASE WHEN r.ex_qos_sync > 0 AND r.ex_qos_avail > 0 AND r.ex_qos_latency > 0
                AND r.cu > 0 THEN r.ex_qos_latency END)                     AS ex_qos_latency_sum,
  COUNT(CASE WHEN r.ex_qos_sync > 0 AND r.ex_qos_avail > 0 AND r.ex_qos_latency > 0
                  AND r.cu > 0 THEN 1 END)::numeric                         AS ex_qos_count
FROM app.relay_payments r
JOIN app.providers p ON p.id = r.provider_id
JOIN app.chains    c ON c.id = r.chain_id
WHERE r.timestamp IS NOT NULL
GROUP BY 1, 2, 3;

-- UNIQUE on the grouping tuple is required for REFRESH ... CONCURRENTLY.
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_relay_daily_pk
  ON app.mv_relay_daily (date, chain_id, provider);

CREATE INDEX IF NOT EXISTS idx_mv_relay_daily_date
  ON app.mv_relay_daily (date);
CREATE INDEX IF NOT EXISTS idx_mv_relay_daily_chain_date
  ON app.mv_relay_daily (chain_id, date);
CREATE INDEX IF NOT EXISTS idx_mv_relay_daily_provider_date
  ON app.mv_relay_daily (provider, date);
