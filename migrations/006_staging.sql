-- Staging: typed, de-duplicated views over the raw layer, one per source.
--
-- Every view is created WITH (security_invoker = true), so the row-level security of
-- the tables underneath is checked as the role querying the view, never as the view
-- owner: a session without app.tenant_id sees no rows, and a scoped one only its own.
-- Nothing here knows a tenant by name. Header aliases come from each file's ledger row
-- (ops.batch_file.detail -> 'columns') and value maps from ops.value_map.

-- The text value of a canonical column in one raw payload. `columns` is the file's
-- raw key -> canonical column map; several raw keys may map to one column (an NDJSON
-- file can use the canonical key on some lines and an alias on others), so the first
-- non-blank value among them wins. Blank strings count as missing.
CREATE FUNCTION staging.column_value(payload jsonb, columns jsonb, canonical text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
RETURN (
  SELECT nullif(btrim(payload ->> c.raw_key), '')
  FROM jsonb_each_text(columns) AS c (raw_key, canonical_column)
  WHERE c.canonical_column = canonical AND nullif(btrim(payload ->> c.raw_key), '') IS NOT NULL
  ORDER BY c.raw_key
  LIMIT 1
);

-- A raw value mapped through the tenant's value map, or unchanged when it has no entry.
CREATE FUNCTION staging.map_value(tenant text, source text, column_name text, raw text) RETURNS text
LANGUAGE sql STABLE PARALLEL SAFE
RETURN coalesce(
  (SELECT v.canonical_value FROM ops.value_map v
   WHERE v.tenant_id = tenant AND v.source = map_value.source
     AND v.column_name = map_value.column_name AND v.raw_value = raw),
  raw
);

-- Safe casts: NULL instead of an error, so one bad value cannot fail a whole view.
-- The rows they null out are listed by staging.invalid_rows.
CREATE FUNCTION staging.try_numeric(value text) RETURNS numeric
LANGUAGE sql IMMUTABLE PARALLEL SAFE
RETURN CASE WHEN pg_input_is_valid(value, 'numeric') THEN
  -- An amount must be finite; numeric also accepts NaN and Infinity.
  CASE WHEN value::numeric NOT IN ('NaN', 'Infinity', '-Infinity') THEN value::numeric END
END;

-- Only ISO dates are accepted (no 'now', 'today' or 'infinity' keywords), and a value
-- without an offset is read as UTC whatever the session's TimeZone.
CREATE FUNCTION staging.try_timestamptz(value text) RETURNS timestamptz
LANGUAGE sql STABLE PARALLEL SAFE
SET TimeZone = 'UTC'
RETURN CASE WHEN value ~ '^\d{4}-\d{2}-\d{2}([ T]|$)' AND pg_input_is_valid(value, 'timestamptz')
            THEN value::timestamptz END;

CREATE FUNCTION staging.try_date(value text) RETURNS date
LANGUAGE sql IMMUTABLE PARALLEL SAFE
RETURN CASE WHEN value ~ '^\d{4}-\d{2}-\d{2}$' AND pg_input_is_valid(value, 'date') THEN value::date END;

-- Every line of every successfully loaded file, with the file's column map and lineage.
-- Quarantined attempts have no raw rows, but the status filter states the contract.
CREATE VIEW staging.loaded_record WITH (security_invoker = true) AS
SELECT r.tenant_id, f.source, r.batch_file_id, f.batch_no, r.line_no, r.payload,
       f.detail -> 'columns' AS columns
FROM raw.record r
JOIN ops.batch_file f ON f.tenant_id = r.tenant_id AND f.id = r.batch_file_id
WHERE f.status = 'loaded';

-- Batches overlap and restate rows, so each view keeps one row per natural key: the
-- latest batch wins, and within a batch the last line (batch_no DESC, line_no DESC).
-- Rows without a natural key cannot be de-duplicated and are left out; they show up in
-- staging.invalid_rows. Days are UTC calendar days.
CREATE VIEW staging.orders WITH (security_invoker = true) AS
SELECT DISTINCT ON (tenant_id, order_id)
       tenant_id, order_id, created_at, (created_at AT TIME ZONE 'UTC')::date AS order_date,
       channel, gross, reported_currency, customer_email, batch_file_id, batch_no, line_no
FROM (
  SELECT r.tenant_id, r.batch_file_id, r.batch_no, r.line_no,
         staging.column_value(r.payload, r.columns, 'order_id') AS order_id,
         staging.try_timestamptz(staging.column_value(r.payload, r.columns, 'created_at')) AS created_at,
         staging.map_value(r.tenant_id, 'orders', 'channel',
                           staging.column_value(r.payload, r.columns, 'channel')) AS channel,
         staging.try_numeric(staging.column_value(r.payload, r.columns, 'gross')) AS gross,
         -- As labelled in the file; marts report the tenant's configured currency instead.
         staging.column_value(r.payload, r.columns, 'currency') AS reported_currency,
         staging.column_value(r.payload, r.columns, 'customer_email') AS customer_email
  FROM staging.loaded_record r
  WHERE r.source = 'orders'
) typed
WHERE order_id IS NOT NULL
ORDER BY tenant_id, order_id, batch_no DESC, line_no DESC;

CREATE VIEW staging.email_events WITH (security_invoker = true) AS
SELECT DISTINCT ON (tenant_id, event_id)
       tenant_id, event_id, event_type, email, campaign_id, occurred_at,
       (occurred_at AT TIME ZONE 'UTC')::date AS event_date, batch_file_id, batch_no, line_no
FROM (
  SELECT r.tenant_id, r.batch_file_id, r.batch_no, r.line_no,
         staging.column_value(r.payload, r.columns, 'event_id') AS event_id,
         staging.map_value(r.tenant_id, 'email_events', 'type',
                           staging.column_value(r.payload, r.columns, 'type')) AS event_type,
         staging.column_value(r.payload, r.columns, 'email') AS email,
         staging.column_value(r.payload, r.columns, 'campaign_id') AS campaign_id,
         staging.try_timestamptz(staging.column_value(r.payload, r.columns, 'occurred_at')) AS occurred_at
  FROM staging.loaded_record r
  WHERE r.source = 'email_events'
) typed
WHERE event_id IS NOT NULL
ORDER BY tenant_id, event_id, batch_no DESC, line_no DESC;

-- Natural key: one row per day, campaign and platform. The platform is value-mapped
-- before de-duplication, so two raw spellings of one platform collapse into one row.
CREATE VIEW staging.ad_spend WITH (security_invoker = true) AS
SELECT DISTINCT ON (tenant_id, spend_date, campaign_id, platform)
       tenant_id, spend_date, campaign_id, platform, spend, batch_file_id, batch_no, line_no
FROM (
  SELECT r.tenant_id, r.batch_file_id, r.batch_no, r.line_no,
         staging.try_date(staging.column_value(r.payload, r.columns, 'date')) AS spend_date,
         staging.column_value(r.payload, r.columns, 'campaign_id') AS campaign_id,
         staging.map_value(r.tenant_id, 'ad_spend', 'platform',
                           staging.column_value(r.payload, r.columns, 'platform')) AS platform,
         staging.try_numeric(staging.column_value(r.payload, r.columns, 'spend')) AS spend
  FROM staging.loaded_record r
  WHERE r.source = 'ad_spend'
) typed
WHERE spend_date IS NOT NULL AND campaign_id IS NOT NULL AND platform IS NOT NULL
ORDER BY tenant_id, spend_date, campaign_id, platform, batch_no DESC, line_no DESC;

-- A refund is an orphan when its order is not in staging.orders for the same tenant.
CREATE VIEW staging.refunds WITH (security_invoker = true) AS
SELECT d.tenant_id, d.refund_id, d.refunded_at, (d.refunded_at AT TIME ZONE 'UTC')::date AS refund_date,
       d.order_id, d.amount, d.reported_currency,
       NOT EXISTS (
         SELECT 1 FROM staging.orders o WHERE o.tenant_id = d.tenant_id AND o.order_id = d.order_id
       ) AS is_orphan,
       d.batch_file_id, d.batch_no, d.line_no
FROM (
  SELECT DISTINCT ON (tenant_id, refund_id) *
  FROM (
    SELECT r.tenant_id, r.batch_file_id, r.batch_no, r.line_no,
           staging.column_value(r.payload, r.columns, 'refund_id') AS refund_id,
           staging.try_timestamptz(staging.column_value(r.payload, r.columns, 'refunded_at')) AS refunded_at,
           staging.column_value(r.payload, r.columns, 'order_id') AS order_id,
           staging.try_numeric(staging.column_value(r.payload, r.columns, 'amount')) AS amount,
           staging.column_value(r.payload, r.columns, 'currency') AS reported_currency
    FROM staging.loaded_record r
    WHERE r.source = 'refunds'
  ) typed
  WHERE refund_id IS NOT NULL
  ORDER BY tenant_id, refund_id, batch_no DESC, line_no DESC
) d;

-- Data quality: every loaded line whose required column is missing, blank or fails its
-- safe cast, one row per bad column. Checked on every loaded line, including lines a
-- later batch supersedes, so a bad delivery stays visible even after it is corrected.
CREATE VIEW staging.invalid_rows WITH (security_invoker = true) AS
SELECT r.tenant_id, r.source, r.batch_file_id, r.batch_no, r.line_no, req.column_name,
       v.raw_value,
       CASE WHEN v.raw_value IS NULL THEN 'missing' ELSE 'invalid ' || req.kind END AS problem
FROM staging.loaded_record r
JOIN (VALUES
  ('orders', 'order_id', 'text'), ('orders', 'created_at', 'timestamptz'),
  ('orders', 'channel', 'text'), ('orders', 'gross', 'numeric'),
  ('email_events', 'event_id', 'text'), ('email_events', 'type', 'text'),
  ('email_events', 'occurred_at', 'timestamptz'),
  ('ad_spend', 'date', 'date'), ('ad_spend', 'campaign_id', 'text'),
  ('ad_spend', 'platform', 'text'), ('ad_spend', 'spend', 'numeric'),
  ('refunds', 'refund_id', 'text'), ('refunds', 'refunded_at', 'timestamptz'),
  ('refunds', 'order_id', 'text'), ('refunds', 'amount', 'numeric')
) AS req (source, column_name, kind) ON req.source = r.source
CROSS JOIN LATERAL (SELECT staging.column_value(r.payload, r.columns, req.column_name) AS raw_value) v
WHERE v.raw_value IS NULL
   OR (req.kind = 'numeric' AND staging.try_numeric(v.raw_value) IS NULL)
   OR (req.kind = 'timestamptz' AND staging.try_timestamptz(v.raw_value) IS NULL)
   OR (req.kind = 'date' AND staging.try_date(v.raw_value) IS NULL);

-- Functions are executable by PUBLIC by default; like the schemas, only the app role gets them.
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA staging FROM PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA staging TO pipeline_app;
GRANT USAGE ON SCHEMA staging TO pipeline_app;
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO pipeline_app;
