-- Current data quality, and order counts that agree with the gross they sit next to.
--
-- staging.invalid_rows lists every bad loaded line, including lines a later batch has
-- since superseded, and stays that way for audit. A health check needs the other view:
-- the bad lines that still reach the marts, so that correcting a row in a later batch
-- clears the finding instead of keeping it open forever.

-- Invalid rows that are still current: the line is the one its staging view kept for
-- its natural key, or the line has no natural key and so never took part in de-dup
-- (the staging views drop it, and nothing can supersede it). A line whose natural key
-- is present but lost to a later line is superseded and left out. The key columns
-- mirror the WHERE clause of each staging view: a key column that is missing or fails
-- its cast is exactly a row of staging.invalid_rows on that column.
CREATE VIEW staging.current_invalid_rows WITH (security_invoker = true) AS
SELECT i.tenant_id, i.source, i.batch_file_id, i.batch_no, i.line_no, i.column_name, i.raw_value, i.problem
FROM staging.invalid_rows i
WHERE EXISTS (
        SELECT 1
        FROM staging.invalid_rows k
        JOIN (VALUES
          ('orders', 'order_id'), ('email_events', 'event_id'),
          ('ad_spend', 'date'), ('ad_spend', 'campaign_id'), ('ad_spend', 'platform'),
          ('refunds', 'refund_id')
        ) AS natural_key (source, column_name)
          ON natural_key.source = k.source AND natural_key.column_name = k.column_name
        WHERE k.tenant_id = i.tenant_id AND k.batch_file_id = i.batch_file_id AND k.line_no = i.line_no
      )
   OR (i.source = 'orders' AND EXISTS (
        SELECT 1 FROM staging.orders s
        WHERE s.tenant_id = i.tenant_id AND s.batch_file_id = i.batch_file_id AND s.line_no = i.line_no
      ))
   OR (i.source = 'email_events' AND EXISTS (
        SELECT 1 FROM staging.email_events s
        WHERE s.tenant_id = i.tenant_id AND s.batch_file_id = i.batch_file_id AND s.line_no = i.line_no
      ))
   OR (i.source = 'ad_spend' AND EXISTS (
        SELECT 1 FROM staging.ad_spend s
        WHERE s.tenant_id = i.tenant_id AND s.batch_file_id = i.batch_file_id AND s.line_no = i.line_no
      ))
   OR (i.source = 'refunds' AND EXISTS (
        SELECT 1 FROM staging.refunds s
        WHERE s.tenant_id = i.tenant_id AND s.batch_file_id = i.batch_file_id AND s.line_no = i.line_no
      ));

GRANT SELECT ON staging.current_invalid_rows TO pipeline_app;

-- An order whose gross is unreadable is excluded from the day's totals: 007 counted it
-- in orders while sum(gross) skipped it, so orders and gross disagreed. It stays in
-- staging.orders, and `pnpm check` reports it as invalid_rows. Same columns, so the
-- view is replaced in place and keeps its grants and security_invoker option.
CREATE OR REPLACE VIEW marts.daily_revenue WITH (security_invoker = true) AS
WITH orders AS (
  SELECT tenant_id, order_date AS day, count(gross) AS orders, sum(gross) AS gross
  FROM staging.orders
  WHERE order_date IS NOT NULL
  GROUP BY tenant_id, order_date
),
refunds AS (
  SELECT tenant_id, refund_date AS day,
         sum(amount) FILTER (WHERE NOT is_orphan) AS refunds,
         count(*) FILTER (WHERE is_orphan) AS orphan_refund_count,
         sum(amount) FILTER (WHERE is_orphan) AS orphan_refund_amount
  FROM staging.refunds
  WHERE refund_date IS NOT NULL
  GROUP BY tenant_id, refund_date
)
SELECT t.tenant_id, d.day, t.currency,
       coalesce(d.orders, 0)::integer AS orders,
       coalesce(d.gross, 0) AS gross,
       coalesce(d.refunds, 0) AS refunds,
       coalesce(d.gross, 0) - coalesce(d.refunds, 0) AS net,
       coalesce(d.orphan_refund_count, 0)::integer AS orphan_refund_count,
       coalesce(d.orphan_refund_amount, 0) AS orphan_refund_amount
FROM (
  -- Days present in either orders or refunds.
  SELECT tenant_id, day, o.orders, o.gross, r.refunds, r.orphan_refund_count, r.orphan_refund_amount
  FROM orders o
  FULL JOIN refunds r USING (tenant_id, day)
) d
JOIN ops.tenant t ON t.tenant_id = d.tenant_id;
