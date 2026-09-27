-- Marts: daily reporting views over staging, one row per tenant and day (and dimension).
--
-- Like staging, every view is WITH (security_invoker = true), so row-level security is
-- evaluated as the querying role. Days are UTC calendar days. Amounts are reported in
-- the tenant's configured currency (ops.tenant.currency): the currency labels in the
-- files are not trusted (lumen's finance summary labels EUR amounts as USD), and no
-- conversion is applied. Rows whose day or amount failed to parse are left out here
-- and listed by staging.invalid_rows.

-- Refunds are counted on a cash basis: on the day of the refund, not of the order.
-- Orphan refunds (no matching order for the tenant) are excluded from refunds and net
-- and reported separately, since they cannot be tied to revenue that was recorded.
CREATE VIEW marts.daily_revenue WITH (security_invoker = true) AS
WITH orders AS (
  SELECT tenant_id, order_date AS day, count(*) AS orders, sum(gross) AS gross
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

CREATE VIEW marts.daily_ad_spend WITH (security_invoker = true) AS
SELECT a.tenant_id, a.spend_date AS day, a.platform, t.currency, sum(a.spend) AS spend
FROM staging.ad_spend a
JOIN ops.tenant t ON t.tenant_id = a.tenant_id
GROUP BY a.tenant_id, a.spend_date, a.platform, t.currency;

CREATE VIEW marts.daily_email_engagement WITH (security_invoker = true) AS
SELECT tenant_id, event_date AS day,
       count(*) FILTER (WHERE event_type = 'delivered')::integer AS delivered,
       count(*) FILTER (WHERE event_type = 'open')::integer AS opens,
       count(*) FILTER (WHERE event_type = 'click')::integer AS clicks,
       count(*) FILTER (WHERE event_type = 'unsubscribe')::integer AS unsubscribes
FROM staging.email_events
WHERE event_date IS NOT NULL
GROUP BY tenant_id, event_date;

-- Revenue (gross) per order channel against ad spend on the platform of the same
-- canonical name. Spend is assumed to be in the tenant currency, like revenue: a header
-- alias such as cost_usd renames a column, it does not convert it. ROAS is NULL when
-- there is no spend to divide by.
CREATE VIEW marts.daily_channel_performance WITH (security_invoker = true) AS
WITH revenue AS (
  SELECT tenant_id, order_date AS day, channel, sum(gross) AS revenue
  FROM staging.orders
  WHERE order_date IS NOT NULL AND channel IS NOT NULL
  GROUP BY tenant_id, order_date, channel
),
spend AS (
  SELECT tenant_id, spend_date AS day, platform AS channel, sum(spend) AS spend
  FROM staging.ad_spend
  GROUP BY tenant_id, spend_date, platform
)
SELECT t.tenant_id, d.day, d.channel, t.currency,
       coalesce(d.revenue, 0) AS revenue,
       coalesce(d.spend, 0) AS spend,
       round(d.revenue / nullif(d.spend, 0), 4) AS roas
FROM (
  SELECT tenant_id, day, channel, r.revenue, s.spend
  FROM revenue r
  FULL JOIN spend s USING (tenant_id, day, channel)
) d
JOIN ops.tenant t ON t.tenant_id = d.tenant_id;

GRANT USAGE ON SCHEMA marts TO pipeline_app;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO pipeline_app;
