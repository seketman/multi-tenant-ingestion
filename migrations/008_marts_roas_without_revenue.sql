-- ROAS of a channel with spend but no revenue that day is 0, not unknown: 007 divided
-- the missing revenue side of the full join, which left it NULL. Same columns, so the
-- view is replaced in place and keeps its grants and security_invoker option.
CREATE OR REPLACE VIEW marts.daily_channel_performance WITH (security_invoker = true) AS
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
       round(coalesce(d.revenue, 0) / nullif(d.spend, 0), 4) AS roas
FROM (
  SELECT tenant_id, day, channel, r.revenue, s.spend
  FROM revenue r
  FULL JOIN spend s USING (tenant_id, day, channel)
) d
JOIN ops.tenant t ON t.tenant_id = d.tenant_id;
