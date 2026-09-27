/**
 * The canonical contract for each source: the column names the rest of the
 * pipeline relies on. Tenant configs map their own headers and values onto these.
 */
export const SOURCE_COLUMNS = {
  orders: ["order_id", "created_at", "channel", "gross", "currency", "customer_email"],
  email_events: ["event_id", "type", "email", "campaign_id", "occurred_at"],
  ad_spend: ["date", "campaign_id", "platform", "spend"],
  refunds: ["refund_id", "refunded_at", "order_id", "amount", "currency"],
} as const satisfies Record<string, readonly string[]>;

export type SourceName = keyof typeof SOURCE_COLUMNS;

export const SOURCE_NAMES = Object.keys(SOURCE_COLUMNS) as SourceName[];
