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

export type SourceColumn<S extends SourceName> = (typeof SOURCE_COLUMNS)[S][number];

/**
 * The canonical values of each column with closed semantics: the marts count these
 * values and nothing else, so a tenant's value map may only target one of them. A
 * column is listed here only when a new value would need new SQL to be counted.
 * Open columns (orders.channel, ad_spend.platform) are left out on purpose: a new
 * client's new channel is configuration, not code.
 */
export const CANONICAL_VALUES = {
  email_events: { type: ["delivered", "open", "click", "unsubscribe"] },
} as const satisfies { [S in SourceName]?: { [C in SourceColumn<S>]?: readonly string[] } };

/** A closed column as `source.column`, e.g. "email_events.type". */
export type ClosedColumn = {
  [S in keyof typeof CANONICAL_VALUES]: `${S}.${keyof (typeof CANONICAL_VALUES)[S] & string}`;
}[keyof typeof CANONICAL_VALUES];

/** The closed columns of one source with their canonical values; empty when it has none. */
export const canonicalValues = (source: SourceName): Partial<Record<string, readonly string[]>> =>
  (CANONICAL_VALUES as Partial<Record<SourceName, Partial<Record<string, readonly string[]>>>>)[source] ?? {};
