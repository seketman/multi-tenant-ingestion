# Adding a tenant

A new client is one JSON file, its batch files and its manifest entries. No TypeScript, SQL or model changes, and nothing anywhere branches on a tenant's name.

## Quick path

1. Write `tenants/<id>.json` (see [The config file](#the-config-file)).
2. Put the batch files under the tenant's `fixturesDir`, for example `fixtures/acme/orders/batch_01.csv`.
3. Add one entry per batch file to `fixtures/manifest.json`.
4. Run:

   ```sh
   pnpm migrate   # validates every tenants/*.json and seeds ops.tenant and ops.value_map
   pnpm load      # loads the new tenant's batches; other tenants' files are skipped
   pnpm check     # the new tenant should report "healthy"
   pnpm report    # publishes version 1 of every day for the new tenant
   ```

5. Verify with a scoped query ([Verify](#verify)).

## The config file

The file name must equal the `id`: `tenants/acme.json` declares `"id": "acme"`.

```jsonc
{
  "id": "acme",                    // tenant key everywhere: RLS, ledger, marts
  "displayName": "Acme",
  "currency": "GBP",               // what marts report amounts in; never converted
  "fixturesDir": "fixtures/acme",  // where this tenant's files must live
  "sources": {
    "orders": {
      // canonical column -> other header names that mean the same thing
      "columnAliases": { "gross": ["total_amount"] },
      // canonical column -> raw value -> canonical value
      "valueMaps": { "channel": { "FB": "facebook", "ADW": "google" } }
    },
    "email_events": {
      "columnAliases": { "type": ["event"] },
      "valueMaps": { "type": { "Opened": "open", "Clicked": "click" } }
    },
    "ad_spend": {
      "columnAliases": { "spend": ["cost"] },
      "valueMaps": { "platform": { "FB": "facebook" } }
    },
    "refunds": {}
  }
}
```

The real file is plain JSON, without the comments. This is the same tenant the test "a third tenant added by configuration only" in `test/staging.test.ts` builds.

| Field | Rule | Used for |
|-------|------|----------|
| `id` | `^[a-z][a-z0-9_]{1,62}$`, equal to the file name | the `tenant_id` of every row; the value of `app.tenant_id` |
| `displayName` | non-empty | `ops.tenant.display_name` |
| `currency` | three uppercase letters (ISO 4217 format) | the `currency` column of every mart; file labels are kept but not trusted |
| `fixturesDir` | relative, no `..` segments | every manifest path for this tenant must resolve inside it, symlinks included |
| `sources` | at least one of `orders`, `email_events`, `ad_spend`, `refunds`; no other keys | a source left out is not loaded (its files are quarantined as `source_not_configured`) |
| `sources.<s>.columnAliases` | optional, defaults to `{}` | header resolution at load time |
| `sources.<s>.valueMaps` | optional | value mapping in staging |

The schema is `tenantConfigSchema` in `src/config/tenants.ts`.

### Canonical columns

Every source has a fixed set of columns (`src/config/sources.ts`). Each file must provide all of them, under the canonical name or a declared alias.

| Source | Format | Columns |
|--------|--------|---------|
| `orders` | CSV | `order_id`, `created_at`, `channel`, `gross`, `currency`, `customer_email` |
| `email_events` | NDJSON | `event_id`, `type`, `email`, `campaign_id`, `occurred_at` |
| `ad_spend` | CSV | `date`, `campaign_id`, `platform`, `spend` |
| `refunds` | CSV | `refund_id`, `refunded_at`, `order_id`, `amount`, `currency` |

### How aliases work

- A header resolves when it is the canonical name or an alias declared for that source. Anything else quarantines the whole file.
- The canonical name is always accepted, so you only list the other names.
- An alias must map to exactly one column, and must not be another column's canonical name. The config is rejected otherwise.
- A file that has both a column and its alias (for example `spend` and `cost_usd`) is quarantined as `ambiguous_column`.
- In NDJSON, some lines may use the canonical key and others the alias.
- Aliases are read from the config file on every `pnpm load`; they do not need seeding. The header map each file was loaded with is stored in its ledger row (`ops.batch_file.detail.columns`), so changing an alias later never reinterprets a loaded file.

### How value maps work

- Seeding copies them into `ops.value_map`, which is tenant data under RLS. Staging looks values up there.
- Matching is exact: `"Meta"` does not match `"meta"`.
- A value without an entry passes through unchanged. Nothing flags it (see [TRADEOFFS.md](../TRADEOFFS.md)).
- Map onto the canonical values the marts use:

  | Column | Canonical values |
  |--------|------------------|
  | `orders.channel`, `ad_spend.platform` | the same names on both sides, so channel revenue joins platform spend (the fixtures use `facebook`, `google`, `email`, `direct`) |
  | `email_events.type` | `delivered`, `open`, `click`, `unsubscribe`; other values are not counted in `marts.daily_email_engagement` |

- Value maps are seeded, so a change needs `pnpm migrate` (or `pnpm seed`). The views follow the new map right away; nothing is reloaded.

## What validation errors look like

`pnpm migrate`, `pnpm load` and `pnpm check` all validate every tenant file first and stop on the first invalid one:

```text
Invalid tenant config acme.json:
✖ ISO 4217 code
  → at currency
✖ must not contain ".." segments
  → at fixturesDir
✖ Unrecognized key: "amount"
  → at sources.orders.columnAliases
✖ alias "platform" for "spend" is itself a column of ad_spend, so that header would be ambiguous
  → at sources.ad_spend.columnAliases.spend
```

```text
Tenant config other.json declares id "acme"; the file name must match the id
```

## Fixtures and manifest

Files can have any name; the manifest is what lists them. A layout that matches the existing tenants:

```text
fixtures/acme/
  orders/batch_01.csv
  orders/batch_02.csv
  email_events/batch_01.ndjson
  ad_spend/batch_01.csv
  refunds/batch_01.csv
```

- CSV: a header row, then one record per line, every record with as many fields as the header. UTF-8 (a byte order mark is fine).
- NDJSON: one JSON object per line, no blank lines in between. Trailing blank lines are ignored.

Add one entry per file to `fixtures/manifest.json`, under `batches`:

```json
{
  "tenant": "acme",
  "source": "orders",
  "batch": 1,
  "path": "acme/orders/batch_01.csv",
  "covers_from": "2026-03-01",
  "covers_to": "2026-03-01"
}
```

| Field | Rule |
|-------|------|
| `tenant` | a tenant `id`; entries for unknown tenants are ignored |
| `source` | one of the four sources |
| `batch` | positive integer, unique per tenant and source; batches load in this order, and a later batch wins on overlapping rows |
| `path` | relative to `fixtures/`, and inside the tenant's `fixturesDir`, or the whole load is rejected before anything is written |
| `covers_from`, `covers_to` | `YYYY-MM-DD`; `pnpm check` uses `covers_to` for freshness |

A listed file that is absent is reported as `missing` by `pnpm load` and `not_received` by `pnpm check`. That is how you declare a batch you expect but have not received.

## Verify

`pnpm check` should print `acme: healthy (N/N batches loaded, as of <latest covers_to>)` if every listed batch loaded. Exit code 2 means findings; the lines below the summary say which batch and why.

Then query the marts as the application role. Every tenant table and view returns zero rows until the transaction sets `app.tenant_id`:

```sh
docker compose exec postgres psql -U pipeline_app -d pipeline
```

```sql
BEGIN;
SELECT set_config('app.tenant_id', 'acme', true);   -- true: local to this transaction
SELECT day, currency, orders, gross, refunds, net FROM marts.daily_revenue ORDER BY day;
SELECT * FROM staging.invalid_rows;                  -- values that failed their cast, per line
SELECT source, batch_no, status, detail FROM ops.batch_file ORDER BY source, batch_no, id;
SELECT mart, day, dims, version FROM marts.reported_metric ORDER BY mart, day;  -- after pnpm report
COMMIT;
```

Application code does the same through `withTenant` (`src/db/tenant-scope.ts`):

```ts
import { withTenant } from "./src/db/index.ts";

const rows = await withTenant("acme", async (client) =>
  (await client.query("SELECT day, gross FROM marts.daily_revenue ORDER BY day")).rows,
);
```

`withTenant` opens a transaction, sets `app.tenant_id` with `is_local = true`, runs the callback and commits. The setting ends with the transaction, so it never reaches the next user of a pooled connection.

## What never needs to change

- Code in `src/`: the loader, parser, check and seeding read tenants from `tenants/*.json`.
- Migrations: RLS policies, staging and marts are generic over `tenant_id` and join `ops.value_map` for mappings.
- Tests for existing tenants: they run under their own throwaway ids.
- Other tenants' config or files: a manifest entry cannot reach outside its tenant's `fixturesDir`.

Removing a tenant file does not delete the tenant or its data from the database; seeding only inserts and updates.
