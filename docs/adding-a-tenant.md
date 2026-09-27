# Adding a tenant

A new client is one JSON file, its batch files and its manifest entries. No TypeScript, SQL or model changes, and nothing anywhere branches on a tenant's name.

The examples use a tenant called `acme`. Replace it with your client's id.

## Before you start

**Prerequisites.** You have run `pnpm install`, `pnpm db:up` and `pnpm migrate` as in [Run it from a clean checkout](../README.md#run-it-from-a-clean-checkout), and port 54329 is free (see [Requirements](../README.md#requirements)).

**What to collect from the client first.** Each item below becomes part of the config. Missing one shows up later as a quarantined file, a finding, or a silent wrong number.

| Collect | Why |
|---------|-----|
| A sample header row (CSV) or the keys (NDJSON) of each source they send | every header must be a canonical column or a declared alias, or the whole file is quarantined ([Canonical columns](#canonical-columns)) |
| The distinct values of `channel` (orders), `platform` (ad spend) and `type` (email events) | these go in `valueMaps`; an unmapped channel or platform passes through as its own channel. `pnpm tenant:validate` warns about it before loading; nothing flags it after |
| The currency | `currency` in the config; amounts are never converted |
| Which of the four sources they send | list only those in `sources`. A configured source with no manifest entries is a `no_manifest_entries` finding, so the tenant is never `healthy` |
| The date window each file covers | `covers_from` and `covers_to` in the manifest; `pnpm report` withholds those days until the file loads |

**Matching is exact.** Headers and values are compared byte for byte: no trimming, no case folding (`src/ingest/headers.ts`, `staging.map_value` in `migrations/006_staging.sql`). A header `Total Amount` needs exactly `"Total Amount"` as an alias; `total_amount` or `Total amount` will not match it. The same goes for `"FB"` and `"fb"` in a value map.

## Checklist

Do the steps in order. Each one says how you know it worked.

1. **Write `tenants/acme.json`** (see [The config file](#the-config-file)).

2. **Put the batch files under the tenant's `fixturesDir`**, for example `fixtures/acme/orders/batch_01.csv`.

3. **Add one entry per batch file to `fixtures/manifest.json`** (see [Fixtures and manifest](#fixtures-and-manifest)).

4. **Run `pnpm tenant:validate acme`.** It needs no database. It checks the config file, acme's manifest entries and every listed file: JSON syntax (with the file name), every schema problem, headers that would be quarantined, dates that do not exist, paths that are not on disk (with near-miss file names), files on disk that the manifest does not list, email event types that would not be counted, and the distinct `channel` and `platform` values after your value maps.
   - Worked: `acme: 0 errors, ...` and exit code 0. Warnings are allowed; read them. Exit code 2 means errors; fix them and run it again. It is the fast loop: edit, validate, edit, with no reset needed.
   - The `info values:` lines show the canonical `channel` and `platform` names acme will have. A raw spelling that has no map entry is a warning. So is a value with no match on the other side, in both directions: an `orders.channel` with no matching `ad_spend.platform` (revenue with no spend), and an `ad_spend.platform` with no matching `orders.channel` (spend with no revenue).

5. **Run `pnpm migrate`.** It validates every `tenants/*.json` and seeds `ops.tenant` and `ops.value_map`.
   - Worked: the last line is `tenants: upserted ...` and the list includes `acme`. It reads `tenants: up to date` if nothing changed since the last run.
   - Migrate comes before load because `ops.batch_file` references `ops.tenant`: loading a tenant that was never seeded fails on its first file.

6. **Run `pnpm load`.** It loads every manifest file that is not already loaded. On a fresh database that means every tenant's files; on a database that already has the other tenants, their files show as `skipped:`.
   - Worked: one `loaded: acme/<source>/batch <n> (<rows> rows)` line per file, and no `quarantined:` or `missing:` line for `acme`. The last line counts every tenant, so lumen's missing file is in it.

7. **Run `pnpm check`.**
   - Worked: `acme: healthy (N/N batches loaded, as of <latest covers_to>)`, possibly followed by `note:` lines for headers read through an alias.
   - The exit code is still 2 with the supplied fixtures, because lumen's `ad_spend` batch 3 never arrived. Read the lines for `acme`, not the exit code. See [Verify](#verify) for what each finding means.

8. **Confirm the distinct channel and platform values in the database** with the scoped query in [A channel or platform value is wrong](#a-channel-or-platform-value-is-wrong).
   - Worked: only the canonical names you expect (the fixtures use `facebook`, `google`, `email`, `direct`), and no raw spelling such as `FB`.

9. **Run `pnpm report`.** It publishes version 1 of every complete day for the new tenant.

> **Do not run `pnpm report` until step 7 is clean for `acme` and steps 4 and 8 show the right values.** A report publishes numbers a client is told. Fixing a mapping after that restates history, and a remapped `channel` or `platform` shows up as a tombstone for the old key plus an unlinked new key (see [TRADEOFFS.md Known gaps](../TRADEOFFS.md#known-gaps)).

If a step does not show what it should, go to [Troubleshooting and undo](#troubleshooting-and-undo).

## The config file

The file name must equal the `id`: `tenants/acme.json` declares `"id": "acme"`.

```jsonc
{
  "id": "acme",                    // tenant key: the tenant_id of every row, enforced by row-level security (RLS)
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

The real file is plain JSON, without the comments. The test "a third tenant added by configuration only" in `test/staging.test.ts` uses the same aliases and value maps, under the id `t_acme_<suffix>` and `fixturesDir` `fixtures/t_acme_<suffix>`.

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
| `email_events` | NDJSON (newline-delimited JSON: one object per line) | `event_id`, `type`, `email`, `campaign_id`, `occurred_at` |
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
- A value without an entry passes through unchanged. For `email_events.type`, `pnpm check` reports it as `uncounted_values` after loading; config validation cannot know which raw values a file will carry (see [TRADEOFFS.md](../TRADEOFFS.md#known-gaps)).
- Targets for `email_events.type` must be canonical, or the config is rejected. A typo such as `"Opened": "opened"` fails with `value map target "opened" for email_events.type is not one of delivered, open, click, unsubscribe`.
- `orders.channel` and `ad_spend.platform` are open on purpose, so a new channel is configuration only. Their targets are not validated: a typo there becomes a channel of its own, and nothing flags it.
- Map onto the canonical values the marts use:

  | Column | Canonical values |
  |--------|------------------|
  | `orders.channel`, `ad_spend.platform` | the same names on both sides, so channel revenue joins platform spend (the fixtures use `facebook`, `google`, `email`, `direct`) |
  | `email_events.type` | `delivered`, `open`, `click`, `unsubscribe`; other values are not counted in `marts.daily_email_engagement` |

- Value maps are seeded, so a change needs `pnpm migrate` (or `pnpm seed`). Seeding makes `ops.value_map` equal to the config: it adds and updates entries, and deletes the ones you removed. The views follow the new map right away; nothing is reloaded.

## What validation errors look like

`pnpm migrate`, `pnpm load` and `pnpm check` all validate every tenant file first, in file-name order, and stop on the first invalid one. Only that file's errors are printed, so fix it and run again to see the next.

A file that fails the schema lists every problem in it:

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
✖ value map target "opened" for email_events.type is not one of delivered, open, click, unsubscribe
  → at sources.email_events.valueMaps.type.Opened
```

```text
Tenant config other.json declares id "acme"; the file name must match the id
```

A file that is not valid JSON (here, a trailing comma) fails before the schema runs, with the parser's message after the file name:

```text
Invalid tenant config acme.json: Expected double-quoted property name in JSON at position 43 (line 4 column 1)
```

A JSON syntax error in `fixtures/manifest.json` reads `Invalid manifest /full/path/to/fixtures/manifest.json: ...`. See [A JSON syntax error](#a-json-syntax-error).

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
| `tenant` | a tenant `id`. `pnpm load` and `pnpm report` ignore entries for unknown tenants without a warning; `pnpm tenant:validate` reports them as `unknown_tenant`, with the closest id |
| `source` | one of the four sources |
| `batch` | positive integer, unique per tenant and source; batches load in this order, and a later batch wins on overlapping rows |
| `path` | relative to `fixtures/`, and inside the tenant's `fixturesDir`, or the whole load is rejected before anything is written |
| `covers_from`, `covers_to` | the `YYYY-MM-DD` format. Load, check and report check only the format, so an impossible date such as `2026-02-30` passes them; `pnpm tenant:validate` rejects it. `covers_from` after `covers_to` rejects the manifest. `pnpm check` uses `covers_to` for freshness, and `pnpm report` withholds days in this window while the batch has not loaded |

A listed file that is absent is reported as `missing` by `pnpm load` and `not_received` by `pnpm check`. That is how you declare a batch you expect but have not received. Until it loads, `pnpm report` withholds the days it covers in every mart built from that source. A tenant with no manifest entries gets no withholding.

## Verify

`pnpm check` should print `acme: healthy (N/N batches loaded, as of <latest covers_to>)` if every listed batch loaded and staging is clean. Exit code 2 means findings; the lines below the summary say which batch and why. The exit code covers every tenant, so with the supplied fixtures it is 2 even when `acme` is healthy: lumen's `ad_spend` batch 3 never arrived. Read the lines for your tenant.

If something is off, these are the lines you will see (a tenant with clean data shows none of the findings):

- `invalid_rows: orders.gross 1 row (first: batch 1 line 3)`: values that are missing or failed their cast and still reach the marts, from `staging.current_invalid_rows`. They are left out of the marts' sums, and an order with a bad `gross` is left out of `orders` too. A corrected row clears the finding and leaves a note instead: `note: orders.gross 1 invalid row superseded by a later batch`.
- `uncounted_values: email_events.type "BOUNCE" 2 rows, not one of delivered, open, click, unsubscribe`: an event type the marts never count, usually a missing value-map entry. Add the entry and run `pnpm migrate`; the views follow the map right away.
- `note: ad_spend batch 1 read header "cost" as spend (declared alias)`: a loaded batch read a header through one of your aliases. Notes are not findings and never change the exit code. `pnpm load` says the same on the file's line: `loaded: acme/ad_spend/batch 1 (<n> rows; header cost read as spend)`.

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

## Troubleshooting and undo

| Symptom | Cause | Fix |
|---------|-------|-----|
| `quarantined: acme/... (unknown_header: ...)` or `missing_column` from `pnpm load` | a header is not a canonical column or a declared alias | fix `columnAliases` and run `pnpm load` again. Only a `loaded` ledger row makes a file skip, so the quarantined attempt does not block the retry, and once the file loads `pnpm check` stops reporting it. Nothing needs a reset |
| `missing:` from `pnpm load`, `not_received:` from `pnpm check` | the client has not sent the file, or the manifest `path` has a typo | check that the file exists at `fixtures/<path>`, using the path printed on the line. If it exists, the `path` is wrong; if not, the file has not arrived |
| `no_manifest_entries: <source>` for `acme`, and `pnpm load` never mentions `acme` | the manifest entries have a misspelled `tenant`. Load and report ignore entries for unknown tenants without a warning | `pnpm tenant:validate` names the entry and the closest id. Fix the `tenant` field, or remove the source from `sources` if the client does not send it |
| A channel or platform you did not expect, and `pnpm check` reports no finding for it | a typo in a value-map target, or a raw spelling you did not map. `pnpm tenant:validate` would have warned about it before loading | see [A channel or platform value is wrong](#a-channel-or-platform-value-is-wrong) |
| `uncounted_values: email_events.type ...` | a raw event type has no value-map entry | add the entry, then `pnpm migrate` (or `pnpm seed`); the views follow right away |
| Wrong numbers from a file that loaded with the wrong alias | an alias change never reinterprets a loaded file; running `pnpm load` again prints `skipped:` | see [Undo a load](#undo-a-load) |
| `Invalid tenant config acme.json: Expected double-quoted property name in JSON ...` | a JSON syntax error in the named tenant file (`Invalid manifest ...` for the manifest) | fix the file at the given line and column; see [A JSON syntax error](#a-json-syntax-error) |

### A channel or platform value is wrong

`orders.channel` and `ad_spend.platform` are open, so a typo or a spelling you did not map becomes a channel of its own. Before loading, `pnpm tenant:validate` lists the values after your value maps and warns on unmapped spellings. After loading, list what staging sees, as the application role:

```sql
BEGIN;
SELECT set_config('app.tenant_id', 'acme', true);
SELECT DISTINCT channel FROM staging.orders ORDER BY 1;
SELECT DISTINCT platform FROM staging.ad_spend ORDER BY 1;
COMMIT;
```

Fix the value map in `tenants/acme.json` and run `pnpm migrate` (or `pnpm seed`). The views follow the new map right away; nothing is reloaded. If `pnpm report` already ran, this restates history (see the warning in the [Checklist](#checklist)).

### Undo a load

A loaded file keeps the header map it was loaded with, and loading it again prints `skipped:`. To load it again under a corrected alias, its rows must go. There are two ways, and both delete history. In production that is an operator decision, not a routine fix.

- **Reset everything**, only on a development database, because it resets every tenant:

  ```sh
  pnpm db:reset && pnpm migrate && pnpm load
  ```

- **Delete only this tenant's rows**, as the owner role inside the tenant's scope. RLS applies to the owner too, so without `set_config` the deletes match nothing. This mirrors the `afterAll` cleanups in `test/*.test.ts`, and keeps `ops.tenant` and `ops.value_map`:

  ```sh
  docker compose exec postgres psql -U pipeline_owner -d pipeline
  ```

  ```sql
  BEGIN;
  SELECT set_config('app.tenant_id', 'acme', true);
  -- Published history, only if pnpm report ran for this tenant:
  DELETE FROM ops.restatement WHERE tenant_id = 'acme';
  DELETE FROM ops.published_metric WHERE tenant_id = 'acme';
  DELETE FROM ops.report_run WHERE tenant_id = 'acme';
  -- Loaded data and the ledger:
  DELETE FROM raw.record WHERE tenant_id = 'acme';
  DELETE FROM ops.batch_file WHERE tenant_id = 'acme';
  COMMIT;
  ```

  The order follows the foreign keys: each table is deleted before the one it references. Then fix the alias and run `pnpm load` again.

### A JSON syntax error

`pnpm migrate`, `load`, `check` and `report` name the file and give the position in it, for example `Invalid tenant config acme.json: Expected double-quoted property name in JSON at position 43 (line 4 column 1)`, or `Invalid manifest /full/path/to/fixtures/manifest.json: ...` for the manifest. They stop at the first invalid file. `pnpm tenant:validate` lists every invalid file in one run, for example `error invalid_json: tenants/acme.json: Expected double-quoted property name ...`, so run it when more than one file may be broken.

## What never needs to change

- Code in `src/`: the loader, parser, check and seeding read tenants from `tenants/*.json`.
- Migrations: RLS policies, staging and marts are generic over `tenant_id` and join `ops.value_map` for mappings.
- Tests: they exercise the supplied tenants and synthetic ones under their own ids, so an added tenant does not change them.
- Other tenants' config or files: a manifest entry cannot reach outside its tenant's `fixturesDir`.

## Removing a tenant

Removing a tenant file does not delete the tenant or its data from the database. Seeding only touches tenants that have a file: for each one it inserts or updates the `ops.tenant` row, and makes its `ops.value_map` rows equal to the config, deleting entries that were removed from it. A tenant without a file is left as it is. To delete its data, use the scoped deletes in [Undo a load](#undo-a-load), followed by `ops.value_map` and `ops.tenant`.
