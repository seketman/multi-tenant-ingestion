# multi-tenant-ingestion

Loads batch exports from several clients (tenants) into one Postgres database and turns them into daily reporting views.

- Every tenant's rows are invisible to every other tenant, enforced by the database, not by the application.
- Each batch file is loaded at most once, in one transaction, and recorded in a ledger with its sha256, so a crashed or repeated run is safe to rerun.
- Files that do not match the tenant's declared shape are quarantined with reasons instead of being half-loaded.
- A health check compares what the manifest says should have arrived with what the ledger says did, and reports rows the marts cannot count.
- A report run publishes the daily numbers as versions. It withholds a day whose source data never arrived. When a late file changes a number a client was already told, it records a restatement instead of changing it silently.

```text
fixtures/<tenant>/<source>/batch_NN.*        what the client sent
        |  pnpm load   (one transaction per file, sha256 ledger, quarantine)
        v
raw.record                                    every line, verbatim jsonb
        |  views
        v
staging.*   typed, value-mapped, de-duplicated (latest batch wins)
        |  views
        v
marts.*     daily revenue, ad spend, email engagement, channel ROAS
        |  pnpm report  (versions only what changed)
        v
published reports + restatements              what each client was told

ops.*  control plane: tenants, value maps, batch file ledger, report runs,
       published versions and restatements. pnpm check reads the ledger.
```

All four layers are tenant-scoped with `FORCE ROW LEVEL SECURITY` (RLS). Adding a tenant is configuration only: see [docs/adding-a-tenant.md](docs/adding-a-tenant.md). Design decisions and what is unfinished are in [TRADEOFFS.md](TRADEOFFS.md).

## Status

I went deep on two areas: loading that is safe to rerun, and tenant isolation enforced by the database. The rest is built and tested, with the gaps below. Each gap is explained in [TRADEOFFS.md](TRADEOFFS.md#known-gaps).

| Area | State | Evidence, and the known gap |
|------|-------|-----------------------------|
| Idempotent loading, crash and replay | Done in depth | `src/ingest/loader.ts`, `test/loader.test.ts` |
| Tenant isolation (forced RLS, non-superuser owner, app role owns nothing) | Done in depth | `migrations/001`-`005`, `test/isolation.test.ts`, `test/staging.test.ts`. Trust boundary: the database enforces scoping, and the pipeline decides which tenant a run acts for, from the manifest and tenant config plus path containment. Clients must never connect as the app role; see [Who uses what](TRADEOFFS.md#who-uses-what) |
| Schema drift: declared aliases, whole-file quarantine otherwise | Built, with known gaps | `src/ingest/headers.ts`, `src/ingest/parse.ts`, `test/headers.test.ts`, `test/parse.test.ts`. `pnpm load` and `pnpm check` name every header read through a declared alias. Gap: value-level drift in open columns (a new `channel` value) is not flagged, and an added column quarantines the whole file by design |
| Staging and daily marts | Built, with known gaps | `migrations/006_staging.sql`, `007_marts.sql`, `008_...`, `011_current_invalid_rows.sql`, `test/staging.test.ts`. `pnpm check` reports current invalid rows and uncounted event types, and notes rows a later batch corrected (`test/check.test.ts`); orders count only a readable `gross`. Gap: a day where every order has an unreadable `gross` is published as `orders` 0 and `gross` 0 instead of being withheld |
| Missing-source detection and the completeness gate | Built, with known gaps | `src/ingest/check.ts`, `src/ingest/coverage.ts`, `migrations/010_report_run_withheld.sql`, `test/check.test.ts`, `test/report.test.ts`. `pnpm report` withholds the days of a batch that never loaded, and the manifest rejects a window whose `covers_from` is after `covers_to`. Gap: a number published before its window became incomplete stays visible until the batch loads, which happens in normal operation when records dated outside their batch's window, such as late refunds, are published before the batch covering that day is listed; `pnpm check` alerts on it (`published_incomplete`), and whether to retract it is a product decision. `pnpm check` reports a configured source with no manifest entries as `no_manifest_entries`, but `pnpm report` publishes its marts without withholding |
| Late arrivals: published versions and restatements | Built, with known gaps | `src/report/publish.ts`, `migrations/009_published_reports.sql`, `test/report.test.ts`. Gap: a value-map edit restates history without naming it as the cause: `caused_by` is empty when no file arrived, blames unrelated newer files when some did, and a remapped `channel` or `platform` shows up as a tombstone plus an unlinked new key (see [Known gaps](TRADEOFFS.md#known-gaps)) |
| Third tenant by configuration only | Built, with known gaps | `src/config/tenants.ts`, `src/onboarding/validate.ts`, "a third tenant added by configuration only" in `test/staging.test.ts`, `test/tenants.test.ts`, `test/validate.test.ts`. The config rejects an event-type target that is not canonical, and `pnpm tenant:validate` checks a new tenant's config, manifest entries and files without a database, before anything loads (see [docs/adding-a-tenant.md](docs/adding-a-tenant.md)). Gap: an unmapped or misspelled value in an open column (`channel`, `platform`) is a warning there, not an error, and nothing flags it after loading |
| Reconciliation with `finance_summary.csv` | Test only | daily gross matches to the cent in `test/loader.test.ts` and `test/staging.test.ts`; the file is not ingested |
| FX conversion | Not built: the data cannot settle it | no rate source or conversion date in the fixtures, and lumen's labels are wrong; see [TRADEOFFS.md](TRADEOFFS.md) |
| Scheduler and alerting | Not built: environment-specific | `pnpm check` exit codes (0 healthy, 2 findings, 1 failed) are the interface a scheduler alerts on |
| Streaming large files | Deferred on purpose | files are read whole to keep per-file all-or-nothing loading simple; inserts are already chunked; see [TRADEOFFS.md](TRADEOFFS.md) |

## Where to look

| Path | What it holds |
|------|---------------|
| `tenants/*.json` | per-tenant config: currency, fixtures directory, column aliases, value maps |
| `src/config/` | the canonical columns per source (`sources.ts`) and the config schema (`tenants.ts`) |
| `src/ingest/loader.ts` | `pnpm load`: planning, sha256 ledger, replay classification, fault injection |
| `src/ingest/headers.ts`, `parse.ts`, `reasons.ts` | header resolution and quarantine reasons |
| `src/ingest/check.ts` | `pnpm check`: manifest vs ledger, data-quality findings, declared-alias notes, exit codes |
| `src/onboarding/validate.ts` | `pnpm tenant:validate`: onboarding checks of config, manifest and files, without a database |
| `src/report/publish.ts`, `src/ingest/coverage.ts` | `pnpm report`: versioned publication, restatements, `caused_by`, withholding incomplete days |
| `src/db/` | pools for the two roles, `withTenant`, migration runner, tenant seeding |
| `migrations/` | roles, schemas, RLS policies, ledger, staging and marts views, published reports |
| `docker/initdb/001_owner.sql` | creates the non-superuser owner role; the only thing the superuser does |
| `fixtures/` | the supplied data and `manifest.json` |
| `test/` | vitest suites against the real database |

## Requirements

| Tool | Version | Source |
|------|---------|--------|
| Docker with Compose | any recent | runs `postgres:17` from `docker-compose.yml` on `localhost:54329`, bound to 127.0.0.1 only; that port must be free |
| Node.js | `^22.18.0` or `>=24.2.0` | `package.json` `engines`; the CLIs use `import.meta.main`, added in Node 22.18.0 and 24.2.0 (not in 23) |
| pnpm | `11.10.0` | `package.json` `packageManager`; `corepack enable` installs it, or use an installed pnpm |

No `.env` is needed: `src/db/env.ts` defaults to the local compose credentials. `.env.example` documents the two variables (`OWNER_DATABASE_URL`, `APP_DATABASE_URL`) for any other environment.

## Run it from a clean checkout

```sh
pnpm install
pnpm db:up        # Postgres 17 on localhost:54329, waits until healthy
pnpm tenant:validate  # checks tenants/*.json, the manifest and the batch files; no database needed
pnpm migrate      # applies migrations/ and seeds ops.tenant + ops.value_map from tenants/*.json
pnpm load         # loads every manifest batch for every tenant
pnpm check        # manifest vs ledger; exits 2 on the supplied fixtures (see below)
pnpm report       # publishes version 1 of every complete mart day; a rerun without new data publishes nothing
pnpm test         # needs the database up and migrated; uses its own throwaway tenants
pnpm typecheck
```

To start again from an empty database (drops the volume, then recreates the schema):

```sh
pnpm db:reset && pnpm migrate
```

Expected output of the first `pnpm load` on the supplied fixtures ends with:

```text
load: 39 loaded, 0 skipped, 0 quarantined, 1 missing, 0 failed, 0 blocked
```

The lines for `ad_spend` batches 4 and 5 of both tenants name the declared alias, for example `loaded: lumen/ad_spend/batch 4 (18 rows; header cost_usd read as spend)`. A skipped file's line does not.

`pnpm report` prints one line per tenant: `published: <tenant> (run <id>; <n> versions, <n> restated, <n> unchanged, <n> withheld)`, or `nothing to publish` in place of the run when nothing changed. For example, after batch 5 arrives late (see [Late arrivals](#late-arrivals-publish-load-late-data-publish-again)): `published: northwind (run 4; 67 versions, 18 restated, 207 unchanged, 0 withheld)`. Restatements and withheld keys are information, not errors: it exits 0 unless a tenant failed. Each tenant publishes in its own transaction.

The one `missing` file is `lumen/ad_spend/batch_03.csv`: the manifest lists it, the fixtures do not contain it. That is a finding about the data, not a failure of the run, so `pnpm load` still exits 0. The same holds for quarantined files: `pnpm load` exits 0 when it quarantines, and 1 only when a file failed to load.

Because of that missing batch, `pnpm check` exits 2 overall on the supplied fixtures even though northwind is healthy: the exit code covers every tenant.

`pnpm report` withholds the days that batch covers instead of publishing them as if no money was spent. A key is withheld when its day falls inside the `covers_from`..`covers_to` window of a manifest batch with no loaded ledger row (not received, or only quarantined), for a source the mart is built from. On the supplied fixtures the first run prints northwind with `0 withheld` and lumen with `24 withheld`: `daily_channel_performance` for 2026-01-18 to 2026-01-23, 6 days times 4 channels. `daily_ad_spend` has no rows for those days, so it withholds no key, but the run still records the window. Revenue and email engagement do not depend on `ad_spend` and are published for those days. A withheld key gets no version, restatement or tombstone: whatever was published before stays the latest, and once the batch loads the key is published as usual. Each run records the windows and keys it held back in `ops.report_run.withheld`; a run that only withholds writes no run row. `pnpm report` reads `fixtures/manifest.json`, or `MANIFEST=<path>`; a tenant absent from the manifest gets no withholding, although `pnpm check` reports each of its configured sources as `no_manifest_entries`. What the gate does not cover is in [Known gaps](TRADEOFFS.md#known-gaps).

`pnpm seed` re-runs only the tenant seeding; `pnpm migrate` already does it. `pnpm db:down` stops the container and keeps the data.

## Try the failure cases

### Crash mid-run, then replay

Start from an empty database (`pnpm db:reset && pnpm migrate`).

```sh
LOADER_FAIL_AFTER_FILES=7 pnpm load   # loads 7 files, then fails inside the 8th file's transaction; exit 1
pnpm load                             # 7 skipped, 32 loaded, 1 missing
pnpm load                             # 39 skipped, 1 missing: nothing is written twice
```

`LOADER_FAIL_AFTER_FILES` is fault injection (`src/ingest/loader.ts`): the 8th file's ledger row and raw rows are inserted, then the transaction throws before `COMMIT`, so nothing of that file survives. The second run skips the 7 committed files by sha256 and loads the rest. The same sequence, with row counts checked against the files on disk, is the first test in `test/loader.test.ts`.

### Missing sources: `pnpm check` exit codes

| Exit | Meaning | How to see it |
|------|---------|---------------|
| `0` | every manifest batch loaded, every configured source current | not reachable with the supplied fixtures, because lumen's `ad_spend` batch 3 never arrived; covered in `test/check.test.ts` |
| `2` | findings | `pnpm check` after `pnpm load` |
| `1` | the check itself failed; nothing is known about the data | `CHECK_AS_OF=03/01/2026 pnpm check` (not `YYYY-MM-DD`) |

```sh
pnpm check; echo $?
# lumen: 1 finding (19/20 batches loaded, as of 2026-02-04)
#   not_received: ad_spend/batch 3 (lumen/ad_spend/batch_03.csv, covers through 2026-01-23)
#   note: ad_spend batches 4-5 read header "cost_usd" as spend (declared alias)
# northwind: healthy (20/20 batches loaded, as of 2026-02-04)
#   note: ad_spend batches 4-5 read header "cost_usd" as spend (declared alias)
# [ELIFECYCLE] Command failed with exit code 2.
# 2

CHECK_AS_OF=2026-03-01 pnpm check; echo $?   # every configured source is also reported stale; 2
```

Freshness alone would miss lumen's gap: `ad_spend` batches 4 and 5 loaded, so the source looks current through 2026-02-04. The check compares each listed batch with the ledger instead. `CHECK_AS_OF` is the date a scheduler expects data through; without it, the check uses the latest `covers_to` in the tenant's manifest. `MANIFEST=<path>` points the check at another manifest.

The check also reads `marts.reported_metric` and exits 2 on a key the client was told whose day lies inside the window of a batch that has not loaded, for a source the mart is built from, one line per window and mart: `published_incomplete: daily_revenue has 2 published keys from 2026-03-03 to 2026-03-04 inside orders/batch 2 (covers 2026-03-03 to 2026-03-04), which has not loaded (not_received)`. `pnpm report` withholds such keys, so they were published before the window became incomplete; the finding clears once the batch loads. On the supplied fixtures the first report already withholds lumen's window, so the output above does not change.

The check also reads staging, and two data-quality findings exit 2 like the others:

- `invalid_rows`: current lines whose value is missing or fails its cast (`staging.current_invalid_rows`), per source and column, for example `invalid_rows: orders.gross 1 row (first: batch 1 line 3)`. A line is current when it is the one de-duplication kept for its natural key, or when its natural key itself is missing or invalid. A bad line a later batch superseded becomes a note instead, so a corrected row clears the finding; `staging.invalid_rows` keeps the full history for audit.
- `uncounted_values`: values of a column the marts count only in canonical form, today `email_events.type`, for example `uncounted_values: email_events.type "BOUNCE" 2 rows, not one of delivered, open, click, unsubscribe`. This is also how a missing value-map entry shows up.

A `note:` line is not a finding and never changes the exit code. It names headers that loaded batches read through a declared alias, from the ledger (a quarantined batch gets no note), and invalid lines a later batch superseded, for example `note: orders.gross 1 invalid row superseded by a later batch`. The supplied fixtures have neither data-quality finding.

### Quarantine: schema drift the config does not declare

Both tenants renamed `spend` to `cost_usd` from `ad_spend` batch 4. The tenant files declare that alias. Remove it to see what an undeclared change does:

```sh
pnpm db:reset && pnpm migrate
# edit tenants/northwind.json: under sources.ad_spend, change "columnAliases" to {}
pnpm load
# quarantined: northwind/ad_spend/batch 4 (unknown_header: header "cost_usd" is neither a column of ad_spend nor a declared alias; missing_column: column "spend" is missing)
# quarantined: northwind/ad_spend/batch 5 (...same reasons...)
pnpm check        # quarantined: ad_spend/batch 4 (unknown_header, missing_column), ..., stale: ad_spend ...; no alias note for northwind
git checkout -- tenants/northwind.json
pnpm load         # batches 4 and 5 load now; everything else is skipped
# loaded: northwind/ad_spend/batch 4 (18 rows; header cost_usd read as spend)
# loaded: northwind/ad_spend/batch 5 (18 rows; header cost_usd read as spend)
pnpm check        # northwind: healthy again, with its "batches 4-5" alias note
```

The whole file is quarantined, never part of it, and the reasons are stored in `ops.batch_file.detail`. Quarantined attempts do not block a retry: the replay guard covers only loaded bytes. While batches 4 and 5 are quarantined, `pnpm report` withholds northwind's channel performance for the days they cover, 2026-01-24 to 2026-02-04 (48 keys); `daily_ad_spend` has no rows for those days, so it withholds no key but records the windows.

### Quarantine: new bytes for a batch that is already loaded

```sh
printf 'NW-99999999,2026-02-04T12:00:00Z,direct,1.00,USD,x@example.invalid\n' >> fixtures/northwind/orders/batch_05.csv
pnpm load         # quarantined: northwind/orders/batch 5 (batch_conflict: batch 5 was already loaded with different content (sha256 ...))
pnpm check        # conflicting_redelivery: orders/batch 5 is loaded, but a later delivery with different content was quarantined (batch_conflict)
git checkout -- fixtures/northwind/orders/batch_05.csv
```

The batch stays `loaded`, but the check no longer lets it look healthy: the numbers already reported may be superseded by data nobody has loaded.

`conflicting_redelivery` stays after the fixture is restored, until `pnpm db:reset`. That is by design: the quarantined attempt is a row in the append-only ledger, which is an audit trail of what was delivered, not a mirror of the files on disk today.

The same bytes under a different batch number are quarantined as `duplicate_content`, since loading them would count every row twice.

### Late arrivals: publish, load late data, publish again

Hold every batch 5 back, publish, then let it arrive. The first report is given a manifest without batch 5, meaning batch 5 was not yet due. With the full manifest it would withhold the batch 5 days instead, and the second report would publish them as version 1 rather than restate them.

```sh
pnpm db:reset && pnpm migrate
for f in fixtures/*/*/batch_05.*; do mv "$f" "$f.late"; done
node -e 'const m = require("./fixtures/manifest.json"); m.batches = m.batches.filter((b) => b.batch <= 4); require("fs").writeFileSync("/tmp/manifest-1-4.json", JSON.stringify(m))'
pnpm load                                   # batches 1-4; every batch 5 is reported missing and leaves no ledger row
MANIFEST=/tmp/manifest-1-4.json pnpm report  # version 1 of every complete (mart, day, dimension) key; lumen: 24 withheld
for f in fixtures/*/*/batch_05.*.late; do mv "$f" "${f%.late}"; done
pnpm load                                   # batch 5 of every source; batches 1-4 are skipped
pnpm report    # northwind: 18 restated, 0 withheld; lumen: 7 restated, 24 withheld
pnpm report    # nothing to publish for either tenant; lumen still reports 24 withheld
```

Lumen's `ad_spend` batch 3 is missing in both runs, so its channel performance for 2026-01-18 to 2026-01-23 is withheld both times and never enters the version or unchanged counts. The first report prints `published: lumen (run 1; 178 versions, 0 restated, 0 unchanged, 24 withheld)`, the second `published: lumen (run 3; 59 versions, 7 restated, 171 unchanged, 24 withheld)` and `published: northwind (run 4; 67 versions, 18 restated, 207 unchanged, 0 withheld)`.

Northwind's email engagement is restated for exactly 2026-01-12 to 2026-01-17, caused by `northwind/email_events/batch_05.ndjson`. Revenue is restated too, and not only at the edge of the window: lumen's refunds batch 5 carries refunds dated January 9 and 28, weeks before its window, so those days' refunds and net go up after the client was told them. A day batch 5 did not touch keeps its single version. The day-by-day breakdown for both tenants is in [TRADEOFFS.md](TRADEOFFS.md#late-arrivals).

`test/report.test.ts` runs the same sequence on clones of the fixtures, with each report given the manifest of what had been delivered by then. It asserts that exactly the keys batch 5 changed are restated, with `before` and `after` equal to the live numbers, that northwind's email restatements are exactly those six days, caused by email batch 5, and that lumen's withheld keys are never published. The counts 18 and 7 come from that run.

Each restatement says what the client was told (`before`), what the number is now (`after`) and which batch files caused it (`caused_by`, `ops.batch_file` ids). Run this in `psql` as the application role (`docker compose exec postgres psql -U pipeline_app -d pipeline`, see [Query the data](#query-the-data)):

```sql
BEGIN;
SELECT set_config('app.tenant_id', 'northwind', true);
SELECT r.mart, r.day, r.from_version, r.to_version, r.before, r.after, f.path
FROM ops.restatement r
JOIN ops.batch_file f ON f.id = ANY (r.caused_by)
WHERE r.mart = 'daily_email_engagement'
ORDER BY r.day;
COMMIT;
```

## Query the data

Every tenant table and view returns zero rows unless the transaction sets `app.tenant_id`. The application code does this through `withTenant` (`src/db/tenant-scope.ts`); by hand it is:

```sh
docker compose exec postgres psql -U pipeline_app -d pipeline
```

```sql
BEGIN;
SELECT set_config('app.tenant_id', 'northwind', true);  -- true: local to this transaction
SELECT day, orders, gross, refunds, net, orphan_refund_count FROM marts.daily_revenue ORDER BY day;
SELECT status, count(*) FROM ops.batch_file GROUP BY status;
COMMIT;
```

Useful views: `marts.daily_revenue`, `marts.daily_ad_spend`, `marts.daily_email_engagement`, `marts.daily_channel_performance`, `staging.invalid_rows`. Published reports: `marts.reported_metric` (the latest published version of each key, meaning what the client was told), `ops.published_metric` (every version), `ops.restatement`, and `ops.report_run.withheld` (what each run held back).

