# multi-tenant-ingestion

Loads batch exports from several clients (tenants) into one Postgres database and turns them into daily reporting views.

- Every tenant's rows are invisible to every other tenant, enforced by the database, not by the application.
- Each batch file is loaded at most once, in one transaction, and recorded in a ledger with its sha256, so a crashed or repeated run is safe to rerun.
- Files that do not match the tenant's declared shape are quarantined with reasons instead of being half-loaded.
- A health check compares what the manifest says should have arrived with what the ledger says did.
- A report run publishes the daily numbers as versions. When a late file changes a number a client was already told, it records a restatement instead of changing it silently.

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

| Area | State | Evidence |
|------|-------|----------|
| Idempotent loading, crash and replay | Finished | `src/ingest/loader.ts`, `test/loader.test.ts` |
| Schema drift: declared aliases, whole-file quarantine otherwise | Finished | `src/ingest/headers.ts`, `src/ingest/parse.ts`, `test/headers.test.ts`, `test/parse.test.ts` |
| Tenant isolation (forced RLS, non-superuser owner, app role owns nothing) | Finished | `migrations/001`-`005`, `test/isolation.test.ts`, `test/staging.test.ts` |
| Third tenant by configuration only | Finished | `src/config/tenants.ts`, "a third tenant added by configuration only" in `test/staging.test.ts` |
| Staging and daily marts | Finished | `migrations/006_staging.sql`, `007_marts.sql`, `008_...`, `test/staging.test.ts` |
| Missing-source detection | Finished | `src/ingest/check.ts`, `test/check.test.ts` |
| Late arrivals: published versions and restatements | Finished | `src/report/publish.ts`, `migrations/009_published_reports.sql`, `test/report.test.ts` |
| Reconciliation with `finance_summary.csv` | Test only | daily gross matches to the cent in `test/loader.test.ts` and `test/staging.test.ts`; the file is not ingested |
| FX conversion, scheduler, alerting, streaming large files | Not built | see [TRADEOFFS.md](TRADEOFFS.md) |

## Where to look

| Path | What it holds |
|------|---------------|
| `tenants/*.json` | per-tenant config: currency, fixtures directory, column aliases, value maps |
| `src/config/` | the canonical columns per source (`sources.ts`) and the config schema (`tenants.ts`) |
| `src/ingest/loader.ts` | `pnpm load`: planning, sha256 ledger, replay classification, fault injection |
| `src/ingest/headers.ts`, `parse.ts`, `reasons.ts` | header resolution and quarantine reasons |
| `src/ingest/check.ts` | `pnpm check`: manifest vs ledger, exit codes |
| `src/report/publish.ts` | `pnpm report`: versioned publication, restatements, `caused_by` |
| `src/db/` | pools for the two roles, `withTenant`, migration runner, tenant seeding |
| `migrations/` | roles, schemas, RLS policies, ledger, staging and marts views, published reports |
| `docker/initdb/001_owner.sql` | creates the non-superuser owner role; the only thing the superuser does |
| `fixtures/` | the supplied data and `manifest.json` |
| `test/` | vitest suites against the real database |

## Requirements

| Tool | Version | Source |
|------|---------|--------|
| Docker with Compose | any recent | runs `postgres:17` from `docker-compose.yml` on `localhost:54329` |
| Node.js | `^22.18.0` or `>=24.2.0` | `package.json` `engines`; the CLIs use `import.meta.main`, added in Node 22.18.0 and 24.2.0 (not in 23) |
| pnpm | `11.10.0` | `package.json` `packageManager`; `corepack enable` installs it, or use an installed pnpm |

No `.env` is needed: `src/db/env.ts` defaults to the local compose credentials. `.env.example` documents the two variables (`OWNER_DATABASE_URL`, `APP_DATABASE_URL`) for any other environment.

## Run it from a clean checkout

```sh
pnpm install
pnpm db:up        # Postgres 17 on localhost:54329, waits until healthy
pnpm migrate      # applies migrations/ and seeds ops.tenant + ops.value_map from tenants/*.json
pnpm load         # loads every manifest batch for every tenant
pnpm check        # manifest vs ledger; exits 2 on the supplied fixtures (see below)
pnpm report       # publishes version 1 of every mart day; a rerun without new data publishes nothing
pnpm test         # needs the database up and migrated; uses its own throwaway tenants
pnpm typecheck
pnpm db:reset     # drops the volume and starts an empty database (then pnpm migrate again)
```

Expected output of the first `pnpm load` on the supplied fixtures ends with:

```text
load: 39 loaded, 0 skipped, 0 quarantined, 1 missing, 0 failed, 0 blocked
```

`pnpm report` prints one line per tenant: `published: <tenant> (run <id>; <n> versions, <n> restated, <n> unchanged)`, or `nothing to publish` in place of the run when nothing changed. For example, after batch 5 arrives late (see [Late arrivals](#late-arrivals-publish-load-late-data-publish-again)): `published: northwind (run 4; 67 versions, 18 restated, 207 unchanged)`. Restatements are information, not errors: it exits 0 unless a tenant failed. Each tenant publishes in its own transaction.

The one `missing` file is `lumen/ad_spend/batch_03.csv`: the manifest lists it, the fixtures do not contain it. That is a finding about the data, not a failure of the run, so `pnpm load` still exits 0.

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
# northwind: healthy (20/20 batches loaded, as of 2026-02-04)
# [ELIFECYCLE] Command failed with exit code 2.
# 2

CHECK_AS_OF=2026-03-01 pnpm check; echo $?   # every configured source is also reported stale; 2
```

Freshness alone would miss lumen's gap: `ad_spend` batches 4 and 5 loaded, so the source looks current through 2026-02-04. The check compares each listed batch with the ledger instead. `CHECK_AS_OF` is the date a scheduler expects data through; without it, the check uses the latest `covers_to` in the tenant's manifest. `MANIFEST=<path>` points the check at another manifest.

### Quarantine: schema drift the config does not declare

Both tenants renamed `spend` to `cost_usd` from `ad_spend` batch 4. The tenant files declare that alias. Remove it to see what an undeclared change does:

```sh
pnpm db:reset && pnpm migrate
# edit tenants/northwind.json: under sources.ad_spend set "columnAliases": {}
pnpm load
# quarantined: northwind/ad_spend/batch 4 (unknown_header: header "cost_usd" is neither a column of ad_spend nor a declared alias; missing_column: column "spend" is missing)
# quarantined: northwind/ad_spend/batch 5 (...same reasons...)
pnpm check        # quarantined: ad_spend/batch 4 (unknown_header, missing_column), ..., stale: ad_spend ...
git checkout -- tenants/northwind.json
pnpm load         # batches 4 and 5 load now; everything else is skipped
```

The whole file is quarantined, never part of it, and the reasons are stored in `ops.batch_file.detail`. Quarantined attempts do not block a retry: the replay guard covers only loaded bytes.

### Quarantine: new bytes for a batch that is already loaded

```sh
printf 'NW-99999999,2026-02-04T12:00:00Z,direct,1.00,USD,x@example.invalid\n' >> fixtures/northwind/orders/batch_05.csv
pnpm load         # quarantined: northwind/orders/batch 5 (batch_conflict: batch 5 was already loaded with different content (sha256 ...))
pnpm check        # conflicting_redelivery: orders/batch 5 is loaded, but a later delivery with different content was quarantined (batch_conflict)
git checkout -- fixtures/northwind/orders/batch_05.csv
```

The batch stays `loaded`, but the check no longer lets it look healthy: the numbers already reported may be superseded by data nobody has loaded.

The same bytes under a different batch number are quarantined as `duplicate_content`, since loading them would count every row twice.

### Late arrivals: publish, load late data, publish again

Hold every batch 5 back, publish, then let it arrive:

```sh
pnpm db:reset && pnpm migrate
for f in fixtures/*/*/batch_05.*; do mv "$f" "$f.late"; done
pnpm load      # batches 1-4; every batch 5 is reported missing and leaves no ledger row
pnpm report    # version 1 of every (mart, day, dimension) key
for f in fixtures/*/*/batch_05.*.late; do mv "$f" "${f%.late}"; done
pnpm load      # batch 5 of every source; batches 1-4 are skipped
pnpm report    # northwind: 18 restated; lumen: 7 restated
pnpm report    # nothing to publish for either tenant
```

Northwind's email engagement is restated for exactly 2026-01-12 to 2026-01-17, caused by `northwind/email_events/batch_05.ndjson`. A day batch 5 did not touch keeps its single version. The full breakdown, and why lumen's revenue days move, is in [TRADEOFFS.md](TRADEOFFS.md#late-arrivals).

`test/report.test.ts` runs the same sequence on clones of the fixtures. It asserts that exactly the keys batch 5 changed are restated, with `before` and `after` equal to the live numbers, and that northwind's email restatements are exactly those six days, caused by email batch 5. The counts 18 and 7 come from that run.

Each restatement says what the client was told (`before`), what the number is now (`after`) and which batch files caused it (`caused_by`, `ops.batch_file` ids):

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

Useful views: `marts.daily_revenue`, `marts.daily_ad_spend`, `marts.daily_email_engagement`, `marts.daily_channel_performance`, `staging.invalid_rows`. Published reports: `marts.reported_metric` (the latest published version of each key, meaning what the client was told), `ops.published_metric` (every version) and `ops.restatement`.

