# Walkthrough

A reproducible tour of the pipeline, in place of a recorded video. Each section runs one failure on purpose and shows what the pipeline does about it: the commands, the expected output with the numbers from the supplied fixtures, what it demonstrates, and where to look in the code.

The numbers below were verified end to end on a fresh database. Sections 1 to 4 build on each other; section 5 resets the database, so run it last.

| # | Section | What it shows |
|---|---------|---------------|
| 1 | [Crash and replay](#1-crash-and-replay) | loading is safe to rerun, and what "already seen" means |
| 2 | [Missing sources and data quality](#2-missing-sources-and-data-quality) | `pnpm check` exit codes and the completeness gate in `pnpm report` |
| 3 | [Schema drift](#3-schema-drift) | declared aliases load; undeclared changes quarantine the whole file |
| 4 | [Tenant isolation](#4-tenant-isolation) | forced row-level security, and where that guarantee ends |
| 5 | [Late arrivals](#5-late-arrivals) | published versions and restatements |
| 6 | [Adding a third client](#6-adding-a-third-client) | configuration only, checked before anything loads |
| 7 | [What the data could not settle, and what is unfinished](#7-what-the-data-could-not-settle-and-what-is-unfinished) | decisions and known gaps |

## Setup

Requirements are in the [README](../README.md#requirements). From the repository root:

```sh
pnpm install
pnpm db:reset && pnpm migrate   # empty, migrated database with both tenants seeded
```

For the SQL in sections 4 and 5, open `psql` as the application role in a second terminal:

```sh
docker compose exec postgres psql -U pipeline_app -d pipeline
```

## 1. Crash and replay

```sh
LOADER_FAIL_AFTER_FILES=7 pnpm load; echo $?
```

Seven files load, then the eighth throws inside its own transaction, after its ledger row and raw rows are inserted and before `COMMIT`. Exit code `1`.

```sh
pnpm load
```

Expected: 7 skipped by sha256, 32 loaded, 1 missing. The summary line is:

```text
load: 32 loaded, 7 skipped, 0 quarantined, 1 missing, 0 failed, 0 blocked
```

Nothing from the crashed file survived, because its ledger row and its raw rows commit together. The lines for `ad_spend` batches 4 and 5 of both tenants end with `header cost_usd read as spend`: declared drift, covered in [section 3](#3-schema-drift).

```sh
pnpm load
```

Expected: 39 skipped, 1 missing. Rerunning is always safe.

**What it demonstrates.** A sha256 per file makes replay look solved, but it is not: the hash and the batch number can agree or disagree independently, and each combination means something different.

| Same bytes? | Same batch? | Outcome |
|-------------|-------------|---------|
| yes | yes | `skipped` (the caller skips it before classification) |
| no | yes | quarantined as `batch_conflict`: the client changed a batch it already delivered |
| yes | no | quarantined as `duplicate_content`: loading it would count every row twice |
| no | no | parsed with the tenant's column aliases |

The first version got the `duplicate_content` case wrong: it matched on the hash alone, so a re-sent batch was reported as `skipped` and left no trace in the ledger. A review caught it before it was committed.

The unique index covers only loaded bytes, so a quarantined file can be retried after a config fix. A transaction-scoped advisory lock per tenant and source keeps two runs from racing past the check.

One case is not a replay at all: northwind's orders batch 3 repeats 14 orders from batch 2 inside different bytes. No file-level check can catch it. Staging resolves it, where the latest batch wins per `order_id`, and only then does daily gross match the client's `finance_summary.csv` to the cent. The tests assert both directions.

**Where to look.**

- `migrations/003_ops_batch_file.sql`: the ledger and the partial unique index `batch_file_loaded_once` (`WHERE status = 'loaded'`).
- `classifyAndParse` in `src/ingest/loader.ts`: the classification above. The advisory lock and the fault injection (`LOADER_FAIL_AFTER_FILES`) are in the same file.
- The first test in `test/loader.test.ts` runs the same crash-and-replay sequence.

## 2. Missing sources and data quality

```sh
pnpm check; echo $?
```

Expected:

```text
lumen: 1 finding (19/20 batches loaded, as of 2026-02-04)
  not_received: ad_spend/batch 3 (lumen/ad_spend/batch_03.csv, covers through 2026-01-23)
  note: ad_spend batches 4-5 read header "cost_usd" as spend (declared alias)
northwind: healthy (20/20 batches loaded, as of 2026-02-04)
  note: ad_spend batches 4-5 read header "cost_usd" as spend (declared alias)
```

Exit code `2`.

**What it demonstrates.**

- The check compares the manifest with the ledger. Lumen's `ad_spend` batch 3 is listed but never arrived. A freshness check would miss it, because batches 4 and 5 loaded.
- Exit codes: `0` healthy, `2` findings, `1` the check itself failed. Northwind is healthy, but the exit code covers every tenant.
- The `note:` lines are the declared drift: reported, never a finding, and never a change to the exit code. Adapted, but not silently.
- The check also reads staging. Values that fail their cast (`invalid_rows`) and event types the marts cannot count (`uncounted_values`) are findings too. Only rows that still reach the marts count: if a later batch corrects a bad row, the finding clears and a note records it. An order with an unreadable `gross` is left out of the day's totals entirely, so the order count and the gross always agree. The supplied fixtures have none of these; the tests cover every source.

```sh
pnpm report
```

Expected: northwind `0 withheld`, lumen `24 withheld`.

The 24 keys are `daily_channel_performance` for 2026-01-18 to 2026-01-23, 6 days times 4 channels: the days covered by the batch that never arrived. Without this gate, the report would tell the client they spent nothing on ads those days. Revenue and email engagement for the same days are published, because they do not depend on `ad_spend`. Each run records what it held back in `ops.report_run.withheld`.

One case the gate cannot undo: a number the client was already told before its batch went missing. `pnpm check` alerts on it as `published_incomplete` until the batch arrives. Whether to retract a number a client already reconciled is a product decision; [TRADEOFFS.md](../TRADEOFFS.md#known-gaps) says so. The manifest itself is validated too: a window whose `covers_from` is after its `covers_to` is rejected, because it would silently disable the gate.

**Where to look.** `src/ingest/check.ts` (findings and exit codes), `src/ingest/coverage.ts` (the completeness gate), `migrations/010_report_run_withheld.sql`, `migrations/011_current_invalid_rows.sql`, `test/check.test.ts`, `test/report.test.ts`.

## 3. Schema drift

Both tenants renamed `spend` to `cost_usd` from `ad_spend` batch 4. `tenants/lumen.json` and `tenants/northwind.json` declare it under `sources.ad_spend.columnAliases`:

```json
"columnAliases": { "spend": ["cost_usd"] }
```

**What it demonstrates.**

- Declared drift is adapted, and both `pnpm load` and `pnpm check` name it, as seen in sections 1 and 2.
- Anything undeclared quarantines the whole file, with reasons in `ops.batch_file.detail`: an unknown header (`unknown_header`), a missing column (`missing_column`), a column and its alias both present (`ambiguous_column`), or a repeated header (`duplicate_header`).
- Noticing is structural: every header must resolve to exactly one canonical column.
- The header map each file used is stored in its ledger row, so a later config change never reinterprets data that is already loaded.

What it does not catch: a new value in an open column, such as a new channel, and a column that keeps its name but changes its unit. Both are listed as known gaps.

To see the quarantine, follow [Quarantine: schema drift the config does not declare](../README.md#quarantine-schema-drift-the-config-does-not-declare) in the README. It starts from a fresh database, so run it after this walkthrough or reload afterwards.

**Where to look.** `src/ingest/headers.ts` (header resolution), `src/ingest/parse.ts`, `src/ingest/reasons.ts` (quarantine reason codes), `test/headers.test.ts`, `test/parse.test.ts`.

## 4. Tenant isolation

In the `psql` session as `pipeline_app`:

```sql
SELECT count(*) FROM marts.daily_revenue;            -- 0: no tenant set
BEGIN;
SELECT set_config('app.tenant_id', 'northwind', true);
SELECT day, orders, gross, refunds, net, orphan_refund_count
FROM marts.daily_revenue ORDER BY day LIMIT 5;
SELECT DISTINCT tenant_id FROM marts.daily_revenue;  -- only northwind
COMMIT;
```

**What it demonstrates.**

- Every tenant table has `FORCE ROW LEVEL SECURITY`, keyed on the transaction-local setting `app.tenant_id`. Unscoped, a query returns zero rows, not an error that might be caught and ignored.
- The role that runs migrations is neither superuser nor `BYPASSRLS`, so the policies apply to it too; even seeding runs inside a tenant scope.
- The app role owns nothing and only has `SELECT` or `INSERT` where it needs it.
- Every staging and marts view is `security_invoker`, so the policy is checked as the reader, not the view owner.

Where the guarantee ends, from [Who uses what](../TRADEOFFS.md#who-uses-what): RLS makes every query scoped and stops a scoped session from crossing tenants. It does not decide who picks the scope. The app role is used by the pipeline, which serves every tenant by design and takes each file's tenant from the manifest and tenant config, with paths checked against the tenant's `fixturesDir`. So a client must never connect as this role. Client access would need a role per client bound to its login, or an access layer that derives the tenant from the authenticated identity. `SET ROLE` from one shared login would not close it.

**Where to look.** `docker/initdb/001_owner.sql`, `migrations/001_roles_and_schemas.sql` to `migrations/005_ops_value_map.sql`, `withTenant` in `src/db/tenant-scope.ts`, `test/isolation.test.ts`, `test/staging.test.ts`.

## 5. Late arrivals

This section resets the database. It holds every batch 5 back, loads, publishes with a manifest of batches 1 to 4 (meaning batch 5 was not yet due), then restores the files. The `mv` commands rename fixture files in the working tree; the last one puts them back.

```sh
pnpm db:reset && pnpm migrate
for f in fixtures/*/*/batch_05.*; do mv "$f" "$f.late"; done
node -e 'const m = require("./fixtures/manifest.json"); m.batches = m.batches.filter((b) => b.batch <= 4); require("fs").writeFileSync("/tmp/manifest-1-4.json", JSON.stringify(m))'
pnpm load && MANIFEST=/tmp/manifest-1-4.json pnpm report
for f in fixtures/*/*/batch_05.*.late; do mv "$f" "${f%.late}"; done
```

The first report prints `published: lumen (run 1; 178 versions, 0 restated, 0 unchanged, 24 withheld)`. Then let batch 5 arrive:

```sh
pnpm load      # batch 5 of every source; batches 1-4 are skipped
pnpm report    # northwind: 18 restated, 0 withheld; lumen: 7 restated, 24 withheld
pnpm report    # nothing to publish for either tenant; lumen still reports 24 withheld
```

The second report prints:

```text
published: lumen (run 3; 59 versions, 7 restated, 171 unchanged, 24 withheld)
published: northwind (run 4; 67 versions, 18 restated, 207 unchanged, 0 withheld)
```

Then, in `psql`:

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

Expected: six rows, 2026-01-12 to 2026-01-17, each caused by `northwind/email_events/batch_05.ndjson`. On 2026-01-13, opens went from 18 to 20.

**What it demonstrates.**

- Marts are live views, so a late file changes them silently. What a client was told is a published, versioned snapshot.
- Northwind's email batch 5 carries 24 events for 2026-01-12 to 2026-01-17, days already reported. The second run published new versions only for the keys that changed: 18 for northwind, six of them exactly those email days, all caused by that one file. Unchanged keys were not republished, and a third run publishes nothing.
- Lumen's 7 restatements are all `daily_revenue`: refunds that arrive weeks late for days already reported, such as a January 9 refund inside the refunds batch covering 2026-01-30 to 2026-02-04. The first written explanation of these restatements was wrong; a blind review against the fixtures caught it, and [TRADEOFFS.md](../TRADEOFFS.md#late-arrivals) now has the day-by-day breakdown.
- Withholding and restating answer different questions. A restatement corrects a number that was complete when published. A withheld key is known to be incomplete right now, so it is not published until its data arrives. Lumen's 24 channel-performance keys are withheld in both runs, because `ad_spend` batch 3 never arrives.

**Where to look.** `src/report/publish.ts` (versions, restatements, `caused_by`), `migrations/009_published_reports.sql` (`ops.published_metric`, `ops.restatement`, `marts.reported_metric`), `test/report.test.ts` (the same sequence on clones of the fixtures; the restated count must equal the number of keys whose values changed).

## 6. Adding a third client

A new client is one JSON file, its batch files and its manifest entries. The step-by-step checklist is [docs/adding-a-tenant.md](adding-a-tenant.md).

Before anything loads, validate the config, the manifest entries and every listed file. It needs no database:

```sh
pnpm tenant:validate; echo $?          # every tenants/*.json
pnpm tenant:validate <id>              # one tenant
```

Exit codes: `0` no errors (warnings are allowed), `2` at least one error, `1` the validator itself failed. On the supplied fixtures it exits `0` and ends with `tenant:validate: 0 errors, 3 warnings`: lumen's missing `ad_spend` batch 3 (`not_on_disk`), and a `channel_mismatch` warning per tenant because `orders.channel` `direct` has no matching `ad_spend.platform`.

**What it demonstrates.**

- The test "a third tenant added by configuration only" in `test/staging.test.ts` adds a GBP tenant with its own column aliases and value maps, loads it through the same loader and reads it through the same views. No code change.
- Nothing branches on a tenant name: value maps are rows in the tenant-scoped table `ops.value_map`.
- A non-canonical event-type target in a value map is rejected when the config loads; `pnpm tenant:validate` flags an event type the marts would not count before loading, and `pnpm check` reports it after loading as `uncounted_values`.
- Channels stay open on purpose, so a new channel is still configuration only. The cost is that an unmapped channel is only a warning.

**Where to look.** `src/config/tenants.ts` (`tenantConfigSchema`), `src/config/sources.ts`, `src/onboarding/validate.ts`, `test/tenants.test.ts`, `test/validate.test.ts`.

## 7. What the data could not settle, and what is unfinished

**Decisions the data could not settle** ([TRADEOFFS.md](../TRADEOFFS.md#decisions-the-data-could-not-settle)):

| Question | Decision |
|----------|----------|
| Lumen's finance summary labels euro amounts as `USD` | Currency comes from tenant config; file labels are kept but not trusted |
| Reported net cannot be derived from gross minus refunds, and is sometimes higher than gross | Net is not reconciled. Ours is gross minus non-orphan refunds, on the day of the refund |
| `spend` became `cost_usd` while lumen is in euros | Treated as a rename, with no conversion, and flagged |
| 6 refunds per tenant point to an order that does not exist | Kept, excluded from refunds and net, reported as `orphan_refund_count` and `orphan_refund_amount` |

**Known gaps** ([TRADEOFFS.md](../TRADEOFFS.md#known-gaps)), among others:

- A number told before its batch went missing stays visible until the batch arrives. `pnpm check` alerts on it; retracting it is a product decision.
- A source with no manifest entries is flagged by `pnpm check`, but `pnpm report` still publishes its marts.
- A day where every order is unreadable is published as zero.
- Before an order arrives, its refund counts as an orphan.

**Not built**, in three groups in the [README status table](../README.md#status): FX conversion, because the data cannot settle it; scheduling and alerting, because they depend on the environment, and the `pnpm check` exit codes are the interface for them; streaming large files, deferred on purpose to keep all-or-nothing loading per file simple.

**How it was built.** The work was built with AI assistance under the author's direction, and each change was adversarially reviewed before commit. A final round of blind reviewers, given only the brief and the repository, found real issues, including the wrong explanation of lumen's restatements described in [section 5](#5-late-arrivals); they were fixed. The newest tests were checked by breaking the code on purpose to see each one fail. The git history is in reviewable units.
