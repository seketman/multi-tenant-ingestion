# multi-tenant-ingestion

```sh
pnpm install
pnpm db:up          # Postgres 17 on localhost:54329
pnpm migrate        # applies migrations/ and seeds ops.tenant from tenants/*.json
pnpm typecheck && pnpm test
```
