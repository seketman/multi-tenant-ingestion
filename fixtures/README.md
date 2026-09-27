# Deep dive fixtures

Four sources for two tenants, delivered the way exports actually land: a
sequence of batch files per source, one format per source.

- `orders/`        CSV, one row per order
- `email_events/`  NDJSON, one JSON object per line
- `ad_spend/`      CSV, daily spend per campaign
- `refunds/`       CSV, one row per refund

`manifest.json` lists every batch the set is supposed to contain, with the
window each one covers. `<tenant>/finance_summary.csv` is what the client
reports they earned, by day.

These fixtures contain the failures described in the brief. They are there
on purpose and they are not all obvious. Some of what you find cannot be
solved from the data at all; the brief says what to do about those. Reading
all of it before you start building is time well spent.
