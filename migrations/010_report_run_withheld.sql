-- What a report run held back because the data behind it is incomplete.
--
-- A mart key whose day falls inside the window (covers_from..covers_to) of a manifest
-- batch that never loaded, for a source the mart is built from, is withheld: not
-- published, not tombstoned, not restated. Whatever version the client was told before
-- stays the latest. `withheld` records, per mart, each incomplete window the run found
-- and the keys it held back for it, so an auditor can tell a key that was not published
-- because it was unchanged from one that was not published because it was not complete.
-- Shape: [{"mart", "source", "batch", "path", "covers_from", "covers_to", "status",
--          "keys": [{"day", "dims"}]}]; '[]' for runs before this migration and runs
-- that withheld nothing. A window with no keys still says the mart is incomplete there.
ALTER TABLE ops.report_run ADD COLUMN withheld jsonb NOT NULL DEFAULT '[]'
  CHECK (jsonb_typeof(withheld) = 'array');
