# Candle timestamp contract

## Scope and correction

`parse_candles` accepts provider **bar-open Unix seconds**. `insert_bars`
converts those seconds to SQLite `bar_data.ts` **Unix milliseconds**; it does
not reinterpret the timezone. RTH classification uses `America/New_York` and
its DST rules. This change does not redefine that calendar or RTH boundary.

A duration is not a grid origin. In particular, 60-minute bars can open at
09:30/10:30 ET instead of on UTC epoch-hour boundaries. Flooring them to the
epoch changes stored bar opens to 09:00/10:00 ET, drops the opening bar from
RTH aggregation, and corrupts session OHLC. A frozen two-bar fixture has
open/high/low 100/150/50; the old path aggregates only 110/112/109.

`normalize_candles_to_interval` now preserves each provider timestamp by
default, sorts by timestamp, and keeps the **last input occurrence** of an
exact duplicate. It copies rows rather than mutating its input. Parsing still
uses a stable timestamp sort: equal-time provider updates keep payload order;
for the explicitly snapped minute path, the later provider timestamp wins
within a minute. No new update-order or OHLC merge policy is introduced.

The existing provisional wall-clock-second workaround is deliberately narrow:

| Caller contract | Behavior |
| --- | --- |
| Requested interval exactly `1m`, resolved provider Yahoo (direct or proxy) | Explicit `snap_provisional_minute=True`; floor to the UTC minute; last update wins |
| IBKR, marketdata.app, unknown provider, or any other interval | Preserve timestamps; exact deduplication only |
| Direct helper call with no explicit opt-in | Preserve timestamps; exact deduplication only |
| Opt-in accidentally passed for a duration other than 60 seconds | Preserve timestamps |

Both backfill and live collection use the **resolved** source, not `auto` or
the initial routing preference. This retains the existing Yahoo minute-jitter
regression while refusing to extrapolate that workaround to unproven sources
or coarser intervals. No first-bar origin inference, fixed 09:30 offset, close-
to-open conversion, or generalized provider/calendar inference is performed.
A future provider-specific grid must have an explicit, separately tested
contract before introducing any new rebucketing.

## Offline regression coverage

`tests/python/test_candle_timestamp_normalization.py` covers:

- Parse → normalize → temporary SQLite write/read → RTH aggregation for the
  09:30/10:30 hourly fixture on March 6/9 and October 30/November 2, 2026
  (weekdays on both sides of US DST changes). Both rows, millisecond storage,
  interval metadata, session OHLC and RTH count must survive.
- Session-offset, UTC-aligned and arbitrary-origin series across minute,
  coarse intraday, daily and weekly durations; stable exact deduplication,
  sorted output, and no input mutation.
- Existing minute jitter, out-of-order provider updates, and equal-time
  payload ordering.
- Both actual callers with frozen fetched payloads, real parsing and temporary
  SQLite persistence; Yahoo, IBKR, marketdata.app and unknown resolved sources
  across `1m`, `5m`, `60m` and `90m`. Network/gamma/label work is stubbed out.

Daily/weekly preservation is a **normalizer** guarantee, not a claim that these
intraday event collectors now support daily/weekly CLI workflows. Existing
interval parsing is unchanged: backfill's reliable duration syntax remains
minute-form (`60m`, not `1h`), while the live parser also accepts hour-form.
Expanding interval parsing or event/label semantics is outside this correction.

## Historical impact assessment: procedure only

**No production database or provider history was inspected or repaired.**
This fix prevents future rebucketing; it cannot recover previously shifted
opens, overwritten rows, or derived events/features/labels.

For a separately authorized assessment:

1. Record the deployed code SHA, affected ingestion time window, configured
   source/interval and actual resolved source. Work only on a verified backup
   or immutable copy, never the live writable database.
2. Inventory intervals and timestamps in the copy. Epoch-hour alignment alone
   is not proof of damage: legitimate provider grids may be UTC-aligned.
3. Compare suspect rows against retained raw payloads or separately authorized
   provider refetches with provenance. Verify original bar-open semantics,
   timestamp units, ET session date and DST offsets. Do not infer an origin
   from the first surviving row, or shift every row forward by 30 minutes.
4. On a disposable copy, compare stored opens/OHLC, RTH membership and affected
   sessions to the source. Trace dependent events, features and labels before
   considering any rebuild. Quantify differences rather than treating this
   fixture as a measured production impact.
5. Require independent review, an approved repair plan, backup/rollback and
   post-repair verification before any historical rewrite or deployment.

Rollback of this code change alone restores the defective coarse-grid behavior;
it is not a data repair. No migration, production service, notification or
historical reconciliation is part of this PR.
