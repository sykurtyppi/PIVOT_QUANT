# Intraday snapshot unavailable retry

## Failure policy

The intraday poll resolves touches in `(ts_event, event_id)` order against the
canonical **event-session** snapshot. Missing files, read failures (including a
file disappearing between the existence check and read), and undecodable JSON
are unavailable inputs, not evidence that a touch contradicts a snapshot.

For unavailable input the poll:

- logs the reason, pending event ID, ET session date, and event age in seconds;
- leaves that event without a terminal disposition and does not pass its cursor;
- may bind, deliver, and checkpoint the validated prefix before that event;
- returns status 3 after a successful prefix, or preserves status 1 if notification
  delivery fails; the next normal poll retries from the durable cursor.

This deliberately blocks later unbound events while input is persistently absent.
There is no new queue or age-based skip. Alert caps still bound the selected prefix.
A previously saved snapshot binding remains usable without reopening its file.
Dry runs neither deliver nor bind nor advance event state. First-start no-backfill
behavior is unchanged.

A successfully read snapshot with a failed contract/identity validation or a touch
with a label/price mismatch remains terminal `snapshot_mismatch`: it is recorded
for audit and checkpointed only after any valid prefix is delivered. This prevents
proven poison events from blocking every subsequent poll.

## Investigation without replay

Use the visible `snapshot unavailable` / `pending retry` line to identify the
symbol/session canonical artifact and how long the touch has waited. Investigate
the normal snapshot publisher and artifact readability. A later matching snapshot
allows the next ordinary poll to deliver the waiting touch once; later polls do
not duplicate the already checkpointed prefix.

Do **not** rewind cursors, delete dispositions, backfill old alerts, edit operator
SQLite databases, restart/change services, or send test messages as part of this
correction. Events already terminally quarantined by an older version are not
repaired or replayed. Any such action needs separate operator authorization and a
reviewed recovery plan. This change alone makes no production delivery guarantee
or deployment claim.

## Deterministic local verification

Use an isolated worktree and its pinned Python 3.11 smoke environment per
`AGENTS.md`. Explicitly select it when an inherited `PYTHON_BIN` points elsewhere:

```bash
PYTHON_BIN="$PWD/.venv/bin/python" bash scripts/_pybin_exec.sh -m unittest discover \
  -s tests/python -p 'test_levels_product*.py' -v
PYTHON_BIN="$PWD/.venv/bin/python" npm run ml:test:smoke
npm run lint && npm test && npm run build
```

The new poll regressions use temporary SQLite databases and real canonical
snapshot files with a frozen clock and stubbed transport/rates. They cover absent
then published input, an ordered valid/missing/later sequence without duplicate
prefix delivery, repeated fresh state connections and caps, dry run, notifier
failure with saved binding reuse, a real read-time disappearance race, and actual
identity and price mismatches. They do not contact notification endpoints or
exercise operator services. Independent spec/quality review and CI are separate
requirements before any readiness decision.
