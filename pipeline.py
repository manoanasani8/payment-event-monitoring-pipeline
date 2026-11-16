"""Process an append-only stream of synthetic payment events into SQLite."""
import argparse
import json
import sqlite3
from pathlib import Path

FIELDS = {"event_id", "payment_id", "state", "version", "amount_cents"}
STATES = {"authorized", "settled", "declined"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS checkpoint (source TEXT PRIMARY KEY, offset INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS event_log (
    event_id TEXT PRIMARY KEY, payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS payment_state (
    payment_id TEXT PRIMARY KEY, version INTEGER NOT NULL,
    state TEXT NOT NULL, amount_cents INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS exceptions (
    source TEXT, offset INTEGER, reason TEXT NOT NULL, raw_event TEXT NOT NULL,
    PRIMARY KEY (source, offset)
);
CREATE TABLE IF NOT EXISTS outcomes (
    source TEXT, offset INTEGER, outcome TEXT NOT NULL, PRIMARY KEY (source, offset)
);
"""

def apply_event(db, raw):
    try:
        event = json.loads(raw)
    except (UnicodeDecodeError, ValueError):
        return "invalid_json"
    if (
        not isinstance(event, dict) or event.keys() != FIELDS
        or any(not isinstance(event[key], str) or not event[key].strip()
               for key in ("event_id", "payment_id", "state"))
        or event["state"] not in STATES
        or type(event["version"]) is not int or not 1 <= event["version"] < 2**63
        or type(event["amount_cents"]) is not int or not 0 <= event["amount_cents"] < 2**63
    ):
        return "invalid_event"

    payload = json.dumps(event, sort_keys=True)
    prior = db.execute("SELECT payload FROM event_log WHERE event_id = ?",
                       (event["event_id"],)).fetchone()
    if prior:
        return "duplicate" if prior[0] == payload else "event_id_conflict"

    payment = event["payment_id"]
    version = event["version"]
    state = event["state"]
    amount = event["amount_cents"]
    current = db.execute(
        "SELECT version, state, amount_cents FROM payment_state WHERE payment_id = ?", (payment,)
    ).fetchone()
    if current and version == current[0] and (state, amount) != current[1:]:
        return "payment_version_conflict"

    db.execute("INSERT INTO event_log VALUES (?, ?)", (event["event_id"], payload))
    if current and version <= current[0]:
        return "stale" if version < current[0] else "same_version"
    db.execute("""
        INSERT INTO payment_state VALUES (?, ?, ?, ?)
        ON CONFLICT(payment_id) DO UPDATE SET version = excluded.version,
        state = excluded.state, amount_cents = excluded.amount_cents
    """, (payment, version, state, amount))
    return "applied"


def ingest(db, source, limit=None):
    db.executescript(SCHEMA)
    source = Path(source).resolve()
    source_key = str(source)
    with source.open("rb") as stream, db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT offset FROM checkpoint WHERE source = ?", (source_key,)).fetchone()
        offset = row[0] if row else 0
        if offset > source.stat().st_size:
            raise ValueError("Input was truncated. Use an append-only source.")
        stream.seek(offset)
        processed = 0
        while limit is None or processed < limit:
            raw = stream.readline()
            if not raw.endswith(b"\n"):
                break
            outcome = apply_event(db, raw)
            if outcome in {"invalid_json", "invalid_event", "event_id_conflict",
                           "payment_version_conflict"}:
                db.execute("INSERT INTO exceptions VALUES (?, ?, ?, ?)",
                           (source_key, offset, outcome, raw.decode("utf-8", errors="replace").rstrip()))
            db.execute("INSERT INTO outcomes VALUES (?, ?, ?)", (source_key, offset, outcome))
            offset = stream.tell()
            processed += 1
        db.execute("""
            INSERT INTO checkpoint VALUES (?, ?)
            ON CONFLICT(source) DO UPDATE SET offset = excluded.offset
        """, (source_key, offset))
    return processed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--db", type=Path, default=Path("payments.sqlite"))
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be positive")
    from contextlib import closing
    with closing(sqlite3.connect(args.db)) as db:
        count = ingest(db, args.source, args.limit)
        outcomes = dict(db.execute("SELECT outcome, COUNT(*) FROM outcomes GROUP BY outcome"))
        snapshot = db.execute("""
            SELECT state, COUNT(*), SUM(amount_cents)
            FROM payment_state GROUP BY state ORDER BY state
        """).fetchall()
        print(json.dumps({"processed_this_run": count, "outcomes": outcomes,
                          "current_state": [{"state": s, "count": n, "amount_cents": a}
                                            for s, n, a in snapshot]}, indent=2))


if __name__ == "__main__":
    main()

