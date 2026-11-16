import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from pipeline import ingest


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.source = Path(self.temp.name) / "events.jsonl"
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)

    def write(self, *events):
        self.source.write_text("".join(json.dumps(event) + "\n" for event in events))

    @staticmethod
    def event(event_id, version, state="authorized", amount=25):
        return dict(event_id=event_id, payment_id="payment", version=version,
                    state=state, amount_cents=amount)

    def test_resume_duplicate_and_late_event(self):
        newest = self.event("new", 2, "settled")
        self.write(newest, newest, self.event("late", 1))
        self.assertEqual(ingest(self.db, self.source, limit=1), 1)
        self.assertEqual(ingest(self.db, self.source), 2)
        self.assertEqual(ingest(self.db, self.source), 0)
        self.assertEqual(self.db.execute("SELECT version, state FROM payment_state").fetchone(),
                         (2, "settled"))
        self.assertEqual(dict(self.db.execute(
            "SELECT outcome, COUNT(*) FROM outcomes GROUP BY outcome"
        )), {"applied": 1, "duplicate": 1, "stale": 1})

    def test_quarantines_invalid_and_conflicting_events(self):
        self.write(self.event("a", 2), self.event("a", 2, "settled"),
                   self.event("b", 2, "declined"), self.event("bad", 0))
        self.assertEqual(ingest(self.db, self.source), 4)
        self.assertEqual(self.db.execute("SELECT reason FROM exceptions ORDER BY offset").fetchall(),
                         [("event_id_conflict",), ("payment_version_conflict",), ("invalid_event",)])

    def test_partial_line_waits_for_newline(self):
        self.source.write_bytes(json.dumps(self.event("a", 1)).encode())
        self.assertEqual(ingest(self.db, self.source), 0)
        with self.source.open("ab") as stream:
            stream.write(b"\n")
        self.assertEqual(ingest(self.db, self.source), 1)

    def test_failed_transaction_does_not_advance_checkpoint(self):
        self.write(self.event("a", 1))
        ingest(self.db, self.source)
        with self.source.open("a") as stream:
            stream.write(json.dumps(self.event("b", 2, "settled")) + "\n")
        self.db.execute("""
            CREATE TRIGGER fail_outcome BEFORE INSERT ON outcomes
            BEGIN SELECT RAISE(ABORT, 'simulated failure'); END
        """)
        with self.assertRaises(sqlite3.IntegrityError):
            ingest(self.db, self.source)
        self.assertEqual(self.db.execute("SELECT version FROM payment_state").fetchone(), (1,))
        self.db.execute("DROP TRIGGER fail_outcome")
        self.assertEqual(ingest(self.db, self.source), 1)
        self.assertEqual(self.db.execute("SELECT version FROM payment_state").fetchone(), (2,))


if __name__ == "__main__":
    unittest.main()

