import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from src.chain import GENESIS_PREV_HASH
from src.domain import Actor, ConflictError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class ChainTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "test.db"
        self.repo = SQLiteRepository(self.db_path)
        self.service = DomainService(self.repo, RuleEngine())
        self.admin = Actor("admin", "admin")

    def tearDown(self):
        self.tmp.cleanup()

    def _prepare_release(self):
        instrument = self.service.create(
            self.admin, "instrument", {"name": "Analyzer", "serial": "A-1"}
        )
        self.service.transition(
            self.admin, instrument["id"], "send_calibration", {}
        )
        self.service.transition(
            self.admin,
            instrument["id"],
            "calibrate",
            {"due_at": "2099-01-01", "passed": True},
        )
        method = self.service.create(self.admin, "method", {"name": "Assay-A", "version": "v1"})
        self.service.transition(
            self.admin,
            method["id"],
            "validate_method",
            {"parameters": {"range": [0, 10]}, "instrument_ids": [instrument["id"]]},
        )
        result = self.service.create(
            self.admin, "result", {"sample_id": "S-1", "measurement": "initial"}
        )
        return instrument, method, result

    def _release(self, instrument, method, result):
        return self.service.transition(
            self.admin,
            result["id"],
            "release",
            {
                "instrument_id": instrument["id"],
                "method_id": method["id"],
                "value": 4.2,
                "unit": "mg/L",
            },
        )

    def test_chain_is_continuous_and_release_links_conclusions(self):
        instrument, method, result = self._prepare_release()
        self._release(instrument, method, result)

        chain = self.repo.list_chain(result["id"])
        self.assertEqual([entry["seq"] for entry in chain], [1, 2])
        self.assertEqual(chain[0]["prev_hash"], GENESIS_PREV_HASH)
        self.assertEqual(chain[1]["prev_hash"], chain[0]["hash"])

        release_entry = chain[-1]["payload"]
        links = release_entry["links"]
        self.assertEqual(links["instrument"]["id"], instrument["id"])
        self.assertEqual(links["method"]["id"], method["id"])
        self.assertEqual(links["instrument"]["status"], "active")
        self.assertEqual(links["method"]["status"], "validated")
        self.assertIn(links["instrument"]["chain_head"], self.repo.chain_hashes(instrument["id"]))
        self.assertIn(links["method"]["chain_head"], self.repo.chain_hashes(method["id"]))

        for entity in (instrument, method, result):
            self.assertTrue(self.service.verify_chain(entity["id"])["ok"])

    def test_tampering_with_old_record_breaks_verification(self):
        instrument, method, result = self._prepare_release()
        self._release(instrument, method, result)
        self.assertTrue(self.service.verify_chain(result["id"])["ok"])

        # 有人直接改旧审计记录里的数值
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                "UPDATE audit_log SET detail = ? WHERE entity_id = ? AND action = ?",
                (json.dumps({"patch": {"value": 9.9}}, sort_keys=True), result["id"], "release"),
            )
        report = self.service.verify_chain(result["id"])
        self.assertFalse(report["ok"])
        self.assertTrue(any("altered" in item["reason"] for item in report["breaks"]))

    def test_deleted_chain_entry_is_detected(self):
        instrument, method, result = self._prepare_release()
        self._release(instrument, method, result)
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                "DELETE FROM chain_entries WHERE entity_id = ? AND seq = 1", (result["id"],)
            )
        report = self.service.verify_chain(result["id"])
        self.assertFalse(report["ok"])
        reasons = [item["reason"] for item in report["breaks"]]
        self.assertTrue(any("sequence break" in reason for reason in reasons))
        self.assertTrue(any("prev_hash mismatch" in reason for reason in reasons))

    def test_concurrent_actions_first_confirmed_wins_no_fork(self):
        entity = self.service.create(
            self.admin, "instrument", {"name": "I", "serial": "S"}
        )
        barrier = threading.Barrier(2)
        outcomes = []

        def act(user):
            barrier.wait()
            try:
                self.service.transition(
                    Actor(user, "admin"),
                    entity["id"],
                    "send_calibration",
                    {},
                    expected_version=1,
                )
                outcomes.append("ok")
            except ConflictError as exc:
                outcomes.append(str(exc))

        threads = [threading.Thread(target=act, args=("u%d" % n,)) for n in (1, 2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(outcomes.count("ok"), 1)
        conflicts = [item for item in outcomes if item != "ok"]
        self.assertEqual(len(conflicts), 1)
        self.assertIn("重新读取", conflicts[0])

        chain = self.repo.list_chain(entity["id"])
        self.assertEqual([entry["seq"] for entry in chain], [1, 2])
        self.assertEqual(len({entry["hash"] for entry in chain}), 2)
        self.assertTrue(self.service.verify_chain(entity["id"])["ok"])
        self.assertEqual(self.service.get(entity["id"])["version"], 2)

    def test_failed_save_leaves_no_state_and_no_credential(self):
        entity = self.service.create(
            self.admin, "instrument", {"name": "I", "serial": "S"}
        )
        original = self.repo._insert_chain_entry

        def boom(*args, **kwargs):
            raise sqlite3.OperationalError("simulated database failure")

        self.repo._insert_chain_entry = boom
        try:
            with self.assertRaises(sqlite3.OperationalError):
                self.service.transition(
                    self.admin, entity["id"], "send_calibration", {}
                )
        finally:
            self.repo._insert_chain_entry = original

        after = self.service.get(entity["id"])
        self.assertEqual(after["status"], "active")
        self.assertEqual(after["version"], 1)
        self.assertEqual(len(self.repo.list_audit(entity["id"])), 1)
        self.assertEqual(len(self.repo.list_chain(entity["id"])), 1)
        self.assertTrue(self.service.verify_chain(entity["id"])["ok"])

    def test_failed_create_leaves_nothing_behind(self):
        original = self.repo._insert_chain_entry
        self.repo._insert_chain_entry = lambda *a, **k: (_ for _ in ()).throw(
            sqlite3.OperationalError("simulated database failure")
        )
        try:
            with self.assertRaises(sqlite3.OperationalError):
                self.service.create(
                    self.admin,
                    "instrument",
                    {"name": "I", "serial": "S"},
                    idempotency_key="k-1",
                )
        finally:
            self.repo._insert_chain_entry = original

        self.assertEqual(self.repo.list_entities(), [])
        self.assertEqual(self.repo.list_audit(), [])
        self.assertEqual(self.repo.list_chain(), [])
        self.assertIsNone(self.repo.get_idempotency("admin", "k-1"))

    def _legacy_insert(self, entity_id, kind, status, version, data, audit_rows):
        """模拟旧系统：只有实体和审计记录，没有链。"""
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, 'legacy', '2026-01-01T00:00:00+00:00', '2026-01-02T00:00:00+00:00')",
                (entity_id, kind, status, version, json.dumps(data, sort_keys=True)),
            )
            for index, (action, from_status, to_status) in enumerate(audit_rows):
                connection.execute(
                    "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
                    "VALUES (?, 'legacy', 'admin', ?, ?, ?, '{}', ?)",
                    (
                        entity_id,
                        action,
                        from_status,
                        to_status,
                        "2026-01-0%dT00:00:00+00:00" % (index + 1),
                    ),
                )

    def test_migration_backfills_history_in_order_and_queries_still_work(self):
        self._legacy_insert(
            "legacy-result",
            "result",
            "released",
            3,
            {"sample_id": "S-old", "measurement": "m"},
            [
                ("create", None, "pending"),
                ("release", "pending", "released"),
            ],
        )
        self._legacy_insert(
            "legacy-instrument",
            "instrument",
            "active",
            1,
            {"name": "Old", "serial": "O-1"},
            [("create", None, "active")],
        )
        self.assertEqual(self.repo.pending_chain_entities(), ["legacy-instrument", "legacy-result"])

        migrated = self.service.migrate_chains()
        self.assertEqual(migrated["migrated"], 2)

        chain = self.repo.list_chain("legacy-result")
        self.assertEqual([entry["payload"]["action"] for entry in chain], ["create", "release"])
        self.assertEqual(chain[0]["prev_hash"], GENESIS_PREV_HASH)
        self.assertEqual(chain[1]["prev_hash"], chain[0]["hash"])
        self.assertTrue(self.service.verify_chain("legacy-result")["ok"])
        self.assertTrue(self.service.verify_chain("legacy-instrument")["ok"])

        # 原有查询方式不受影响
        self.assertEqual(len(self.service.audit_log("legacy-result")), 2)
        self.assertEqual(self.service.get("legacy-result")["status"], "released")

        # 迁移后收样与签发照常，且新动作接在链尾
        instrument, method, result = self._prepare_release()
        self._release(instrument, method, result)
        self.assertTrue(self.service.verify_chain(result["id"])["ok"])

    def test_legacy_entity_is_backfilled_lazily_on_next_write(self):
        self._legacy_insert(
            "legacy-instrument",
            "instrument",
            "active",
            1,
            {"name": "Old", "serial": "O-1"},
            [("create", None, "active")],
        )
        # 迁移尚未跑到这个实体，新动作先把它补成链再追加，不分叉
        self.service.transition(
            self.admin, "legacy-instrument", "send_calibration", {}
        )
        chain = self.repo.list_chain("legacy-instrument")
        self.assertEqual(
            [entry["payload"]["action"] for entry in chain], ["create", "send_calibration"]
        )
        self.assertEqual([entry["seq"] for entry in chain], [1, 2])
        self.assertTrue(self.service.verify_chain("legacy-instrument")["ok"])
        self.assertEqual(self.repo.pending_chain_entities(), [])


if __name__ == "__main__":
    unittest.main()
