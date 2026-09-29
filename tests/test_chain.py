import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from src.chain import GENESIS_HASH
from src.domain import Actor, ConflictError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


ADMIN = Actor("admin", "admin")
ANALYST = Actor("analyst-1", "analyst")
ANALYST2 = Actor("analyst-2", "analyst")


def build_full_workflow(service):
    """收样 -> 设备校准链 -> 方法授权 -> 签发，返回各对象 id。"""
    instrument = service.create(
        ADMIN, "instrument", {"name": "Analyzer", "serial": "A-1"}
    )
    service.transition(
        ADMIN, instrument["id"], "send_calibration", {}, expected_version=instrument["version"]
    )
    instrument = service.get(instrument["id"])
    service.transition(
        ADMIN,
        instrument["id"],
        "calibrate",
        {"due_at": "2099-01-01", "passed": True},
        expected_version=instrument["version"],
    )

    calibration = service.create(
        Actor("metro-1", "metrology"),
        "calibration",
        {"instrument_id": instrument["id"], "requested_at": "2026-01-01"},
    )
    service.transition(
        Actor("metro-1", "metrology"),
        calibration["id"],
        "perform",
        {"result": "passed", "performed_at": "2026-01-02", "uncertainty": 0.01,
         "due_at": "2099-01-01"},
    )
    calibration = service.get(calibration["id"])
    service.transition(
        Actor("auth-1", "authorizer"),
        calibration["id"],
        "approve",
        {"authorized_by": "QA-1"},
        expected_version=calibration["version"],
    )

    method = service.create(
        Actor("auth-1", "authorizer"),
        "method",
        {"name": "Assay-A", "version": "v1"},
    )
    service.transition(
        Actor("auth-1", "authorizer"),
        method["id"],
        "validate_method",
        {"parameters": {"range": [0, 10]}, "instrument_ids": [instrument["id"]]},
    )

    result = service.create(
        ANALYST, "result", {"sample_id": "S-1", "measurement": "initial"}
    )
    service.transition(
        ANALYST,
        result["id"],
        "release",
        {"instrument_id": instrument["id"], "method_id": method["id"],
         "value": 4.2, "unit": "mg/L"},
    )
    return {
        "instrument": instrument["id"],
        "calibration": calibration["id"],
        "method": method["id"],
        "result": result["id"],
    }


class ChainServiceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "test.db")
        self.repo = SQLiteRepository(self.db_path)
        self.service = DomainService(self.repo, RuleEngine())
        self.repo.wait_for_migration()

    def tearDown(self):
        self.tmp.cleanup()

    def test_chain_is_linear_from_intake_to_issuance(self):
        ids = build_full_workflow(self.service)
        report = self.service.verify_chain()
        self.assertTrue(report["ok"], report)

        chain = self.service.chain(entity_id=ids["result"])
        actions = [node["action"] for node in chain]
        self.assertEqual(actions, ["create", "release"])

        # 结果链与设备、方法链同属一条全局连续链。
        all_nodes = self.service.chain()
        seqs = [node["seq"] for node in all_nodes]
        self.assertEqual(seqs, list(range(1, len(seqs) + 1)))
        prev = GENESIS_HASH
        for node in all_nodes:
            self.assertEqual(node["prev_hash"], prev)
            prev = node["entry_hash"]

        result = self.service.get(ids["result"])
        self.assertEqual(result["head_seq"], all_nodes[-1]["seq"])
        self.assertEqual(result["head_hash"], all_nodes[-1]["entry_hash"])

    def test_calibration_and_method_conclusions_enter_same_chain(self):
        ids = build_full_workflow(self.service)
        release_node = [
            node for node in self.service.chain(entity_id=ids["result"])
            if node["action"] == "release"
        ][0]
        evidence = release_node["detail"]["evidence"]
        self.assertEqual(evidence["instrument"]["status"], "active")
        self.assertEqual(evidence["latest_calibration"]["status"], "approved")
        self.assertEqual(evidence["latest_calibration"]["authorized_by"], "QA-1")
        self.assertEqual(evidence["method"]["status"], "validated")
        # 被引用结论的链锚点也在链上，哈希共同咬合。
        self.assertTrue(self.service.verify_chain()["ok"])

    def test_concurrent_conflict_first_confirmation_occupies_position(self):
        result = self.service.create(
            ANALYST, "result", {"sample_id": "S-9", "measurement": "x"}
        )
        instrument = self.service.create(
            ADMIN, "instrument", {"name": "I", "serial": "S"}
        )
        self.service.transition(
            ADMIN, instrument["id"], "send_calibration", {},
        )
        instrument = self.service.get(instrument["id"])
        self.service.transition(
            ADMIN, instrument["id"], "calibrate",
            {"due_at": "2099-01-01", "passed": True},
        )
        calibration = self.service.create(
            ADMIN, "calibration",
            {"instrument_id": instrument["id"], "requested_at": "2026-01-01"},
        )
        self.service.transition(
            ADMIN, calibration["id"], "perform",
            {"result": "passed", "performed_at": "2026-01-02",
             "uncertainty": 0.01, "due_at": "2099-01-01"},
        )
        calibration = self.service.get(calibration["id"])
        self.service.transition(
            ADMIN, calibration["id"], "approve", {"authorized_by": "QA"},
        )
        method = self.service.create(
            ADMIN, "method", {"name": "M", "version": "v1"}
        )
        self.service.transition(
            ADMIN, method["id"], "validate_method",
            {"parameters": {"range": [0, 1]}, "instrument_ids": [instrument["id"]]},
        )

        # 两个岗位基于同一版本几乎同时处理同一份结果。
        payload = {"instrument_id": instrument["id"], "method_id": method["id"],
                   "value": 1.0, "unit": "mg/L"}
        outcomes = []
        barrier = threading.Barrier(2)

        def worker(actor):
            barrier.wait()
            try:
                updated = self.service.transition(
                    actor, result["id"], "release", dict(payload),
                    expected_version=result["version"],
                    expected_head_seq=result["head_seq"],
                )
                outcomes.append(("ok", actor.user_id, updated["head_seq"]))
            except ConflictError as exc:
                outcomes.append(("conflict", actor.user_id, str(exc)))

        t1 = threading.Thread(target=worker, args=(ANALYST,))
        t2 = threading.Thread(target=worker, args=(ANALYST2,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        statuses = sorted(item[0] for item in outcomes)
        self.assertEqual(statuses, ["conflict", "ok"])
        winner = next(item for item in outcomes if item[0] == "ok")
        loser = next(item for item in outcomes if item[0] == "conflict")
        self.assertIn("重新读取", loser[2])

        stored = self.service.get(result["id"])
        self.assertEqual(stored["status"], "released")
        self.assertEqual(stored["head_seq"], winner[2])
        self.assertEqual(stored["version"], 2)

        # 链没有分叉：失败动作不得留下任何节点，全局序号仍然连续。
        result_nodes = self.service.chain(entity_id=result["id"])
        self.assertEqual(len(result_nodes), 2)
        self.assertTrue(self.service.verify_chain()["ok"])

        # 后到者重读后，链位置已前进；对新的待处理结果再占旧位置仍被拒绝。
        other = self.service.create(
            ANALYST2, "result", {"sample_id": "S-10", "measurement": "y"}
        )
        with self.assertRaises(ConflictError):
            self.service.transition(
                ANALYST2, other["id"], "block", {"reason": "r"},
                expected_version=other["version"],
                expected_head_seq=other["head_seq"] - 1,
            )

    def test_tampering_with_old_record_breaks_chain_at_that_point(self):
        ids = build_full_workflow(self.service)
        target_seq = self.service.chain(entity_id=ids["calibration"])[0]["seq"]

        connection = sqlite3.connect(self.db_path)
        connection.execute(
            "UPDATE chain_records SET to_status = 'hacked' WHERE seq = ?",
            (target_seq,),
        )
        connection.commit()
        connection.close()

        report = self.service.verify_chain()
        self.assertFalse(report["ok"])
        self.assertEqual(report["break_at"], target_seq)
        self.assertIn("entry_hash mismatch", report["reason"])

    def test_bypassing_chain_to_edit_state_is_detected(self):
        ids = build_full_workflow(self.service)
        connection = sqlite3.connect(self.db_path)
        connection.execute(
            "UPDATE entities SET status = 'released' WHERE id = ?",
            (ids["result"],),
        )
        # 让 pending -> released 以模拟“直接改状态”
        connection.execute(
            "UPDATE entities SET status = 'altered' WHERE id = ?",
            (ids["result"],),
        )
        connection.commit()
        connection.close()

        report = self.service.verify_chain()
        self.assertFalse(report["ok"])
        self.assertIn("current state", report["reason"])

    def test_save_failure_leaves_neither_state_nor_credentials(self):
        # 让链节点插入后、提交前的实体更新阶段失败：整个事务必须回滚，
        # 状态变化、链节点、审计、幂等凭据一样都不能留下。
        original = SQLiteRepository.transition_entity_atomic
        failures = {"count": 0}

        def broken(self, *args, **kwargs):
            failures["count"] += 1
            raise sqlite3.DatabaseError("simulated power loss mid-save")

        SQLiteRepository.transition_entity_atomic = broken
        try:
            result = self.service.create(
                ANALYST,
                "result",
                {"sample_id": "S-42", "measurement": "x"},
                idempotency_key="intake-42",
            )
            before_chain = len(self.service.chain())
            with self.assertRaises(sqlite3.DatabaseError):
                self.service.transition(
                    ANALYST, result["id"], "block", {"reason": "suspect"},
                )
        finally:
            SQLiteRepository.transition_entity_atomic = original

        stored = self.service.get(result["id"])
        self.assertEqual(stored["status"], "pending")
        self.assertEqual(stored["version"], 1)
        self.assertEqual(len(self.service.audit_log(entity_id=result["id"])), 1)
        self.assertEqual(len(self.service.chain(entity_id=result["id"])), 1)
        self.assertEqual(len(self.service.chain()), before_chain)
        self.assertTrue(self.service.verify_chain()["ok"])

        # 重试：此前留下的只是 create 的幂等凭据，与失败动作无关。
        self.service.transition(
            ANALYST, result["id"], "block", {"reason": "suspect"},
        )
        self.assertEqual(self.service.get(result["id"])["status"], "blocked")

    def test_create_failure_leaves_no_credentials(self):
        original = SQLiteRepository.create_entity_atomic
        SQLiteRepository.create_entity_atomic = staticmethod(
            lambda *a, **k: (_ for _ in ()).throw(
                sqlite3.DatabaseError("power loss on intake")
            )
        )
        try:
            with self.assertRaises(sqlite3.DatabaseError):
                self.service.create(
                    ANALYST, "result", {"sample_id": "S-77", "measurement": "x"},
                    idempotency_key="intake-77",
                )
        finally:
            SQLiteRepository.create_entity_atomic = original

        # 凭据随事务回滚，用同一个键可正常重新收样。
        retry = self.service.create(
            ANALYST, "result", {"sample_id": "S-77", "measurement": "x"},
            idempotency_key="intake-77",
        )
        self.assertEqual(retry["status"], "pending")
        self.assertTrue(self.service.verify_chain()["ok"])


class BackfillMigrationTest(unittest.TestCase):
    """旧库：先存在一批没有链的审计，再在线迁移。"""

    def _legacy_db(self, path):
        """用最小 DDL 造一个“旧版本”数据库并写入历史审计。"""
        connection = sqlite3.connect(path)
        connection.executescript(
            """
            CREATE TABLE entities (
                id TEXT PRIMARY KEY, kind TEXT NOT NULL, status TEXT NOT NULL,
                version INTEGER NOT NULL, data TEXT NOT NULL, created_by TEXT NOT NULL,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE TABLE audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT, entity_id TEXT NOT NULL,
                actor_id TEXT NOT NULL, actor_role TEXT NOT NULL, action TEXT NOT NULL,
                from_status TEXT, to_status TEXT NOT NULL, detail TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE idempotency (
                actor_id TEXT NOT NULL, idem_key TEXT NOT NULL, entity_id TEXT NOT NULL,
                created_at TEXT NOT NULL, PRIMARY KEY(actor_id, idem_key)
            );
            """
        )
        for index in range(4):
            eid = "old-result-%d" % index
            connection.execute(
                "INSERT INTO entities VALUES (?, 'result', 'released', 2, ?, "
                "'legacy', ?, ?)",
                (
                    eid,
                    '{"sample_id": "OLD-%d"}' % index,
                    "2026-01-0%dT00:00:00+00:00" % index,
                    "2026-01-0%dT00:00:00+00:00" % (index + 1),
                ),
            )
            connection.execute(
                "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, "
                "from_status, to_status, detail, created_at) VALUES "
                "('old-result-%d', 'legacy', 'analyst', 'create', NULL, 'pending', "
                "'{\"kind\":\"result\"}', '2026-01-0%dT00:00:00+00:00')" % (index, index),
            )
            connection.execute(
                "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, "
                "from_status, to_status, detail, created_at) VALUES "
                "('old-result-%d', 'legacy', 'analyst', 'release', 'pending', "
                "'released', '{}', '2026-01-0%dT00:00:00+00:00')" % (index, index + 1),
            )
        connection.commit()
        connection.close()

    def test_legacy_audit_backfilled_in_original_order(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = str(Path(tmp.name) / "legacy.db")
        self._legacy_db(path)

        repo = SQLiteRepository(path)
        service = DomainService(repo, RuleEngine())

        # 迁移尚未完成时原有查询依然可用。
        legacy = service.list(kind="result")
        self.assertEqual(len(legacy), 4)
        audit = service.audit_log()
        self.assertEqual(len(audit), 8)

        # 迁移期间继续收样：新业务节点与回填节点共用同一条链。
        fresh = service.create(
            ANALYST, "result", {"sample_id": "NEW-1", "measurement": "m"}
        )

        self.assertTrue(repo.wait_for_migration(timeout=10))
        status = service.chain_status()
        self.assertTrue(status["migrated"])
        self.assertEqual(status["head_seq"], status["audit_total"])

        chain = service.chain()
        self.assertEqual([node["seq"] for node in chain], list(range(1, len(chain) + 1)))
        # 旧节点严格按 audit id 原顺序出现。
        migrated = [node for node in chain if node["migrated"]]
        self.assertEqual(len(migrated), 8)
        audit_ids = [node["source_audit_id"] for node in migrated]
        self.assertEqual(audit_ids, sorted(audit_ids))

        # 新收样的结果也在链上，并且链可完整校验。
        self.assertEqual(
            [node["action"] for node in service.chain(entity_id=fresh["id"])],
            ["create"],
        )
        self.assertTrue(service.verify_chain()["ok"])

        # 旧查询方式继续可用（含迁移期间新收的 1 条）。
        self.assertEqual(len(service.list(kind="result")), 5)
        self.assertEqual(len(service.audit_log()), 9)

        # 迁移完成后直接改老实体状态，即使没有逐版 data 也能在末节点发现。
        connection = sqlite3.connect(path)
        connection.execute(
            "UPDATE entities SET status = 'blocked' WHERE id = 'old-result-0'"
        )
        connection.commit()
        connection.close()
        report = service.verify_chain()
        self.assertFalse(report["ok"])
        self.assertIn("migrated record", report["reason"])


if __name__ == "__main__":
    unittest.main()
