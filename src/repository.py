import json
import sqlite3
import threading
import time
from datetime import datetime, timezone

from .chain import (
    GENESIS_HASH,
    compute_entry_hash,
    compute_state_hash,
)
from .domain import ConflictError, NotFoundError

# 后台回填线程单批搬运的旧审计条数；回填与业务写入串行，批量要短，
# 保证“迁移期间不停止收样”。
_BACKFILL_BATCH = 25
_BACKFILL_SLEEP = 0.05


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class SQLiteRepository:
    def __init__(self, path):
        self.path = str(path)
        self._initialize()
        self._backfill_thread = threading.Thread(
            target=self._backfill_loop, name="chain-backfill", daemon=True
        )
        self._backfill_thread.start()

    def _connect(self):
        # isolation_level=None：所有事务由代码显式 BEGIN/COMMIT 控制，
        # 避免 sqlite3 模块在语句前隐式开事务而打乱原子边界。
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_entities_kind_status
                    ON entities(kind, status);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_entity
                    ON audit_log(entity_id, id);
                CREATE TABLE IF NOT EXISTS idempotency (
                    actor_id TEXT NOT NULL,
                    idem_key TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_id, idem_key)
                );
                CREATE TABLE IF NOT EXISTS chain_meta (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    head_seq INTEGER NOT NULL,
                    head_hash TEXT NOT NULL,
                    migrated INTEGER NOT NULL,
                    audit_watermark INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS chain_records (
                    seq INTEGER PRIMARY KEY,
                    entity_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    state_hash TEXT NOT NULL,
                    entry_hash TEXT NOT NULL,
                    prev_hash TEXT NOT NULL,
                    source_audit_id INTEGER,
                    migrated INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_chain_entity
                    ON chain_records(entity_id, seq);
                CREATE INDEX IF NOT EXISTS idx_chain_audit
                    ON chain_records(source_audit_id);
                """
            )
            self._ensure_column(connection, "entities", "head_seq", "INTEGER")
            self._ensure_column(connection, "entities", "head_hash", "TEXT")
            row = connection.execute(
                "SELECT head_seq, head_hash, migrated, audit_watermark FROM chain_meta WHERE id = 1"
            ).fetchone()
            if not row:
                # 旧版本库没有链：若已存在历史审计，则置为未迁移，由回填
                # 线程与业务写入共同把旧记录补进历史链；空库直接视为完成。
                legacy_count = connection.execute(
                    "SELECT COUNT(*) AS n FROM audit_log"
                ).fetchone()["n"]
                connection.execute(
                    "INSERT INTO chain_meta(id, head_seq, head_hash, migrated, audit_watermark) "
                    "VALUES (1, 0, ?, ?, 0)",
                    (GENESIS_HASH, 0 if legacy_count else 1),
                )

    @staticmethod
    def _ensure_column(connection, table, column, decl):
        existing = {
            item["name"]
            for item in connection.execute(
                "PRAGMA table_info(%s)" % table
            ).fetchall()
        }
        if column not in existing:
            connection.execute(
                "ALTER TABLE %s ADD COLUMN %s %s" % (table, column, decl)
            )

    @staticmethod
    def _entity_from_row(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "version": int(row["version"]),
            "data": json.loads(row["data"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "head_seq": row["head_seq"],
            "head_hash": row["head_hash"],
        }

    # ------------------------------------------------------------------
    # 普通查询（只读）
    # ------------------------------------------------------------------
    def get_entity(self, entity_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
        return self._entity_from_row(row) if row else None

    def list_entities(self, kind=None, status=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
            ).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def find_entities(self, kind, field, value):
        return [
            entity
            for entity in self.list_entities(kind=kind)
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def list_audit(self, entity_id=None):
        with self._connect() as connection:
            if entity_id:
                rows = connection.execute(
                    "SELECT * FROM audit_log WHERE entity_id = ? ORDER BY id",
                    (entity_id,),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM audit_log ORDER BY id"
                ).fetchall()
        return [self._audit_from_row(row) for row in rows]

    @staticmethod
    def _audit_from_row(row):
        return {
            "id": row["id"],
            "entity_id": row["entity_id"],
            "actor_id": row["actor_id"],
            "actor_role": row["actor_role"],
            "action": row["action"],
            "from_status": row["from_status"],
            "to_status": row["to_status"],
            "detail": json.loads(row["detail"]),
            "created_at": row["created_at"],
        }

    def get_idempotency(self, actor_id, idem_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                (actor_id, idem_key),
            ).fetchone()
        return row["entity_id"] if row else None

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True

    # ------------------------------------------------------------------
    # 校验链读取与验证
    # ------------------------------------------------------------------
    def chain_status(self):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT head_seq, head_hash, migrated, audit_watermark FROM chain_meta WHERE id = 1"
            ).fetchone()
            total_audit = connection.execute(
                "SELECT COUNT(*) AS n FROM audit_log"
            ).fetchone()["n"]
        return {
            "head_seq": int(row["head_seq"]),
            "head_hash": row["head_hash"],
            "migrated": bool(row["migrated"]),
            "audit_watermark": int(row["audit_watermark"]),
            "audit_total": int(total_audit),
        }

    def list_chain(self, entity_id=None, limit=None):
        sql = "SELECT * FROM chain_records"
        params = []
        if entity_id:
            sql += " WHERE entity_id = ?"
            params.append(entity_id)
        sql += " ORDER BY seq"
        if limit is not None:
            sql += " LIMIT %d" % int(limit)
        with self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [self._chain_from_row(row) for row in rows]

    @staticmethod
    def _chain_from_row(row):
        return {
            "seq": int(row["seq"]),
            "entity_id": row["entity_id"],
            "kind": row["kind"],
            "actor_id": row["actor_id"],
            "actor_role": row["actor_role"],
            "action": row["action"],
            "from_status": row["from_status"],
            "to_status": row["to_status"],
            "detail": json.loads(row["detail"]),
            "state_hash": row["state_hash"],
            "entry_hash": row["entry_hash"],
            "prev_hash": row["prev_hash"],
            "source_audit_id": (
                int(row["source_audit_id"]) if row["source_audit_id"] is not None else None
            ),
            "migrated": bool(row["migrated"]),
            "created_at": row["created_at"],
        }

    def verify_chain(self):
        """从链头反向核验所有节点。

        依次检查：序号连续、前向哈希咬合、节点内容未被改写、实体当前状态
        与其最新链节点一致、实体锚点指向链头。返回首个断口，全部通过时
        ``ok`` 为真。评审员据此能精确定位链条在哪一段断开。
        """
        with self._connect() as connection:
            meta = connection.execute(
                "SELECT * FROM chain_meta WHERE id = 1"
            ).fetchone()
            rows = connection.execute(
                "SELECT * FROM chain_records ORDER BY seq"
            ).fetchall()
            entities = {
                row["id"]: dict(row)
                for row in connection.execute("SELECT * FROM entities").fetchall()
            }

        if not int(meta["head_seq"]):
            return {
                "ok": not rows,
                "checked": 0,
                "break_at": None if not rows else 1,
                "reason": None if not rows else "chain head is empty but records exist",
            }

        prev_hash = GENESIS_HASH
        last_seq_by_entity = {}
        last_node_migrated = {}
        for index, row in enumerate(rows, start=1):
            seq = int(row["seq"])
            if seq != index:
                return self._break(seq, "seq is not continuous: expected %d" % index)
            if row["prev_hash"] != prev_hash:
                return self._break(seq, "prev_hash mismatch")
            try:
                detail = json.loads(row["detail"])
            except ValueError:
                return self._break(seq, "detail is not valid JSON")
            expected_hash = compute_entry_hash(
                seq=seq,
                entity_id=row["entity_id"],
                kind=row["kind"],
                action=row["action"],
                from_status=row["from_status"],
                to_status=row["to_status"],
                actor_id=row["actor_id"],
                actor_role=row["actor_role"],
                detail=detail,
                state_hash=row["state_hash"],
                created_at=row["created_at"],
                prev_hash=row["prev_hash"],
            )
            if expected_hash != row["entry_hash"]:
                return self._break(seq, "entry_hash mismatch: record has been altered")
            last_seq_by_entity[row["entity_id"]] = seq
            last_node_migrated[row["entity_id"]] = bool(int(row["migrated"]))
            prev_hash = row["entry_hash"]
        head_seq = int(meta["head_seq"])
        if head_seq != len(rows):
            return self._break(
                head_seq,
                "chain head seq %s does not match record count %d"
                % (head_seq, len(rows)),
            )
        if meta["head_hash"] != rows[-1]["entry_hash"]:
            return self._break(head_seq, "chain head hash mismatch")

        # 实体当前状态必须与它在链上的最后一个节点一致；实体锚点必须是
        # 该节点。绕过链直接改 entities 表会在这里暴露。
        for entity_id, seq in last_seq_by_entity.items():
            entity = entities.get(entity_id)
            if entity is None:
                return self._break(seq, "entity %s is missing" % entity_id)
            if int(entity["head_seq"] or 0) != seq:
                return self._break(
                    seq,
                    "entity %s head_seq %s does not anchor to chain %d"
                    % (entity_id, entity["head_seq"], seq),
                )
            row = rows[seq - 1]
            if entity["head_hash"] != row["entry_hash"]:
                return self._break(
                    seq,
                    "entity %s head_hash does not match its chain anchor" % entity_id,
                )
            if not last_node_migrated.get(entity_id):
                # 实时节点持有完整 state_hash，逐字段比对实体当前状态。
                current_state = compute_state_hash(
                    entity["status"], int(entity["version"]), json.loads(entity["data"])
                )
                if current_state != row["state_hash"]:
                    return self._break(
                        seq,
                        "entity %s current state does not match its chain record"
                        % entity_id,
                    )
            elif entity["status"] != row["to_status"]:
                # 历史节点没有逐版 data，可比对的是当时记录的最终状态。
                return self._break(
                    seq,
                    "entity %s status %s does not match migrated record %s"
                    % (entity_id, entity["status"], row["to_status"]),
                )

        for entity in entities.values():
            if entity["head_seq"] is None and rows:
                return self._break(
                    None,
                    "entity %s has no chain anchor" % entity["id"],
                )

        return {"ok": True, "checked": len(rows), "break_at": None, "reason": None}

    @staticmethod
    def _break(seq, reason):
        return {"ok": False, "checked": 0, "break_at": seq, "reason": reason}

    # ------------------------------------------------------------------
    # 原子业务写入
    # ------------------------------------------------------------------
    def create_entity_atomic(
        self, entity_id, kind, status, data, actor, idempotency_key=None
    ):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        detail = {"kind": kind}
        state_hash = compute_state_hash(status, 1, data)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            if idempotency_key:
                existing = connection.execute(
                    "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                    (actor.user_id, idempotency_key),
                ).fetchone()
                if existing:
                    connection.rollback()
                    existing_entity = self.get_entity(existing["entity_id"])
                    if existing_entity:
                        return existing_entity, True
            duplicate = connection.execute(
                "SELECT 1 FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if duplicate:
                raise ConflictError("entity already exists: " + entity_id)
            # 幂等与唯一性检查不会写入，先做；回填排水放在它们之后，避免一次
            # 提前返回把同事务里已搬运的回填节点一并回滚。
            self._drain_migration_locked(connection)
            seq, entry_hash = self._append_chain_locked(
                connection,
                entity_id=entity_id,
                kind=kind,
                actor=actor,
                action="create",
                from_status=None,
                to_status=status,
                detail=detail,
                state_hash=state_hash,
                created_at=now,
                migrated=False,
                source_audit_id=None,
            )
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, "
                "created_at, updated_at, head_seq, head_hash) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?)",
                (
                    entity_id,
                    kind,
                    status,
                    payload,
                    actor.user_id,
                    now,
                    now,
                    seq,
                    entry_hash,
                ),
            )
            self._insert_audit_locked(
                connection, entity_id, actor, "create", None, status, detail, now
            )
            if idempotency_key:
                connection.execute(
                    "INSERT INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                    "VALUES (?, ?, ?, ?)",
                    (actor.user_id, idempotency_key, entity_id, now),
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id), False

    def transition_entity_atomic(
        self,
        entity_id,
        expected_version,
        expected_head_seq,
        status,
        data,
        actor,
        action,
        from_status,
        detail,
    ):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._drain_migration_locked(connection)
            row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_version = int(row["version"])
            if expected_version is not None and current_version != int(expected_version):
                raise ConflictError(
                    "版本冲突：当前已是第 %s 版（你持有的是第 %s 版），请重新读取后再提交"
                    % (current_version, expected_version)
                )
            current_head = int(row["head_seq"] or 0)
            if expected_head_seq is not None and current_head != int(expected_head_seq):
                raise ConflictError(
                    "链位置冲突：位置 %s 已被先确认的动作占据（当前位置 %s），请重新读取后再提交"
                    % (expected_head_seq, current_head)
                )
            new_version = current_version + 1
            state_hash = compute_state_hash(status, new_version, data)
            seq, entry_hash = self._append_chain_locked(
                connection,
                entity_id=entity_id,
                kind=row["kind"],
                actor=actor,
                action=action,
                from_status=from_status,
                to_status=status,
                detail=detail,
                state_hash=state_hash,
                created_at=now,
                migrated=False,
                source_audit_id=None,
            )
            connection.execute(
                "UPDATE entities SET status = ?, version = ?, data = ?, "
                "updated_at = ?, head_seq = ?, head_hash = ? WHERE id = ?",
                (status, new_version, payload, now, seq, entry_hash, entity_id),
            )
            self._insert_audit_locked(
                connection, entity_id, actor, action, from_status, status, detail, now
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id)

    @staticmethod
    def _insert_audit_locked(
        connection, entity_id, actor, action, from_status, to_status, detail, now
    ):
        cursor = connection.execute(
            "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, "
            "from_status, to_status, detail, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                entity_id,
                actor.user_id,
                actor.role,
                action,
                from_status,
                to_status,
                json.dumps(detail, ensure_ascii=False, sort_keys=True),
                now,
            ),
        )
        return cursor.lastrowid

    def _append_chain_locked(
        self,
        connection,
        *,
        entity_id,
        kind,
        actor,
        action,
        from_status,
        to_status,
        detail,
        state_hash,
        created_at,
        migrated,
        source_audit_id,
    ):
        """在已持有的事务里抢占全局链的下一个位置。

        seq 在此处读取并立即写入，所有写入方都经 BEGIN IMMEDIATE 串行化，
        因此先确认的动作先占位置；后到者的乐观锁检查必然失败并被拒绝，
        链上不可能出现分叉。
        """
        meta = connection.execute(
            "SELECT head_seq, head_hash FROM chain_meta WHERE id = 1"
        ).fetchone()
        seq = int(meta["head_seq"]) + 1
        entry_hash = compute_entry_hash(
            seq=seq,
            entity_id=entity_id,
            kind=kind,
            action=action,
            from_status=from_status,
            to_status=to_status,
            actor_id=actor.user_id,
            actor_role=actor.role,
            detail=detail,
            state_hash=state_hash,
            created_at=created_at,
            prev_hash=meta["head_hash"],
        )
        connection.execute(
            "INSERT INTO chain_records(seq, entity_id, kind, actor_id, actor_role, "
            "action, from_status, to_status, detail, state_hash, entry_hash, "
            "prev_hash, source_audit_id, migrated, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                seq,
                entity_id,
                kind,
                actor.user_id,
                actor.role,
                action,
                from_status,
                to_status,
                json.dumps(detail, ensure_ascii=False, sort_keys=True),
                state_hash,
                entry_hash,
                meta["head_hash"],
                source_audit_id,
                1 if migrated else 0,
                created_at,
            ),
        )
        connection.execute(
            "UPDATE chain_meta SET head_seq = ?, head_hash = ? WHERE id = 1",
            (seq, entry_hash),
        )
        return seq, entry_hash

    # ------------------------------------------------------------------
    # 旧审计在线回填
    # ------------------------------------------------------------------
    def _backfill_once(self, batch_size=_BACKFILL_BATCH):
        """把一批旧审计记录按原顺序补进历史链。

        与业务写入共用同一把 BEGIN IMMEDIATE 锁，因此回填节点和新业务
        节点共享同一条全局链，永远不分叉。返回 (本批搬运数, 是否全部完成)。
        """
        connection = self._connect()
        moved = 0
        done = False
        try:
            connection.execute("BEGIN IMMEDIATE")
            meta = connection.execute(
                "SELECT migrated, audit_watermark FROM chain_meta WHERE id = 1"
            ).fetchone()
            if not int(meta["migrated"]):
                watermark = int(meta["audit_watermark"])
                rows = connection.execute(
                    "SELECT a.*, e.kind AS entity_kind "
                    "FROM audit_log a LEFT JOIN entities e ON a.entity_id = e.id "
                    "WHERE a.id > ? ORDER BY a.id LIMIT ?",
                    (watermark, int(batch_size)),
                ).fetchall()
                entity_head_updates = {}
                for row in rows:
                    # 旧记录当时的完整 data 已无法逐版复原，state_hash 置空
                    # 表示“历史节点只锚定操作本身，不做实体状态比对”；
                    # 实时节点始终带完整 state_hash。
                    actor = _BackfillActor(row["actor_id"], row["actor_role"])
                    seq, _ = self._append_chain_locked(
                        connection,
                        entity_id=row["entity_id"],
                        kind=row["entity_kind"] or "",
                        actor=actor,
                        action=row["action"],
                        from_status=row["from_status"],
                        to_status=to_status,
                        detail=json.loads(row["detail"]),
                        state_hash="",
                        created_at=row["created_at"],
                        migrated=True,
                        source_audit_id=int(row["id"]),
                    )
                    entity_head_updates[row["entity_id"]] = seq
                    watermark = int(row["id"])
                    moved += 1
                for entity_id, seq in entity_head_updates.items():
                    connection.execute(
                        "UPDATE entities SET head_seq = ?, head_hash = "
                        "(SELECT entry_hash FROM chain_records WHERE seq = ?) "
                        "WHERE id = ? AND (head_seq IS NULL OR head_seq < ?)",
                        (seq, seq, entity_id, seq),
                    )
                connection.execute(
                    "UPDATE chain_meta SET audit_watermark = ? WHERE id = 1",
                    (watermark,),
                )
                if moved < int(batch_size):
                    connection.execute(
                        "UPDATE chain_meta SET migrated = 1 WHERE id = 1"
                    )
                    done = True
            else:
                done = True
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return moved, done

    def _drain_migration_locked(self, connection):
        """业务事务在锁内完成剩余回填（切换点），保证取号顺序。"""
        meta = connection.execute(
            "SELECT migrated FROM chain_meta WHERE id = 1"
        ).fetchone()
        if int(meta["migrated"]):
            return
        while True:
            rows = connection.execute(
                "SELECT a.*, e.kind AS entity_kind "
                "FROM audit_log a LEFT JOIN entities e ON a.entity_id = e.id "
                "WHERE a.id > (SELECT audit_watermark FROM chain_meta WHERE id = 1) "
                "ORDER BY a.id LIMIT ?",
                (_BACKFILL_BATCH,),
            ).fetchall()
            if not rows:
                connection.execute(
                    "UPDATE chain_meta SET migrated = 1 WHERE id = 1"
                )
                return
            watermark = None
            entity_head_updates = {}
            for row in rows:
                actor = _BackfillActor(row["actor_id"], row["actor_role"])
                seq, _ = self._append_chain_locked(
                    connection,
                    entity_id=row["entity_id"],
                    kind=row["entity_kind"] or "",
                    actor=actor,
                    action=row["action"],
                    from_status=row["from_status"],
                    to_status=row["to_status"],
                    detail=json.loads(row["detail"]),
                    state_hash="",
                    created_at=row["created_at"],
                    migrated=True,
                    source_audit_id=int(row["id"]),
                )
                entity_head_updates[row["entity_id"]] = seq
                watermark = int(row["id"])
            for entity_id, seq in entity_head_updates.items():
                connection.execute(
                    "UPDATE entities SET head_seq = ?, head_hash = "
                    "(SELECT entry_hash FROM chain_records WHERE seq = ?) "
                    "WHERE id = ? AND (head_seq IS NULL OR head_seq < ?)",
                    (seq, seq, entity_id, seq),
                )
            connection.execute(
                "UPDATE chain_meta SET audit_watermark = ? WHERE id = 1",
                (watermark,),
            )

    def _backfill_loop(self):
        while True:
            try:
                _moved, done = self._backfill_once()
                if done:
                    return
            except Exception:
                # 与业务写入抢锁失败或临时库错误时，稍后重试，不吞掉服务。
                pass
            time.sleep(_BACKFILL_SLEEP)

    def wait_for_migration(self, timeout=30):
        """供测试与启动检查使用：阻塞等待旧记录回填完成。"""
        deadline_step = 0.05
        waited = 0.0
        while waited < timeout:
            status = self.chain_status()
            if status["migrated"]:
                return True
            try:
                self._backfill_once()
            except Exception:
                pass
            waited += deadline_step
            time.sleep(deadline_step)
        return self.chain_status()["migrated"]


class _BackfillActor:
    """回填节点不产生新的操作者语义，仅复用取号写入逻辑。"""

    def __init__(self, user_id, role):
        self.user_id = user_id
        self.role = role
