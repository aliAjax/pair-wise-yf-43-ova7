import json
import sqlite3
from datetime import datetime, timezone

from .chain import GENESIS_PREV_HASH, build_payload, canonical_json, hash_payload
from .domain import ConflictError, NotFoundError


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class SQLiteRepository:
    def __init__(self, path):
        self.path = str(path)
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        # 保证提交真正落盘：断电时事务要么整体存在，要么整体不存在。
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript("""
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
                CREATE TABLE IF NOT EXISTS chain_entries (
                    entity_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    audit_id INTEGER NOT NULL,
                    prev_hash TEXT NOT NULL,
                    hash TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (entity_id, seq)
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_chain_audit
                    ON chain_entries(audit_id);
            """)

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
        }

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

    @staticmethod
    def _chain_from_row(row):
        return {
            "entity_id": row["entity_id"],
            "seq": int(row["seq"]),
            "audit_id": row["audit_id"],
            "prev_hash": row["prev_hash"],
            "hash": row["hash"],
            "payload": json.loads(row["payload"]),
            "created_at": row["created_at"],
        }

    # ------------------------------------------------------------------
    # 链内部操作：调用方必须已持有写事务（BEGIN IMMEDIATE）。
    # ------------------------------------------------------------------

    def _chain_head(self, connection, entity_id):
        row = connection.execute(
            "SELECT seq, hash FROM chain_entries WHERE entity_id = ? "
            "ORDER BY seq DESC LIMIT 1",
            (entity_id,),
        ).fetchone()
        if not row:
            return 0, GENESIS_PREV_HASH
        return int(row["seq"]), row["hash"]

    def _insert_chain_entry(self, connection, entity_id, audit_row, extra=None):
        seq, prev_hash = self._chain_head(connection, entity_id)
        seq += 1
        payload = build_payload(entity_id, seq, prev_hash, audit_row, extra)
        digest = hash_payload(payload)
        connection.execute(
            "INSERT INTO chain_entries(entity_id, seq, audit_id, prev_hash, hash, payload, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (entity_id, seq, audit_row["id"], prev_hash, digest, canonical_json(payload), utcnow()),
        )
        return digest

    def _backfill_chain(self, connection, entity_id):
        """把尚未入链的审计记录按原始顺序补进链。迁移与正常写入共用此逻辑。"""
        rows = connection.execute(
            "SELECT a.* FROM audit_log a WHERE a.entity_id = ? "
            "AND NOT EXISTS (SELECT 1 FROM chain_entries c WHERE c.audit_id = a.id) "
            "ORDER BY a.id",
            (entity_id,),
        ).fetchall()
        for row in rows:
            self._insert_chain_entry(connection, entity_id, self._audit_from_row(row))

    # ------------------------------------------------------------------
    # 原子写入：实体状态、审计记录、链凭据在同一个事务里提交或回滚。
    # ------------------------------------------------------------------

    def apply_create(self, entity_id, kind, status, data, actor_id, actor_role, idem_key=None):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            if connection.execute(
                "SELECT 1 FROM entities WHERE id = ?", (entity_id,)
            ).fetchone():
                raise ConflictError("entity already exists: " + entity_id)
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                (entity_id, kind, status, payload, actor_id, now, now),
            )
            detail = {"kind": kind}
            cursor = connection.execute(
                "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entity_id,
                    actor_id,
                    actor_role,
                    "create",
                    None,
                    status,
                    json.dumps(detail, ensure_ascii=False, sort_keys=True),
                    now,
                ),
            )
            audit_row = {
                "id": cursor.lastrowid,
                "actor_id": actor_id,
                "actor_role": actor_role,
                "action": "create",
                "from_status": None,
                "to_status": status,
                "detail": detail,
                "created_at": now,
            }
            self._insert_chain_entry(connection, entity_id, audit_row)
            if idem_key:
                connection.execute(
                    "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                    "VALUES (?, ?, ?, ?)",
                    (actor_id, idem_key, entity_id, now),
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id)

    def apply_transition(
        self,
        entity_id,
        expected_version,
        new_status,
        data,
        actor_id,
        actor_role,
        action,
        detail,
        links=(),
    ):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version, status FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_version = int(row["version"])
            if expected_version is not None and current_version != int(expected_version):
                raise ConflictError(
                    "sequence conflict: expected version %s, found %s; "
                    "another action was confirmed first, please re-read and retry "
                    "(序号冲突，请重新读取最新状态后重试)" % (expected_version, current_version)
                )
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ? AND version = ?",
                (new_status, payload, now, entity_id, current_version),
            )
            # 老实体首次写入时，先把历史审计记录补成链，再追加本次动作。
            self._backfill_chain(connection, entity_id)
            link_snapshot = None
            if links:
                link_snapshot = {}
                for label, link_id in links:
                    self._backfill_chain(connection, link_id)
                    link_row = connection.execute(
                        "SELECT version, status, data FROM entities WHERE id = ?", (link_id,)
                    ).fetchone()
                    if link_row:
                        link_seq, link_head = self._chain_head(connection, link_id)
                        link_snapshot[label] = {
                            "id": link_id,
                            "version": int(link_row["version"]),
                            "status": link_row["status"],
                            "data": json.loads(link_row["data"]),
                            "chain_head": link_head if link_seq else None,
                        }
            cursor = connection.execute(
                "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entity_id,
                    actor_id,
                    actor_role,
                    action,
                    row["status"],
                    new_status,
                    json.dumps(detail, ensure_ascii=False, sort_keys=True),
                    now,
                ),
            )
            audit_row = {
                "id": cursor.lastrowid,
                "actor_id": actor_id,
                "actor_role": actor_role,
                "action": action,
                "from_status": row["status"],
                "to_status": new_status,
                "detail": detail,
                "created_at": now,
            }
            extra = {"links": link_snapshot} if link_snapshot else None
            self._insert_chain_entry(connection, entity_id, audit_row, extra)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id)

    # ------------------------------------------------------------------
    # 在线迁移：每个实体一个短事务，迁移期间不阻塞收样等新写入。
    # ------------------------------------------------------------------

    def pending_chain_entities(self):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT DISTINCT entity_id FROM audit_log a "
                "WHERE NOT EXISTS (SELECT 1 FROM chain_entries c WHERE c.audit_id = a.id) "
                "ORDER BY entity_id"
            ).fetchall()
        return [row["entity_id"] for row in rows]

    def migrate_chains(self):
        migrated = []
        for entity_id in self.pending_chain_entities():
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._backfill_chain(connection, entity_id)
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()
            migrated.append(entity_id)
        return migrated

    # ------------------------------------------------------------------
    # 查询
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
                    "SELECT * FROM audit_log WHERE entity_id = ? ORDER BY id", (entity_id,)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
        return [self._audit_from_row(row) for row in rows]

    def list_chain(self, entity_id=None):
        with self._connect() as connection:
            if entity_id:
                rows = connection.execute(
                    "SELECT * FROM chain_entries WHERE entity_id = ? ORDER BY seq",
                    (entity_id,),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM chain_entries ORDER BY entity_id, seq"
                ).fetchall()
        return [self._chain_from_row(row) for row in rows]

    def chain_hashes(self, entity_id):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT hash FROM chain_entries WHERE entity_id = ?", (entity_id,)
            ).fetchall()
        return {row["hash"] for row in rows}

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
