from uuid import uuid4

from .audit import AuditTrail
from .domain import NotFoundError
from .rules import RuleEngine


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        self.rules.validate_create(actor, kind, payload, self._lookup)
        entity_id = str(payload.pop("id", "") or uuid4())
        # 实体、链节点、审计、幂等凭据在同一事务里落地：要么都留下，要么都不留。
        entity, _replayed = self.repository.create_entity_atomic(
            entity_id, kind, self.rules.initial_status(kind), payload,
            actor, idempotency_key,
        )
        return entity

    def transition(
        self,
        actor,
        entity_id,
        action,
        data=None,
        expected_version=None,
        expected_head_seq=None,
    ):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        expected_head = (
            int(expected_head_seq)
            if expected_head_seq is not None
            else entity["head_seq"]
        )
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        detail = {"patch": patch}
        evidence = self._evidence(actor, entity, action, dict(data or {}))
        if evidence:
            detail["evidence"] = evidence
        updated = self.repository.transition_entity_atomic(
            entity_id=entity_id,
            expected_version=expected,
            expected_head_seq=expected_head,
            status=next_status,
            data=merged,
            actor=actor,
            action=action,
            from_status=entity["status"],
            detail=detail,
        )
        return updated

    def _evidence(self, actor, entity, action, data):
        """设备校准与方法授权的最新结论，随动作一起进入同一条校验链。"""
        if entity["kind"] != "result" or action != "release":
            return None
        instrument = self._first("instrument", "id", data.get("instrument_id"))
        method = self._first("method", "id", data.get("method_id"))
        calibration = self._latest_calibration(data.get("instrument_id"))
        evidence = {}
        if instrument:
            evidence["instrument"] = {
                "id": instrument["id"],
                "status": instrument["status"],
                "calibration_due_at": instrument["data"].get("due_at"),
                "head_seq": instrument["head_seq"],
                "head_hash": instrument["head_hash"],
            }
        if calibration:
            evidence["latest_calibration"] = {
                "id": calibration["id"],
                "status": calibration["status"],
                "due_at": calibration["data"].get("due_at"),
                "authorized_by": calibration["data"].get("authorized_by"),
                "head_seq": calibration["head_seq"],
                "head_hash": calibration["head_hash"],
            }
        if method:
            evidence["method"] = {
                "id": method["id"],
                "status": method["status"],
                "version": method["data"].get("version"),
                "head_seq": method["head_seq"],
                "head_hash": method["head_hash"],
            }
        return evidence or None

    def _first(self, kind, field, value):
        rows = self._lookup(kind, field, value) or []
        return rows[0] if rows else None

    def _latest_calibration(self, instrument_id):
        calibrations = self._lookup("calibration", "instrument_id", instrument_id) or []
        approved = [item for item in calibrations if item["status"] == "approved"]
        pool = approved or calibrations
        return sorted(pool, key=lambda item: item["head_seq"] or 0)[-1] if pool else None

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)

    def chain(self, entity_id=None, limit=None):
        return self.repository.list_chain(entity_id=entity_id, limit=limit)

    def chain_status(self):
        return self.repository.chain_status()

    def verify_chain(self):
        return self.repository.verify_chain()

    def wait_for_migration(self, timeout=30):
        return self.repository.wait_for_migration(timeout=timeout)
