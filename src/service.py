from uuid import uuid4

from .audit import AuditTrail
from .chain import verify_entries
from .domain import NotFoundError
from .rules import RuleEngine

# 结果签发时，把设备校准与方法授权的最新结论一并写进同一条链。
RELEASE_LINKS = ("instrument_id", "method_id")


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
        status = self.rules.initial_status(kind)
        return self.repository.apply_create(
            entity_id, kind, status, payload, actor.user_id, actor.role, idempotency_key
        )

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        links = ()
        if entity["kind"] == "result" and action == "release":
            links = tuple(
                (field.removesuffix("_id"), patch[field])
                for field in RELEASE_LINKS
                if patch.get(field)
            )
        return self.repository.apply_transition(
            entity_id,
            expected,
            next_status,
            merged,
            actor.user_id,
            actor.role,
            action,
            {"patch": patch},
            links,
        )

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

    def chain(self, entity_id):
        self.get(entity_id)
        return self.repository.list_chain(entity_id)

    def verify_chain(self, entity_id):
        self.get(entity_id)
        entries = self.repository.list_chain(entity_id)
        audit_by_id = {row["id"]: row for row in self.repository.list_audit(entity_id)}
        return verify_entries(entity_id, entries, audit_by_id, self.repository.chain_hashes)

    def migrate_chains(self):
        migrated = self.repository.migrate_chains()
        return {"migrated": len(migrated), "entity_ids": migrated}
