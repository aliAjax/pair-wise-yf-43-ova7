import hashlib
import json

GENESIS_PREV_HASH = "0" * 64


def canonical_json(payload):
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def hash_payload(payload):
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def build_payload(entity_id, seq, prev_hash, audit_row, extra=None):
    payload = {
        "entity_id": entity_id,
        "seq": int(seq),
        "prev_hash": prev_hash,
        "audit_id": audit_row["id"],
        "actor_id": audit_row["actor_id"],
        "actor_role": audit_row["actor_role"],
        "action": audit_row["action"],
        "from_status": audit_row["from_status"],
        "to_status": audit_row["to_status"],
        "detail": audit_row["detail"],
        "created_at": audit_row["created_at"],
    }
    if extra:
        payload.update(extra)
    return payload


def verify_entries(entity_id, entries, audit_by_id, linked_hashes):
    """Recompute a chain and report every break. entries must be ordered by seq."""
    breaks = []
    expected_seq = 1
    prev_hash = GENESIS_PREV_HASH
    for entry in entries:
        seq = entry["seq"]
        if seq != expected_seq:
            breaks.append(
                {"seq": seq, "reason": "sequence break: expected position %d" % expected_seq}
            )
            expected_seq = seq
        if entry["prev_hash"] != prev_hash:
            breaks.append({"seq": seq, "reason": "prev_hash mismatch: chain is broken here"})
        payload = entry["payload"]
        if (
            payload.get("entity_id") != entity_id
            or payload.get("seq") != seq
            or payload.get("prev_hash") != entry["prev_hash"]
        ):
            breaks.append({"seq": seq, "reason": "payload header inconsistent with stored entry"})
        if hash_payload(payload) != entry["hash"]:
            breaks.append({"seq": seq, "reason": "hash mismatch: entry content was altered"})
        audit = audit_by_id.get(entry["audit_id"])
        if audit is None:
            breaks.append({"seq": seq, "reason": "referenced audit record is missing"})
        else:
            for field in ("actor_id", "actor_role", "action", "from_status", "to_status", "created_at"):
                if payload.get(field) != audit.get(field):
                    breaks.append({"seq": seq, "reason": "audit field %s was altered" % field})
            if payload.get("detail") != audit.get("detail"):
                breaks.append({"seq": seq, "reason": "audit detail was altered"})
        for label, link in (payload.get("links") or {}).items():
            head = link.get("chain_head")
            if head and head not in linked_hashes(link["id"]):
                breaks.append(
                    {"seq": seq, "reason": "linked %s conclusion not found in its own chain" % label}
                )
        prev_hash = entry["hash"]
        expected_seq += 1
    return {
        "entity_id": entity_id,
        "ok": not breaks,
        "length": len(entries),
        "head": entries[-1]["hash"] if entries else None,
        "breaks": breaks,
    }
