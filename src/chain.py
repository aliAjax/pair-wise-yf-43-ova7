"""连续校验链的哈希规则。

链上每个节点都携带全局连续序号 ``seq``、上一节点的 ``entry_hash`` 和
本节点的 ``entry_hash``。任何对历史节点的增、删、改都会让哈希在断口处
对不上；节点同时快照实体当时的 ``state_hash``，因此只改实体状态而不走
链也能被发现。
"""

import hashlib
import json

# 创世前缀：第一条节点的 prev_hash 固定指向它。
GENESIS_HASH = "0" * 64


def canonical_json(value):
    """所有哈希共用的规范化 JSON，保证回填与实时写入算法一致。"""
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def digest(value):
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def compute_state_hash(status, version, data):
    """实体某一版完整状态的指纹。"""
    return digest({"status": status, "version": version, "data": data})


def compute_entry_hash(
    *,
    seq,
    entity_id,
    kind,
    action,
    from_status,
    to_status,
    actor_id,
    actor_role,
    detail,
    state_hash,
    created_at,
    prev_hash,
):
    payload = {
        "seq": seq,
        "entity_id": entity_id,
        "kind": kind,
        "action": action,
        "from_status": from_status,
        "to_status": to_status,
        "actor_id": actor_id,
        "actor_role": actor_role,
        "detail": detail,
        "state_hash": state_hash,
        "created_at": created_at,
        "prev_hash": prev_hash,
    }
    return digest(payload)
