class AuditTrail:
    """审计时间线的读取入口；写入已并入 repository 的原子事务。"""

    def __init__(self, repository):
        self.repository = repository

    def list(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)
