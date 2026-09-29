class AuditTrail:
    """实体操作审计时间线的只读视图。

    写入不再经过本类：状态、审计记录与校验链节点在 repository 的同一事务
    中一起提交，避免“审计留下了、状态没留下”的半截写入。
    """

    def __init__(self, repository):
        self.repository = repository

    def list(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)
