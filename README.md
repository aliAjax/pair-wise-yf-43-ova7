# 实验室仪器校准与方法验证

这是一个只使用Python标准库和SQLite的模块化项目，默认端口为`8309`。所有业务规则集中在`src/rules.py`，`app.py`只负责组装依赖和启动服务。

## 模块结构

- `app.py`：命令行参数、依赖组装、启动和信号处理。
- `src/domain.py`：角色、数据结构、领域异常和基础校验。
- `src/rules.py`：状态机、权限、领域计算、冲突和跨对象校验。
- `src/repository.py`：SQLite建表、查询、事务和乐观锁；实体状态、审计记录、链凭据在同一事务提交。
- `src/service.py`：用例编排、幂等处理、版本控制和链上证据组装。
- `src/http_api.py`：HTTP路由、请求解析和统一错误响应。
- `src/audit.py`：实体操作审计时间线读取。
- `src/chain.py`：哈希校验链的构建与验证。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、规则、失败场景和校验链测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8309
```

服务启动时会自动建表。`--host`可修改监听地址，`--db`可指定其他SQLite文件。

## 核心对象

- `instrument`：仪器状态；`calibration`：校准记录；`method`：方法版本；`result`：检测结果。

## 主要接口

- `GET /health`：健康检查。
- `GET /api/<kind>`：按对象类型查询，可用`?status=`过滤。
- `POST /api/<kind>`：创建对象；请求体为JSON。
- `GET /api/entities/<id>`：读取对象当前版本。
- `POST /api/entities/<id>/actions`：提交`{"action":"动作名","data":{...},"expected_version":数字}`。
- `GET /api/entities/<id>/chain`：读取该对象的校验链。
- `GET /api/entities/<id>/chain/verify`：重算并校验该对象的整条链。
- `GET /api/audit`：读取审计记录。

## 校验链

- 每个对象从创建起的每次变化都生成一条链上凭据：`seq`从1连续递增，`prev_hash`指向前一条的SHA-256，内容含操作者、动作、前后状态、明细和时间。任何一条被改、被删或被插入，验证时都会在断开处报出。
- 结果签发时，设备校准与方法授权的最新结论（状态、版本、数据快照及各自链头哈希）作为`links`写进结果的同一条链，评审员可核对签发所依据的结论未被事后改动。
- 两个岗位同时处理同一对象时，先确认的动作占住下一序号；后到者因`expected_version`冲突被拒绝（409），提示重新读取后重试，链不分叉。`chain_entries`的`(entity_id, seq)`主键与`audit_id`唯一索引是数据库层的兜底。
- 保存时实体状态、审计记录、链凭据在同一事务提交：断电或数据库错误时要么都留下，要么都回滚。
- 历史数据迁移：`python3 app.py --db ./data.db --migrate-chain`。按审计记录原始顺序补成历史链，每个实体一个短事务，迁移期间收样等新写入不受影响；尚未迁移的实体在下次写入时会在同一事务内先补链再追加，不会分叉。原有`GET /api/audit`等查询方式不受影响。

请求身份通过`X-User-Id`和`X-Role`请求头传入。创建和动作的可执行角色由规则引擎控制。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

校准周期、误差和放行规则是可演示的业务模型，不替代实验室质量体系或计量认证。校验链能发现库内篡改，但若攻击者掌握写库权限并重算全部后续哈希，则需要把链头哈希定期导出到库外（如签发到第三方）才能检测，本演示未包含该导出。
