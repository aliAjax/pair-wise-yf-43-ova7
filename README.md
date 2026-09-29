# 实验室仪器校准与方法验证

这是一个只使用Python标准库和SQLite的模块化项目，默认端口为`8309`。所有业务规则集中在`src/rules.py`，`app.py`只负责组装依赖和启动服务。

## 模块结构

- `app.py`：命令行参数、依赖组装、启动和信号处理。
- `src/domain.py`：角色、数据结构、领域异常和基础校验。
- `src/rules.py`：状态机、权限、领域计算、冲突和跨对象校验。
- `src/repository.py`：SQLite建表、查询、事务和乐观锁。
- `src/service.py`：用例编排、幂等处理、版本控制与证据快照。
- `src/http_api.py`：HTTP路由、请求解析和统一错误响应。
- `src/audit.py`：实体操作审计时间线（只读视图）。
- `src/chain.py`：连续校验链的规范化哈希规则。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、规则、失败场景与校验链测试。

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
- `POST /api/<kind>`：创建对象；请求体为JSON，支持`Idempotency-Key`头。
- `GET /api/entities/<id>`：读取对象当前版本，响应额外带`head_seq`与`head_hash`锚点。
- `POST /api/entities/<id>/actions`：提交`{"action":"动作名","data":{...},"expected_version":数字,"expected_head_seq":数字}`。
- `GET /api/audit`：读取审计记录（原查询方式保持不变）。
- `GET /api/chain`：读取连续校验链（`?limit=`可选）。
- `GET /api/entities/<id>/chain`：读取单个对象的链段。
- `GET /api/chain/verify`：从链头核验全链，返回`{ok,checked,break_at,reason}`；断链时HTTP 422。
- `GET /api/chain/status`：链头位置与旧记录迁移进度。

请求身份通过`X-User-Id`和`X-Role`请求头传入。创建和动作的可执行角色由规则引擎控制。

## 连续校验链

- 每次状态变化（收样、放行、签发、校准结论、方法授权等）都追加一个全局连续节点`chain_records`：`seq`为全局单调序号，`prev_hash`指向上一节点，`entry_hash`覆盖节点全部字段并附带实体当时的`state_hash`。创世节点的`prev_hash`为64个`0`。
- 结果签发（`release`）节点的`detail.evidence`同时快照当时的设备状态、最新已批准校准结论（含授权人）与方法授权状态，连同其链锚点一起进入哈希，事后无法单独替换。
- 序号由写事务在`BEGIN IMMEDIATE`锁内读取并立即占用。两个岗位几乎同时处理同一份结果时，先确认的动作占住下一位置；后到者的`expected_version`或`expected_head_seq`对不上，返回HTTP 409并提示“请重新读取后再提交”，链上不会出现分叉。
- 实体状态、链节点、审计记录、幂等凭据在**同一个SQLite事务**提交：断电或数据库错误时要么全部留下，要么全部不留。
- 旧库的历史`audit_log`在服务启动后由后台线程按原`id`顺序小批回填成历史链（标记`migrated=true`）；迁移期间收样不中断，新节点与回填节点共用同一条链。原有`/api/audit`等查询方式保持不变。可用`GET /api/chain/verify`定位链条断开的具体位置。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

校准周期、误差和放行规则是可演示的业务模型，不替代实验室质量体系或计量认证。
