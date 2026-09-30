# 柿园采收批次账本

面向“西溪柿园采收”运营协作的批次账本服务：七千多棵火柿树的采收记录要同时供
加工摊、文创摊位和体验活动使用，现场还会发生分级拆分、合并、退回、冷藏预留与
月底盘点。服务以**只追加事件流**为唯一事实来源，用可重建的台账支持额度预留、
安全重放、争议冻结和期间结算。

## 记账模型

- **原始事实不可变**：称重（树区、品种、采收人、质量复核人、等级）建成原始批次；
  后续拆分、合并、退回、复核、盘点调整都只追加事件，绝不改写或删除历史。
- **批次流转**：拆分会从源批次转出、生成新批次；合并把多批货并入新批次；
  退回只核减在库，原始产量不变。数量恒等式：
  `在库 = 采收入库 + 转入 + 盘点调整 − 转出 − 出库 − 退回`，
  `可用 = 在库 − 已预留`。
- **下游领取两步走**：摊位先 `request_quota` 取得 held 额度（原子条件扣减，
  整单失败整体回滚），再 `confirm_outbound` 实际出库；未使用的额度可
  `release_quota`。并发领取不会超发。
- **安全重放**：称重、出库等消息携带 `message_id`；同号同体返回首次结果，
  同号异体拒绝（防止消息标识复用造成错账）。
- **争议冻结**：`freeze` 后的批次禁止一切数量变动（含已 held 预留的出库），
  `unfreeze` 解除。
- **盘点与责任**：盘点单按“期间结束时刻”重放事件取账面，差异=实盘−账面，
  记录责任人；处理决定 `adjust`（调账）/ `reject`（挂账）/ `freeze`
  （冻结争议批次）都关联决定人并落成事件。
- **期间结算快照**：按 YYYY-MM 重放期间结束前事件生成不可变快照，区分原始
  产量、转出转入、实际出库、退回和期末在库，用于解释“产量与实际出库为何不同”。
  跨月后录入次月事件或盘亏调整都不影响已出具的快照。
- **崩溃恢复**：held 预留随事务落库；重启后 `recover_pending` 列出未完成事务
  可继续出库，`sweep_expired` 自动释放过期预留。

所有时间戳来自可注入时钟（`FixedClock`），测试用固定时钟确定性复现跨月、
并发与恢复场景。仅依赖 Python 标准库，数据落本地 SQLite（文件库启用 WAL）。

## 目录

- `src/persimmon_ledger/domain.py`：时钟、错误类型、批次视图与数量口径。
- `src/persimmon_ledger/store.py`：SQLite 表结构（事件流/台账/预留/幂等/
  盘点/快照）、显式事务与线程安全的写串行化。
- `src/persimmon_ledger/service.py`：应用服务（称重、拆分、合并、退回、
  预留、出库、释放、冻结、盘点、追溯、结算、恢复）。
- `src/persimmon_ledger/api.py`：`{"action": ...}` JSON 适配层与统一错误码。
- `tests/test_ledger.py`：固定时钟下的跨月盘点、并发扣减、崩溃恢复等测试。

## 接口动作（api.handle）

| action | 说明 |
| --- | --- |
| `weigh_in` | 采收称重建档（lot_id/zone/variety/weight/harvester_id/quality_reviewer_id/quality_grade/message_id） |
| `quality_review` | 质量复核改级（只追加） |
| `split` / `merge` / `return` | 拆分 / 合并 / 退回，均校验可用额度 |
| `request_quota` | 取得 held 可用额度（items: [{lot_id, weight}]） |
| `confirm_outbound` / `release_quota` | 凭预留出库 / 释放预留 |
| `recover_pending` / `sweep_expired` | 恢复未完成事务 / 释放过期预留 |
| `freeze` / `unfreeze` | 冻结 / 解冻争议批次 |
| `create_stocktake` / `decide_stocktake` / `get_stocktake` | 盘点、差异定责与处理决定 |
| `trace` | 按批次追溯：台账、血缘、全部事件 |
| `settlement` / `get_settlement` | 生成期间结算快照 / 读取快照 |
| `get_lot` / `list_lots` | 批次查询 |

## 运行

```
PYTHONPATH=src python3 -m unittest discover -s tests
python3 -m compileall src
```

只使用 Python 标准库，测试和运行不需要启动其他服务。
