# 柿园采收批次账本

面向“西溪柿园采收”运营协作的批次账本服务：服务把**只追加的事实事件**、批次当前
余额投影、下游额度预占和盘点决定保存到本地 SQLite，应用层通过进程内 JSON 适配器
调用。服务支持七千多棵树在成熟季里的采收称重、分拣拆分/合并、摊位领取、体验
退回、冷藏预留和月底结算。

## 记账原则

- **原始事实不改写**：采收、拆分、合并、退回、预占、出库、释放、冻结、盘点调账
  都写成不可修改的事件（`events` 表）。纠正错误只能追加新事件（退回/调账）。
- **额度先行**：加工摊、文创摊、体验活动、冷藏库必须先 `reserve` 取得可用额度，
  再 `confirm_outbound` 出库；未用完的额度可 `release`，过期预占由
  `recover_pending` 回收。
- **消息安全重放**：每个写动作接受 `idempotency_key`，重复的称重或出库消息返回
  第一次的结果，绝不重复记账。
- **时间可注入**：`Service(store, clock)` 接受时钟；测试和演练使用
  `domain.FixedClock` 确定性地推进时间。

## 目录

- `src/persimmon_ledger/domain.py`：批次/事件对象、状态与事件常量、时钟。
- `src/persimmon_ledger/store.py`：SQLite 表结构、只追加事件流、投影与事务。
- `src/persimmon_ledger/service.py`：全部账本用例（登记/拆分/合并/退回/冻结/
  预占/出库/释放/恢复/盘点/追溯/结算快照）。
- `src/persimmon_ledger/api.py`：JSON 请求适配，动作名即服务方法。
- `tests/`：基线行为与固定时钟下的跨月、并发、恢复测试。

## 主要接口（JSON 动作）

| 动作 | 说明 |
| --- | --- |
| `harvest` | 采收称重登记：树区、品种、采收人、质量复核人、等级、重量 |
| `split` | 一批货拆出子批次（可改分拣等级），只追加事实 |
| `merge` | 多个同品种批次（全部或部分）合并为新批次 |
| `return` | 下游退回，作为新事件重新入帐，旧出库事实不变 |
| `freeze` / `unfreeze` | 冻结/解冻争议批次，冻结期间禁止一切额度变动 |
| `reserve` | 下游领取前预占额度（带 TTL），可用额度移入预占 |
| `confirm_outbound` | 凭预占确认出库 |
| `release` | 手动释放未使用预占 |
| `recover_pending` | 服务重启后恢复：回收过期仍占用的额度 |
| `stocktake` | 盘点：实盘数、差异、责任人、处理决定（adjust/freeze/waive） |
| `trace` | 按批次追溯：现状、完整事实链、预占与盘点记录 |
| `settlement_snapshot` | 生成半开区间 `[period_start, period_end)` 的结算快照 |

写动作可带 `idempotency_key`；规则冲突返回 `{"error": ...}`，不抛异常，消息
队列可安全重试。

## 期间结算如何解释“产量与实际出库不同”

`settlement_snapshot` 通过**回放事件流**（不依赖当前投影）给出每个批次的：
期初可用/预占、期间采收、拆入拆出、并入并出、退回、预占总额、实际出库、释放、
盘点调账、期末可用/预占/在园总量。恒等式：

```
期末在园 = 期初在园 + 采收 + 拆入净额 + 并入净额 + 退回 - 出库 + 调账
```

## 运行

运行测试：`PYTHONPATH=src python3 -m unittest discover -s tests`

检查源码：`python3 -m compileall src`

项目只使用 Python 标准库，测试和运行不需要启动其他服务。
