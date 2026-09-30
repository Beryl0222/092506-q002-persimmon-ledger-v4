"""采收批次与出库额度的基础领域对象。

账本采用“只追加事件 + 当前投影”的方式保存：任何采收、拆分、合并、
退回、出库和盘点动作都以事件形式永久留档（不改写原始事实），批次当前
余额由投影表维护。时间统一由可注入的时钟给出，测试时使用固定时钟。
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Callable

# 批次状态
STATE_ACTIVE = "active"        # 正常在账
STATE_FROZEN = "frozen"        # 争议冻结，禁止任何变动
STATE_CLOSED = "closed"        # 余额归零，流程结束

# 事件类型：每条事件是不可改写的原始事实
EV_HARVESTED = "harvested"                 # 采收登记（称重）
EV_SPLIT = "split"                         # 从父批次拆出
EV_SPLIT_CHILD = "split_child"             # 子批次拆入
EV_MERGE_SOURCE = "merge_source"           # 来源批次被并出
EV_MERGED_IN = "merged_in"                 # 目标批次并入
EV_RETURNED = "returned"                   # 下游退回
EV_RESERVED = "reserved"                   # 额度预占
EV_CONFIRMED = "confirmed"                 # 预占确认出库
EV_RELEASED = "released"                   # 预占释放
EV_RETURN_HOLD = "return_hold"             # 退回在途预占
EV_DISPATCHED = "dispatched"               # 无预占直接出库（兼容）
EV_FROZEN = "frozen"                       # 冻结
EV_UNFROZEN = "unfrozen"                   # 解冻
EV_STOCK_ADJUST = "stock_adjust"           # 盘点调账
EV_SNAPSHOT = "snapshot"                   # 期间结算快照标记


class LedgerError(Exception):
    """账本规则冲突（余额不足、状态非法等）。"""


class ConcurrencyError(LedgerError):
    """并发提交冲突，调用方可用同一幂等键安全重试。"""


@dataclass(frozen=True)
class Record:
    """旧版登记记录，保留以兼容既有调用与基线测试。"""
    record_id: str
    owner_id: str
    state: str = "draft"
    created_at: str = ""

    def with_timestamp(self, now: str = "") -> "Record":
        return Record(self.record_id, self.owner_id, self.state,
                      self.created_at or now or Clock().now_iso())


@dataclass(frozen=True)
class Lot:
    """一个采收批次（或拆分/合并产生的新批次）。"""
    lot_id: str
    zone: str                         # 树区
    variety: str                      # 品种
    harvester_id: str                 # 采收人
    quality_checker_id: str           # 质量复核人
    quality_grade: str                # 分拣等级
    quantity: float                   # 初始重量（千克）
    state: str = STATE_ACTIVE
    balance: float = 0.0              # 可用余额（可领取额度）
    held: float = 0.0                 # 已预占、尚未出库的重量
    parent_lot_id: str = ""           # 拆分来源批次
    created_event_id: int = 0
    created_at: str = ""
    frozen_reason: str = ""


@dataclass(frozen=True)
class Event:
    """追加到账本上的一条事实。payload 为结构化明细（JSON 字符串）。"""
    seq: int
    event_type: str
    lot_id: str
    quantity: float
    actor_id: str
    occurred_at: str
    payload: dict[str, Any] = field(default_factory=dict)
    idempotency_key: str = ""
    event_id: int = 0


class Clock:
    """时间来源。生产使用系统 UTC 时钟，测试替换为 FixedClock。"""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    def now_iso(self) -> str:
        return self.now().isoformat()


class FixedClock(Clock):
    """固定时钟：手动设定/推进当前时间，用于跨月盘点等确定性测试。"""

    def __init__(self, start: datetime | str) -> None:
        if isinstance(start, str):
            start = datetime.fromisoformat(start)
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        self._now = start.astimezone(timezone.utc)

    def now(self) -> datetime:
        return self._now

    def advance(self, **kwargs: Any) -> None:
        self._now = self._now + timedelta(**kwargs)

    def set(self, value: datetime | str) -> None:
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        self._now = value.astimezone(timezone.utc)
