"""采收批次与出库额度的基础领域对象。

账本以“只追加事件”为唯一事实来源：称重、拆分、合并、退回、预留、出库、
冻结、盘点调整全部记录为不可变事件；批次台账等表都是可由事件重放重建的
派生状态。所有时间戳取自可替换的时钟，以便用固定时钟复现跨月场景。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


# 重量以千克(kg)计，统一保留三位小数（克级），避免浮点尾差影响扣减判断。
WEIGHT_SCALE = 3


def qweight(value: float) -> float:
    """把入参重量规整到克级。"""
    return round(float(value), WEIGHT_SCALE)


class Clock:
    """时钟接口：服务只依赖 now()，测试可替换为固定时钟。"""

    def now(self) -> datetime:
        raise NotImplementedError


class SystemClock(Clock):
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class FixedClock(Clock):
    """固定在某个时刻的时钟，可手动推进，用于跨月/并发的确定性测试。"""

    def __init__(self, start: datetime | str = "2026-09-30T08:00:00+00:00") -> None:
        if isinstance(start, str):
            start = datetime.fromisoformat(start)
        self._now = start

    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float = 0, **kwargs: Any) -> datetime:
        if seconds:
            kwargs["seconds"] = kwargs.get("seconds", 0) + seconds
        self._now = self._now + timedelta(**kwargs)
        return self._now


def period_of(moment: datetime) -> str:
    """返回结算期间键，格式 YYYY-MM。"""
    return moment.strftime("%Y-%m")


def period_bounds(period: str) -> tuple[datetime, datetime]:
    """返回期间 [起, 止) 两个 UTC 时刻（止为次月一号零点）。"""
    year, month = (int(x) for x in period.split("-"))
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return start, end


class LedgerError(Exception):
    """账本业务错误基类。"""

    code = "ledger_error"

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class NotFound(LedgerError):
    code = "not_found"


class InvalidOperation(LedgerError):
    code = "invalid_operation"


class Conflict(LedgerError):
    """同一条消息被重放但请求体与首次不同，或唯一约束冲突。"""

    code = "conflict"


class InsufficientQuota(LedgerError):
    """可用额度不足（含并发扣减失败）。"""

    code = "insufficient_quota"


class FrozenLot(LedgerError):
    """争议批次已被冻结，禁止变动。"""

    code = "frozen_lot"


@dataclass(frozen=True)
class Record:
    """兼容早期登记入口的简单记录。"""

    record_id: str
    owner_id: str
    state: str = "draft"
    created_at: str = ""

    def with_timestamp(self, clock: Clock | None = None) -> "Record":
        if self.created_at:
            return self
        clock = clock or SystemClock()
        return Record(self.record_id, self.owner_id, self.state,
                      clock.now().isoformat())


@dataclass(frozen=True)
class Event:
    seq: int
    event_id: str
    event_type: str
    lot_id: str
    occurred_at: str
    payload: dict[str, Any]


# 记账时各类事件对应的数量字段。
# stock = inbound + transfer_in + adjustment - outbound - returned - transfer_out
# available = stock - reserved
LOT_QUANTITY_FIELDS = (
    "inbound_weight", "transfer_in_weight", "outbound_weight",
    "returned_weight", "transfer_out_weight", "adjustment_weight",
    "reserved_weight",
)


def empty_quantities() -> dict[str, float]:
    return {name: 0.0 for name in LOT_QUANTITY_FIELDS}


@dataclass
class LotView:
    """批次台账的对外视图。"""

    lot_id: str
    zone: str
    variety: str
    harvester_id: str
    quality_reviewer_id: str
    quality_grade: str | None
    inbound_weight: float = 0.0
    transfer_in_weight: float = 0.0
    transfer_out_weight: float = 0.0
    outbound_weight: float = 0.0
    returned_weight: float = 0.0
    adjustment_weight: float = 0.0
    reserved_weight: float = 0.0
    frozen: bool = False
    created_at: str = ""
    updated_at: str = ""

    @property
    def stock_weight(self) -> float:
        """账面在库（含已预留但尚未出库的部分）。"""
        return qweight(
            self.inbound_weight + self.transfer_in_weight
            + self.adjustment_weight - self.outbound_weight
            - self.returned_weight - self.transfer_out_weight
        )

    @property
    def available_weight(self) -> float:
        """可被下游领取/拆分/退回动用的额度。"""
        return qweight(self.stock_weight - self.reserved_weight)

    def to_dict(self) -> dict[str, Any]:
        data = {
            "lot_id": self.lot_id,
            "zone": self.zone,
            "variety": self.variety,
            "harvester_id": self.harvester_id,
            "quality_reviewer_id": self.quality_reviewer_id,
            "quality_grade": self.quality_grade,
            "inbound_weight": qweight(self.inbound_weight),
            "transfer_in_weight": qweight(self.transfer_in_weight),
            "transfer_out_weight": qweight(self.transfer_out_weight),
            "outbound_weight": qweight(self.outbound_weight),
            "returned_weight": qweight(self.returned_weight),
            "adjustment_weight": qweight(self.adjustment_weight),
            "reserved_weight": qweight(self.reserved_weight),
            "stock_weight": self.stock_weight,
            "available_weight": self.available_weight,
            "frozen": self.frozen,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
        return data
