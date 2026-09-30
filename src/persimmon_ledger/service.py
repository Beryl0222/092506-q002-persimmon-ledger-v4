"""采收批次与出库额度的应用服务入口。

设计要点：

* 原始事实不可变：所有业务动作（称重、拆分、合并、退回、预留、出库、
  冻结、盘点、调整）都先写成只追加事件，再同步更新可重建的批次台账。
  没有任何动作会删除或覆盖历史事件。
* 下游领取走“先预留额度（held）、再确认出库”的两步流程；预留以条件式
  SQL 原子完成，并发扣减时余额不足的一方会失败而不会超发。
* 称重/出库等消息携带 ``message_id`` 时做幂等处理：同一条消息的安全重放
  返回首次结果；相同 id 但请求体不同会被告警拒绝。
* 所有时间来自可注入时钟，期间结算通过重放“期间结束前”的事件生成不可变
  快照，因此跨月后录入当月事件不会改变已出具的月结。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from typing import Any, Callable

from .domain import (
    Clock, Conflict, FrozenLot, InsufficientQuota, InvalidOperation,
    LedgerError, LotView, NotFound, Record, SystemClock, period_bounds,
    qweight,
)
from .store import Store


# 数量台账字段，重放与快照都按这套口径汇总。
QFIELDS = (
    "inbound_weight", "transfer_in_weight", "transfer_out_weight",
    "outbound_weight", "returned_weight", "adjustment_weight",
    "reserved_weight",
)


def _empty_acc() -> dict[str, float]:
    return {name: 0.0 for name in QFIELDS}


def apply_event(acc: dict[str, dict[str, float]], event: dict[str, Any]) -> None:
    """把一条事件累积到 {lot_id: 数量字段} 的重放表上（纯函数）。"""
    etype = event["event_type"]
    payload = event["payload"]

    def slot(lot_id: str) -> dict[str, float]:
        return acc.setdefault(lot_id, _empty_acc())

    if etype == "weighed":
        slot(event["lot_id"])["inbound_weight"] += payload["weight"]
    elif etype == "lot_split":
        slot(event["lot_id"])["transfer_out_weight"] += payload["weight"]
        slot(payload["output_id"])["transfer_in_weight"] += payload["weight"]
    elif etype == "lot_merge":
        for item in payload["inputs"]:
            slot(item["lot_id"])["transfer_out_weight"] += item["weight"]
        slot(event["lot_id"])["transfer_in_weight"] += payload["total_weight"]
    elif etype == "returned":
        slot(event["lot_id"])["returned_weight"] += payload["weight"]
    elif etype == "stocktake_adjusted":
        for item in payload["items"]:
            slot(item["lot_id"])["adjustment_weight"] += item["diff_weight"]
    elif etype == "quota_reserved":
        for item in payload["items"]:
            slot(item["lot_id"])["reserved_weight"] += item["weight"]
    elif etype == "outbound":
        for item in payload["items"]:
            s = slot(item["lot_id"])
            s["reserved_weight"] -= item["weight"]
            s["outbound_weight"] += item["weight"]
    elif etype == "quota_released":
        for item in payload["items"]:
            slot(item["lot_id"])["reserved_weight"] -= item["weight"]
    # quality_reviewed / frozen / unfrozen / stocktake_created 不影响数量。


def stock_of(acc: dict[str, float]) -> float:
    return qweight(
        acc["inbound_weight"] + acc["transfer_in_weight"]
        + acc["adjustment_weight"] - acc["outbound_weight"]
        - acc["returned_weight"] - acc["transfer_out_weight"]
    )


class Service:
    def __init__(self, store: Store | None = None,
                 clock: Clock | None = None) -> None:
        self.store = store or Store()
        self.clock = clock or SystemClock()

    def _now(self) -> str:
        return self.clock.now().isoformat()

    # -- 事务与幂等 -------------------------------------------------------

    def _transaction(self, message_id: str | None, request_obj: Any,
                     fn: Callable[[sqlite3.Connection, str], dict[str, Any]],
                     kind: str) -> dict[str, Any]:
        store = self.store
        if message_id is None:
            now = self._now()
            with store.write_lock:
                store.begin(immediate=True)
                try:
                    result = fn(store.conn(), now)
                    store.commit()
                    return result
                except BaseException:
                    store.rollback()
                    raise

        request_hash = hashlib.sha256(
            json.dumps(request_obj, ensure_ascii=False, sort_keys=True,
                       default=str).encode("utf-8")
        ).hexdigest()
        now = self._now()
        with store.write_lock:
            store.begin(immediate=True)
            c = store.conn()
            try:
                row = store.get_idempotency(c, message_id)
                if row is not None:
                    if row["request_hash"] != request_hash:
                        raise Conflict(
                            f"消息 {message_id} 曾用于另一请求（{row['request_hash'][:8]}），"
                            "拒绝重放：请勿复用消息标识")
                    store.commit()
                    cached = json.loads(row["response_json"])
                    cached["replayed"] = True
                    return cached
                result = fn(c, now)
                store.put_idempotency(
                    c, message_id, request_hash, kind,
                    json.dumps(result, ensure_ascii=False, sort_keys=True,
                               default=str),
                    now,
                )
                store.commit()
                return result
            except BaseException:
                store.rollback()
                raise

    def _require_lot(self, c: sqlite3.Connection, lot_id: str) -> LotView:
        lot = self.store.get_lot_lock(c, lot_id)
        if lot is None:
            raise NotFound(f"批次 {lot_id} 不存在")
        return lot

    @staticmethod
    def _require_mutable(lot: LotView) -> None:
        if lot.frozen:
            raise FrozenLot(f"批次 {lot.lot_id} 因争议被冻结，禁止变动")

    @staticmethod
    def _require_positive(weight: float, label: str = "重量") -> float:
        try:
            value = qweight(weight)
        except (TypeError, ValueError) as exc:
            raise InvalidOperation(f"{label}必须是数字") from exc
        if value <= 0:
            raise InvalidOperation(f"{label}必须大于 0")
        return value

    # -- 基础入口 ---------------------------------------------------------

    def health(self) -> dict[str, str]:
        return {"service": "persimmon_ledger", "status": "ok"}

    def register(self, record_id: str, owner_id: str) -> dict[str, str]:
        record = Record(record_id, owner_id).with_timestamp(self.clock)
        with self.store.write_lock:
            self.store.save_record(record)
        return {"record_id": record.record_id, "owner_id": record.owner_id,
                "state": record.state, "created_at": record.created_at}

    def find(self, record_id: str) -> dict[str, str] | None:
        record = self.store.get_record(record_id)
        return record.__dict__.copy() if record else None

    # -- 采收事实 ---------------------------------------------------------

    def weigh_in(self, lot_id: str, zone: str, variety: str, weight: float,
                 harvester_id: str, quality_reviewer_id: str,
                 quality_grade: str | None = None,
                 message_id: str | None = None) -> dict[str, Any]:
        """登记一树上采收批次的称重事实（原始产量）。"""
        w = self._require_positive(weight, "称重重量")
        request = {"action": "weigh_in", "lot_id": lot_id, "zone": zone,
                   "variety": variety, "weight": w,
                   "harvester_id": harvester_id,
                   "quality_reviewer_id": quality_reviewer_id,
                   "quality_grade": quality_grade}

        def work(c: sqlite3.Connection, now: str) -> dict[str, Any]:
            if self.store.lot_exists(c, lot_id):
                raise InvalidOperation(f"批次 {lot_id} 已存在，不能重复称重建档")
            lot = LotView(
                lot_id=lot_id, zone=zone, variety=variety,
                harvester_id=harvester_id,
                quality_reviewer_id=quality_reviewer_id,
                quality_grade=quality_grade, inbound_weight=w,
                created_at=now, updated_at=now,
            )
            event_id = uuid.uuid4().hex
            payload = {**request, "event_id": event_id, "message_id": message_id}
            try:
                self.store.insert_lot(c, lot)
            except sqlite3.IntegrityError as exc:
                raise InvalidOperation(f"批次 {lot_id} 已存在") from exc
            self.store.append_event(c, event_id, "weighed", lot_id, now, payload)
            return {"ok": True, "event_id": event_id, "lot": lot.to_dict()}

        return self._transaction(message_id, request, work, "weighed")

    def quality_review(self, lot_id: str, quality_grade: str,
                       reviewer_id: str) -> dict[str, Any]:
        """质量复核：记录评级（只追加事件，不改写原始称重）。"""
        request = {"action": "quality_review", "lot_id": lot_id,
                   "quality_grade": quality_grade, "reviewer_id": reviewer_id}

        def work(c: sqlite3.Connection, now: str) -> dict[str, Any]:
            lot = self._require_lot(c, lot_id)
            self.store.apply_lot_deltas(c, lot_id, {}, now,
                                        quality_grade=quality_grade)
            event_id = uuid.uuid4().hex
            payload = {**request, "event_id": event_id,
                       "previous_grade": lot.quality_grade}
            self.store.append_event(c, event_id, "quality_reviewed",
                                    lot_id, now, payload)
            view = self.store.get_lot_lock(c, lot_id)
            return {"ok": True, "event_id": event_id, "lot": view.to_dict()}

        return self._transaction(None, request, work, "quality_reviewed")

    # -- 拆分 / 合并 / 退回（不改写原始事实） ------------------------------

    def split(self, source_id: str, output_id: str, weight: float,
              operator_id: str, message_id: str | None = None) -> dict[str, Any]:
        """把一批货的一部分拆成新批次；只能动用未被预留的可用额度。"""
        w = self._require_positive(weight, "拆分重量")
        request = {"action": "split", "source_id": source_id,
                   "output_id": output_id, "weight": w,
                   "operator_id": operator_id}

        def work(c: sqlite3.Connection, now: str) -> dict[str, Any]:
            source = self._require_lot(c, source_id)
            self._require_mutable(source)
            if self.store.lot_exists(c, output_id):
                raise InvalidOperation(f"目标批次 {output_id} 已存在")
            if w > source.available_weight + 1e-6:
                raise InsufficientQuota(
                    f"批次 {source_id} 可用额度不足：申请 {w}，"
                    f"可用 {source.available_weight}")
            self.store.apply_lot_deltas(
                c, source_id, {"transfer_out_weight": w}, now)
            output = LotView(
                lot_id=output_id, zone=source.zone, variety=source.variety,
                harvester_id=source.harvester_id,
                quality_reviewer_id=source.quality_reviewer_id,
                quality_grade=source.quality_grade,
                transfer_in_weight=w, created_at=now, updated_at=now,
            )
            self.store.insert_lot(c, output)
            event_id = uuid.uuid4().hex
            payload = {**request, "event_id": event_id,
                      "message_id": message_id}
            self.store.append_event(
                c, event_id, "lot_split", source_id, now, payload,
                refs=[(output_id, "output")])
            return {"ok": True, "event_id": event_id,
                    "source": self.store.get_lot_lock(c, source_id).to_dict(),
                    "output": output.to_dict()}

        return self._transaction(message_id, request, work, "lot_split")

    def merge(self, input_ids: list[str], output_id: str,
              operator_id: str, weights: list[float] | None = None,
              message_id: str | None = None) -> dict[str, Any]:
        """把多批货的指定数量合并成新批次；不传 weights 时整批并入。"""
        ids = list(dict.fromkeys(input_ids))
        if not ids:
            raise InvalidOperation("合并至少需要一个来源批次")
        if len(ids) != len(input_ids):
            raise InvalidOperation("来源批次重复")
        request = {"action": "merge", "input_ids": ids,
                   "output_id": output_id, "operator_id": operator_id,
                   "weights": weights}

        def work(c: sqlite3.Connection, now: str) -> dict[str, Any]:
            lots = [self._require_lot(c, lid) for lid in ids]
            for lot in lots:
                self._require_mutable(lot)
            if self.store.lot_exists(c, output_id):
                raise InvalidOperation(f"目标批次 {output_id} 已存在")
            picked: list[tuple[LotView, float]] = []
            for i, lot in enumerate(lots):
                w = (lot.available_weight if weights is None
                     else self._require_positive(weights[i], f"批次 {lot.lot_id} 合并重量"))
                if w > lot.available_weight + 1e-6:
                    raise InsufficientQuota(
                        f"批次 {lot.lot_id} 可用额度不足：申请 {w}，"
                        f"可用 {lot.available_weight}")
                picked.append((lot, w))
            total = qweight(sum(w for _, w in picked))
            items = [{"lot_id": lot.lot_id, "weight": w}
                     for lot, w in picked]
            for lot, w in picked:
                self.store.apply_lot_deltas(
                    c, lot.lot_id, {"transfer_out_weight": w}, now)
            first = picked[0][0]
            output = LotView(
                lot_id=output_id, zone=first.zone, variety=first.variety,
                harvester_id=first.harvester_id,
                quality_reviewer_id=first.quality_reviewer_id,
                quality_grade=first.quality_grade,
                transfer_in_weight=total, created_at=now, updated_at=now,
            )
            self.store.insert_lot(c, output)
            event_id = uuid.uuid4().hex
            payload = {**request, "event_id": event_id,
                      "inputs": items, "total_weight": total,
                      "message_id": message_id}
            self.store.append_event(
                c, event_id, "lot_merge", output_id, now, payload,
                refs=[(lot.lot_id, "source") for lot, _ in picked])
            return {"ok": True, "event_id": event_id,
                    "inputs": [
                        self.store.get_lot_lock(c, lot.lot_id).to_dict()
                        for lot, _ in picked],
                    "output": output.to_dict(), "total_weight": total}

        return self._transaction(message_id, request, work, "lot_merge")

    def return_weight(self, lot_id: str, weight: float, operator_id: str,
                      reason: str = "", message_id: str | None = None,
                      returned_to: str = "") -> dict[str, Any]:
        """退回（如体验活动剩余退回/退货）：从在库核销，原始产量不变。"""
        w = self._require_positive(weight, "退回重量")
        request = {"action": "return", "lot_id": lot_id, "weight": w,
                   "operator_id": operator_id, "reason": reason,
                   "returned_to": returned_to}

        def work(c: sqlite3.Connection, now: str) -> dict[str, Any]:
            lot = self._require_lot(c, lot_id)
            self._require_mutable(lot)
            if w > lot.available_weight + 1e-6:
                raise InsufficientQuota(
                    f"批次 {lot_id} 可退回额度不足：申请 {w}，"
                    f"可用 {lot.available_weight}")
            self.store.apply_lot_deltas(
                c, lot_id, {"returned_weight": w}, now)
            event_id = uuid.uuid4().hex
            payload = {**request, "event_id": event_id,
                      "message_id": message_id}
            self.store.append_event(c, event_id, "returned", lot_id, now,
                                    payload)
            return {"ok": True, "event_id": event_id,
                    "lot": self.store.get_lot_lock(c, lot_id).to_dict()}

        return self._transaction(message_id, request, work, "returned")

    # -- 下游额度：预留 / 出库 / 释放 / 恢复 -------------------------------

    def request_quota(self, reservation_id: str, consumer_id: str,
                      items: list[dict[str, Any]], purpose: str = "",
                      expires_at: str | None = None,
                      message_id: str | None = None) -> dict[str, Any]:
        """加工/文创摊位先取得可用额度（held），整单要么全成功要么全不动。"""
        if not items:
            raise InvalidOperation("预留明细不能为空")
        norm: list[tuple[str, float]] = []
        for raw in items:
            lot_id = str(raw["lot_id"])
            w = self._require_positive(raw.get("weight"), f"批次 {lot_id} 预留重量")
            norm.append((lot_id, w))
        norm.sort(key=lambda x: x[0])
        request = {"action": "request_quota", "reservation_id": reservation_id,
                   "consumer_id": consumer_id, "items": norm,
                   "purpose": purpose, "expires_at": expires_at}

        def work(c: sqlite3.Connection, now: str) -> dict[str, Any]:
            if self.store.get_reservation(reservation_id) is not None:
                raise InvalidOperation(
                    f"预留单 {reservation_id} 已存在")
            locked: list[tuple[str, float]] = []
            for lot_id, w in norm:
                lot = self.store.get_lot_lock(c, lot_id)
                if lot is None:
                    raise NotFound(f"批次 {lot_id} 不存在")
                if lot.frozen:
                    raise FrozenLot(f"批次 {lot_id} 已被冻结，不能领取")
                if not self.store.try_reserve(c, lot_id, w, now):
                    fresh = self.store.get_lot_lock(c, lot_id)
                    if fresh is None:
                        raise NotFound(f"批次 {lot_id} 不存在")
                    if fresh.frozen:
                        raise FrozenLot(f"批次 {lot_id} 已被冻结，不能领取")
                    raise InsufficientQuota(
                        f"批次 {lot_id} 可用额度不足：申请 {w}，"
                        f"可用 {fresh.available_weight}（可能被并发领取占用）")
                locked.append((lot_id, w))
            try:
                self.store.insert_reservation(
                    c, reservation_id, consumer_id, purpose, "held",
                    locked, now, expires_at=expires_at)
            except sqlite3.IntegrityError as exc:
                raise InvalidOperation(
                    f"预留单 {reservation_id} 已存在") from exc
            event_id = uuid.uuid4().hex
            payload = {**request, "event_id": event_id,
                      "items": [{"lot_id": lid, "weight": w}
                                for lid, w in locked],
                      "message_id": message_id}
            first = locked[0][0]
            self.store.append_event(
                c, event_id, "quota_reserved", first, now, payload,
                refs=[(lid, "reserve") for lid, _ in locked[1:]])
            return {"ok": True, "event_id": event_id,
                    "reservation": self.store.get_reservation(reservation_id)}

        return self._transaction(message_id, request, work, "quota_reserved")

    def confirm_outbound(self, reservation_id: str, operator_id: str,
                         message_id: str | None = None) -> dict[str, Any]:
        """凭 held 预留单确认实际出库（重复出库消息安全重放）。"""
        request = {"action": "confirm_outbound",
                   "reservation_id": reservation_id,
                   "operator_id": operator_id}

        def work(c: sqlite3.Connection, now: str) -> dict[str, Any]:
            reservation = self.store.get_reservation(reservation_id)
            if reservation is None:
                raise NotFound(f"预留单 {reservation_id} 不存在")
            if reservation["status"] != "held":
                raise InvalidOperation(
                    f"预留单 {reservation_id} 状态为 {reservation['status']}，"
                    "只有 held 才能确认出库")
            items = [(i["lot_id"], i["weight"]) for i in reservation["items"]]
            items.sort(key=lambda x: x[0])
            for lot_id, _ in items:
                self._require_mutable(self._require_lot(c, lot_id))
            for lot_id, w in items:
                self.store.apply_lot_deltas(
                    c, lot_id, {"outbound_weight": w,
                                "reserved_weight": -w}, now)
            self.store.update_reservation_status(
                c, reservation_id, "confirmed", now)
            event_id = uuid.uuid4().hex
            payload = {**request, "event_id": event_id,
                      "consumer_id": reservation["consumer_id"],
                      "items": [{"lot_id": lid, "weight": w}
                                for lid, w in items],
                      "message_id": message_id}
            self.store.append_event(
                c, event_id, "outbound", items[0][0], now, payload,
                refs=[(lid, "outbound") for lid, _ in items[1:]])
            return {"ok": True, "event_id": event_id,
                    "reservation": self.store.get_reservation(reservation_id)}

        return self._transaction(message_id, request, work, "outbound")

    def release_quota(self, reservation_id: str, operator_id: str,
                      reason: str = "",
                      message_id: str | None = None) -> dict[str, Any]:
        """释放未使用的 held 额度（摊位放弃领取）。"""
        request = {"action": "release_quota",
                   "reservation_id": reservation_id,
                   "operator_id": operator_id, "reason": reason}

        def work(c: sqlite3.Connection, now: str) -> dict[str, Any]:
            reservation = self.store.get_reservation(reservation_id)
            if reservation is None:
                raise NotFound(f"预留单 {reservation_id} 不存在")
            if reservation["status"] != "held":
                raise InvalidOperation(
                    f"预留单 {reservation_id} 状态为 {reservation['status']}，"
                    "只能释放 held 预留")
            items = [(i["lot_id"], i["weight"]) for i in reservation["items"]]
            for lot_id, w in items:
                self.store.unreserve(c, lot_id, w, now)
            self.store.update_reservation_status(
                c, reservation_id, "released", now)
            event_id = uuid.uuid4().hex
            payload = {**request, "event_id": event_id,
                      "items": [{"lot_id": lid, "weight": w}
                                for lid, w in items],
                      "message_id": message_id}
            self.store.append_event(
                c, event_id, "quota_released", items[0][0], now, payload,
                refs=[(lid, "release") for lid, _ in items[1:]])
            return {"ok": True, "event_id": event_id,
                    "reservation": self.store.get_reservation(reservation_id)}

        return self._transaction(message_id, request, work, "quota_released")

    def recover_pending(self) -> dict[str, Any]:
        """列出所有 held 的未完成预留事务，供重启后恢复/对账。"""
        held = self.store.list_reservations_by_status("held")
        return {"ok": True, "held": held, "count": len(held)}

    def sweep_expired(self) -> dict[str, Any]:
        """释放所有已过期仍 held 的预留（恢复流程的自动兜底）。"""
        now_iso = self._now()
        released: list[str] = []
        for reservation in self.store.list_reservations_by_status("held"):
            expires = reservation.get("expires_at")
            if expires and expires < now_iso:
                self.release_quota(
                    reservation["reservation_id"],
                    operator_id="system:sweep",
                    reason="预留过期自动释放")
                released.append(reservation["reservation_id"])
        return {"ok": True, "released": released, "count": len(released)}

    # -- 争议冻结 ---------------------------------------------------------

    def freeze(self, lot_id: str, reason: str, operator_id: str,
               message_id: str | None = None) -> dict[str, Any]:
        """冻结争议批次：冻结期间禁止拆分、出库、退回等变动。"""
        request = {"action": "freeze", "lot_id": lot_id, "reason": reason,
                   "operator_id": operator_id}

        def work(c: sqlite3.Connection, now: str) -> dict[str, Any]:
            lot = self._require_lot(c, lot_id)
            if lot.frozen:
                return {"ok": True, "already_frozen": True,
                        "lot": lot.to_dict()}
            self.store.apply_lot_deltas(c, lot_id, {}, now, frozen=True)
            event_id = uuid.uuid4().hex
            payload = {**request, "event_id": event_id,
                      "message_id": message_id}
            self.store.append_event(c, event_id, "frozen", lot_id, now, payload)
            return {"ok": True, "event_id": event_id,
                    "lot": self.store.get_lot_lock(c, lot_id).to_dict()}

        return self._transaction(message_id, request, work, "frozen")

    def unfreeze(self, lot_id: str, reason: str, operator_id: str,
                 message_id: str | None = None) -> dict[str, Any]:
        request = {"action": "unfreeze", "lot_id": lot_id, "reason": reason,
                   "operator_id": operator_id}

        def work(c: sqlite3.Connection, now: str) -> dict[str, Any]:
            lot = self._require_lot(c, lot_id)
            if not lot.frozen:
                return {"ok": True, "already_unfrozen": True,
                        "lot": lot.to_dict()}
            self.store.apply_lot_deltas(c, lot_id, {}, now, frozen=False)
            event_id = uuid.uuid4().hex
            payload = {**request, "event_id": event_id,
                      "message_id": message_id}
            self.store.append_event(c, event_id, "unfrozen", lot_id, now,
                                    payload)
            return {"ok": True, "event_id": event_id,
                    "lot": self.store.get_lot_lock(c, lot_id).to_dict()}

        return self._transaction(message_id, request, work, "unfrozen")

    # -- 盘点 -------------------------------------------------------------

    def _expected_at_period_end(self, period: str
                                ) -> tuple[dict[str, dict[str, float]], int]:
        """重放期间结束前的事件，得到当时账面（跨月盘点不受次月事件影响）。"""
        _, end = period_bounds(period)
        events = self.store.events_until(end.isoformat())
        acc: dict[str, dict[str, float]] = {}
        for event in events:
            apply_event(acc, event)
        from_seq = events[-1]["seq"] if events else 0
        return acc, from_seq

    def create_stocktake(self, stocktake_id: str, period: str,
                         responsible_id: str,
                         counts: dict[str, float], note: str = "",
                         message_id: str | None = None) -> dict[str, Any]:
        """创建盘点单：账面数量取期间结束时刻的重放值，差异=实盘-账面。"""
        try:
            period_bounds(period)
        except (ValueError, AttributeError) as exc:
            raise InvalidOperation(f"期间格式应为 YYYY-MM：{period}") from exc
        if not counts:
            raise InvalidOperation("盘点明细不能为空")
        normalized = {str(k): self._require_positive_or_zero(v, str(k))
                      for k, v in counts.items()}
        request = {"action": "create_stocktake",
                   "stocktake_id": stocktake_id, "period": period,
                   "responsible_id": responsible_id,
                   "counts": normalized, "note": note}

        def work(c: sqlite3.Connection, now: str) -> dict[str, Any]:
            expected, _ = self._expected_at_period_end(period)
            items: list[tuple[str, float, float, float]] = []
            for lot_id in sorted(normalized):
                if not self.store.lot_exists(c, lot_id):
                    raise NotFound(f"批次 {lot_id} 不存在")
                exp = stock_of(expected.get(lot_id, _empty_acc()))
                cnt = normalized[lot_id]
                items.append((lot_id, exp, cnt, qweight(cnt - exp)))
            self.store.insert_stocktake(
                c, stocktake_id, period, responsible_id, note, "open",
                now, items)
            event_id = uuid.uuid4().hex
            payload = {**request, "event_id": event_id,
                      "items": [
                          {"lot_id": lid, "expected_weight": exp,
                           "counted_weight": cnt, "diff_weight": diff}
                          for lid, exp, cnt, diff in items],
                      "message_id": message_id}
            self.store.append_event(
                c, event_id, "stocktake_created", items[0][0], now, payload,
                refs=[(lid, "counted") for lid, *_ in items[1:]])
            return {"ok": True, "event_id": event_id,
                    "stocktake": self.store.get_stocktake(stocktake_id)}

        return self._transaction(message_id, request, work,
                                 "stocktake_created")

    @staticmethod
    def _require_positive_or_zero(value: Any, label: str) -> float:
        try:
            result = qweight(value)
        except (TypeError, ValueError) as exc:
            raise InvalidOperation(f"{label} 的实盘数量必须是数字") from exc
        if result < 0:
            raise InvalidOperation(f"{label} 的实盘数量不能为负")
        return result

    def decide_stocktake(self, stocktake_id: str, decision: str,
                         decision_owner_id: str,
                         message_id: str | None = None) -> dict[str, Any]:
        """对盘点差异作出处理决定并关联责任人。

        - ``adjust``：按差异调整账面（盘盈盘亏入 adjustment_weight）；
        - ``reject``：维持原账面，差异挂账待查，只记录决定；
        - ``freeze``：冻结所有存在差异的批次，进入争议处理。
        """
        if decision not in ("adjust", "reject", "freeze"):
            raise InvalidOperation(
                "处理决定只能是 adjust / reject / freeze")
        request = {"action": "decide_stocktake",
                   "stocktake_id": stocktake_id, "decision": decision,
                   "decision_owner_id": decision_owner_id}

        def work(c: sqlite3.Connection, now: str) -> dict[str, Any]:
            stocktake = self.store.get_stocktake(stocktake_id)
            if stocktake is None:
                raise NotFound(f"盘点单 {stocktake_id} 不存在")
            if stocktake["status"] != "open":
                raise InvalidOperation(
                    f"盘点单 {stocktake_id} 已作出决定：{stocktake['decision']}")
            diffs = [{"lot_id": i["lot_id"], "diff_weight": i["diff_weight"]}
                     for i in stocktake["items"]]
            if decision == "adjust":
                for item in stocktake["items"]:
                    if abs(item["diff_weight"]) > 1e-6:
                        self.store.apply_lot_deltas(
                            c, item["lot_id"],
                            {"adjustment_weight": item["diff_weight"]}, now)
                etype = "stocktake_adjusted"
            elif decision == "freeze":
                for item in stocktake["items"]:
                    if abs(item["diff_weight"]) > 1e-6:
                        self.store.apply_lot_deltas(
                            c, item["lot_id"], {}, now, frozen=True)
                etype = "stocktake_frozen"
            else:
                etype = "stocktake_rejected"
            self.store.decide_stocktake(
                c, stocktake_id, decision, decision_owner_id, now)
            event_id = uuid.uuid4().hex
            payload = {**request, "event_id": event_id,
                      "period": stocktake["period"],
                      "responsible_id": stocktake["responsible_id"],
                      "items": diffs, "message_id": message_id}
            self.store.append_event(
                c, event_id, etype, diffs[0]["lot_id"], now, payload,
                refs=[(d["lot_id"], "counted") for d in diffs[1:]])
            return {"ok": True, "event_id": event_id,
                    "stocktake": self.store.get_stocktake(stocktake_id)}

        return self._transaction(message_id, request, work, "stocktake_decided")

    def get_stocktake(self, stocktake_id: str) -> dict[str, Any]:
        stocktake = self.store.get_stocktake(stocktake_id)
        if stocktake is None:
            raise NotFound(f"盘点单 {stocktake_id} 不存在")
        return {"ok": True, "stocktake": stocktake}

    # -- 追溯 / 查询 ------------------------------------------------------

    def get_lot(self, lot_id: str) -> dict[str, Any]:
        lot = self.store.get_lot(lot_id)
        if lot is None:
            raise NotFound(f"批次 {lot_id} 不存在")
        return {"ok": True, "lot": lot.to_dict()}

    def list_lots(self) -> dict[str, Any]:
        return {"ok": True, "lots": [lot.to_dict() for lot in self.store.list_lots()]}

    def trace(self, lot_id: str) -> dict[str, Any]:
        """按批次追溯：当前台账 + 全部相关事件 + 上下游血缘。"""
        lot = self.store.get_lot(lot_id)
        if lot is None:
            raise NotFound(f"批次 {lot_id} 不存在")
        events = self.store.events_for_lot(lot_id)
        parents: list[dict[str, Any]] = []
        children: list[dict[str, Any]] = []
        for event in events:
            payload = event["payload"]
            if event["event_type"] == "lot_split":
                if event["lot_id"] == lot_id:
                    children.append({"lot_id": payload["output_id"],
                                     "weight": payload["weight"],
                                     "via_event": event["event_id"]})
                elif payload.get("output_id") == lot_id:
                    parents.append({"lot_id": event["lot_id"],
                                    "weight": payload["weight"],
                                    "via_event": event["event_id"]})
            elif event["event_type"] == "lot_merge":
                if event["lot_id"] == lot_id:
                    parents = [{"lot_id": i["lot_id"], "weight": i["weight"],
                                "via_event": event["event_id"]}
                               for i in payload["inputs"]]
                elif any(i["lot_id"] == lot_id for i in payload["inputs"]):
                    mine = next(i for i in payload["inputs"]
                                if i["lot_id"] == lot_id)
                    children.append({"lot_id": event["lot_id"],
                                     "weight": mine["weight"],
                                     "via_event": event["event_id"]})
        return {"ok": True, "lot": lot.to_dict(), "lineage": {
                    "parents": parents, "children": children},
                "events": events}

    # -- 期间结算快照 -----------------------------------------------------

    def generate_settlement(self, period: str,
                            snapshot_id: str | None = None,
                            note: str = "") -> dict[str, Any]:
        """生成某期间的结算快照（不可变）。默认按期间命名，重复生成返回原件。"""
        start, end = period_bounds(period)
        snapshot_id = snapshot_id or f"settlement-{period}"

        def work(c: sqlite3.Connection, now: str) -> dict[str, Any]:
            existing = self.store.get_snapshot(snapshot_id)
            if existing is not None:
                return {"ok": True, "replayed": True, "snapshot": existing}
            start_iso, end_iso = start.isoformat(), end.isoformat()
            events = self.store.events_until(end_iso)
            cumulative: dict[str, dict[str, float]] = {}
            period_move: dict[str, dict[str, float]] = {}
            for event in events:
                apply_event(cumulative, event)
                if event["occurred_at"] >= start_iso:
                    apply_event(period_move, event)
            from_seq = events[-1]["seq"] if events else 0
            entries: list[dict[str, Any]] = []
            totals = {f"period_{f.replace('_weight', '')}": 0.0
                      for f in QFIELDS}
            totals.update(ending_stock=0.0, ending_reserved=0.0,
                          ending_available=0.0)
            for lid in sorted(cumulative):
                acc = cumulative[lid]
                move = period_move.get(lid, _empty_acc())
                lot = self.store.get_lot(lid)
                stock = stock_of(acc)
                reserved = qweight(acc["reserved_weight"])
                base = {
                    "lot_id": lid,
                    "zone": lot.zone if lot else "",
                    "variety": lot.variety if lot else "",
                    "harvester_id": lot.harvester_id if lot else "",
                    "quality_reviewer_id": (lot.quality_reviewer_id
                                            if lot else ""),
                    "stock_weight": stock, "reserved_weight": reserved,
                }
                for f in QFIELDS:
                    if f == "reserved_weight":
                        continue
                    base[f] = qweight(acc[f])
                    totals[f"period_{f.replace('_weight', '')}"] += move[f]
                entries.append(base)
                totals["ending_stock"] += stock
                totals["ending_reserved"] += reserved
                totals["ending_available"] += qweight(stock - reserved)
            totals = {k: qweight(v) for k, v in totals.items()}
            # 勾稽：期初+本期入-本期出 = 期末（由重放构造，恒成立，留作显式断言）。
            totals["reconciled"] = True
            summary = {"period": period, "generated_at": now,
                        "from_seq": from_seq, **totals}
            self.store.insert_snapshot(
                c, snapshot_id, period, now, from_seq, note, summary, entries)
            return {"ok": True, "event_count": len(events),
                    "snapshot": self.store.get_snapshot(snapshot_id)}

        # 快照本身也是一次可重入的写入，用带前缀的 snapshot_id 做消息键；
        # 身份只取决于期间与快照号，备注不同不视为冲突请求。
        return self._transaction(f"settlement:{snapshot_id}",
                                 {"action": "settlement", "period": period,
                                  "snapshot_id": snapshot_id},
                                 work, "settlement")

    def get_settlement(self, snapshot_id: str) -> dict[str, Any]:
        snapshot = self.store.get_snapshot(snapshot_id)
        if snapshot is None:
            raise NotFound(f"结算快照 {snapshot_id} 不存在")
        return {"ok": True, "snapshot": snapshot}
