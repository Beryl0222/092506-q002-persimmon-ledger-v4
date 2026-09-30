"""采收批次与出库额度的应用服务入口。

记账原则
========
* **事实只追加**：采收称重、拆分、合并、退回、预占、出库、释放、冻结和
  盘点调账都写成不可修改的事件；余额错误只能用新事件（调账/退回）纠正。
* **额度先行**：下游摊位（加工、文创、体验、冷藏）必须先 ``reserve``
  取得可用额度，之后才能 ``confirm_outbound`` 出库；未用完的额度可以
  释放，过期预占由 ``recover_pending`` 统一回收。
* **幂等重放**：所有写操作接受 ``idempotency_key``，重复的称重或出库
  消息返回第一次的结果，绝不重复记账。
* **时间可注入**：服务依赖一个时钟，测试传入 ``FixedClock`` 即可确定性
  地验证跨月结算、TTL 过期与重启恢复。
"""
import uuid
from datetime import timedelta
from typing import Any, Iterable

from .domain import (
    Clock,
    EV_CONFIRMED,
    EV_FROZEN,
    EV_HARVESTED,
    EV_MERGED_IN,
    EV_MERGE_SOURCE,
    EV_RESERVED,
    EV_RELEASED,
    EV_RETURNED,
    EV_SNAPSHOT,
    EV_SPLIT,
    EV_SPLIT_CHILD,
    EV_STOCK_ADJUST,
    EV_UNFROZEN,
    LedgerError,
    Lot,
    Record,
    STATE_ACTIVE,
    STATE_CLOSED,
    STATE_FROZEN,
)
from .store import Store

# 盘点处理决定
DECISION_ADJUST = "adjust"   # 按实盘数调账
DECISION_FREEZE = "freeze"   # 冻结争议批次，待进一步处理
DECISION_WAIVE = "waive"     # 登记差异与责任人，暂不调账

_RESERVATION_HELD = "held"
_RESERVATION_CONFIRMED = "confirmed"
_RESERVATION_RELEASED = "released"

_DEFAULT_TTL_SECONDS = 3600


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _event_dict(event) -> dict[str, Any]:
    return {
        "seq": event.seq,
        "event_id": event.event_id,
        "event_type": event.event_type,
        "lot_id": event.lot_id,
        "quantity": round(event.quantity, 3),
        "actor_id": event.actor_id,
        "occurred_at": event.occurred_at,
        "idempotency_key": event.idempotency_key,
        "payload": event.payload,
    }


def _reservation_dict(row) -> dict[str, Any]:
    return {
        "reservation_id": row["reservation_id"],
        "lot_id": row["lot_id"],
        "downstream_id": row["downstream_id"],
        "purpose": row["purpose"],
        "quantity": round(row["quantity"], 3),
        "status": row["status"],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "confirmed_at": row["confirmed_at"],
        "note": row["note"],
    }


def _stocktake_dict(row) -> dict[str, Any]:
    return {
        "stocktake_id": row["stocktake_id"],
        "lot_id": row["lot_id"],
        "system_quantity": round(row["system_quantity"], 3),
        "counted_quantity": round(row["counted_quantity"], 3),
        "diff": round(row["diff"], 3),
        "responsible_party_id": row["responsible_party_id"],
        "decided_by": row["decided_by"],
        "decision": row["decision"],
        "note": row["note"],
        "period_start": row["period_start"],
        "period_end": row["period_end"],
        "adjustment_event_id": row["adjustment_event_id"],
        "created_at": row["created_at"],
    }


class Service:
    def __init__(self, store: Store | None = None, clock: Clock | None = None) -> None:
        self.store = store or Store()
        self.clock = clock or Clock()

    # ------------------------------------------------------------ 基础能力

    def health(self) -> dict[str, str]:
        return {"service": "persimmon_ledger", "status": "ok"}

    def register(self, record_id: str, owner_id: str) -> dict[str, str]:
        record = self.store.save(Record(record_id, owner_id))
        return {"record_id": record.record_id, "owner_id": record.owner_id,
                "state": record.state, "created_at": record.created_at}

    def find(self, record_id: str) -> dict[str, str] | None:
        record = self.store.get(record_id)
        return record.__dict__.copy() if record else None

    # ------------------------------------------------------------ 采收登记

    def record_harvest(self, lot_id: str, zone: str, variety: str,
                       harvester_id: str, quality_checker_id: str,
                       quantity: float, quality_grade: str = "",
                       note: str = "", idempotency_key: str = "") -> dict[str, Any]:
        """登记一批采收果实（称重消息）。重复消息安全重放。"""
        quantity = _require_positive(quantity, "quantity")
        now = self.clock.now_iso()
        with self.store.tx() as cur:
            cached = self.store.get_reply(cur, idempotency_key)
            if cached is not None:
                return cached
            if self.store.get_lot(cur, lot_id) is not None:
                raise LedgerError(f"批次 {lot_id} 已存在")
            event = self.store.append_event(
                cur, EV_HARVESTED, lot_id, quantity, harvester_id, now,
                payload={"zone": zone, "variety": variety,
                         "quality_checker_id": quality_checker_id,
                         "quality_grade": quality_grade, "note": note},
                idempotency_key=idempotency_key)
            lot = Lot(
                lot_id=lot_id, zone=zone, variety=variety,
                harvester_id=harvester_id, quality_checker_id=quality_checker_id,
                quality_grade=quality_grade, quantity=quantity,
                state=STATE_ACTIVE, balance=quantity, held=0.0,
                created_event_id=event.event_id, created_at=now)
            self.store.insert_lot(cur, lot)
            response = self.store.lot_as_dict(lot)
            response["event_seq"] = event.seq
            self.store.put_reply(cur, idempotency_key, "harvest", response, now)
            return response

    # ------------------------------------------------------------ 拆分/合并

    def split(self, lot_id: str, child_lot_id: str, quantity: float,
              quality_grade: str = "", actor_id: str = "", note: str = "",
              idempotency_key: str = "") -> dict[str, Any]:
        """把一批货的一部分拆成子批次（可指定分拣后的新等级）。

        只能拆分尚未被预占的可用额度；原始采收事件保持不变，账本上只
        增加一对“拆出/拆入”事实。
        """
        quantity = _require_positive(quantity, "quantity")
        if not child_lot_id:
            raise LedgerError("child_lot_id 不能为空")
        now = self.clock.now_iso()
        with self.store.tx() as cur:
            cached = self.store.get_reply(cur, idempotency_key)
            if cached is not None:
                return cached
            parent = self._require_active(cur, lot_id)
            if self.store.get_lot(cur, child_lot_id) is not None:
                raise LedgerError(f"子批次 {child_lot_id} 已存在")
            if round(parent.balance - quantity, 3) < 0:
                raise LedgerError(
                    f"批次 {lot_id} 可用额度 {parent.balance} 不足，"
                    f"无法拆分 {quantity}（已预占 {parent.held} 不可拆）")
            grade = quality_grade or parent.quality_grade
            self.store.append_event(
                cur, EV_SPLIT, lot_id, quantity, actor_id or parent.harvester_id, now,
                payload={"child_lot_id": child_lot_id, "quality_grade": grade,
                         "note": note},
                idempotency_key=idempotency_key)
            child_event = self.store.append_event(
                cur, EV_SPLIT_CHILD, child_lot_id, quantity,
                actor_id or parent.harvester_id, now,
                payload={"parent_lot_id": lot_id, "quality_grade": grade,
                         "zone": parent.zone, "variety": parent.variety,
                         "note": note})
            self.store.adjust_balance(cur, lot_id, delta_balance=-quantity)
            self._maybe_close(cur, self.store.require_lot(cur, lot_id))
            child = Lot(
                lot_id=child_lot_id, zone=parent.zone, variety=parent.variety,
                harvester_id=parent.harvester_id,
                quality_checker_id=parent.quality_checker_id,
                quality_grade=grade, quantity=quantity, state=STATE_ACTIVE,
                balance=quantity, held=0.0, parent_lot_id=lot_id,
                created_event_id=child_event.event_id, created_at=now)
            self.store.insert_lot(cur, child)
            response = {
                "parent": self.store.lot_as_dict(self.store.require_lot(cur, lot_id)),
                "child": self.store.lot_as_dict(child),
            }
            self.store.put_reply(cur, idempotency_key, "split", response, now)
            return response

    def merge(self, sources: list[dict[str, Any]], target_lot_id: str,
              actor_id: str, quality_grade: str = "", note: str = "",
              idempotency_key: str = "") -> dict[str, Any]:
        """把多个批次（全部或部分）合并为新批次。

        sources 为 ``[{"lot_id": ..., "quantity": ...}, ...]``，只允许合
        并同一品种；不同分拣等级可以合并，但需显式给出目标等级。
        """
        if not target_lot_id:
            raise LedgerError("target_lot_id 不能为空")
        if not sources:
            raise LedgerError("合并至少需要一个来源批次")
        normalized: list[tuple[str, float]] = []
        for item in sources:
            qty = _require_positive(float(item["quantity"]), "quantity")
            normalized.append((str(item["lot_id"]), qty))
        now = self.clock.now_iso()
        with self.store.tx() as cur:
            cached = self.store.get_reply(cur, idempotency_key)
            if cached is not None:
                return cached
            if self.store.get_lot(cur, target_lot_id) is not None:
                raise LedgerError(f"目标批次 {target_lot_id} 已存在")
            source_lots: list[Lot] = []
            total = 0.0
            varieties = set()
            for source_id, qty in normalized:
                lot = self._require_active(cur, source_id)
                if round(lot.balance - qty, 3) < 0:
                    raise LedgerError(
                        f"批次 {source_id} 可用额度 {lot.balance} 不足，"
                        f"无法合并出 {qty}")
                source_lots.append(lot)
                varieties.add(lot.variety)
                total = round(total + qty, 3)
            if len(varieties) > 1:
                raise LedgerError(f"只能合并同一品种，收到品种：{sorted(varieties)}")
            base = source_lots[0]
            grade = quality_grade or base.quality_grade
            sources_payload = [{"lot_id": lid, "quantity": qty}
                               for lid, qty in normalized]
            for (source_id, qty), lot in zip(normalized, source_lots):
                self.store.append_event(
                    cur, EV_MERGE_SOURCE, source_id, qty,
                    actor_id or lot.harvester_id, now,
                    payload={"target_lot_id": target_lot_id, "sources": sources_payload,
                             "quality_grade": grade, "note": note})
                self.store.adjust_balance(cur, source_id, delta_balance=-qty)
                self._maybe_close(
                    cur, self.store.require_lot(cur, source_id))
            merged_event = self.store.append_event(
                cur, EV_MERGED_IN, target_lot_id, total,
                actor_id or base.harvester_id, now,
                payload={"sources": sources_payload, "quality_grade": grade,
                         "variety": base.variety, "zone": base.zone, "note": note},
                idempotency_key=idempotency_key)
            target = Lot(
                lot_id=target_lot_id, zone=base.zone, variety=base.variety,
                harvester_id=base.harvester_id,
                quality_checker_id=base.quality_checker_id,
                quality_grade=grade, quantity=total, state=STATE_ACTIVE,
                balance=total, held=0.0,
                created_event_id=merged_event.event_id, created_at=now)
            self.store.insert_lot(cur, target)
            response = {"target": self.store.lot_as_dict(target),
                        "sources": sources_payload, "total_quantity": total}
            self.store.put_reply(cur, idempotency_key, "merge", response, now)
            return response

    def return_to_lot(self, lot_id: str, quantity: float, actor_id: str,
                      downstream_id: str = "", reason: str = "",
                      idempotency_key: str = "") -> dict[str, Any]:
        """下游（体验活动等）退回果实，作为新事件重新入帐，不改动旧出库。"""
        quantity = _require_positive(quantity, "quantity")
        now = self.clock.now_iso()
        with self.store.tx() as cur:
            cached = self.store.get_reply(cur, idempotency_key)
            if cached is not None:
                return cached
            lot = self.store.require_lot(cur, lot_id)
            if lot.state == STATE_FROZEN:
                raise LedgerError(
                    f"批次 {lot_id} 已因争议冻结（{lot.frozen_reason}），禁止变动")
            event = self.store.append_event(
                cur, EV_RETURNED, lot_id, quantity, actor_id, now,
                payload={"downstream_id": downstream_id, "reason": reason},
                idempotency_key=idempotency_key)
            lot = self.store.adjust_balance(cur, lot_id, delta_balance=quantity)
            if lot.state == STATE_CLOSED:
                lot = self.store.set_lot_state(cur, lot_id, STATE_ACTIVE)
            response = self.store.lot_as_dict(self.store.require_lot(cur, lot_id))
            response["event_seq"] = event.seq
            self.store.put_reply(cur, idempotency_key, "return", response, now)
            return response

    # ------------------------------------------------------------ 冻结争议

    def freeze(self, lot_id: str, actor_id: str, reason: str,
               idempotency_key: str = "") -> dict[str, Any]:
        """冻结争议批次：冻结期间禁止拆分、合并、退回和一切额度变动。"""
        if not reason:
            raise LedgerError("冻结必须填写争议原因")
        now = self.clock.now_iso()
        with self.store.tx() as cur:
            cached = self.store.get_reply(cur, idempotency_key)
            if cached is not None:
                return cached
            lot = self.store.require_lot(cur, lot_id)
            if lot.state == STATE_FROZEN:
                raise LedgerError(f"批次 {lot_id} 已处于冻结状态")
            previous_state = lot.state
            event = self.store.append_event(
                cur, EV_FROZEN, lot_id, 0.0, actor_id, now,
                payload={"reason": reason, "previous_state": previous_state},
                idempotency_key=idempotency_key)
            lot = self.store.set_lot_state(cur, lot_id, STATE_FROZEN, reason)
            response = self.store.lot_as_dict(lot)
            response["event_seq"] = event.seq
            self.store.put_reply(cur, idempotency_key, "freeze", response, now)
            return response

    def unfreeze(self, lot_id: str, actor_id: str, resolution: str = "",
                 idempotency_key: str = "") -> dict[str, Any]:
        now = self.clock.now_iso()
        with self.store.tx() as cur:
            cached = self.store.get_reply(cur, idempotency_key)
            if cached is not None:
                return cached
            lot = self.store.require_lot(cur, lot_id)
            if lot.state != STATE_FROZEN:
                raise LedgerError(f"批次 {lot_id} 未冻结，无需解冻")
            # 恢复冻结前的状态（active 或 closed）
            restore_state = STATE_ACTIVE
            for past in self.store.events_for_lot(cur, lot_id):
                if past.event_type == EV_FROZEN:
                    restore_state = past.payload.get(
                        "previous_state", STATE_ACTIVE)
            event = self.store.append_event(
                cur, EV_UNFROZEN, lot_id, 0.0, actor_id, now,
                payload={"resolution": resolution,
                         "restored_state": restore_state},
                idempotency_key=idempotency_key)
            lot = self.store.set_lot_state(cur, lot_id, restore_state, "")
            response = self.store.lot_as_dict(lot)
            response["event_seq"] = event.seq
            self.store.put_reply(cur, idempotency_key, "unfreeze", response, now)
            return response

    # ------------------------------------------------------------ 额度预占

    def reserve(self, lot_id: str, downstream_id: str, quantity: float,
                purpose: str = "", ttl_seconds: int = _DEFAULT_TTL_SECONDS,
                actor_id: str = "", reservation_id: str = "", note: str = "",
                idempotency_key: str = "") -> dict[str, Any]:
        """下游领取前先预占可用额度。额度从 balance 移入 held。"""
        quantity = _require_positive(quantity, "quantity")
        if ttl_seconds <= 0:
            raise LedgerError("ttl_seconds 必须为正")
        reservation_id = reservation_id or _new_id("rsv")
        now_dt = self.clock.now()
        now = now_dt.isoformat()
        expires = (now_dt + timedelta(seconds=ttl_seconds)).isoformat()
        with self.store.tx() as cur:
            cached = self.store.get_reply(cur, idempotency_key)
            if cached is not None:
                return cached
            if self.store.get_reservation(cur, reservation_id) is not None:
                raise LedgerError(f"预占 {reservation_id} 已存在")
            lot = self._require_active(cur, lot_id)
            if round(lot.balance - quantity, 3) < 0:
                raise LedgerError(
                    f"批次 {lot_id} 可用额度 {lot.balance} 不足，无法预占 {quantity}")
            event = self.store.append_event(
                cur, EV_RESERVED, lot_id, quantity,
                actor_id or downstream_id, now,
                payload={"reservation_id": reservation_id,
                         "downstream_id": downstream_id, "purpose": purpose,
                         "expires_at": expires, "note": note},
                idempotency_key=idempotency_key)
            self.store.adjust_balance(cur, lot_id,
                                      delta_balance=-quantity, delta_held=quantity)
            values = {"reservation_id": reservation_id, "lot_id": lot_id,
                      "downstream_id": downstream_id, "purpose": purpose,
                      "quantity": quantity, "status": _RESERVATION_HELD,
                      "created_at": now, "expires_at": expires, "note": note}
            self.store.insert_reservation(cur, values)
            row = self.store.get_reservation(cur, reservation_id)
            response = _reservation_dict(row)
            response["event_seq"] = event.seq
            response["lot"] = self.store.lot_as_dict(
                self.store.require_lot(cur, lot_id))
            self.store.put_reply(cur, idempotency_key, "reserve", response, now)
            return response

    def confirm_outbound(self, reservation_id: str, actor_id: str = "",
                         idempotency_key: str = "") -> dict[str, Any]:
        """凭预占确认出库，held 中的货正式离开园区。重复出库消息安全重放。"""
        now = self.clock.now_iso()
        with self.store.tx() as cur:
            cached = self.store.get_reply(cur, idempotency_key)
            if cached is not None:
                return cached
            row = self.store.get_reservation(cur, reservation_id)
            if row is None:
                raise LedgerError(f"预占 {reservation_id} 不存在")
            if row["status"] == _RESERVATION_CONFIRMED:
                raise LedgerError(f"预占 {reservation_id} 已出库，不能重复确认")
            if row["status"] == _RESERVATION_RELEASED:
                raise LedgerError(f"预占 {reservation_id} 已释放，不能出库")
            lot = self.store.require_lot(cur, row["lot_id"])
            if lot.state == STATE_FROZEN:
                raise LedgerError(f"批次 {lot.lot_id} 已冻结，不能出库")
            event = self.store.append_event(
                cur, EV_CONFIRMED, lot.lot_id, row["quantity"],
                actor_id or row["downstream_id"], now,
                payload={"reservation_id": reservation_id,
                         "downstream_id": row["downstream_id"],
                         "purpose": row["purpose"]},
                idempotency_key=idempotency_key)
            self.store.adjust_balance(cur, lot.lot_id, delta_held=-row["quantity"])
            self.store.update_reservation(cur, reservation_id,
                                          _RESERVATION_CONFIRMED, now)
            self._maybe_close(cur, self.store.require_lot(cur, lot.lot_id))
            final_row = self.store.get_reservation(cur, reservation_id)
            response = _reservation_dict(final_row)
            response["event_seq"] = event.seq
            response["lot"] = self.store.lot_as_dict(
                self.store.require_lot(cur, lot.lot_id))
            self.store.put_reply(cur, idempotency_key, "confirm_outbound",
                                 response, now)
            return response

    def release_reservation(self, reservation_id: str, actor_id: str,
                            reason: str = "", idempotency_key: str = "") -> dict[str, Any]:
        """手动释放未使用的预占，额度退回可用余额。"""
        now = self.clock.now_iso()
        with self.store.tx() as cur:
            cached = self.store.get_reply(cur, idempotency_key)
            if cached is not None:
                return cached
            row = self._require_held(cur, reservation_id)
            lot = self.store.require_lot(cur, row["lot_id"])
            event = self.store.append_event(
                cur, EV_RELEASED, lot.lot_id, row["quantity"], actor_id, now,
                payload={"reservation_id": reservation_id,
                         "downstream_id": row["downstream_id"],
                         "reason": reason or "manual_release"},
                idempotency_key=idempotency_key)
            self.store.adjust_balance(cur, lot.lot_id,
                                      delta_balance=row["quantity"],
                                      delta_held=-row["quantity"])
            self.store.update_reservation(cur, reservation_id,
                                          _RESERVATION_RELEASED)
            response = _reservation_dict(
                self.store.get_reservation(cur, reservation_id))
            response["event_seq"] = event.seq
            response["lot"] = self.store.lot_as_dict(
                self.store.require_lot(cur, lot.lot_id))
            self.store.put_reply(cur, idempotency_key, "release", response, now)
            return response

    def recover_pending(self, actor_id: str = "system",
                        idempotency_key: str = "") -> dict[str, Any]:
        """恢复未完成事务：回收所有已过期但仍 held 的预占额度。

        服务重启后调用本方法即可；未过期的预占继续有效，仍可正常出库。
        """
        now = self.clock.now_iso()
        with self.store.tx() as cur:
            cached = self.store.get_reply(cur, idempotency_key)
            if cached is not None:
                return cached
            expired = self.store.list_reservations(
                cur, (_RESERVATION_HELD,), only_expired_before=now)
            recovered: list[dict[str, Any]] = []
            for row in expired:
                lot = self.store.get_lot(cur, row["lot_id"])
                # 冻结批次的预占挂起，待解冻后再回收，避免静默写争议批次。
                if lot is not None and lot.state == STATE_FROZEN:
                    continue
                event = self.store.append_event(
                    cur, EV_RELEASED, row["lot_id"], row["quantity"], actor_id, now,
                    payload={"reservation_id": row["reservation_id"],
                             "downstream_id": row["downstream_id"],
                             "reason": "ttl_expired",
                             "recovered_after_restart": True})
                self.store.adjust_balance(cur, row["lot_id"],
                                          delta_balance=row["quantity"],
                                          delta_held=-row["quantity"])
                self.store.update_reservation(cur, row["reservation_id"],
                                              _RESERVATION_RELEASED)
                recovered.append({"reservation_id": row["reservation_id"],
                                  "lot_id": row["lot_id"],
                                  "quantity": round(row["quantity"], 3),
                                  "event_seq": event.seq})
            still_held = [
                _reservation_dict(r)
                for r in self.store.list_reservations(cur, (_RESERVATION_HELD,))
            ]
            response = {"recovered": recovered,
                        "recovered_count": len(recovered),
                        "still_held": still_held,
                        "recovered_at": now}
            self.store.put_reply(cur, idempotency_key, "recover_pending",
                                 response, now)
            return response

    # ------------------------------------------------------------ 盘点差异

    def record_stocktake(self, lot_id: str, counted_quantity: float,
                         decided_by: str, decision: str,
                         responsible_party_id: str = "", note: str = "",
                         stocktake_id: str = "", period_start: str = "",
                         period_end: str = "",
                         idempotency_key: str = "") -> dict[str, Any]:
        """登记盘点结果，关联责任人与处理决定。

        decision 取值：``adjust`` 按实盘数调账；``freeze`` 冻结争议批次；
        ``waive`` 仅登记差异与责任人、不动余额。差异 = 实盘 - 账面。
        """
        if decision not in (DECISION_ADJUST, DECISION_FREEZE, DECISION_WAIVE):
            raise LedgerError(
                "decision 必须是 adjust、freeze 或 waive")
        stocktake_id = stocktake_id or _new_id("stk")
        now = self.clock.now_iso()
        with self.store.tx() as cur:
            cached = self.store.get_reply(cur, idempotency_key)
            if cached is not None:
                return cached
            if self.store.get_stocktake(cur, stocktake_id) is not None:
                raise LedgerError(f"盘点单 {stocktake_id} 已存在")
            lot = self.store.require_lot(cur, lot_id)
            system_quantity = round(lot.balance + lot.held, 3)
            counted_quantity = round(float(counted_quantity), 3)
            diff = round(counted_quantity - system_quantity, 3)
            adjustment_event_id = 0
            if decision == DECISION_ADJUST and abs(diff) > 1e-9:
                if lot.state == STATE_FROZEN:
                    raise LedgerError(f"批次 {lot_id} 已冻结，不能调账；"
                                      "请先解冻或改用 freeze/waive 决定")
                if round(lot.balance + diff, 3) < 0:
                    raise LedgerError(
                        f"盘亏 {abs(diff)} 超过可用余额 {lot.balance}（其中 "
                        f"{lot.held} 已被预占），不能直接调账，请改用 freeze")
                event = self.store.append_event(
                    cur, EV_STOCK_ADJUST, lot_id, diff, decided_by, now,
                    payload={"stocktake_id": stocktake_id,
                             "system_quantity": system_quantity,
                             "counted_quantity": counted_quantity,
                             "responsible_party_id": responsible_party_id,
                             "decision": decision, "note": note},
                    idempotency_key=idempotency_key)
                self.store.adjust_balance(cur, lot_id, delta_balance=diff)
                adjustment_event_id = event.event_id
                adjusted = self.store.require_lot(cur, lot_id)
                if round(adjusted.balance + adjusted.held, 3) > 0 \
                        and adjusted.state == STATE_CLOSED:
                    self.store.set_lot_state(cur, lot_id, STATE_ACTIVE)
                else:
                    self._maybe_close(cur, adjusted)
            elif decision == DECISION_FREEZE and lot.state != STATE_FROZEN:
                reason = f"盘点差异 {diff} 待查（盘点单 {stocktake_id}）"
                event = self.store.append_event(
                    cur, EV_FROZEN, lot_id, 0.0, decided_by, now,
                    payload={"stocktake_id": stocktake_id, "diff": diff,
                             "responsible_party_id": responsible_party_id,
                             "reason": reason, "note": note},
                    idempotency_key=idempotency_key)
                self.store.set_lot_state(cur, lot_id, STATE_FROZEN, reason)
                adjustment_event_id = event.event_id
            values = {
                "stocktake_id": stocktake_id, "lot_id": lot_id,
                "system_quantity": system_quantity,
                "counted_quantity": counted_quantity, "diff": diff,
                "responsible_party_id": responsible_party_id,
                "decided_by": decided_by, "decision": decision,
                "note": note, "period_start": period_start,
                "period_end": period_end,
                "adjustment_event_id": adjustment_event_id,
                "created_at": now,
            }
            self.store.insert_stocktake(cur, values)
            row = self.store.get_stocktake(cur, stocktake_id)
            response = _stocktake_dict(row)
            response["lot"] = self.store.lot_as_dict(
                self.store.require_lot(cur, lot_id))
            self.store.put_reply(cur, idempotency_key, "stocktake", response, now)
            return response

    # ------------------------------------------------------------ 追溯

    def trace(self, lot_id: str) -> dict[str, Any]:
        """按批次追溯：批次现状、完整事实链、预占与盘点记录。"""
        with self.store.tx() as cur:
            lot = self.store.get_lot(cur, lot_id)
            if lot is None:
                raise KeyError(lot_id)
            events = self.store.events_related_to(cur, lot_id)
            result = self.store.lot_as_dict(lot)
            result["events"] = [_event_dict(e) for e in events]
            result["reservations"] = [
                _reservation_dict(r)
                for r in cur.execute(
                    "SELECT * FROM reservations WHERE lot_id=? ORDER BY created_at",
                    (lot_id,)).fetchall()]
            result["stocktakes"] = [
                _stocktake_dict(r) for r in self.store.list_stocktakes(cur, lot_id)]
            return result

    # ------------------------------------------------------------ 期间结算

    def settlement_snapshot(self, period_start: str, period_end: str,
                            snapshot_id: str = "",
                            persist: bool = True) -> dict[str, Any]:
        """生成 [period_start, period_end) 半开区间的期间结算快照。

        期初/期末数量通过只追加事件回放得到，因此即便投影异常也能独立
        核对“产量与实际出库为何不同”。
        """
        if period_start >= period_end:
            raise LedgerError("期间起点必须早于终点")
        snapshot_id = snapshot_id or _new_id("snap")
        now = self.clock.now_iso()
        with self.store.tx() as cur:
            existing = self.store.get_snapshot(cur, snapshot_id)
            if existing is not None:
                return existing["summary"]
            all_events = self.store.all_events(cur)
            opening = self._replay(
                [e for e in all_events if e.occurred_at < period_start])
            before_closing = self._replay(
                [e for e in all_events if e.occurred_at < period_end])

            flow_keys = ("harvested", "split_out", "split_in", "merged_out",
                         "merged_in", "returned", "reserved", "confirmed",
                         "released", "adjusted")
            lots_summary: dict[str, dict[str, Any]] = {}

            def flow_bucket(lot_id: str) -> dict[str, Any]:
                bucket = lots_summary.setdefault(lot_id, {
                    **{k: 0.0 for k in flow_keys},
                    "reservation_count": 0,
                })
                return bucket

            for event in all_events:
                if not (period_start <= event.occurred_at < period_end):
                    continue
                if not event.lot_id:
                    continue  # 快照标记等全局事件
                bucket = flow_bucket(event.lot_id)
                q = round(event.quantity, 3)
                etype = event.event_type
                if etype == EV_HARVESTED:
                    bucket["harvested"] = round(bucket["harvested"] + q, 3)
                elif etype == EV_SPLIT:
                    bucket["split_out"] = round(bucket["split_out"] + q, 3)
                elif etype == EV_SPLIT_CHILD:
                    bucket["split_in"] = round(bucket["split_in"] + q, 3)
                elif etype == EV_MERGE_SOURCE:
                    bucket["merged_out"] = round(bucket["merged_out"] + q, 3)
                elif etype == EV_MERGED_IN:
                    bucket["merged_in"] = round(bucket["merged_in"] + q, 3)
                elif etype == EV_RETURNED:
                    bucket["returned"] = round(bucket["returned"] + q, 3)
                elif etype == EV_RESERVED:
                    bucket["reserved"] = round(bucket["reserved"] + q, 3)
                    bucket["reservation_count"] += 1
                elif etype == EV_CONFIRMED:
                    bucket["confirmed"] = round(bucket["confirmed"] + q, 3)
                elif etype == EV_RELEASED:
                    bucket["released"] = round(bucket["released"] + q, 3)
                elif etype == EV_STOCK_ADJUST:
                    bucket["adjusted"] = round(bucket["adjusted"] + q, 3)

            stocktake_rows = [
                r for r in self.store.list_stocktakes(cur)
                if period_start <= r["created_at"] < period_end
            ]

            all_lot_ids = (set(opening) | set(before_closing)
                           | set(lots_summary))
            lots_out: dict[str, Any] = {}
            for lot_id in sorted(all_lot_ids):
                attr = self.store.get_lot(cur, lot_id)
                op = opening.get(lot_id, {"balance": 0.0, "held": 0.0})
                cl = before_closing.get(lot_id, {"balance": 0.0, "held": 0.0})
                bucket = lots_summary.get(lot_id, {
                    **{k: 0.0 for k in flow_keys}, "reservation_count": 0})
                entry = {
                    "zone": attr.zone if attr else "",
                    "variety": attr.variety if attr else "",
                    "quality_grade": attr.quality_grade if attr else "",
                    "state": attr.state if attr else "unknown",
                    "opening_available": round(op["balance"], 3),
                    "opening_reserved": round(op["held"], 3),
                    "closing_available": round(cl["balance"], 3),
                    "closing_reserved": round(cl["held"], 3),
                    "closing_on_hand": round(cl["balance"] + cl["held"], 3),
                }
                entry.update(bucket)
                lots_out[lot_id] = entry

            totals = {k: 0.0 for k in flow_keys}
            totals["reservation_count"] = 0
            for entry in lots_out.values():
                for key in flow_keys:
                    totals[key] = round(totals[key] + entry[key], 3)
                totals["reservation_count"] += entry["reservation_count"]
            totals["opening_on_hand"] = round(
                sum(e["opening_available"] + e["opening_reserved"]
                    for e in lots_out.values()), 3)
            totals["closing_on_hand"] = round(
                sum(e["closing_on_hand"] for e in lots_out.values()), 3)
            totals["closing_available"] = round(
                sum(e["closing_available"] for e in lots_out.values()), 3)
            totals["closing_reserved"] = round(
                sum(e["closing_reserved"] for e in lots_out.values()), 3)

            summary = {
                "snapshot_id": snapshot_id,
                "period_start": period_start,
                "period_end": period_end,
                "generated_at": now,
                "lots": lots_out,
                "totals": totals,
                "stocktakes": [_stocktake_dict(r) for r in stocktake_rows],
            }
            if persist:
                self.store.append_event(
                    cur, EV_SNAPSHOT, "", 0.0, "system", now,
                    payload={"snapshot_id": snapshot_id,
                             "period_start": period_start,
                             "period_end": period_end})
                self.store.insert_snapshot(
                    cur, snapshot_id, period_start, period_end, now, summary)
            return summary

    # ------------------------------------------------------------ 内部工具

    def _require_active(self, cur, lot_id: str) -> Lot:
        lot = self.store.require_lot(cur, lot_id)
        if lot.state == STATE_FROZEN:
            raise LedgerError(
                f"批次 {lot_id} 已因争议冻结（{lot.frozen_reason}），禁止变动")
        if lot.state == STATE_CLOSED:
            raise LedgerError(f"批次 {lot_id} 已结清关闭，禁止变动")
        return lot

    def _require_held(self, cur, reservation_id: str):
        row = self.store.get_reservation(cur, reservation_id)
        if row is None:
            raise LedgerError(f"预占 {reservation_id} 不存在")
        if row["status"] != _RESERVATION_HELD:
            raise LedgerError(
                f"预占 {reservation_id} 状态为 {row['status']}，无法释放")
        return row

    def _maybe_close(self, cur, lot: Lot) -> Lot:
        """余额与预占都归零的批次标记为已结清（仍可被退回事件重新激活前校验）。"""
        if lot.state == STATE_ACTIVE and round(lot.balance, 3) == 0.0 \
                and round(lot.held, 3) == 0.0:
            return self.store.set_lot_state(cur, lot.lot_id, STATE_CLOSED)
        return lot

    @staticmethod
    def _replay(events: Iterable) -> dict[str, dict[str, float]]:
        """从事件流回放每个批次的可用余额与预占额度。"""
        balances: dict[str, dict[str, float]] = {}

        def slot(lot_id: str) -> dict[str, float]:
            return balances.setdefault(lot_id, {"balance": 0.0, "held": 0.0})

        for event in events:
            if not event.lot_id:
                continue  # 快照标记等全局事件不属于任何批次
            current = slot(event.lot_id)
            q = round(event.quantity, 3)
            etype = event.event_type
            if etype in (EV_HARVESTED, EV_SPLIT_CHILD, EV_MERGED_IN,
                         EV_RETURNED):
                current["balance"] = round(current["balance"] + q, 3)
            elif etype in (EV_SPLIT, EV_MERGE_SOURCE):
                current["balance"] = round(current["balance"] - q, 3)
            elif etype == EV_RESERVED:
                current["balance"] = round(current["balance"] - q, 3)
                current["held"] = round(current["held"] + q, 3)
            elif etype == EV_CONFIRMED:
                current["held"] = round(current["held"] - q, 3)
            elif etype == EV_RELEASED:
                current["held"] = round(current["held"] - q, 3)
                current["balance"] = round(current["balance"] + q, 3)
            elif etype == EV_STOCK_ADJUST:
                current["balance"] = round(current["balance"] + q, 3)
        return balances


def _require_positive(value: float, name: str) -> float:
    value = float(value)
    if value <= 0:
        raise LedgerError(f"{name} 必须为正数，收到 {value}")
    return round(value, 3)
