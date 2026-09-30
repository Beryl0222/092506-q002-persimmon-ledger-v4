"""供进程内调用的轻量请求适配层。

请求/响应均为 JSON 文本。写动作可以带 ``idempotency_key``，重复的称重或
出库消息会返回首次结果。业务规则冲突统一回 ``{"error": ...}``，不向
调用方抛出，便于消息队列安全重试。
"""
import json
from typing import Any

from .domain import Clock, FixedClock, LedgerError
from .service import Service
from .store import Store


def build_service(db_path: str = ":memory:", fixed_now: str | None = None) -> Service:
    """构造服务实例；fixed_now 非空时使用固定时钟（主要用于测试/演练）。"""
    clock: Clock = FixedClock(fixed_now) if fixed_now else Clock()
    return Service(Store(db_path), clock)


def _pick(body: dict[str, Any], key: str, required: bool = False,
          default: Any = None) -> Any:
    if key in body and body[key] is not None:
        return body[key]
    if required:
        raise LedgerError(f"缺少必填字段: {key}")
    return default


def handle(payload: str, service: Service | None = None) -> str:
    service = service or Service()
    body = json.loads(payload)
    action = body.get("action")

    def response(result: Any) -> str:
        return json.dumps(result, ensure_ascii=False)

    try:
        if action == "health":
            return response(service.health())
        if action == "register":
            return response(service.register(str(body["record_id"]),
                                             str(body["owner_id"])))
        if action == "find":
            return response(service.find(str(body["record_id"])))

        if action == "harvest":
            return response(service.record_harvest(
                lot_id=_pick(body, "lot_id", True),
                zone=_pick(body, "zone", True),
                variety=_pick(body, "variety", True),
                harvester_id=_pick(body, "harvester_id", True),
                quality_checker_id=_pick(body, "quality_checker_id", True),
                quantity=float(_pick(body, "quantity", True)),
                quality_grade=_pick(body, "quality_grade", default=""),
                note=_pick(body, "note", default=""),
                idempotency_key=_pick(body, "idempotency_key", default="")))

        if action == "split":
            return response(service.split(
                lot_id=_pick(body, "lot_id", True),
                child_lot_id=_pick(body, "child_lot_id", True),
                quantity=float(_pick(body, "quantity", True)),
                quality_grade=_pick(body, "quality_grade", default=""),
                actor_id=_pick(body, "actor_id", default=""),
                note=_pick(body, "note", default=""),
                idempotency_key=_pick(body, "idempotency_key", default="")))

        if action == "merge":
            return response(service.merge(
                sources=_pick(body, "sources", True),
                target_lot_id=_pick(body, "target_lot_id", True),
                actor_id=_pick(body, "actor_id", True),
                quality_grade=_pick(body, "quality_grade", default=""),
                note=_pick(body, "note", default=""),
                idempotency_key=_pick(body, "idempotency_key", default="")))

        if action == "return":
            return response(service.return_to_lot(
                lot_id=_pick(body, "lot_id", True),
                quantity=float(_pick(body, "quantity", True)),
                actor_id=_pick(body, "actor_id", True),
                downstream_id=_pick(body, "downstream_id", default=""),
                reason=_pick(body, "reason", default=""),
                idempotency_key=_pick(body, "idempotency_key", default="")))

        if action == "freeze":
            return response(service.freeze(
                lot_id=_pick(body, "lot_id", True),
                actor_id=_pick(body, "actor_id", True),
                reason=_pick(body, "reason", True),
                idempotency_key=_pick(body, "idempotency_key", default="")))

        if action == "unfreeze":
            return response(service.unfreeze(
                lot_id=_pick(body, "lot_id", True),
                actor_id=_pick(body, "actor_id", True),
                resolution=_pick(body, "resolution", default=""),
                idempotency_key=_pick(body, "idempotency_key", default="")))

        if action == "reserve":
            return response(service.reserve(
                lot_id=_pick(body, "lot_id", True),
                downstream_id=_pick(body, "downstream_id", True),
                quantity=float(_pick(body, "quantity", True)),
                purpose=_pick(body, "purpose", default=""),
                ttl_seconds=int(_pick(body, "ttl_seconds", default=3600)),
                actor_id=_pick(body, "actor_id", default=""),
                reservation_id=_pick(body, "reservation_id", default=""),
                note=_pick(body, "note", default=""),
                idempotency_key=_pick(body, "idempotency_key", default="")))

        if action == "confirm_outbound":
            return response(service.confirm_outbound(
                reservation_id=_pick(body, "reservation_id", True),
                actor_id=_pick(body, "actor_id", default=""),
                idempotency_key=_pick(body, "idempotency_key", default="")))

        if action == "release":
            return response(service.release_reservation(
                reservation_id=_pick(body, "reservation_id", True),
                actor_id=_pick(body, "actor_id", True),
                reason=_pick(body, "reason", default=""),
                idempotency_key=_pick(body, "idempotency_key", default="")))

        if action == "recover_pending":
            return response(service.recover_pending(
                actor_id=_pick(body, "actor_id", default="system"),
                idempotency_key=_pick(body, "idempotency_key", default="")))

        if action == "stocktake":
            return response(service.record_stocktake(
                lot_id=_pick(body, "lot_id", True),
                counted_quantity=float(_pick(body, "counted_quantity", True)),
                decided_by=_pick(body, "decided_by", True),
                decision=_pick(body, "decision", True),
                responsible_party_id=_pick(body, "responsible_party_id",
                                           default=""),
                note=_pick(body, "note", default=""),
                stocktake_id=_pick(body, "stocktake_id", default=""),
                period_start=_pick(body, "period_start", default=""),
                period_end=_pick(body, "period_end", default=""),
                idempotency_key=_pick(body, "idempotency_key", default="")))

        if action == "trace":
            return response(service.trace(_pick(body, "lot_id", True)))

        if action == "settlement_snapshot":
            return response(service.settlement_snapshot(
                period_start=_pick(body, "period_start", True),
                period_end=_pick(body, "period_end", True),
                snapshot_id=_pick(body, "snapshot_id", default=""),
                persist=bool(_pick(body, "persist", default=True))))
    except (LedgerError, KeyError) as exc:
        return json.dumps(
            {"error": str(exc).strip("'\""), "action": action},
            ensure_ascii=False)
    except ValueError as exc:
        return json.dumps({"error": f"请求参数无效: {exc}", "action": action},
                          ensure_ascii=False)
    raise ValueError("不支持的请求动作")
