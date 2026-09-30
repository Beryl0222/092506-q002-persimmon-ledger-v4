"""供进程内调用的轻量请求适配层。

请求形如 ``{"action": "...", ...参数}``，响应为 JSON 字符串。
业务错误（LedgerError）统一返回 ``{"ok": false, "error": {...}}``，
调用方可按 ``error.code`` 区分 not_found / insufficient_quota /
frozen_lot / conflict / invalid_operation。
"""
from __future__ import annotations

import json
from typing import Any

from .domain import LedgerError
from .service import Service


def _err(exc: Exception, code: str = "invalid_request") -> str:
    return json.dumps(
        {"ok": False, "error": {"code": getattr(exc, "code", code),
                                "message": str(getattr(exc, "message", exc))}},
        ensure_ascii=False,
    )


def handle(payload: str | dict[str, Any],
           service: Service | None = None) -> str:
    service = service or Service()
    body = json.loads(payload) if isinstance(payload, str) else payload
    action = body.get("action")
    try:
        result = _dispatch(service, action, body)
        return json.dumps(result, ensure_ascii=False, default=str)
    except LedgerError as exc:
        return _err(exc)
    except (KeyError, TypeError, ValueError) as exc:
        return _err(InvalidRequest(str(exc)))


class InvalidRequest(LedgerError):
    code = "invalid_request"


def _s(body: dict[str, Any], key: str, default: Any = None,
       required: bool = True) -> Any:
    if key not in body or body[key] is None:
        if required and default is None:
            raise InvalidRequest(f"缺少参数：{key}")
        return default
    return body[key]


def _dispatch(service: Service, action: str, b: dict[str, Any]) -> dict[str, Any]:
    if action == "health":
        return service.health()
    if action == "register":
        return service.register(str(b["record_id"]), str(b["owner_id"]))
    if action == "find":
        return service.find(str(b["record_id"]))

    if action == "weigh_in":
        return service.weigh_in(
            lot_id=str(_s(b, "lot_id")), zone=str(_s(b, "zone")),
            variety=str(_s(b, "variety")), weight=_s(b, "weight"),
            harvester_id=str(_s(b, "harvester_id")),
            quality_reviewer_id=str(_s(b, "quality_reviewer_id")),
            quality_grade=_s(b, "quality_grade", required=False),
            message_id=_s(b, "message_id", required=False))
    if action == "quality_review":
        return service.quality_review(
            lot_id=str(_s(b, "lot_id")),
            quality_grade=str(_s(b, "quality_grade")),
            reviewer_id=str(_s(b, "reviewer_id")))
    if action == "split":
        return service.split(
            source_id=str(_s(b, "source_id")),
            output_id=str(_s(b, "output_id")), weight=_s(b, "weight"),
            operator_id=str(_s(b, "operator_id")),
            message_id=_s(b, "message_id", required=False))
    if action == "merge":
        return service.merge(
            input_ids=[str(x) for x in _s(b, "input_ids")],
            output_id=str(_s(b, "output_id")),
            operator_id=str(_s(b, "operator_id")),
            weights=_s(b, "weights", required=False),
            message_id=_s(b, "message_id", required=False))
    if action == "return":
        return service.return_weight(
            lot_id=str(_s(b, "lot_id")), weight=_s(b, "weight"),
            operator_id=str(_s(b, "operator_id")),
            reason=str(_s(b, "reason", default="", required=False)),
            returned_to=str(_s(b, "returned_to", default="", required=False)),
            message_id=_s(b, "message_id", required=False))
    if action == "request_quota":
        return service.request_quota(
            reservation_id=str(_s(b, "reservation_id")),
            consumer_id=str(_s(b, "consumer_id")),
            items=list(_s(b, "items")),
            purpose=str(_s(b, "purpose", default="", required=False)),
            expires_at=_s(b, "expires_at", required=False),
            message_id=_s(b, "message_id", required=False))
    if action == "confirm_outbound":
        return service.confirm_outbound(
            reservation_id=str(_s(b, "reservation_id")),
            operator_id=str(_s(b, "operator_id")),
            message_id=_s(b, "message_id", required=False))
    if action == "release_quota":
        return service.release_quota(
            reservation_id=str(_s(b, "reservation_id")),
            operator_id=str(_s(b, "operator_id")),
            reason=str(_s(b, "reason", default="", required=False)),
            message_id=_s(b, "message_id", required=False))
    if action == "recover_pending":
        return service.recover_pending()
    if action == "sweep_expired":
        return service.sweep_expired()
    if action == "freeze":
        return service.freeze(
            lot_id=str(_s(b, "lot_id")), reason=str(_s(b, "reason")),
            operator_id=str(_s(b, "operator_id")),
            message_id=_s(b, "message_id", required=False))
    if action == "unfreeze":
        return service.unfreeze(
            lot_id=str(_s(b, "lot_id")), reason=str(_s(b, "reason")),
            operator_id=str(_s(b, "operator_id")),
            message_id=_s(b, "message_id", required=False))
    if action == "create_stocktake":
        return service.create_stocktake(
            stocktake_id=str(_s(b, "stocktake_id")),
            period=str(_s(b, "period")),
            responsible_id=str(_s(b, "responsible_id")),
            counts=dict(_s(b, "counts")),
            note=str(_s(b, "note", default="", required=False)),
            message_id=_s(b, "message_id", required=False))
    if action == "decide_stocktake":
        return service.decide_stocktake(
            stocktake_id=str(_s(b, "stocktake_id")),
            decision=str(_s(b, "decision")),
            decision_owner_id=str(_s(b, "decision_owner_id")),
            message_id=_s(b, "message_id", required=False))
    if action == "get_stocktake":
        return service.get_stocktake(str(_s(b, "stocktake_id")))
    if action == "get_lot":
        return service.get_lot(str(_s(b, "lot_id")))
    if action == "list_lots":
        return service.list_lots()
    if action == "trace":
        return service.trace(str(_s(b, "lot_id")))
    if action == "settlement":
        return service.generate_settlement(
            period=str(_s(b, "period")),
            snapshot_id=_s(b, "snapshot_id", required=False),
            note=str(_s(b, "note", default="", required=False)))
    if action == "get_settlement":
        return service.get_settlement(str(_s(b, "snapshot_id")))
    raise InvalidRequest(f"不支持的请求动作：{action!r}")
