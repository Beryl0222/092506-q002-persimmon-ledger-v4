"""采收批次与出库额度的本地持久化边界。

所有业务写入都在同一个 SQLite 事务里完成“追加事件 + 更新投影”，因此
不会出现事件已记而余额未改的半成品事务；服务重启后只需按投影与预占
记录恢复即可。连接只允许通过 ``tx()`` 开启立即事务，跨线程共享时由
进程内写锁串行化。
"""
import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .domain import (
    Event,
    Lot,
    Record,
)

Q = "?"  # 占位符，便于阅读


class Store:
    def __init__(self, path: str | Path = ":memory:") -> None:
        self.connection = sqlite3.connect(str(path), check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA busy_timeout=5000")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self._write_lock = threading.RLock()
        self._create_schema()

    def _create_schema(self) -> None:
        ddl = [
            """
            CREATE TABLE IF NOT EXISTS harvest_lot (
                record_id TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            -- 批次投影：当前余额与状态由事件流回放维护
            CREATE TABLE IF NOT EXISTS lots (
                lot_id TEXT PRIMARY KEY,
                zone TEXT NOT NULL,
                variety TEXT NOT NULL,
                harvester_id TEXT NOT NULL,
                quality_checker_id TEXT NOT NULL,
                quality_grade TEXT NOT NULL DEFAULT '',
                quantity REAL NOT NULL,
                balance REAL NOT NULL,
                held REAL NOT NULL DEFAULT 0,
                state TEXT NOT NULL,
                frozen_reason TEXT NOT NULL DEFAULT '',
                parent_lot_id TEXT NOT NULL DEFAULT '',
                created_event_id INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
            """,
            """
            -- 只追加事件流：一旦写入永不更新、永不删除
            CREATE TABLE IF NOT EXISTS events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                seq INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                lot_id TEXT NOT NULL,
                quantity REAL NOT NULL,
                actor_id TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                idempotency_key TEXT NOT NULL DEFAULT ''
            )
            """,
            "CREATE UNIQUE INDEX IF NOT EXISTS events_seq_ux ON events(seq)",
            "CREATE UNIQUE INDEX IF NOT EXISTS events_idem_ux ON events(idempotency_key) "
            "WHERE idempotency_key <> ''",
            "CREATE INDEX IF NOT EXISTS events_lot_ix ON events(lot_id, seq)",
            """
            -- 下游额度预占（先占额度，再确认出库）
            CREATE TABLE IF NOT EXISTS reservations (
                reservation_id TEXT PRIMARY KEY,
                lot_id TEXT NOT NULL,
                downstream_id TEXT NOT NULL,
                purpose TEXT NOT NULL DEFAULT '',
                quantity REAL NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                confirmed_at TEXT NOT NULL DEFAULT '',
                note TEXT NOT NULL DEFAULT ''
            )
            """,
            "CREATE INDEX IF NOT EXISTS reservations_lot_ix ON reservations(lot_id)",
            "CREATE INDEX IF NOT EXISTS reservations_status_ix ON reservations(status, expires_at)",
            """
            -- 盘点单：差异、责任人与处理决定
            CREATE TABLE IF NOT EXISTS stocktakes (
                stocktake_id TEXT PRIMARY KEY,
                lot_id TEXT NOT NULL,
                system_quantity REAL NOT NULL,
                counted_quantity REAL NOT NULL,
                diff REAL NOT NULL,
                responsible_party_id TEXT NOT NULL DEFAULT '',
                decided_by TEXT NOT NULL DEFAULT '',
                decision TEXT NOT NULL,
                note TEXT NOT NULL DEFAULT '',
                period_start TEXT NOT NULL DEFAULT '',
                period_end TEXT NOT NULL DEFAULT '',
                adjustment_event_id INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS stocktakes_lot_ix ON stocktakes(lot_id)",
            """
            -- 期间结算快照
            CREATE TABLE IF NOT EXISTS snapshots (
                snapshot_id TEXT PRIMARY KEY,
                period_start TEXT NOT NULL,
                period_end TEXT NOT NULL,
                created_at TEXT NOT NULL,
                summary_json TEXT NOT NULL
            )
            """,
            """
            -- 消息幂等日志：重复称重/出库消息安全重放为同一结果
            CREATE TABLE IF NOT EXISTS request_log (
                idempotency_key TEXT PRIMARY KEY,
                action TEXT NOT NULL,
                response_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
        ]
        with self._write_lock:
            for statement in ddl:
                self.connection.execute(statement)
            self.connection.commit()

    # ------------------------------------------------------------------ 事务

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Cursor]:
        """开启一个立即写事务；异常回滚，正常退出提交。"""
        with self._write_lock:
            self.connection.execute("BEGIN IMMEDIATE")
            cur = self.connection.cursor()
            try:
                yield cur
            except Exception:
                self.connection.rollback()
                raise
            else:
                self.connection.commit()
            finally:
                cur.close()

    # ------------------------------------------------------------ 幂等重放

    def get_reply(self, cur: sqlite3.Cursor, key: str) -> dict[str, Any] | None:
        if not key:
            return None
        row = cur.execute(
            "SELECT response_json FROM request_log WHERE idempotency_key=?", (key,)
        ).fetchone()
        return json.loads(row["response_json"]) if row else None

    def put_reply(self, cur: sqlite3.Cursor, key: str, action: str,
                  response: dict[str, Any], now_iso: str) -> None:
        if not key:
            return
        cur.execute(
            "INSERT INTO request_log(idempotency_key, action, response_json, created_at) "
            "VALUES(?,?,?,?)",
            (key, action, json.dumps(response, ensure_ascii=False), now_iso),
        )

    # ---------------------------------------------------------------- 事件

    def next_seq(self, cur: sqlite3.Cursor) -> int:
        row = cur.execute("SELECT COALESCE(MAX(seq), 0) + 1 AS next FROM events").fetchone()
        return int(row["next"])

    def append_event(self, cur: sqlite3.Cursor, event_type: str, lot_id: str,
                     quantity: float, actor_id: str, occurred_at: str,
                     payload: dict[str, Any] | None = None,
                     idempotency_key: str = "") -> Event:
        seq = self.next_seq(cur)
        cur.execute(
            "INSERT INTO events(seq, event_type, lot_id, quantity, actor_id, occurred_at, "
            "payload_json, idempotency_key) VALUES(?,?,?,?,?,?,?,?)",
            (seq, event_type, lot_id, float(quantity), actor_id, occurred_at,
             json.dumps(payload or {}, ensure_ascii=False), idempotency_key),
        )
        event_id = cur.lastrowid
        return Event(seq=seq, event_type=event_type, lot_id=lot_id,
                     quantity=float(quantity), actor_id=actor_id,
                     occurred_at=occurred_at, payload=payload or {},
                     idempotency_key=idempotency_key, event_id=event_id)

    def _row_to_event(self, row: sqlite3.Row) -> Event:
        return Event(
            seq=row["seq"], event_type=row["event_type"], lot_id=row["lot_id"],
            quantity=row["quantity"], actor_id=row["actor_id"],
            occurred_at=row["occurred_at"],
            payload=json.loads(row["payload_json"] or "{}"),
            idempotency_key=row["idempotency_key"], event_id=row["event_id"],
        )

    def events_for_lot(self, cur: sqlite3.Cursor, lot_id: str) -> list[Event]:
        rows = cur.execute(
            "SELECT * FROM events WHERE lot_id=? ORDER BY seq", (lot_id,)
        ).fetchall()
        return [self._row_to_event(r) for r in rows]

    def events_related_to(self, cur: sqlite3.Cursor, lot_id: str) -> list[Event]:
        """直接落在本批次，或在 payload 中指向本批次（拆分/合并）的事件。"""
        rows = cur.execute(
            "SELECT * FROM events WHERE lot_id=? "
            "OR json_extract(payload_json, '$.child_lot_id')=? "
            "OR json_extract(payload_json, '$.target_lot_id')=? ORDER BY seq",
            (lot_id, lot_id, lot_id),
        ).fetchall()
        return [self._row_to_event(r) for r in rows]

    def all_events(self, cur: sqlite3.Cursor) -> list[Event]:
        return [self._row_to_event(r)
                for r in cur.execute("SELECT * FROM events ORDER BY seq").fetchall()]

    def events_between(self, cur: sqlite3.Cursor, start: str, end: str) -> list[Event]:
        """半开区间 [start, end)，按 ISO 字符串比较（统一 UTC）。"""
        rows = cur.execute(
            "SELECT * FROM events WHERE occurred_at>=? AND occurred_at<? ORDER BY seq",
            (start, end),
        ).fetchall()
        return [self._row_to_event(r) for r in rows]

    # ---------------------------------------------------------------- 批次

    @staticmethod
    def _row_to_lot(row: sqlite3.Row) -> Lot:
        return Lot(
            lot_id=row["lot_id"], zone=row["zone"], variety=row["variety"],
            harvester_id=row["harvester_id"],
            quality_checker_id=row["quality_checker_id"],
            quality_grade=row["quality_grade"], quantity=row["quantity"],
            state=row["state"], balance=row["balance"], held=row["held"],
            parent_lot_id=row["parent_lot_id"],
            created_event_id=row["created_event_id"], created_at=row["created_at"],
            frozen_reason=row["frozen_reason"],
        )

    def lot_as_dict(self, lot: Lot) -> dict[str, Any]:
        return {
            "lot_id": lot.lot_id, "zone": lot.zone, "variety": lot.variety,
            "harvester_id": lot.harvester_id,
            "quality_checker_id": lot.quality_checker_id,
            "quality_grade": lot.quality_grade,
            "initial_quantity": round(lot.quantity, 3),
            "available_quantity": round(lot.balance, 3),
            "reserved_quantity": round(lot.held, 3),
            "on_hand_quantity": round(lot.balance + lot.held, 3),
            "state": lot.state, "frozen_reason": lot.frozen_reason,
            "parent_lot_id": lot.parent_lot_id,
            "created_event_id": lot.created_event_id, "created_at": lot.created_at,
        }

    def insert_lot(self, cur: sqlite3.Cursor, lot: Lot) -> None:
        cur.execute(
            "INSERT INTO lots(lot_id, zone, variety, harvester_id, quality_checker_id, "
            "quality_grade, quantity, balance, held, state, frozen_reason, parent_lot_id, "
            "created_event_id, created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (lot.lot_id, lot.zone, lot.variety, lot.harvester_id,
             lot.quality_checker_id, lot.quality_grade, float(lot.quantity),
             float(lot.balance), float(lot.held), lot.state, lot.frozen_reason,
             lot.parent_lot_id, lot.created_event_id, lot.created_at),
        )

    def get_lot(self, cur: sqlite3.Cursor, lot_id: str) -> Lot | None:
        row = cur.execute("SELECT * FROM lots WHERE lot_id=?", (lot_id,)).fetchone()
        return self._row_to_lot(row) if row else None

    def list_lots(self, cur: sqlite3.Cursor) -> list[Lot]:
        return [self._row_to_lot(r)
                for r in cur.execute("SELECT * FROM lots ORDER BY lot_id").fetchall()]

    def adjust_balance(self, cur: sqlite3.Cursor, lot_id: str,
                       delta_balance: float = 0.0, delta_held: float = 0.0) -> Lot:
        lot = self.require_lot(cur, lot_id)
        new_balance = round(lot.balance + delta_balance, 3)
        new_held = round(lot.held + delta_held, 3)
        cur.execute("UPDATE lots SET balance=?, held=? WHERE lot_id=?",
                    (new_balance, new_held, lot_id))
        return self.require_lot(cur, lot_id)

    def set_lot_state(self, cur: sqlite3.Cursor, lot_id: str, state: str,
                      reason: str = "") -> Lot:
        cur.execute("UPDATE lots SET state=?, frozen_reason=? WHERE lot_id=?",
                    (state, reason, lot_id))
        return self.require_lot(cur, lot_id)

    def require_lot(self, cur: sqlite3.Cursor, lot_id: str) -> Lot:
        lot = self.get_lot(cur, lot_id)
        if lot is None:
            raise KeyError(lot_id)
        return lot

    # ---------------------------------------------------------------- 预占

    def insert_reservation(self, cur: sqlite3.Cursor, values: dict[str, Any]) -> None:
        cur.execute(
            "INSERT INTO reservations(reservation_id, lot_id, downstream_id, purpose, "
            "quantity, status, created_at, expires_at, confirmed_at, note) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            (values["reservation_id"], values["lot_id"], values["downstream_id"],
             values.get("purpose", ""), float(values["quantity"]), values["status"],
             values["created_at"], values["expires_at"],
             values.get("confirmed_at", ""), values.get("note", "")),
        )

    def get_reservation(self, cur: sqlite3.Cursor, reservation_id: str) -> sqlite3.Row | None:
        return cur.execute(
            "SELECT * FROM reservations WHERE reservation_id=?", (reservation_id,)
        ).fetchone()

    def list_reservations(self, cur: sqlite3.Cursor, statuses: tuple[str, ...] = (),
                          only_expired_before: str = "") -> list[sqlite3.Row]:
        sql = "SELECT * FROM reservations"
        clauses: list[str] = []
        params: list[Any] = []
        if statuses:
            clauses.append("status IN (%s)" % ",".join(Q for _ in statuses))
            params.extend(statuses)
        if only_expired_before:
            clauses.append("expires_at<?")
            params.append(only_expired_before)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at, reservation_id"
        return list(cur.execute(sql, params).fetchall())

    def update_reservation(self, cur: sqlite3.Cursor, reservation_id: str,
                           status: str, confirmed_at: str = "") -> None:
        cur.execute(
            "UPDATE reservations SET status=?, confirmed_at=? WHERE reservation_id=?",
            (status, confirmed_at, reservation_id),
        )

    # ---------------------------------------------------------------- 盘点

    def insert_stocktake(self, cur: sqlite3.Cursor, values: dict[str, Any]) -> None:
        cur.execute(
            "INSERT INTO stocktakes(stocktake_id, lot_id, system_quantity, counted_quantity, "
            "diff, responsible_party_id, decided_by, decision, note, period_start, "
            "period_end, adjustment_event_id, created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (values["stocktake_id"], values["lot_id"],
             float(values["system_quantity"]), float(values["counted_quantity"]),
             float(values["diff"]), values.get("responsible_party_id", ""),
             values.get("decided_by", ""), values["decision"],
             values.get("note", ""), values.get("period_start", ""),
             values.get("period_end", ""), values.get("adjustment_event_id", 0),
             values["created_at"]),
        )

    def get_stocktake(self, cur: sqlite3.Cursor, stocktake_id: str) -> sqlite3.Row | None:
        return cur.execute(
            "SELECT * FROM stocktakes WHERE stocktake_id=?", (stocktake_id,)
        ).fetchone()

    def list_stocktakes(self, cur: sqlite3.Cursor, lot_id: str = "") -> list[sqlite3.Row]:
        if lot_id:
            rows = cur.execute(
                "SELECT * FROM stocktakes WHERE lot_id=? ORDER BY created_at", (lot_id,)
            ).fetchall()
        else:
            rows = cur.execute("SELECT * FROM stocktakes ORDER BY created_at").fetchall()
        return list(rows)

    # ---------------------------------------------------------------- 快照

    def insert_snapshot(self, cur: sqlite3.Cursor, snapshot_id: str, start: str,
                        end: str, created_at: str, summary: dict[str, Any]) -> None:
        cur.execute(
            "INSERT INTO snapshots(snapshot_id, period_start, period_end, created_at, "
            "summary_json) VALUES(?,?,?,?,?)",
            (snapshot_id, start, end, created_at,
             json.dumps(summary, ensure_ascii=False)),
        )

    def get_snapshot(self, cur: sqlite3.Cursor, snapshot_id: str) -> dict[str, Any] | None:
        row = cur.execute(
            "SELECT * FROM snapshots WHERE snapshot_id=?", (snapshot_id,)
        ).fetchone()
        if not row:
            return None
        return {
            "snapshot_id": row["snapshot_id"], "period_start": row["period_start"],
            "period_end": row["period_end"], "created_at": row["created_at"],
            "summary": json.loads(row["summary_json"]),
        }

    # -------------------------------------------------- 旧版登记（保持兼容）

    def save(self, record: Record) -> Record:
        from .domain import Clock
        value = record.with_timestamp(Clock().now_iso())
        with self._write_lock:
            self.connection.execute(
                "INSERT INTO harvest_lot(record_id, owner_id, state, created_at) "
                "VALUES(?,?,?,?)",
                (value.record_id, value.owner_id, value.state, value.created_at),
            )
            self.connection.commit()
        return value

    def get(self, record_id: str) -> Record | None:
        with self._write_lock:
            row = self.connection.execute(
                "SELECT record_id, owner_id, state, created_at FROM harvest_lot "
                "WHERE record_id=?", (record_id,),
            ).fetchone()
        return Record(**dict(row)) if row else None

    def close(self) -> None:
        with self._write_lock:
            self.connection.close()
