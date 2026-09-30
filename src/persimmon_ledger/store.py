"""采收批次与出库额度的本地持久化边界。

事实层：
- ``event``/``event_ref``：只追加的领域事件流及其涉及批次的引用索引
  （subject/source/output），用于双向追溯与按时刻重放。
- ``idempotency``：称重、出库等消息的去重表，保存首次响应供安全重放。

派生层（可由事件重建）：
- ``lot``：每批次的数量台账（入库/转入/转出/出库/退回/调整/预留/冻结）。
- ``reservation``/``reservation_item``：下游领取的可用额度预留（held 即
  崩溃后仍可恢复的“未完成事务”）。
- ``stocktake(_item)``：盘点单及差异。
- ``settlement_snapshot(_entry)``：期间结算快照。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from pathlib import Path
from typing import Any, Iterable

from .domain import LotView, Record, qweight


SCHEMA = """
CREATE TABLE IF NOT EXISTS harvest_lot (
    record_id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event_log (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    lot_id TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_event_lot_time ON event_log(occurred_at, seq);

CREATE TABLE IF NOT EXISTS event_ref (
    event_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    lot_id TEXT NOT NULL,
    role TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    PRIMARY KEY (event_id, lot_id)
);
CREATE INDEX IF NOT EXISTS idx_event_ref_lot ON event_ref(lot_id, seq);

CREATE TABLE IF NOT EXISTS lot (
    lot_id TEXT PRIMARY KEY,
    zone TEXT NOT NULL,
    variety TEXT NOT NULL,
    harvester_id TEXT NOT NULL,
    quality_reviewer_id TEXT NOT NULL,
    quality_grade TEXT,
    inbound_weight REAL NOT NULL DEFAULT 0,
    transfer_in_weight REAL NOT NULL DEFAULT 0,
    transfer_out_weight REAL NOT NULL DEFAULT 0,
    outbound_weight REAL NOT NULL DEFAULT 0,
    returned_weight REAL NOT NULL DEFAULT 0,
    adjustment_weight REAL NOT NULL DEFAULT 0,
    reserved_weight REAL NOT NULL DEFAULT 0,
    frozen INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reservation (
    reservation_id TEXT PRIMARY KEY,
    consumer_id TEXT NOT NULL,
    purpose TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    expires_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reservation_status ON reservation(status);

CREATE TABLE IF NOT EXISTS reservation_item (
    reservation_id TEXT NOT NULL REFERENCES reservation(reservation_id),
    lot_id TEXT NOT NULL REFERENCES lot(lot_id),
    weight REAL NOT NULL,
    PRIMARY KEY (reservation_id, lot_id)
);

CREATE TABLE IF NOT EXISTS idempotency (
    message_id TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL,
    response_ref TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stocktake (
    stocktake_id TEXT PRIMARY KEY,
    period TEXT NOT NULL,
    responsible_id TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    decided_at TEXT,
    decision TEXT,
    decision_owner_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_stocktake_period ON stocktake(period);

CREATE TABLE IF NOT EXISTS stocktake_item (
    stocktake_id TEXT NOT NULL REFERENCES stocktake(stocktake_id),
    lot_id TEXT NOT NULL REFERENCES lot(lot_id),
    expected_weight REAL NOT NULL,
    counted_weight REAL NOT NULL,
    diff_weight REAL NOT NULL,
    PRIMARY KEY (stocktake_id, lot_id)
);

CREATE TABLE IF NOT EXISTS settlement_snapshot (
    snapshot_id TEXT PRIMARY KEY,
    period TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    from_seq INTEGER NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    summary_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_snapshot_period ON settlement_snapshot(period);

CREATE TABLE IF NOT EXISTS settlement_entry (
    snapshot_id TEXT NOT NULL REFERENCES settlement_snapshot(snapshot_id),
    lot_id TEXT NOT NULL,
    zone TEXT NOT NULL,
    variety TEXT NOT NULL,
    harvester_id TEXT NOT NULL,
    quality_reviewer_id TEXT NOT NULL,
    inbound_weight REAL NOT NULL,
    transfer_in_weight REAL NOT NULL,
    transfer_out_weight REAL NOT NULL,
    outbound_weight REAL NOT NULL,
    returned_weight REAL NOT NULL,
    adjustment_weight REAL NOT NULL,
    stock_weight REAL NOT NULL,
    reserved_weight REAL NOT NULL,
    PRIMARY KEY (snapshot_id, lot_id)
);
"""


class Store:
    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        if self.path == ":memory:":
            # 共享缓存的命名内存库：同一进程内多个连接（多线程）看到同一份数据，
            # keeper 连接保证库在连接全部关闭前不被回收。
            self._uri = f"file:persimmon_{uuid.uuid4().hex}?mode=memory&cache=shared"
            self._in_memory = True
        else:
            self._uri = self.path
            self._in_memory = False
        self._tls = threading.local()
        self._all_conns: list[sqlite3.Connection] = []
        self._conns_lock = threading.Lock()
        # SQLite 同一时刻只允许一个写事务；共享缓存的内存库更是表级锁、
        # 不等待 busy_timeout，因此所有写事务在应用层串行化。条件式 UPDATE
        # 仍是“余额不足不超发”的最终原子保障。
        self.write_lock = threading.RLock()
        self.keeper = self._new_connection()
        self.keeper.executescript(SCHEMA)
        self.keeper.commit()

    # -- 连接管理 ---------------------------------------------------------

    def _new_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._uri, uri=True, isolation_level=None,
                               timeout=10, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        if not self._in_memory:
            conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        conn.execute("PRAGMA synchronous=NORMAL")
        with self._conns_lock:
            self._all_conns.append(conn)
        return conn

    def conn(self) -> sqlite3.Connection:
        """返回当前线程的连接（autocommit 模式，事务显式开启）。"""
        local = self._tls
        if getattr(local, "conn", None) is None:
            local.conn = self._new_connection()
        return local.conn

    def begin(self, immediate: bool = False) -> None:
        self.conn().execute("BEGIN IMMEDIATE" if immediate else "BEGIN")

    def commit(self) -> None:
        self.conn().commit()

    def rollback(self) -> None:
        self.conn().rollback()

    def close(self) -> None:
        with self._conns_lock:
            conns = list(self._all_conns)
            self._all_conns.clear()
        for c in conns:
            c.close()

    # -- 早期登记入口（兼容基线） -----------------------------------------

    def save_record(self, record: Record) -> Record:
        c = self.conn()
        c.execute(
            "INSERT INTO harvest_lot(record_id, owner_id, state, created_at)"
            " VALUES(?,?,?,?)",
            (record.record_id, record.owner_id, record.state, record.created_at),
        )
        c.commit()
        return record

    def get_record(self, record_id: str) -> Record | None:
        row = self.conn().execute(
            "SELECT record_id, owner_id, state, created_at"
            " FROM harvest_lot WHERE record_id=?",
            (record_id,),
        ).fetchone()
        return Record(**dict(row)) if row else None

    # -- 幂等 -------------------------------------------------------------

    def get_idempotency(self, c: sqlite3.Connection, message_id: str):
        return c.execute(
            "SELECT request_hash, response_json FROM idempotency WHERE message_id=?",
            (message_id,),
        ).fetchone()

    def put_idempotency(self, c: sqlite3.Connection, message_id: str,
                        request_hash: str, response_ref: str,
                        response_json: str, created_at: str) -> None:
        c.execute(
            "INSERT INTO idempotency(message_id, request_hash, response_ref,"
            " response_json, created_at) VALUES(?,?,?,?,?)",
            (message_id, request_hash, response_ref, response_json, created_at),
        )

    # -- 事件 -------------------------------------------------------------

    def append_event(self, c: sqlite3.Connection, event_id: str, event_type: str,
                     lot_id: str, occurred_at: str, payload: dict[str, Any],
                     refs: Iterable[tuple[str, str]] = ()) -> int:
        """写入一条不可变事件并登记批次引用。返回事件序号。"""
        cur = c.execute(
            "INSERT INTO event_log(event_id, event_type, lot_id, occurred_at,"
            " payload) VALUES(?,?,?,?,?)",
            (event_id, event_type, lot_id, occurred_at,
             json.dumps(payload, ensure_ascii=False, sort_keys=True)),
        )
        seq = cur.lastrowid
        c.execute(
            "INSERT INTO event_ref(event_id, seq, lot_id, role, occurred_at)"
            " VALUES(?,?,?,?,?)",
            (event_id, seq, lot_id, "subject", occurred_at),
        )
        for ref_lot_id, role in refs:
            if ref_lot_id == lot_id:
                continue
            c.execute(
                "INSERT OR IGNORE INTO event_ref(event_id, seq, lot_id, role,"
                " occurred_at) VALUES(?,?,?,?,?)",
                (event_id, seq, ref_lot_id, role, occurred_at),
            )
        return seq

    def events_for_lot(self, lot_id: str) -> list[dict[str, Any]]:
        rows = self.conn().execute(
            "SELECT e.seq, e.event_id, e.event_type, e.lot_id, e.occurred_at,"
            " e.payload, er.role FROM event_ref er"
            " JOIN event_log e ON e.event_id = er.event_id"
            " WHERE er.lot_id=? ORDER BY e.seq, er.role",
            (lot_id,),
        ).fetchall()
        events: dict[str, dict[str, Any]] = {}
        for row in rows:
            item = events.get(row["event_id"])
            if item is None:
                item = {
                    "seq": row["seq"],
                    "event_id": row["event_id"],
                    "event_type": row["event_type"],
                    "lot_id": row["lot_id"],
                    "occurred_at": row["occurred_at"],
                    "payload": json.loads(row["payload"]),
                    "roles": [],
                }
                events[row["event_id"]] = item
            item["roles"].append({"lot_id": row["lot_id"], "role": row["role"]})
        return [events[k] for k in sorted(events, key=lambda k: events[k]["seq"])]

    def events_until(self, exclusive_time: str) -> list[dict[str, Any]]:
        rows = self.conn().execute(
            "SELECT seq, event_id, event_type, lot_id, occurred_at, payload"
            " FROM event_log WHERE occurred_at < ? ORDER BY seq",
            (exclusive_time,),
        ).fetchall()
        return [
            {
                "seq": r["seq"], "event_id": r["event_id"],
                "event_type": r["event_type"], "lot_id": r["lot_id"],
                "occurred_at": r["occurred_at"], "payload": json.loads(r["payload"]),
            }
            for r in rows
        ]

    def last_seq(self) -> int:
        row = self.conn().execute("SELECT COALESCE(MAX(seq),0) AS s FROM event_log").fetchone()
        return int(row["s"])

    # -- 批次台账 ----------------------------------------------------------

    def insert_lot(self, c: sqlite3.Connection, lot: LotView) -> None:
        c.execute(
            "INSERT INTO lot(lot_id, zone, variety, harvester_id,"
            " quality_reviewer_id, quality_grade, inbound_weight,"
            " transfer_in_weight, transfer_out_weight, outbound_weight,"
            " returned_weight, adjustment_weight, reserved_weight, frozen,"
            " created_at, updated_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (lot.lot_id, lot.zone, lot.variety, lot.harvester_id,
             lot.quality_reviewer_id, lot.quality_grade,
             qweight(lot.inbound_weight), qweight(lot.transfer_in_weight),
             qweight(lot.transfer_out_weight), qweight(lot.outbound_weight),
             qweight(lot.returned_weight), qweight(lot.adjustment_weight),
             qweight(lot.reserved_weight), 1 if lot.frozen else 0,
             lot.created_at, lot.updated_at),
        )

    def lot_exists(self, c: sqlite3.Connection, lot_id: str) -> bool:
        return c.execute("SELECT 1 FROM lot WHERE lot_id=?", (lot_id,)).fetchone() is not None

    def _row_to_lot(self, row: sqlite3.Row) -> LotView:
        data = dict(row)
        data["frozen"] = bool(data["frozen"])
        return LotView(**data)

    def get_lot(self, lot_id: str) -> LotView | None:
        row = self.conn().execute("SELECT * FROM lot WHERE lot_id=?", (lot_id,)).fetchone()
        return self._row_to_lot(row) if row else None

    def get_lot_lock(self, c: sqlite3.Connection, lot_id: str) -> LotView | None:
        row = c.execute("SELECT * FROM lot WHERE lot_id=?", (lot_id,)).fetchone()
        return self._row_to_lot(row) if row else None

    def list_lots(self) -> list[LotView]:
        rows = self.conn().execute("SELECT * FROM lot ORDER BY lot_id").fetchall()
        return [self._row_to_lot(r) for r in rows]

    def apply_lot_deltas(self, c: sqlite3.Connection, lot_id: str,
                         deltas: dict[str, float], updated_at: str,
                         quality_grade: str | None = -1,  # type: ignore[assignment]
                         frozen: bool | None = None) -> None:
        fields = [
            "inbound_weight", "transfer_in_weight", "transfer_out_weight",
            "outbound_weight", "returned_weight", "adjustment_weight",
            "reserved_weight",
        ]
        assignments = ", ".join(f"{f} = {f} + ?" for f in fields if deltas.get(f))
        params: list[Any] = [qweight(deltas[f]) for f in fields if deltas.get(f)]
        sets = []
        if assignments:
            sets.append(assignments)
        if quality_grade != -1:
            sets.append("quality_grade = ?")
            params.append(quality_grade)
        if frozen is not None:
            sets.append("frozen = ?")
            params.append(1 if frozen else 0)
        sets.append("updated_at = ?")
        params.append(updated_at)
        params.append(lot_id)
        c.execute(f"UPDATE lot SET {', '.join(sets)} WHERE lot_id=?", params)

    def try_reserve(self, c: sqlite3.Connection, lot_id: str, weight: float,
                    updated_at: str) -> bool:
        """条件式原子扣减可用额度；余额不足或批次冻结时返回 False。"""
        cur = c.execute(
            "UPDATE lot SET reserved_weight = reserved_weight + ?,"
            " updated_at = ? WHERE lot_id = ? AND frozen = 0"
            " AND (inbound_weight + transfer_in_weight + adjustment_weight"
            "      - outbound_weight - returned_weight - transfer_out_weight"
            "      - reserved_weight) >= ? - 1e-6",
            (qweight(weight), updated_at, lot_id, qweight(weight)),
        )
        return cur.rowcount == 1

    def unreserve(self, c: sqlite3.Connection, lot_id: str, weight: float,
                  updated_at: str) -> None:
        c.execute(
            "UPDATE lot SET reserved_weight = reserved_weight - ?,"
            " updated_at = ? WHERE lot_id=?",
            (qweight(weight), updated_at, lot_id),
        )

    # -- 预留单 ------------------------------------------------------------

    def insert_reservation(self, c: sqlite3.Connection, reservation_id: str,
                           consumer_id: str, purpose: str, status: str,
                           items: list[tuple[str, float]], created_at: str,
                           expires_at: str | None = None) -> None:
        c.execute(
            "INSERT INTO reservation(reservation_id, consumer_id, purpose,"
            " status, expires_at, created_at, updated_at)"
            " VALUES(?,?,?,?,?,?,?)",
            (reservation_id, consumer_id, purpose, status, expires_at,
             created_at, created_at),
        )
        c.executemany(
            "INSERT INTO reservation_item(reservation_id, lot_id, weight)"
            " VALUES(?,?,?)",
            [(reservation_id, lot_id, qweight(w)) for lot_id, w in items],
        )

    def get_reservation(self, reservation_id: str) -> dict[str, Any] | None:
        c = self.conn()
        row = c.execute(
            "SELECT * FROM reservation WHERE reservation_id=?",
            (reservation_id,),
        ).fetchone()
        if not row:
            return None
        items = c.execute(
            "SELECT lot_id, weight FROM reservation_item WHERE reservation_id=?"
            " ORDER BY lot_id",
            (reservation_id,),
        ).fetchall()
        result = dict(row)
        result["items"] = [{"lot_id": r["lot_id"], "weight": qweight(r["weight"])}
                           for r in items]
        return result

    def list_reservations_by_status(self, status: str) -> list[dict[str, Any]]:
        out = []
        rows = self.conn().execute(
            "SELECT reservation_id FROM reservation WHERE status=?"
            " ORDER BY created_at",
            (status,),
        ).fetchall()
        for row in rows:
            loaded = self.get_reservation(row["reservation_id"])
            if loaded:
                out.append(loaded)
        return out

    def update_reservation_status(self, c: sqlite3.Connection, reservation_id: str,
                                  status: str, updated_at: str) -> None:
        c.execute(
            "UPDATE reservation SET status=?, updated_at=? WHERE reservation_id=?",
            (status, updated_at, reservation_id),
        )

    # -- 盘点 --------------------------------------------------------------

    def insert_stocktake(self, c: sqlite3.Connection, stocktake_id: str,
                         period: str, responsible_id: str, note: str,
                         status: str, created_at: str,
                         items: list[tuple[str, float, float, float]]) -> None:
        c.execute(
            "INSERT INTO stocktake(stocktake_id, period, responsible_id, note,"
            " status, created_at) VALUES(?,?,?,?,?,?)",
            (stocktake_id, period, responsible_id, note, status, created_at),
        )
        c.executemany(
            "INSERT INTO stocktake_item(stocktake_id, lot_id, expected_weight,"
            " counted_weight, diff_weight) VALUES(?,?,?,?,?)",
            [(stocktake_id, lot_id, qweight(exp), qweight(cnt), qweight(diff))
             for lot_id, exp, cnt, diff in items],
        )

    def get_stocktake(self, stocktake_id: str) -> dict[str, Any] | None:
        c = self.conn()
        row = c.execute(
            "SELECT * FROM stocktake WHERE stocktake_id=?",
            (stocktake_id,),
        ).fetchone()
        if not row:
            return None
        items = c.execute(
            "SELECT lot_id, expected_weight, counted_weight, diff_weight"
            " FROM stocktake_item WHERE stocktake_id=? ORDER BY lot_id",
            (stocktake_id,),
        ).fetchall()
        result = dict(row)
        result["items"] = [dict(r) for r in items]
        return result

    def decide_stocktake(self, c: sqlite3.Connection, stocktake_id: str,
                         decision: str, decision_owner_id: str,
                         decided_at: str) -> None:
        c.execute(
            "UPDATE stocktake SET status='decided', decision=?,"
            " decision_owner_id=?, decided_at=? WHERE stocktake_id=?",
            (decision, decision_owner_id, decided_at, stocktake_id),
        )

    # -- 结算快照 ----------------------------------------------------------

    def snapshot_exists(self, snapshot_id: str) -> bool:
        return self.conn().execute(
            "SELECT 1 FROM settlement_snapshot WHERE snapshot_id=?",
            (snapshot_id,),
        ).fetchone() is not None

    def insert_snapshot(self, c: sqlite3.Connection, snapshot_id: str,
                        period: str, generated_at: str, from_seq: int,
                        note: str, summary: dict[str, Any],
                        entries: list[dict[str, Any]]) -> None:
        c.execute(
            "INSERT INTO settlement_snapshot(snapshot_id, period, generated_at,"
            " from_seq, note, summary_json) VALUES(?,?,?,?,?,?)",
            (snapshot_id, period, generated_at, from_seq, note,
             json.dumps(summary, ensure_ascii=False, sort_keys=True)),
        )
        c.executemany(
            "INSERT INTO settlement_entry(snapshot_id, lot_id, zone, variety,"
            " harvester_id, quality_reviewer_id, inbound_weight,"
            " transfer_in_weight, transfer_out_weight, outbound_weight,"
            " returned_weight, adjustment_weight, stock_weight, reserved_weight)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (
                    snapshot_id, e["lot_id"], e["zone"], e["variety"],
                    e["harvester_id"], e["quality_reviewer_id"],
                    qweight(e["inbound_weight"]), qweight(e["transfer_in_weight"]),
                    qweight(e["transfer_out_weight"]), qweight(e["outbound_weight"]),
                    qweight(e["returned_weight"]), qweight(e["adjustment_weight"]),
                    qweight(e["stock_weight"]), qweight(e["reserved_weight"]),
                )
                for e in entries
            ],
        )

    def get_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        c = self.conn()
        row = c.execute(
            "SELECT * FROM settlement_snapshot WHERE snapshot_id=?",
            (snapshot_id,),
        ).fetchone()
        if not row:
            return None
        items = c.execute(
            "SELECT * FROM settlement_entry WHERE snapshot_id=? ORDER BY lot_id",
            (snapshot_id,),
        ).fetchall()
        result = dict(row)
        result["summary"] = json.loads(result.pop("summary_json"))
        result["entries"] = [dict(r) for r in items]
        return result
