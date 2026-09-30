"""采收、分拣、领取、盘点全流程的行为测试。

所有跨时间的场景都由 FixedClock 驱动，确保跨月结算和 TTL 恢复的结果
确定可复现。
"""
import json
import os
import tempfile
import threading
import unittest

from persimmon_ledger.api import handle
from persimmon_ledger.domain import FixedClock, LedgerError, STATE_FROZEN
from persimmon_ledger.service import Service
from persimmon_ledger.store import Store

SEP = "2026-09-30T23:00:00+00:00"
OCT = "2026-10-01T01:00:00+00:00"


def make_service(start: str = "2026-09-28T08:00:00+00:00",
                 path: str = ":memory:") -> tuple[Service, FixedClock]:
    clock = FixedClock(start)
    return Service(Store(path), clock), clock


class 采收登记与幂等重放测试(unittest.TestCase):
    def setUp(self):
        self.service, self.clock = make_service()

    def test_登记树区品种采收人与质量复核(self):
        lot = self.service.record_harvest(
            "lot-1", "东区火柿林", "火柿", "pick-张", "qc-李",
            120.5, quality_grade="特级")
        self.assertEqual(lot["zone"], "东区火柿林")
        self.assertEqual(lot["variety"], "火柿")
        self.assertEqual(lot["harvester_id"], "pick-张")
        self.assertEqual(lot["quality_checker_id"], "qc-李")
        self.assertEqual(lot["available_quantity"], 120.5)
        self.assertEqual(lot["state"], "active")

    def test_重复称重消息返回同一结果且不重复入账(self):
        first = self.service.record_harvest(
            "lot-1", "东区", "火柿", "p1", "q1", 100,
            idempotency_key="weigh-msg-001")
        second = self.service.record_harvest(
            "lot-1", "东区", "火柿", "p1", "q1", 100,
            idempotency_key="weigh-msg-001")
        self.assertEqual(first["event_seq"], second["event_seq"])
        self.assertEqual(len(self.service.trace("lot-1")["events"]), 1)

    def test_同一批次号不同消息重复登记被拒绝(self):
        self.service.record_harvest("lot-1", "东", "火柿", "p", "q", 10)
        with self.assertRaises(LedgerError):
            self.service.record_harvest("lot-1", "东", "火柿", "p", "q", 10,
                                        idempotency_key="other-key")


class 拆分合并不改写原始事实测试(unittest.TestCase):
    def setUp(self):
        self.service, self.clock = make_service()
        self.service.record_harvest("lot-A", "一区", "火柿", "p1", "q1", 100,
                                    quality_grade="混级")

    def test_拆分只增加事实且原始采收数量不变(self):
        result = self.service.split("lot-A", "lot-A1", 40,
                                    quality_grade="特级", actor_id="sorter-1")
        self.assertEqual(result["parent"]["available_quantity"], 60)
        self.assertEqual(result["child"]["available_quantity"], 40)
        self.assertEqual(result["child"]["parent_lot_id"], "lot-A")
        chain = self.service.trace("lot-A")["events"]
        self.assertEqual([e["event_type"] for e in chain],
                         ["harvested", "split"])
        # 原始事实：采收事件的数量仍是 100，未被改写
        self.assertEqual(chain[0]["quantity"], 100)
        child_chain = self.service.trace("lot-A1")["events"]
        # 子批次溯源先看到父批次的拆出事实（payload 指向本批次），再是拆入
        self.assertEqual([e["event_type"] for e in child_chain],
                         ["split", "split_child"])
        self.assertEqual(child_chain[0]["payload"]["child_lot_id"], "lot-A1")

    def test_已预占额度不能被拆分(self):
        self.service.reserve("lot-A", "加工摊", 70, purpose="柿饼")
        with self.assertRaises(LedgerError):
            self.service.split("lot-A", "lot-A2", 50, actor_id="sorter-1")

    def test_合并多批次并保持事实链(self):
        self.service.record_harvest("lot-B", "一区", "火柿", "p2", "q2", 50)
        merged = self.service.merge(
            [{"lot_id": "lot-A", "quantity": 30},
             {"lot_id": "lot-B", "quantity": 20}],
            "lot-M", actor_id="sorter-1", quality_grade="一级")
        self.assertEqual(merged["total_quantity"], 50)
        self.assertEqual(self.service.trace("lot-A")["events"][-1]
                         ["payload"]["target_lot_id"], "lot-M")
        target_chain = self.service.trace("lot-M")["events"]
        # 目标批次溯源：先看到各来源并出，最后是本批次并入
        self.assertEqual(target_chain[0]["event_type"], "merge_source")
        self.assertEqual(target_chain[-1]["event_type"], "merged_in")

    def test_不同品种禁止合并(self):
        self.service.record_harvest("lot-C", "二区", "甜柿", "p3", "q3", 10)
        with self.assertRaises(LedgerError):
            self.service.merge([{"lot_id": "lot-A", "quantity": 10},
                                {"lot_id": "lot-C", "quantity": 10}],
                               "lot-X", actor_id="s1")

    def test_退回作为新事件入账而不改出库记录(self):
        rsv = self.service.reserve("lot-A", "体验活动", 20, purpose="采摘体验")
        self.service.confirm_outbound(rsv["reservation_id"], actor_id="staff-1")
        self.service.return_to_lot("lot-A", 5, actor_id="staff-2",
                                   downstream_id="体验活动", reason="碰伤退回",
                                   idempotency_key="ret-1")
        chain = self.service.trace("lot-A")["events"]
        self.assertEqual([e["event_type"] for e in chain],
                         ["harvested", "reserved", "confirmed", "returned"])
        self.assertEqual(chain[2]["quantity"], 20)  # 原出库事实不变
        self.assertEqual(self.service.trace("lot-A")["available_quantity"], 85)

    def test_拆分合并消息重放安全(self):
        first = self.service.split("lot-A", "lot-A1", 10, actor_id="s",
                                   idempotency_key="split-msg-1")
        second = self.service.split("lot-A", "lot-A1", 10, actor_id="s",
                                    idempotency_key="split-msg-1")
        self.assertEqual(first["child"]["lot_id"], second["child"]["lot_id"])
        self.assertEqual(len(self.service.trace("lot-A")["events"]), 2)


class 额度预占与出库测试(unittest.TestCase):
    def setUp(self):
        self.service, self.clock = make_service()
        self.service.record_harvest("lot-1", "南坡", "火柿", "p1", "q1", 100)

    def test_下游必须先取得额度才能出库(self):
        with self.assertRaises(LedgerError):
            self.service.confirm_outbound("不存在的预占")
        rsv = self.service.reserve("lot-1", "文创摊", 30, purpose="柿染材料")
        lot = rsv["lot"]
        self.assertEqual(lot["available_quantity"], 70)
        self.assertEqual(lot["reserved_quantity"], 30)
        confirmed = self.service.confirm_outbound(rsv["reservation_id"])
        self.assertEqual(confirmed["status"], "confirmed")
        self.assertEqual(confirmed["lot"]["available_quantity"], 70)
        self.assertEqual(confirmed["lot"]["reserved_quantity"], 0)

    def test_预占超额被拒绝(self):
        with self.assertRaises(LedgerError):
            self.service.reserve("lot-1", "加工摊", 120)

    def test_释放后额度可再次领取(self):
        rsv = self.service.reserve("lot-1", "加工摊", 40, purpose="预留")
        self.service.release_reservation(rsv["reservation_id"], actor_id="mgr",
                                         reason="计划取消")
        self.assertEqual(self.service.trace("lot-1")["available_quantity"], 100)
        rsv2 = self.service.reserve("lot-1", "冷藏库", 90, purpose="冷藏")
        self.assertEqual(rsv2["lot"]["available_quantity"], 10)

    def test_重复出库消息只扣一次(self):
        rsv = self.service.reserve("lot-1", "加工摊", 50)
        first = self.service.confirm_outbound(
            rsv["reservation_id"], idempotency_key="out-msg-9")
        second = self.service.confirm_outbound(
            rsv["reservation_id"], idempotency_key="out-msg-9")
        self.assertEqual(first["event_seq"], second["event_seq"])
        chain = self.service.trace("lot-1")["events"]
        self.assertEqual(len([e for e in chain if e["event_type"] == "confirmed"]),
                         1)
        self.assertEqual(self.service.trace("lot-1")["on_hand_quantity"], 50)

    def test_同一预占无幂等键的二次确认被拒绝(self):
        rsv = self.service.reserve("lot-1", "加工摊", 10)
        self.service.confirm_outbound(rsv["reservation_id"])
        with self.assertRaises(LedgerError):
            self.service.confirm_outbound(rsv["reservation_id"])


class 并发扣减测试(unittest.TestCase):
    def test_多线程并发预占不超卖(self):
        service, _ = make_service()
        service.record_harvest("lot-c", "西坡", "火柿", "p", "q", 100)
        results: list[tuple[int, object]] = []
        lock = threading.Lock()

        def worker(idx: int) -> None:
            try:
                rsv = service.reserve(
                    "lot-c", f"摊位-{idx}", 15,
                    reservation_id=f"rsv-{idx}",
                    idempotency_key=f"req-{idx}")
                outcome = rsv["reservation_id"]
            except LedgerError as exc:
                outcome = str(exc)
            with lock:
                results.append((idx, outcome))

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        success = [r for _, r in results if str(r).startswith("rsv-")]
        self.assertEqual(len(success), 6)  # 6*15=90，第 7 个起余额不足
        trace = service.trace("lot-c")
        self.assertEqual(trace["available_quantity"], 10)
        self.assertEqual(trace["reserved_quantity"], 90)
        # 每个成功请求有独立预占，合计 90，绝不超过 100
        self.assertLessEqual(
            trace["available_quantity"] + trace["reserved_quantity"], 100)

    def test_并发确认与释放后账目守恒(self):
        service, clock = make_service()
        service.record_harvest("lot-k", "北坡", "火柿", "p", "q", 60)
        rsvs = [service.reserve("lot-k", f"摊-{i}", 10,
                                reservation_id=f"r-{i}")
                for i in range(6)]

        def confirm(rsv_id: str) -> None:
            service.confirm_outbound(rsv_id)

        def release(rsv_id: str) -> None:
            service.release_reservation(rsv_id, actor_id="mgr")

        threads = []
        for i, rsv in enumerate(rsvs):
            target = confirm if i % 2 == 0 else release
            threads.append(threading.Thread(target=target,
                                            args=(rsv["reservation_id"],)))
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        lot = service.trace("lot-k")
        # 3 笔出库 30，3 笔释放回到可用 30，总量守恒
        self.assertEqual(lot["available_quantity"], 30)
        self.assertEqual(lot["reserved_quantity"], 0)


class 冻结争议批次测试(unittest.TestCase):
    def setUp(self):
        self.service, self.clock = make_service()
        self.service.record_harvest("lot-f", "争议林", "火柿", "p", "q", 80)

    def test_冻结后禁止一切变动(self):
        self.service.freeze("lot-f", actor_id="主管", reason="分级复核争议")
        lot = self.service.trace("lot-f")
        self.assertEqual(lot["state"], STATE_FROZEN)
        with self.assertRaises(LedgerError):
            self.service.split("lot-f", "lot-f1", 10, actor_id="s")
        with self.assertRaises(LedgerError):
            self.service.reserve("lot-f", "加工摊", 10)
        with self.assertRaises(LedgerError):
            self.service.return_to_lot("lot-f", 5, actor_id="x")
        with self.assertRaises(LedgerError):
            self.service.record_stocktake("lot-f", 70, decided_by="主管",
                                          decision="adjust")

    def test_解冻后操作恢复且冻结事实留痕(self):
        self.service.freeze("lot-f", actor_id="主管", reason="称重异议")
        self.service.unfreeze("lot-f", actor_id="主管",
                              resolution="复称确认无误")
        rsv = self.service.reserve("lot-f", "加工摊", 20)
        self.assertEqual(rsv["lot"]["available_quantity"], 60)
        chain = self.service.trace("lot-f")["events"]
        self.assertEqual(
            [e["event_type"] for e in chain
             if e["event_type"] in ("frozen", "unfrozen")],
            ["frozen", "unfrozen"])


class 盘点差异与责任决定测试(unittest.TestCase):
    def setUp(self):
        self.service, self.clock = make_service()
        self.service.record_harvest("lot-s", "仓库", "火柿", "p1", "q1", 100)

    def test_盘亏关联责任人并按决定调账(self):
        # 30 已出库，账面应剩 70，实盘 65，盘亏 5
        rsv = self.service.reserve("lot-s", "加工摊", 30)
        self.service.confirm_outbound(rsv["reservation_id"])
        result = self.service.record_stocktake(
            "lot-s", counted_quantity=65, decided_by="园长",
            decision="adjust", responsible_party_id="仓管-王",
            note="转运损耗待赔付", stocktake_id="stk-9月",
            period_start="2026-09-01T00:00:00+00:00",
            period_end=SEP)
        self.assertEqual(result["diff"], -5)
        self.assertEqual(result["responsible_party_id"], "仓管-王")
        self.assertEqual(result["lot"]["available_quantity"], 65)
        adjust_events = [e for e in self.service.trace("lot-s")["events"]
                         if e["event_type"] == "stock_adjust"]
        self.assertEqual(adjust_events[0]["payload"]["stocktake_id"],
                         "stk-9月")

    def test_争议差异冻结而不调账(self):
        result = self.service.record_stocktake(
            "lot-s", counted_quantity=80, decided_by="园长",
            decision="freeze", responsible_party_id="分拣-赵",
            note="数量对不上，先冻结查监控")
        self.assertEqual(result["diff"], -20)
        self.assertEqual(result["adjustment_event_id"] != 0, True)
        self.assertEqual(result["lot"]["state"], STATE_FROZEN)
        self.assertEqual(result["lot"]["available_quantity"], 100)

    def test_waive只登记差异与责任人不动账(self):
        result = self.service.record_stocktake(
            "lot-s", counted_quantity=99.5, decided_by="园长",
            decision="waive", responsible_party_id="体验组",
            note="允许的体验损耗")
        self.assertEqual(result["diff"], -0.5)
        self.assertEqual(result["lot"]["available_quantity"], 100)
        types = {e["event_type"] for e in
                 self.service.trace("lot-s")["events"]}
        self.assertNotIn("stock_adjust", types)

    def test_盘亏超过可用余额建议冻结(self):
        self.service.reserve("lot-s", "冷藏库", 90)  # 可用仅 10
        with self.assertRaises(LedgerError):
            self.service.record_stocktake("lot-s", counted_quantity=0,
                                          decided_by="园长", decision="adjust")


class 跨月结算快照测试(unittest.TestCase):
    def setUp(self):
        self.service, self.clock = make_service("2026-09-29T08:00:00+00:00")
        # 9 月：采收 100，加工摊领取并出库 30，体验预占 20（月末仍未出库）
        self.service.record_harvest("lot-m", "东坡", "火柿", "p1", "q1", 100,
                                    idempotency_key="h1")
        rsv = self.service.reserve("lot-m", "加工摊", 30, purpose="柿饼",
                                   reservation_id="rsv-sep-1")
        self.service.confirm_outbound(rsv["reservation_id"])
        self.service.reserve("lot-m", "体验活动", 20, purpose="国庆体验",
                             reservation_id="rsv-sep-2", ttl_seconds=7200)

    def test_九月快照解释产量与出库差异(self):
        snap = self.service.settlement_snapshot(
            "2026-09-01T00:00:00+00:00", SEP, snapshot_id="snap-sep")
        lot = snap["lots"]["lot-m"]
        self.assertEqual(lot["harvested"], 100)
        self.assertEqual(lot["confirmed"], 30)          # 已实际出库
        # 期间共申请过两笔预占 30+20=50（其中 30 当月出库）
        self.assertEqual(lot["reserved"], 50)
        self.assertEqual(lot["closing_reserved"], 20)   # 期末仍占未出库
        self.assertEqual(lot["closing_available"], 50)
        self.assertEqual(lot["closing_on_hand"], 70)
        # 产量 100 = 出库 30 + 预占 20 + 可用 50，差异可解释
        self.assertEqual(lot["harvested"] - lot["confirmed"],
                         lot["closing_on_hand"])
        self.assertEqual(snap["totals"]["confirmed"], 30)

    def test_跨月后预占过期回收与十月快照(self):
        self.service.settlement_snapshot("2026-09-01T00:00:00+00:00", SEP,
                                         snapshot_id="snap-sep")
        # 进入十月，体验活动的 20 预占早已过期
        self.clock.set(OCT)
        recovery = self.service.recover_pending(actor_id="system")
        self.assertEqual(recovery["recovered_count"], 1)
        self.assertEqual(recovery["recovered"][0]["quantity"], 20)
        self.assertEqual(self.service.trace("lot-m")["available_quantity"], 70)

        # 十月：冷藏库领取 40 并出库；退回 5
        rsv = self.service.reserve("lot-m", "冷藏库", 40,
                                   reservation_id="rsv-oct-1")
        self.service.confirm_outbound(rsv["reservation_id"])
        self.service.return_to_lot("lot-m", 5, actor_id="冷藏库",
                                   downstream_id="冷藏库",
                                   reason="抽检退回")
        oct_snap = self.service.settlement_snapshot(
            "2026-10-01T00:00:00+00:00", "2026-11-01T00:00:00+00:00",
            snapshot_id="snap-oct")
        lot = oct_snap["lots"]["lot-m"]
        # 十月期初承接（00:00，回收发生在 01:00）：可用 50，预占 20
        self.assertEqual(lot["opening_available"], 50)
        self.assertEqual(lot["opening_reserved"], 20)
        self.assertEqual(lot["released"], 20)
        self.assertEqual(lot["confirmed"], 40)
        self.assertEqual(lot["returned"], 5)
        self.assertEqual(lot["closing_available"], 35)

    def test_盘点单进入所在期间快照(self):
        self.service.record_stocktake(
            "lot-m", counted_quantity=69, decided_by="园长",
            decision="adjust", responsible_party_id="仓管-王",
            stocktake_id="stk-sep", period_end=SEP)
        snap = self.service.settlement_snapshot(
            "2026-09-01T00:00:00+00:00", SEP, snapshot_id="snap-sep2")
        self.assertEqual(len(snap["stocktakes"]), 1)
        self.assertEqual(snap["stocktakes"][0]["diff"], -1)
        self.assertEqual(snap["lots"]["lot-m"]["adjusted"], -1)

    def test_快照重放为同一结果(self):
        first = self.service.settlement_snapshot(
            "2026-09-01T00:00:00+00:00", SEP, snapshot_id="snap-idem")
        second = self.service.settlement_snapshot(
            "2026-09-01T00:00:00+00:00", SEP, snapshot_id="snap-idem")
        self.assertEqual(first["totals"], second["totals"])


class 重启后恢复未完成事务测试(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.path = self.tmp.name

    def tearDown(self):
        os.unlink(self.path)

    def test_重启后恢复held预占并回收过期额度(self):
        clock = FixedClock("2026-09-30T20:00:00+00:00")
        store = Store(self.path)
        service = Service(store, clock)
        service.record_harvest("lot-r", "恢复林", "火柿", "p", "q", 100)
        service.reserve("lot-r", "加工摊", 30,
                        reservation_id="rsv-valid", ttl_seconds=14400)
        service.reserve("lot-r", "体验活动", 20,
                        reservation_id="rsv-expired", ttl_seconds=60)

        # 模拟服务停机后重启：新连接、新服务实例，时间推进到预占之一过期
        store.close()
        clock2 = FixedClock("2026-09-30T23:00:00+00:00")
        service2 = Service(Store(self.path), clock2)
        lot = service2.trace("lot-r")
        self.assertEqual(lot["available_quantity"], 50)
        self.assertEqual(lot["reserved_quantity"], 50)

        recovery = service2.recover_pending(actor_id="system")
        recovered_ids = {r["reservation_id"] for r in recovery["recovered"]}
        self.assertEqual(recovered_ids, {"rsv-expired"})
        held_ids = {r["reservation_id"] for r in recovery["still_held"]}
        self.assertEqual(held_ids, {"rsv-valid"})
        lot = service2.trace("lot-r")
        self.assertEqual(lot["available_quantity"], 70)
        self.assertEqual(lot["reserved_quantity"], 30)

        # 未过期预占仍可正常出库
        result = service2.confirm_outbound("rsv-valid")
        self.assertEqual(result["status"], "confirmed")
        final = service2.trace("lot-r")
        self.assertEqual(final["available_quantity"], 70)
        self.assertEqual(final["reserved_quantity"], 0)
        self.assertEqual(final["on_hand_quantity"], 70)  # 采收 100 - 出库 30

    def test_重启后重复出库消息不重复扣减(self):
        clock = FixedClock("2026-09-30T20:00:00+00:00")
        service = Service(Store(self.path), clock)
        service.record_harvest("lot-d", "重放林", "火柿", "p", "q", 50)
        service.reserve("lot-d", "加工摊", 10, reservation_id="rsv-d")
        first = service.confirm_outbound("rsv-d", idempotency_key="msg-d")
        service.store.close()

        service2 = Service(Store(self.path),
                           FixedClock("2026-09-30T21:00:00+00:00"))
        second = service2.confirm_outbound("rsv-d", idempotency_key="msg-d")
        self.assertEqual(first["event_seq"], second["event_seq"])
        self.assertEqual(service2.trace("lot-d")["on_hand_quantity"], 40)


class 批次结清状态测试(unittest.TestCase):
    def setUp(self):
        self.service, self.clock = make_service()
        self.service.record_harvest("lot-z", "东坡", "火柿", "p", "q", 100)

    def test_整批拆出或全部出库后结清(self):
        result = self.service.split("lot-z", "lot-z1", 100, actor_id="s")
        self.assertEqual(result["parent"]["state"], "closed")
        self.assertEqual(result["child"]["state"], "active")

    def test_结清批次冻结再解冻保持结清(self):
        rsv = self.service.reserve("lot-z", "加工摊", 100,
                                   reservation_id="r1")
        self.service.confirm_outbound("r1")
        self.assertEqual(self.service.trace("lot-z")["state"], "closed")
        self.service.freeze("lot-z", actor_id="主管", reason="票据后补核查")
        self.assertEqual(self.service.trace("lot-z")["state"], STATE_FROZEN)
        self.service.unfreeze("lot-z", actor_id="主管", resolution="票据齐全")
        self.assertEqual(self.service.trace("lot-z")["state"], "closed")

    def test_退回让结清批次重新在账(self):
        rsv = self.service.reserve("lot-z", "体验活动", 100,
                                   reservation_id="r2")
        self.service.confirm_outbound("r2")
        self.assertEqual(self.service.trace("lot-z")["state"], "closed")
        self.service.return_to_lot("lot-z", 8, actor_id="staff",
                                   downstream_id="体验活动")
        lot = self.service.trace("lot-z")
        self.assertEqual(lot["state"], "active")
        self.assertEqual(lot["available_quantity"], 8)


class 接口适配层测试(unittest.TestCase):
    def test_全流程经JSON接口可用(self):
        service, clock = make_service()
        call = lambda obj: json.loads(handle(json.dumps(obj, ensure_ascii=False),
                                             service))
        self.assertEqual(call({"action": "health"})["status"], "ok")
        h = call({"action": "harvest", "lot_id": "L1", "zone": "一区",
                  "variety": "火柿", "harvester_id": "p", "quality_checker_id": "q",
                  "quantity": 100, "idempotency_key": "w1"})
        self.assertEqual(h["available_quantity"], 100)
        # 消息重放
        self.assertEqual(
            call({"action": "harvest", "lot_id": "L1", "zone": "一区",
                  "variety": "火柿", "harvester_id": "p",
                  "quality_checker_id": "q", "quantity": 100,
                  "idempotency_key": "w1"})["event_seq"], h["event_seq"])
        r = call({"action": "reserve", "lot_id": "L1",
                  "downstream_id": "加工摊", "quantity": 40})
        self.assertNotIn("error", r)
        out = call({"action": "confirm_outbound",
                    "reservation_id": r["reservation_id"],
                    "idempotency_key": "o1"})
        self.assertEqual(out["status"], "confirmed")
        self.assertEqual(
            call({"action": "confirm_outbound",
                  "reservation_id": r["reservation_id"],
                  "idempotency_key": "o1"})["event_seq"], out["event_seq"])
        tr = call({"action": "trace", "lot_id": "L1"})
        self.assertEqual(len(tr["events"]), 3)

    def test_规则错误返回error而不抛异常(self):
        service, _ = make_service()
        result = json.loads(handle(json.dumps(
            {"action": "reserve", "lot_id": "missing",
             "downstream_id": "x", "quantity": 5}), service))
        self.assertIn("error", result)


if __name__ == "__main__":
    unittest.main()
