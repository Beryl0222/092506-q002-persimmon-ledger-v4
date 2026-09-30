"""批次账本的固定时钟测试。

覆盖需求中的关键场景：
- 只追加事实：拆分/合并/退回/复核都不改写原始称重事件；
- 幂等：重复称重、出库消息安全重放，同号异体会被拒绝；
- 跨月：10 月对 9 月做盘点与结算，账面按 9 月末事件重放，快照不可变；
- 并发：多线程同时扣减同一批次额度，不超发、不死锁；
- 恢复：预留已提交但出库未完成时“崩溃”，重开服务后可恢复并继续；
- 冻结争议批次、盘点差异关联责任人与处理决定。
"""
import json
import os
import tempfile
import threading
import unittest
from datetime import datetime, timezone

from persimmon_ledger.api import handle
from persimmon_ledger.domain import (
    Conflict, FixedClock, FrozenLot, InsufficientQuota, InvalidOperation,
    NotFound,
)
from persimmon_ledger.service import Service
from persimmon_ledger.store import Store


SEED = "2026-09-25T08:00:00+00:00"


def make_service(path: str = ":memory:", at: str = SEED):
    clock = FixedClock(at)
    store = Store(path)
    return Service(store, clock), store, clock


class 事实与额度测试(unittest.TestCase):
    def setUp(self):
        self.svc, self.store, self.clock = make_service()

    def test_称重建档记录树区品种采收人与质量复核(self):
        out = self.svc.weigh_in(
            "L-1", "北区A-03", "火柿", 120.5, "harv-zhao", "qc-qian",
            quality_grade="A", message_id="msg-1")
        lot = out["lot"]
        self.assertEqual(lot["zone"], "北区A-03")
        self.assertEqual(lot["variety"], "火柿")
        self.assertEqual(lot["harvester_id"], "harv-zhao")
        self.assertEqual(lot["quality_reviewer_id"], "qc-qian")
        self.assertEqual(lot["quality_grade"], "A")
        self.assertEqual(lot["stock_weight"], 120.5)
        self.assertEqual(lot["available_weight"], 120.5)

        # 复核改级只追加事件，不改重量事实
        self.svc.quality_review("L-1", "B", "qc-qian")
        lot = self.svc.get_lot("L-1")["lot"]
        self.assertEqual(lot["quality_grade"], "B")
        self.assertEqual(lot["inbound_weight"], 120.5)
        trace = self.svc.trace("L-1")
        self.assertEqual([e["event_type"] for e in trace["events"]],
                         ["weighed", "quality_reviewed"])

    def test_重复称重消息安全重放_同号异体被拒绝(self):
        kwargs = dict(lot_id="L-1", zone="z", variety="火柿", weight=10.0,
                      harvester_id="h", quality_reviewer_id="q")
        first = self.svc.weigh_in(message_id="dup-1", **kwargs)
        again = self.svc.weigh_in(message_id="dup-1", **kwargs)
        self.assertTrue(again["replayed"])
        self.assertEqual(again["event_id"], first["event_id"])
        self.assertEqual(len(self.svc.list_lots()["lots"]), 1)

        # 同一 message_id 携带不同请求体：必须拒绝而不是再记一笔
        with self.assertRaises(Conflict):
            self.svc.weigh_in(message_id="dup-1", lot_id="L-2", zone="z",
                              variety="火柿", weight=10.0,
                              harvester_id="h", quality_reviewer_id="q")

        # 无消息号时重复建档直接报错
        with self.assertRaises(InvalidOperation):
            self.svc.weigh_in(lot_id="L-1", zone="z", variety="火柿",
                              weight=10.0, harvester_id="h",
                              quality_reviewer_id="q")

    def test_拆分合并退回不改写原始事实且血缘可追溯(self):
        self.svc.weigh_in("T1", "z1", "火柿", 100.0, "h1", "q1",
                          message_id="w1")
        self.svc.split("T1", "T2", 30.0, "op1", message_id="sp1")
        self.svc.split("T1", "T3", 20.0, "op1", message_id="sp2")
        merged = self.svc.merge(["T2", "T3"], "T4", "op1")
        self.assertEqual(merged["total_weight"], 50.0)
        self.svc.return_weight("T4", 5.0, "op1", "体验活动剩余退回",
                               message_id="rt1")

        # 原始称重仍是 100，产量不因后续流转改变
        t1 = self.svc.get_lot("T1")["lot"]
        self.assertEqual(t1["inbound_weight"], 100.0)
        self.assertEqual(t1["stock_weight"], 50.0)
        t4 = self.svc.get_lot("T4")["lot"]
        self.assertEqual(t4["inbound_weight"], 0.0)
        self.assertEqual(t4["transfer_in_weight"], 50.0)
        self.assertEqual(t4["returned_weight"], 5.0)
        self.assertEqual(t4["stock_weight"], 45.0)

        trace4 = self.svc.trace("T4")
        parents = sorted(p["lot_id"] for p in trace4["lineage"]["parents"])
        self.assertEqual(parents, ["T2", "T3"])
        # 沿血缘回溯到原始树区批次
        self.assertEqual(
            sorted(p["lot_id"] for p in self.svc.trace("T2")["lineage"]["parents"]),
            ["T1"])

        # 事件流只追加：T1 上保留全部历史动作
        t1_types = [e["event_type"] for e in self.svc.trace("T1")["events"]]
        self.assertIn("weighed", t1_types)
        self.assertEqual(t1_types.count("lot_split"), 2)

    def test_拆分与退回不能超过可用额度(self):
        self.svc.weigh_in("Q1", "z", "火柿", 10.0, "h", "q")
        self.svc.request_quota("R1", "加工摊",
                               [{"lot_id": "Q1", "weight": 8.0}])
        with self.assertRaises(InsufficientQuota):
            self.svc.split("Q1", "Q2", 3.0, "op")
        with self.assertRaises(InsufficientQuota):
            self.svc.return_weight("Q1", 3.0, "op")
        self.svc.release_quota("R1", "op")
        # 释放后额度恢复，拆分成功
        self.svc.split("Q1", "Q2", 3.0, "op")
        self.assertEqual(self.svc.get_lot("Q1")["lot"]["available_weight"], 7.0)

    def test_下游必须先取得额度_重复出库消息安全重放(self):
        self.svc.weigh_in("D1", "z", "火柿", 40.0, "h", "q")
        with self.assertRaises(NotFound):
            self.svc.confirm_outbound("nope", "op")
        self.svc.request_quota("RR1", "文创摊",
                               [{"lot_id": "D1", "weight": 25.0}],
                               purpose="柿染材料")
        # held 状态：额度已扣可用、但还没出库
        self.assertEqual(self.svc.get_lot("D1")["lot"]["available_weight"], 15.0)
        self.assertEqual(self.svc.get_lot("D1")["lot"]["stock_weight"], 40.0)

        out = self.svc.confirm_outbound("RR1", "op", message_id="out-1")
        self.assertEqual(out["reservation"]["status"], "confirmed")
        replay = self.svc.confirm_outbound("RR1", "op", message_id="out-1")
        self.assertTrue(replay["replayed"])
        lot = self.svc.get_lot("D1")["lot"]
        self.assertEqual(lot["outbound_weight"], 25.0)
        self.assertEqual(lot["stock_weight"], 15.0)
        self.assertEqual(lot["reserved_weight"], 0.0)

        # 已完成的预留不能再次出库
        with self.assertRaises(InvalidOperation):
            self.svc.confirm_outbound("RR1", "op")

    def test_预留部分失败时整单回滚_不泄漏额度(self):
        self.svc.weigh_in("A1", "z", "火柿", 10.0, "h", "q")
        self.svc.weigh_in("A2", "z", "火柿", 10.0, "h", "q")
        with self.assertRaises(InsufficientQuota):
            self.svc.request_quota(
                "RX1", "加工摊",
                [{"lot_id": "A1", "weight": 6.0},
                 {"lot_id": "A2", "weight": 30.0}])
        self.assertEqual(self.svc.get_lot("A1")["lot"]["reserved_weight"], 0.0)
        self.assertEqual(self.svc.get_lot("A2")["lot"]["reserved_weight"], 0.0)
        self.assertIsNone(self.svc.store.get_reservation("RX1"))


class 冻结争议测试(unittest.TestCase):
    def setUp(self):
        self.svc, self.store, self.clock = make_service()
        self.svc.weigh_in("F1", "z", "火柿", 50.0, "h", "q")

    def test_冻结后禁止变动_解冻恢复(self):
        self.svc.freeze("F1", "等级争议待复核", "mgr-li")
        with self.assertRaises(FrozenLot):
            self.svc.split("F1", "F2", 1.0, "op")
        with self.assertRaises(FrozenLot):
            self.svc.return_weight("F1", 1.0, "op")
        with self.assertRaises(FrozenLot):
            self.svc.request_quota("RF", "摊", [{"lot_id": "F1", "weight": 1.0}])
        self.svc.unfreeze("F1", "复核完毕维持 A 级", "mgr-li")
        self.svc.split("F1", "F2", 1.0, "op")
        self.assertEqual(self.svc.get_lot("F2")["lot"]["stock_weight"], 1.0)

    def test_已held的批次被冻结_出库被拦截(self):
        self.svc.request_quota("RH", "摊", [{"lot_id": "F1", "weight": 5.0}])
        self.svc.freeze("F1", "争议", "mgr")
        with self.assertRaises(FrozenLot):
            self.svc.confirm_outbound("RH", "op")
        # 额度仍被占用，释放后才回到可用
        self.svc.unfreeze("F1", "解决", "mgr")
        self.svc.release_quota("RH", "op")
        self.assertEqual(self.svc.get_lot("F1")["lot"]["available_weight"], 50.0)


class 跨月盘点与结算测试(unittest.TestCase):
    def test_十月发生业务后_九月盘点仍按九月末账面(self):
        svc, store, clock = make_service(at="2026-09-28T08:00:00+00:00")
        svc.weigh_in("M1", "北1", "火柿", 100.0, "h1", "q1",
                     message_id="w-m1")
        svc.request_quota("RM1", "加工摊",
                          [{"lot_id": "M1", "weight": 60.0}])
        svc.confirm_outbound("RM1", "op", message_id="o-m1")  # 余 40

        # 时间推进到 10 月 5 日，再发生出库与新称重
        clock.advance(days=7)
        self.assertEqual(clock.now().strftime("%Y-%m"), "2026-10")
        svc.request_quota("RM2", "文创摊",
                          [{"lot_id": "M1", "weight": 10.0}])
        svc.confirm_outbound("RM2", "op", message_id="o-m2")
        svc.weigh_in("M2", "北2", "火柿", 200.0, "h2", "q1",
                     message_id="w-m2")

        # 对 9 月做盘点：M1 账面应为 9 月末的 40，而不是当前的 30
        st = svc.create_stocktake(
            "ST-9", "2026-09", "resp-wang",
            {"M1": 37.0}, note="火柿季九月盘点", message_id="st-msg-1")
        item = st["stocktake"]["items"][0]
        self.assertEqual(item["expected_weight"], 40.0)
        self.assertEqual(item["counted_weight"], 37.0)
        self.assertEqual(item["diff_weight"], -3.0)

        # 盘点重放
        again = svc.create_stocktake(
            "ST-9", "2026-09", "resp-wang", {"M1": 37.0},
            note="火柿季九月盘点", message_id="st-msg-1")
        self.assertTrue(again["replayed"])

        # 处理决定关联责任人（责任主管 + 决定人），盘亏调整账面
        decided = svc.decide_stocktake("ST-9", "adjust", "mgr-li",
                                       message_id="dec-1")
        self.assertEqual(decided["stocktake"]["responsible_id"], "resp-wang")
        self.assertEqual(decided["stocktake"]["decision_owner_id"], "mgr-li")
        self.assertEqual(decided["stocktake"]["decision"], "adjust")
        self.assertEqual(svc.get_lot("M1")["lot"]["adjustment_weight"], -3.0)
        self.assertEqual(svc.get_lot("M1")["lot"]["stock_weight"], 27.0)

        # reject：差异挂账不改数量
        st2 = svc.create_stocktake("ST-10a", "2026-10", "resp-wang",
                                   {"M2": 198.0})
        d2 = svc.decide_stocktake("ST-10a", "reject", "mgr-li")
        self.assertEqual(d2["stocktake"]["decision"], "reject")
        self.assertEqual(svc.get_lot("M2")["lot"]["stock_weight"], 200.0)

        # freeze：差异批次被冻结
        st3 = svc.create_stocktake("ST-10b", "2026-10", "resp-wang",
                                   {"M2": 190.0})
        svc.decide_stocktake("ST-10b", "freeze", "mgr-li")
        self.assertTrue(svc.get_lot("M2")["lot"]["frozen"])
        with self.assertRaises(FrozenLot):
            svc.return_weight("M2", 1.0, "op")

    def test_期间结算快照不可变且解释产量与出库差异(self):
        svc, store, clock = make_service(at="2026-09-29T08:00:00+00:00")
        svc.weigh_in("S1", "北1", "火柿", 100.0, "h1", "q1",
                     message_id="w1")
        svc.split("S1", "S2", 30.0, "op", message_id="sp1")  # 转体验活动备料
        svc.request_quota("RS1", "加工摊",
                          [{"lot_id": "S1", "weight": 40.0}])
        svc.confirm_outbound("RS1", "op", message_id="o1")
        svc.return_weight("S2", 5.0, "op", "体验剩余", message_id="r1")

        # 10 月 1 日出具 9 月结算
        clock.advance(days=2)
        snap = svc.generate_settlement("2026-09", note="九月月结")
        snap_id = snap["snapshot"]["snapshot_id"]
        self.assertEqual(snap_id, "settlement-2026-09")
        summary = snap["snapshot"]["summary"]
        self.assertEqual(summary["period_inbound"], 100.0)     # 原始产量
        self.assertEqual(summary["period_outbound"], 40.0)    # 实际出库
        self.assertEqual(summary["period_returned"], 5.0)
        # 拆分只在批次间转移，转入转出合计相抵
        self.assertEqual(summary["period_transfer_in"],
                         summary["period_transfer_out"])
        # 期末在库 = 100 - 40 - 5 = 55
        self.assertEqual(summary["ending_stock"], 55.0)
        self.assertEqual(summary["ending_reserved"], 0.0)

        # 10 月再录入事件与盘亏调整，重取快照内容不变
        svc.weigh_in("S3", "北3", "火柿", 500.0, "h2", "q1",
                     message_id="w2")
        svc.create_stocktake("ST9", "2026-09", "resp-w", {"S1": 52.0})
        svc.decide_stocktake("ST9", "adjust", "mgr-li")
        fetched = svc.get_settlement("settlement-2026-09")["snapshot"]
        self.assertEqual(fetched["summary"]["ending_stock"], 55.0)
        self.assertEqual(
            {e["lot_id"]: e["inbound_weight"] for e in fetched["entries"]},
            {"S1": 100.0, "S2": 0.0})
        # 重复生成返回同一份快照
        self.assertTrue(svc.generate_settlement("2026-09")["replayed"])
        # 10 月快照独立
        oct_summary = svc.generate_settlement("2026-10")["snapshot"]["summary"]
        self.assertEqual(oct_summary["period_inbound"], 500.0)


class 并发扣减测试(unittest.TestCase):
    def test_多线程并发领取不超发(self):
        svc, store, clock = make_service()
        svc.weigh_in("C1", "北1", "火柿", 100.0, "h1", "q1")

        results: list[bool] = []
        lock = threading.Lock()

        def worker(idx: int):
            try:
                svc.request_quota(
                    f"R-{idx}", f"摊-{idx}",
                    [{"lot_id": "C1", "weight": 10.0}])
                ok = True
            except InsufficientQuota:
                ok = False
            with lock:
                results.append(ok)

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        success = sum(results)
        self.assertEqual(success, 10)          # 100kg / 每次10kg
        self.assertEqual(len(results), 20)
        lot = svc.get_lot("C1")["lot"]
        self.assertEqual(lot["reserved_weight"], 100.0)
        self.assertEqual(lot["available_weight"], 0.0)
        # 再领必然失败
        with self.assertRaises(InsufficientQuota):
            svc.request_quota("R-late", "late",
                              [{"lot_id": "C1", "weight": 0.001}])

    def test_并发确认与释放最终账面一致(self):
        svc, store, clock = make_service()
        svc.weigh_in("C2", "z", "火柿", 50.0, "h", "q")
        for i in range(5):
            svc.request_quota(f"P-{i}", "摊",
                              [{"lot_id": "C2", "weight": 10.0}])

        def worker(idx: int, outbound: bool):
            if outbound:
                svc.confirm_outbound(f"P-{idx}", "op")
            else:
                svc.release_quota(f"P-{idx}", "op")

        threads = [threading.Thread(target=worker, args=(i, i % 2 == 0))
                   for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        lot = svc.get_lot("C2")["lot"]
        # 3 个出库（0,2,4）共 30，2 个释放（1,3）
        self.assertEqual(lot["outbound_weight"], 30.0)
        self.assertEqual(lot["reserved_weight"], 0.0)
        self.assertEqual(lot["stock_weight"], 20.0)
        self.assertEqual(lot["available_weight"], 20.0)


class 崩溃恢复测试(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "ledger.db")

    def tearDown(self):
        self.tmp.cleanup()

    def test_预留后崩溃_重开服务可恢复并完成出库(self):
        svc, store, clock = make_service(self.path)
        svc.weigh_in("K1", "z", "火柿", 80.0, "h", "q", message_id="wk")
        svc.request_quota("RK1", "加工摊",
                          [{"lot_id": "K1", "weight": 45.0}],
                          expires_at="2026-09-26T08:00:00+00:00")
        store.close()  # 模拟：已 held、未出库时进程退出

        # 用新的存储连接重开（相当于重启）
        clock2 = FixedClock("2026-09-25T12:00:00+00:00")
        svc2 = Service(Store(self.path), clock2)
        pending = svc2.recover_pending()
        self.assertEqual(pending["count"], 1)
        held = pending["held"][0]
        self.assertEqual(held["reservation_id"], "RK1")
        self.assertEqual(held["items"], [{"lot_id": "K1", "weight": 45.0}])
        # 恢复后继续完成出库，账面与事件连续
        svc2.confirm_outbound("RK1", "op", message_id="ok1")
        lot = svc2.get_lot("K1")["lot"]
        self.assertEqual(lot["stock_weight"], 35.0)
        self.assertEqual(lot["outbound_weight"], 45.0)
        self.assertEqual(svc2.recover_pending()["count"], 0)
        svc2.store.close()

    def test_过期held预留由扫场释放(self):
        svc, store, clock = make_service(self.path)
        svc.weigh_in("K2", "z", "火柿", 80.0, "h", "q")
        svc.request_quota("RK2", "加工摊",
                          [{"lot_id": "K2", "weight": 30.0}],
                          expires_at="2026-09-26T08:00:00+00:00")
        store.close()

        clock2 = FixedClock("2026-09-27T08:00:00+00:00")
        svc2 = Service(Store(self.path), clock2)
        swept = svc2.sweep_expired()
        self.assertEqual(swept["released"], ["RK2"])
        self.assertEqual(svc2.get_lot("K2")["lot"]["available_weight"], 80.0)
        self.assertEqual(svc2.recover_pending()["count"], 0)
        svc2.store.close()

    def test_事务中途异常不留下半成品(self):
        # 文件库上模拟第二批次不足导致整单回滚，重开后仍然干净
        svc, store, clock = make_service(self.path)
        svc.weigh_in("K3", "z", "火柿", 10.0, "h", "q")
        svc.weigh_in("K4", "z", "火柿", 10.0, "h", "q")
        with self.assertRaises(InsufficientQuota):
            svc.request_quota(
                "RK3", "摊",
                [{"lot_id": "K3", "weight": 9.0},
                 {"lot_id": "K4", "weight": 99.0}])
        store.close()

        svc2 = Service(Store(self.path), FixedClock("2026-09-25T09:00:00+00:00"))
        self.assertEqual(svc2.get_lot("K3")["lot"]["reserved_weight"], 0.0)
        self.assertEqual(svc2.recover_pending()["count"], 0)
        # 消息号也未被占用，可以原样重试成功
        svc2.request_quota(
            "RK3", "摊", [{"lot_id": "K3", "weight": 9.0}],
            message_id="hold-retry")
        self.assertEqual(svc2.get_lot("K3")["lot"]["available_weight"], 1.0)
        svc2.store.close()


class 接口适配测试(unittest.TestCase):
    def test_api_完整链路与错误码(self):
        service = Service(Store(), FixedClock("2026-09-30T08:00:00+00:00"))

        def call(body):
            return json.loads(handle(json.dumps(body, ensure_ascii=False), service))

        self.assertEqual(call({"action": "health"})["status"], "ok")
        w = call({"action": "weigh_in", "lot_id": "J1", "zone": "z",
                  "variety": "火柿", "weight": 20, "harvester_id": "h",
                  "quality_reviewer_id": "q", "message_id": "m1"})
        self.assertTrue(w["ok"])
        again = call({"action": "weigh_in", "lot_id": "J1", "zone": "z",
                      "variety": "火柿", "weight": 20, "harvester_id": "h",
                      "quality_reviewer_id": "q", "message_id": "m1"})
        self.assertTrue(again["replayed"])

        err = call({"action": "return", "lot_id": "missing", "weight": 1,
                    "operator_id": "op"})
        self.assertFalse(err["ok"])
        self.assertEqual(err["error"]["code"], "not_found")

        bad = call({"action": "split", "source_id": "J1",
                    "output_id": "J2", "weight": 999,
                    "operator_id": "op"})
        self.assertEqual(bad["error"]["code"], "insufficient_quota")

        missing_param = call({"action": "weigh_in", "lot_id": "X"})
        self.assertEqual(missing_param["error"]["code"], "invalid_request")


if __name__ == "__main__":
    unittest.main()
