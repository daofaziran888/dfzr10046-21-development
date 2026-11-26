"""影响链工作队列测试：分叉后汇合链的并发领取、来路撤销与排期冲突裁决。"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from app.pipeline.impact_chain_queue import (
    CONFLICT_INFLIGHT,
    RESOLVE_COMPLETE_LAST,
    RESOLVE_WITHDRAW,
    ImpactChainQueue,
)

# 分叉后汇合的链：A 分叉到 B/C，B/C 在 D 汇合
FORK_JOIN_EDGES = [
    {"upstream": "A", "downstream": "B"},
    {"upstream": "A", "downstream": "C"},
    {"upstream": "B", "downstream": "D"},
    {"upstream": "C", "downstream": "D"},
]
PATH_VIA_B = ["A", "B", "D"]
PATH_VIA_C = ["A", "C", "D"]


def build_queue() -> ImpactChainQueue:
    return ImpactChainQueue()


def node_of(queue: ImpactChainQueue, node: str) -> dict:
    snap = queue.queue_snapshot()
    for entry in snap["nodes"]:
        if entry["node"] == node:
            return entry
    raise AssertionError(f"节点 {node} 不在队列中")


class TestImpactChainSelection:
    """下游处置筛选：从上游对象枚举实际影响链。"""

    def test_fork_join_yields_two_chains(self):
        queue = build_queue()
        chains = queue.select_impact_chains("A", FORK_JOIN_EDGES)
        assert sorted("->".join(c) for c in chains) == ["A->B->D", "A->C->D"]

    def test_unknown_upstream_has_no_chain(self):
        queue = build_queue()
        assert queue.select_impact_chains("X", FORK_JOIN_EDGES) == []

    def test_cycle_does_not_hang(self):
        edges = [
            {"upstream": "A", "downstream": "B"},
            {"upstream": "B", "downstream": "A"},
        ]
        chains = ImpactChainQueue.select_impact_chains("A", edges)
        assert chains == [["A", "B"]]


class TestConcurrentClaim:
    """沿两条路径并发提交领取时，汇合节点只生成一份待办。"""

    def test_concurrent_claim_single_todo_at_join(self):
        queue = build_queue()
        chains = queue.select_impact_chains("A", FORK_JOIN_EDGES)
        assert len(chains) == 2
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda p: queue.claim_chain(p), chains))
        assert all(ok for ok, _, _ in results)

        snap = queue.queue_snapshot()
        # 队列的待办数量：B、C、D 各一份，汇合节点 D 不重复
        assert snap["todo_count"] == 3
        d_todos = [t for t in snap["todos"] if t["node"] == "D"]
        assert len(d_todos) == 1
        # D 同时登记了两条来路
        d_entry = node_of(queue, "D")
        assert set(d_entry["claims"]) == {"A->B->D", "A->C->D"}
        assert d_entry["status"] == "已领取"
        # 重跑页得到的节点不重复
        rerun_nodes = [t["node"] for t in queue.rerun_todos()]
        assert sorted(rerun_nodes) == ["B", "C", "D"]

    def test_repeat_claim_same_path_is_idempotent(self):
        queue = build_queue()
        queue.claim_chain(PATH_VIA_B)
        ok, _, _ = queue.claim_chain(PATH_VIA_B)
        assert ok
        snap = queue.queue_snapshot()
        assert snap["todo_count"] == 2  # B、D 各一份，没有新增

    def test_claim_too_short_chain_rejected(self):
        queue = build_queue()
        ok, msg, _ = queue.claim_chain(["A"])
        assert not ok
        assert "至少" in msg


class TestRevokeBranch:
    """撤去一支后继续路径说明；撤销最后一支后停止新的领取。"""

    def test_revoke_one_branch_shows_valid_path(self):
        queue = build_queue()
        queue.claim_chain(PATH_VIA_B)
        queue.claim_chain(PATH_VIA_C)
        ok, _, detail = queue.revoke_chain(PATH_VIA_C)
        assert ok

        # 汇合节点 D 还有效来路：已领取任务说明从 A->B->D 继续
        d_entry = node_of(queue, "D")
        assert d_entry["status"] == "已领取"
        assert d_entry["continue_path"] == PATH_VIA_B
        d_events = [e for e in detail["events"] if e["node"] == "D"]
        assert d_events and d_events[0]["event"] == "继续有效路径"
        assert d_events[0]["path"] == PATH_VIA_B
        todo = queue.todo_for_node("D")
        assert todo["active_path"] == PATH_VIA_B
        assert todo["source_paths"] == ["A->B->D"]

        # C 失去唯一来路：止领，且在途排期（待排期）冲突挂起
        c_entry = node_of(queue, "C")
        assert c_entry["status"] == "已止领"
        assert c_entry["conflict"] == CONFLICT_INFLIGHT
        assert c_entry["last_valid_path"] == PATH_VIA_C

    def test_revoke_last_branch_stops_new_claims(self):
        queue = build_queue()
        queue.claim_chain(PATH_VIA_B)
        queue.claim_chain(PATH_VIA_C)
        queue.revoke_chain(PATH_VIA_C)
        ok, _, detail = queue.revoke_chain(PATH_VIA_B)
        assert ok

        # D 的全部来路撤销：止领，待办待排期 → 在途排期冲突
        d_entry = node_of(queue, "D")
        assert d_entry["status"] == "已止领"
        assert d_entry["conflict"] == CONFLICT_INFLIGHT
        assert d_entry["last_valid_path"] == PATH_VIA_B
        todo = queue.todo_for_node("D")
        assert todo["status"] == "待排期" and todo["held"]

        # 停止新的领取：沿新路径提交被拒不生成新待办
        ok, msg, _ = queue.claim_chain(["A", "E", "D"])
        d_todos = [t for t in queue.queue_snapshot()["todos"] if t["node"] == "D"]
        assert len(d_todos) == 1
        d_claim = [r for r in queue.queue_snapshot()["nodes"] if r["node"] == "D"][0]
        assert d_claim["status"] == "已止领"

    def test_revoke_unknown_chain_rejected(self):
        queue = build_queue()
        ok, msg, _ = queue.revoke_chain(PATH_VIA_B)
        assert not ok
        assert "没有可撤销" in msg


class TestInflightConflictResolution:
    """在途排期冲突：两边确认一个决定，不能启动两份任务。"""

    def build_conflict(self) -> ImpactChainQueue:
        queue = build_queue()
        queue.claim_chain(PATH_VIA_B)
        queue.claim_chain(PATH_VIA_C)
        queue.revoke_chain(PATH_VIA_C)
        queue.revoke_chain(PATH_VIA_B)  # D 止领 + 在途排期冲突
        return queue

    def test_complete_with_last_valid_path(self):
        queue = self.build_conflict()
        d_entry = node_of(queue, "D")
        assert d_entry["conflict"] == CONFLICT_INFLIGHT

        ok, msg = queue.resolve_conflict("D", RESOLVE_COMPLETE_LAST)
        assert ok
        assert "A->B->D" in msg
        # 按最后一次有效路径完成这轮排期：同一份待办继续开跑
        todo = queue.todo_for_node("D")
        assert todo["status"] == "重跑中"
        assert todo["active_path"] == PATH_VIA_B
        assert not todo["held"]
        # 不启动两份任务：D 的在途待办仍只有一份
        d_todos = [t for t in queue.rerun_todos() if t["node"] == "D"]
        assert len(d_todos) == 1

    def test_withdraw_slot_waits_for_new_path(self):
        queue = self.build_conflict()
        ok, msg = queue.resolve_conflict("D", RESOLVE_WITHDRAW)
        assert ok
        assert "撤回" in msg
        # 名额撤回：重跑页不再看到 D 的在途待办
        assert all(t["node"] != "D" for t in queue.rerun_todos())
        # 节点回到待领取，等待新路径
        assert node_of(queue, "D")["status"] == "待领取"

        # 新路径到来重新领取：另起一轮，在途任务仍只有一份
        ok, _, _ = queue.claim_chain(["A", "E", "D"])
        assert ok
        d_todos = [t for t in queue.rerun_todos() if t["node"] == "D"]
        assert len(d_todos) == 1
        assert d_todos[0]["round"] == 2
        all_d = [t for t in queue.queue_snapshot()["todos"] if t["node"] == "D"]
        assert {t["status"] for t in all_d} == {"已撤回", "待排期"}

    def test_resolve_without_conflict_rejected(self):
        queue = build_queue()
        queue.claim_chain(PATH_VIA_B)
        ok, msg = queue.resolve_conflict("D", RESOLVE_COMPLETE_LAST)
        assert not ok
        assert "没有待裁决" in msg

    def test_unknown_decision_rejected(self):
        queue = self.build_conflict()
        ok, msg = queue.resolve_conflict("D", "别的决定")
        assert not ok
        assert "未知裁决" in msg

    def test_confirm_blocked_until_resolved(self):
        queue = self.build_conflict()
        ok, msg = queue.confirm_node("D", "assignee")
        assert not ok
        assert "先裁决" in msg


class TestConfirmAndUncovered:
    """接手人确认实际节点，再回到原队列处理未覆盖节点。"""

    def test_confirm_node_and_remaining_uncovered(self):
        queue = build_queue()
        queue.claim_chain(PATH_VIA_B)
        queue.claim_chain(PATH_VIA_C)
        ok, msg = queue.confirm_node("D", "assignee-1")
        assert ok
        d_entry = node_of(queue, "D")
        assert d_entry["status"] == "已确认"
        assert d_entry["confirmed_by"] == "assignee-1"
        # 确认后待办开跑
        assert queue.todo_for_node("D")["status"] == "重跑中"
        # 原队列剩余未覆盖节点：B、C 仍待接手人处理
        assert sorted(queue.queue_snapshot()["uncovered"]) == ["B", "C"]

    def test_double_confirm_rejected(self):
        queue = build_queue()
        queue.claim_chain(PATH_VIA_B)
        queue.confirm_node("D", "assignee-1")
        ok, msg = queue.confirm_node("D", "assignee-2")
        assert not ok
        assert "无需重复确认" in msg


class TestServiceIntegration:
    """服务层跨模块：血缘侧领取生成重跑待办，重跑页确认与裁决。"""

    @pytest.fixture()
    def fork_join_edges(self):
        from app.services.lineage_edge import LineageEdgeService
        from app.services.impact_queue import IMPACT_QUEUE
        from app.store import store

        # 共享单例先重置，避免测试间队列状态互相污染
        IMPACT_QUEUE.reset()
        # 用测试专属节点登记分叉后汇合的血缘边，避免与种子数据互相影响
        svc = LineageEdgeService()
        for edge_id, up, down in [
            ("LG-T1", "T-上游", "T-左"), ("LG-T2", "T-上游", "T-右"),
            ("LG-T3", "T-左", "T-汇合"), ("LG-T4", "T-右", "T-汇合"),
        ]:
            svc.register_edge(edge_id, up, down, "关联", "重跑", 1)
        yield svc
        # 清理：重置队列并撤掉测试数据，保持其他测试的共享状态干净
        IMPACT_QUEUE.reset()
        store._tables["reprocess_task"] = [
            r for r in store.rows("reprocess_task")
            if not str(r.get("影响事实表", "")).startswith("T-")
        ]
        store._tables["lineage_edge"] = [
            r for r in store.rows("lineage_edge")
            if not str(r.get("血缘编号", "")).startswith("LG-T")
        ]

    def test_claim_generates_reprocess_todo(self, fork_join_edges):
        from app.store import store

        svc = fork_join_edges
        ok, _, chains = svc.list_impact_chains("T-上游")
        assert ok and len(chains) == 2
        for chain in chains:
            ok, _, _ = svc.claim_impact_chain(chain, "on_duty")
            assert ok
        # 重跑页得到的节点：左、右、汇合各一份待办
        rerun_rows = [
            r for r in store.rows("reprocess_task")
            if r.get("触发原因") == "影响链处置"
            and str(r.get("影响事实表", "")).startswith("T-")
        ]
        assert {r["影响事实表"] for r in rerun_rows} == {"T-汇合", "T-左", "T-右"}
        assert all(r["status"] == "待排期" for r in rerun_rows)

    def test_revoke_last_path_then_resolve(self, fork_join_edges):
        from app.services.reprocess_task import ReprocessTaskService
        from app.store import store

        svc = fork_join_edges
        rp = ReprocessTaskService()
        _, _, chains = svc.list_impact_chains("T-上游")
        for chain in chains:
            svc.claim_impact_chain(chain)
        # 撤去一支：汇合节点说明从另一支继续
        svc.revoke_impact_chain(chains[1])
        snap = svc.impact_queue_status()
        join = next(n for n in snap["nodes"] if n["node"] == "T-汇合")
        assert join["continue_path"] == chains[0]
        # 撤销最后一支：汇合节点止领 + 在途排期冲突
        svc.revoke_impact_chain(chains[0])
        join = next(
            n for n in svc.impact_queue_status()["nodes"] if n["node"] == "T-汇合")
        assert join["status"] == "已止领" and join["conflict"] == "在途排期冲突"
        # 重跑页裁决：按最后一次有效路径完成这轮排期
        ok, msg = rp.resolve_schedule_conflict("T-汇合", "complete_last_path")
        assert ok and "->".join(chains[0]) in msg
        row = next(
            r for r in store.rows("reprocess_task") if r.get("影响事实表") == "T-汇合")
        assert row["status"] == "重跑中"
        # 接手人确认实际节点
        ok, _ = rp.confirm_impact_node("T-汇合", "assignee")
        assert ok
        # 原队列未覆盖节点：左、右（右已止领待裁决/处理）
        uncovered = svc.impact_queue_status()["uncovered"]
        assert "T-汇合" not in uncovered
