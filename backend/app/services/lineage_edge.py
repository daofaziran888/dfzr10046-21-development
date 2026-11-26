"""血缘关系领域服务：维护维度事实汇总层血缘边数据集，校验依赖传递和影响层级。"""
from __future__ import annotations

from typing import Any

from app.pipeline.impact_chain_queue import ImpactChainQueue
from app.services.base import BaseModuleService
from app.services.impact_queue import IMPACT_QUEUE
from app.store import store


class LineageEdgeService(BaseModuleService):
    MODULE = "lineage_edge"
    ENTITY = "血缘边"
    LIST_FIELDS = ["血缘编号", "上游对象", "下游对象", "传递方式", "重算触发", "影响层级", "血缘版本", "最近更新"]
    REQUIRED_FIELDS = ["血缘编号", "上游对象", "下游对象"]
    STATUS_ORDER = ["已登记", "待核对", "已核对", "已失效"]
    ACTION_RULES = {"提交核对": "待核对", "确认核对": "已核对", "标记失效": "已失效"}
    NEGATIVE_ACTIONS = []

    def register_edge(
        self, edge_id: str, upstream: str, downstream: str,
        propagation: str, recalc_trigger: str, impact_level: int,
    ) -> tuple[bool, str, dict[str, Any] | None]:
        """登记血缘边：上游对象→下游对象的传递方式与重算触发。"""
        rows = store.rows(self.MODULE)
        entry = {
            "血缘编号": edge_id, "上游对象": upstream,
            "下游对象": downstream, "传递方式": propagation,
            "重算触发": recalc_trigger, "影响层级": impact_level,
            "血缘版本": "v1", "最近更新": "2026-11-21",
            "status": "已登记", "pending": False, "abnormal": False,
        }
        entry["id"] = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        rows.append(entry)
        return True, f"血缘边 {upstream}→{downstream} 已登记", entry

    def propagate_impact(
        self, upstream_id: str, levels: int = 3,
    ) -> list[dict[str, Any]]:
        """沿血缘边向下传递重跑影响范围，多层传播。"""
        rows = store.rows(self.MODULE)
        current = [upstream_id]
        seen = {upstream_id}
        result: list[dict[str, Any]] = []
        for _ in range(levels):
            next_level: list[str] = []
            for up in current:
                for r in rows:
                    if r.get("上游对象") == up and r.get("下游对象") not in seen:
                        down = str(r.get("下游对象", ""))
                        next_level.append(down)
                        seen.add(down)
                        result.append({
                            "upstream": up, "downstream": down,
                            "propagation": r.get("传递方式"),
                            "impact_level": r.get("影响层级"),
                        })
            current = next_level
            if not current:
                break
        return result

    # ------------------------------------------------------------------
    # 影响链工作队列：下游处置筛选 → 跨模块领取 → 来路撤销
    # ------------------------------------------------------------------
    def list_impact_chains(self, upstream: str) -> tuple[bool, str, list[list[str]]]:
        """下游处置筛选：值班人从上游对象选择一条实际影响链。

        只沿未失效的血缘边枚举路径；分叉后汇合的链会产出多条可选路径。
        """
        upstream = str(upstream or "").strip()
        if not upstream:
            return False, "请先指定上游对象", []
        edges = [
            {"upstream": str(r.get("上游对象", "")),
             "downstream": str(r.get("下游对象", ""))}
            for r in store.rows(self.MODULE)
            if r.get("status") != "已失效"
        ]
        chains = ImpactChainQueue.select_impact_chains(upstream, edges)
        if not chains:
            return False, f"上游对象 {upstream} 没有可用的实际影响链", []
        return True, f"上游对象 {upstream} 共 {len(chains)} 条实际影响链", chains

    def claim_impact_chain(
        self, path: list[str], applicant: str = "on_duty",
    ) -> tuple[bool, str, dict[str, Any]]:
        """跨模块申请：沿影响链领取待接手节点，在重跑任务侧生成待办。

        两条路径并发提交时汇合节点只生成一份待办；已生成的待办同步写入
        重跑任务数据集，接手人从重跑页确认实际节点。
        """
        ok, message, detail = IMPACT_QUEUE.claim_chain(path, applicant)
        if not ok:
            return False, message, detail
        rerun_rows = store.rows("reprocess_task")
        for item in detail.get("results", []):
            if not (item.get("ok") and item.get("todo_created")):
                continue
            todo = IMPACT_QUEUE.get_todo(item["todo_id"])
            if todo is None:
                continue
            entry = {
                "重跑编号": todo["todo_id"], "触发原因": "影响链处置",
                "影响事实表": todo["node"],
                "重跑区间": "-", "覆盖策略": "并存留痕",
                "依赖批次": "->".join(todo["active_path"]),
                "重跑水位": "", "重跑耗时": "",
                "status": "待排期", "pending": True, "abnormal": False,
            }
            entry["id"] = max((int(r.get("id", 0)) for r in rerun_rows), default=0) + 1
            rerun_rows.append(entry)
        return True, message, detail

    def revoke_impact_chain(self, path: list[str]) -> tuple[bool, str, dict[str, Any]]:
        """撤销一条来路：已领取任务说明从哪条有效路径继续。

        撤销最后一条来路后节点止领、停止新的领取；若在途排期尚未开跑，
        冲突交给重跑页裁决，裁决结果同步回重跑任务数据集。
        """
        ok, message, detail = IMPACT_QUEUE.revoke_chain(path)
        if not ok:
            return False, message, detail
        self._sync_rerun_rows(detail.get("events", []))
        return True, message, detail

    def impact_queue_status(self) -> dict[str, Any]:
        """原队列快照：各节点状态、待办数量与未覆盖节点。"""
        return IMPACT_QUEUE.queue_snapshot()

    @staticmethod
    def _sync_rerun_rows(events: list[dict[str, Any]]) -> None:
        """把撤销引起的待办变化同步到重跑任务数据集（如挂起在途排期）。"""
        rerun_rows = store.rows("reprocess_task")
        for event in events:
            todo_id = event.get("todo_id")
            if not todo_id:
                continue
            todo = IMPACT_QUEUE.get_todo(todo_id)
            if todo is None:
                continue
            for row in rerun_rows:
                if row.get("重跑编号") == todo_id:
                    row["status"] = todo["status"]
                    row["pending"] = todo["status"] not in ("已完成", "已撤回")
                    if todo.get("held"):
                        row["abnormal"] = True