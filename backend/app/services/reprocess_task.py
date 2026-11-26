"""重跑任务领域服务：覆盖策略与血缘影响传递，调度重跑状态机和水位推进，产出重跑任务数据集。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.impact_queue import IMPACT_QUEUE
from app.store import store


class ReprocessTaskService(BaseModuleService):
    MODULE = "reprocess_task"
    ENTITY = "重跑任务"
    LIST_FIELDS = ["重跑编号", "触发原因", "影响事实表", "重跑区间", "覆盖策略", "依赖批次", "重跑水位", "重跑耗时"]
    REQUIRED_FIELDS = ["重跑编号", "触发原因", "影响事实表"]
    STATUS_ORDER = ["待排期", "重跑中", "已完成", "重跑失败"]
    ACTION_RULES = {"排期重跑": "重跑中", "确认完成": "已完成", "标记失败": "重跑失败"}
    NEGATIVE_ACTIONS = ["标记失败"]

    def schedule_reprocess(
        self, task_id: str, reason: str, target_table: str,
        reprocess_range: str, strategy: str, depends_on: str,
    ) -> tuple[bool, str, dict[str, Any]]:
        """排期重跑任务：覆盖策略决定旧区间是整段替换还是并存留痕。"""
        if strategy not in ("整段替换", "并存留痕"):
            return False, f"覆盖策略 {strategy} 无效", {}

        entry = {
            "重跑编号": task_id, "触发原因": reason,
            "影响事实表": target_table, "重跑区间": reprocess_range,
            "覆盖策略": strategy, "依赖批次": depends_on,
            "重跑水位": "", "重跑耗时": "",
            "status": "待排期", "pending": True, "abnormal": False,
        }
        rows = store.rows(self.MODULE)
        entry["id"] = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        rows.append(entry)
        return True, f"重跑任务 {task_id} 已排期，策略：{strategy}", entry

    def propagate_lineage_impact(self, target_table: str) -> dict[str, list[str]]:
        """沿血缘边传递重跑影响范围。"""
        lineage_rows = store.rows("lineage_edge")
        direct: list[str] = []
        for r in lineage_rows:
            if r.get("上游对象") == target_table:
                direct.append(str(r.get("下游对象", "")))
        seen = set(direct)
        indirect: list[str] = []
        for d in direct:
            for r in lineage_rows:
                if r.get("上游对象") == d and r.get("下游对象") not in seen:
                    indirect.append(str(r.get("下游对象", "")))
                    seen.add(str(r.get("下游对象", "")))
        return {"direct_impact": direct, "indirect_impact": indirect}

    def complete_reprocess(self, task_id: str, watermark: str) -> tuple[bool, str]:
        """确认重跑完成：更新重跑水位。"""
        rows = store.rows(self.MODULE)
        for r in rows:
            if r.get("重跑编号") == task_id:
                r["status"] = "已完成"
                r["重跑水位"] = watermark
                r["pending"] = False
                return True, f"重跑任务 {task_id} 已完成，水位 {watermark}"
        return False, f"重跑任务 {task_id} 不存在"

    # ------------------------------------------------------------------
    # 影响链待办：接手人确认与在途排期冲突裁决
    # ------------------------------------------------------------------
    def impact_todos(self) -> list[dict[str, Any]]:
        """重跑页得到的影响链待办节点：未撤回的在途任务，同一节点只有一份。"""
        return IMPACT_QUEUE.rerun_todos()

    def confirm_impact_node(self, node: str, operator: str) -> tuple[bool, str]:
        """接手人从重跑页确认实际节点，同步重跑任务记录状态。"""
        ok, message = IMPACT_QUEUE.confirm_node(node, operator)
        if ok:
            self._sync_todo_row(node)
        return ok, message

    def resolve_schedule_conflict(self, node: str, decision: str) -> tuple[bool, str]:
        """裁决取消最后路径与在途排期的冲突。

        complete_last_path：按最后一次有效路径完成这轮排期；
        withdraw_slot：撤回名额等新路径。两种决定都不会启动两份任务。
        """
        ok, message = IMPACT_QUEUE.resolve_conflict(node, decision)
        if ok:
            self._sync_todo_row(node)
        return ok, message

    @staticmethod
    def _sync_todo_row(node: str) -> None:
        """把队列里的待办状态回写到重跑任务数据集。"""
        todo = IMPACT_QUEUE.todo_for_node(node)
        if todo is None:
            # 名额已撤回：把对应的重跑记录标记为已撤回
            for row in store.rows("reprocess_task"):
                if row.get("影响事实表") == node and row.get("触发原因") == "影响链处置" \
                        and row.get("status") not in ("已完成", "已撤回"):
                    row["status"] = "已撤回"
                    row["pending"] = False
            return
        for row in store.rows("reprocess_task"):
            if row.get("重跑编号") == todo["todo_id"]:
                row["status"] = todo["status"]
                row["pending"] = todo["status"] not in ("已完成", "已撤回")
                row["abnormal"] = bool(todo.get("held"))