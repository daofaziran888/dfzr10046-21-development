"""影响链工作队列：追踪一条影响链上待接手节点的领取、撤销与重跑待办联动。

核心行为：
- 下游处置筛选：从上游对象出发，沿有效血缘边枚举全部实际影响链
- 跨模块领取：值班人沿选中的影响链申请领取，链上每个下游节点在重跑任务
  侧生成一份待办；两条路径并发提交时，汇合节点只生成一份待办（锁内幂等）
- 来路撤销：撤掉一条来路时，已领取任务说明从哪条有效路径继续；
  撤销最后一条来路后节点止领，停止新的领取
- 在途排期冲突：节点止领时若重跑页已受理排期尚未开跑（待排期），
  两边需确认是按最后一次有效路径完成这轮排期，还是撤回名额等新路径；
  任一决定都保证同一节点不会启动两份重跑任务
- 接手确认：接手人从重跑页确认实际节点，队列其余节点保持未覆盖待处理
"""
from __future__ import annotations

import threading
from typing import Any

# 队列节点状态
NODE_PENDING = "待领取"
NODE_CLAIMED = "已领取"
NODE_CONFIRMED = "已确认"
NODE_STOPPED = "已止领"

# 重跑待办状态
TODO_SCHEDULED = "待排期"   # 已受理排期，尚未开跑
TODO_RUNNING = "重跑中"
TODO_DONE = "已完成"
TODO_WITHDRAWN = "已撤回"

# 在途排期冲突的两种裁决
RESOLVE_COMPLETE_LAST = "complete_last_path"  # 按最后一次有效路径完成这轮排期
RESOLVE_WITHDRAW = "withdraw_slot"            # 撤回名额等新路径

CONFLICT_INFLIGHT = "在途排期冲突"


def path_key(path: list[str]) -> str:
    """影响链路径的稳定标识。"""
    return "->".join(path)


class ImpactChainQueue:
    """影响链工作队列：血缘关系侧持有，重跑任务侧共享同一份待办。"""

    def __init__(self) -> None:
        self._nodes: dict[str, dict[str, Any]] = {}
        self._todos: dict[str, dict[str, Any]] = {}
        self._seq = 0
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # 下游处置筛选
    # ------------------------------------------------------------------
    @staticmethod
    def select_impact_chains(
        upstream: str,
        edges: list[dict[str, str]],
        max_depth: int = 8,
    ) -> list[list[str]]:
        """从上游对象出发，沿有效血缘边枚举全部实际影响链路径。

        分叉后汇合的链会产出多条路径，汇合节点同时出现在各条路径上。
        """
        adjacency: dict[str, list[str]] = {}
        for edge in edges:
            up = str(edge.get("upstream", "")).strip()
            down = str(edge.get("downstream", "")).strip()
            if up and down:
                adjacency.setdefault(up, []).append(down)
        chains: list[list[str]] = []

        def dfs(path: list[str], depth: int) -> None:
            if depth >= max_depth:
                if len(path) > 1:
                    chains.append(list(path))
                return
            downs = [d for d in adjacency.get(path[-1], []) if d not in path]
            if not downs:
                if len(path) > 1:
                    chains.append(list(path))
                return
            for down in downs:
                dfs(path + [down], depth + 1)

        start = str(upstream).strip()
        if start:
            dfs([start], 0)
        return chains

    # ------------------------------------------------------------------
    # 领取
    # ------------------------------------------------------------------
    def claim_chain(self, path: list[str], applicant: str = "on_duty") -> tuple[bool, str, dict[str, Any]]:
        """沿一条实际影响链申请领取：链上每个下游节点登记来路并幂等生成待办。

        整条路径在锁内办结，两条路径并发提交时汇合节点只生成一份待办。
        """
        nodes = [str(p).strip() for p in path if str(p).strip()]
        if len(nodes) < 2:
            return False, "影响链至少包含上游对象和一个下游节点", {}
        key = path_key(nodes)
        results: list[dict[str, Any]] = []
        with self._lock:
            for node in nodes[1:]:
                results.append(self._claim_node_locked(node, key, nodes, applicant))
        claimed = [r["node"] for r in results if r["ok"]]
        rejected = [r["node"] for r in results if not r["ok"]]
        if not claimed:
            return False, f"影响链 {key} 没有可领取节点：{'、'.join(rejected)}", {
                "results": results, "claimed": [], "rejected": rejected,
            }
        message = f"影响链 {key} 已领取 {len(claimed)} 个节点"
        if rejected:
            message += f"，{len(rejected)} 个节点被拒（{'、'.join(rejected)}）"
        return True, message, {
            "results": results, "claimed": claimed, "rejected": rejected,
        }

    def _claim_node_locked(
        self, node: str, key: str, path: list[str], applicant: str,
    ) -> dict[str, Any]:
        entry = self._nodes.get(node)
        if entry is None:
            entry = {
                "node": node, "status": NODE_PENDING,
                "claims": {}, "todo_id": None,
                "continue_path": None, "last_valid_path": None,
                "conflict": None, "applicant": applicant,
                "confirmed_by": None,
            }
            self._nodes[node] = entry
        if entry["status"] == NODE_STOPPED and not entry["claims"]:
            return {"node": node, "ok": False,
                    "message": f"节点 {node} 已停止新的领取"}
        if entry["status"] == NODE_CONFIRMED:
            return {"node": node, "ok": False,
                    "message": f"节点 {node} 已确认，无需重复领取"}
        # 登记来路（同一路径重复提交是幂等的）
        entry["claims"][key] = list(path)
        entry["continue_path"] = list(path)
        entry["status"] = NODE_CLAIMED
        entry["applicant"] = applicant
        # 幂等关键：同一节点只保留一份在途待办；已撤回的名额才允许新一轮
        todo = self._todos.get(entry["todo_id"]) if entry["todo_id"] else None
        todo_created = False
        if todo is None or todo["status"] == TODO_WITHDRAWN:
            todo = self._new_todo_locked(node, path)
            entry["todo_id"] = todo["todo_id"]
            todo_created = True
        if key not in todo["source_paths"]:
            todo["source_paths"].append(key)
        todo["active_path"] = list(path)
        return {
            "node": node, "ok": True, "todo_id": todo["todo_id"],
            "todo_created": todo_created,
            "message": f"节点 {node} 已领取，待办 {todo['todo_id']}",
        }

    def _new_todo_locked(self, node: str, path: list[str]) -> dict[str, Any]:
        self._seq += 1
        todo_id = f"TODO-{self._seq:04d}"
        round_no = 1 + max(
            (t["round"] for t in self._todos.values() if t["node"] == node),
            default=0,
        )
        todo = {
            "todo_id": todo_id, "node": node,
            "status": TODO_SCHEDULED, "held": False,
            "source_paths": [path_key(path)],
            "active_path": list(path), "round": round_no,
        }
        self._todos[todo_id] = todo
        return todo

    # ------------------------------------------------------------------
    # 撤销来路
    # ------------------------------------------------------------------
    def revoke_chain(self, path: list[str]) -> tuple[bool, str, dict[str, Any]]:
        """撤销一条来路：更新继续路径说明；最后一条被撤销时节点止领。

        止领时若重跑页已受理排期尚未开跑（待排期），标记在途排期冲突并挂起，
        待两边裁决；已开跑的任务按最后一次有效路径跑完本轮。
        """
        nodes = [str(p).strip() for p in path if str(p).strip()]
        key = path_key(nodes)
        events: list[dict[str, Any]] = []
        with self._lock:
            for node in nodes[1:]:
                entry = self._nodes.get(node)
                if entry is None or key not in entry["claims"]:
                    continue
                del entry["claims"][key]
                todo = self._todos.get(entry["todo_id"]) if entry["todo_id"] else None
                if todo and key in todo["source_paths"]:
                    todo["source_paths"].remove(key)
                if entry["status"] == NODE_CONFIRMED:
                    events.append({"node": node, "event": "已确认节点来路撤销"})
                    continue
                if entry["claims"]:
                    # 还有效来路：已领取任务说明从哪条有效路径继续
                    continue_path = list(next(iter(entry["claims"].values())))
                    entry["continue_path"] = continue_path
                    if todo:
                        todo["active_path"] = list(continue_path)
                    events.append({
                        "node": node, "event": "继续有效路径",
                        "path": continue_path,
                    })
                    continue
                # 最后一条来路被撤销：停止新的领取
                entry["status"] = NODE_STOPPED
                entry["last_valid_path"] = list(nodes)
                entry["continue_path"] = None
                if todo and todo["status"] == TODO_SCHEDULED:
                    # 在途排期尚未开跑：与取消最后路径冲突，挂起待裁决
                    todo["held"] = True
                    entry["conflict"] = CONFLICT_INFLIGHT
                    events.append({
                        "node": node, "event": CONFLICT_INFLIGHT,
                        "todo_id": todo["todo_id"],
                        "last_valid_path": list(nodes),
                    })
                elif todo and todo["status"] == TODO_RUNNING:
                    # 已开跑：按最后一次有效路径跑完本轮，不再新起任务
                    todo["active_path"] = list(nodes)
                    events.append({
                        "node": node, "event": "在跑任务按最后路径完成",
                        "todo_id": todo["todo_id"],
                    })
                else:
                    events.append({"node": node, "event": "已止领"})
        if not events:
            return False, f"影响链 {key} 没有可撤销的来路", {"events": []}
        return True, f"影响链 {key} 已撤销，{len(events)} 个节点受影响", {"events": events}

    # ------------------------------------------------------------------
    # 在途排期冲突裁决
    # ------------------------------------------------------------------
    def resolve_conflict(self, node: str, decision: str) -> tuple[bool, str]:
        """裁决取消最后路径与在途排期的冲突，两边按同一决定办结。

        - complete_last_path：按最后一次有效路径完成这轮排期（一份任务继续）
        - withdraw_slot：撤回名额等新路径（旧待办撤回，新路径来时另起一轮）
        两种决定都保证同一节点不会同时存在两份在途任务。
        """
        with self._lock:
            entry = self._nodes.get(node)
            if entry is None or entry.get("conflict") != CONFLICT_INFLIGHT:
                return False, f"节点 {node} 没有待裁决的在途排期冲突"
            todo = self._todos.get(entry["todo_id"])
            if todo is None:
                return False, f"节点 {node} 缺少对应的重跑待办"
            last_path = list(entry["last_valid_path"] or [])
            if decision == RESOLVE_COMPLETE_LAST:
                todo["held"] = False
                todo["active_path"] = last_path
                todo["status"] = TODO_RUNNING
                entry["conflict"] = None
                entry["continue_path"] = last_path
                return True, (
                    f"节点 {node} 按最后一次有效路径 {path_key(last_path)} "
                    f"完成这轮排期，待办 {todo['todo_id']} 继续（第 {todo['round']} 轮）"
                )
            if decision == RESOLVE_WITHDRAW:
                todo["status"] = TODO_WITHDRAWN
                todo["held"] = False
                entry["conflict"] = None
                entry["status"] = NODE_PENDING
                entry["todo_id"] = None
                return True, (
                    f"节点 {node} 已撤回排期名额（待办 {todo['todo_id']} 作废），"
                    "等待新路径重新领取"
                )
            return False, f"未知裁决「{decision}」，仅支持按最后路径完成或撤回名额"

    # ------------------------------------------------------------------
    # 接手人确认
    # ------------------------------------------------------------------
    def confirm_node(self, node: str, operator: str) -> tuple[bool, str]:
        """接手人从重跑页确认实际节点；确认后待办开跑，节点退出未覆盖集合。"""
        with self._lock:
            entry = self._nodes.get(node)
            if entry is None:
                return False, f"节点 {node} 不在影响链队列中"
            if entry["status"] == NODE_CONFIRMED:
                return False, f"节点 {node} 已确认，无需重复确认"
            if entry["conflict"]:
                return False, f"节点 {node} 存在在途排期冲突，请先裁决"
            todo = self._todos.get(entry["todo_id"]) if entry["todo_id"] else None
            entry["status"] = NODE_CONFIRMED
            entry["confirmed_by"] = operator
            if todo and todo["status"] == TODO_SCHEDULED and not todo["held"]:
                todo["status"] = TODO_RUNNING
            return True, f"接手人 {operator} 已确认实际节点 {node}"

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def reset(self) -> None:
        """清空队列：供测试隔离与运维重建使用。"""
        with self._lock:
            self._nodes.clear()
            self._todos.clear()
            self._seq = 0

    def get_todo(self, todo_id: str) -> dict[str, Any] | None:
        todo = self._todos.get(todo_id)
        return dict(todo) if todo else None

    def todo_for_node(self, node: str) -> dict[str, Any] | None:
        entry = self._nodes.get(node)
        if not entry or not entry.get("todo_id"):
            return None
        todo = self._todos.get(entry["todo_id"])
        return dict(todo) if todo else None

    def rerun_todos(self) -> list[dict[str, Any]]:
        """重跑页得到的待办：未撤回的在途任务，同一节点只有一份。"""
        with self._lock:
            return [
                {**t, "source_paths": list(t["source_paths"]),
                 "active_path": list(t["active_path"])}
                for t in self._todos.values()
                if t["status"] != TODO_WITHDRAWN
            ]

    def queue_snapshot(self) -> dict[str, Any]:
        """原队列快照：节点状态、待办数量与未覆盖节点。"""
        with self._lock:
            nodes = [
                {**n, "claims": {k: list(v) for k, v in n["claims"].items()}}
                for n in self._nodes.values()
            ]
            todos = [
                {**t, "source_paths": list(t["source_paths"]),
                 "active_path": list(t["active_path"])}
                for t in self._todos.values()
            ]
            return {
                "nodes": nodes,
                "todos": todos,
                "todo_count": sum(1 for t in todos if t["status"] != TODO_WITHDRAWN),
                "uncovered": [n["node"] for n in nodes if n["status"] != NODE_CONFIRMED],
            }
