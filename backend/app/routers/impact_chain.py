"""影响链工作队列接口：下游处置筛选、跨模块领取、来路撤销与排期冲突裁决。

血缘关系侧发起筛选/领取/撤销，重跑任务侧确认节点与裁决冲突，
两边共享同一份队列，保证同一节点不会启动两份重跑任务。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services import service_for
from app.services.lineage_edge import LineageEdgeService
from app.services.reprocess_task import ReprocessTaskService

router = APIRouter(prefix="/api/impact-chain", tags=["影响链工作队列"])


class ChainPayload(BaseModel):
    path: list[str] = Field(default_factory=list)
    applicant: str = "on_duty"


class ConfirmPayload(BaseModel):
    node: str
    operator: str = "assignee"


class ResolvePayload(BaseModel):
    node: str
    decision: str  # complete_last_path / withdraw_slot


def _lineage() -> LineageEdgeService:
    service = service_for("lineage_edge")
    if not isinstance(service, LineageEdgeService):  # pragma: no cover
        raise HTTPException(status_code=500, detail="血缘关系服务未就绪")
    return service


def _reprocess() -> ReprocessTaskService:
    service = service_for("reprocess_task")
    if not isinstance(service, ReprocessTaskService):  # pragma: no cover
        raise HTTPException(status_code=500, detail="重跑任务服务未就绪")
    return service


@router.get("/chains")
def list_chains(upstream: str = Query(..., description="上游对象")) -> dict[str, Any]:
    """下游处置筛选：列出上游对象的全部实际影响链，值班人从中选择一条。"""
    ok, message, chains = _lineage().list_impact_chains(upstream)
    return {"ok": ok, "message": message, "chains": chains}


@router.post("/claim")
def claim_chain(payload: ChainPayload) -> dict[str, Any]:
    """跨模块申请：沿选中的影响链领取待接手节点，在重跑任务生成待办。"""
    ok, message, detail = _lineage().claim_impact_chain(
        payload.path, payload.applicant)
    return {"ok": ok, "message": message, **detail}


@router.post("/revoke")
def revoke_chain(payload: ChainPayload) -> dict[str, Any]:
    """撤销一条来路：已领取任务说明继续路径；最后一条被撤销时停止新领取。"""
    ok, message, detail = _lineage().revoke_impact_chain(payload.path)
    return {"ok": ok, "message": message, **detail}


@router.post("/confirm")
def confirm_node(payload: ConfirmPayload) -> dict[str, Any]:
    """接手人从重跑页确认实际节点。"""
    ok, message = _reprocess().confirm_impact_node(
        payload.node, payload.operator)
    return {"ok": ok, "message": message}


@router.post("/resolve")
def resolve_conflict(payload: ResolvePayload) -> dict[str, Any]:
    """裁决在途排期冲突：按最后有效路径完成本轮，或撤回名额等新路径。"""
    ok, message = _reprocess().resolve_schedule_conflict(
        payload.node, payload.decision)
    return {"ok": ok, "message": message}


@router.get("/queue")
def queue_status() -> dict[str, Any]:
    """原队列快照：节点状态、待办数量与未覆盖节点。"""
    return _lineage().impact_queue_status()


@router.get("/todos")
def rerun_todos() -> dict[str, Any]:
    """重跑页得到的影响链待办节点。"""
    todos = _reprocess().impact_todos()
    return {"items": todos, "total": len(todos)}
