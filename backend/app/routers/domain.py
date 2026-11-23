"""跨模块领域接口：转换方案部署/血缘登记、流水线启动/运行详情、按运行编号的血缘回查。

通用资源路由只承载列表/登记/动作，跨模块取数的业务编排集中在这里，
路由层不做业务判断，全部委派给领域服务。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services import service_for
from app.services.etl_pipeline import EtlPipelineService
from app.services.lineage_graph import LineageGraphService
from app.services.transform_plan import TransformPlanService

router = APIRouter(prefix="/api", tags=["跨模块领域"])


class DeployPayload(BaseModel):
    pipeline_id: int


class ExecutePayload(BaseModel):
    force_full: bool = False
    register_lineage: bool = False


def _transform_service() -> TransformPlanService:
    service = service_for("transform_plan")
    assert isinstance(service, TransformPlanService)
    return service


def _pipeline_service() -> EtlPipelineService:
    service = service_for("etl_pipeline")
    assert isinstance(service, EtlPipelineService)
    return service


def _lineage_service() -> LineageGraphService:
    service = service_for("lineage_graph")
    assert isinstance(service, LineageGraphService)
    return service


@router.post("/transform_plan/{plan_id}/deploy")
def deploy_plan(plan_id: int, payload: DeployPayload) -> dict[str, Any]:
    """把已发布的转换方案部署到指定流水线。"""
    result = _transform_service().deploy_to_pipeline(plan_id, payload.pipeline_id)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.post("/transform_plan/{plan_id}/lineage")
def register_plan_lineage(plan_id: int) -> dict[str, Any]:
    """登记入口：从转换方案登记血缘（同一版重复提交幂等，返回同一组血缘编号）。"""
    result = _transform_service().register_lineage_from_plan(plan_id)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.get("/transform_plan/{plan_id}/lineage")
def list_plan_lineage(plan_id: int) -> dict[str, Any]:
    """查询某一版转换方案已登记的血缘编号清单。"""
    service = _transform_service()
    if service.get_entry(plan_id) is None:
        raise HTTPException(status_code=404, detail="转换方案不存在")
    rows = service.plan_lineage_rows(plan_id)
    return {
        "plan_id": plan_id,
        "lineage_complete": service.is_lineage_complete(plan_id),
        "lineage_ids": [int(row["id"]) for row in rows],
        "items": rows,
    }


@router.get("/etl_pipeline/{pipeline_id}/effective-plan")
def get_effective_plan(pipeline_id: int) -> dict[str, Any]:
    """查询本轮启动将采用哪一版转换方案及血缘完整性结论。"""
    result = _pipeline_service().resolve_effective_plan(pipeline_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@router.get("/etl_pipeline/{pipeline_id}/stages")
def get_pipeline_stages(pipeline_id: int) -> dict[str, Any]:
    """查询流水线阶段规划（转换阶段与当轮生效方案取同一份规则）。"""
    plan = _pipeline_service().plan_pipeline_stages(pipeline_id)
    if plan is None:
        raise HTTPException(status_code=404, detail="流水线不存在")
    return plan


@router.post("/etl_pipeline/{pipeline_id}/execute")
def execute_pipeline(pipeline_id: int, payload: ExecutePayload | None = None) -> dict[str, Any]:
    """启动一轮流水线；可选在启动时为已发布方案补齐血缘登记。"""
    payload = payload or ExecutePayload()
    result = _pipeline_service().execute_pipeline(
        pipeline_id,
        force_full=payload.force_full,
        register_lineage=payload.register_lineage,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.get("/pipeline_monitor/runs/{run_id}")
def get_run_detail(run_id: int) -> dict[str, Any]:
    """按运行编号回查运行详情：生效方案、转换步骤、表映射与血缘快照。"""
    result = _pipeline_service().get_run_detail(run_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@router.get("/pipeline_monitor/runs/{run_id}/lineage")
def trace_run_lineage(run_id: int, target_table: str, max_depth: int = 5) -> dict[str, Any]:
    """按运行编号回查当轮完整血缘链路（只在该轮登记的血缘范围内追溯）。"""
    result = _lineage_service().trace_run_upstream(run_id, target_table, max_depth=max_depth)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result["message"])
    return result
