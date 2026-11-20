"""断流恢复接口：维护检查点，覆盖触发检查、标记可用、标记过期等动作。

断流恢复通过状态管理模块生成的快照整理索引装载有效内容，
装载结果回写快照对账清单，供回收端判断引用可否解除。
"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import snapshot_gc
from app.services.checkpoint import CheckpointService

router = APIRouter(prefix="/api/checkpoint", tags=["断流恢复"])

service = CheckpointService()

LIST_FIELDS = ["检查点编号", "关联流水线", "位点时间", "状态大小", "触发方式", "存活轮数", "恢复耗时", "创建时间"]
STATUSES = ["创建中", "可用", "已过期", "恢复失败"]


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段，忽略分页等保留参数。"""
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in LIST_FIELDS and value.strip()
    }


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按检查点编号检索"),
    status: str | None = Query(default=None, description="创建中、可用、已过期、恢复失败"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、检查点编号关键字与状态过滤断流恢复列表。"""
    items, total = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV；必须声明在 /{entry_id} 之前。"""
    items, _ = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *LIST_FIELDS, "status"])
    for item in items:
        row = []
        for value in [item.get("id"), *(item.get(field, "") for field in LIST_FIELDS), item.get("status", "")]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="checkpoint.csv"'})


@router.get("/recovery-queue", response_model=PageResult[dict])
def get_recovery_queue() -> PageResult[dict]:
    """恢复排队记录：含与快照回收重叠时的冲突登记；必须声明在 /{entry_id} 之前。"""
    items = snapshot_gc.recovery_queue()
    return PageResult(items=items, total=len(items), page=1, size=len(items) or 1)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条检查点明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"检查点 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条检查点，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="检查点已登记", entry=entry)


@router.post("/{entry_id}/restore", response_model=ActionResult)
def restore_checkpoint(entry_id: int) -> ActionResult:
    """断流恢复：通过快照整理索引装载有效内容，装载结果回写快照对账清单。"""
    result = snapshot_gc.restore_via_index(entry_id)
    if result is None:
        return ActionResult(ok=False, message=f"检查点 {entry_id} 不存在或已归档")
    return ActionResult(ok=bool(result["ok"]), message=result["message"], entry=result)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条检查点执行触发检查、标记可用、标记过期；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
