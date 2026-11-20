"""状态管理接口：维护算子状态，覆盖初始化状态、触发快照、清理状态等动作。

快照生命周期（清理校验、整理索引、回收、对账清单）也挂在本模块，
与断流恢复模块共用同一份快照引用判断。
"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult, ReclaimPayload, SnapshotRegisterPayload
from app.services import snapshot_gc
from app.services.state_store import StateStoreService

router = APIRouter(prefix="/api/state_store", tags=["状态管理"])

service = StateStoreService()

LIST_FIELDS = ["状态编号", "关联算子", "存储后端", "分区数量", "状态大小", "过期策略", "快照周期", "最后快照"]
STATUSES = ["未初始化", "运行中", "快照中", "已清理"]


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
    keyword: str | None = Query(default=None, description="按状态编号检索"),
    status: str | None = Query(default=None, description="未初始化、运行中、快照中、已清理"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、状态编号关键字与状态过滤状态管理列表。"""
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
        headers={"Content-Disposition": 'attachment; filename="state_store.csv"'})


@router.get("/snapshots", response_model=PageResult[dict])
def list_snapshots(
    readable: bool = Query(default=False, description="只返回可直接读取的快照；已释放的一律不回"),
) -> PageResult[dict]:
    """快照清单；必须声明在 /{entry_id} 之前。"""
    items = snapshot_gc.list_snapshots(readable_only=readable)
    return PageResult(items=items, total=len(items), page=1, size=len(items) or 1)


@router.get("/snapshots/index", response_model=dict)
def get_snapshot_index() -> dict:
    """快照整理索引：依据算子状态数据与检查点的依赖关系生成。"""
    return snapshot_gc.current_index()


@router.get("/snapshots/reconciliation", response_model=PageResult[dict])
def get_reconciliation() -> PageResult[dict]:
    """快照对账清单：回收端与恢复端共同核对的校验/回收/装载状态。"""
    items = snapshot_gc.reconciliation_list()
    return PageResult(items=items, total=len(items), page=1, size=len(items) or 1)


@router.get("/snapshots/reclaim-records", response_model=PageResult[dict])
def get_reclaim_records() -> PageResult[dict]:
    """可核对的快照回收记录：每次回收动作的留痕。"""
    items = snapshot_gc.reclaim_records()
    return PageResult(items=items, total=len(items), page=1, size=len(items) or 1)


@router.post("/snapshots/cleanup/validate", response_model=ActionResult)
def validate_snapshot_cleanup() -> ActionResult:
    """算子状态快照清理校验：重建整理索引并刷新对账清单的可回收口径。"""
    result = snapshot_gc.validate_cleanup()
    return ActionResult(
        ok=True,
        message=f"快照清理校验完成：可回收 {len(result['可回收'])} 条，校验失败 {len(result['校验失败'])} 条",
        entry=result,
    )


@router.post("/snapshots/reclaim", response_model=ActionResult)
def reclaim_snapshots(payload: ReclaimPayload) -> ActionResult:
    """执行快照回收：释放已过期数据占用，每个动作都落可核对的回收记录。"""
    result = snapshot_gc.reclaim_snapshots(
        payload.snapshot_ids or None, simulate_failure=payload.simulate_failure)
    released = sum(1 for r in result["records"] if r["动作"] == "释放")
    return ActionResult(
        ok=True,
        message=f"快照回收完成：释放 {released} 条，合计 {result['释放字节合计']} 字节",
        entry=result,
    )


@router.post("/snapshots/register", response_model=ActionResult)
def register_snapshot(payload: SnapshotRegisterPayload) -> ActionResult:
    """登记算子状态快照；落库失败时不留指向空数据的恢复点。"""
    snap, error = snapshot_gc.register_snapshot(
        payload.state_code, payload.content,
        expired=payload.expired, simulate_persist_failure=payload.simulate_persist_failure)
    if error:
        return ActionResult(ok=False, message=error)
    message = "快照已登记" if snap.get("落库状态") == "已落库" else "快照落库失败，已记录替代内容，未留下指向空数据的恢复点"
    return ActionResult(ok=True, message=message, entry=snap)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条算子状态明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"算子状态 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条算子状态，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="算子状态已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条算子状态执行初始化状态、触发快照、清理状态；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
