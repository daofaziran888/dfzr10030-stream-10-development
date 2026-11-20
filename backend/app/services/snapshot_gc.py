"""算子状态快照生命周期：清理校验、整理索引、回收记录与断流恢复装载。

设计约定
--------
* 快照整理索引：依据算子状态数据（state_store/state_snapshot）与检查点
  （checkpoint）的依赖关系生成并落库，断流恢复按它装载有效内容。
* 共用引用判断：``checked_snapshot_ref`` 是回收端与恢复端唯一的引用核对
  口径，抽取出的已校验快照引用（SnapshotRef）供双方核对，避免两套标准。
* 快照对账清单：每个快照一条，记录校验/回收/装载状态；恢复端回写实际装载
  结果，回收端据此判断该引用可否解除。
* 回收记录：每次回收动作（成功或失败）都落一条可核对的记录，释放字节、
  失败原因、当时引用它的检查点都可追查。
* 落库失败：不留下指向空数据的恢复点——索引生成时把检查点依赖改指到
  替代内容，并记录原编号与替代内容供追查。
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.store import store

SNAPSHOT_TABLE = "state_snapshot"
RECON_TABLE = "snapshot_recon"
RECLAIM_TABLE = "snapshot_reclaim_record"
INDEX_TABLE = "_snapshot_index"
QUEUE_TABLE = "_recovery_queue"

# 只有存活（创建中/可用）的检查点才会钉住快照引用
ACTIVE_CHECKPOINT_STATUSES = {"创建中", "可用"}
# 回收动作已发起但未成功落定：恢复请求与之重叠时按冲突登记
RECLAIM_IN_FLIGHT_STATUSES = {"回收中", "回收失败"}


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _snapshots() -> list[dict[str, Any]]:
    return store.rows(SNAPSHOT_TABLE)


def _recon_rows() -> list[dict[str, Any]]:
    return store.rows(RECON_TABLE)


def find_snapshot(snapshot_id: str) -> dict[str, Any] | None:
    for row in _snapshots():
        if row.get("快照编号") == snapshot_id:
            return row
    return None


def find_recon(snapshot_id: str) -> dict[str, Any] | None:
    for row in _recon_rows():
        if row.get("快照编号") == snapshot_id:
            return row
    return None


def ensure_recon(snapshot_id: str) -> dict[str, Any]:
    """对账清单按快照编号一条，缺了先补一条保留中的记录。"""
    row = find_recon(snapshot_id)
    if row is None:
        row = {
            "快照编号": snapshot_id,
            "校验状态": "待校验",
            "回收状态": "保留中",
            "回收尝试次数": 0,
            "装载状态": "未装载",
            "可回收": False,
            "更新时间": _now(),
        }
        _recon_rows().append(row)
    return row


def find_checkpoint_by_code(checkpoint_code: str | None) -> dict[str, Any] | None:
    if not checkpoint_code:
        return None
    for row in store.rows("checkpoint"):
        if row.get("检查点编号") == checkpoint_code:
            return row
    return None


def _active_refs(snapshot_id: str) -> list[str]:
    """快照当前被哪些存活检查点引用：直接依赖 + 对账清单里的装载回写。"""
    refs: list[str] = []
    for ckpt in store.rows("checkpoint"):
        deps = ckpt.get("依赖快照") or []
        if ckpt.get("status") in ACTIVE_CHECKPOINT_STATUSES and snapshot_id in deps:
            refs.append(ckpt.get("检查点编号"))
    recon = find_recon(snapshot_id)
    if recon and recon.get("装载状态") == "已装载":
        loader_code = recon.get("装载检查点")
        loader = find_checkpoint_by_code(loader_code)
        if loader and loader.get("status") in ACTIVE_CHECKPOINT_STATUSES and loader_code not in refs:
            refs.append(loader_code)
    return refs


@dataclass(frozen=True)
class SnapshotRef:
    """已校验的快照引用：回收端与恢复端共用同一份判断结论。"""

    snapshot_id: str
    exists: bool
    persisted: bool
    has_content: bool
    expired: bool
    referenced_by: tuple[str, ...] = field(default_factory=tuple)
    released: bool = False
    reclaiming: bool = False
    loaded: bool = False

    @property
    def releasable(self) -> bool:
        """回收端口径：已落库、已过期、无存活引用、未释放、未在回收中。"""
        return (
            self.exists and self.persisted and self.expired
            and not self.referenced_by and not self.released and not self.reclaiming
        )

    @property
    def readable(self) -> bool:
        """恢复端口径：已落库、有内容、未释放的才能被直接读取。"""
        return self.exists and self.persisted and self.has_content and not self.released

    def to_dict(self) -> dict[str, Any]:
        return {
            "快照编号": self.snapshot_id,
            "存在": self.exists,
            "已落库": self.persisted,
            "有内容": self.has_content,
            "过期": self.expired,
            "引用检查点": list(self.referenced_by),
            "已释放": self.released,
            "回收中": self.reclaiming,
            "已装载": self.loaded,
            "可释放": self.releasable,
            "可读取": self.readable,
        }


def checked_snapshot_ref(snapshot_id: str) -> SnapshotRef:
    """抽取已校验的快照引用：回收与恢复共用的引用判断，双方以此核对。"""
    snap = find_snapshot(snapshot_id)
    recon = find_recon(snapshot_id)
    return SnapshotRef(
        snapshot_id=snapshot_id,
        exists=snap is not None,
        persisted=bool(snap and snap.get("落库状态") == "已落库"),
        has_content=bool(snap and snap.get("数据内容")),
        expired=bool(snap and snap.get("过期标记")),
        referenced_by=tuple(_active_refs(snapshot_id)),
        released=bool(recon and recon.get("回收状态") == "已释放"),
        reclaiming=bool(recon and recon.get("回收状态") in RECLAIM_IN_FLIGHT_STATUSES),
        loaded=bool(recon and recon.get("装载状态") == "已装载"),
    )


def build_consolidation_index() -> dict[str, Any]:
    """依据算子状态数据与检查点的依赖关系生成快照整理索引。

    落库失败（或内容为空）的快照不能留在恢复点上：把检查点依赖改指到
    替代内容，修复记录（原编号 -> 替代内容）写进索引与对账清单供追查。
    """
    snapshots = {row.get("快照编号"): row for row in _snapshots()}
    repairs: list[dict[str, Any]] = []
    checkpoint_refs: dict[str, list[str]] = {}
    snapshot_refs: dict[str, dict[str, list[str]]] = {
        sid: {"被引用检查点": [], "存活引用": []} for sid in snapshots
    }

    for ckpt in store.rows("checkpoint"):
        cid = ckpt.get("检查点编号")
        raw_deps = list(ckpt.get("依赖快照") or [])
        fixed: list[str] = []
        for sid in raw_deps:
            snap = snapshots.get(sid)
            if snap is None:
                repairs.append({"检查点编号": cid, "原编号": sid, "替代内容": None, "原因": "快照不存在"})
                continue
            if snap.get("落库状态") != "已落库" or not snap.get("数据内容"):
                replacement = snap.get("替代内容")
                repairs.append({"检查点编号": cid, "原编号": sid, "替代内容": replacement, "原因": "落库失败"})
                recon = ensure_recon(sid)
                recon["校验状态"] = "校验失败"
                recon["失败原因"] = "落库失败"
                recon["替代内容"] = replacement
                recon["更新时间"] = _now()
                repl_snap = snapshots.get(replacement)
                if repl_snap and repl_snap.get("数据内容"):
                    fixed.append(replacement)
                continue
            fixed.append(sid)
        deduped = list(dict.fromkeys(fixed))
        if deduped != raw_deps:
            # 恢复点改指实际可读快照，不留指向空数据的依赖
            ckpt["依赖快照"] = deduped
        checkpoint_refs[cid] = deduped
        for sid in deduped:
            entry = snapshot_refs.setdefault(sid, {"被引用检查点": [], "存活引用": []})
            entry["被引用检查点"].append(cid)
            if ckpt.get("status") in ACTIVE_CHECKPOINT_STATUSES:
                entry["存活引用"].append(cid)

    entries: dict[str, Any] = {}
    for sid, snap in snapshots.items():
        refs = snapshot_refs.get(sid, {"被引用检查点": [], "存活引用": []})
        entries[sid] = {
            "快照编号": sid,
            "状态编号": snap.get("状态编号"),
            "过期": bool(snap.get("过期标记")),
            "落库状态": snap.get("落库状态"),
            "被引用检查点": refs["被引用检查点"],
            "存活引用": refs["存活引用"],
            "可回收": checked_snapshot_ref(sid).releasable,
        }

    previous = current_index(required=False)
    index = {
        "版本": int(previous.get("版本", 0)) + 1 if previous else 1,
        "生成时间": _now(),
        "entries": entries,
        "checkpoint_refs": checkpoint_refs,
        "repairs": repairs,
    }
    table = store.rows(INDEX_TABLE)
    table.clear()
    table.append(index)
    return index


def current_index(*, required: bool = True) -> dict[str, Any] | None:
    """读取已生成的整理索引；没有就现生成一份，保证恢复端随时可用。"""
    table = store.rows(INDEX_TABLE)
    if table:
        return table[0]
    if required:
        return build_consolidation_index()
    return None


def validate_cleanup() -> dict[str, Any]:
    """快照清理校验：重建整理索引，刷新对账清单的校验结论与可回收口径。"""
    index = build_consolidation_index()
    reclaimable: list[str] = []
    failed: list[str] = []
    freed_bytes = 0
    for snap in _snapshots():
        sid = snap.get("快照编号")
        recon = ensure_recon(sid)
        if recon.get("回收状态") == "已释放":
            # 已释放条目保留回收留痕，不再参与校验，也不可被直接读取
            recon["可回收"] = False
            recon["更新时间"] = _now()
            continue
        ref = checked_snapshot_ref(sid)
        if ref.persisted and ref.has_content:
            recon["校验状态"] = "已校验"
            recon.pop("失败原因", None)
        else:
            recon["校验状态"] = "校验失败"
            recon.setdefault("失败原因", "落库失败")
            failed.append(sid)
        recon["可回收"] = ref.releasable
        recon["引用检查点"] = list(ref.referenced_by)
        recon["更新时间"] = _now()
        if ref.releasable:
            reclaimable.append(sid)
            freed_bytes += int(snap.get("数据大小") or 0)
    return {
        "index": index,
        "可回收": reclaimable,
        "校验失败": failed,
        "可释放字节合计": freed_bytes,
        "校验时间": _now(),
    }


def _write_reclaim_record(
    snapshot_id: str,
    action: str,
    freed_bytes: int,
    reason: str | None,
    ref: SnapshotRef,
) -> dict[str, Any]:
    records = store.rows(RECLAIM_TABLE)
    record = {
        "记录编号": f"RECLAIM-{len(records) + 1:04d}",
        "快照编号": snapshot_id,
        "动作": action,
        "释放字节": freed_bytes,
        "失败原因": reason,
        "引用检查点": list(ref.referenced_by),
        "时间": _now(),
    }
    records.append(record)
    return record


def reclaim_snapshots(
    snapshot_ids: list[str] | None = None,
    *,
    simulate_failure: bool = False,
) -> dict[str, Any]:
    """执行快照回收：释放已过期数据占用，每个动作都落可核对的回收记录。

    是否可释放完全以 ``checked_snapshot_ref`` 为准——恢复端回写到对账清单
    的装载结果（装载状态/装载检查点）会在这里被一并核对，引用未解除就不放。
    ``simulate_failure`` 让首次回收尝试失败，用于演练回收与恢复重叠的冲突。
    """
    if snapshot_ids is None:
        candidates = [
            snap.get("快照编号")
            for snap in _snapshots()
            if checked_snapshot_ref(snap.get("快照编号")).releasable
        ]
    else:
        candidates = list(snapshot_ids)

    records: list[dict[str, Any]] = []
    for sid in candidates:
        snap = find_snapshot(sid)
        ref = checked_snapshot_ref(sid)
        if snap is None:
            records.append(_write_reclaim_record(sid, "释放失败", 0, "快照不存在", ref))
            continue
        recon = ensure_recon(sid)
        if recon.get("回收状态") == "已释放":
            continue  # 已释放的条目不重复回收，清单留痕即可
        recon["回收尝试次数"] = int(recon.get("回收尝试次数", 0)) + 1
        if simulate_failure and recon["回收尝试次数"] == 1:
            recon["回收状态"] = "回收失败"
            recon["更新时间"] = _now()
            records.append(_write_reclaim_record(sid, "释放失败", 0, "模拟回收失败（首次尝试）", ref))
            continue
        if not ref.releasable:
            recon["回收状态"] = "回收失败"
            recon["更新时间"] = _now()
            if ref.referenced_by:
                reason = "引用未解除"
            elif not ref.persisted:
                reason = "落库失败快照不可直接回收"
            elif not ref.expired:
                reason = "快照未过期"
            else:
                reason = "快照不可释放"
            records.append(_write_reclaim_record(sid, "释放失败", 0, reason, ref))
            continue
        freed = int(snap.get("数据大小") or 0)
        snap["数据内容"] = None
        snap["数据大小"] = 0
        recon["回收状态"] = "已释放"
        recon["释放时间"] = _now()
        recon["可回收"] = False
        recon["更新时间"] = _now()
        records.append(_write_reclaim_record(sid, "释放", freed, None, ref))
    return {
        "records": records,
        "释放字节合计": sum(int(r["释放字节"]) for r in records),
        "回收时间": _now(),
    }


def register_snapshot(
    state_code: str,
    content: dict[str, Any] | None,
    *,
    expired: bool = False,
    simulate_persist_failure: bool = False,
) -> tuple[dict[str, Any] | None, str | None]:
    """登记一条算子状态快照。

    落库失败时不留下指向空数据的恢复点：快照本体标记落库失败、内容为空，
    对账清单记录替代内容（同状态最近一条可读快照），且不向任何检查点依赖
    写入新编号；原编号与替代内容都可追查。
    """
    state = None
    for row in store.rows("state_store"):
        if row.get("状态编号") == state_code:
            state = row
            break
    if state is None:
        return None, f"算子状态 {state_code} 不存在"
    existing = [int(s.get("id", 0)) for s in _snapshots()]
    snapshot_id = f"SNAP-{max(existing, default=0) + 1:04d}"
    snap: dict[str, Any] = {
        "id": max(existing, default=0) + 1,
        "快照编号": snapshot_id,
        "状态编号": state_code,
        "关联算子": state.get("关联算子"),
        "创建时间": _now(),
        "过期标记": expired,
    }
    if simulate_persist_failure:
        replacement = None
        for candidate in reversed(_snapshots()):
            if candidate is snap:
                continue
            if (
                candidate.get("状态编号") == state_code
                and candidate.get("落库状态") == "已落库"
                and candidate.get("数据内容")
            ):
                replacement = candidate.get("快照编号")
                break
        snap.update({"落库状态": "落库失败", "数据内容": None, "数据大小": 0, "替代内容": replacement})
        _snapshots().append(snap)
        recon = ensure_recon(snapshot_id)
        recon["校验状态"] = "校验失败"
        recon["失败原因"] = "落库失败"
        recon["替代内容"] = replacement
        recon["更新时间"] = _now()
        return snap, None
    payload = content if content is not None else {}
    snap.update({
        "落库状态": "已落库",
        "数据内容": payload,
        "数据大小": len(json.dumps(payload, ensure_ascii=False)),
        "替代内容": None,
    })
    _snapshots().append(snap)
    ensure_recon(snapshot_id)
    state["最后快照"] = snapshot_id
    return snap, None


def restore_via_index(entry_id: int) -> dict[str, Any] | None:
    """断流恢复：通过快照整理索引装载有效内容，结果回写快照对账清单。

    * 装载目标来自整理索引；每条快照能否读取以共用引用判断为准。
    * 与回收动作重叠（回收中/回收失败）时在恢复排队里登记冲突，回收首次
      失败落定后恢复继续，最终用于确认的恢复内容必须完整。
    * 任一快照不可读即恢复失败，不留半成品；成功时检查点依赖改指实际读取
      的快照集合，保证检查点仍能追到实际读取的内容。
    """
    ckpt = store.find("checkpoint", entry_id)
    if ckpt is None:
        return None
    index = current_index()
    cid = ckpt.get("检查点编号")
    snap_ids = list((index.get("checkpoint_refs") or {}).get(cid, []))

    queue = store.rows(QUEUE_TABLE)
    ticket = {
        "恢复单号": f"REST-{len(queue) + 1:04d}",
        "检查点编号": cid,
        "状态": "排队中",
        "入队时间": _now(),
        "冲突": None,
    }
    queue.append(ticket)
    conflicting = [
        sid for sid in snap_ids
        if (recon := find_recon(sid)) and recon.get("回收状态") in RECLAIM_IN_FLIGHT_STATUSES
    ]
    if conflicting:
        ticket["冲突"] = "空间回收与恢复排队冲突"
        ticket["冲突快照"] = conflicting

    loaded: list[dict[str, Any]] = []
    missing: list[str] = []
    for sid in snap_ids:
        ref = checked_snapshot_ref(sid)
        if not ref.readable:
            # 已释放/未落库的条目不会被返回为可直接读取，恢复按缺失处理
            missing.append(sid)
            continue
        snap = find_snapshot(sid)
        loaded.append({"快照编号": sid, "内容": snap.get("数据内容"), "字节": snap.get("数据大小", 0)})
        recon = ensure_recon(sid)
        recon["装载状态"] = "已装载"
        recon["装载检查点"] = cid
        recon["实际读取快照"] = sid
        recon["校验状态"] = "已校验"
        recon["更新时间"] = _now()

    if missing or not loaded:
        ckpt["status"] = "恢复失败"
        ticket["状态"] = "失败"
        ticket["缺失快照"] = missing or ["无可用快照"]
        ticket["完成时间"] = _now()
        return {
            "ok": False,
            "message": f"检查点 {cid} 恢复失败：快照不可直接读取（{'、'.join(missing) or '无可用快照'}）",
            "恢复单号": ticket["恢复单号"],
            "缺失快照": missing,
            "冲突": ticket["冲突"],
        }

    ckpt["依赖快照"] = [item["快照编号"] for item in loaded]
    ckpt["status"] = "可用"
    ticket["状态"] = "完成"
    ticket["完成时间"] = _now()
    return {
        "ok": True,
        "message": f"检查点 {cid} 已通过快照整理索引恢复，装载 {len(loaded)} 条快照",
        "恢复单号": ticket["恢复单号"],
        "装载内容": loaded,
        "实际读取快照": [item["快照编号"] for item in loaded],
        "冲突": ticket["冲突"],
    }


def list_snapshots(*, readable_only: bool = False) -> list[dict[str, Any]]:
    """快照清单；readable_only 时只返回可直接读取的（已释放的一律不回）。"""
    rows: list[dict[str, Any]] = []
    for snap in _snapshots():
        sid = snap.get("快照编号")
        ref = checked_snapshot_ref(sid)
        if readable_only and not ref.readable:
            continue
        recon = ensure_recon(sid)
        row = dict(snap)
        row["校验状态"] = recon.get("校验状态")
        row["回收状态"] = recon.get("回收状态")
        row["装载状态"] = recon.get("装载状态")
        row["可直接读取"] = ref.readable
        row["引用检查点"] = list(ref.referenced_by)
        rows.append(row)
    return rows


def reconciliation_list() -> list[dict[str, Any]]:
    """快照对账清单：每个快照一条，供回收端与恢复端共同核对。"""
    for snap in _snapshots():
        ensure_recon(snap.get("快照编号"))
    return [dict(row) for row in _recon_rows()]


def reclaim_records() -> list[dict[str, Any]]:
    """可核对的快照回收记录。"""
    return [dict(row) for row in store.rows(RECLAIM_TABLE)]


def recovery_queue() -> list[dict[str, Any]]:
    """恢复排队记录：含与回收重叠时的冲突登记。"""
    return [dict(row) for row in store.rows(QUEUE_TABLE)]
