"""快照生命周期测试：清理校验、整理索引、回收记录、断流恢复装载与对账清单。

覆盖的核对点：
* 依据算子状态数据与检查点的依赖关系生成快照整理索引；
* 落库失败不留指向空数据的恢复点，原编号与替代内容可追查；
* 回收与恢复共用同一份快照引用判断；
* 回收释放已过期数据占用并落可核对的回收记录；
* 恢复通过整理索引装载，实际装载结果回写快照对账清单；
* 失败一次的回收与恢复重叠时冲突可登记、恢复内容完整、快照可重读；
* 清单里已释放的条目不会被返回为可直接读取。
"""
from app.services import snapshot_gc
from app.store import store


def _recon(snapshot_id: str) -> dict:
    row = snapshot_gc.find_recon(snapshot_id)
    assert row is not None, f"对账清单缺少 {snapshot_id}"
    return row


def _checkpoint(code: str) -> dict:
    row = snapshot_gc.find_checkpoint_by_code(code)
    assert row is not None, f"检查点 {code} 不存在"
    return row


class TestCleanupValidate:
    def test_index_generated_from_dependencies(self):
        result = snapshot_gc.validate_cleanup()
        index = result["index"]
        # 依据检查点依赖关系生成：CHEC-0002 依赖 SNAP-0001/SNAP-0005
        assert index["checkpoint_refs"]["CHEC-0002"] == ["SNAP-0001", "SNAP-0005"]
        entry = index["entries"]["SNAP-0001"]
        assert "CHEC-0002" in entry["被引用检查点"]
        assert "CHEC-0002" in entry["存活引用"]

    def test_expired_unreferenced_marked_reclaimable(self):
        result = snapshot_gc.validate_cleanup()
        # SNAP-0002 只被已过期检查点引用、SNAP-0004 无引用且已过期
        assert set(result["可回收"]) == {"SNAP-0002", "SNAP-0004"}
        assert result["可释放字节合计"] == 4096 + 1024
        assert _recon("SNAP-0002")["可回收"] is True
        assert _recon("SNAP-0001")["可回收"] is False

    def test_recon_validation_status_refreshed(self):
        snapshot_gc.validate_cleanup()
        assert _recon("SNAP-0001")["校验状态"] == "已校验"
        assert _recon("SNAP-0003")["校验状态"] == "校验失败"


class TestPersistFailure:
    def test_no_recovery_point_to_empty_data(self):
        # CHEC-0001 种子数据指向落库失败的 SNAP-0003，校验后必须改指替代内容
        snapshot_gc.validate_cleanup()
        ckpt = _checkpoint("CHEC-0001")
        assert "SNAP-0003" not in (ckpt["依赖快照"] or [])
        assert ckpt["依赖快照"] == ["SNAP-0001"]

    def test_original_id_and_replaceable_content_traceable(self):
        result = snapshot_gc.validate_cleanup()
        repairs = [r for r in result["index"]["repairs"] if r["原编号"] == "SNAP-0003"]
        assert repairs and repairs[0]["替代内容"] == "SNAP-0001"
        recon = _recon("SNAP-0003")
        assert recon["校验状态"] == "校验失败"
        assert recon["失败原因"] == "落库失败"
        assert recon["替代内容"] == "SNAP-0001"

    def test_register_persist_failure_keeps_no_dangling_ref(self):
        snap, err = snapshot_gc.register_snapshot(
            "STAT-0002", {"kv": {"u": 1}}, simulate_persist_failure=True)
        assert err is None
        assert snap["落库状态"] == "落库失败"
        assert snap["数据内容"] is None
        # 替代内容指向同状态最近一条可读快照，可追查
        assert snap["替代内容"] == "SNAP-0005"
        recon = _recon(snap["快照编号"])
        assert recon["校验状态"] == "校验失败"
        assert recon["替代内容"] == "SNAP-0005"
        # 任何检查点依赖都不会指向这条空数据快照
        snapshot_gc.validate_cleanup()
        for ckpt in store.rows("checkpoint"):
            assert snap["快照编号"] not in (ckpt.get("依赖快照") or [])

    def test_restore_after_repair_reads_replacement(self):
        snapshot_gc.validate_cleanup()
        result = snapshot_gc.restore_via_index(1)  # CHEC-0001
        assert result["ok"] is True
        assert result["实际读取快照"] == ["SNAP-0001"]
        assert _checkpoint("CHEC-0001")["依赖快照"] == ["SNAP-0001"]


class TestReclaim:
    def test_reclaim_releases_expired_data(self):
        snapshot_gc.validate_cleanup()
        result = snapshot_gc.reclaim_snapshots(["SNAP-0002", "SNAP-0004"])
        assert result["释放字节合计"] == 4096 + 1024
        snap = snapshot_gc.find_snapshot("SNAP-0002")
        assert snap["数据内容"] is None
        assert snap["数据大小"] == 0
        assert _recon("SNAP-0002")["回收状态"] == "已释放"

    def test_reclaim_records_are_verifiable(self):
        snapshot_gc.validate_cleanup()
        snapshot_gc.reclaim_snapshots(["SNAP-0004"])
        records = snapshot_gc.reclaim_records()
        assert len(records) == 1
        record = records[0]
        assert record["记录编号"] == "RECLAIM-0001"
        assert record["快照编号"] == "SNAP-0004"
        assert record["动作"] == "释放"
        assert record["释放字节"] == 1024
        assert record["时间"]

    def test_reclaim_blocked_while_referenced(self):
        snapshot_gc.validate_cleanup()
        result = snapshot_gc.reclaim_snapshots(["SNAP-0001"])
        record = result["records"][0]
        assert record["动作"] == "释放失败"
        assert record["失败原因"] == "引用未解除"
        assert "CHEC-0002" in record["引用检查点"]
        # 数据内容未被释放
        assert snapshot_gc.find_snapshot("SNAP-0001")["数据内容"]
        assert _recon("SNAP-0001")["回收状态"] == "回收失败"

    def test_default_candidates_come_from_shared_judgment(self):
        snapshot_gc.validate_cleanup()
        result = snapshot_gc.reclaim_snapshots()
        released = {r["快照编号"] for r in result["records"] if r["动作"] == "释放"}
        assert released == {"SNAP-0002", "SNAP-0004"}


class TestSharedRefJudgment:
    def test_reclaim_and_restore_share_the_same_ref(self):
        snapshot_gc.validate_cleanup()
        ref = snapshot_gc.checked_snapshot_ref("SNAP-0005")
        # 同一份已校验引用：回收端看到不可释放，恢复端看到可读取
        assert ref.releasable is False
        assert ref.readable is True
        assert "CHEC-0002" in ref.referenced_by

    def test_restore_loads_what_reclaim_cannot_release(self):
        snapshot_gc.validate_cleanup()
        result = snapshot_gc.restore_via_index(2)  # CHEC-0002
        assert result["ok"] is True
        loaded_ids = {item["快照编号"] for item in result["装载内容"]}
        assert loaded_ids == {"SNAP-0001", "SNAP-0005"}
        # 装载后回收端仍判定引用未解除
        blocked = snapshot_gc.reclaim_snapshots(["SNAP-0005"])
        assert blocked["records"][0]["动作"] == "释放失败"
        assert blocked["records"][0]["失败原因"] == "引用未解除"


class TestRestoreViaIndex:
    def test_restore_loads_complete_content_via_index(self):
        snapshot_gc.validate_cleanup()
        result = snapshot_gc.restore_via_index(2)  # CHEC-0002
        assert result["ok"] is True
        contents = {item["快照编号"]: item["内容"] for item in result["装载内容"]}
        # 最终用于确认的恢复内容必须完整
        assert contents["SNAP-0001"] == {"offset": 1024, "kv": {"u1001": 3, "u1002": 1}}
        assert contents["SNAP-0005"] == {"offset": 4096, "kv": {"u1003": 8, "u1004": 2}}

    def test_checkpoint_traces_actually_read_snapshots(self):
        snapshot_gc.validate_cleanup()
        snapshot_gc.restore_via_index(2)
        # 检查点仍能追到实际读取的快照
        assert _checkpoint("CHEC-0002")["依赖快照"] == ["SNAP-0001", "SNAP-0005"]
        assert _checkpoint("CHEC-0002")["status"] == "可用"

    def test_load_result_written_back_to_recon(self):
        snapshot_gc.validate_cleanup()
        snapshot_gc.restore_via_index(2)
        recon = _recon("SNAP-0005")
        assert recon["装载状态"] == "已装载"
        assert recon["装载检查点"] == "CHEC-0002"
        assert recon["实际读取快照"] == "SNAP-0005"

    def test_restore_unknown_checkpoint_returns_none(self):
        assert snapshot_gc.restore_via_index(9999) is None


class TestReclaimRestoreOverlap:
    def test_failed_reclaim_then_restore_conflict_registered(self):
        snapshot_gc.validate_cleanup()
        # 首次回收失败一次：空间占用尚未释放
        first = snapshot_gc.reclaim_snapshots(["SNAP-0002"], simulate_failure=True)
        assert first["records"][0]["动作"] == "释放失败"
        assert _recon("SNAP-0002")["回收状态"] == "回收失败"
        # 回收过程中启动恢复：排队单登记空间与恢复的冲突
        result = snapshot_gc.restore_via_index(3)  # CHEC-0003 依赖 SNAP-0002
        assert result["ok"] is True
        assert result["冲突"] == "空间回收与恢复排队冲突"
        ticket = snapshot_gc.recovery_queue()[0]
        assert ticket["冲突快照"] == ["SNAP-0002"]
        assert ticket["状态"] == "完成"

    def test_recovery_content_complete_and_traceable(self):
        snapshot_gc.validate_cleanup()
        snapshot_gc.reclaim_snapshots(["SNAP-0002"], simulate_failure=True)
        result = snapshot_gc.restore_via_index(3)
        assert result["装载内容"][0]["内容"] == {"offset": 2048, "kv": {"u1003": 7}}
        assert _checkpoint("CHEC-0003")["依赖快照"] == ["SNAP-0002"]
        assert _checkpoint("CHEC-0003")["status"] == "可用"
        assert _recon("SNAP-0002")["实际读取快照"] == "SNAP-0002"

    def test_validated_snapshot_is_rereadable(self):
        snapshot_gc.validate_cleanup()
        snapshot_gc.reclaim_snapshots(["SNAP-0002"], simulate_failure=True)
        snapshot_gc.restore_via_index(3)
        readable = {row["快照编号"] for row in snapshot_gc.list_snapshots(readable_only=True)}
        # 恢复校验成功的快照应能重读
        assert "SNAP-0002" in readable

    def test_reclaim_blocked_after_restore_writeback(self):
        snapshot_gc.validate_cleanup()
        snapshot_gc.reclaim_snapshots(["SNAP-0002"], simulate_failure=True)
        snapshot_gc.restore_via_index(3)
        # 恢复装载结果已回写对账清单，回收端据此判断引用不可解除
        retry = snapshot_gc.reclaim_snapshots(["SNAP-0002"])
        assert retry["records"][0]["动作"] == "释放失败"
        assert retry["records"][0]["失败原因"] == "引用未解除"
        assert snapshot_gc.find_snapshot("SNAP-0002")["数据内容"]


class TestReleasedNotReadable:
    def test_released_entries_not_returned_as_readable(self):
        snapshot_gc.validate_cleanup()
        snapshot_gc.reclaim_snapshots(["SNAP-0002"])
        readable = {row["快照编号"] for row in snapshot_gc.list_snapshots(readable_only=True)}
        assert "SNAP-0002" not in readable
        # 完整清单里仍留痕，但标记为不可直接读取
        full = {row["快照编号"]: row for row in snapshot_gc.list_snapshots()}
        assert full["SNAP-0002"]["回收状态"] == "已释放"
        assert full["SNAP-0002"]["可直接读取"] is False

    def test_restore_fails_when_snapshot_released(self):
        snapshot_gc.validate_cleanup()
        snapshot_gc.reclaim_snapshots(["SNAP-0002"])  # CHEC-0003 已过期，不阻塞回收
        result = snapshot_gc.restore_via_index(3)
        assert result["ok"] is False
        assert "SNAP-0002" in result["缺失快照"]
        assert _checkpoint("CHEC-0003")["status"] == "恢复失败"
        # 已释放条目不会因为是恢复目标就变成可直接读取
        readable = {row["快照编号"] for row in snapshot_gc.list_snapshots(readable_only=True)}
        assert "SNAP-0002" not in readable
        assert _recon("SNAP-0002")["回收状态"] == "已释放"

    def test_released_entry_not_reclaimed_twice(self):
        snapshot_gc.validate_cleanup()
        snapshot_gc.reclaim_snapshots(["SNAP-0002"])
        again = snapshot_gc.reclaim_snapshots(["SNAP-0002"])
        assert again["records"] == []
        assert len(snapshot_gc.reclaim_records()) == 1
