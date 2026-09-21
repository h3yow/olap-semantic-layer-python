"""版本化物化存储。

每个物化键保存一串不可变的已提交版本；每次刷新在暂存区构建完整新状态，
成功后才原子发布指针，失败则旧版本继续可见、暂存被清理：

Entry = {
  "snapshot_version": int,        # 发布时的快照版本
  "definition_version": int,      # 口径定义版本（度量版本集合的指纹）
  "committed_at": str,
  "partitions": {                 # partition_id -> {group_key(json)} -> {measure: encoded_state}
     ...
  },
  "partition_meta": {partition_id: {"updated_at": ..., "source_watermark": ...}},
}

关键不变量：
- 同一分区重复提交是「替换」而非累加，天然幂等，不会重复计入；
- 读方按 begin_read 得到的快照版本读取对应 Entry，看不到写入中的暂存；
- 任何时刻至多一个已发布版本（指针切换是原子的）。
"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from typing import Any

from .errors import MaterializationConflictError, ResourceLeakError


def group_key_json(group_key: tuple) -> str:
    return json.dumps(list(group_key), ensure_ascii=False, default=str)


class _Entry:
    __slots__ = ("snapshot_version", "definition_version", "committed_at",
                 "partitions", "partition_meta")

    def __init__(self, snapshot_version: int, definition_version: int,
                 partitions: dict, partition_meta: dict) -> None:
        self.snapshot_version = snapshot_version
        self.definition_version = definition_version
        self.committed_at = datetime.now(timezone.utc).isoformat()
        self.partitions = partitions
        self.partition_meta = partition_meta

    def to_dict(self) -> dict:
        return {
            "snapshot_version": self.snapshot_version,
            "definition_version": self.definition_version,
            "committed_at": self.committed_at,
            "partitions": self.partitions,
            "partition_meta": self.partition_meta,
        }


class MaterializationStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._history: dict[str, list[_Entry]] = {}
        # 活跃暂存：key -> {staging_version -> 可变状态}；用于失败时检测残留
        self._staging: dict[str, dict[int, dict]] = {}

    # ---- 暂存生命周期 ------------------------------------------------
    def begin_staging(self, key: str, snapshot_version: int) -> int:
        """基于最新已提交状态复制出一份可变暂存（copy-on-write）。"""
        with self._lock:
            base = self._latest_entry(key)
            staging = {
                "snapshot_version": snapshot_version,
                "definition_version": base.definition_version if base else 1,
                "partitions": json.loads(json.dumps(
                    base.partitions if base else {}, default=str)),
                "partition_meta": dict(base.partition_meta if base else {}),
            }
            self._staging.setdefault(key, {})[snapshot_version] = staging
        return snapshot_version

    def put_partition(self, key: str, staging_version: int, partition_id: str,
                      groups: dict[str, dict[str, dict]],
                      source_watermark: str | None = None,
                      definition_version: int | None = None) -> None:
        """用本次重算结果「替换」单个分区（幂等）。groups 已为编码状态。"""
        staging = self._get_staging(key, staging_version)
        if definition_version is not None:
            staging["definition_version"] = definition_version
        # 替换语义：删除旧分区后写入，保证重复提交不累加
        staging["partitions"].pop(partition_id, None)
        staging["partitions"][partition_id] = groups
        staging["partition_meta"][partition_id] = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "source_watermark": source_watermark,
        }

    def remove_partition(self, key: str, staging_version: int, partition_id: str) -> None:
        staging = self._get_staging(key, staging_version)
        staging["partitions"].pop(partition_id, None)
        staging["partition_meta"].pop(partition_id, None)

    def commit(self, key: str, staging_version: int) -> dict:
        staging = self._get_staging(key, staging_version)
        with self._lock:
            entry = _Entry(
                snapshot_version=staging_version,
                definition_version=staging["definition_version"],
                partitions=staging["partitions"],
                partition_meta=staging["partition_meta"],
            )
            self._history.setdefault(key, []).append(entry)
            self._discard_staging(key, staging_version)
            return entry.to_dict()

    def abort(self, key: str, staging_version: int) -> None:
        """刷新失败：丢弃暂存，已发布版本不变。"""
        self._discard_staging(key, staging_version, raise_if_missing=False)

    def assert_no_leftovers(self) -> None:
        """供测试/关闭时调用：存在未结束暂存即视为资源/半成品泄漏。"""
        with self._lock:
            leftover = {k: sorted(v) for k, v in self._staging.items() if v}
            if leftover:
                raise ResourceLeakError(
                    f"检测到 {sum(len(v) for v in leftover.values())} 个未释放暂存",
                    reason="STAGING_LEFTOVER",
                    details={"staging": leftover},
                )

    # ---- 读 -----------------------------------------------------------
    def read(self, key: str, as_of_snapshot: int | None = None) -> dict:
        with self._lock:
            history = self._history.get(key, [])
            if not history:
                return self._empty_view(0)
            if as_of_snapshot is None:
                return history[-1].to_dict()
            chosen = None
            for entry in history:
                if entry.snapshot_version <= as_of_snapshot:
                    chosen = entry
                else:
                    break
            if chosen is None:
                return self._empty_view(0)
            return chosen.to_dict()

    def latest_snapshot_version(self, key: str) -> int:
        with self._lock:
            entry = self._latest_entry(key)
            return entry.snapshot_version if entry else 0

    def definition_version(self, key: str) -> int:
        with self._lock:
            entry = self._latest_entry(key)
            return entry.definition_version if entry else 1

    def partitions(self, key: str, as_of_snapshot: int | None = None) -> list[str]:
        return sorted(self.read(key, as_of_snapshot)["partitions"])

    # ---- 内部 ---------------------------------------------------------
    def _latest_entry(self, key: str) -> _Entry | None:
        history = self._history.get(key)
        return history[-1] if history else None

    def _get_staging(self, key: str, version: int) -> dict:
        with self._lock:
            bucket = self._staging.get(key)
            if not bucket or version not in bucket:
                raise MaterializationConflictError(
                    f"物化 {key} 暂存版本 {version} 不存在或已结束",
                    reason="STAGING_NOT_FOUND",
                    details={"key": key, "staging_version": version},
                )
            return bucket[version]

    def _discard_staging(self, key: str, version: int,
                         raise_if_missing: bool = True) -> None:
        with self._lock:
            bucket = self._staging.get(key, {})
            if version not in bucket:
                if raise_if_missing:
                    raise MaterializationConflictError(
                        f"重复结束暂存 {key}@{version}",
                        reason="STAGING_ALREADY_FINISHED",
                    )
                return
            del bucket[version]

    @staticmethod
    def _empty_view(snapshot_version: int) -> dict:
        return {
            "snapshot_version": snapshot_version,
            "definition_version": 1,
            "committed_at": None,
            "partitions": {},
            "partition_meta": {},
        }
