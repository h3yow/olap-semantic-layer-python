"""口径版本留存：同一物化目标并行保留多套版本，超限按策略淘汰。

语义保证：
- 口径调整后以新版本发布，历史版本不被覆盖，可按版本逐行原样取回；
- 版本数超过 RetentionPolicy.max_versions 时，淘汰最旧的非保护版本
  （当前最新版本永不淘汰）；受保护版本（已锁定对外的口径）不参与淘汰；
- 被淘汰的版本留下墓碑：再次访问时报 version_evicted（曾存在但已超期），
  与 version_not_found（从未发布）明确区分，绝不悄悄返回空结果；
- 发布/淘汰与读取互斥（同一把锁），读取拿到的是完整版本快照，
  淘汰不会破坏仍在保留范围内的版本。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any

from .errors import VersionError, VersionEvictedError, VersionNotFoundError


@dataclass(frozen=True)
class RetentionPolicy:
    """版本留存策略。

    - max_versions: 同一目标最多保留的版本数；当前最新版本始终保留，
      若其余版本全部受保护导致无法淘汰，则允许暂时超限（留存让位于可审计性）；
    - protected: 受保护版本（如已对外发布锁定的口径），永不被淘汰。
    """

    max_versions: int = 3
    protected: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if self.max_versions < 1:
            raise VersionError(
                "max_versions 必须 >= 1",
                detail={"max_versions": self.max_versions})


class VersionedResultStore:
    """单物化目标的多版本结果存储。"""

    def __init__(self, target: str, policy: RetentionPolicy | None = None) -> None:
        self._target = target
        self._policy = policy or RetentionPolicy()
        self._lock = threading.RLock()
        # version -> {(partition, group_key): row}
        self._versions: dict[str, dict[tuple, dict[str, Any]]] = {}
        self._publish_order: list[str] = []
        self._protected: set[str] = set(self._policy.protected)
        self._evicted: list[str] = []  # 淘汰墓碑：读旧版本时给出明确拒绝原因

    def publish(self, version: str, rows: dict[tuple, dict[str, Any]]) -> list[str]:
        """发布/合并一个版本。返回本次被淘汰的版本列表。

        - 新版本：独立开槽存放，不覆盖任何其他版本；
        - 已存在的版本：按行键 (partition, group_key) 覆盖式合并——
          用于同一口径版本内因迟到数据重刷分区，未涉及的分区行保持原样；
          重复发布同内容幂等（覆盖同值，结果不变）。
        """
        with self._lock:
            if version in self._evicted:
                raise VersionEvictedError(
                    f"目标 {self._target} 的版本 {version} 已被淘汰，不允许重新发布同名版本",
                    detail={"target": self._target, "version": version})
            slot = self._versions.get(version)
            if slot is None:
                slot = {}
                self._versions[version] = slot
                self._publish_order.append(version)
            for key, row in rows.items():
                slot[key] = dict(row)
            return self._evict_overflow()

    def _evict_overflow(self) -> list[str]:
        """超限时淘汰最旧的非保护、非当前版本。调用方须持锁。"""
        evicted_now: list[str] = []
        while len(self._versions) > self._policy.max_versions:
            current = self._publish_order[-1]
            candidate = next(
                (v for v in self._publish_order
                 if v != current and v not in self._protected),
                None)
            if candidate is None:
                break  # 其余全部受保护：留存让位于可审计性，允许暂时超限
            del self._versions[candidate]
            self._publish_order.remove(candidate)
            self._evicted.append(candidate)
            evicted_now.append(candidate)
        return evicted_now

    def read(self, version: str) -> list[dict[str, Any]]:
        """按版本取回结果快照。

        - 版本保留中：返回完整行集（与发布时逐行一致）；
        - 版本已被淘汰：version_evicted（曾发布但超期淘汰）；
        - 版本从未发布：version_not_found（查无此版本）。
        """
        with self._lock:
            rows = self._versions.get(version)
            if rows is not None:
                return [dict(r) for r in rows.values()]
            if version in self._evicted:
                raise VersionEvictedError(
                    f"目标 {self._target} 的版本 {version} 已被留存策略淘汰，"
                    "无法原样取回（曾发布过）",
                    detail={"target": self._target, "version": version,
                            "retained": list(self._publish_order)})
            raise VersionNotFoundError(
                f"目标 {self._target} 从未发布过版本 {version}",
                detail={"target": self._target, "version": version,
                        "retained": list(self._publish_order)})

    def protect(self, version: str) -> None:
        """将版本标记为受保护（不被淘汰）。版本不存在时拒绝，避免保护了个笔误。"""
        with self._lock:
            if version not in self._versions:
                raise VersionNotFoundError(
                    f"目标 {self._target} 不存在版本 {version}，无法标记保护",
                    detail={"target": self._target, "version": version})
            self._protected.add(version)

    def versions(self) -> list[str]:
        """当前保留中的版本（按发布顺序）。"""
        with self._lock:
            return list(self._publish_order)

    def evicted(self) -> list[str]:
        """已被淘汰的版本（墓碑，按淘汰顺序）。"""
        with self._lock:
            return list(self._evicted)
