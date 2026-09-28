"""口径版本留存：多版本并行保留、可配置上限与保护名单、淘汰记录。

设计要点：
- 同一物化目标可并行保留多套口径版本，按版本独立取回；
- 版本数量超过上限时淘汰最旧的非保护版本；
- 被淘汰的版本记入淘汰册，读取时抛 VersionEvictedError（可与
  “从未发布” version_not_found 和“查无此数据” 空结果明确区分）；
- 淘汰与读取在同一把锁下完成，互不破坏。
"""

from __future__ import annotations

from dataclasses import dataclass

from .errors import VersionEvictedError, VersionNotFoundError


@dataclass(frozen=True)
class RetentionPolicy:
    """版本留存策略。

    max_versions: 每个目标最多保留的版本数；None 表示不限制。
    protected: 受保护的版本号，永不淘汰（如已审计锁定的口径）。
    """

    max_versions: int | None = None
    protected: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if self.max_versions is not None and self.max_versions < 1:
            raise ValueError("max_versions 必须 >= 1")


@dataclass
class EvictionRecord:
    """一次淘汰的凭证：哪个版本、因何被淘汰。"""

    version: str
    reason: str
    evicted_after_publish: str  # 触发本次淘汰的新发布版本


class VersionTimeline:
    """单个物化目标的版本时间线：发布顺序、当前版本、淘汰册。

    非线程安全：调用方（MaterializationManager）需在同一把锁下访问，
    保证淘汰与读取互不破坏。
    """

    def __init__(self) -> None:
        self.published_order: list[str] = []      # 按发布先后排列的现存版本
        self.current: str | None = None           # 当前对外版本
        self.evicted: dict[str, EvictionRecord] = {}
        # 发布后运行时锁定的受保护版本（区别于建策略时的静态保护名单）
        self.protected: set[str] = set()

    def clone(self) -> "VersionTimeline":
        """复制一份时间线，供两阶段提交在副本上预演（不影响线上状态）。"""
        clone = VersionTimeline()
        clone.published_order = list(self.published_order)
        clone.current = self.current
        clone.evicted = dict(self.evicted)
        clone.protected = set(self.protected)
        return clone

    def protect(self, version: str) -> None:
        """发布后把一个现存版本锁定为受保护版本。

        锁定是持久的：之后无论再发布多少版本都不淘汰它。
        已淘汰版本 -> VersionEvictedError；从未发布 -> VersionNotFoundError，
        与读取路径的拒绝原因完全一致，便于调用方区分处理。
        """
        if version in self.published_order:
            self.protected.add(version)
            return
        if version in self.evicted:
            rec = self.evicted[version]
            raise VersionEvictedError(
                f"口径版本 {version} 已按留存策略淘汰（{rec.reason}），"
                "无法再锁定为受保护版本",
                detail={"version": version, "reason": rec.reason,
                        "evicted_after_publish": rec.evicted_after_publish})
        raise VersionNotFoundError(
            f"口径版本 {version} 从未发布过，无法锁定",
            detail={"version": version, "available": list(self.published_order)})

    def is_protected(self, version: str, policy: RetentionPolicy) -> bool:
        return version in policy.protected or version in self.protected

    def publish(self, version: str) -> None:
        """登记一次发布。重复发布同一版本幂等（不重复计入留存数）。"""
        if version in self.evicted:
            # 已淘汰的版本不允许原号复用，避免与历史发布混淆
            raise VersionEvictedError(
                f"口径版本 {version} 已被淘汰，不允许原号重新发布",
                detail={"version": version})
        if version not in self.published_order:
            self.published_order.append(version)
        self.current = version

    def evictions_for(self, policy: RetentionPolicy) -> list[str]:
        """按策略计算应淘汰的版本：超出的部分按发布先后取最旧的非保护版本。

        保护来源有两处且等效：建策略时声明的静态保护名单，与发布后通过
        protect() 运行时锁定的版本。保护优先于上限——若现存版本全部受保护，
        则一个都不淘汰（实际保留数可能超过 max_versions）。当前对外版本
        同样永不淘汰。
        """
        if policy.max_versions is None:
            return []
        excess = len(self.published_order) - policy.max_versions
        if excess <= 0:
            return []
        victims: list[str] = []
        for v in self.published_order:  # 最旧的在前
            if len(victims) >= excess:
                break
            if self.is_protected(v, policy) or v == self.current:
                continue
            victims.append(v)
        return victims

    def record_eviction(self, version: str, *, after_publish: str,
                        reason: str) -> None:
        self.published_order.remove(version)
        if self.current == version:
            # 当前版本不会被淘汰（它总是最新发布的），防御性处理
            self.current = self.published_order[-1] if self.published_order else None
        self.evicted[version] = EvictionRecord(
            version=version, reason=reason, evicted_after_publish=after_publish)

    def check_readable(self, version: str) -> None:
        """读取前校验：已淘汰 -> VersionEvictedError；从未发布 -> VersionNotFoundError。"""
        if version in self.published_order:
            return
        if version in self.evicted:
            rec = self.evicted[version]
            raise VersionEvictedError(
                f"口径版本 {version} 已按留存策略淘汰（{rec.reason}），"
                "该版本曾对外发布，但已超出留存窗口，无法取回",
                detail={"version": version, "reason": rec.reason,
                        "evicted_after_publish": rec.evicted_after_publish})
        raise VersionNotFoundError(
            f"口径版本 {version} 从未发布过",
            detail={"version": version,
                    "available": list(self.published_order)})
