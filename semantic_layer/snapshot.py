"""多版本快照协调（轻量 MVCC）。

- begin_read() 取当前已提交版本号：读事务期间的并发提交不影响该读视图；
- begin_write() 串行化所有写（全局写锁），返回新版本号；
- commit_write() 发布新版本，等待中的读仍看旧版本；
- abort_write() 放弃，不推进已提交版本；
- 实际状态存储由 materialization.MaterializationStore 按版本保存不可变快照。

本模块只负责版本编排与锁语义，不关心具体负载类型。
"""
from __future__ import annotations

import threading


class SnapshotManager:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._write_held = threading.Lock()
        self._committed_version = 0
        self._next_version = 1
        self._active_reads = 0
        self._read_done = threading.Condition(self._lock)

    def begin_read(self) -> int:
        with self._lock:
            self._active_reads += 1
            return self._committed_version

    def end_read(self) -> None:
        with self._lock:
            self._active_reads -= 1
            if self._active_reads == 0:
                self._read_done.notify_all()

    def begin_write(self, timeout: float = 30.0) -> int:
        """获取写互斥（写之间完全串行），返回待提交的新版本号。"""
        if not self._write_held.acquire(timeout=timeout):
            from .errors import MaterializationConflictError
            raise MaterializationConflictError(
                "获取写锁超时，存在并发刷新",
                reason="WRITE_LOCK_TIMEOUT",
            )
        with self._lock:
            version = self._next_version
            self._next_version += 1
            return version

    def commit_write(self, version: int) -> int:
        with self._lock:
            # 只允许提交最新的待写版本，旧版本必须先 abort
            self._committed_version = version
            committed = self._committed_version
        self._write_held.release()
        return committed

    def abort_write(self, version: int) -> None:
        with self._lock:
            # 回退预占版本号，保证版本号不产生可见空洞假象
            if version == self._next_version - 1:
                self._next_version -= 1
        try:
            self._write_held.release()
        except RuntimeError:
            pass

    def wait_reads_drain(self, timeout: float = 30.0) -> bool:
        """等待所有活跃读结束（清理旧版本前使用）。"""
        with self._read_done:
            return self._read_done.wait_for(
                lambda: self._active_reads == 0, timeout=timeout
            )

    @property
    def current_version(self) -> int:
        with self._lock:
            return self._committed_version
