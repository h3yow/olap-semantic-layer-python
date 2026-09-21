"""数据源抽象：本地内存关系存储。不接入任何外部服务/数据库。

设计要点：
- 表为行（dict）列表，append 支持事实/维度变更的增量到达（含迟到数据）。
- snapshot() 返回某时刻不可变拷贝，供一致快照读使用。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterable

Row = dict[str, Any]


class DataSource(ABC):
    @abstractmethod
    def scan(self, table: str) -> list[Row]:
        ...

    @abstractmethod
    def table_names(self) -> list[str]:
        ...


class InMemoryDataSource(DataSource):
    """字典式内存数据源，测试与本地复现使用。"""

    def __init__(self, tables: dict[str, list[Row]] | None = None) -> None:
        # 每个表保存为 list[Row]；做防御性拷贝
        self._tables: dict[str, list[Row]] = {
            name: [dict(r) for r in rows] for name, rows in (tables or {}).items()
        }

    def load(self, table: str, rows: Iterable[Row]) -> None:
        """整表覆盖加载（建表/重放用）。"""
        self._tables[table] = [dict(r) for r in rows]

    def append(self, table: str, rows: Iterable[Row]) -> int:
        """增量追加事实或维度变更行，返回新增行数。"""
        bucket = self._tables.setdefault(table, [])
        added = 0
        for r in rows:
            bucket.append(dict(r))
            added += 1
        return added

    def scan(self, table: str) -> list[Row]:
        """返回当前数据的浅拷贝（行 dict 拷贝），调用方修改不影响存储。"""
        if table not in self._tables:
            raise KeyError(f"未知表: {table}")
        return [dict(r) for r in self._tables[table]]

    def snapshot(self, table: str) -> tuple[Row, ...]:
        """返回不可变元组视图，代表一个稳定读版本。"""
        if table not in self._tables:
            raise KeyError(f"未知表: {table}")
        return tuple(dict(r) for r in self._tables[table])

    def table_names(self) -> list[str]:
        return sorted(self._tables)
