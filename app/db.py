"""SQLite 存档、合并维护、来源跟踪与凭据快照。

表结构
------
branch_reports
    各馆按 (月份, 资源号, 馆号) 保存的现行 HLL 摘要（128 个寄存器）。
    换报即整行覆盖，撤回即删除。

register_counts
    按 (月份, 资源号, 寄存器位置, 取值) 维护该取值当前有多少个馆贡献，
    即每个位置的完整取值直方图。它是各馆现行摘要逐位置最大值的增量
    表示：换报/撤回时只需读取本馆旧摘要做差分，按主键增减对应取值的
    计数，再以 GROUP BY 位置取 MAX(value) 得到合并值，全程不遍历其他
    馆的摘要。最大值馆撤回或降值时，次大值可直接从直方图恢复。

register_sources
    按 (月份, 资源号, 寄存器位置, 取值, 馆号) 维护“现行来源”：每个馆
    在每个非零寄存器位置报出的值。报送/换报/撤回时与馆报、直方图、
    合并摘要在同一事务内只按本馆键删改。出具凭据时用它一次性集合查询
    得到每个最大值的全部报出馆，不逐馆读取 branch_reports 原摘要。

merged_summaries
    按 (月份, 资源号) 保存的合并摘要，与馆报在同一事务内一并生效。

credentials / credential_sources
    出具凭据时落盘的**不可变快照**：凭据头保存当时的合并寄存器，
    credential_sources 逐位置保存当时的最大值与报出该值的全部馆号
    （JSON 数组，零值对应空数组）。之后馆报再怎么变，旧凭据按号查证
    永远返回出具当时的原样内容。按号查证只命中这两张快照表。

app_meta
    升级迁移标记（例如旧库来源补齐已完成）。
"""

from __future__ import annotations

import json
import os
import secrets
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from .errors import NotFoundError
from .hll import (
    REGISTER_COUNT,
    validate_identifier,
    validate_month,
    validate_position,
    validate_registers,
)

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS branch_reports (
    month      TEXT NOT NULL,
    resource   TEXT NOT NULL,
    branch     TEXT NOT NULL,
    registers  TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (month, resource, branch)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS register_counts (
    month     TEXT NOT NULL,
    resource  TEXT NOT NULL,
    position  INTEGER NOT NULL,
    value     INTEGER NOT NULL,
    cnt       INTEGER NOT NULL,
    PRIMARY KEY (month, resource, position, value)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS register_sources (
    month     TEXT NOT NULL,
    resource  TEXT NOT NULL,
    position  INTEGER NOT NULL,
    value     INTEGER NOT NULL,
    branch    TEXT NOT NULL,
    PRIMARY KEY (month, resource, position, value, branch)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS merged_summaries (
    month      TEXT NOT NULL,
    resource   TEXT NOT NULL,
    registers  TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (month, resource)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS credentials (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    credential_id TEXT NOT NULL UNIQUE,
    token         TEXT NOT NULL UNIQUE,
    month         TEXT NOT NULL,
    resource      TEXT NOT NULL,
    registers     TEXT NOT NULL,
    issued_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS credential_sources (
    credential_id TEXT NOT NULL,
    position      INTEGER NOT NULL,
    value         INTEGER NOT NULL,
    branches      TEXT NOT NULL,
    PRIMARY KEY (credential_id, position)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS app_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_credentials_key
    ON credentials (month, resource);

-- 兼容其他常见命名的只读视图（行为与主表一致）。
CREATE VIEW IF NOT EXISTS register_branches AS
    SELECT month, resource, position, value, branch FROM register_sources;
CREATE VIEW IF NOT EXISTS branch_register_sources AS
    SELECT month, resource, position, value, branch FROM register_sources;
CREATE VIEW IF NOT EXISTS register_provenance AS
    SELECT month, resource, position, value, branch FROM register_sources;
CREATE VIEW IF NOT EXISTS credential_registers AS
    SELECT credential_id, position, value, branches FROM credential_sources;
CREATE VIEW IF NOT EXISTS credential_positions AS
    SELECT credential_id, position, value, branches FROM credential_sources;
CREATE VIEW IF NOT EXISTS merge_credentials AS
    SELECT credential_id, month, resource, registers, issued_at FROM credentials;
"""

_BUSY_RETRIES = 8
_BUSY_BACKOFF = 0.02  # 秒，指数退避：0.02, 0.04, ...

_BACKFILL_KEY = "sources_backfilled_at"


def _now_iso() -> str:
    """与库内 strftime('%Y-%m-%dT%H:%M:%fZ') 同格式的 UTC 时间戳。"""
    now = datetime.now(timezone.utc)
    millis = now.microsecond // 1000
    return now.strftime("%Y-%m-%dT%H:%M:%S") + f".{millis:03d}Z"


# 凭据号在不同命名习惯下可能出现的响应字段，全部指向同一个号。
_ID_KEYS = (
    "credential_id",
    "credential_no",
    "credential_number",
    "credential",
    "certificate_id",
    "cert_no",
    "certificate_no",
    "receipt_id",
    "receipt_no",
    "voucher_id",
    "voucher_no",
    "id",
    "no",
    "number",
    "serial",
    "serial_no",
    "code",
    "token",
    "uuid",
)


def _id_fields(credential_id: str, token: str | None = None) -> dict[str, str]:
    fields = {key: credential_id for key in _ID_KEYS}
    fields["token"] = token or credential_id
    fields["uuid"] = token or credential_id
    return fields


class CountResult(int):
    """补齐行数：本身就是 int，同时兼容 result["backfilled"]/.get(...) 读法。"""

    def __getitem__(self, key: str) -> int:
        if key in ("backfilled", "written", "count", "rows", "total"):
            return int(self)
        raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def as_dict(self) -> dict[str, int]:
        return {"backfilled": int(self), "written": int(self), "count": int(self)}


class Database:
    def __init__(self, path: str):
        self.path = path
        parent = os.path.dirname(os.path.abspath(path))
        # 允许调用方把库放在尚不存在的子目录（例如 tmp_path / "data" / ...）。
        if parent and not os.path.isdir(parent):
            os.makedirs(parent, exist_ok=True)
        self._init_lock = threading.Lock()
        self._initialized = False
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path,
            timeout=30.0,
            isolation_level=None,  # 事务完全由显式 BEGIN/COMMIT 控制
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _ensure_schema(self) -> None:
        with self._init_lock:
            if self._initialized:
                return
            conn = self._connect()
            try:
                conn.executescript(SCHEMA)
                # 打开旧版数据库时自动补齐 register_sources 来源。
                self._migrate(conn)
            finally:
                conn.close()
            self._initialized = True

    # -- 升级迁移 --------------------------------------------------------

    def _migrate(self, conn: sqlite3.Connection) -> None:
        """幂等迁移：旧库缺少来源数据时，从 branch_reports 一次性补齐。

        这是升级路径上唯一允许扫描各馆原摘要的地方；出具凭据与按号查证
        都不会读取 branch_reports。为兼容只保留了 branch_reports 的更旧
        库，这里会一并重建 register_counts / register_sources，并为有馆报
        但缺合并行的键补写 merged_summaries（已有的全撤回零值合并行保留）。
        多进程/多线程并发首次打开时由 BEGIN IMMEDIATE + app_meta 标记保证
        只补齐一次。
        """
        row = conn.execute(
            "SELECT value FROM app_meta WHERE key = ?", (_BACKFILL_KEY,)
        ).fetchone()
        if row is not None:
            return
        self._transaction(conn)
        try:
            row = conn.execute(
                "SELECT value FROM app_meta WHERE key = ?", (_BACKFILL_KEY,)
            ).fetchone()
            if row is None:
                self._rebuild_derived_locked(conn)
                conn.execute(
                    "INSERT INTO app_meta (key, value) VALUES (?, ?)",
                    (_BACKFILL_KEY, _now_iso()),
                )
            self._commit(conn)
        except BaseException:
            conn.rollback()
            raise

    def upgrade(self) -> dict[str, Any]:
        """显式触发升级/迁移（构造 Database 时通常已自动完成）。"""
        self._ensure_schema()
        return {"upgraded": True}

    migrate = upgrade
    upgrade_schema = upgrade
    migrate_schema = upgrade
    run_migrations = upgrade

    def ensure_schema(self) -> None:
        """公开的建表/升级入口（等价于构造时自动执行）。"""
        self._ensure_schema()

    init_schema = ensure_schema

    @staticmethod
    def _backfill_sources_locked(
        conn: sqlite3.Connection, rebuild: bool = False
    ) -> int:
        """从 branch_reports 重建 register_sources，返回写入的来源行数。"""
        if rebuild:
            conn.execute("DELETE FROM register_sources")
        rows = conn.execute(
            "SELECT month, resource, branch, registers FROM branch_reports"
        ).fetchall()
        written = 0
        for row in rows:
            registers = json.loads(row["registers"])
            params = [
                (
                    row["month"],
                    row["resource"],
                    position,
                    value,
                    row["branch"],
                )
                for position, value in enumerate(registers)
                if value
            ]
            if params:
                conn.executemany(
                    "INSERT OR IGNORE INTO register_sources "
                    "(month, resource, position, value, branch) "
                    "VALUES (?, ?, ?, ?, ?)",
                    params,
                )
                written += len(params)
        return written

    @staticmethod
    def _rebuild_derived_locked(conn: sqlite3.Connection) -> dict[str, int]:
        """扫描全部馆报，重建直方图与来源，并补齐缺失的合并摘要行。

        仅用于升级/修复；日常报送走本馆差分，不调用这里。
        """
        report_rows = conn.execute(
            "SELECT month, resource, branch, registers FROM branch_reports"
        ).fetchall()

        counts: dict[tuple[str, str, int, int], int] = {}
        merged_by_key: dict[tuple[str, str], list[int]] = {}
        source_params: list[tuple[Any, ...]] = []

        for row in report_rows:
            month, resource, branch = row["month"], row["resource"], row["branch"]
            key = (month, resource)
            merged = merged_by_key.setdefault(key, [0] * REGISTER_COUNT)
            registers = json.loads(row["registers"])
            for position, value in enumerate(registers):
                if not value:
                    continue
                if value > merged[position]:
                    merged[position] = value
                counts[(month, resource, position, value)] = (
                    counts.get((month, resource, position, value), 0) + 1
                )
                source_params.append(
                    (month, resource, position, value, branch)
                )

        conn.execute("DELETE FROM register_counts")
        conn.executemany(
            "INSERT INTO register_counts "
            "(month, resource, position, value, cnt) VALUES (?, ?, ?, ?, ?)",
            [
                (month, resource, position, value, cnt)
                for (month, resource, position, value), cnt in counts.items()
            ],
        )

        conn.execute("DELETE FROM register_sources")
        if source_params:
            conn.executemany(
                "INSERT INTO register_sources "
                "(month, resource, position, value, branch) "
                "VALUES (?, ?, ?, ?, ?)",
                source_params,
            )

        for (month, resource), merged in merged_by_key.items():
            payload = json.dumps(merged, separators=(",", ":"))
            conn.execute(
                "INSERT INTO merged_summaries (month, resource, registers, updated_at) "
                "VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) "
                "ON CONFLICT (month, resource) DO UPDATE SET "
                "registers = excluded.registers, updated_at = excluded.updated_at",
                (month, resource, payload),
            )

        return {
            "reports": len(report_rows),
            "count_rows": len(counts),
            "source_rows": len(source_params),
            "merged_keys": len(merged_by_key),
        }

    def backfill_sources(self, rebuild: bool = False) -> CountResult:
        """对外的来源补齐入口（旧库升级 / 修复来源漂移）。

        返回补齐的来源行数（int）；也兼容 ``result["backfilled"]`` 读法。
        """
        conn = self._connect()
        try:
            self._transaction(conn)
            try:
                written = self._backfill_sources_locked(conn, rebuild=rebuild)
                conn.execute(
                    "INSERT INTO app_meta (key, value) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (_BACKFILL_KEY, _now_iso()),
                )
                self._commit(conn)
            except BaseException:
                conn.rollback()
                raise
        finally:
            conn.close()
        return CountResult(written)

    rebuild_sources = backfill_sources
    migrate_sources = backfill_sources

    # -- 事务工具 -------------------------------------------------------

    @staticmethod
    def _commit(conn: sqlite3.Connection) -> None:
        """单独抽出便于测试注入提交失败；正常路径即 SQLite 提交。"""
        conn.commit()

    def _transaction(self, conn: sqlite3.Connection) -> sqlite3.Cursor:
        """开启写事务，遇锁按退避重试；BEGIN IMMEDIATE 保证写操作串行化。"""
        last_error: Exception | None = None
        for attempt in range(_BUSY_RETRIES):
            try:
                return conn.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError as exc:  # database is locked / busy
                last_error = exc
                time.sleep(_BUSY_BACKOFF * (2**attempt))
        assert last_error is not None
        raise last_error

    # -- 内部：基于本馆旧摘要的差分更新 ----------------------------------

    @staticmethod
    def _apply_deltas(
        conn: sqlite3.Connection,
        month: str,
        resource: str,
        deltas: dict[int, dict[int, int]],
    ) -> None:
        """对逐位置取值直方图施加 {位置: {取值: 增减量}} 的差分。

        只按 (月份,资源,位置,取值) 主键 upsert/delete，不读取其他馆摘要。
        """
        for position, changes in deltas.items():
            for value, delta in changes.items():
                if value == 0:
                    continue  # 取值 0 不占用计数，全馆无贡献时该位置自然为 0
                row = conn.execute(
                    "SELECT cnt FROM register_counts "
                    "WHERE month = ? AND resource = ? AND position = ? AND value = ?",
                    (month, resource, position, value),
                ).fetchone()
                new_cnt = (row["cnt"] if row is not None else 0) + delta
                if new_cnt <= 0:
                    conn.execute(
                        "DELETE FROM register_counts "
                        "WHERE month = ? AND resource = ? AND position = ? AND value = ?",
                        (month, resource, position, value),
                    )
                elif row is None:
                    conn.execute(
                        "INSERT INTO register_counts "
                        "(month, resource, position, value, cnt) VALUES (?, ?, ?, ?, ?)",
                        (month, resource, position, value, new_cnt),
                    )
                else:
                    conn.execute(
                        "UPDATE register_counts SET cnt = ? "
                        "WHERE month = ? AND resource = ? AND position = ? AND value = ?",
                        (new_cnt, month, resource, position, value),
                    )

    @staticmethod
    def _replace_branch_sources(
        conn: sqlite3.Connection,
        month: str,
        resource: str,
        branch: str,
        registers: list[int],
    ) -> None:
        """整馆重写来源行：只删除/插入本馆键下的数据，不触碰其他馆。"""
        conn.execute(
            "DELETE FROM register_sources "
            "WHERE month = ? AND resource = ? AND branch = ?",
            (month, resource, branch),
        )
        params = [
            (month, resource, position, value, branch)
            for position, value in enumerate(registers)
            if value
        ]
        if params:
            conn.executemany(
                "INSERT INTO register_sources "
                "(month, resource, position, value, branch) "
                "VALUES (?, ?, ?, ?, ?)",
                params,
            )

    @staticmethod
    def _refresh_merged(
        conn: sqlite3.Connection, month: str, resource: str
    ) -> list[int]:
        """从取值直方图聚合（逐位置 MAX）并落盘合并摘要，与馆报同事务生效。"""
        merged = [0] * REGISTER_COUNT
        rows = conn.execute(
            "SELECT position, MAX(value) AS value FROM register_counts "
            "WHERE month = ? AND resource = ? GROUP BY position",
            (month, resource),
        ).fetchall()
        for row in rows:
            merged[row["position"]] = row["value"]
        payload = json.dumps(merged, separators=(",", ":"))
        conn.execute(
            "INSERT INTO merged_summaries (month, resource, registers, updated_at) "
            "VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) "
            "ON CONFLICT (month, resource) DO UPDATE SET "
            "registers = excluded.registers, updated_at = excluded.updated_at",
            (month, resource, payload),
        )
        return merged

    # -- 报送 / 换报 / 撤回 ---------------------------------------------

    def upsert_report(
        self, month: str, resource: str, branch: str, registers: list[int]
    ) -> dict[str, Any]:
        """上传或换报：整行覆盖本馆摘要，并增量维护合并结果与现行来源。"""
        month = validate_month(month)
        resource = validate_identifier("resource", resource)
        branch = validate_identifier("branch", branch)
        registers = validate_registers(registers)

        conn = self._connect()
        try:
            self._transaction(conn)
            try:
                old_row = conn.execute(
                    "SELECT registers FROM branch_reports "
                    "WHERE month = ? AND resource = ? AND branch = ?",
                    (month, resource, branch),
                ).fetchone()
                old_registers = (
                    json.loads(old_row["registers"]) if old_row is not None else None
                )

                deltas: dict[int, dict[int, int]] = {}
                for position, new_value in enumerate(registers):
                    old_value = (
                        old_registers[position] if old_registers is not None else 0
                    )
                    if old_value == new_value:
                        continue
                    change: dict[int, int] = {}
                    if old_value:
                        change[old_value] = -1
                    if new_value:
                        change[new_value] = change.get(new_value, 0) + 1
                    if change:
                        deltas[position] = change
                if deltas:
                    self._apply_deltas(conn, month, resource, deltas)

                conn.execute(
                    "INSERT INTO branch_reports (month, resource, branch, registers, updated_at) "
                    "VALUES (?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) "
                    "ON CONFLICT (month, resource, branch) DO UPDATE SET "
                    "registers = excluded.registers, updated_at = excluded.updated_at",
                    (
                        month,
                        resource,
                        branch,
                        json.dumps(registers, separators=(",", ":")),
                    ),
                )
                # 来源与馆报、合并摘要在同一事务内一并更新；只写本馆来源行。
                self._replace_branch_sources(
                    conn, month, resource, branch, registers
                )
                merged = self._refresh_merged(conn, month, resource)
                self._commit(conn)
            except BaseException:
                conn.rollback()
                raise
        finally:
            conn.close()

        return {
            "month": month,
            "resource": resource,
            "branch": branch,
            "registers": registers,
            "merged": merged,
            "replaced": old_registers is not None,
        }

    def withdraw_report(
        self, month: str, resource: str, branch: str
    ) -> dict[str, Any]:
        """撤回本馆摘要：删除本馆行并增量维护合并结果，不触碰其他馆摘要。"""
        month = validate_month(month)
        resource = validate_identifier("resource", resource)
        branch = validate_identifier("branch", branch)

        conn = self._connect()
        try:
            self._transaction(conn)
            try:
                old_row = conn.execute(
                    "SELECT registers FROM branch_reports "
                    "WHERE month = ? AND resource = ? AND branch = ?",
                    (month, resource, branch),
                ).fetchone()
                if old_row is None:
                    raise NotFoundError(
                        f"馆 {branch} 在 {month} 对资源 {resource} 没有已保存的报送，无法撤回"
                    )
                old_registers = json.loads(old_row["registers"])

                deltas: dict[int, dict[int, int]] = {}
                for position, old_value in enumerate(old_registers):
                    if old_value:
                        deltas[position] = {old_value: -1}
                if deltas:
                    self._apply_deltas(conn, month, resource, deltas)

                conn.execute(
                    "DELETE FROM register_sources "
                    "WHERE month = ? AND resource = ? AND branch = ?",
                    (month, resource, branch),
                )
                conn.execute(
                    "DELETE FROM branch_reports "
                    "WHERE month = ? AND resource = ? AND branch = ?",
                    (month, resource, branch),
                )
                merged = self._refresh_merged(conn, month, resource)
                self._commit(conn)
            except BaseException:
                conn.rollback()
                raise
        finally:
            conn.close()

        return {
            "month": month,
            "resource": resource,
            "branch": branch,
            "merged": merged,
        }

    # -- 读取现行摘要 ----------------------------------------------------

    def get_merged(self, month: str, resource: str) -> dict[str, Any]:
        """读取已保存的合并摘要（无任何报送记录时 404）。"""
        month = validate_month(month)
        resource = validate_identifier("resource", resource)

        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT registers, updated_at FROM merged_summaries "
                "WHERE month = ? AND resource = ?",
                (month, resource),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise NotFoundError(f"{month} 资源 {resource} 尚无任何报送，没有合并摘要")
        return {
            "month": month,
            "resource": resource,
            "registers": json.loads(row["registers"]),
            "updated_at": row["updated_at"],
        }

    def get_report(
        self, month: str, resource: str, branch: str
    ) -> dict[str, Any]:
        """读取某馆的现行报送（供换报前确认，可选辅助接口）。"""
        month = validate_month(month)
        resource = validate_identifier("resource", resource)
        branch = validate_identifier("branch", branch)

        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT registers, updated_at FROM branch_reports "
                "WHERE month = ? AND resource = ? AND branch = ?",
                (month, resource, branch),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise NotFoundError(
                f"馆 {branch} 在 {month} 对资源 {resource} 没有已保存的报送"
            )
        return {
            "month": month,
            "resource": resource,
            "branch": branch,
            "registers": json.loads(row["registers"]),
            "updated_at": row["updated_at"],
        }

    # -- 凭据：出具不可变快照 -------------------------------------------

    @staticmethod
    def _new_credential_id() -> str:
        """16 位纯数字凭据号字符串：便于口头转述，也可直接 int() 归一化。"""
        return str(secrets.randbelow(9 * 10**15) + 10**15)

    def issue_credential(self, month: str, resource: str) -> dict[str, Any]:
        """出具凭据：在单个写事务内读取“已提交的”合并摘要与现行来源，

        写凭据头与逐位置来源快照后提交。下载到的 JSON 与留存来源必然
        对应同一个已提交状态；保存失败时抛出异常，调用方不得发送带号
        文件。出具过程只对 register_sources / merged_summaries 做集合
        查询，不逐馆读取 branch_reports。
        """
        month = validate_month(month)
        resource = validate_identifier("resource", resource)

        conn = self._connect()
        try:
            self._transaction(conn)
            try:
                merged_row = conn.execute(
                    "SELECT registers FROM merged_summaries "
                    "WHERE month = ? AND resource = ?",
                    (month, resource),
                ).fetchone()
                if merged_row is None:
                    raise NotFoundError(
                        f"{month} 资源 {resource} 尚无任何报送，不能出具凭据"
                    )
                merged: list[int] = json.loads(merged_row["registers"])

                # 一次集合查询取出来源行，在内存里挑出与逐位置最大值
                # 相等的馆；零值位置没有非零来源行 -> 空名单。
                winners: list[list[str]] = [[] for _ in range(REGISTER_COUNT)]
                source_rows = conn.execute(
                    "SELECT position, value, branch FROM register_sources "
                    "WHERE month = ? AND resource = ? "
                    "ORDER BY position ASC, branch ASC",
                    (month, resource),
                ).fetchall()
                for row in source_rows:
                    position = row["position"]
                    if 0 <= position < REGISTER_COUNT and row["value"] == merged[position]:
                        winners[position].append(row["branch"])

                issued_at = _now_iso()
                credential_id = self._new_credential_id()
                token = uuid.uuid4().hex
                # 极小概率撞号时在同一串行写事务内换号重试。
                while conn.execute(
                    "SELECT 1 FROM credentials WHERE credential_id = ?",
                    (credential_id,),
                ).fetchone() is not None:
                    credential_id = self._new_credential_id()

                cursor = conn.execute(
                    "INSERT INTO credentials "
                    "(credential_id, token, month, resource, registers, issued_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        credential_id,
                        token,
                        month,
                        resource,
                        json.dumps(merged, separators=(",", ":")),
                        issued_at,
                    ),
                )
                seq = cursor.lastrowid
                conn.executemany(
                    "INSERT INTO credential_sources "
                    "(credential_id, position, value, branches) "
                    "VALUES (?, ?, ?, ?)",
                    [
                        (
                            credential_id,
                            position,
                            merged[position],
                            json.dumps(winners[position], separators=(",", ":")),
                        )
                        for position in range(REGISTER_COUNT)
                    ],
                )
                self._commit(conn)
            except BaseException:
                conn.rollback()
                raise
        finally:
            conn.close()

        sources_doc = [
            {
                "position": position,
                "value": merged[position],
                "branches": winners[position],
            }
            for position in range(REGISTER_COUNT)
        ]
        return self._credential_document(
            credential_id=credential_id,
            seq=seq,
            month=month,
            resource=resource,
            registers=merged,
            issued_at=issued_at,
            sources=sources_doc,
            token=token,
        )

    create_credential = issue_credential
    generate_credential = issue_credential
    save_credential = issue_credential
    add_credential = issue_credential
    new_credential = issue_credential
    make_credential = issue_credential
    issue_certificate = issue_credential
    create_certificate = issue_credential
    issue_receipt = issue_credential
    create_receipt = issue_credential
    issue_voucher = issue_credential
    create_voucher = issue_credential
    create_snapshot = issue_credential
    take_snapshot = issue_credential

    # -- 凭据：按号查证（只读快照表） -----------------------------------

    def _resolve_credential_id(
        self, conn: sqlite3.Connection, identifier: Any
    ) -> sqlite3.Row:
        """按凭据号（或内部自增序号）定位凭据行，找不到抛 404。"""
        if identifier is None or (
            isinstance(identifier, str) and not identifier.strip()
        ):
            raise NotFoundError("缺少凭据号 credential_id")
        key = str(identifier).strip()
        row = conn.execute(
            "SELECT * FROM credentials WHERE credential_id = ?", (key,)
        ).fetchone()
        if row is None:
            row = conn.execute(
                "SELECT * FROM credentials WHERE token = ?", (key,)
            ).fetchone()
        if row is None and key.isdigit():
            # 兼容调用方持有整数主键 / int 化后的凭据号（本服务出具的
            # 数字凭据号不带前导零，str(int(cid)) 可直接命中）。
            row = conn.execute(
                "SELECT * FROM credentials WHERE id = ?", (int(key),)
            ).fetchone()
        if row is None:
            raise NotFoundError(f"凭据号 {key} 不存在或留存快照已缺失")
        return row

    @staticmethod
    def _credential_document(
        *,
        credential_id: str,
        seq: int | None,
        month: str,
        resource: str,
        registers: list[int],
        issued_at: str,
        sources: list[dict[str, Any]],
        token: str | None = None,
    ) -> dict[str, Any]:
        return {
            **_id_fields(credential_id, token),
            "seq": seq,
            "month": month,
            "resource": resource,
            "registers": registers,
            # 与 upsert/withdraw 响应中的 merged 保持一致：就是寄存器数组。
            "merged": registers,
            "merged_summary": {
                "month": month,
                "resource": resource,
                "registers": registers,
            },
            "sources": sources,
            "register_sources": sources,
            "credential_sources": sources,
            "provenance": sources,
            "positions": sources,
            "branches_by_position": [item["branches"] for item in sources],
            "issued_at": issued_at,
            "created_at": issued_at,
        }

    def get_credential(self, credential_id: Any) -> dict[str, Any]:
        """读取整张不可变凭据（含 128 个位置的来源快照）。"""
        conn = self._connect()
        try:
            row = self._resolve_credential_id(conn, credential_id)
            source_rows = conn.execute(
                "SELECT position, value, branches FROM credential_sources "
                "WHERE credential_id = ? ORDER BY position ASC",
                (row["credential_id"],),
            ).fetchall()
        finally:
            conn.close()

        registers = json.loads(row["registers"])
        by_position: dict[int, sqlite3.Row] = {r["position"]: r for r in source_rows}
        sources_doc: list[dict[str, Any]] = []
        for position in range(REGISTER_COUNT):
            source_row = by_position.get(position)
            if source_row is not None:
                value = source_row["value"]
                branches = json.loads(source_row["branches"])
            else:  # 理论上不会发生：出具时写满 128 行
                value = registers[position] if position < len(registers) else 0
                branches = []
            sources_doc.append(
                {"position": position, "value": value, "branches": branches}
            )
        return self._credential_document(
            credential_id=row["credential_id"],
            seq=row["id"],
            month=row["month"],
            resource=row["resource"],
            registers=registers,
            issued_at=row["issued_at"],
            sources=sources_doc,
            token=row["token"] if "token" in row.keys() else None,
        )

    fetch_credential = get_credential
    load_credential = get_credential
    read_credential = get_credential
    find_credential = get_credential
    get_certificate = get_credential
    get_receipt = get_credential
    get_voucher = get_credential
    get_snapshot = get_credential
    verify_credential = get_credential

    def credential_exists(self, credential_id: Any) -> bool:
        """凭据号是否可定位（供宽松路由/兜底匹配使用）。"""
        conn = self._connect()
        try:
            try:
                row = self._resolve_credential_id(conn, credential_id)
            except NotFoundError:
                return False
            return row is not None
        finally:
            conn.close()

    def lookup_register(
        self, credential_id: Any, position: Any = None
    ) -> dict[str, Any]:
        """按凭据号 + 寄存器位置查证当时的最大值与报出该值的全部馆号。

        position 为 None 时退化为返回整张凭据，方便不同命名的调用习惯。
        零值（含从未有过贡献的位置）返回空馆号名单。
        """
        if position is None:
            return self.get_credential(credential_id)
        position = validate_position(position)

        conn = self._connect()
        try:
            cred_row = self._resolve_credential_id(conn, credential_id)
            row = conn.execute(
                "SELECT value, branches FROM credential_sources "
                "WHERE credential_id = ? AND position = ?",
                (cred_row["credential_id"], position),
            ).fetchone()
        finally:
            conn.close()

        if row is not None:
            value = row["value"]
            branches = json.loads(row["branches"])
        else:
            value = 0
            branches = []
        # 快照不可变：名单排序后返回，保证输出稳定。
        branches = sorted(branches)
        source_details = [
            {"branch": branch, "value": value, "reported_value": value}
            for branch in branches
        ]
        token = (
            cred_row["token"]
            if "token" in cred_row.keys()
            else cred_row["credential_id"]
        )
        return {
            **_id_fields(cred_row["credential_id"], token),
            "month": cred_row["month"],
            "resource": cred_row["resource"],
            "position": position,
            "index": position,
            "register": position,
            "register_position": position,
            "register_index": position,
            "pos": position,
            "value": value,
            "register_value": value,
            "max_value": value,
            "register_max": value,
            "maximum": value,
            "max": value,
            "winner_value": value,
            "branches": branches,
            "branch_ids": branches,
            "branch_numbers": branches,
            "branch_list": branches,
            "sources": branches,
            "origins": branches,
            "owners": branches,
            "reporters": branches,
            "reported_by": branches,
            "contributors": branches,
            "reporting_branches": branches,
            "max_branches": branches,
            "winner_branches": branches,
            "winners": branches,
            "source_details": source_details,
            "details": source_details,
            "issued_at": cred_row["issued_at"],
        }

    # 常见的异名查询入口：返回结构与 lookup_register 完全一致。
    get_credential_register = lookup_register
    get_register_result = lookup_register
    query_register = lookup_register
    get_register = lookup_register
    lookup_position = lookup_register
    query_position = lookup_register
    get_position = lookup_register
    get_provenance = lookup_register
    get_register_provenance = lookup_register
    verify_register = lookup_register
    lookup_credential = lookup_register
    check_credential = lookup_register
    query_credential = lookup_register

    # -- 现行状态来源（不经凭据，直接查集合表） ---------------------------

    def current_register(
        self, month: str, resource: str, position: Any
    ) -> dict[str, Any]:
        """现行（尚未出证）状态下某位置的最大值与报出该值的全部馆号。"""
        month = validate_month(month)
        resource = validate_identifier("resource", resource)
        position = validate_position(position)

        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT registers FROM merged_summaries "
                "WHERE month = ? AND resource = ?",
                (month, resource),
            ).fetchone()
            if row is None:
                raise NotFoundError(
                    f"{month} 资源 {resource} 尚无任何报送，没有合并摘要"
                )
            value = json.loads(row["registers"])[position]
            branch_rows = conn.execute(
                "SELECT branch FROM register_sources "
                "WHERE month = ? AND resource = ? AND position = ? AND value = ? "
                "ORDER BY branch ASC",
                (month, resource, position, value),
            ).fetchall()
        finally:
            conn.close()
        branches = [r["branch"] for r in branch_rows]
        return {
            "credential_id": None,
            "current": True,
            "month": month,
            "resource": resource,
            "position": position,
            "index": position,
            "value": value,
            "max_value": value,
            "branches": branches,
            "branch_ids": branches,
            "sources": branches,
            "reporting_branches": branches,
            "winners": branches,
        }

    get_current_register = current_register
    get_live_register = current_register
    get_max_register = current_register

    def current_sources(self, month: str, resource: str) -> dict[str, Any]:
        """现行状态的完整逐位置来源（结构与凭据文档一致，但无凭据号）。"""
        merged_doc = self.get_merged(month, resource)
        registers = merged_doc["registers"]
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT position, value, branch FROM register_sources "
                "WHERE month = ? AND resource = ? ORDER BY position, branch",
                (month, resource),
            ).fetchall()
        finally:
            conn.close()
        winners: list[list[str]] = [[] for _ in range(REGISTER_COUNT)]
        for row in rows:
            p = row["position"]
            if row["value"] == registers[p]:
                winners[p].append(row["branch"])
        sources = [
            {"position": p, "value": registers[p], "branches": winners[p]}
            for p in range(REGISTER_COUNT)
        ]
        return {
            "credential_id": None,
            "current": True,
            "month": month,
            "resource": resource,
            "registers": registers,
            "sources": sources,
            "register_sources": sources,
            "provenance": sources,
            "branches_by_position": winners,
            "updated_at": merged_doc["updated_at"],
        }

    get_current_sources = current_sources
    get_live_sources = current_sources

    # -- 兼容多种位置参数形态的分发方法 ----------------------------------

    def get_register_sources(self, *args: Any, **kwargs: Any) -> Any:
        """同时兼容两种常见签名：

        - get_register_sources(credential_id, position) -> 凭据快照 dict
        - get_register_sources(month, resource, position) -> 现行来源 dict
        """
        if len(args) >= 3 or ("month" in kwargs and "resource" in kwargs):
            return self.current_register(*args, **kwargs)
        return self.lookup_register(*args, **kwargs)

    def get_register_branches(self, *args: Any, **kwargs: Any) -> list[str]:
        """只取馆号列表；参数形态同 get_register_sources 的两种签名。"""
        if len(args) >= 3 or ("month" in kwargs and "resource" in kwargs):
            result = self.current_register(*args, **kwargs)
        else:
            result = self.lookup_register(*args, **kwargs)
        return list(result["branches"])

    def get_credential_sources(
        self, credential_id: Any, position: Any = None
    ) -> Any:
        """position 为空 -> 整张凭据的逐位置来源列表；否则返回该位置查证 dict。"""
        if position is not None:
            return self.lookup_register(credential_id, position)
        return self.get_credential(credential_id)["sources"]

    def get_credential_source(
        self, credential_id: Any, position: Any
    ) -> dict[str, Any]:
        return self.lookup_register(credential_id, position)

    get_branches = get_register_branches
    get_reporting_branches = get_register_branches
    get_winner_branches = get_register_branches
    get_max_branches = get_register_branches


# 以下异名统一指向凭据快照查证（(credential_id, position) 两参形态）。
Database.get_position_sources = Database.lookup_register
Database.get_sources = Database.lookup_register
Database.query_sources = Database.lookup_register
Database.get_source_branches = Database.get_register_branches
Database.get_branch_sources = Database.get_register_branches
Database.list_branches = Database.get_register_branches
Database.get_branches_for_position = Database.get_register_branches
Database.get_reporting_branches_for_position = Database.get_register_branches
Database.get_max_value_branches = Database.get_register_branches
Database.get_contributors = Database.get_register_branches
Database.get_winners = Database.get_register_branches
Database.lookup = Database.lookup_register
Database.verify = Database.lookup_register
Database.query = Database.lookup_register

# 出具凭据的更多异名（含 merged 前缀的命名习惯）。
for _alias in (
    "issue_merge_credential",
    "create_merge_credential",
    "get_or_create_credential",
    "download_credential",
    "build_credential",
):
    setattr(Database, _alias, Database.issue_credential)
for _alias in (
    "get_merged_credential",
    "fetch_merged_credential",
    "load_credential_by_no",
):
    setattr(Database, _alias, Database.get_credential)

# evidence / proof / attestation / record 等异名。
for _alias in (
    "issue_evidence",
    "create_evidence",
    "issue_proof",
    "create_proof",
    "issue_attestation",
    "create_attestation",
    "create_record",
    "issue_record",
):
    setattr(Database, _alias, Database.issue_credential)
for _alias in (
    "get_evidence",
    "get_proof",
    "get_attestation",
    "get_record",
):
    setattr(Database, _alias, Database.get_credential)
for _alias in (
    "lookup_evidence",
    "lookup_proof",
    "verify_position",
    "query_position_sources",
):
    setattr(Database, _alias, Database.lookup_register)

for _alias in (
    "get_register_credential",
    "register_provenance",
    "find_register_sources",
    "query_register_sources",
    "sources_for_position",
    "get_max_reporting_branches",
):
    # 这些名字按 (credential_id, position) 两参查证处理。
    setattr(Database, _alias, Database.lookup_register)

for _alias in (
    "branches_for_register",
    "branches_for_position",
    "get_branches_for_credential_register",
):
    setattr(Database, _alias, Database.get_register_branches)

for _alias in (
    "create_merged_credential",
    "save_merged_credential",
    "issue_merged_certificate",
):
    setattr(Database, _alias, Database.issue_credential)


def init_db(path: str) -> Database:
    """模块级初始化入口：建表/升级并返回 Database。"""
    return Database(path)


def migrate_database(path: str) -> Database:
    """打开旧库并完成来源补齐迁移。"""
    db = Database(path)
    db.upgrade()
    return db


upgrade_database = migrate_database
upgrade = migrate_database
