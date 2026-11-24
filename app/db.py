"""SQLite 持久化：多版本事件账、暂存导入与锚点。

设计要点：
* 每次 CSV 导入生成一个新的 dataset 版本（staging → active → retired），
  事件与锚点都带 dataset_id，旧账与旧锚点整体保留、整体退出核查；
* 现用账由单行指针表 active_ledger 指向。换账提交在一个写事务内只做
  状态翻转与指针更新，不读写任何 event/anchor 行，写锁内的事件读写量
  恒为 O(1)，与新旧账的事件条数无关；
* 大批量事件写入发生在暂存事务里，不触碰现用账；配合 WAL，
  暂存写入期间核查/补登仍可读取现用账；
* 每个事件在导入时就写好 prefix_min/prefix_amb，核查任意事件对只需
  按 event_no 点查；核查所需的事件前缀量与锚点在同一个只读事务的
  快照内取出，保证取自同一已提交状态；
* 确认更正与复核放在同一事务内，失败或冲突即整体回滚，原账保留；
* 识别并迁移旧版（v1，单行 dataset + 无 dataset_id 的 event/anchor）库，
  升级后旧账与旧锚点可继续使用。
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from .reconcile import analyze_counts

SCHEMA_VERSION = 2

SCHEMA_STATEMENTS = [
    """
CREATE TABLE IF NOT EXISTS dataset (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT NOT NULL,
    event_count INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('staging', 'active', 'retired')),
    expected_active_id INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
)
""",
    """
CREATE TABLE IF NOT EXISTS event (
    dataset_id INTEGER NOT NULL,
    event_no INTEGER NOT NULL,
    count INTEGER NOT NULL,
    prefix_min INTEGER NOT NULL,
    prefix_amb INTEGER NOT NULL,
    PRIMARY KEY (dataset_id, event_no),
    FOREIGN KEY (dataset_id) REFERENCES dataset(id) ON DELETE CASCADE
)
""",
    """
CREATE TABLE IF NOT EXISTS anchor (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id INTEGER NOT NULL,
    event_no INTEGER NOT NULL,
    utc_ms INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (dataset_id, event_no),
    FOREIGN KEY (dataset_id) REFERENCES dataset(id) ON DELETE CASCADE
)
""",
    """
CREATE TABLE IF NOT EXISTS active_ledger (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    dataset_id INTEGER NOT NULL,
    FOREIGN KEY (dataset_id) REFERENCES dataset(id)
)
""",
]


def _create_schema(conn: sqlite3.Connection) -> None:
    # 逐条执行而非 executescript：后者会隐式提交，破坏迁移事务的原子性。
    for stmt in SCHEMA_STATEMENTS:
        conn.execute(stmt)


class ImportConflict(RuntimeError):
    """换账冲突：导入开始后现用账已被别的导入替换，本次换账必须退回。"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    # WAL：暂存批量写不阻塞现用账的读；busy_timeout 避免锁等待直接报错。
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    _ensure_schema(conn)
    return conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}


def _has_table(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _ensure_schema(conn: sqlite3.Connection) -> None:
    """建表或把旧版库迁移到多版本结构，幂等。"""
    if not _has_table(conn, "dataset"):
        _create_schema(conn)
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        return

    if _table_columns(conn, "dataset").issuperset({"status", "expected_active_id"}):
        # 新版结构可能由旧版本建出但缺指针表，逐个补齐（IF NOT EXISTS 幂等）
        _create_schema(conn)
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        return

    # ---- v1 → v2 迁移：旧表为 dataset(id 恒为 1)/event/anchor，无 dataset_id ----
    conn.execute("BEGIN")
    try:
        conn.execute("ALTER TABLE dataset RENAME TO dataset_legacy")
        conn.execute("ALTER TABLE event RENAME TO event_legacy")
        conn.execute("ALTER TABLE anchor RENAME TO anchor_legacy")
        _create_schema(conn)

        legacy = conn.execute(
            "SELECT id, filename, event_count, created_at FROM dataset_legacy"
        ).fetchone()
        if legacy is not None:
            conn.execute(
                "INSERT INTO dataset (id, filename, event_count, status, created_at)"
                " VALUES (?, ?, ?, 'active', ?)",
                (legacy["id"], legacy["filename"], legacy["event_count"],
                 legacy["created_at"]),
            )
            conn.execute(
                "INSERT INTO event (dataset_id, event_no, count, prefix_min, prefix_amb)"
                " SELECT dl.id, el.event_no, el.count, el.prefix_min, el.prefix_amb"
                " FROM event_legacy el JOIN dataset_legacy dl"
            )
            conn.execute(
                "INSERT INTO anchor (id, dataset_id, event_no, utc_ms, created_at)"
                " SELECT al.id, dl.id, al.event_no, al.utc_ms, al.created_at"
                " FROM anchor_legacy al JOIN dataset_legacy dl"
            )
            conn.execute(
                "INSERT INTO active_ledger (id, dataset_id) VALUES (1, ?)",
                (legacy["id"],),
            )
        conn.execute("DROP TABLE dataset_legacy")
        conn.execute("DROP TABLE event_legacy")
        conn.execute("DROP TABLE anchor_legacy")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")


# ---------- 现用账指针 ----------

def get_active_dataset_id(conn: sqlite3.Connection) -> int | None:
    row = conn.execute(
        "SELECT dataset_id FROM active_ledger WHERE id = 1"
    ).fetchone()
    return row["dataset_id"] if row else None


def has_dataset(conn: sqlite3.Connection) -> bool:
    return get_active_dataset_id(conn) is not None


def get_dataset_meta(
    conn: sqlite3.Connection, dataset_id: int | None = None
) -> dict | None:
    if dataset_id is None:
        dataset_id = get_active_dataset_id(conn)
    if dataset_id is None:
        return None
    row = conn.execute(
        "SELECT id, filename, event_count, status, expected_active_id, created_at"
        " FROM dataset WHERE id = ?",
        (dataset_id,),
    ).fetchone()
    return dict(row) if row else None


def list_datasets(conn: sqlite3.Connection) -> list[dict]:
    """全部账版本：现用、暂存（待启用）、已退出（retired）。"""
    return [
        dict(r)
        for r in conn.execute(
            "SELECT id, filename, event_count, status, expected_active_id, created_at"
            " FROM dataset ORDER BY id"
        )
    ]


def list_pending_imports(conn: sqlite3.Connection) -> list[dict]:
    return [
        dict(r)
        for r in conn.execute(
            "SELECT id, filename, event_count, expected_active_id, created_at"
            " FROM dataset WHERE status = 'staging' ORDER BY id"
        )
    ]


# ---------- 两阶段导入 ----------

def stage_import(
    conn: sqlite3.Connection,
    filename: str,
    counts: list[int],
    expected_active_id: int | None,
) -> dict:
    """整份文件校验并预算后写入一个 staging 版本，不触碰现用账与现用锚点。

    analyze_counts 沿用原有计数规则，且在任何写库之前完成；
    写入失败由事务回滚，现用账保持不变。
    """
    records = analyze_counts(counts)
    with conn:
        cur = conn.execute(
            "INSERT INTO dataset (filename, event_count, status, expected_active_id)"
            " VALUES (?, ?, 'staging', ?)",
            (filename, len(records), expected_active_id),
        )
        import_id = cur.lastrowid
        conn.executemany(
            "INSERT INTO event (dataset_id, event_no, count, prefix_min, prefix_amb)"
            " VALUES (?, ?, ?, ?, ?)",
            [
                (import_id, r["event_no"], r["count"], r["prefix_min"], r["prefix_amb"])
                for r in records
            ],
        )
    meta = get_dataset_meta(conn, import_id)
    assert meta is not None
    return meta


def commit_import(conn: sqlite3.Connection, import_id: int) -> dict:
    """启用暂存账：写锁内只翻转状态、更新指针，O(1)，不读写事件/锚点。

    暂存时记录的 expected_active_id 与当前指针不一致即换账冲突
    （哪怕两份文件同名同条数），抛 ImportConflict 并保留现用账。
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT id, status, expected_active_id FROM dataset WHERE id = ?",
            (import_id,),
        ).fetchone()
        if row is None:
            raise LookupError(f"暂存导入 {import_id} 不存在")
        if row["status"] != "staging":
            raise ImportConflict(
                f"导入 {import_id} 状态为 {row['status']}，不能重复启用"
            )
        current_id = get_active_dataset_id(conn)
        if current_id != row["expected_active_id"]:
            raise ImportConflict(
                "导入开始后已有另一份账生效，本次换账退回，现用账与锚点保留"
            )
        if current_id is not None:
            conn.execute(
                "UPDATE dataset SET status = 'retired' WHERE id = ?", (current_id,)
            )
        conn.execute(
            "UPDATE dataset SET status = 'active' WHERE id = ?", (import_id,)
        )
        conn.execute(
            "INSERT INTO active_ledger (id, dataset_id) VALUES (1, ?)"
            " ON CONFLICT(id) DO UPDATE SET dataset_id = excluded.dataset_id",
            (import_id,),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return get_dataset_meta(conn, import_id)


def discard_import(conn: sqlite3.Connection, import_id: int) -> bool:
    """放弃暂存账（级联删除其事件）；现用账不受影响。"""
    with conn:
        cur = conn.execute(
            "DELETE FROM dataset WHERE id = ? AND status = 'staging'", (import_id,)
        )
        return cur.rowcount > 0


def replace_dataset(
    conn: sqlite3.Connection, filename: str, counts: list[int]
) -> dict:
    """兼容旧接口：暂存 + 立即提交的一次性换账。"""
    expected = get_active_dataset_id(conn)
    staged = stage_import(conn, filename, counts, expected)
    commit_import(conn, staged["id"])
    meta = get_dataset_meta(conn)
    assert meta is not None
    return meta


# ---------- 事件（现用账） ----------

def load_records(
    conn: sqlite3.Connection, dataset_id: int
) -> dict[int, tuple[int, int]]:
    """取出某版本全部事件的前缀量（字典点查，核查时不扫描事件）。"""
    return {
        row["event_no"]: (row["prefix_min"], row["prefix_amb"])
        for row in conn.execute(
            "SELECT event_no, prefix_min, prefix_amb FROM event"
            " WHERE dataset_id = ? ORDER BY event_no",
            (dataset_id,),
        )
    }


def event_exists(
    conn: sqlite3.Connection, event_no: int, dataset_id: int | None = None
) -> bool:
    if dataset_id is None:
        dataset_id = get_active_dataset_id(conn)
    return conn.execute(
        "SELECT 1 FROM event WHERE dataset_id = ? AND event_no = ?",
        (dataset_id, event_no),
    ).fetchone() is not None


def list_events(
    conn: sqlite3.Connection, limit: int | None = None
) -> list[dict]:
    dataset_id = get_active_dataset_id(conn)
    if dataset_id is None:
        return []
    sql = (
        "SELECT event_no, count, prefix_min, prefix_amb FROM event"
        " WHERE dataset_id = ? ORDER BY event_no"
    )
    params: list = [dataset_id]
    if limit:
        sql += " LIMIT ?"
        params.append(int(limit))
    return [dict(r) for r in conn.execute(sql, params)]


# ---------- 锚点（现用账） ----------

def list_anchors(conn: sqlite3.Connection) -> list[dict]:
    dataset_id = get_active_dataset_id(conn)
    if dataset_id is None:
        return []
    return [
        dict(r)
        for r in conn.execute(
            "SELECT id, event_no, utc_ms, created_at FROM anchor"
            " WHERE dataset_id = ? ORDER BY event_no",
            (dataset_id,),
        )
    ]


def add_anchor(conn: sqlite3.Connection, event_no: int, utc_ms: int) -> dict:
    """补登记锚点；事件号重复时抛 IntegrityError，由调用层保留原账。"""
    dataset_id = get_active_dataset_id(conn)
    cur = conn.execute(
        "INSERT INTO anchor (dataset_id, event_no, utc_ms) VALUES (?, ?, ?)",
        (dataset_id, event_no, utc_ms),
    )
    row = conn.execute(
        "SELECT id, event_no, utc_ms, created_at FROM anchor WHERE id = ?",
        (cur.lastrowid,),
    ).fetchone()
    return dict(row)


def get_anchor(conn: sqlite3.Connection, anchor_id: int) -> dict | None:
    row = conn.execute(
        "SELECT id, dataset_id, event_no, utc_ms FROM anchor WHERE id = ?",
        (anchor_id,),
    ).fetchone()
    return dict(row) if row else None


def delete_anchor(conn: sqlite3.Connection, anchor_id: int) -> bool:
    cur = conn.execute("DELETE FROM anchor WHERE id = ?", (anchor_id,))
    return cur.rowcount > 0


def update_anchor_utc(conn: sqlite3.Connection, anchor_id: int, utc_ms: int) -> bool:
    cur = conn.execute(
        "UPDATE anchor SET utc_ms = ? WHERE id = ?", (utc_ms, anchor_id)
    )
    return cur.rowcount > 0


# ---------- 同一已提交状态快照 ----------

def read_snapshot(conn: sqlite3.Connection) -> tuple[int, dict, dict, list[dict]] | None:
    """在一个只读事务里取现用账的 (id, meta, 事件前缀量, 锚点)。

    事务期间别的连接即使提交了换账，本快照仍看到同一已提交状态。
    """
    conn.execute("BEGIN")
    try:
        active_id = get_active_dataset_id(conn)
        if active_id is None:
            conn.execute("COMMIT")
            return None
        meta = get_dataset_meta(conn, active_id)
        records = load_records(conn, active_id)
        anchors = [
            dict(r)
            for r in conn.execute(
                "SELECT id, event_no, utc_ms, created_at FROM anchor"
                " WHERE dataset_id = ? ORDER BY event_no",
                (active_id,),
            )
        ]
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return active_id, meta, records, anchors
