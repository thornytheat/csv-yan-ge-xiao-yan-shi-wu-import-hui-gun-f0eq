"""SQLite 访问（薄封装，无 ORM）。

表：
- meta(key,value)                  seed 版本
- batches(...)                     已入库批次，sha256 用于重复文件判定
- fact_orders(...)                 明细事实，仅 committed 批次写入
- batch_aggregates(...)            按 site 聚合快照
- import_attempts(...)             每次上传的结论（含回滚记录）/review 用
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from . import settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batches (
  batch_id TEXT PRIMARY KEY,
  filename TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  encoding TEXT NOT NULL,
  bom INTEGER NOT NULL DEFAULT 0,
  row_count INTEGER NOT NULL,
  total_amount_cents INTEGER NOT NULL,
  total_qty INTEGER NOT NULL,
  forced INTEGER NOT NULL DEFAULT 0,
  superseded_by TEXT,
  created_at TEXT NOT NULL,
  note TEXT
);
CREATE INDEX IF NOT EXISTS idx_batches_sha ON batches(sha256);

CREATE TABLE IF NOT EXISTS fact_orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL,
  row_no INTEGER NOT NULL,
  order_id INTEGER NOT NULL,
  site TEXT NOT NULL,
  category TEXT NOT NULL,
  order_date TEXT NOT NULL,
  amount_cents INTEGER NOT NULL,
  qty INTEGER NOT NULL,
  memo TEXT
);
CREATE INDEX IF NOT EXISTS idx_fact_batch ON fact_orders(batch_id);

CREATE TABLE IF NOT EXISTS batch_aggregates (
  batch_id TEXT NOT NULL,
  site TEXT NOT NULL,
  amount_cents INTEGER NOT NULL,
  qty INTEGER NOT NULL,
  row_count INTEGER NOT NULL,
  PRIMARY KEY (batch_id, site)
);

CREATE TABLE IF NOT EXISTS import_attempts (
  attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
  filename TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  encoding TEXT,
  status TEXT NOT NULL,
  error_count INTEGER NOT NULL DEFAULT 0,
  row_count INTEGER NOT NULL DEFAULT 0,
  batch_id TEXT,
  detail TEXT,
  errors_json TEXT,
  created_at TEXT NOT NULL
);

-- 分片续传会话：一个文件身份（file_id=完整文件 sha256）至多一条会话
CREATE TABLE IF NOT EXISTS chunk_sessions (
  file_id TEXT PRIMARY KEY,
  filename TEXT NOT NULL,
  version TEXT NOT NULL,
  total_size INTEGER NOT NULL,
  segment_size INTEGER NOT NULL,
  total_segments INTEGER NOT NULL,
  total_rows INTEGER NOT NULL,
  amounts_json TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'open',
  received_count INTEGER NOT NULL DEFAULT 0,
  quarantine_reason TEXT NOT NULL DEFAULT '',
  gap_json TEXT NOT NULL DEFAULT '[]',
  mismatch_json TEXT NOT NULL DEFAULT '[]',
  batch_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sessions_status ON chunk_sessions(status);

-- 已收片段：先到的片段落这里作为续传依据；(file_id,index) 唯一，重传同段幂等
CREATE TABLE IF NOT EXISTS chunk_segments (
  file_id TEXT NOT NULL,
  segment_index INTEGER NOT NULL,
  checksum TEXT NOT NULL,
  size_bytes INTEGER NOT NULL,
  start_offset INTEGER NOT NULL,
  end_offset INTEGER NOT NULL,
  stored_path TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (file_id, segment_index)
);

-- 每个逻辑文件（filename）当前对外发布的版本；切换只改这一行，失败不动它
CREATE TABLE IF NOT EXISTS published_files (
  filename TEXT PRIMARY KEY,
  file_id TEXT NOT NULL,
  version TEXT NOT NULL,
  batch_id TEXT NOT NULL,
  total_rows INTEGER NOT NULL,
  published_at TEXT NOT NULL
);
"""

# 分片会话允许的状态
SESSION_STATUSES = ("open", "quarantined", "published", "stale_rejected")

# 允许的上传策略（避免路由随意传值）
STATUSES = ("committed", "duplicate_file", "rejected", "encoding_failed", "rolled_back")


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def connect(path: Path | str | None = None) -> sqlite3.Connection:
    target = Path(path) if path else settings.DB_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(target), isolation_level=None, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


def get_meta(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


def count_rows(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) AS c FROM fact_orders").fetchone()["c"]


def find_batch_by_sha(conn: sqlite3.Connection, sha256: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM batches WHERE sha256 = ? ORDER BY created_at ASC LIMIT 1", (sha256,)
    ).fetchone()


def list_batches(conn: sqlite3.Connection, limit: int = 30) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM batches ORDER BY created_at DESC, batch_id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [
        {
            "batch_id": r["batch_id"],
            "filename": r["filename"],
            "sha256": r["sha256"],
            "encoding": r["encoding"],
            "bom": bool(r["bom"]),
            "row_count": r["row_count"],
            "total_amount": f"{r['total_amount_cents'] // 100}.{r['total_amount_cents'] % 100:02d}",
            "total_qty": r["total_qty"],
            "forced": bool(r["forced"]),
            "superseded_by": r["superseded_by"],
            "created_at": r["created_at"],
        }
        for r in rows
    ]


def get_batch(conn: sqlite3.Connection, batch_id: str) -> dict | None:
    row = conn.execute("SELECT * FROM batches WHERE batch_id = ?", (batch_id,)).fetchone()
    if not row:
        return None
    agg = [
        {
            "site": a["site"],
            "amount": f"{a['amount_cents'] // 100}.{a['amount_cents'] % 100:02d}",
            "amount_cents": a["amount_cents"],
            "qty": a["qty"],
            "row_count": a["row_count"],
        }
        for a in conn.execute(
            "SELECT * FROM batch_aggregates WHERE batch_id = ? ORDER BY amount_cents DESC",
            (batch_id,),
        ).fetchall()
    ]
    sample = [
        dict(r) for r in conn.execute(
            "SELECT row_no, order_id, site, category, order_date, amount_cents, qty, memo "
            "FROM fact_orders WHERE batch_id = ? ORDER BY row_no LIMIT 20",
            (batch_id,),
        ).fetchall()
    ]
    for r in sample:
        r["amount"] = f"{r['amount_cents'] // 100}.{r['amount_cents'] % 100:02d}"
    return {
        "batch_id": row["batch_id"],
        "filename": row["filename"],
        "sha256": row["sha256"],
        "encoding": row["encoding"],
        "bom": bool(row["bom"]),
        "row_count": row["row_count"],
        "total_amount": f"{row['total_amount_cents'] // 100}.{row['total_amount_cents'] % 100:02d}",
        "total_amount_cents": row["total_amount_cents"],
        "total_qty": row["total_qty"],
        "forced": bool(row["forced"]),
        "superseded_by": row["superseded_by"],
        "created_at": row["created_at"],
        "note": row["note"],
        "aggregates": agg,
        "sample_rows": sample,
    }


def list_attempts(conn: sqlite3.Connection, limit: int = 30) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM import_attempts ORDER BY attempt_id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]


def get_attempt(conn: sqlite3.Connection, attempt_id: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM import_attempts WHERE attempt_id = ?", (attempt_id,)
    ).fetchone()
    if not row:
        return None
    data = dict(row)
    try:
        data["errors"] = json.loads(data.get("errors_json") or "[]")
    except json.JSONDecodeError:
        data["errors"] = []
    return data


def record_attempt(conn: sqlite3.Connection, *, filename: str, sha256: str, status: str,
                   encoding: str = "", error_count: int = 0, row_count: int = 0,
                   batch_id: str | None = None, detail: str = "",
                   errors: list[dict] | None = None) -> int:
    payload = json.dumps(errors[:200] if errors else [], ensure_ascii=False)
    cur = conn.execute(
        "INSERT INTO import_attempts(filename, sha256, encoding, status, error_count,"
        " row_count, batch_id, detail, errors_json, created_at)"
        " VALUES(?,?,?,?,?,?,?,?,?,?)",
        (filename, sha256, encoding, status, error_count, row_count, batch_id, detail,
         payload, now_iso()),
    )
    return int(cur.lastrowid or 0)


def stats(conn: sqlite3.Connection) -> dict:
    batches = conn.execute("SELECT COUNT(*) AS c FROM batches").fetchone()["c"]
    fact = count_rows(conn)
    rejected = conn.execute(
        "SELECT COUNT(*) AS c FROM import_attempts WHERE status IN ('rejected','encoding_failed')"
    ).fetchone()["c"]
    sites = conn.execute("SELECT COUNT(DISTINCT site) AS c FROM fact_orders").fetchone()["c"]
    return {"batches": batches, "fact_rows": fact, "rejected_attempts": rejected, "sites": sites}


# ---------------------------------------------------------------- 分片续传


def _session_row_to_dict(row: sqlite3.Row) -> dict:
    data = dict(row)
    for key in ("amounts_json", "gap_json", "mismatch_json"):
        raw = data.pop(key, "[]")
        try:
            data[key.replace("_json", "")] = json.loads(raw or "[]")
        except json.JSONDecodeError:
            data[key.replace("_json", "")] = []
    return data


def get_session(conn: sqlite3.Connection, file_id: str) -> dict | None:
    row = conn.execute(
        "SELECT * FROM chunk_sessions WHERE file_id = ?", (file_id,)
    ).fetchone()
    return _session_row_to_dict(row) if row else None


def create_session(conn: sqlite3.Connection, manifest: dict) -> dict:
    """幂等创建；同 file_id 已存在则直接返回既有会话（重试同一文件不新增批次/会话）。"""
    existing = get_session(conn, manifest["file_id"])
    if existing:
        return existing
    now = now_iso()
    conn.execute(
        "INSERT INTO chunk_sessions(file_id, filename, version, total_size, segment_size,"
        " total_segments, total_rows, amounts_json, status, received_count,"
        " quarantine_reason, gap_json, mismatch_json, created_at, updated_at)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (manifest["file_id"], manifest["filename"], manifest["version"],
         manifest["total_size"], manifest["segment_size"], manifest["total_segments"],
         manifest["total_rows"], json.dumps(manifest["amounts"], ensure_ascii=False),
         "open", 0, "", "[]", "[]", now, now),
    )
    return get_session(conn, manifest["file_id"])  # type: ignore[return-value]


def touch_session(conn: sqlite3.Connection, file_id: str) -> None:
    conn.execute(
        "UPDATE chunk_sessions SET updated_at = ? WHERE file_id = ?",
        (now_iso(), file_id),
    )


def update_session_status(conn: sqlite3.Connection, file_id: str, status: str, *,
                          quarantine_reason: str = "", gaps: list | None = None,
                          mismatches: list | None = None, batch_id: str | None = None) -> None:
    if status not in SESSION_STATUSES:
        raise ValueError(f"非法会话状态 {status}")
    conn.execute(
        "UPDATE chunk_sessions SET status = ?, quarantine_reason = ?, gap_json = ?,"
        " mismatch_json = ?, batch_id = COALESCE(?, batch_id), updated_at = ?"
        " WHERE file_id = ?",
        (status, quarantine_reason,
         json.dumps(gaps or [], ensure_ascii=False),
         json.dumps(mismatches or [], ensure_ascii=False),
         batch_id, now_iso(), file_id),
    )


def list_sessions(conn: sqlite3.Connection, limit: int = 30) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM chunk_sessions ORDER BY updated_at DESC, file_id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [_session_row_to_dict(r) for r in rows]


def get_segment(conn: sqlite3.Connection, file_id: str, index: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM chunk_segments WHERE file_id = ? AND segment_index = ?",
        (file_id, index),
    ).fetchone()
    return dict(row) if row else None


def list_segments(conn: sqlite3.Connection, file_id: str) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM chunk_segments WHERE file_id = ? ORDER BY segment_index ASC",
        (file_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def received_indexes(conn: sqlite3.Connection, file_id: str) -> list[int]:
    rows = conn.execute(
        "SELECT segment_index FROM chunk_segments WHERE file_id = ? ORDER BY segment_index ASC",
        (file_id,),
    ).fetchall()
    return [r["segment_index"] for r in rows]


def upsert_segment(conn: sqlite3.Connection, *, file_id: str, index: int, checksum: str,
                   size_bytes: int, start_offset: int, end_offset: int,
                   stored_path: str) -> None:
    """同段重传覆盖（先删旧文件记录再插新），靠主键保证并发下只有一条。"""
    conn.execute(
        "INSERT INTO chunk_segments(file_id, segment_index, checksum, size_bytes,"
        " start_offset, end_offset, stored_path, created_at)"
        " VALUES(?,?,?,?,?,?,?,?)"
        " ON CONFLICT(file_id, segment_index) DO UPDATE SET"
        " checksum=excluded.checksum, size_bytes=excluded.size_bytes,"
        " start_offset=excluded.start_offset, end_offset=excluded.end_offset,"
        " stored_path=excluded.stored_path, created_at=excluded.created_at",
        (file_id, index, checksum, size_bytes, start_offset, end_offset,
         stored_path, now_iso()),
    )
    conn.execute(
        "UPDATE chunk_sessions SET received_count ="
        " (SELECT COUNT(*) FROM chunk_segments WHERE file_id = ?), updated_at = ?"
        " WHERE file_id = ?",
        (file_id, now_iso(), file_id),
    )


def get_published(conn: sqlite3.Connection, filename: str) -> dict | None:
    row = conn.execute(
        "SELECT * FROM published_files WHERE filename = ?", (filename,)
    ).fetchone()
    return dict(row) if row else None


def upsert_published(conn: sqlite3.Connection, *, filename: str, file_id: str, version: str,
                     batch_id: str, total_rows: int) -> None:
    conn.execute(
        "INSERT INTO published_files(filename, file_id, version, batch_id, total_rows,"
        " published_at) VALUES(?,?,?,?,?,?)"
        " ON CONFLICT(filename) DO UPDATE SET file_id=excluded.file_id,"
        " version=excluded.version, batch_id=excluded.batch_id,"
        " total_rows=excluded.total_rows, published_at=excluded.published_at",
        (filename, file_id, version, batch_id, total_rows, now_iso()),
    )


def list_published(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM published_files ORDER BY published_at DESC, filename ASC"
    ).fetchall()
    return [dict(r) for r in rows]
