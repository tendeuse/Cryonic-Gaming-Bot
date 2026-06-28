"""
cogs/db.py — Shared SQLite access layer for Cryonic-Gaming-Bot.

All persisted state lives in a single SQLite database file on the Railway
**/data volume** (path from ``SQLITE_PATH``, default ``/data/bot.db``). This
replaced an external MySQL/TiDB backend; the public interface below is
unchanged, so cogs did not need to change how they read/write.

Two access patterns are provided:

1. Key-value document store (``kv_store`` table) for cogs that load a whole
   dict and write it back wholesale. Use :func:`kv_load` / :func:`kv_save`
   (or the async ``akv_*`` wrappers). The old filename stem is the key
   (e.g. ``ign_registry.json`` -> key ``"ign_registry"``).

2. Relational tables for cogs that used their own SQLite/relational data.
   Use :func:`fetchone` / :func:`fetchall` / :func:`execute` /
   :func:`executemany`. SQL uses ``%s`` placeholders (translated to SQLite's
   ``?``) so cog SQL written for the MySQL era keeps working.

All calls are synchronous. From async cog code wrap them in
``asyncio.to_thread(...)`` so the Discord event loop is never blocked. A single
shared connection is guarded by a lock; SQLite WAL mode lets the separate
``legacy_conn()`` connections (cogs/Buyback.py) read concurrently.
"""

from __future__ import annotations

import asyncio
import base64
import gzip
import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

# Default to the Railway volume mount. Overridable via SQLITE_PATH (e.g. a local
# path for development/tests).
DB_PATH = os.getenv("SQLITE_PATH") or "/data/bot.db"

_conn: Optional[sqlite3.Connection] = None
_lock = threading.RLock()


def _truthy(val: Optional[str]) -> bool:
    return (val or "").strip().lower() in {"1", "true", "yes", "on"}


def _connect() -> sqlite3.Connection:
    """Open (once) the process-wide SQLite connection."""
    global _conn
    if _conn is None:
        d = os.path.dirname(DB_PATH)
        if d:
            os.makedirs(d, exist_ok=True)
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
        _conn.row_factory = sqlite3.Row
        # WAL: concurrent readers (incl. legacy_conn) alongside one writer.
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA synchronous=NORMAL")
        _conn.execute("PRAGMA busy_timeout=30000")
        _conn.execute("PRAGMA foreign_keys=ON")
        print(f"[DB] SQLite ready -> {DB_PATH}")
    return _conn


def _xlate(sql: str) -> str:
    """Translate MySQL-era ``%s`` placeholders to SQLite's ``?``."""
    return sql.replace("%s", "?")


def wait_until_ready(timeout: float = 120.0, interval: float = 3.0) -> None:
    """Ensure the SQLite database is openable. (Kept for boot-time API parity;
    a local file needs no network wait.)"""
    conn = _connect()
    conn.execute("SELECT 1").fetchone()


@contextmanager
def cursor(commit: bool = False):
    """Yield a cursor on the shared connection, serialised by a lock."""
    with _lock:
        conn = _connect()
        cur = conn.cursor()
        try:
            yield cur
            if commit:
                conn.commit()
        finally:
            cur.close()


# ---------------------------------------------------------------------------
# Relational helpers
# ---------------------------------------------------------------------------

def fetchone(sql: str, params: tuple = ()) -> Optional[dict]:
    with cursor() as cur:
        cur.execute(_xlate(sql), params)
        row = cur.fetchone()
        return dict(row) if row is not None else None


def fetchall(sql: str, params: tuple = ()) -> list[dict]:
    with cursor() as cur:
        cur.execute(_xlate(sql), params)
        return [dict(r) for r in cur.fetchall()]


def execute(sql: str, params: tuple = ()) -> tuple[int, int]:
    """Run a write. Returns ``(lastrowid, rowcount)``."""
    with cursor(commit=True) as cur:
        cur.execute(_xlate(sql), params)
        return cur.lastrowid, cur.rowcount


def executemany(sql: str, seq_of_params) -> int:
    """Run a bulk write. Returns rowcount."""
    with cursor(commit=True) as cur:
        cur.executemany(_xlate(sql), seq_of_params)
        return cur.rowcount


# ---------------------------------------------------------------------------
# Key-value document store
# ---------------------------------------------------------------------------

# Documents whose serialised JSON exceeds this are gzip-compressed before
# storage. SQLite has no per-row size cap, but compression still trims the file
# and is harmless; large docs (e.g. arc_seat members) stay small on disk.
_KV_COMPRESS_THRESHOLD = 1_000_000          # ~1 MB of JSON text
_KV_GZIP_PREFIX = "gzip:b64:"               # marker for a compressed document


def encode_doc(obj: Any) -> str:
    """Serialise a JSON document for storage in a ``data`` column, gzip+base64
    compressing it (as a JSON string scalar) when large. Reverse: decode_doc."""
    payload = json.dumps(obj, ensure_ascii=False)
    if len(payload) > _KV_COMPRESS_THRESHOLD:
        blob = base64.b64encode(gzip.compress(payload.encode("utf-8"), 6)).decode("ascii")
        payload = json.dumps(_KV_GZIP_PREFIX + blob)
    return payload


def decode_doc(raw: Any) -> Any:
    """Decode a stored ``data`` column value into a Python object,
    transparently inflating gzip-compressed documents."""
    if isinstance(raw, (dict, list)):
        return raw
    val = json.loads(raw)
    if isinstance(val, str) and val.startswith(_KV_GZIP_PREFIX):
        blob = base64.b64decode(val[len(_KV_GZIP_PREFIX):])
        return json.loads(gzip.decompress(blob).decode("utf-8"))
    return val


# Back-compat alias (used by the migration/restore scripts).
_decode_kv = decode_doc


def kv_load(name: str, default: Any = None) -> Any:
    """Load a JSON document by key. Returns ``default`` if absent."""
    row = fetchone("SELECT data FROM kv_store WHERE name=%s", (name,))
    if not row:
        return default
    try:
        return decode_doc(row["data"])
    except (TypeError, ValueError):
        return default


def kv_save(name: str, obj: Any) -> None:
    """Upsert a JSON document by key (whole-document replace)."""
    execute(
        "INSERT INTO kv_store (name, data, updated_at) "
        "VALUES (%s, %s, datetime('now')) "
        "ON CONFLICT(name) DO UPDATE SET data=excluded.data, updated_at=datetime('now')",
        (name, encode_doc(obj)),
    )


async def akv_load(name: str, default: Any = None) -> Any:
    return await asyncio.to_thread(kv_load, name, default)


async def akv_save(name: str, obj: Any) -> None:
    await asyncio.to_thread(kv_save, name, obj)


# ---------------------------------------------------------------------------
# arc_seat members (one row per member)
# ---------------------------------------------------------------------------

def seat_members_load() -> dict:
    """Return ``{discord_id_str: member_record}`` from the seat_members table."""
    rows = fetchall("SELECT discord_id, data FROM seat_members")
    return {r["discord_id"]: decode_doc(r["data"]) for r in rows}


def seat_members_save(members: dict) -> None:
    """Replace the seat_members table with ``members`` (whole-collection sync).

    Upserts every member, then deletes any rows no longer present. Refuses to
    wipe the table on an empty set (almost always a transient upstream load
    failure, not a real "no members" state)."""
    members = members or {}
    if not members:
        print("[db] seat_members_save: empty members set — refusing to wipe table.")
        return
    executemany(
        "INSERT INTO seat_members (discord_id, data, updated_at) "
        "VALUES (%s, %s, datetime('now')) "
        "ON CONFLICT(discord_id) DO UPDATE SET data=excluded.data, updated_at=datetime('now')",
        [(str(k), encode_doc(v)) for k, v in members.items()],
    )
    keys = [str(k) for k in members.keys()]
    placeholders = ",".join(["%s"] * len(keys))
    execute(
        f"DELETE FROM seat_members WHERE discord_id NOT IN ({placeholders})",
        tuple(keys),
    )


# ---------------------------------------------------------------------------
# Legacy sqlite3-style connection (cogs/Buyback.py)
# ---------------------------------------------------------------------------
# Buyback.py threads a connection through many call sites, uses ``?`` params,
# ``INSERT OR REPLACE`` and tuple rows — all native SQLite. Give it its own
# connection to the same file (WAL allows concurrent access) so its commit/close
# lifecycle doesn't touch the shared connection.

class _LegacyConn:
    def __init__(self):
        self._conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
        self._conn.execute("PRAGMA busy_timeout=30000")

    def execute(self, sql: str, params: tuple = ()):
        cur = self._conn.cursor()  # default cursor -> tuple rows (matches sqlite3)
        cur.execute(sql, params)
        return cur

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()


def legacy_conn() -> "_LegacyConn":
    """Return a standalone sqlite3-style connection to the shared database."""
    return _LegacyConn()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
# SQLite dialect: INTEGER PRIMARY KEY AUTOINCREMENT for surrogate ids, TEXT for
# VARCHAR/JSON, REAL for DOUBLE. Inline indexes are not allowed, so they are
# separate CREATE INDEX statements. updated_at has no ON UPDATE in SQLite; the
# kv/seat upserts set it explicitly.
_SCHEMA: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS kv_store (
        name       TEXT PRIMARY KEY,
        data       TEXT NOT NULL,
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS missions (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        title       TEXT    NOT NULL,
        description TEXT    NOT NULL,
        reward      TEXT    NOT NULL,
        status      TEXT    NOT NULL DEFAULT 'open',
        created_by  INTEGER NOT NULL,
        assigned_to INTEGER,
        guild_id    INTEGER NOT NULL,
        created_at  TEXT    NOT NULL,
        updated_at  TEXT    NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ap_audit (
        id       INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id  INTEGER NOT NULL,
        ts       TEXT NOT NULL,
        delta    REAL NOT NULL DEFAULT 0,
        source   TEXT NOT NULL DEFAULT '',
        reason   TEXT,
        actor_id INTEGER
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_apaudit_user ON ap_audit(user_id)",
    "CREATE INDEX IF NOT EXISTS idx_apaudit_ts ON ap_audit(ts)",
    """
    CREATE TABLE IF NOT EXISTS ap_ledger (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        discord_id     TEXT NOT NULL,
        character_name TEXT NOT NULL,
        mission_name   TEXT NOT NULL,
        faction        TEXT NOT NULL DEFAULT '',
        level          INTEGER NOT NULL DEFAULT 4,
        standing_gain  REAL NOT NULL DEFAULT 0,
        ap             INTEGER NOT NULL DEFAULT 50,
        recorded_at    TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_ap_discord ON ap_ledger(discord_id)",
    """
    CREATE TABLE IF NOT EXISTS char_discord_map (
        character_name TEXT PRIMARY KEY,
        discord_id     TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS eve_tokens (
        discord_user_id INTEGER PRIMARY KEY,
        character_id    INTEGER NOT NULL,
        character_name  TEXT NOT NULL,
        access_token    TEXT NOT NULL,
        refresh_token   TEXT NOT NULL,
        expires_at      REAL NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS seat_tokens (
        discord_user_id INTEGER NOT NULL,
        character_id    INTEGER NOT NULL,
        character_name  TEXT NOT NULL,
        access_token    TEXT NOT NULL,
        refresh_token   TEXT NOT NULL,
        expires_at      REAL NOT NULL,
        PRIMARY KEY (discord_user_id, character_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS seat_members (
        discord_id TEXT PRIMARY KEY,
        data       TEXT NOT NULL,
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS type_cache (
        type_id   INTEGER PRIMARY KEY,
        name      TEXT NOT NULL,
        cached_at INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS price_cache (
        type_id   INTEGER PRIMARY KEY,
        jita_buy  REAL NOT NULL,
        cached_at INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS char_name_cache (
        character_id INTEGER PRIMARY KEY,
        name         TEXT NOT NULL,
        cached_at    INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS buyback_paid (
        contract_id        INTEGER PRIMARY KEY,
        paid_at            INTEGER NOT NULL,
        paid_by_discord_id INTEGER NOT NULL,
        paid_by_tag        TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tickets (
        ticket_id  TEXT PRIMARY KEY,
        name       TEXT NOT NULL,
        open_time  TEXT NOT NULL,
        end_time   TEXT NOT NULL,
        day8_sent  INTEGER NOT NULL DEFAULT 0,
        day12_sent INTEGER NOT NULL DEFAULT 0,
        day15_sent INTEGER NOT NULL DEFAULT 0
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS invites (
        ign        TEXT PRIMARY KEY COLLATE NOCASE,
        invited_by TEXT NOT NULL,
        invited_at TEXT NOT NULL
    )
    """,
)


def init_db() -> None:
    """Create the key-value store and every relational table (idempotent)."""
    wait_until_ready()
    with cursor(commit=True) as cur:
        for stmt in _SCHEMA:
            cur.execute(stmt)
    print(f"[DB] Schema ready ({len(_SCHEMA)} statements applied) at {DB_PATH}.")


# Relational tables (everything except the kv_store document table).
RELATIONAL_TABLES: tuple[str, ...] = (
    "ap_audit",
    "missions", "ap_ledger", "char_discord_map", "eve_tokens", "seat_tokens",
    "seat_members",
    "type_cache", "price_cache", "char_name_cache", "buyback_paid",
    "tickets", "invites",
)


def export_all() -> dict:
    """Dump the kv_store documents plus every relational table as a plain dict
    (JSON-serialisable with default=str). Used by /export_volume."""
    out: dict = {"kv_store": {}, "tables": {}}
    for row in fetchall("SELECT name, data FROM kv_store"):
        out["kv_store"][row["name"]] = decode_doc(row["data"])
    for table in RELATIONAL_TABLES:
        out["tables"][table] = fetchall(f"SELECT * FROM {table}")
    return out


def list_summary() -> dict:
    """Return {kv_keys: [...], table_counts: {table: n}} for diagnostics."""
    keys = [r["name"] for r in fetchall("SELECT name FROM kv_store ORDER BY name")]
    counts = {}
    for table in RELATIONAL_TABLES:
        row = fetchone(f"SELECT COUNT(*) AS n FROM {table}")
        counts[table] = int(row["n"]) if row else 0
    return {"kv_keys": keys, "table_counts": counts}
