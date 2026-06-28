"""
cogs/db.py — Shared MySQL access layer for Cryonic-Gaming-Bot.

Replaces the per-cog SQLite files and JSON files that used to live on the
Railway volume (/data). The volume was filling up; all persisted state now
lives in the Railway MySQL service instead.

Two access patterns are provided:

1. Key-value document store (``kv_store`` table) for cogs that previously
   loaded a whole dict from a JSON file and wrote it back wholesale.
   Use :func:`kv_load` / :func:`kv_save` (or the async ``akv_*`` wrappers).
   The old filename stem becomes the key (e.g. ``ign_registry.json`` ->
   key ``"ign_registry"``).

2. Relational tables for cogs that previously used their own SQLite database
   (missions, ap_ledger, char_discord_map, eve_tokens, seat_tokens, the
   buyback caches, onboarding tickets, recruiter invites). Use
   :func:`fetchone` / :func:`fetchall` / :func:`execute` / :func:`executemany`.

All calls here are synchronous (PyMySQL + a DBUtils connection pool). From
async cog code, wrap them in ``asyncio.to_thread(...)`` so the Discord event
loop is never blocked — this mirrors what cogs/scheduling.py already did for
its SQLite calls.

Connection settings come from the Railway MySQL plugin env vars
(``MYSQLHOST`` / ``MYSQLPORT`` / ``MYSQLUSER`` / ``MYSQLPASSWORD`` /
``MYSQLDATABASE``), with a single ``MYSQL_URL`` / ``DATABASE_URL`` fallback.
"""

from __future__ import annotations

import asyncio
import base64
import gzip
import json
import os
import time
from contextlib import contextmanager
from typing import Any, Optional
from urllib.parse import urlparse, unquote

import pymysql
from pymysql.cursors import DictCursor, Cursor
from dbutils.pooled_db import PooledDB

# ---------------------------------------------------------------------------
# Connection config
# ---------------------------------------------------------------------------

_pool: Optional[PooledDB] = None


def _parse_url(url: str) -> dict:
    p = urlparse(url)
    return {
        "host": p.hostname or "localhost",
        "port": p.port or 3306,
        "user": unquote(p.username) if p.username else "root",
        "password": unquote(p.password) if p.password else "",
        "database": (p.path or "/railway").lstrip("/") or "railway",
    }


def _config() -> dict:
    """Resolve MySQL connection parameters from the environment.

    Priority:
      1. ``MYSQL_PUBLIC_URL`` — Railway's public TCP proxy. Preferred because the
         private network (``mysql.railway.internal``, IPv6-only) is not always
         reachable from another service; the public proxy always is.
      2. The discrete ``MYSQL*`` variables (internal host).
      3. A single ``MYSQL_URL`` / ``DATABASE_URL``.
    Raises if none are present so startup fails fast with a clear message.
    """
    public_url = os.getenv("MYSQL_PUBLIC_URL")
    if public_url:
        return _parse_url(public_url)

    host = os.getenv("MYSQLHOST") or os.getenv("MYSQL_HOST")
    if host:
        return {
            "host": host,
            "port": int(os.getenv("MYSQLPORT") or os.getenv("MYSQL_PORT") or 3306),
            "user": os.getenv("MYSQLUSER") or os.getenv("MYSQL_USER") or "root",
            "password": os.getenv("MYSQLPASSWORD") or os.getenv("MYSQL_PASSWORD") or "",
            "database": (
                os.getenv("MYSQLDATABASE") or os.getenv("MYSQL_DATABASE") or "railway"
            ),
        }

    url = os.getenv("MYSQL_URL") or os.getenv("DATABASE_URL")
    if url:
        return _parse_url(url)

    raise RuntimeError(
        "MySQL is not configured. Set MYSQL_PUBLIC_URL (Railway public proxy), "
        "the discrete MYSQL* vars, or a single MYSQL_URL / DATABASE_URL."
    )


def _truthy(val: Optional[str]) -> bool:
    return (val or "").strip().lower() in {"1", "true", "yes", "on"}


def _ssl_config() -> Optional[dict]:
    """Return a PyMySQL ``ssl`` dict when TLS is requested, else ``None``.

    External managed MySQL (TiDB Cloud Serverless, Aiven, PlanetScale, etc.)
    require TLS. Enable it by setting ``MYSQL_SSL=1``. Provide ``MYSQL_SSL_CA``
    with a path to a CA bundle if your provider hands you one (Aiven does);
    otherwise we fall back to certifi's public CA bundle, which validates
    TiDB Serverless out of the box (its cert chains to a public root).

    Railway's internal/private MySQL does NOT speak TLS, so leave ``MYSQL_SSL``
    unset there.
    """
    if not _truthy(os.getenv("MYSQL_SSL")):
        return None
    ca = os.getenv("MYSQL_SSL_CA")
    if not ca:
        try:
            import certifi
            ca = certifi.where()
        except Exception:
            ca = None
    return {"ca": ca} if ca else {}


def get_pool() -> PooledDB:
    """Return the process-wide connection pool, creating it on first use."""
    global _pool
    if _pool is None:
        cfg = _config()
        ssl_cfg = _ssl_config()
        _pool = PooledDB(
            creator=pymysql,
            # Keep the pool small: each pooled connection can retain a multi-MB
            # read buffer that Python won't always return to the OS, which inflates
            # RSS. The bot rarely needs many concurrent DB ops, so cap low.
            maxconnections=4,
            mincached=0,         # don't eagerly connect at pool creation (see wait_until_ready)
            blocking=True,
            ping=4,              # ping a connection before handing it out (reconnect if dropped)
            autocommit=True,
            charset="utf8mb4",
            cursorclass=DictCursor,
            connect_timeout=10,
            # No single row is large anymore (arc_seat members were split into
            # their own rows and big kv docs are gzip-compressed), so the old
            # 128 MB ceiling — which let connections buffer the giant arc_seat
            # blob and balloon memory — is no longer needed. 16 MB is PyMySQL's
            # default and far more than any current row.
            max_allowed_packet=16 * 1024 * 1024,
            **({"ssl": ssl_cfg} if ssl_cfg is not None else {}),
            **cfg,
        )
        print(
            f"[DB] MySQL pool configured -> {cfg['host']}:{cfg['port']}/{cfg['database']}"
            f"{' (TLS)' if ssl_cfg is not None else ''}"
        )
    return _pool


def wait_until_ready(timeout: float = 120.0, interval: float = 3.0) -> None:
    """Block until MySQL accepts a connection, or raise after `timeout` seconds.

    Railway's private network (mysql.railway.internal) takes a few seconds to
    come up after a container starts, so the very first connection attempt can
    time out. Retry instead of crashing the boot.
    """
    deadline = time.time() + timeout
    attempt = 0
    while True:
        attempt += 1
        try:
            with cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            if attempt > 1:
                print(f"[DB] MySQL reachable after {attempt} attempt(s).")
            return
        except Exception as e:
            if time.time() >= deadline:
                raise RuntimeError(
                    f"MySQL not reachable after {timeout:.0f}s: {type(e).__name__}: {e}"
                ) from e
            print(f"[DB] waiting for MySQL... ({type(e).__name__}); retrying in {interval:.0f}s")
            # The failed attempt may have cached a dead connection; drop the pool.
            _reset_pool()
            time.sleep(interval)


def _reset_pool() -> None:
    global _pool
    try:
        if _pool is not None:
            _pool.close()
    except Exception:
        pass
    _pool = None


@contextmanager
def cursor(commit: bool = False):
    """Borrow a pooled connection and yield a cursor.

    The connection is returned to the pool on exit. Pass ``commit=True`` for
    writes (harmless under autocommit, but explicit and future-proof).
    """
    conn = get_pool().connection()
    cur = conn.cursor()
    try:
        yield cur
        if commit:
            conn.commit()
    finally:
        cur.close()
        conn.close()  # returns the connection to the pool


# ---------------------------------------------------------------------------
# Relational helpers
# ---------------------------------------------------------------------------

def fetchone(sql: str, params: tuple = ()) -> Optional[dict]:
    with cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def fetchall(sql: str, params: tuple = ()) -> list[dict]:
    with cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def execute(sql: str, params: tuple = ()) -> tuple[int, int]:
    """Run a write. Returns ``(lastrowid, rowcount)``."""
    with cursor(commit=True) as cur:
        cur.execute(sql, params)
        return cur.lastrowid, cur.rowcount


def executemany(sql: str, seq_of_params) -> int:
    """Run a bulk write. Returns rowcount."""
    with cursor(commit=True) as cur:
        cur.executemany(sql, seq_of_params)
        return cur.rowcount


# ---------------------------------------------------------------------------
# Key-value document store (replacement for the per-cog JSON files)
# ---------------------------------------------------------------------------

# Documents whose serialised JSON exceeds this are gzip-compressed before
# storage. A few kv docs (notably ``arc_seat``, ~24MB of embedded ESI data)
# are far too big for hosts that cap a single row — e.g. TiDB's 6MB entry
# limit. Compression (~10x on this data) keeps them well under such caps and
# also trims the bytes shipped over the wire. Small docs stay as readable JSON.
_KV_COMPRESS_THRESHOLD = 1_000_000          # ~1 MB of JSON text
_KV_GZIP_PREFIX = "gzip:b64:"               # marker for a compressed document


def encode_doc(obj: Any) -> str:
    """Serialise a JSON document for storage in a ``data`` column.

    Documents larger than ``_KV_COMPRESS_THRESHOLD`` are gzip+base64 compressed
    and wrapped as a JSON string scalar (prefixed with ``_KV_GZIP_PREFIX``);
    :func:`decode_doc` reverses this. Used by both the kv_store and the
    seat_members table so large rows fit hosts that cap a single row (TiDB)."""
    payload = json.dumps(obj, ensure_ascii=False)
    if len(payload) > _KV_COMPRESS_THRESHOLD:
        blob = base64.b64encode(gzip.compress(payload.encode("utf-8"), 6)).decode("ascii")
        payload = json.dumps(_KV_GZIP_PREFIX + blob)
    return payload


def decode_doc(raw: Any) -> Any:
    """Decode a stored ``data`` column value into a Python object,
    transparently inflating gzip-compressed documents."""
    if isinstance(raw, (dict, list)):  # driver auto-decoded a JSON object/array
        return raw
    val = json.loads(raw)
    if isinstance(val, str) and val.startswith(_KV_GZIP_PREFIX):
        blob = base64.b64decode(val[len(_KV_GZIP_PREFIX):])
        return json.loads(gzip.decompress(blob).decode("utf-8"))
    return val


# Back-compat alias (used by the migration/restore scripts).
_decode_kv = decode_doc


def kv_load(name: str, default: Any = None) -> Any:
    """Load a JSON document by key. Returns ``default`` if the key is absent.

    Mirrors the old ``load_state()`` semantics where a missing file yielded a
    default dict; cogs keep their existing ``setdefault`` back-fill logic.
    """
    row = fetchone("SELECT data FROM kv_store WHERE name=%s", (name,))
    if not row:
        return default
    try:
        return _decode_kv(row["data"])
    except (TypeError, ValueError):
        return default


def kv_save(name: str, obj: Any) -> None:
    """Upsert a JSON document by key (whole-document replace).

    Documents larger than ``_KV_COMPRESS_THRESHOLD`` are gzip+base64 compressed
    and stored as a JSON string scalar (prefixed with ``_KV_GZIP_PREFIX``);
    :func:`_decode_kv` reverses this on read. This is transparent to callers.
    """
    execute(
        "INSERT INTO kv_store (name, data) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE data=VALUES(data), updated_at=CURRENT_TIMESTAMP",
        (name, encode_doc(obj)),
    )


async def akv_load(name: str, default: Any = None) -> Any:
    return await asyncio.to_thread(kv_load, name, default)


async def akv_save(name: str, obj: Any) -> None:
    await asyncio.to_thread(kv_save, name, obj)


# ---------------------------------------------------------------------------
# arc_seat members (one row per member instead of one giant kv document)
# ---------------------------------------------------------------------------
# arc_seat used to embed every member (with multi-MB ESI caches) in a single
# kv_store document, which grows toward host row-size caps. These helpers store
# each member as its own row in ``seat_members`` (per-row compression via
# encode_doc), so no single row carries the whole corp. cogs/arc_seat.py keeps
# the same in-memory ``data["members"]`` dict — only the persistence is split.

def seat_members_load() -> dict:
    """Return ``{discord_id_str: member_record}`` from the seat_members table."""
    rows = fetchall("SELECT discord_id, data FROM seat_members")
    return {r["discord_id"]: decode_doc(r["data"]) for r in rows}


def seat_members_save(members: dict) -> None:
    """Replace the seat_members table with ``members`` (whole-collection sync).

    Upserts every member in one batch, then deletes any rows no longer present.
    Raises on failure so the caller can avoid stripping the legacy embedded copy
    before the table write has succeeded (no-loss migration)."""
    members = members or {}
    if not members:
        # Refuse to wipe the whole table on an empty set — for this corp that is
        # almost always a transient upstream load failure, not a real "no
        # members" state. Leaving stale rows is harmless; deleting all is not.
        print("[db] seat_members_save: empty members set — refusing to wipe table.")
        return
    executemany(
        "INSERT INTO seat_members (discord_id, data) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE data=VALUES(data), updated_at=CURRENT_TIMESTAMP",
        [(str(k), encode_doc(v)) for k, v in members.items()],
    )
    keys = [str(k) for k in members.keys()]
    placeholders = ",".join(["%s"] * len(keys))
    execute(
        f"DELETE FROM seat_members WHERE discord_id NOT IN ({placeholders})",
        tuple(keys),
    )


# ---------------------------------------------------------------------------
# Legacy sqlite3-style connection adapter
# ---------------------------------------------------------------------------
# A handful of cogs (cogs/Buyback.py) thread a connection object through many
# async call sites and unpack rows as tuples. Rather than rewrite all of that,
# this adapter exposes the small slice of the sqlite3 connection API they use
# over the shared MySQL pool: ``.execute(sql, params)`` returning a tuple
# cursor, ``.commit()`` and ``.close()``. It rewrites ``?`` placeholders to
# ``%s`` and ``INSERT OR REPLACE`` to ``REPLACE`` so the existing SQL works
# unchanged. New code should use kv_*/fetch*/execute instead.

class _LegacyConn:
    def __init__(self):
        self._conn = get_pool().connection()

    @staticmethod
    def _translate(sql: str) -> str:
        if "INSERT OR REPLACE" in sql:
            sql = sql.replace("INSERT OR REPLACE", "REPLACE")
        return sql.replace("?", "%s")

    def execute(self, sql: str, params: tuple = ()):
        cur = self._conn.cursor(Cursor)  # tuple rows (not dict), to match sqlite3
        cur.execute(self._translate(sql), params)
        return cur

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()  # returns the connection to the pool


def legacy_conn() -> "_LegacyConn":
    """Return a minimal sqlite3-style connection backed by the MySQL pool."""
    return _LegacyConn()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

# Every table the bot relies on. Created once at startup (and by the migration
# script). Statements are idempotent (IF NOT EXISTS). Discord/EVE IDs are
# BIGINT; token columns are TEXT (EVE JWT access tokens exceed VARCHAR(255)).
_SCHEMA: tuple[str, ...] = (
    # --- key-value document store (was: the ~20 JSON files) ---
    """
    CREATE TABLE IF NOT EXISTS kv_store (
        name       VARCHAR(191) PRIMARY KEY,
        data       JSON NOT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                   ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    # --- missions.db: missions (missions.py) ---
    """
    CREATE TABLE IF NOT EXISTS missions (
        id          BIGINT PRIMARY KEY AUTO_INCREMENT,
        title       TEXT    NOT NULL,
        description TEXT     NOT NULL,
        reward      TEXT     NOT NULL,
        status      VARCHAR(20) NOT NULL DEFAULT 'open',
        created_by  BIGINT  NOT NULL,
        assigned_to BIGINT  NULL,
        guild_id    BIGINT  NOT NULL,
        created_at  VARCHAR(40) NOT NULL,
        updated_at  VARCHAR(40) NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    # --- ap_tracking.py: ap_audit (was the per-user "audit" list inside the
    #     huge ap_data.json; broken out into rows so ap_data stays tiny) ---
    """
    CREATE TABLE IF NOT EXISTS ap_audit (
        id       BIGINT PRIMARY KEY AUTO_INCREMENT,
        user_id  BIGINT NOT NULL,
        ts       VARCHAR(40) NOT NULL,
        delta    DOUBLE NOT NULL DEFAULT 0,
        source   VARCHAR(255) NOT NULL DEFAULT '',
        reason   TEXT NULL,
        actor_id BIGINT NULL,
        INDEX idx_apaudit_user (user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    # --- missions.db: ap_ledger (missions_ap.py) ---
    """
    CREATE TABLE IF NOT EXISTS ap_ledger (
        id             BIGINT PRIMARY KEY AUTO_INCREMENT,
        discord_id     VARCHAR(32) NOT NULL,
        character_name VARCHAR(191) NOT NULL,
        mission_name   VARCHAR(255) NOT NULL,
        faction        VARCHAR(191) NOT NULL DEFAULT '',
        level          INT NOT NULL DEFAULT 4,
        standing_gain  DOUBLE NOT NULL DEFAULT 0,
        ap             INT NOT NULL DEFAULT 50,
        recorded_at    VARCHAR(40) NOT NULL,
        INDEX idx_ap_discord (discord_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    # --- missions.db: char_discord_map (missions_ap.py) ---
    """
    CREATE TABLE IF NOT EXISTS char_discord_map (
        character_name VARCHAR(191) PRIMARY KEY,
        discord_id     VARCHAR(32) NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    # --- missions.db: eve_tokens (overlay_api.py) ---
    """
    CREATE TABLE IF NOT EXISTS eve_tokens (
        discord_user_id BIGINT PRIMARY KEY,
        character_id    BIGINT NOT NULL,
        character_name  VARCHAR(191) NOT NULL,
        access_token    TEXT NOT NULL,
        refresh_token   TEXT NOT NULL,
        expires_at      DOUBLE NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    # --- arc_seat.db: seat_tokens (arc_seat.py) ---
    """
    CREATE TABLE IF NOT EXISTS seat_tokens (
        discord_user_id BIGINT NOT NULL,
        character_id    BIGINT NOT NULL,
        character_name  VARCHAR(191) NOT NULL,
        access_token    TEXT NOT NULL,
        refresh_token   TEXT NOT NULL,
        expires_at      DOUBLE NOT NULL,
        PRIMARY KEY (discord_user_id, character_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    # --- arc_seat members: one row per Discord member (was embedded in the
    #     arc_seat kv document). data is a JSON document, gzip-compressed by
    #     encode_doc when large so no single member row hits a host row cap. ---
    """
    CREATE TABLE IF NOT EXISTS seat_members (
        discord_id VARCHAR(32) PRIMARY KEY,
        data       JSON NOT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                   ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    # --- buyback_contracts.db: type_cache (Buyback.py) ---
    """
    CREATE TABLE IF NOT EXISTS type_cache (
        type_id   BIGINT PRIMARY KEY,
        name      VARCHAR(255) NOT NULL,
        cached_at BIGINT NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    # --- buyback_contracts.db: price_cache (Buyback.py) ---
    """
    CREATE TABLE IF NOT EXISTS price_cache (
        type_id   BIGINT PRIMARY KEY,
        jita_buy  DOUBLE NOT NULL,
        cached_at BIGINT NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    # --- buyback_contracts.db: char_name_cache (Buyback.py) ---
    """
    CREATE TABLE IF NOT EXISTS char_name_cache (
        character_id BIGINT PRIMARY KEY,
        name         VARCHAR(255) NOT NULL,
        cached_at    BIGINT NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    # --- buyback_contracts.db: buyback_paid (Buyback.py) ---
    """
    CREATE TABLE IF NOT EXISTS buyback_paid (
        contract_id        BIGINT PRIMARY KEY,
        paid_at            BIGINT NOT NULL,
        paid_by_discord_id BIGINT NOT NULL,
        paid_by_tag        VARCHAR(255) NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    # --- onboarding_tickets.db: tickets (onboardingtm.py) ---
    """
    CREATE TABLE IF NOT EXISTS tickets (
        ticket_id  VARCHAR(191) PRIMARY KEY,
        name       TEXT NOT NULL,
        open_time  VARCHAR(40) NOT NULL,
        end_time   VARCHAR(40) NOT NULL,
        day8_sent  INT NOT NULL DEFAULT 0,
        day12_sent INT NOT NULL DEFAULT 0,
        day15_sent INT NOT NULL DEFAULT 0
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    # --- recruits.db: invites (scheduling.py). ign is case-insensitive (was COLLATE NOCASE) ---
    """
    CREATE TABLE IF NOT EXISTS invites (
        ign        VARCHAR(191) COLLATE utf8mb4_general_ci PRIMARY KEY,
        invited_by VARCHAR(255) NOT NULL,
        invited_at VARCHAR(40) NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
)


def init_db() -> None:
    """Create the key-value store and every relational table (idempotent).

    Waits for MySQL to become reachable first (Railway private networking can
    take a few seconds to initialise at container start)."""
    wait_until_ready()
    with cursor(commit=True) as cur:
        for stmt in _SCHEMA:
            cur.execute(stmt)
    print(f"[DB] Schema ready ({len(_SCHEMA)} tables verified).")


# Relational tables (everything except the kv_store document table).
RELATIONAL_TABLES: tuple[str, ...] = (
    "ap_audit",
    "missions", "ap_ledger", "char_discord_map", "eve_tokens", "seat_tokens",
    "seat_members",
    "type_cache", "price_cache", "char_name_cache", "buyback_paid",
    "tickets", "invites",
)


def export_all() -> dict:
    """Dump everything in the database — the kv_store documents plus every
    relational table — as a plain dict (JSON-serialisable with default=str).
    Used by the /export_volume owner command as a full backup."""
    out: dict = {"kv_store": {}, "tables": {}}
    for row in fetchall("SELECT name, data FROM kv_store"):
        out["kv_store"][row["name"]] = _decode_kv(row["data"])
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
