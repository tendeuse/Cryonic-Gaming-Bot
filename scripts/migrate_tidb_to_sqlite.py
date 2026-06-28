"""
scripts/migrate_tidb_to_sqlite.py

One-time copy of all data from the external MySQL/TiDB database into the local
SQLite file on the Railway /data volume. Runs before the bot:

    python scripts/migrate_tidb_to_sqlite.py && python bot.py

It is idempotent — a sentinel row (``_migration_to_sqlite_done`` in kv_store)
makes it a no-op after the first success, so it is safe to leave in the start
command. If no source DB env is present, it assumes a fresh SQLite install and
just marks itself done.

Source (TiDB) comes from the existing env: ``MYSQL_PUBLIC_URL`` (or ``MYSQL*`` /
``MYSQL_URL``) + ``MYSQL_SSL``. Destination is SQLite at ``SQLITE_PATH``
(default ``/data/bot.db``). Big tables are streamed server-side and inserted in
batches so peak memory stays low.
"""
from __future__ import annotations

import datetime as _dt
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlparse, unquote

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

SENTINEL = "_migration_to_sqlite_done"


def _truthy(v) -> bool:
    return (v or "").strip().lower() in {"1", "true", "yes", "on"}


def _src_cfg() -> dict | None:
    url = os.getenv("MYSQL_PUBLIC_URL") or os.getenv("MYSQL_URL") or os.getenv("DATABASE_URL")
    if url:
        p = urlparse(url)
        return {
            "host": p.hostname or "localhost",
            "port": p.port or 3306,
            "user": unquote(p.username) if p.username else "root",
            "password": unquote(p.password) if p.password else "",
            "database": (p.path or "/railway").lstrip("/") or "railway",
        }
    host = os.getenv("MYSQLHOST") or os.getenv("MYSQL_HOST")
    if host:
        return {
            "host": host,
            "port": int(os.getenv("MYSQLPORT") or os.getenv("MYSQL_PORT") or 3306),
            "user": os.getenv("MYSQLUSER") or os.getenv("MYSQL_USER") or "root",
            "password": os.getenv("MYSQLPASSWORD") or os.getenv("MYSQL_PASSWORD") or "",
            "database": os.getenv("MYSQLDATABASE") or os.getenv("MYSQL_DATABASE") or "railway",
        }
    return None


def _coerce(v):
    if isinstance(v, (_dt.datetime, _dt.date)):
        return v.isoformat(sep=" ")
    return v


def _copy_kv(src, db) -> int:
    from pymysql.cursors import SSDictCursor
    total = 0
    batch = []
    with src.cursor(SSDictCursor) as c:
        c.execute("SELECT name, data FROM kv_store")
        for row in c:
            if row["name"] == SENTINEL:
                continue
            batch.append((row["name"], row["data"]))
            if len(batch) >= 500:
                _kv_insert(db, batch); total += len(batch); batch = []
    if batch:
        _kv_insert(db, batch); total += len(batch)
    return total


def _kv_insert(db, batch) -> None:
    db.executemany(
        "INSERT OR REPLACE INTO kv_store (name, data, updated_at) "
        "VALUES (%s, %s, datetime('now'))",
        batch,
    )


def _copy_table(src, db, table: str) -> int:
    from pymysql.cursors import SSDictCursor
    total = 0
    batch = []
    with src.cursor(SSDictCursor) as c:
        c.execute(f"SELECT * FROM `{table}`")
        cols = [d[0] for d in c.description]
        collist = ",".join(cols)
        ph = ",".join(["%s"] * len(cols))
        sql = f"INSERT OR REPLACE INTO {table} ({collist}) VALUES ({ph})"
        for row in c:
            batch.append(tuple(_coerce(row[col]) for col in cols))
            if len(batch) >= 5000:
                db.executemany(sql, batch); total += len(batch); batch = []
        if batch:
            db.executemany(sql, batch); total += len(batch)
    return total


def main() -> None:
    from cogs import db
    db.init_db()  # ensure the SQLite schema exists

    if db.fetchone("SELECT 1 AS x FROM kv_store WHERE name=%s", (SENTINEL,)):
        print("[sqlite-migrate] already done — skipping.")
        return

    cfg = _src_cfg()
    if not cfg:
        print("[sqlite-migrate] no source DB env present — assuming fresh SQLite install.")
        db.kv_save(SENTINEL, {"done_at": time.strftime("%Y-%m-%dT%H:%M:%S"), "note": "no source"})
        return

    import pymysql
    ssl = None
    if _truthy(os.getenv("MYSQL_SSL")):
        ca = os.getenv("MYSQL_SSL_CA")
        if not ca:
            try:
                import certifi
                ca = certifi.where()
            except Exception:
                ca = None
        ssl = {"ca": ca} if ca else {}

    src = pymysql.connect(
        charset="utf8mb4", connect_timeout=20, max_allowed_packet=128 * 1024 * 1024,
        **({"ssl": ssl} if ssl is not None else {}), **cfg,
    )
    print(f"[sqlite-migrate] {cfg['host']}:{cfg['port']}/{cfg['database']} -> {db.DB_PATH}")

    n = _copy_kv(src, db)
    print(f"[sqlite-migrate]   kv_store: {n} doc(s)")
    for t in db.RELATIONAL_TABLES:
        c = _copy_table(src, db, t)
        print(f"[sqlite-migrate]   {t}: {c} row(s)")

    src.close()
    db.kv_save(SENTINEL, {"done_at": time.strftime("%Y-%m-%dT%H:%M:%S")})
    print("[sqlite-migrate] DONE.")


if __name__ == "__main__":
    main()
