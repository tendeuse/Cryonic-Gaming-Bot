"""
scripts/restore_kv_doc.py

Restore a single kv_store document from the source (Railway) DB to the
destination (TiDB), decoding it correctly. Used to recover the ``arc_seat``
document after the initial migration's catch-up run double-compressed it
(it was already gzip-compressed at the source), which made the bot unable to
load it and caused it to rebuild member data from scratch — losing the
``skill_snapshots`` history (the caches self-heal via ESI, snapshots cannot).

RUN WITH THE BOT STOPPED, otherwise the live bot may overwrite the restored
document before it boots and reloads it.

Usage (PowerShell):

    $env:MYSQL_SRC_URL      = "mysql://user:pass@host:port/railway"
    $env:MYSQL_DST_HOST     = "gateway01.us-east-1.prod.aws.tidbcloud.com"
    $env:MYSQL_DST_PORT     = "4000"
    $env:MYSQL_DST_USER     = "xxxxxxxx.root"
    $env:MYSQL_DST_PASSWORD = "your-tidb-password"
    $env:MYSQL_DST_DATABASE = "test"
    $env:MYSQL_DST_SSL      = "1"
    $env:KV_NAME            = "arc_seat"          # optional, defaults to arc_seat
    python scripts/restore_kv_doc.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from urllib.parse import urlparse, unquote

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pymysql


def _truthy(v): return (v or "").strip().lower() in {"1", "true", "yes", "on"}


def _require(name: str) -> str:
    v = os.getenv(name)
    if not v:
        sys.exit(f"[restore] missing env var {name}")
    return v


def _parse_url(url: str) -> dict:
    p = urlparse(url)
    return {
        "host": p.hostname or "localhost",
        "port": p.port or 3306,
        "user": unquote(p.username) if p.username else "root",
        "password": unquote(p.password) if p.password else "",
        "database": (p.path or "/railway").lstrip("/") or "railway",
    }


def _snap_count(doc) -> int:
    if not isinstance(doc, dict):
        return -1
    ss = doc.get("skill_snapshots", {})
    return sum(len(v) for bychar in ss.values() for v in bychar.values() if isinstance(v, list))


def main() -> None:
    name = os.getenv("KV_NAME", "arc_seat")
    src_cfg = _parse_url(_require("MYSQL_SRC_URL"))

    # Point db.py at the destination (discrete vars + TLS).
    os.environ.pop("MYSQL_PUBLIC_URL", None)
    os.environ["MYSQLHOST"] = _require("MYSQL_DST_HOST")
    os.environ["MYSQLPORT"] = os.getenv("MYSQL_DST_PORT") or "4000"
    os.environ["MYSQLUSER"] = _require("MYSQL_DST_USER")
    os.environ["MYSQLPASSWORD"] = _require("MYSQL_DST_PASSWORD")
    os.environ["MYSQLDATABASE"] = os.getenv("MYSQL_DST_DATABASE") or "test"
    if _truthy(os.getenv("MYSQL_DST_SSL")):
        os.environ["MYSQL_SSL"] = "1"

    from cogs import db

    # Read + decode the source document.
    src = pymysql.connect(charset="utf8mb4", connect_timeout=15,
                          max_allowed_packet=128 * 1024 * 1024, **src_cfg)
    with src.cursor() as c:
        c.execute("SELECT data FROM kv_store WHERE name=%s", (name,))
        row = c.fetchone()
    src.close()
    if not row:
        sys.exit(f"[restore] source has no kv doc named {name!r}")
    obj = db._decode_kv(row[0])
    print(f"[restore] source {name}: snapshots={_snap_count(obj)}")

    # Show what's currently on the destination, then overwrite it.
    before = db.kv_load(name, {})
    print(f"[restore] dest BEFORE {name}: snapshots={_snap_count(before)}")
    db.kv_save(name, obj)
    after = db.kv_load(name, {})
    print(f"[restore] dest AFTER  {name}: snapshots={_snap_count(after)}")
    print("[restore] DONE — start the bot now; it will load the restored document.")


if __name__ == "__main__":
    main()
