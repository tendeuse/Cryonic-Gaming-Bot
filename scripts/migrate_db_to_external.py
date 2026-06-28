"""
scripts/migrate_db_to_external.py

One-shot copy of the bot's MySQL database from one server to another. Used to
move off the **always-on Railway MySQL container** (its 24/7 RAM is the biggest
line on the Railway bill) onto a free external serverless MySQL — TiDB Cloud
Serverless — which scales to zero and costs nothing at this data size.

Strategy: the destination schema is built with the bot's own ``db.init_db()``
(canonical, portable DDL: plain utf8mb4 / utf8mb4_general_ci, no MySQL-8-only
collations that TiDB might reject), then rows are copied table-by-table. Any
source table not created by init_db is recreated from a sanitised
``SHOW CREATE TABLE``. Safe to re-run: each destination table is emptied first.

Run with the bot STOPPED so the source is quiescent.

The SOURCE (Railway) is given as a ready-made URL (Railway hands you one with the
password already encoded). The DESTINATION (TiDB) is given as separate fields so
you never have to URL-encode a password that contains symbols.

Usage (PowerShell):

    # Source = Railway MySQL -> Connect -> Public Network (copy the mysql://... URL)
    $env:MYSQL_SRC_URL      = "mysql://user:pass@host:port/railway"

    # Destination = TiDB -> Connect (Public). Paste each field as-is.
    $env:MYSQL_DST_HOST     = "gateway01.us-east-1.prod.aws.tidbcloud.com"
    $env:MYSQL_DST_PORT     = "4000"
    $env:MYSQL_DST_USER     = "xxxxxxxx.root"
    $env:MYSQL_DST_PASSWORD = "your-tidb-password"
    $env:MYSQL_DST_DATABASE = "test"
    $env:MYSQL_DST_SSL      = "1"          # TiDB public endpoint requires TLS

    python scripts/migrate_db_to_external.py

Optional: MYSQL_DST_SSL_CA = path to a CA bundle. If unset, certifi's public CA
bundle is used, which validates TiDB Serverless.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from urllib.parse import urlparse, unquote

# Make `cogs` importable when run as `python scripts/migrate_db_to_external.py`.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pymysql


def _truthy(val: str | None) -> bool:
    return (val or "").strip().lower() in {"1", "true", "yes", "on"}


def _require(name: str) -> str:
    val = os.getenv(name)
    if not val:
        sys.exit(f"[migrate] missing env var {name}")
    return val


def _parse_url(url: str) -> dict:
    p = urlparse(url)
    return {
        "host": p.hostname or "localhost",
        "port": p.port or 3306,
        "user": unquote(p.username) if p.username else "root",
        "password": unquote(p.password) if p.password else "",
        "database": (p.path or "/railway").lstrip("/") or "railway",
    }


def _dst_cfg() -> dict:
    return {
        "host": _require("MYSQL_DST_HOST"),
        "port": int(os.getenv("MYSQL_DST_PORT") or 4000),
        "user": _require("MYSQL_DST_USER"),
        "password": _require("MYSQL_DST_PASSWORD"),
        "database": os.getenv("MYSQL_DST_DATABASE") or "test",
    }


def _ssl_kwargs() -> dict:
    if not _truthy(os.getenv("MYSQL_DST_SSL")):
        return {}
    ca = os.getenv("MYSQL_DST_SSL_CA")
    if not ca:
        try:
            import certifi
            ca = certifi.where()
        except Exception:
            ca = None
    return {"ssl": {"ca": ca} if ca else {}}


def _connect(cfg: dict, ssl: bool, label: str) -> pymysql.connections.Connection:
    kwargs = dict(
        autocommit=True,
        charset="utf8mb4",
        connect_timeout=15,
        max_allowed_packet=128 * 1024 * 1024,
        **cfg,
    )
    if ssl:
        kwargs.update(_ssl_kwargs())
    print(f"[migrate] connect {label} -> {cfg['host']}:{cfg['port']}/{cfg['database']}"
          f"{' (TLS)' if ssl else ''}")
    return pymysql.connect(**kwargs)


def _sanitise_ddl(ddl: str) -> str:
    """Make a MySQL-8 SHOW CREATE TABLE statement portable to TiDB."""
    return ddl.replace("utf8mb4_0900_ai_ci", "utf8mb4_general_ci")


def main() -> None:
    src_cfg = _parse_url(_require("MYSQL_SRC_URL"))
    dst_cfg = _dst_cfg()
    dst_ssl = _truthy(os.getenv("MYSQL_DST_SSL"))

    # Build the destination schema with the canonical init_db() DDL. db.py reads
    # discrete MYSQL* vars (and MYSQL_SSL); make sure no stale URL shadows them.
    os.environ.pop("MYSQL_PUBLIC_URL", None)
    os.environ.pop("MYSQL_URL", None)
    os.environ.pop("DATABASE_URL", None)
    os.environ["MYSQLHOST"] = dst_cfg["host"]
    os.environ["MYSQLPORT"] = str(dst_cfg["port"])
    os.environ["MYSQLUSER"] = dst_cfg["user"]
    os.environ["MYSQLPASSWORD"] = dst_cfg["password"]
    os.environ["MYSQLDATABASE"] = dst_cfg["database"]
    if dst_ssl:
        os.environ["MYSQL_SSL"] = "1"
        if os.getenv("MYSQL_DST_SSL_CA"):
            os.environ["MYSQL_SSL_CA"] = os.environ["MYSQL_DST_SSL_CA"]

    from cogs import db  # noqa: E402  (env must be set first)
    print("[migrate] creating destination schema via db.init_db() ...")
    db.init_db()

    src = _connect(src_cfg, ssl=False, label="SOURCE (Railway)")
    dst = _connect(dst_cfg, ssl=dst_ssl, label="DEST (TiDB)")

    with src.cursor() as c:
        c.execute("SHOW TABLES")
        tables = [row[0] for row in c.fetchall()]
    if not tables:
        sys.exit("[migrate] source has no tables — wrong MYSQL_SRC_URL?")
    print(f"[migrate] {len(tables)} source table(s): {tables}")

    with dst.cursor() as dc:
        dc.execute("SET FOREIGN_KEY_CHECKS=0")
        dc.execute("SHOW TABLES")
        dst_tables = {row[0] for row in dc.fetchall()}

    counts: dict[str, int] = {}
    for t in tables:
        # kv_store goes through db.kv_save so oversized documents (e.g. the
        # ~24MB arc_seat blob) get gzip-compressed to fit row-size caps.
        if t == "kv_store":
            with src.cursor() as c:
                c.execute("SELECT name, data FROM `kv_store`")
                kv_rows = c.fetchall()
            with dst.cursor() as dc:
                dc.execute("DELETE FROM `kv_store`")
            for name, data in kv_rows:
                if data is None:
                    continue
                obj = data if isinstance(data, (dict, list)) else json.loads(data)
                db.kv_save(name, obj)
            counts[t] = len(kv_rows)
            print(f"[migrate]   {t}: {len(kv_rows)} doc(s) copied (compressed if large)")
            continue

        with src.cursor() as c:
            if t not in dst_tables:
                c.execute(f"SHOW CREATE TABLE `{t}`")
                create_sql = _sanitise_ddl(c.fetchone()[1])
                with dst.cursor() as dc:
                    dc.execute(create_sql)
                    dst_tables.add(t)
            c.execute(f"SELECT * FROM `{t}`")
            rows = c.fetchall()
            cols = [d[0] for d in c.description]

        with dst.cursor() as dc:
            dc.execute(f"DELETE FROM `{t}`")
            if rows:
                placeholders = ",".join(["%s"] * len(cols))
                collist = ",".join(f"`{col}`" for col in cols)
                dc.executemany(
                    f"INSERT INTO `{t}` ({collist}) VALUES ({placeholders})",
                    rows,
                )
        counts[t] = len(rows)
        print(f"[migrate]   {t}: {len(rows)} row(s) copied")

    with dst.cursor() as dc:
        dc.execute("SET FOREIGN_KEY_CHECKS=1")

    print("\n[migrate] DONE. Verify these counts against the source:")
    for t, n in sorted(counts.items()):
        print(f"  {t:24} {n}")

    src.close()
    dst.close()


if __name__ == "__main__":
    main()
