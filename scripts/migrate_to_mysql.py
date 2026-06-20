"""
scripts/migrate_to_mysql.py — one-time migration from the Railway volume to MySQL.

Run this ONCE, on Railway, while the /data volume is still mounted and the
MySQL env vars are present on the service. It:

  1. Creates the MySQL schema (db.init_db()).
  2. Copies every persisted JSON file under PERSIST_ROOT into the kv_store
     table (filename stem -> key), matching what the cogs now read/write.
  3. Copies every SQLite table into its MySQL table (ids preserved).
  4. Prints a per-source summary and a row-count verification.

It is idempotent: kv writes are upserts and relational copies use REPLACE INTO,
so re-running is safe. The source files on the volume are only ever READ, so a
failed run leaves the old data untouched and the old code still works.

Usage (from the repo root, with MYSQL* env vars set):
    python -m scripts.migrate_to_mysql
or
    python scripts/migrate_to_mysql.py
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path

# Make `from cogs import db` work whether run as a module or a script.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from cogs import db  # noqa: E402

# ---------------------------------------------------------------------------
# Config (mirrors the cogs)
# ---------------------------------------------------------------------------

PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
MISSION_DB_PATH = Path(os.getenv("MISSION_DB_PATH", str(PERSIST_ROOT / "missions.db")))

# Directories under the volume to ignore (generated artifacts, not source data).
SKIP_DIRS = {"ap_exports"}

# Top-level JSON files to ignore entirely:
#  - ap_data.json: handled specially (audit split out — see _migrate_ap_data)
#  - the rest: legacy files written by cogs that no longer exist in the codebase.
SKIP_JSON_FILES = {
    "ap_data.json",
    "transfer_applications.json",
    "shift_state.json",
    "new_member_tracker.json",
}

# Files whose kv key differs from the filename stem.
SPECIAL_FILE_KEYS = {
    "data.json": "embed_builder",   # embed_builder.py used a relative data.json
}

# Relative-path files some cogs wrote to the CWD instead of the volume.
CWD_FILES = ("data.json", "signature_tagging_attempts.json")

# SQLite database -> tables to copy. Column names already match the MySQL schema.
SQLITE_SOURCES = [
    (MISSION_DB_PATH,                       ["missions", "ap_ledger", "char_discord_map", "eve_tokens"]),
    (PERSIST_ROOT / "arc_seat.db",          ["seat_tokens"]),
    (PERSIST_ROOT / "buyback_contracts.db", ["type_cache", "price_cache", "char_name_cache", "buyback_paid"]),
    (PERSIST_ROOT / "onboarding_tickets.db", ["tickets"]),
    (PERSIST_ROOT / "recruits.db",          ["invites"]),
]


# ---------------------------------------------------------------------------
# JSON -> kv_store
# ---------------------------------------------------------------------------

def _migrate_json_file(path: Path) -> bool:
    key = SPECIAL_FILE_KEYS.get(path.name, path.stem)
    try:
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            print(f"  skip (empty): {path}")
            return False
        obj = json.loads(text)
    except Exception as e:
        print(f"  SKIP (unreadable): {path} -> {e}")
        return False
    db.kv_save(key, obj)
    print(f"  kv['{key}'] <- {path}")
    return True


def migrate_json() -> int:
    print("\n[1/2] Migrating JSON documents -> kv_store ...")
    count = 0

    if PERSIST_ROOT.exists():
        for path in sorted(PERSIST_ROOT.glob("*.json")):  # top level only; skips ap_exports/
            if path.parent.name in SKIP_DIRS:
                continue
            if path.name in SKIP_JSON_FILES:
                print(f"  skip (special/legacy): {path.name}")
                continue
            count += _migrate_json_file(path)
    else:
        print(f"  (PERSIST_ROOT {PERSIST_ROOT} does not exist — no volume JSON to migrate)")

    # Relative-path files written to the working directory.
    for name in CWD_FILES:
        p = Path(name)
        if p.exists():
            count += _migrate_json_file(p)

    print(f"  -> {count} JSON document(s) migrated.")
    return count


# ---------------------------------------------------------------------------
# ap_data.json -> slim kv doc + ap_audit table
# ---------------------------------------------------------------------------

def migrate_ap_data() -> tuple[int, int]:
    """Split the huge ap_data.json: per-user `audit` lists go into the ap_audit
    table; the remaining (tiny) document is stored under the 'ap_data' kv key.

    Returns (member_records, audit_rows_inserted). Idempotent: clears ap_audit
    first so re-runs don't duplicate rows.
    """
    path = PERSIST_ROOT / "ap_data.json"
    if not path.exists():
        path = Path("ap_data.json")  # fall back to CWD
    if not path.exists():
        print("\n[1b] ap_data.json not found — skipping AP split.")
        return (0, 0)

    print("\n[1b] Splitting ap_data.json -> kv['ap_data'] + ap_audit table ...")
    data = json.loads(path.read_text(encoding="utf-8"))

    audit_rows: list[tuple] = []
    for uid, rec in data.items():
        if not isinstance(rec, dict):
            continue
        audit = rec.pop("audit", None)   # remove from the slim doc
        if not isinstance(audit, list) or not uid.isdigit():
            continue
        user_id = int(uid)
        for e in audit:
            if not isinstance(e, dict):
                continue
            audit_rows.append((
                user_id,
                str(e.get("ts", "")),
                float(e.get("delta", 0) or 0),
                str(e.get("source", "")),
                e.get("reason"),
                e.get("actor_id"),
            ))

    db.kv_save("ap_data", data)  # slim document (audit removed)

    db.execute("DELETE FROM ap_audit")  # idempotent reset
    sql = ("INSERT INTO ap_audit (user_id, ts, delta, source, reason, actor_id) "
           "VALUES (%s, %s, %s, %s, %s, %s)")
    BATCH = 2000
    inserted = 0
    for i in range(0, len(audit_rows), BATCH):
        chunk = audit_rows[i:i + BATCH]
        db.executemany(sql, chunk)
        inserted += len(chunk)

    print(f"  kv['ap_data'] <- {len(data)} member record(s) (audit stripped)")
    print(f"  ap_audit <- {inserted} row(s) inserted")
    return (len(data), inserted)


# ---------------------------------------------------------------------------
# SQLite -> MySQL tables
# ---------------------------------------------------------------------------

def _copy_table(sqlite_path: Path, table: str) -> tuple[int, int]:
    """Copy one SQLite table into MySQL. Returns (source_rows, mysql_rows_after)."""
    con = sqlite3.connect(str(sqlite_path))
    con.row_factory = sqlite3.Row
    try:
        try:
            rows = con.execute(f"SELECT * FROM {table}").fetchall()
        except sqlite3.OperationalError as e:
            print(f"  - {table}: skipped ({e})")
            return (0, 0)
    finally:
        con.close()

    if rows:
        cols = list(rows[0].keys())
        collist = ", ".join(cols)
        placeholders = ", ".join(["%s"] * len(cols))
        sql = f"REPLACE INTO {table} ({collist}) VALUES ({placeholders})"
        db.executemany(sql, [tuple(r[c] for c in cols) for r in rows])

    after = db.fetchone(f"SELECT COUNT(*) AS n FROM {table}")
    mysql_n = int(after["n"]) if after else 0
    flag = "OK" if mysql_n >= len(rows) else "!! MISMATCH"
    print(f"  - {table}: {len(rows)} source row(s) -> {mysql_n} in MySQL  [{flag}]")
    return (len(rows), mysql_n)


def migrate_sqlite() -> int:
    print("\n[2/2] Migrating SQLite tables -> MySQL ...")
    total = 0
    for sqlite_path, tables in SQLITE_SOURCES:
        if not sqlite_path.exists():
            print(f"  (missing, skipped): {sqlite_path}")
            continue
        print(f"  {sqlite_path}:")
        for table in tables:
            src, _ = _copy_table(sqlite_path, table)
            total += src
    print(f"  -> {total} relational row(s) migrated.")
    return total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    print("=== Cryonic-Gaming-Bot: volume -> MySQL migration ===")
    print(f"PERSIST_ROOT   = {PERSIST_ROOT}")
    print(f"MISSION_DB_PATH = {MISSION_DB_PATH}")

    print("\nEnsuring MySQL schema ...")
    db.init_db()

    json_n = migrate_json()
    ap_members, ap_audit_n = migrate_ap_data()
    rel_n = migrate_sqlite()

    print("\n=== Done ===")
    print(f"JSON documents: {json_n}")
    print(f"ap_data members: {ap_members}  (audit rows -> ap_audit: {ap_audit_n})")
    print(f"Relational rows: {rel_n}")
    print("\nVerify a few bot commands, then the /data volume can be detached.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
