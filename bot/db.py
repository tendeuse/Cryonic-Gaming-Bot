from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


class MissionDB:
    def __init__(self, sqlite_path: str) -> None:
        self.sqlite_path = sqlite_path
        Path(sqlite_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def _connect(self) -> Iterable[sqlite3.Connection]:
        conn = sqlite3.connect(self.sqlite_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS missions (
                    mission_id TEXT PRIMARY KEY,
                    revision INTEGER NOT NULL,
                    pack_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    json TEXT NOT NULL,
                    deprecated INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS packs (
                    pack_id TEXT PRIMARY KEY,
                    revision INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    json TEXT NOT NULL,
                    published INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    actor TEXT NOT NULL,
                    action TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    details TEXT,
                    created_at TEXT NOT NULL
                );
                """
            )

    def seed_defaults(self) -> None:
        defaults = load_default_seed()
        for pack in defaults["packs"]:
            self.upsert_pack(pack, actor="seed")
        for mission in defaults["missions"]:
            self.upsert_mission(mission, actor="seed")

    def upsert_mission(self, mission: dict[str, Any], actor: str) -> None:
        mission_id = mission["mission_id"]
        now = utc_now()
        with self._connect() as conn:
            row = conn.execute("SELECT revision FROM missions WHERE mission_id = ?", (mission_id,)).fetchone()
            revision = int(row["revision"]) + 1 if row else 1
            mission["revision"] = revision
            mission["updated_at"] = now
            conn.execute(
                """
                INSERT INTO missions (mission_id, revision, pack_id, title, json, deprecated, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(mission_id) DO UPDATE SET
                    revision=excluded.revision,
                    pack_id=excluded.pack_id,
                    title=excluded.title,
                    json=excluded.json,
                    deprecated=excluded.deprecated,
                    updated_at=excluded.updated_at;
                """,
                (
                    mission_id,
                    revision,
                    mission["pack_id"],
                    mission["title"],
                    json.dumps(mission),
                    1 if mission.get("deprecated") else 0,
                    now,
                ),
            )
            self._audit(conn, actor, "upsert_mission", mission_id, json.dumps(mission))

    def upsert_pack(self, pack: dict[str, Any], actor: str) -> None:
        pack_id = pack["pack_id"]
        now = utc_now()
        with self._connect() as conn:
            row = conn.execute("SELECT revision FROM packs WHERE pack_id = ?", (pack_id,)).fetchone()
            revision = int(row["revision"]) + 1 if row else 1
            pack["revision"] = revision
            pack["updated_at"] = now
            conn.execute(
                """
                INSERT INTO packs (pack_id, revision, name, json, published, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(pack_id) DO UPDATE SET
                    revision=excluded.revision,
                    name=excluded.name,
                    json=excluded.json,
                    published=excluded.published,
                    updated_at=excluded.updated_at;
                """,
                (pack_id, revision, pack["name"], json.dumps(pack), 1 if pack.get("published") else 0, now),
            )
            self._audit(conn, actor, "upsert_pack", pack_id, json.dumps(pack))

    def deprecate_mission(self, mission_id: str, actor: str) -> bool:
        with self._connect() as conn:
            existing = conn.execute("SELECT mission_id, json FROM missions WHERE mission_id = ?", (mission_id,)).fetchone()
            if not existing:
                return False
            mission = json.loads(existing["json"])
            mission["deprecated"] = True
            self.upsert_mission(mission, actor=actor)
            return True

    def list_missions(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT json FROM missions ORDER BY mission_id").fetchall()
            return [json.loads(r["json"]) for r in rows]

    def get_mission(self, mission_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT json FROM missions WHERE mission_id = ?", (mission_id,)).fetchone()
            return json.loads(row["json"]) if row else None

    def list_packs(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT json FROM packs ORDER BY pack_id").fetchall()
            return [json.loads(r["json"]) for r in rows]

    def get_pack_with_missions(self, pack_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT json FROM packs WHERE pack_id = ?", (pack_id,)).fetchone()
            if not row:
                return None
            pack = json.loads(row["json"])
            missions = conn.execute("SELECT json FROM missions WHERE pack_id = ? AND deprecated = 0", (pack_id,)).fetchall()
            pack["missions"] = [json.loads(r["json"]) for r in missions]
            return pack

    def updates_since(self, since: str) -> dict[str, Any]:
        with self._connect() as conn:
            missions = conn.execute("SELECT json FROM missions WHERE updated_at > ?", (since,)).fetchall()
            packs = conn.execute("SELECT json FROM packs WHERE updated_at > ?", (since,)).fetchall()
            deprecated = conn.execute(
                "SELECT mission_id FROM missions WHERE updated_at > ? AND deprecated = 1", (since,)
            ).fetchall()
            return {
                "server_time": utc_now(),
                "changed_missions": [json.loads(r["json"]) for r in missions],
                "changed_packs": [json.loads(r["json"]) for r in packs],
                "deprecated_mission_ids": [r["mission_id"] for r in deprecated],
            }

    @staticmethod
    def _audit(conn: sqlite3.Connection, actor: str, action: str, target_id: str, details: str) -> None:
        conn.execute(
            "INSERT INTO audit_log (actor, action, target_id, details, created_at) VALUES (?, ?, ?, ?, ?)",
            (actor, action, target_id, details, utc_now()),
        )


def load_default_seed() -> dict[str, Any]:
    seed_path = Path(__file__).parent / "seed_missions.json"
    return json.loads(seed_path.read_text(encoding="utf-8"))
