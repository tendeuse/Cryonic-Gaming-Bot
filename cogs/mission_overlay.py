from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import discord
from discord import app_commands
from discord.ext import commands
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
import uvicorn

log = logging.getLogger(__name__)

DB_PATH = Path("/data/mission_overlay.db")
DEFAULT_PACKS = [
    {
        "pack_id": "default-caldari",
        "name": "Default Caldari Missions",
        "faction": "CALDARI",
        "description": "Core reputation and LP progression for the Caldari State.",
        "published": 1,
    },
    {"pack_id": "ore-pack", "name": "ORE Pack", "faction": "ORE", "description": "ORE missions.", "published": 0},
    {
        "pack_id": "concord-pack",
        "name": "CONCORD Pack",
        "faction": "CONCORD",
        "description": "CONCORD operations.",
        "published": 0,
    },
    {
        "pack_id": "edencom-pack",
        "name": "EDENCOM Pack",
        "faction": "EDENCOM",
        "description": "EDENCOM defense missions.",
        "published": 0,
    },
    {
        "pack_id": "soe-pack",
        "name": "Sisters of EVE Pack",
        "faction": "SOE",
        "description": "Sisters of EVE support missions.",
        "published": 0,
    },
]


@dataclass
class MissionEnvelope:
    mission_id: str
    pack_id: str
    title: str
    lore: str
    faction: str
    alpha_omega: str
    objectives: list[dict[str, Any]]
    rewards: dict[str, Any]


class MissionOverlayCog(commands.Cog):
    """Mission content management + embedded overlay API for existing bots."""

    mission_group = app_commands.Group(name="mission", description="Mission overlay admin commands")
    pack_group = app_commands.Group(name="pack", description="Mission pack commands", parent=mission_group)

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db_path = DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.overlay_key = os.getenv("OVERLAY_API_KEY", "")
        if not self.overlay_key:
            log.warning("OVERLAY_API_KEY missing; API will reject all requests.")

        self._db_lock = asyncio.Lock()
        self._http_task: Optional[asyncio.Task[None]] = None
        self._uvicorn_server: Optional[uvicorn.Server] = None
        self._ready = False

        self._init_db()
        self._seed_defaults()
        self._setup_fastapi()

    async def cog_load(self) -> None:
        if not self._ready:
            self.bot.tree.add_command(self.mission_group)
            self._ready = True
        self._http_task = asyncio.create_task(self._run_http_server(), name="mission-overlay-http")

    async def cog_unload(self) -> None:
        if self._ready:
            self.bot.tree.remove_command(self.mission_group.name)
            self._ready = False
        if self._uvicorn_server:
            self._uvicorn_server.should_exit = True
        if self._http_task:
            self._http_task.cancel()
            try:
                await self._http_task
            except asyncio.CancelledError:
                pass

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        return con

    def _init_db(self) -> None:
        with self._connect() as con:
            con.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS mission_packs (
                    pack_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    faction TEXT NOT NULL,
                    description TEXT NOT NULL,
                    published INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS missions (
                    mission_id TEXT PRIMARY KEY,
                    revision INTEGER NOT NULL,
                    pack_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    lore TEXT NOT NULL,
                    faction TEXT NOT NULL,
                    alpha_omega TEXT NOT NULL CHECK(alpha_omega IN ('ALPHA','OMEGA','BOTH')),
                    objectives_json TEXT NOT NULL,
                    rewards_json TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'DRAFT' CHECK(status IN ('DRAFT','PUBLISHED','DEPRECATED')),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(pack_id) REFERENCES mission_packs(pack_id)
                );

                CREATE TABLE IF NOT EXISTS mission_revisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mission_id TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    snapshot_json TEXT NOT NULL,
                    changed_by TEXT NOT NULL,
                    changed_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS change_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_type TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    details_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )

    def _seed_defaults(self) -> None:
        now = self._now()
        with self._connect() as con:
            for pack in DEFAULT_PACKS:
                con.execute(
                    """
                    INSERT OR IGNORE INTO mission_packs (
                        pack_id,name,faction,description,published,created_at,updated_at
                    ) VALUES (?,?,?,?,?,?,?)
                    """,
                    (pack["pack_id"], pack["name"], pack["faction"], pack["description"], pack["published"], now, now),
                )

    def _setup_fastapi(self) -> None:
        app = FastAPI(title="Mission Overlay API", version="1.0.0")

        def auth(key: str | None) -> None:
            if not self.overlay_key or key != self.overlay_key:
                raise HTTPException(status_code=401, detail="Unauthorized")

        @app.get("/overlay/api/v1/packs")
        async def get_packs(x_overlay_key: str | None = Header(default=None)) -> Any:
            auth(x_overlay_key)
            rows = await self._fetch_all("SELECT * FROM mission_packs WHERE published = 1 ORDER BY pack_id")
            return [dict(row) for row in rows]

        @app.get("/overlay/api/v1/packs/{pack_id}")
        async def get_pack(pack_id: str, x_overlay_key: str | None = Header(default=None)) -> Any:
            auth(x_overlay_key)
            pack = await self._fetch_one("SELECT * FROM mission_packs WHERE pack_id = ?", (pack_id,))
            if not pack:
                raise HTTPException(status_code=404, detail="Pack not found")
            missions = await self._fetch_all(
                "SELECT mission_id,revision,title,faction,alpha_omega,status,updated_at FROM missions WHERE pack_id = ? AND status = 'PUBLISHED' ORDER BY mission_id",
                (pack_id,),
            )
            payload = dict(pack)
            payload["missions"] = [dict(m) for m in missions]
            return payload

        @app.get("/overlay/api/v1/missions/{mission_id}")
        async def get_mission(mission_id: str, x_overlay_key: str | None = Header(default=None)) -> Any:
            auth(x_overlay_key)
            mission = await self._fetch_one("SELECT * FROM missions WHERE mission_id = ?", (mission_id,))
            if not mission:
                raise HTTPException(status_code=404, detail="Mission not found")
            return self._mission_row_to_payload(mission)

        @app.get("/overlay/api/v1/updates")
        async def updates(since: str, x_overlay_key: str | None = Header(default=None)) -> Any:
            auth(x_overlay_key)
            rows = await self._fetch_all(
                "SELECT * FROM missions WHERE updated_at > ? ORDER BY updated_at ASC",
                (since,),
            )
            return JSONResponse({"updates": [self._mission_row_to_payload(row) for row in rows]})

        self._fastapi = app

    async def _run_http_server(self) -> None:
        host = os.getenv("OVERLAY_API_HOST", "127.0.0.1")
        port = int(os.getenv("OVERLAY_API_PORT", "8765"))
        cfg = uvicorn.Config(self._fastapi, host=host, port=port, log_level="warning", loop="asyncio")
        self._uvicorn_server = uvicorn.Server(cfg)
        log.info("Starting mission overlay API on %s:%s", host, port)
        await self._uvicorn_server.serve()

    async def _fetch_all(self, query: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        async with self._db_lock:
            with self._connect() as con:
                return list(con.execute(query, params).fetchall())

    async def _fetch_one(self, query: str, params: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        async with self._db_lock:
            with self._connect() as con:
                return con.execute(query, params).fetchone()

    async def _execute(self, query: str, params: tuple[Any, ...] = ()) -> None:
        async with self._db_lock:
            with self._connect() as con:
                con.execute(query, params)
                con.commit()

    async def _log_change(self, entity_type: str, entity_id: str, action: str, actor: str, details: dict[str, Any]) -> None:
        await self._execute(
            "INSERT INTO change_log (entity_type,entity_id,action,actor,details_json,created_at) VALUES (?,?,?,?,?,?)",
            (entity_type, entity_id, action, actor, json.dumps(details), self._now()),
        )

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _validate_mission_payload(self, payload: dict[str, Any]) -> MissionEnvelope:
        required = ["mission_id", "pack_id", "title", "lore", "faction", "alpha_omega", "objectives", "rewards"]
        missing = [k for k in required if k not in payload]
        if missing:
            raise ValueError(f"Missing fields: {', '.join(missing)}")
        if payload["alpha_omega"] not in {"ALPHA", "OMEGA", "BOTH"}:
            raise ValueError("alpha_omega must be ALPHA/OMEGA/BOTH")
        if not isinstance(payload["objectives"], list):
            raise ValueError("objectives must be a list")
        if not isinstance(payload["rewards"], dict):
            raise ValueError("rewards must be an object")

        for obj in payload["objectives"]:
            if not isinstance(obj, dict) or "id" not in obj or "type" not in obj or "target" not in obj:
                raise ValueError("each objective needs id/type/target")
            if obj["type"] not in {"standings_at_least", "skills_trained", "wallet_isk_change", "lp_total"}:
                raise ValueError(f"unsupported objective type {obj['type']}")

        return MissionEnvelope(
            mission_id=str(payload["mission_id"]),
            pack_id=str(payload["pack_id"]),
            title=str(payload["title"]),
            lore=str(payload["lore"]),
            faction=str(payload["faction"]),
            alpha_omega=str(payload["alpha_omega"]),
            objectives=payload["objectives"],
            rewards=payload["rewards"],
        )

    def _mission_row_to_payload(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "mission_id": row["mission_id"],
            "revision": row["revision"],
            "pack_id": row["pack_id"],
            "title": row["title"],
            "lore": row["lore"],
            "faction": row["faction"],
            "alpha_omega": row["alpha_omega"],
            "objectives": json.loads(row["objectives_json"]),
            "rewards": json.loads(row["rewards_json"]),
            "status": row["status"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    @mission_group.command(name="create", description="Create a mission from JSON payload.")
    @app_commands.describe(payload_json="Mission JSON payload")
    async def mission_create(self, interaction: discord.Interaction, payload_json: str) -> None:
        try:
            payload = self._validate_mission_payload(json.loads(payload_json))
        except Exception as exc:
            await interaction.response.send_message(f"Validation failed: {exc}", ephemeral=True)
            return

        now = self._now()
        actor = str(interaction.user)
        async with self._db_lock:
            with self._connect() as con:
                exists = con.execute("SELECT 1 FROM missions WHERE mission_id = ?", (payload.mission_id,)).fetchone()
                if exists:
                    await interaction.response.send_message("mission_id already exists", ephemeral=True)
                    return
                con.execute(
                    """
                    INSERT INTO missions (
                      mission_id,revision,pack_id,title,lore,faction,alpha_omega,
                      objectives_json,rewards_json,status,created_at,updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        payload.mission_id,
                        1,
                        payload.pack_id,
                        payload.title,
                        payload.lore,
                        payload.faction,
                        payload.alpha_omega,
                        json.dumps(payload.objectives),
                        json.dumps(payload.rewards),
                        "DRAFT",
                        now,
                        now,
                    ),
                )
                snapshot = con.execute("SELECT * FROM missions WHERE mission_id = ?", (payload.mission_id,)).fetchone()
                con.execute(
                    "INSERT INTO mission_revisions (mission_id,revision,snapshot_json,changed_by,changed_at) VALUES (?,?,?,?,?)",
                    (payload.mission_id, 1, json.dumps(self._mission_row_to_payload(snapshot)), actor, now),
                )
                con.commit()

        await self._log_change("mission", payload.mission_id, "create", actor, {"revision": 1})
        await interaction.response.send_message(f"Created mission `{payload.mission_id}` revision 1", ephemeral=True)

    @mission_group.command(name="edit", description="Edit existing mission with JSON patch (full mission expected).")
    @app_commands.describe(mission_id="Mission ID", payload_json="Full mission JSON payload")
    async def mission_edit(self, interaction: discord.Interaction, mission_id: str, payload_json: str) -> None:
        try:
            payload_data = json.loads(payload_json)
            payload_data["mission_id"] = mission_id
            payload = self._validate_mission_payload(payload_data)
        except Exception as exc:
            await interaction.response.send_message(f"Validation failed: {exc}", ephemeral=True)
            return

        actor = str(interaction.user)
        now = self._now()
        async with self._db_lock:
            with self._connect() as con:
                row = con.execute("SELECT revision,status,created_at FROM missions WHERE mission_id = ?", (mission_id,)).fetchone()
                if not row:
                    await interaction.response.send_message("Mission not found", ephemeral=True)
                    return
                revision = int(row["revision"]) + 1
                con.execute(
                    """
                    UPDATE missions
                    SET revision=?,pack_id=?,title=?,lore=?,faction=?,alpha_omega=?,objectives_json=?,rewards_json=?,updated_at=?
                    WHERE mission_id=?
                    """,
                    (
                        revision,
                        payload.pack_id,
                        payload.title,
                        payload.lore,
                        payload.faction,
                        payload.alpha_omega,
                        json.dumps(payload.objectives),
                        json.dumps(payload.rewards),
                        now,
                        mission_id,
                    ),
                )
                snapshot = con.execute("SELECT * FROM missions WHERE mission_id = ?", (mission_id,)).fetchone()
                con.execute(
                    "INSERT INTO mission_revisions (mission_id,revision,snapshot_json,changed_by,changed_at) VALUES (?,?,?,?,?)",
                    (mission_id, revision, json.dumps(self._mission_row_to_payload(snapshot)), actor, now),
                )
                con.commit()

        await self._log_change("mission", mission_id, "edit", actor, {"revision": revision})
        await interaction.response.send_message(f"Mission `{mission_id}` updated to revision {revision}", ephemeral=True)

    @mission_group.command(name="publish", description="Publish mission.")
    async def mission_publish(self, interaction: discord.Interaction, mission_id: str) -> None:
        row = await self._fetch_one("SELECT status FROM missions WHERE mission_id = ?", (mission_id,))
        if not row:
            await interaction.response.send_message("Mission not found", ephemeral=True)
            return
        await self._execute("UPDATE missions SET status='PUBLISHED', updated_at=? WHERE mission_id=?", (self._now(), mission_id))
        await self._log_change("mission", mission_id, "publish", str(interaction.user), {})
        await interaction.response.send_message(f"Published `{mission_id}`", ephemeral=True)

    @mission_group.command(name="list", description="List missions.")
    async def mission_list(self, interaction: discord.Interaction, pack_id: Optional[str] = None) -> None:
        if pack_id:
            rows = await self._fetch_all(
                "SELECT mission_id,revision,status,pack_id,title FROM missions WHERE pack_id = ? ORDER BY mission_id",
                (pack_id,),
            )
        else:
            rows = await self._fetch_all("SELECT mission_id,revision,status,pack_id,title FROM missions ORDER BY mission_id")
        if not rows:
            await interaction.response.send_message("No missions found.", ephemeral=True)
            return
        text = "\n".join(
            f"`{r['mission_id']}` rev:{r['revision']} [{r['status']}] ({r['pack_id']}) - {r['title']}" for r in rows[:40]
        )
        await interaction.response.send_message(text, ephemeral=True)

    @mission_group.command(name="show", description="Show full mission JSON.")
    async def mission_show(self, interaction: discord.Interaction, mission_id: str) -> None:
        row = await self._fetch_one("SELECT * FROM missions WHERE mission_id = ?", (mission_id,))
        if not row:
            await interaction.response.send_message("Mission not found", ephemeral=True)
            return
        payload = json.dumps(self._mission_row_to_payload(row), indent=2)
        await interaction.response.send_message(f"```json\n{payload[:1800]}\n```", ephemeral=True)

    @mission_group.command(name="deprecate", description="Deprecate mission.")
    async def mission_deprecate(self, interaction: discord.Interaction, mission_id: str) -> None:
        row = await self._fetch_one("SELECT status FROM missions WHERE mission_id = ?", (mission_id,))
        if not row:
            await interaction.response.send_message("Mission not found", ephemeral=True)
            return
        await self._execute("UPDATE missions SET status='DEPRECATED', updated_at=? WHERE mission_id=?", (self._now(), mission_id))
        await self._log_change("mission", mission_id, "deprecate", str(interaction.user), {})
        await interaction.response.send_message(f"Deprecated `{mission_id}`", ephemeral=True)

    @pack_group.command(name="create", description="Create mission pack.")
    async def mission_pack_create(
        self,
        interaction: discord.Interaction,
        pack_id: str,
        name: str,
        faction: str,
        description: str,
    ) -> None:
        now = self._now()
        try:
            await self._execute(
                "INSERT INTO mission_packs (pack_id,name,faction,description,published,created_at,updated_at) VALUES (?,?,?,?,0,?,?)",
                (pack_id, name, faction, description, now, now),
            )
        except sqlite3.IntegrityError:
            await interaction.response.send_message("Pack already exists", ephemeral=True)
            return
        await self._log_change("pack", pack_id, "create", str(interaction.user), {"name": name})
        await interaction.response.send_message(f"Created pack `{pack_id}`", ephemeral=True)

    @pack_group.command(name="publish", description="Publish mission pack.")
    async def mission_pack_publish(self, interaction: discord.Interaction, pack_id: str) -> None:
        row = await self._fetch_one("SELECT pack_id FROM mission_packs WHERE pack_id = ?", (pack_id,))
        if not row:
            await interaction.response.send_message("Pack not found", ephemeral=True)
            return
        await self._execute("UPDATE mission_packs SET published=1, updated_at=? WHERE pack_id=?", (self._now(), pack_id))
        await self._log_change("pack", pack_id, "publish", str(interaction.user), {})
        await interaction.response.send_message(f"Published pack `{pack_id}`", ephemeral=True)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(MissionOverlayCog(bot))
