import os
import json
import sqlite3
import asyncio
from datetime import datetime
from typing import Optional

import discord
from discord.ext import commands
from discord import app_commands

from fastapi import FastAPI, HTTPException, Request
import uvicorn

DB_PATH = "/data/mission_overlay.db"
API_KEY = os.getenv("OVERLAY_API_KEY")

def now():
    return datetime.utcnow().isoformat()

def ensure_db():
    os.makedirs("/data", exist_ok=True)

    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS packs(
            pack_id TEXT PRIMARY KEY,
            title TEXT,
            faction TEXT,
            published INTEGER DEFAULT 0
        )""")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS missions(
            mission_id TEXT PRIMARY KEY,
            revision INTEGER,
            pack_id TEXT,
            title TEXT,
            lore TEXT,
            faction TEXT,
            alpha_omega TEXT,
            objectives TEXT,
            rewards TEXT,
            created_at TEXT,
            updated_at TEXT,
            deprecated INTEGER DEFAULT 0
        )""")

        cur.execute("""
        INSERT OR IGNORE INTO packs VALUES
        ('default-caldari','Caldari State','CALDARI',1),
        ('ore-pack','ORE','ORE',0),
        ('concord-pack','CONCORD','CONCORD',0),
        ('edencom-pack','EDENCOM','EDENCOM',0),
        ('soe-pack','Sisters of EVE','SOE',0)
        """)

        con.commit()

class MissionOverlayCog(commands.Cog):

    def __init__(self, bot):
        self.bot = bot
        ensure_db()

        self.app = FastAPI()
        self.app.middleware("http")(self.auth)

        self.app.get("/overlay/api/v1/packs")(self.get_packs)
        self.app.get("/overlay/api/v1/missions/{mission_id}")(self.get_mission)

        asyncio.create_task(self.run_api())

    async def auth(self, req: Request, call_next):
        if req.headers.get("X-Overlay-Key") != API_KEY:
            raise HTTPException(401)
        return await call_next(req)

    async def run_api(self):
        config = uvicorn.Config(self.app, host="0.0.0.0", port=8000, loop="asyncio")
        await uvicorn.Server(config).serve()

    async def get_packs(self):
        with sqlite3.connect(DB_PATH) as con:
            cur = con.cursor()
            cur.execute("SELECT pack_id,title FROM packs WHERE published=1")
            return [{"pack":r[0],"title":r[1]} for r in cur.fetchall()]

    async def get_mission(self, mission_id):
        with sqlite3.connect(DB_PATH) as con:
            cur = con.cursor()
            cur.execute("SELECT * FROM missions WHERE mission_id=?", (mission_id,))
            row = cur.fetchone()

        if not row:
            raise HTTPException(404)

        return {
            "mission_id": row[0],
            "revision": row[1],
            "pack_id": row[2],
            "title": row[3],
            "lore": row[4],
            "faction": row[5],
            "alpha_omega": row[6],
            "objectives": json.loads(row[7]),
            "rewards": json.loads(row[8])
        }

    @app_commands.command(name="mission_list")
    async def mission_list(self, interaction: discord.Interaction):
        with sqlite3.connect(DB_PATH) as con:
            cur = con.cursor()
            cur.execute("SELECT mission_id FROM missions")
            rows = cur.fetchall()

        await interaction.response.send_message(
            "\n".join(r[0] for r in rows) or "No missions yet.",
            ephemeral=True
        )

async def setup(bot):
    await bot.add_cog(MissionOverlayCog(bot))
