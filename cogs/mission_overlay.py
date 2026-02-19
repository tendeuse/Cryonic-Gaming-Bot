import os
import json
import sqlite3
import asyncio
import secrets
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

import discord
from discord.ext import commands
from discord import app_commands

from fastapi import FastAPI, HTTPException, Request
import uvicorn

DB_PATH = "/data/mission_overlay.db"
MASTER_API_KEY = os.getenv("OVERLAY_API_KEY")  # master key (do NOT ship to overlay)

# Pairing settings
PAIR_CODE_TTL_MINUTES = int(os.getenv("OVERLAY_PAIR_TTL_MINUTES", "10"))
PAIR_TOKEN_TTL_DAYS = int(os.getenv("OVERLAY_PAIR_TOKEN_TTL_DAYS", "365"))  # long-lived by default
PAIR_TOKEN_BYTES = int(os.getenv("OVERLAY_PAIR_TOKEN_BYTES", "32"))         # 32 bytes ~ 43 chars urlsafe


def now_iso() -> str:
    return datetime.utcnow().isoformat()


def utc_now() -> datetime:
    return datetime.utcnow()


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

        # Pairing codes (short-lived)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS overlay_pair_codes(
            code TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            issued_by_discord_id TEXT NOT NULL,
            note TEXT
        )""")

        # Issued overlay tokens (long-lived)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS overlay_tokens(
            token TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            issued_by_discord_id TEXT NOT NULL,
            revoked INTEGER DEFAULT 0
        )""")

        # Default packs
        cur.execute("""
        INSERT OR IGNORE INTO packs VALUES
        ('default-caldari','Caldari State','CALDARI',1),
        ('ore-pack','ORE','ORE',0),
        ('concord-pack','CONCORD','CONCORD',0),
        ('edencom-pack','EDENCOM','EDENCOM',0),
        ('soe-pack','Sisters of EVE','SOE',0)
        """)

        con.commit()


def _parse_iso(dt_str: str) -> datetime:
    # stored via datetime.isoformat() -> parse with fromisoformat
    return datetime.fromisoformat(dt_str)


def _is_expired(expires_at_iso: str) -> bool:
    try:
        return utc_now() >= _parse_iso(expires_at_iso)
    except Exception:
        # if parsing fails, treat as expired for safety
        return True


def _require_master_key():
    if not MASTER_API_KEY or len(MASTER_API_KEY.strip()) < 8:
        raise RuntimeError("OVERLAY_API_KEY env var is missing/too short. Set it in Railway variables.")


class MissionOverlayCog(commands.Cog):
    """
    Discord Cog + internal FastAPI server.

    Auth model:
    - Master key: header X-Overlay-Key == OVERLAY_API_KEY (server secret)
    - Overlay token: header X-Overlay-Token == token issued via /overlay pair
      Tokens are issued by Discord command /overlay pair and exchanged via HTTP endpoint.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        ensure_db()
        _require_master_key()

        self.app = FastAPI()
        self.app.middleware("http")(self.auth)

        # Existing endpoints
        self.app.get("/overlay/api/v1/packs")(self.get_packs)
        self.app.get("/overlay/api/v1/missions/{mission_id}")(self.get_mission)

        # Pairing endpoints
        # Overlay calls this with a one-time code and receives a long-lived token.
        self.app.post("/overlay/api/v1/pair/exchange")(self.pair_exchange)

        asyncio.create_task(self.run_api())

    # -------------------------
    # AUTH MIDDLEWARE
    # -------------------------
    async def auth(self, req: Request, call_next):
        """
        Accept either:
        - Master key in X-Overlay-Key (server-to-server / admin)
        - Issued overlay token in X-Overlay-Token (end-user overlays)
        """
        # Allow health checks without auth if you later add them (none right now)
        path = req.url.path

        # Pair exchange: requires master key OR valid short-lived pairing code in body (we check in handler),
        # so we skip auth here and enforce inside handler to avoid reading body twice in middleware.
        if path == "/overlay/api/v1/pair/exchange":
            return await call_next(req)

        master = req.headers.get("X-Overlay-Key")
        if master and master == MASTER_API_KEY:
            return await call_next(req)

        token = req.headers.get("X-Overlay-Token")
        if token and await self._token_is_valid(token):
            return await call_next(req)

        raise HTTPException(status_code=401, detail="Unauthorized")

    async def _token_is_valid(self, token: str) -> bool:
        with sqlite3.connect(DB_PATH) as con:
            cur = con.cursor()
            cur.execute("SELECT expires_at, revoked FROM overlay_tokens WHERE token=?", (token,))
            row = cur.fetchone()
        if not row:
            return False
        expires_at, revoked = row[0], row[1]
        if revoked:
            return False
        if _is_expired(expires_at):
            return False
        return True

    # -------------------------
    # FASTAPI SERVER
    # -------------------------
    async def run_api(self):
        config = uvicorn.Config(self.app, host="0.0.0.0", port=8000, loop="asyncio", log_level="info")
        await uvicorn.Server(config).serve()

    # -------------------------
    # API ENDPOINTS
    # -------------------------
    async def get_packs(self):
        with sqlite3.connect(DB_PATH) as con:
            cur = con.cursor()
            cur.execute("SELECT pack_id, title FROM packs WHERE published=1")
            return [{"pack": r[0], "title": r[1]} for r in cur.fetchall()]

    async def get_mission(self, mission_id: str):
        with sqlite3.connect(DB_PATH) as con:
            cur = con.cursor()
            cur.execute("SELECT * FROM missions WHERE mission_id=?", (mission_id,))
            row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Not found")

        return {
            "mission_id": row[0],
            "revision": row[1],
            "pack_id": row[2],
            "title": row[3],
            "lore": row[4],
            "faction": row[5],
            "alpha_omega": row[6],
            "objectives": json.loads(row[7]) if row[7] else [],
            "rewards": json.loads(row[8]) if row[8] else {},
            "created_at": row[9],
            "updated_at": row[10],
            "deprecated": bool(row[11]),
        }

    async def pair_exchange(self, payload: Dict[str, Any]):
        """
        POST /overlay/api/v1/pair/exchange
        Body:
          { "code": "ABC123-XYZ789" }

        Returns:
          { "token": "<long-lived token>", "expires_at": "ISO8601" }

        Security:
          - The code is created via Discord command /overlay pair (ephemeral).
          - Code expires quickly (default 10 minutes).
          - No master key needed for exchange (the code is the proof).
        """
        code = (payload or {}).get("code", "")
        if not isinstance(code, str) or len(code.strip()) < 8:
            raise HTTPException(status_code=400, detail="Invalid code")

        code = code.strip()

        with sqlite3.connect(DB_PATH) as con:
            cur = con.cursor()
            cur.execute("SELECT expires_at, issued_by_discord_id FROM overlay_pair_codes WHERE code=?", (code,))
            row = cur.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="Code not found")

            expires_at, issued_by = row[0], row[1]
            if _is_expired(expires_at):
                # delete expired code
                cur.execute("DELETE FROM overlay_pair_codes WHERE code=?", (code,))
                con.commit()
                raise HTTPException(status_code=410, detail="Code expired")

            # One-time use: delete code now
            cur.execute("DELETE FROM overlay_pair_codes WHERE code=?", (code,))

            # Create token
            token = secrets.token_urlsafe(PAIR_TOKEN_BYTES)
            created_at = now_iso()
            token_expires_at = (utc_now() + timedelta(days=PAIR_TOKEN_TTL_DAYS)).isoformat()

            cur.execute(
                "INSERT INTO overlay_tokens(token, created_at, expires_at, issued_by_discord_id, revoked) VALUES(?,?,?,?,0)",
                (token, created_at, token_expires_at, issued_by),
            )
            con.commit()

        return {"token": token, "expires_at": token_expires_at}

    # -------------------------
    # DISCORD COMMANDS
    # -------------------------
    overlay_group = app_commands.Group(name="overlay", description="Overlay pairing and management")

    @overlay_group.command(name="pair", description="Generate a one-time code to pair the Windows overlay.")
    async def overlay_pair(self, interaction: discord.Interaction, note: Optional[str] = None):
        """
        /overlay pair [note]
        - Returns an ephemeral one-time pairing code.
        - User enters the code in the overlay once; overlay receives a long-lived token.
        """
        # Optional: restrict who can pair (uncomment if desired)
        # if not interaction.user.guild_permissions.manage_guild:
        #     await interaction.response.send_message("You don't have permission to pair overlays.", ephemeral=True)
        #     return

        code = self._make_pair_code()
        created_at = now_iso()
        expires_at = (utc_now() + timedelta(minutes=PAIR_CODE_TTL_MINUTES)).isoformat()
        issued_by = str(interaction.user.id)

        with sqlite3.connect(DB_PATH) as con:
            cur = con.cursor()
            cur.execute(
                "INSERT INTO overlay_pair_codes(code, created_at, expires_at, issued_by_discord_id, note) VALUES(?,?,?,?,?)",
                (code, created_at, expires_at, issued_by, note),
            )
            con.commit()

        await interaction.response.send_message(
            f"**Overlay Pairing Code (expires in {PAIR_CODE_TTL_MINUTES} min):**\n`{code}`\n\n"
            f"Enter this in the overlay to get a token. You only need to pair once per device.",
            ephemeral=True
        )

    @overlay_group.command(name="revoke", description="Revoke an overlay token (admin).")
    async def overlay_revoke(self, interaction: discord.Interaction, token: str):
        """
        /overlay revoke <token>
        Marks a token as revoked.
        """
        # Minimal restriction: require Manage Guild
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("Missing permission: Manage Server.", ephemeral=True)
            return

        token = token.strip()
        with sqlite3.connect(DB_PATH) as con:
            cur = con.cursor()
            cur.execute("UPDATE overlay_tokens SET revoked=1 WHERE token=?", (token,))
            changed = cur.rowcount
            con.commit()

        if changed:
            await interaction.response.send_message("Token revoked.", ephemeral=True)
        else:
            await interaction.response.send_message("Token not found.", ephemeral=True)

    def _make_pair_code(self) -> str:
        # Human-friendly format: 6-6 chars
        a = secrets.token_hex(3).upper()  # 6 hex
        b = secrets.token_hex(3).upper()
        return f"{a}-{b}"

    # Keep your existing /mission_list command too (unchanged)
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

    async def cog_load(self):
        # Register the /overlay command group when the cog loads
        try:
            self.bot.tree.add_command(self.overlay_group)
        except Exception:
            # If already added elsewhere, ignore
            pass

    async def cog_unload(self):
        try:
            self.bot.tree.remove_command(self.overlay_group.name, type=self.overlay_group.type)
        except Exception:
            pass


async def setup(bot: commands.Bot):
    await bot.add_cog(MissionOverlayCog(bot))
