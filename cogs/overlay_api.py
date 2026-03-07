"""
=============================================================================
  overlay_api.py  —  À AJOUTER dans cogs/missions.py
=============================================================================

Ce fichier contient :
  1. Un mini-serveur FastAPI lancé en background thread par le cog
  2. Tous les endpoints attendus par l'overlay C#
  3. Un système de pairing code sécurisé pour lier l'overlay au bot

DÉPENDANCES — ajoutez dans requirements.txt :
  fastapi>=0.110.0
  uvicorn>=0.29.0
  python-jose[cryptography]>=3.3.0

VARIABLES D'ENVIRONNEMENT (Railway) :
  OVERLAY_JWT_SECRET   — secret aléatoire pour signer les JWT (obligatoire)
  OVERLAY_API_PORT     — port du serveur API (défaut: 8080)
  OVERLAY_TOKEN_TTL_H  — durée de vie des tokens en heures (défaut: 720 = 30 jours)

INTÉGRATION dans missions.py :
  1. Copiez ce fichier entier à la suite de vos imports dans missions.py
     OU gardez-le séparé et importez : from cogs.overlay_api import OverlayApiCog
  2. Dans setup() en bas de missions.py, ajoutez :
        await bot.add_cog(OverlayApiCog(bot))
=============================================================================
"""

import asyncio
import hashlib
import hmac
import os
import secrets
import sqlite3
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

# FastAPI imports — installed via requirements.txt
try:
    import uvicorn
    from fastapi import FastAPI, HTTPException, Depends, Header
    from fastapi.middleware.cors import CORSMiddleware
    from pydantic import BaseModel
    _FASTAPI_OK = True
except ImportError:
    _FASTAPI_OK = False
    print("[OverlayAPI] WARNING: fastapi/uvicorn not installed. Add to requirements.txt.")

try:
    from jose import jwt, JWTError
    _JOSE_OK = True
except ImportError:
    _JOSE_OK = False
    print("[OverlayAPI] WARNING: python-jose not installed. Add to requirements.txt.")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

JWT_SECRET   = os.getenv("OVERLAY_JWT_SECRET", secrets.token_hex(32))
API_PORT     = int(os.getenv("PORT", os.getenv("OVERLAY_API_PORT", "8080")))  # Railway injecte $PORT
TOKEN_TTL_H  = int(os.getenv("OVERLAY_TOKEN_TTL_H", "720"))   # 30 days
ALGORITHM    = "HS256"

# In-memory stores (reset on restart — acceptable for ephemeral data)
_pair_codes: dict[str, dict] = {}   # code → {discord_user_id, expires_at}
_intel_store: list[dict]     = []   # recent intel reports (last 50)
_MAX_INTEL   = 50


# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------

def create_token(discord_user_id: int) -> tuple[str, str]:
    """Returns (token, expires_at_iso)."""
    if not _JOSE_OK:
        raise RuntimeError("python-jose not installed")
    now     = datetime.now(timezone.utc)
    expires = now + timedelta(hours=TOKEN_TTL_H)
    payload = {
        "sub": str(discord_user_id),
        "iat": int(now.timestamp()),
        "exp": int(expires.timestamp()),
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=ALGORITHM)
    return token, expires.isoformat()


def verify_token(token: str) -> int:
    """Returns discord_user_id or raises HTTPException 401."""
    if not _JOSE_OK:
        raise HTTPException(status_code=503, detail="JWT library not installed")
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[ALGORITHM])
        return int(payload["sub"])
    except JWTError as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {e}")


# ---------------------------------------------------------------------------
# Dependency: extract Bearer token → user_id
# ---------------------------------------------------------------------------

async def get_current_user(authorization: Optional[str] = Header(None)) -> int:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = authorization.removeprefix("Bearer ").strip()
    return verify_token(token)


# ---------------------------------------------------------------------------
# Pydantic models (request/response bodies)
# ---------------------------------------------------------------------------

class PairExchangeRequest(BaseModel):
    code: str

class PairExchangeResponse(BaseModel):
    token:      str
    expires_at: str

class IntelReportRequest(BaseModel):
    system: str
    type:   str    # gate_camp | pirate | roaming | clear | neutral
    count:  int = 1
    notes:  str = ""

class MissionOut(BaseModel):
    id:          int
    title:       str
    description: str
    reward:      str
    status:      str
    created_by:  str
    assigned_to: str
    created_at:  str
    updated_at:  str

class CharacterOut(BaseModel):
    character_id:    int
    character_name:  str
    corporation:     str
    alliance:        str
    ship_name:       str
    ship_type:       str
    solar_system:    str
    region:          str
    security_status: float

class SnapshotOut(BaseModel):
    character: Optional[CharacterOut]
    missions:  list[MissionOut]
    intel:     list[dict]


# ---------------------------------------------------------------------------
# FastAPI app factory
# ---------------------------------------------------------------------------

def build_api(bot: commands.Bot, db_path: str) -> "FastAPI":
    """Build and return the FastAPI app. Called once at cog load."""
    app = FastAPI(title="ARC Overlay API", version="1.0.0", docs_url=None, redoc_url=None)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------
    @app.get("/overlay/api/v1/health")
    async def health():
        return {"status": "ok", "bot": str(bot.user)}

    # ------------------------------------------------------------------
    # Pairing — step 1: Discord /overlay pair generates a short code
    #           step 2: overlay POSTs code → receives JWT
    # ------------------------------------------------------------------
    @app.post("/overlay/api/v1/pair/exchange", response_model=PairExchangeResponse)
    async def pair_exchange(body: PairExchangeRequest):
        code = body.code.strip().upper()
        entry = _pair_codes.get(code)

        if entry is None:
            raise HTTPException(status_code=404, detail="Code not found or expired")

        if time.time() > entry["expires_at"]:
            _pair_codes.pop(code, None)
            raise HTTPException(status_code=410, detail="Code expired")

        discord_user_id = entry["discord_user_id"]
        _pair_codes.pop(code, None)   # single-use

        token, expires_at = create_token(discord_user_id)
        return PairExchangeResponse(token=token, expires_at=expires_at)

    # ------------------------------------------------------------------
    # Missions — read-only for overlay (writes go through Discord bot)
    # ------------------------------------------------------------------
    @app.get("/overlay/api/v1/missions", response_model=list[MissionOut])
    async def get_missions(
        status: Optional[str] = None,
        user_id: int = Depends(get_current_user),
    ):
        guild = _get_first_guild(bot)
        return _fetch_missions(db_path, guild, status)

    @app.post("/overlay/api/v1/missions/{mission_id}/assign", response_model=MissionOut)
    async def assign_mission(
        mission_id: int,
        user_id: int = Depends(get_current_user),
    ):
        _db_execute(db_path,
            "UPDATE missions SET assigned_to=?, status='in_progress', updated_at=? "
            "WHERE id=? AND status='open'",
            (user_id, _now(), mission_id))
        row = _db_fetchone(db_path, "SELECT * FROM missions WHERE id=?", (mission_id,))
        if row is None:
            raise HTTPException(status_code=404, detail="Mission not found")
        return _row_to_mission(row)

    @app.post("/overlay/api/v1/missions/{mission_id}/complete", response_model=MissionOut)
    async def complete_mission(
        mission_id: int,
        user_id: int = Depends(get_current_user),
    ):
        row = _db_fetchone(db_path, "SELECT * FROM missions WHERE id=?", (mission_id,))
        if row is None:
            raise HTTPException(status_code=404, detail="Mission not found")
        if row["assigned_to"] != user_id:
            raise HTTPException(status_code=403, detail="Not your mission")
        _db_execute(db_path,
            "UPDATE missions SET status='completed', updated_at=? WHERE id=? AND status='in_progress'",
            (_now(), mission_id))
        return _row_to_mission(_db_fetchone(db_path, "SELECT * FROM missions WHERE id=?", (mission_id,)))

    # ------------------------------------------------------------------
    # Character (stub — real ESI integration requires EVE SSO OAuth2)
    # The overlay will show "—" until ESI is implemented.
    # ------------------------------------------------------------------
    @app.get("/overlay/api/v1/character")
    async def get_character(user_id: int = Depends(get_current_user)):
        # TODO: Fetch from ESI using stored EVE OAuth2 token for user_id
        # For now return None → overlay shows "—" gracefully
        return None

    # ------------------------------------------------------------------
    # Intel reports
    # ------------------------------------------------------------------
    @app.get("/overlay/api/v1/intel")
    async def get_intel(user_id: int = Depends(get_current_user)):
        # Return last 10 reports, freshest first, max 30 min old
        cutoff = time.time() - 1800   # 30 min
        fresh  = [r for r in _intel_store if r["reported_at"] > cutoff]
        return fresh[-10:][::-1]

    @app.post("/overlay/api/v1/intel", status_code=201)
    async def post_intel(body: IntelReportRequest, user_id: int = Depends(get_current_user)):
        guild  = _get_first_guild(bot)
        member = guild.get_member(user_id) if guild else None
        report = {
            "system":      body.system,
            "type":        body.type,
            "count":       body.count,
            "notes":       body.notes,
            "reported_by": member.display_name if member else str(user_id),
            "reported_at": time.time(),
            "age_label":   "just now",
        }
        _intel_store.append(report)
        if len(_intel_store) > _MAX_INTEL:
            _intel_store.pop(0)

        # Mirror to Discord #eve-missions channel
        asyncio.run_coroutine_threadsafe(
            _post_intel_to_discord(bot, report),
            bot.loop
        )
        return {"ok": True}

    # ------------------------------------------------------------------
    # Snapshot — missions + character + intel in one call
    # ------------------------------------------------------------------
    @app.get("/overlay/api/v1/snapshot", response_model=SnapshotOut)
    async def get_snapshot(user_id: int = Depends(get_current_user)):
        guild    = _get_first_guild(bot)
        missions = _fetch_missions(db_path, guild, status=None)
        cutoff   = time.time() - 1800
        intel    = [r for r in _intel_store if r["reported_at"] > cutoff][-10:][::-1]
        return SnapshotOut(character=None, missions=missions, intel=intel)

    return app


# ---------------------------------------------------------------------------
# Discord → channel intel mirror
# ---------------------------------------------------------------------------

async def _post_intel_to_discord(bot: commands.Bot, report: dict):
    """Post intel report to #eve-missions channel."""
    try:
        guild = _get_first_guild(bot)
        if not guild:
            return
        channel = discord.utils.get(guild.text_channels, name="eve-missions")
        if not channel:
            return

        type_emoji = {
            "gate_camp": "⛔", "pirate": "💀",
            "roaming": "⚠️", "clear": "✅",
        }.get(report["type"], "👁️")

        embed = discord.Embed(
            title=f"{type_emoji}  Intel Report — {report['system']}",
            colour=discord.Colour.orange() if report["type"] != "clear" else discord.Colour.green(),
        )
        embed.add_field(name="Type",       value=report["type"].replace("_", " ").upper(), inline=True)
        embed.add_field(name="Count",      value=str(report["count"]),                     inline=True)
        embed.add_field(name="Reported by", value=report["reported_by"],                   inline=True)
        if report["notes"]:
            embed.add_field(name="Notes",  value=report["notes"],                          inline=False)
        embed.set_footer(text=f"Via Overlay  •  {datetime.now().strftime('%H:%M:%S')}")
        await channel.send(embed=embed)
    except Exception as e:
        print(f"[OverlayAPI] Intel Discord mirror failed: {e}")


# ---------------------------------------------------------------------------
# DB helpers (read from the same SQLite file as MissionCog)
# ---------------------------------------------------------------------------

def _db_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def _db_fetchone(db_path: str, sql: str, params=()) -> Optional[sqlite3.Row]:
    with _db_connect(db_path) as conn:
        return conn.execute(sql, params).fetchone()

def _db_execute(db_path: str, sql: str, params=()):
    with _db_connect(db_path) as conn:
        conn.execute(sql, params)

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _fetch_missions(db_path: str, guild: Optional[discord.Guild], status: Optional[str]) -> list[MissionOut]:
    if status:
        rows = _db_connect(db_path).execute(
            "SELECT * FROM missions WHERE status=? ORDER BY id DESC", (status,)
        ).fetchall()
    else:
        rows = _db_connect(db_path).execute(
            "SELECT * FROM missions WHERE status != 'cancelled' ORDER BY id DESC"
        ).fetchall()
    return [_row_to_mission(r) for r in rows]

def _row_to_mission(row: sqlite3.Row) -> MissionOut:
    return MissionOut(
        id=row["id"],
        title=row["title"],
        description=row["description"] or "",
        reward=row["reward"] or "",
        status=row["status"],
        created_by=str(row["created_by"]),
        assigned_to=str(row["assigned_to"] or ""),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )

def _get_first_guild(bot: commands.Bot) -> Optional[discord.Guild]:
    return next(iter(bot.guilds), None)


# ---------------------------------------------------------------------------
# Cog — manages the API server lifecycle
# ---------------------------------------------------------------------------

class OverlayApiCog(commands.Cog, name="OverlayAPI"):
    """Runs a FastAPI server in a background thread for the C# overlay."""

    def __init__(self, bot: commands.Bot, db_path: str):
        self.bot      = bot
        self.db_path  = db_path
        self._server  = None
        self._thread  = None

    @commands.Cog.listener()
    async def on_ready(self):
        if not _FASTAPI_OK or not _JOSE_OK:
            print("[OverlayAPI] Missing dependencies — API server not started.")
            return
        if self._thread and self._thread.is_alive():
            return   # already running

        app = build_api(self.bot, self.db_path)
        config = uvicorn.Config(app, host="0.0.0.0", port=API_PORT,
                                log_level="warning", loop="asyncio")
        self._server = uvicorn.Server(config)

        self._thread = threading.Thread(
            target=self._server.run, daemon=True, name="overlay-api"
        )
        self._thread.start()
        print(f"[OverlayAPI] Server running on port {API_PORT}")

    def cog_unload(self):
        if self._server:
            self._server.should_exit = True

    # ----------------------------------------------------------------
    # Discord command: /overlay pair
    # Generates a one-time 8-char code the user pastes into the C# app
    # ----------------------------------------------------------------
    @app_commands.command(
        name="overlay_pair",
        description="Generate a pairing code to link the ARC Overlay desktop app to your account.",
    )
    async def overlay_pair(self, interaction: discord.Interaction):
        code       = secrets.token_hex(4).upper()   # e.g. "A3F2B819"
        expires_at = time.time() + 300               # 5 minutes

        _pair_codes[code] = {
            "discord_user_id": interaction.user.id,
            "expires_at":      expires_at,
        }

        embed = discord.Embed(
            title="🛸  ARC Overlay — Pairing Code",
            colour=discord.Colour.from_rgb(0, 180, 212),
        )
        embed.add_field(name="Your Code", value=f"```{code}```", inline=False)
        embed.add_field(
            name="Instructions",
            value=(
                "1. Open the **ARC Overlay** app on your PC\n"
                "2. On first launch, paste this code in the **Pair Code** field\n"
                f"3. API URL: `{os.getenv('RAILWAY_PUBLIC_DOMAIN', 'https://your-bot.up.railway.app')}`\n"
                "⏱️ This code expires in **5 minutes**."
            ),
            inline=False,
        )
        embed.set_footer(text="Code is single-use and tied to your Discord account.")
        await interaction.response.send_message(embed=embed, ephemeral=True)


# ---------------------------------------------------------------------------
# Required by discord.py cog loader — bot.py auto-loads this file
# ---------------------------------------------------------------------------

async def setup(bot: commands.Bot):
    from pathlib import Path
    db_path = str(Path(os.getenv("MISSION_DB_PATH", "/data/missions.db")))
    await bot.add_cog(OverlayApiCog(bot, db_path))
    print("[OverlayApiCog] Cog registered.")
