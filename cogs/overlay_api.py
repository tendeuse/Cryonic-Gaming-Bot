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
import httpx

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

_raw_secret = os.getenv("OVERLAY_JWT_SECRET")
if not _raw_secret:
    print("[OverlayAPI] ⚠️  OVERLAY_JWT_SECRET not set in environment!")
    print("[OverlayAPI]    Tokens will be invalidated on every bot restart.")
    print("[OverlayAPI]    Set OVERLAY_JWT_SECRET in Railway environment variables.")
    _raw_secret = secrets.token_hex(32)
JWT_SECRET = _raw_secret
# Railway injects $PORT — must use it or nginx 502s.
# Read at startup so Railway has time to inject it before on_ready fires.
def _get_api_port() -> int:
    raw = os.getenv("PORT") or os.getenv("OVERLAY_API_PORT") or "8080"
    port = int(raw)
    print(f"[OverlayAPI] Will bind on port {port} ($PORT={os.getenv('PORT', 'not set')})")
    return port
TOKEN_TTL_H  = int(os.getenv("OVERLAY_TOKEN_TTL_H", "720"))   # 30 days
ALGORITHM    = "HS256"

# EVE SSO OAuth2 config
# Register your app at https://developers.eveonline.com
# Callback URL must be set to: https://<your-railway-domain>/overlay/api/v1/eve/callback
EVE_CLIENT_ID     = os.getenv("EVE_CLIENT_ID", "")
EVE_CLIENT_SECRET = os.getenv("EVE_CLIENT_SECRET", "")
EVE_CALLBACK_URL  = os.getenv("EVE_CALLBACK_URL", "")
EVE_SCOPES        = "esi-characters.read_standings.v1 esi-location.read_location.v1 esi-location.read_ship_type.v1"
EVE_SSO_AUTH_URL  = "https://login.eveonline.com/v2/oauth/authorize"
EVE_SSO_TOKEN_URL = "https://login.eveonline.com/v2/oauth/token"
EVE_SSO_VERIFY_URL= "https://esi.evetech.net/verify/"

# In-memory stores (reset on restart — acceptable for ephemeral data)
_pair_codes: dict[str, dict] = {}   # code → {discord_user_id, expires_at}
_intel_store: list[dict]     = []   # recent intel reports (last 50)
_MAX_INTEL   = 50
# EVE OAuth2 state → discord_user_id (expires after 10 min)
_eve_oauth_states: dict[str, dict] = {}


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
    # EVE SSO OAuth2 — /eve/link  /eve/callback  /eve/status  /eve/unlink
    # ------------------------------------------------------------------

    @app.get("/overlay/api/v1/eve/link")
    async def eve_link(user_id: int = Depends(get_current_user)):
        """Returns the EVE SSO authorization URL for this user."""
        if not EVE_CLIENT_ID or not EVE_CALLBACK_URL:
            raise HTTPException(status_code=503,
                detail="EVE_CLIENT_ID / EVE_CALLBACK_URL not configured on server")
        state = secrets.token_hex(16)
        _eve_oauth_states[state] = {
            "discord_user_id": user_id,
            "expires_at":      time.time() + 600
        }
        from urllib.parse import urlencode
        params = {
            "response_type": "code",
            "client_id":     EVE_CLIENT_ID,
            "redirect_uri":  EVE_CALLBACK_URL,
            "scope":         EVE_SCOPES,
            "state":         state,
        }
        url = f"{EVE_SSO_AUTH_URL}?{urlencode(params)}"
        return {"auth_url": url}

    @app.get("/overlay/api/v1/eve/callback")
    async def eve_callback(code: str, state: str):
        """EVE SSO redirects here after the player authorises. No auth header needed.""""
        # Validate state
        entry = _eve_oauth_states.pop(state, None)
        if entry is None or time.time() > entry["expires_at"]:
            raise HTTPException(status_code=400, detail="Invalid or expired OAuth state")
        discord_user_id = entry["discord_user_id"]

        # Exchange code for tokens
        import base64
        creds = base64.b64encode(f"{EVE_CLIENT_ID}:{EVE_CLIENT_SECRET}".encode()).decode()
        async with httpx.AsyncClient() as client:
            r = await client.post(EVE_SSO_TOKEN_URL,
                headers={"Authorization": f"Basic {creds}",
                         "Content-Type": "application/x-www-form-urlencoded"},
                data={"grant_type":   "authorization_code",
                      "code":         code,
                      "redirect_uri": EVE_CALLBACK_URL}
            )
            if r.status_code != 200:
                raise HTTPException(status_code=502,
                    detail=f"EVE token exchange failed: {r.status_code}")
            tokens = r.json()

            # Verify token → get character info
            v = await client.get(EVE_SSO_VERIFY_URL,
                headers={"Authorization": f"Bearer {tokens['access_token']}"})
            if v.status_code != 200:
                raise HTTPException(status_code=502, detail="EVE token verify failed")
            char = v.json()
            character_id   = char["CharacterID"]
            character_name = char["CharacterName"]

        _save_eve_token(db_path, discord_user_id, character_id, character_name,
                        tokens["access_token"], tokens["refresh_token"],
                        tokens.get("expires_in", 1200))

        # Return a friendly HTML page the browser shows after auth
        html = f"""<!DOCTYPE html>
<html><head><title>EVE Linked</title>
<style>body{{background:#0a1a2f;color:#ccd6f6;font-family:Consolas;
  display:flex;align-items:center;justify-content:center;height:100vh;margin:0}}
.box{{text-align:center;border:1px solid #1e3148;padding:40px;border-radius:8px}}
h1{{color:#00b4d4}}p{{color:#8a99aa}}</style></head>
<body><div class="box">
  <h1>✅ EVE Character Linked</h1>
  <p><strong>{character_name}</strong> is now linked to your Discord account.</p>
  <p>You can close this window and return to the overlay.</p>
</div></body></html>"""
        from fastapi.responses import HTMLResponse
        return HTMLResponse(html)

    @app.get("/overlay/api/v1/eve/status")
    async def eve_status(user_id: int = Depends(get_current_user)):
        """Returns linked character info, or null if not linked.""""
        row = _get_eve_token(db_path, user_id)
        if row is None:
            return {"linked": False, "character_id": None, "character_name": None}
        return {"linked": True,
                "character_id":   row["character_id"],
                "character_name": row["character_name"]}

    @app.get("/overlay/api/v1/eve/unlink")
    async def eve_unlink(user_id: int = Depends(get_current_user)):
        _ensure_eve_tokens_table(db_path)
        with _db_connect(db_path) as conn:
            conn.execute("DELETE FROM eve_tokens WHERE discord_user_id=?", (user_id,))
        return {"ok": True}

    # ------------------------------------------------------------------
    # Health check — no auth, always reachable
    # Returns: bot status, dependency status, uptime
    # ------------------------------------------------------------------
    @app.get("/overlay/api/v1/health")
    async def health():
        return {
            "status":    "ok",
            "bot_ready": bot.is_ready(),
            "guilds":    len(bot.guilds),
            "fastapi":   _FASTAPI_OK,
            "jose":      _JOSE_OK,
            "jwt_secret_set": bool(os.getenv("OVERLAY_JWT_SECRET")),
            "uptime":    time.time(),
        }

    # ------------------------------------------------------------------
    # Character (stub — real ESI integration requires EVE SSO OAuth2)
    # The overlay will show "—" until ESI is implemented.
    # ------------------------------------------------------------------
    @app.get("/overlay/api/v1/character")
    async def get_character(user_id: int = Depends(get_current_user)):
        access_token = await _get_valid_access_token(db_path, user_id)
        if access_token is None:
            return None   # not linked → overlay shows "—"
        row = _get_eve_token(db_path, user_id)
        char_id = row["character_id"]
        try:
            async with httpx.AsyncClient(timeout=8) as client:
                # Location
                loc_r = await client.get(
                    f"https://esi.evetech.net/latest/characters/{char_id}/location/",
                    headers={"Authorization": f"Bearer {access_token}"}
                )
                # Ship
                ship_r = await client.get(
                    f"https://esi.evetech.net/latest/characters/{char_id}/ship/",
                    headers={"Authorization": f"Bearer {access_token}"}
                )
                # Public info (corporation, security status)
                pub_r = await client.get(
                    f"https://esi.evetech.net/latest/characters/{char_id}/"
                )

                solar_system_id = loc_r.json().get("solar_system_id") if loc_r.is_success else None
                ship_type_id    = ship_r.json().get("ship_type_id")   if ship_r.is_success else None
                ship_name       = ship_r.json().get("ship_name", "")  if ship_r.is_success else ""
                pub             = pub_r.json() if pub_r.is_success else {}

                # Resolve solar system name
                system_name = ""
                if solar_system_id:
                    sys_r = await client.get(
                        f"https://esi.evetech.net/latest/universe/systems/{solar_system_id}/"
                    )
                    system_name = sys_r.json().get("name", "") if sys_r.is_success else ""

                # Resolve ship type name
                ship_type_name = ""
                if ship_type_id:
                    type_r = await client.get(
                        f"https://esi.evetech.net/latest/universe/types/{ship_type_id}/"
                    )
                    ship_type_name = type_r.json().get("name", "") if type_r.is_success else ""

                sec  = round(pub.get("security_status", 0.0), 1)
                sec_colour = (
                    "#2ECC71" if sec >= 0.5 else
                    "#F39C12" if sec >= 0.0 else
                    "#E74C3C"
                )
                corp_id = pub.get("corporation_id")
                corp_name = ""
                if corp_id:
                    corp_r = await client.get(
                        f"https://esi.evetech.net/latest/corporations/{corp_id}/"
                    )
                    corp_name = corp_r.json().get("name", "") if corp_r.is_success else ""

                return {
                    "character_name":  row["character_name"],
                    "character_id":    char_id,
                    "corporation":     corp_name,
                    "ship_type":       ship_type_name or ship_name,
                    "solar_system":    system_name,
                    "security_status": sec,
                    "security_colour": sec_colour,
                }
        except Exception as e:
            print(f"[ESI] get_character error: {e}")
            return None

    # ------------------------------------------------------------------
    # ESI — Faction Standings
    # Fetches standings from the EVE ESI public API.
    # Requires the user to have linked their EVE character via ESI OAuth2.
    # Until OAuth2 is implemented, returns an empty list so the overlay
    # falls back to manual standing input gracefully.
    # ------------------------------------------------------------------
    @app.get("/overlay/api/v1/standings")
    async def get_standings(user_id: int = Depends(get_current_user)):
        """
        Returns a list of faction standings for the authenticated pilot.
        Format:
          [
            { "faction_id": 500001, "faction_name": "Caldari State",
              "standing": 3.72, "modified": false },
            ...
          ]
        Requires ESI OAuth2 scope: esi-characters.read_standings.v1
        Falls back to [] until OAuth2 is wired up.
        """
        # ── ESI faction ID → display name map ───────────────────────
        FACTION_NAMES = {
            500001: "Caldari State",
            500002: "Minmatar Republic",
            500003: "Amarr Empire",
            500004: "Gallente Federation",
            500005: "Jove Empire",
            500010: "CONCORD Assembly",
            500011: "Ammatar Mandate",
            500012: "Khanid Kingdom",
            500013: "The Syndicate",
            500014: "Guristas Pirates",
            500015: "Angel Cartel",
            500016: "Blood Raider Covenant",
            500017: "The Servant Sisters of EVE",
            500018: "The Society of Conscious Thought",
            500019: "Mordu's Legion Command",
            500020: "Sansha's Nation",
            500021: "Serpentis",
            500024: "Outer Ring Excavations",  # ORE
            500026: "EDENCOM",
            500027: "Triglavian Collective",
        }

        # TODO: Replace with real ESI call using stored OAuth2 token for user_id
        # Example ESI call (once OAuth2 is implemented):
        #
        # async with httpx.AsyncClient() as client:
        #     r = await client.get(
        #         f"https://esi.evetech.net/latest/characters/{character_id}/standings/",
        #         headers={"Authorization": f"Bearer {access_token}"}
        #     )
        #     data = r.json()
        #     # data is a list of {"from_id": int, "from_type": str, "standing": float}
        #     factions = [
        #         {
        #             "faction_id":   e["from_id"],
        #             "faction_name": FACTION_NAMES.get(e["from_id"], str(e["from_id"])),
        #             "standing":     round(e["standing"], 2),
        #             "modified":     False,
        #         }
        #         for e in data if e["from_type"] == "faction"
        #     ]
        #     return factions

        access_token = await _get_valid_access_token(db_path, user_id)
        if access_token is None:
            return []   # not linked — overlay falls back to manual input

        row = _get_eve_token(db_path, user_id)
        char_id = row["character_id"]
        try:
            async with httpx.AsyncClient(timeout=8) as client:
                r = await client.get(
                    f"https://esi.evetech.net/latest/characters/{char_id}/standings/",
                    headers={"Authorization": f"Bearer {access_token}"}
                )
                if not r.is_success:
                    print(f"[ESI] standings error: {r.status_code}")
                    return []
                return [
                    {
                        "faction_id":   e["from_id"],
                        "faction_name": FACTION_NAMES.get(e["from_id"], str(e["from_id"])),
                        "standing":     round(e["standing"], 2),
                        "modified":     False,
                    }
                    for e in r.json() if e.get("from_type") == "faction"
                ]
        except Exception as e:
            print(f"[ESI] get_standings error: {e}")
            return []

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

def _ensure_eve_tokens_table(db_path: str):
    """Create eve_tokens table if it doesn't exist (migration-safe)."""
    with _db_connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS eve_tokens(
                discord_user_id INTEGER PRIMARY KEY,
                character_id    INTEGER NOT NULL,
                character_name  TEXT    NOT NULL,
                access_token    TEXT    NOT NULL,
                refresh_token   TEXT    NOT NULL,
                expires_at      REAL    NOT NULL
            )
        """)

def _save_eve_token(db_path: str, discord_user_id: int, character_id: int,
                    character_name: str, access_token: str,
                    refresh_token: str, expires_in: int):
    _ensure_eve_tokens_table(db_path)
    import time as _time
    with _db_connect(db_path) as conn:
        conn.execute("""
            INSERT INTO eve_tokens
                (discord_user_id, character_id, character_name,
                 access_token, refresh_token, expires_at)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(discord_user_id) DO UPDATE SET
                character_id=excluded.character_id,
                character_name=excluded.character_name,
                access_token=excluded.access_token,
                refresh_token=excluded.refresh_token,
                expires_at=excluded.expires_at
        """, (discord_user_id, character_id, character_name,
                access_token, refresh_token, _time.time() + expires_in))

def _get_eve_token(db_path: str, discord_user_id: int) -> Optional[sqlite3.Row]:
    _ensure_eve_tokens_table(db_path)
    return _db_fetchone(db_path,
        "SELECT * FROM eve_tokens WHERE discord_user_id=?", (discord_user_id,))

async def _refresh_eve_token(db_path: str, row: sqlite3.Row) -> Optional[str]:
    """Refresh an expired EVE access token. Returns new access_token or None on failure."""
    try:
        import base64
        creds = base64.b64encode(f"{EVE_CLIENT_ID}:{EVE_CLIENT_SECRET}".encode()).decode()
        async with httpx.AsyncClient() as client:
            r = await client.post(EVE_SSO_TOKEN_URL,
                headers={"Authorization": f"Basic {creds}",
                         "Content-Type": "application/x-www-form-urlencoded"},
                data={"grant_type": "refresh_token", "refresh_token": row["refresh_token"]}
            )
            if r.status_code != 200:
                print(f"[ESI] Token refresh failed: {r.status_code} {r.text}")
                return None
            data = r.json()
            _save_eve_token(db_path, row["discord_user_id"],
                            row["character_id"], row["character_name"],
                            data["access_token"], data.get("refresh_token", row["refresh_token"]),
                            data.get("expires_in", 1200))
            return data["access_token"]
    except Exception as e:
        print(f"[ESI] Token refresh exception: {e}")
        return None

async def _get_valid_access_token(db_path: str, discord_user_id: int) -> Optional[str]:
    """Returns a valid access token, refreshing if needed. None if not linked."""    import time as _time
    row = _get_eve_token(db_path, discord_user_id)
    if row is None:
        return None
    if _time.time() > row["expires_at"] - 60:   # refresh 60s early
        return await _refresh_eve_token(db_path, row)
    return row["access_token"]

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
        port = _get_api_port()
        config = uvicorn.Config(
            app,
            host="0.0.0.0",
            port=port,
            log_level="info",
            loop="none",
            lifespan="off",
        )
        self._server = uvicorn.Server(config)

        def _run_server():
            """Run uvicorn in its own event loop — required when the discord.py
            bot already owns the main event loop on the main thread."""
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self._server.serve())
            finally:
                loop.close()

        self._thread = threading.Thread(
            target=_run_server, daemon=True, name="overlay-api"
        )
        self._thread.start()
        print(f"[OverlayAPI] FastAPI server starting on 0.0.0.0:{port}")

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
        # Defer immediately — guarantees Discord gets an ACK within 3s
        # regardless of what happens next. Prevents "Unknown Integration".
        await interaction.response.defer(ephemeral=True)
        try:
            code       = secrets.token_hex(4).upper()
            expires_at = time.time() + 300

            _pair_codes[code] = {
                "discord_user_id": interaction.user.id,
                "expires_at":      expires_at,
            }

            api_url = "https://cryonic-gaming-bot-production.up.railway.app"
            instructions = (
                "1. Ouvrez l'**ARC Overlay** sur votre PC\n"
                "2. Cliquez sur **⚙** dans la barre de titre → **Re-pair**\n"
                f"3. API URL : `{api_url}`\n"
                "Ce code expire dans **5 minutes**."
            )

            embed = discord.Embed(
                title="ARC Overlay — Pairing Code",
                colour=discord.Colour.from_rgb(0, 180, 212),
            )
            embed.add_field(name="Votre code", value=f"```{code}```", inline=False)
            embed.add_field(name="Instructions", value=instructions, inline=False)
            embed.set_footer(text="Code à usage unique, lié à votre compte Discord.")
            await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            print(f"[OverlayAPI] /overlay_pair error: {e}", flush=True)
            try:
                await interaction.followup.send(
                    f"⚠️ Erreur lors de la génération du code : {e}",
                    ephemeral=True
                )
            except Exception as e2:
                print(f"[OverlayAPI] followup also failed: {e2}", flush=True)

    @app_commands.command(
        name="eve_link",
        description="Link your EVE Online character to the ARC Overlay for live ESI data.",
    )
    async def eve_link_cmd(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            if not EVE_CLIENT_ID or not EVE_CALLBACK_URL:
                await interaction.followup.send(
                    "⚠️ EVE SSO n'est pas configuré sur le serveur.\n"
                    "L'admin doit définir `EVE_CLIENT_ID`, `EVE_CLIENT_SECRET` et `EVE_CALLBACK_URL` "
                    "dans les variables Railway.",
                    ephemeral=True
                )
                return

            # Generate auth URL directly (reuse the /eve/link logic)
            state = secrets.token_hex(16)
            _eve_oauth_states[state] = {
                "discord_user_id": interaction.user.id,
                "expires_at":      time.time() + 600
            }
            from urllib.parse import urlencode
            params = {
                "response_type": "code",
                "client_id":     EVE_CLIENT_ID,
                "redirect_uri":  EVE_CALLBACK_URL,
                "scope":         EVE_SCOPES,
                "state":         state,
            }
            auth_url = f"{EVE_SSO_AUTH_URL}?{urlencode(params)}"

            embed = discord.Embed(
                title="🔗 Lier votre personnage EVE",
                colour=discord.Colour.from_rgb(0, 180, 212),
                description=(
                    "Cliquez sur le bouton ci-dessous pour autoriser l'overlay à lire\n"
                    "votre position, vaisseau et standings de faction via l'API ESI.\n\n"
                    "⏱️ Ce lien expire dans **10 minutes**."
                )
            )
            embed.add_field(
                name="Autorisations demandées",
                value="• Localisation · Vaisseau · Standings de faction",
                inline=False
            )
            view = discord.ui.View()
            view.add_item(discord.ui.Button(
                label="Autoriser sur EVE Online",
                url=auth_url,
                style=discord.ButtonStyle.link,
                emoji="🚀"
            ))
            await interaction.followup.send(embed=embed, view=view, ephemeral=True)

        except Exception as e:
            print(f"[OverlayAPI] /eve_link error: {e}", flush=True)
            try:
                await interaction.followup.send(f"⚠️ Erreur: {e}", ephemeral=True)
            except Exception:
                pass

# ---------------------------------------------------------------------------
# Required by discord.py cog loader — bot.py auto-loads this file
# ---------------------------------------------------------------------------

async def setup(bot: commands.Bot):
    from pathlib import Path
    db_path = str(Path(os.getenv("MISSION_DB_PATH", "/data/missions.db")))
    await bot.add_cog(OverlayApiCog(bot, db_path))
    print("[OverlayApiCog] Cog registered.")
