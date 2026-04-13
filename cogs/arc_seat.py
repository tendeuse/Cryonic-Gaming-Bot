# cogs/arc_seat.py
#
# ARC-SEAT  —  Autonomous EVE Intelligence & Member Tracking
# ===========================================================
#
# A self-contained SeAT equivalent built as a discord.py cog.
# Completely independent from overlay_api.py except for one coupling point:
# the OAuth callback route is registered on the overlay's FastAPI server
# (the only publicly reachable HTTP endpoint on a single-port Railway service).
# All data, tokens, and scopes are entirely separate.
#
# AUTH FLOW
# ---------
# Members authenticate via /seat_add_char — this generates an EVE SSO link
# and stores the token in arc_seat's own SQLite database (/data/arc_seat.db).
# This is completely independent of the overlay's /eve_link and missions.db.
# Multiple characters per Discord account are fully supported.
#
# DATA STORAGE
# ------------
# /data/arc_seat.db   — SQLite, seat_tokens table (composite PK per character)
# /data/arc_seat.json — Character cache, spy flags, skill snapshots
#
# ONLY COUPLING WITH overlay_api.py
# ----------------------------------
# arc_seat registers one GET route (/seat/auth/callback) on the overlay's
# FastAPI app. This is documented and isolated to _register_callback_route().
# If overlay_api is not loaded, a warning is logged and auth is unavailable
# until the bot restarts — all other SEAT features continue to work.
#
# FEATURES
# --------
# 1.  /seat_add_char  — EVE SSO auth, stores token in own DB
# 2.  /seat_sync      — import characters already in own DB (e.g. after restart)
# 3.  Full ESI pull every 6 h per character
#     • Corp membership, corp history, character info  (public)
#     • Skills + skill queue, wallet, assets, contacts, standings,
#       clones, implants, industry jobs  (authenticated)
#     • Killmails via zkillboard public API  (no auth)
# 4.  Automated spy-detection scoring on every ESI pull
# 5.  Corp sync loop every 1 h
#     • Corp check fails → ARC Security auto-removed
#     • ARC Subsidized + rank roles → flagged for manual review only
# 6.  Skill snapshot every 24 h  (SP progression history)
# 7.  Forum watch-list — one thread per flagged member
# 8.  Migration from ign_registry.json on first run  (names/IDs only, no tokens)
#
# RAILWAY ENV VARS
# ----------------
#   EVE_CLIENT_ID      — EVE developer app client ID
#   EVE_CLIENT_SECRET  — EVE developer app client secret
#   SEAT_CALLBACK_URL  — full callback URL, e.g.
#                        https://your-app.up.railway.app/seat/auth/callback
#   EVE_CORP_ID        — integer ARC corporation ID
#
# COMMANDS
# --------
#   /seat_add_char      — link an EVE character (repeatable for multiple accounts)
#   /seat_sync          — re-import characters from own DB (after restart)
#   /seat_status        — view your profile
#   /seat_whois         — [admin] full intel profile for a member
#   /seat_skills        — [admin] skill progression
#   /seat_scan          — [admin] force spy scan
#   /seat_verify_all    — [admin] force corp sync for all members
#   /seat_hostile_corp  — [admin] add/remove hostile corp
#   /seat_unlink        — remove a character from your profile

import asyncio
import base64
import io
import json
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks

# ============================================================
# PATHS
# ============================================================
PERSIST_ROOT  = Path(os.getenv("PERSIST_ROOT", "/data"))
DATA_FILE     = PERSIST_ROOT / "arc_seat.json"
IGN_DATA_FILE = PERSIST_ROOT / "ign_registry.json"   # read-only migration source

# ARC-SEAT's own SQLite database — completely separate from missions.db
SEAT_DB_PATH  = str(PERSIST_ROOT / "arc_seat.db")

# ============================================================
# ENV VARS
# ============================================================
EVE_CLIENT_ID     = os.getenv("EVE_CLIENT_ID",     "")
EVE_CLIENT_SECRET = os.getenv("EVE_CLIENT_SECRET", "")
SEAT_CALLBACK_URL = os.getenv("SEAT_CALLBACK_URL", "")
# EVE_CORP_IDS — comma-separated list of ALL approved ARC corporation IDs
# e.g. "98743131,98791781"  (main corp first, subsidiaries after)
# Falls back to legacy EVE_CORP_ID if EVE_CORP_IDS is not set.
def _parse_corp_ids(raw: str) -> List[int]:
    ids: List[int] = []
    for part in raw.replace(" ", "").split(","):
        try:
            cid = int(part)
            if cid > 0:
                ids.append(cid)
        except ValueError:
            pass
    return ids

_raw_corp_ids = os.getenv("EVE_CORP_IDS", "") or os.getenv("EVE_CORP_ID", "")
ARC_APPROVED_CORP_IDS: List[int] = _parse_corp_ids(_raw_corp_ids)
# Keep a single primary ID for legacy/spy-engine usage (first in the list)
ARC_CORP_ID_ENV: Optional[int] = ARC_APPROVED_CORP_IDS[0] if ARC_APPROVED_CORP_IDS else None

# ============================================================
# ESI / SSO ENDPOINTS
# ============================================================
ESI_BASE       = "https://esi.evetech.net/latest"
SSO_AUTH_URL   = "https://login.eveonline.com/v2/oauth/authorize"
SSO_TOKEN_URL  = "https://login.eveonline.com/v2/oauth/token"
ZKILL_BASE     = "https://zkillboard.com/api"

# ============================================================
# ESI SCOPES  — defined here, independent of overlay_api.py
# ============================================================
SEAT_SCOPES = " ".join([
    "esi-skills.read_skills.v1",
    "esi-skills.read_skillqueue.v1",
    "esi-wallet.read_character_wallet.v1",
    "esi-assets.read_assets.v1",
    "esi-characters.read_contacts.v1",
    "esi-characters.read_standings.v1",
    "esi-location.read_location.v1",
    "esi-location.read_ship_type.v1",
    "esi-clones.read_clones.v1",
    "esi-clones.read_implants.v1",
    "esi-killmails.read_killmails.v1",
    "esi-industry.read_character_jobs.v1",
    "esi-characters.read_fatigue.v1",
    "esi-characters.read_corporation_roles.v1",
])

# OAuth state TTL
OAUTH_STATE_TTL = 600  # 10 min

# ============================================================
# DISCORD CONFIG
# ============================================================
WATCH_LIST_CHANNEL_ID = 1461162252173316249   # existing forum channel — never recreated
ARC_SECURITY_ROLE     = "ARC Security"
ARC_SUBSIDIZED_ROLE   = "ARC Subsidized"
HIERARCHY_LOG_CH      = "arc-hierarchy-log"

# Roles that may press role-removal prompt buttons
PROMPT_AUTHORIZED_ROLES: Set[str] = {
    "ARC Security Administration Council",
    "ARC Security Corporation Leader",
}

# Rank roles flagged for manual review (NOT auto-removed)
ARC_RANK_ROLES: Set[str] = {
    "ARC Petty Officer",
    "ARC Lieutenant",
    "ARC Commander",
    "ARC General",
    "ARC Security Administration Council",
    "ARC Security Corporation Leader",
}

# ============================================================
# SPY DETECTION THRESHOLDS
# ============================================================
RISK_HIGH_THRESHOLD   = 6
RISK_MEDIUM_THRESHOLD = 3

# ============================================================
# BACKGROUND TASK INTERVALS  (seconds)
# ============================================================
TOKEN_REFRESH_INTERVAL  = 900    # 15 min
CORP_SYNC_INTERVAL      = 3600   # 1 h
ESI_PULL_INTERVAL       = 21600  # 6 h
SKILL_SNAPSHOT_INTERVAL = 86400  # 24 h

# ============================================================
# ESI SKILL IDs  (spy-relevant)
# ============================================================
SPY_SKILL_IDS: Dict[int, str] = {
    11579: "Cloaking",
    12093: "Covert Ops",
    3412:  "Astrometrics",
    21718: "Hacking",
    25338: "Archaeology",
    3186:  "Astrometric Rangefinding",
    3185:  "Astrometric Acquisition",
}


# ============================================================
# DATA HELPERS
# ============================================================

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_ts() -> int:
    return int(time.time())


def _default_data() -> Dict[str, Any]:
    return {
        "members":        {},   # str(discord_id) → member record
        "config": {
            "arc_corp_id":            ARC_CORP_ID_ENV,          # primary corp (spy engine)
            "arc_approved_corp_ids":  ARC_APPROVED_CORP_IDS,    # all approved corps (✅ check)
            "hostile_corps":          [],   # List[int]  corp IDs
            "hostile_alliances":      [],   # List[int]  alliance IDs
        },
        "oauth_states":   {},   # state_token → {discord_id, is_alt, expires}
        "skill_snapshots": {},  # str(discord_id) → {str(char_id) → [snapshots]}
    }


def _is_approved_corp(corp_id: Optional[int], cfg: Dict[str, Any]) -> bool:
    """
    Returns True if corp_id is in the list of ARC-approved corporation IDs.
    Approved corps are loaded from EVE_CORP_IDS env var (comma-separated).
    Returns False if corp_id is None or the approved list is empty.
    """
    if not corp_id:
        return False
    approved: List[int] = cfg.get("arc_approved_corp_ids") or []
    return corp_id in approved


def _default_member(discord_id: int) -> Dict[str, Any]:
    return {
        "discord_id":             discord_id,
        "verified":               False,
        "registered_at":          None,
        "last_corp_check":        None,
        "last_esi_pull":          None,
        "discord_roles_synced_at": None,
        "watch_list_thread_id":   None,
        "characters":             [],
        "flags":                  [],
        "risk_score":             0,
        "risk_level":             "UNKNOWN",
        "migrated_from_ign":      False,
    }


def _default_character(
    character_id:   int,
    character_name: str,
    is_main:        bool = False,
) -> Dict[str, Any]:
    return {
        "character_id":    character_id,
        "character_name":  character_name,
        "is_main":         is_main,
        "corporation_id":  None,
        "corporation_name": None,
        "alliance_id":     None,
        "alliance_name":   None,
        "security_status": 0.0,
        "birthday":        None,
        "total_sp":        0,
        "in_arc_corp":     False,
        "last_esi_pull":   None,
        "has_tokens":      False,   # True when overlay DB has a valid token for this user
        "cache": {
            "skills":         None,
            "skill_queue":    None,
            "wallet_balance": None,
            "wallet_journal": None,
            "assets":         None,
            "contacts":       None,
            "standings":      None,
            "corp_history":   None,
            "location":       None,
            "clones":         None,
            "implants":       None,
            "killmails":      None,
            "industry_jobs":  None,
        },
    }


# ============================================================
# PERSISTENCE
# ============================================================

_file_lock: Optional[asyncio.Lock] = None


def _get_file_lock() -> asyncio.Lock:
    global _file_lock
    if _file_lock is None:
        _file_lock = asyncio.Lock()
    return _file_lock


def _atomic_write(data: Dict[str, Any]) -> None:
    PERSIST_ROOT.mkdir(parents=True, exist_ok=True)
    tmp = DATA_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    tmp.replace(DATA_FILE)


async def load_seat_data() -> Dict[str, Any]:
    async with _get_file_lock():
        if not DATA_FILE.exists():
            return _default_data()
        try:
            with open(DATA_FILE, encoding="utf-8") as f:
                raw = f.read().strip()
            if not raw:
                return _default_data()
            data = json.loads(raw)
            if not isinstance(data, dict):
                return _default_data()
            # Back-fill any missing top-level keys
            for k, v in _default_data().items():
                data.setdefault(k, v)
            return data
        except Exception as e:
            print(f"[ARC-SEAT] Data load error: {e} — starting fresh")
            return _default_data()


async def save_seat_data(data: Dict[str, Any]) -> None:
    async with _get_file_lock():
        _atomic_write(data)


# ============================================================
# ARC-SEAT TOKEN DATABASE
# ============================================================
# All tokens are stored in /data/arc_seat.db — completely separate
# from the overlay's missions.db.  The seat_tokens table uses a
# composite primary key so each Discord user can have unlimited
# EVE characters.

import sqlite3 as _sqlite3


def _seat_db_connect() -> _sqlite3.Connection:
    PERSIST_ROOT.mkdir(parents=True, exist_ok=True)
    conn = _sqlite3.connect(SEAT_DB_PATH)
    conn.row_factory = _sqlite3.Row
    return conn


def _seat_db_ensure() -> None:
    """Create the seat_tokens table if it doesn't exist (migration-safe)."""
    try:
        with _seat_db_connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS seat_tokens (
                    discord_user_id INTEGER NOT NULL,
                    character_id    INTEGER NOT NULL,
                    character_name  TEXT    NOT NULL,
                    access_token    TEXT    NOT NULL,
                    refresh_token   TEXT    NOT NULL,
                    expires_at      REAL    NOT NULL,
                    PRIMARY KEY (discord_user_id, character_id)
                )
            """)
    except Exception as e:
        print(f"[ARC-SEAT] DB init error: {e}")


def _seat_get_token(
    discord_user_id: int,
    character_id:    int,
) -> Optional[Dict[str, Any]]:
    """Read a specific character's token row. Returns dict or None."""
    try:
        _seat_db_ensure()
        with _seat_db_connect() as conn:
            row = conn.execute(
                "SELECT * FROM seat_tokens "
                "WHERE discord_user_id=? AND character_id=?",
                (discord_user_id, character_id),
            ).fetchone()
            return dict(row) if row else None
    except Exception as e:
        print(f"[ARC-SEAT] Token read error: {e}")
        return None


def _seat_get_all_tokens(discord_user_id: int) -> List[Dict[str, Any]]:
    """Return all token rows for a Discord user."""
    try:
        _seat_db_ensure()
        with _seat_db_connect() as conn:
            rows = conn.execute(
                "SELECT * FROM seat_tokens WHERE discord_user_id=?",
                (discord_user_id,),
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        print(f"[ARC-SEAT] Token read-all error: {e}")
        return []


def _seat_get_all_tokens_global() -> List[Dict[str, Any]]:
    """Return every token row in the DB (used by refresh loop)."""
    try:
        _seat_db_ensure()
        with _seat_db_connect() as conn:
            rows = conn.execute("SELECT * FROM seat_tokens").fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        print(f"[ARC-SEAT] Token read-global error: {e}")
        return []


def _seat_save_token(
    discord_user_id: int,
    character_id:    int,
    character_name:  str,
    access_token:    str,
    refresh_token:   str,
    expires_in:      int,
) -> None:
    """Upsert a character token into seat_tokens."""
    try:
        _seat_db_ensure()
        with _seat_db_connect() as conn:
            conn.execute("""
                INSERT INTO seat_tokens
                    (discord_user_id, character_id, character_name,
                     access_token, refresh_token, expires_at)
                VALUES (?,?,?,?,?,?)
                ON CONFLICT(discord_user_id, character_id) DO UPDATE SET
                    character_name=excluded.character_name,
                    access_token=excluded.access_token,
                    refresh_token=excluded.refresh_token,
                    expires_at=excluded.expires_at
            """, (
                discord_user_id, character_id, character_name,
                access_token, refresh_token,
                time.time() + expires_in,
            ))
    except Exception as e:
        print(f"[ARC-SEAT] Token save error: {e}")


def _seat_delete_token(discord_user_id: int, character_id: int) -> None:
    """Remove a specific character's token."""
    try:
        _seat_db_ensure()
        with _seat_db_connect() as conn:
            conn.execute(
                "DELETE FROM seat_tokens "
                "WHERE discord_user_id=? AND character_id=?",
                (discord_user_id, character_id),
            )
    except Exception as e:
        print(f"[ARC-SEAT] Token delete error: {e}")


# ============================================================
# ESI CLIENT
# ============================================================

class ESIClient:
    """
    Async ESI + EVE SSO client.
    Manages a single aiohttp session for the lifetime of the cog.
    All methods are safe to call concurrently.
    """

    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None

    async def _sess(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"User-Agent": "ARC-SEAT-Bot/1.0"}
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    # ── Token management ─────────────────────────────────────────────────────

    def _basic_auth_header(self) -> str:
        creds = base64.b64encode(
            f"{EVE_CLIENT_ID}:{EVE_CLIENT_SECRET}".encode()
        ).decode()
        return f"Basic {creds}"

    async def exchange_code(self, code: str) -> Optional[Dict[str, Any]]:
        """Exchange auth code → {access_token, refresh_token, expires_in}."""
        sess = await self._sess()
        try:
            async with sess.post(
                SSO_TOKEN_URL,
                headers={
                    "Authorization": self._basic_auth_header(),
                    "Content-Type":  "application/x-www-form-urlencoded",
                },
                data={
                    "grant_type":   "authorization_code",
                    "code":         code,
                    "redirect_uri": SEAT_CALLBACK_URL,
                },
                timeout=aiohttp.ClientTimeout(total=30),
            ) as r:
                return await r.json() if r.status == 200 else None
        except Exception as e:
            print(f"[ARC-SEAT] Code exchange error: {e}")
            return None

    async def refresh_token(
        self,
        discord_user_id: int,
        character_id:    int,
        character_name:  str,
        refresh_tok:     str,
    ) -> Optional[str]:
        """
        Refresh an EVE access token and write it back to arc_seat.db.
        Returns the new access_token or None on failure.
        """
        sess = await self._sess()
        try:
            async with sess.post(
                SSO_TOKEN_URL,
                headers={
                    "Authorization": self._basic_auth_header(),
                    "Content-Type":  "application/x-www-form-urlencoded",
                },
                data={
                    "grant_type":    "refresh_token",
                    "refresh_token": refresh_tok,
                },
                timeout=aiohttp.ClientTimeout(total=30),
            ) as r:
                if r.status != 200:
                    return None
                new_tokens = await r.json()

            access  = new_tokens.get("access_token")
            refresh = new_tokens.get("refresh_token", refresh_tok)
            expires = int(new_tokens.get("expires_in", 1200))

            if not access:
                return None

            _seat_save_token(
                discord_user_id, character_id, character_name,
                access, refresh, expires,
            )
            return access

        except Exception as e:
            print(f"[ARC-SEAT] Token refresh error: {e}")
            return None

    # ── ESI GET wrapper ───────────────────────────────────────────────────────

    async def get(
        self,
        path: str,
        access_token: Optional[str] = None,
        params: Optional[Dict] = None,
    ) -> Optional[Any]:
        sess = await self._sess()
        url  = f"{ESI_BASE}{path}"
        hdrs: Dict[str, str] = {}
        if access_token:
            hdrs["Authorization"] = f"Bearer {access_token}"
        try:
            async with sess.get(
                url,
                headers=hdrs,
                params=params or {},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as r:
                if r.status == 200:
                    return await r.json()
                if r.status == 304:
                    return None    # not modified — use cached value
                print(f"[ARC-SEAT] ESI {path} → HTTP {r.status}")
                return None
        except Exception as e:
            print(f"[ARC-SEAT] ESI GET {path} error: {e}")
            return None

    # ── zkillboard ────────────────────────────────────────────────────────────

    async def zkill_character(
        self,
        char_id: int,
        page:    int = 1,
    ) -> List[Dict[str, Any]]:
        sess = await self._sess()
        url  = f"{ZKILL_BASE}/kills/characterID/{char_id}/page/{page}/"
        try:
            async with sess.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status == 200:
                    data = await r.json()
                    return data if isinstance(data, list) else []
                return []
        except Exception:
            return []


# ============================================================
# SPY DETECTION ENGINE
# ============================================================

class SpyDetectionEngine:
    """
    Analyses ESI cache data and produces a list of flags + risk score.

    Flag structure:
        {
            "type":     str,   # machine-readable key
            "severity": str,   # HIGH | MEDIUM | LOW
            "title":    str,   # short human-readable summary
            "detail":   str,   # longer description
        }
    """

    SEVERITY_SCORES = {"HIGH": 4, "MEDIUM": 2, "LOW": 1}

    def __init__(
        self,
        hostile_corps:     List[int],
        hostile_alliances: List[int],
        arc_corp_id:       Optional[int],
    ) -> None:
        self._hostile_corps     = set(hostile_corps)
        self._hostile_alliances = set(hostile_alliances)
        self._arc_corp_id       = arc_corp_id

    # ── Public entry point ────────────────────────────────────────────────────

    def scan_member(
        self,
        member: Dict[str, Any],
    ) -> Tuple[List[Dict[str, Any]], int, str]:
        """
        Full scan of all characters belonging to a member.
        Returns (flags, total_score, risk_level).
        """
        all_flags: List[Dict[str, Any]] = []
        total_score = 0

        characters = member.get("characters", [])

        for char in characters:
            flags, score = self._scan_character(char, characters)
            all_flags.extend(flags)
            total_score += score

        risk_level = self._risk_level(total_score)
        return all_flags, total_score, risk_level

    # ── Per-character analysis ────────────────────────────────────────────────

    def _scan_character(
        self,
        character:   Dict[str, Any],
        all_chars:   List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], int]:
        flags: List[Dict[str, Any]] = []
        score = 0
        cache = character.get("cache", {})
        cname = character.get("character_name", "?")

        # ── 1. Corp history ───────────────────────────────────────────────────
        corp_history = cache.get("corp_history") or []
        if corp_history:
            f, s = self._flag_corp_history(cname, corp_history)
            flags.extend(f); score += s

        # ── 2. Alts in hostile corps ─────────────────────────────────────────
        f, s = self._flag_hostile_alts(cname, character, all_chars)
        flags.extend(f); score += s

        # ── 3. Character age + injected SP ───────────────────────────────────
        if character.get("birthday"):
            f, s = self._flag_character_age(cname, character)
            flags.extend(f); score += s

        # ── 4. Spy skill profile ─────────────────────────────────────────────
        if cache.get("skills"):
            f, s = self._flag_spy_skills(cname, cache["skills"])
            flags.extend(f); score += s

        # ── 5. Wallet anomalies ───────────────────────────────────────────────
        if cache.get("wallet_journal"):
            f, s = self._flag_wallet(cname, cache["wallet_journal"])
            flags.extend(f); score += s

        # ── 6. Suspicious contacts / standings ───────────────────────────────
        if cache.get("contacts"):
            f, s = self._flag_contacts(cname, cache["contacts"])
            flags.extend(f); score += s

        # ── 7. ARC member kills (zkillboard) ─────────────────────────────────
        if cache.get("killmails"):
            f, s = self._flag_killmails(cname, cache["killmails"])
            flags.extend(f); score += s

        # ── 8. Widespread assets ─────────────────────────────────────────────
        if cache.get("assets"):
            f, s = self._flag_assets(cname, cache["assets"])
            flags.extend(f); score += s

        return flags, score

    # ── Individual flag checks ────────────────────────────────────────────────

    def _flag_corp_history(
        self,
        cname:   str,
        history: List[Dict[str, Any]],
    ) -> Tuple[List, int]:
        flags: List[Dict] = []
        score = 0
        now   = datetime.now(timezone.utc)

        # Sort newest-first
        sorted_h = sorted(
            history,
            key=lambda x: x.get("start_date", ""),
            reverse=True,
        )

        # Hostile corp membership
        for entry in sorted_h:
            corp_id = entry.get("corporation_id")
            if corp_id and corp_id in self._hostile_corps:
                flags.append({
                    "type":     "HOSTILE_CORP_HISTORY",
                    "severity": "HIGH",
                    "title":    f"[{cname}] Previously in hostile corp",
                    "detail":   (
                        f"Corp ID {corp_id} on {entry.get('start_date', '?')[:10]}. "
                        "This corp is on the hostile list."
                    ),
                })
                score += 4

        # Corp hopping — 3+ corps in last 180 days
        recent: List[Dict] = []
        for entry in sorted_h:
            try:
                start = datetime.fromisoformat(
                    entry["start_date"].replace("Z", "+00:00")
                )
                if (now - start).days <= 180:
                    recent.append(entry)
            except Exception:
                pass

        if len(recent) >= 3:
            flags.append({
                "type":     "CORP_HOPPING",
                "severity": "HIGH",
                "title":    f"[{cname}] Corp-hopping — {len(recent)} corps in 6 months",
                "detail":   (
                    "Rapid corp changes are a common infiltration pattern. "
                    f"Corp IDs: {', '.join(str(e.get('corporation_id','?')) for e in recent[:5])}"
                ),
            })
            score += 3

        # Joined ARC within 30 days of leaving another corp
        if len(sorted_h) >= 2 and self._arc_corp_id:
            top    = sorted_h[0]
            second = sorted_h[1]
            if top.get("corporation_id") == self._arc_corp_id:
                try:
                    arc_join  = datetime.fromisoformat(
                        top["start_date"].replace("Z", "+00:00")
                    )
                    prev_start = datetime.fromisoformat(
                        second["start_date"].replace("Z", "+00:00")
                    )
                    gap = (arc_join - prev_start).days
                    if 0 < gap < 30:
                        flags.append({
                            "type":     "RAPID_ARC_JOIN",
                            "severity": "MEDIUM",
                            "title":    f"[{cname}] Joined ARC {gap} days after leaving previous corp",
                            "detail":   (
                                f"Previous corp ID: {second.get('corporation_id','?')}. "
                                "Very short gap before joining ARC."
                            ),
                        })
                        score += 2
                except Exception:
                    pass

        return flags, score

    def _flag_hostile_alts(
        self,
        cname:     str,
        character: Dict[str, Any],
        all_chars: List[Dict[str, Any]],
    ) -> Tuple[List, int]:
        flags: List[Dict] = []
        score = 0

        for alt in all_chars:
            if alt["character_id"] == character["character_id"]:
                continue
            corp_id     = alt.get("corporation_id")
            alliance_id = alt.get("alliance_id")
            alt_name    = alt.get("character_name", "?")

            if corp_id and corp_id in self._hostile_corps:
                flags.append({
                    "type":     "ALT_IN_HOSTILE_CORP",
                    "severity": "HIGH",
                    "title":    f"[{cname}] Alt '{alt_name}' is in a hostile corp",
                    "detail":   (
                        f"Alt corp: {alt.get('corporation_name', corp_id)}  "
                        f"(ID {corp_id}). Same account also holds ARC roles."
                    ),
                })
                score += 4

            if alliance_id and alliance_id in self._hostile_alliances:
                flags.append({
                    "type":     "ALT_IN_HOSTILE_ALLIANCE",
                    "severity": "HIGH",
                    "title":    f"[{cname}] Alt '{alt_name}' is in a hostile alliance",
                    "detail":   f"Alliance ID: {alliance_id}",
                })
                score += 4

        return flags, score

    def _flag_character_age(
        self,
        cname:     str,
        character: Dict[str, Any],
    ) -> Tuple[List, int]:
        flags: List[Dict] = []
        score = 0

        try:
            birthday = datetime.fromisoformat(
                character["birthday"].replace("Z", "+00:00")
            )
            age_days = (datetime.now(timezone.utc) - birthday).days
            total_sp = int(character.get("total_sp") or 0)

            if age_days < 90:
                flags.append({
                    "type":     "NEW_CHARACTER",
                    "severity": "HIGH",
                    "title":    f"[{cname}] Very new character — {age_days} days old",
                    "detail":   "Created less than 90 days ago. High-risk profile.",
                })
                score += 4
            elif age_days < 365:
                flags.append({
                    "type":     "YOUNG_CHARACTER",
                    "severity": "MEDIUM",
                    "title":    f"[{cname}] Young character — {age_days} days old",
                    "detail":   "Under 1 year old.",
                })
                score += 2

            # SP vs age — rough natural cap ~2000 SP/hr
            natural_cap = age_days * 24 * 2000
            if total_sp > 0 and total_sp > natural_cap and age_days < 730:
                excess_m = (total_sp - natural_cap) / 1_000_000
                flags.append({
                    "type":     "INJECTED_SP",
                    "severity": "MEDIUM",
                    "title":    f"[{cname}] Injected SP — {total_sp:,} SP at {age_days} days",
                    "detail":   (
                        f"~{excess_m:.0f}M SP above natural cap. "
                        "Heavily injected — possible bought / planted account."
                    ),
                })
                score += 2

        except Exception:
            pass

        return flags, score

    def _flag_spy_skills(
        self,
        cname:  str,
        skills: Dict[str, Any],
    ) -> Tuple[List, int]:
        flags: List[Dict] = []
        score = 0

        skill_map: Dict[int, int] = {
            sk["skill_id"]: sk.get("trained_skill_level", 0)
            for sk in skills.get("skills", [])
        }

        high_spy: List[str] = []
        for skill_id, skill_name in SPY_SKILL_IDS.items():
            lvl = skill_map.get(skill_id, 0)
            if lvl >= 4:
                high_spy.append(f"{skill_name} {lvl}")

        if len(high_spy) >= 2:
            flags.append({
                "type":     "SPY_SKILL_PROFILE",
                "severity": "MEDIUM",
                "title":    f"[{cname}] High espionage-relevant skills ({len(high_spy)} maxed)",
                "detail":   ", ".join(high_spy),
            })
            score += 2

        return flags, score

    def _flag_wallet(
        self,
        cname:   str,
        journal: List[Dict[str, Any]],
    ) -> Tuple[List, int]:
        flags: List[Dict] = []
        score = 0

        TRANSFER_TYPES = {
            "player_trading", "contract_price", "contract_reward",
            "isk_transfer", "bounty_prizes",
        }
        large = [
            e for e in journal
            if e.get("ref_type") in TRANSFER_TYPES
            and abs(e.get("amount", 0)) > 500_000_000
        ]

        if large:
            total_isk = sum(abs(e.get("amount", 0)) for e in large)
            flags.append({
                "type":     "LARGE_ISK_TRANSFERS",
                "severity": "MEDIUM",
                "title":    (
                    f"[{cname}] {len(large)} large ISK transfer(s) — "
                    f"{total_isk / 1e9:.1f}B ISK total"
                ),
                "detail":   (
                    f"Largest single transfer: "
                    f"{max(abs(e.get('amount',0)) for e in large) / 1e9:.2f}B ISK. "
                    "Review for payments to hostile entities."
                ),
            })
            score += 2

        return flags, score

    def _flag_contacts(
        self,
        cname:    str,
        contacts: List[Dict[str, Any]],
    ) -> Tuple[List, int]:
        flags: List[Dict] = []
        score = 0

        # High standing contacts that might be hostile
        high_standing = [
            c for c in contacts
            if float(c.get("standing", 0)) >= 5.0
        ]
        if len(high_standing) >= 10:
            flags.append({
                "type":     "SUSPICIOUS_CONTACTS",
                "severity": "LOW",
                "title":    f"[{cname}] {len(high_standing)} high-standing contacts outside ARC",
                "detail":   "Large contact list with high standings — review for hostile affiliations.",
            })
            score += 1

        return flags, score

    def _flag_killmails(
        self,
        cname:    str,
        kms:      List[Dict[str, Any]],
    ) -> Tuple[List, int]:
        flags: List[Dict] = []
        score = 0

        arc_kills = [
            km for km in kms
            if km.get("victim", {}).get("corporation_id") == self._arc_corp_id
        ]
        if arc_kills:
            flags.append({
                "type":     "KILLED_ARC_MEMBERS",
                "severity": "HIGH",
                "title":    f"[{cname}] {len(arc_kills)} killmail(s) against ARC members",
                "detail":   "Has previously engaged ARC Security members in combat.",
            })
            score += 4

        return flags, score

    def _flag_assets(
        self,
        cname:  str,
        assets: List[Dict[str, Any]],
    ) -> Tuple[List, int]:
        flags: List[Dict] = []
        score = 0

        loc_ids = {a.get("location_id") for a in assets if a.get("location_id")}
        if len(loc_ids) > 25:
            flags.append({
                "type":     "WIDESPREAD_ASSETS",
                "severity": "LOW",
                "title":    f"[{cname}] Assets across {len(loc_ids)} locations",
                "detail":   "Assets spread across many regions — may indicate multiple parallel operations.",
            })
            score += 1

        return flags, score

    # ── Risk level ────────────────────────────────────────────────────────────

    @staticmethod
    def _risk_level(score: int) -> str:
        if score >= RISK_HIGH_THRESHOLD:
            return "HIGH"
        if score >= RISK_MEDIUM_THRESHOLD:
            return "MEDIUM"
        if score > 0:
            return "LOW"
        return "CLEAN"

    @staticmethod
    def risk_emoji(level: str) -> str:
        return {
            "HIGH":    "🔴",
            "MEDIUM":  "🟡",
            "LOW":     "🟢",
            "CLEAN":   "✅",
            "UNKNOWN": "⬜",
        }.get(level, "⬜")


# ============================================================
# MIGRATION HELPER
# ============================================================

def migrate_from_ign_registry() -> Dict[str, Any]:
    """
    Read ign_registry.json and return a partial arc_seat.json members dict.
    Tokens are NOT migrated — members must re-auth via /seat_auth.
    Returns {} if no migration data is found.
    """
    if not IGN_DATA_FILE.exists():
        return {}

    try:
        with open(IGN_DATA_FILE, encoding="utf-8") as f:
            ign_data = json.load(f)
    except Exception as e:
        print(f"[ARC-SEAT] Migration read error: {e}")
        return {}

    migrated: Dict[str, Any] = {}
    users = ign_data.get("users", {})

    for discord_id_str, user_rec in users.items():
        try:
            discord_id = int(discord_id_str)
        except ValueError:
            continue

        member = _default_member(discord_id)
        member["migrated_from_ign"] = True
        member["registered_at"]     = _now_iso()

        # IGN records store characters as a list of {name, character_id}
        for i, char_rec in enumerate(user_rec.get("characters", [])):
            char_id   = char_rec.get("character_id")
            char_name = char_rec.get("name") or char_rec.get("character_name") or "Unknown"
            if not char_id:
                continue
            char = _default_character(
                character_id=   int(char_id),
                character_name= str(char_name),
                is_main=        (i == 0),
            )
            member["characters"].append(char)

        if member["characters"]:
            migrated[discord_id_str] = member

    print(f"[ARC-SEAT] Migration: imported {len(migrated)} member(s) from ign_registry.json")
    return migrated


# ============================================================
# ROLE REMOVAL PROMPT VIEW
# ============================================================

class RoleRemovalView(discord.ui.View):
    """
    Sent inside a member's watchlist thread when a corp check fails.
    Presents two buttons — Remove All Roles / Keep Roles.
    Only PROMPT_AUTHORIZED_ROLES may interact.
    Disables itself after any action or after 48 h timeout.
    """

    def __init__(
        self,
        cog:             "ArcSeatCog",
        guild:           discord.Guild,
        target_member:   discord.Member,
        roles_to_remove: List[discord.Role],
    ) -> None:
        super().__init__(timeout=172800)   # 48 hours
        self.cog             = cog
        self.guild           = guild
        self.target_member   = target_member
        self.roles_to_remove = roles_to_remove
        self.prompt_msg: Optional[discord.Message] = None   # set after send

    # ── Auth check ────────────────────────────────────────────────────────────

    def _is_authorized(self, interaction: discord.Interaction) -> bool:
        if not isinstance(interaction.user, discord.Member):
            return False
        return any(r.name in PROMPT_AUTHORIZED_ROLES for r in interaction.user.roles)

    def _disable_all(self) -> None:
        for item in self.children:
            item.disabled = True  # type: ignore[attr-defined]
        self.stop()

    # ── Remove All Roles ──────────────────────────────────────────────────────

    @discord.ui.button(
        label="Remove All Roles",
        style=discord.ButtonStyle.danger,
        emoji="🗑️",
    )
    async def btn_remove(
        self,
        interaction: discord.Interaction,
        button:      discord.ui.Button,
    ) -> None:
        if not self._is_authorized(interaction):
            await interaction.response.send_message(
                "❌ Only the **Administration Council** or **Corporation Leader** "
                "can take this action.",
                ephemeral=True,
            )
            return

        await interaction.response.defer()
        self._disable_all()

        removed: List[str] = []
        failed:  List[str] = []

        for role in self.roles_to_remove:
            try:
                if role in self.target_member.roles:
                    await self.target_member.remove_roles(
                        role,
                        reason=(
                            f"ARC-SEAT: corp check — manual removal "
                            f"by {interaction.user} ({interaction.user.id})"
                        ),
                    )
                    removed.append(role.name)
            except discord.Forbidden:
                failed.append(role.name)
            except Exception as e:
                print(f"[ARC-SEAT] Role removal error for {role.name}: {e}")
                failed.append(role.name)

        result_embed = discord.Embed(
            title=     "✅ Roles Removed",
            color=     discord.Color.green(),
            timestamp= datetime.now(timezone.utc),
        )
        result_embed.add_field(
            name="Member",    value=self.target_member.mention, inline=True
        )
        result_embed.add_field(
            name="Actioned by", value=interaction.user.mention, inline=True
        )
        if removed:
            result_embed.add_field(
                name="🗑️ Roles removed",
                value="\n".join(f"• {r}" for r in removed),
                inline=False,
            )
        if failed:
            result_embed.add_field(
                name="⚠️ Could not remove (missing permissions)",
                value="\n".join(f"• {r}" for r in failed),
                inline=False,
            )

        if self.prompt_msg:
            try:
                await self.prompt_msg.edit(embed=result_embed, view=self)
            except Exception as e:
                print(f"[ARC-SEAT] Prompt message edit failed: {e}")

        await self.cog._log_to_hierarchy(
            self.guild,
            self.target_member,
            roles_removed=removed,
            rank_roles_flagged=[],
            actioned_by=interaction.user,
        )

    # ── Keep Roles ────────────────────────────────────────────────────────────

    @discord.ui.button(
        label="Keep Roles",
        style=discord.ButtonStyle.secondary,
        emoji="🔒",
    )
    async def btn_keep(
        self,
        interaction: discord.Interaction,
        button:      discord.ui.Button,
    ) -> None:
        if not self._is_authorized(interaction):
            await interaction.response.send_message(
                "❌ Only the **Administration Council** or **Corporation Leader** "
                "can take this action.",
                ephemeral=True,
            )
            return

        await interaction.response.defer()
        self._disable_all()

        result_embed = discord.Embed(
            title=     "🔒 No Action Taken — Roles Kept",
            color=     discord.Color.greyple(),
            timestamp= datetime.now(timezone.utc),
        )
        result_embed.add_field(
            name="Member",     value=self.target_member.mention, inline=True
        )
        result_embed.add_field(
            name="Decision by", value=interaction.user.mention,  inline=True
        )
        result_embed.add_field(
            name="Roles retained",
            value="\n".join(f"• {r.name}" for r in self.roles_to_remove) or "None",
            inline=False,
        )

        if self.prompt_msg:
            try:
                await self.prompt_msg.edit(embed=result_embed, view=self)
            except Exception as e:
                print(f"[ARC-SEAT] Prompt message edit failed: {e}")

        await self.cog._log_to_hierarchy(
            self.guild,
            self.target_member,
            roles_removed=[],
            rank_roles_flagged=[r.name for r in self.roles_to_remove],
            actioned_by=interaction.user,
            kept=True,
        )

    # ── Timeout ───────────────────────────────────────────────────────────────

    async def on_timeout(self) -> None:
        self._disable_all()
        if self.prompt_msg:
            try:
                timeout_embed = discord.Embed(
                    title=       "⏰ Role Review Timed Out — No Action Taken",
                    color=       discord.Color.orange(),
                    description= (
                        f"No decision was made for {self.target_member.mention} "
                        "within 48 hours. Roles have been retained. "
                        "Please review manually."
                    ),
                    timestamp=   datetime.now(timezone.utc),
                )
                timeout_embed.add_field(
                    name="Roles still held",
                    value="\n".join(f"• {r.name}" for r in self.roles_to_remove) or "None",
                    inline=False,
                )
                await self.prompt_msg.edit(embed=timeout_embed, view=self)
            except Exception as e:
                print(f"[ARC-SEAT] Timeout embed update failed: {e}")


# ============================================================
# COG
# ============================================================

class ArcSeatCog(commands.Cog, name="ArcSeat"):
    """
    ARC-SEAT — EVE Intelligence & Member Tracking System.
    Tracks corp membership, skills, wallet, assets, and spy indicators.
    """

    def __init__(self, bot: commands.Bot) -> None:
        self.bot            = bot
        self._esi           = ESIClient()
        self._spy_engine:   Optional[SpyDetectionEngine] = None

        # OAuth state store for /seat_add_char flow (keyed by state token)
        self._oauth_states: Dict[str, Dict[str, Any]] = {}

        # Start background tasks
        if not self._token_refresh_loop.is_running():
            self._token_refresh_loop.start()
        if not self._corp_sync_loop.is_running():
            self._corp_sync_loop.start()
        if not self._esi_pull_loop.is_running():
            self._esi_pull_loop.start()
        if not self._skill_snapshot_loop.is_running():
            self._skill_snapshot_loop.start()

    def cog_unload(self) -> None:
        for t in (
            self._token_refresh_loop,
            self._corp_sync_loop,
            self._esi_pull_loop,
            self._skill_snapshot_loop,
        ):
            if t.is_running():
                t.cancel()
        asyncio.create_task(self._esi.close())

    # ── on_ready ──────────────────────────────────────────────────────────────

    @commands.Cog.listener()
    async def on_ready(self) -> None:
        await self._setup()

    async def _setup(self) -> None:
        # Load data and rebuild spy engine
        data             = await load_seat_data()
        cfg              = data.get("config", {})
        self._spy_engine = SpyDetectionEngine(
            hostile_corps=     cfg.get("hostile_corps",     []),
            hostile_alliances= cfg.get("hostile_alliances", []),
            arc_corp_id=       cfg.get("arc_corp_id"),
        )

        # Migration
        members = data.get("members", {})
        if not members:
            migrated = migrate_from_ign_registry()
            if migrated:
                data["members"] = migrated
                await save_seat_data(data)

        # Register OAuth callback for /seat_add_char on the overlay FastAPI app
        self._register_callback_route()

        # Sync characters from own DB (arc_seat.db) into arc_seat.json
        await self._sync_characters_from_db(data)

        # Ensure watch-list channel is reachable
        for guild in self.bot.guilds:
            await self._ensure_watchlist_channel(guild, data)

        await save_seat_data(data)
        print(
            f"[ARC-SEAT] Ready. {len(data.get('members', {}))} member(s) tracked. "
            "Tokens shared with overlay_api via overlay DB."
        )

    def _register_callback_route(self) -> None:
        """
        Schedules the callback route registration as a background task.
        This avoids on_ready race conditions between cogs — the task
        retries until the overlay FastAPI app is available.
        """
        asyncio.create_task(self._register_callback_route_with_retry())

    async def _register_callback_route_with_retry(self) -> None:
        """
        Retry registering /seat/auth/callback on the overlay FastAPI app
        for up to 60 seconds (30 attempts × 2 s). This handles the case
        where overlay_api's on_ready fires after arc_seat's on_ready.
        """
        for attempt in range(30):
            overlay_cog = (
                self.bot.get_cog("OverlayAPI")
                or self.bot.get_cog("OverlayApiCog")
            )
            app = getattr(overlay_cog, "app", None)

            if app is not None:
                try:
                    app.add_api_route(
                        "/seat/auth/callback",
                        self._oauth_callback_handler,
                        methods=["GET"],
                    )
                    print(
                        f"[ARC-SEAT] /seat/auth/callback registered "
                        f"(attempt {attempt + 1})."
                    )
                    return
                except Exception as e:
                    print(f"[ARC-SEAT] Callback route registration error: {e}")
                    return

            await asyncio.sleep(2)

        print(
            "[ARC-SEAT] ⚠️  Could not register /seat/auth/callback after 60 s. "
            "Ensure overlay_api.py is loaded and exposes self.app. "
            "/seat_add_char will not work until the bot restarts."
        )

    async def _oauth_callback_handler(self, code: str, state: str):
        """
        FastAPI GET /seat/auth/callback
        Called by EVE SSO after a member authorises via /seat_add_char.

        IMPORTANT: This runs inside FastAPI's event loop (background thread),
        NOT the discord.py main loop. Therefore:
          • _seat_save_token()  — sync SQLite write, safe from any loop ✅
          • _atomic_write()     — sync file write, bypasses asyncio lock ✅
          • ESI pull            — scheduled on bot loop via run_coroutine_threadsafe ✅
        Never call load_seat_data() / save_seat_data() here — they use an
        asyncio.Lock() that belongs to the discord.py loop and will deadlock.
        """
        from fastapi.responses import HTMLResponse

        def _html(title: str, body: str, colour: str = "#2ECC71") -> HTMLResponse:
            return HTMLResponse(f"""<!DOCTYPE html>
<html><head><title>ARC SEAT</title>
<style>body{{background:#0a1a2f;color:#ccd6f6;font-family:Consolas;
  display:flex;align-items:center;justify-content:center;height:100vh;margin:0}}
.box{{text-align:center;border:1px solid #1e3148;padding:40px;border-radius:8px;max-width:480px}}
h1{{color:{colour}}}p{{color:#8a99aa}}</style></head>
<body><div class="box"><h1>{title}</h1><p>{body}</p>
<p style="margin-top:20px">You can close this window and return to Discord.</p>
</div></body></html>""")

        # Validate state
        state_entry = self._oauth_states.pop(state, None)
        if state_entry is None or time.time() > state_entry["expires"]:
            return _html("❌ Auth Failed", "Invalid or expired state token.", "#E74C3C")

        discord_id = state_entry["discord_id"]

        # ── Token exchange ────────────────────────────────────────────────────
        # Fresh session scoped to this (FastAPI) event loop.
        creds = base64.b64encode(
            f"{EVE_CLIENT_ID}:{EVE_CLIENT_SECRET}".encode()
        ).decode()

        async with aiohttp.ClientSession() as sess:
            async with sess.post(
                SSO_TOKEN_URL,
                headers={
                    "Authorization": f"Basic {creds}",
                    "Content-Type":  "application/x-www-form-urlencoded",
                },
                data={
                    "grant_type":   "authorization_code",
                    "code":         code,
                    "redirect_uri": SEAT_CALLBACK_URL,
                },
                timeout=aiohttp.ClientTimeout(total=30),
            ) as r:
                if r.status != 200:
                    text = await r.text()
                    print(f"[ARC-SEAT] Token exchange failed {r.status}: {text[:200]}")
                    return _html("❌ Auth Failed", "Token exchange with EVE SSO failed.", "#E74C3C")
                tokens = await r.json()

        if "access_token" not in tokens:
            return _html("❌ Auth Failed", "Token exchange with EVE SSO failed.", "#E74C3C")

        # ── Extract character info from JWT payload ────────────────────────────
        # EVE SSO v2 issues JWT access tokens. The character ID and name are
        # encoded directly in the payload — no verify HTTP call needed.
        # payload['sub'] = "CHARACTER:EVE:<character_id>"
        # payload['name'] = "Character Name"
        try:
            parts   = tokens["access_token"].split(".")
            padding = "=" * (4 - len(parts[1]) % 4)
            payload = json.loads(
                base64.urlsafe_b64decode(parts[1] + padding).decode("utf-8")
            )
            sub       = payload.get("sub", "")      # "CHARACTER:EVE:12345678"
            char_id   = int(sub.split(":")[-1])
            char_name = str(payload.get("name", ""))
            if not char_id or not char_name:
                raise ValueError(f"Missing sub/name in JWT payload: {payload}")
        except Exception as e:
            print(f"[ARC-SEAT] JWT decode failed: {e}")
            return _html("❌ Auth Failed", "Could not read character info from token.", "#E74C3C")

        expires   = int(tokens.get("expires_in", 1200))

        # ── 1. Save token to arc_seat.db  (sync — safe from any event loop) ──
        _seat_save_token(
            discord_id, char_id, char_name,
            tokens["access_token"], tokens["refresh_token"], expires,
        )

        # ── 2. Update arc_seat.json  (sync atomic write — no asyncio lock) ───
        try:
            raw  = DATA_FILE.read_text(encoding="utf-8").strip() if DATA_FILE.exists() else ""
            data = json.loads(raw) if raw else _default_data()
            if not isinstance(data, dict):
                data = _default_data()
        except Exception:
            data = _default_data()

        key     = str(discord_id)
        members = data.setdefault("members", {})

        if key not in members:
            members[key] = _default_member(discord_id)

        member = members[key]
        chars  = member.setdefault("characters", [])

        if not any(c["character_id"] == char_id for c in chars):
            char = _default_character(
                character_id=   char_id,
                character_name= char_name,
                is_main=        not chars,
            )
            char["has_tokens"]      = True
            chars.append(char)
            member["verified"]      = True
            member["registered_at"] = member.get("registered_at") or _now_iso()
            data["members"][key]    = member

        try:
            _atomic_write(data)
        except Exception as e:
            print(f"[ARC-SEAT] Callback JSON write error: {e}")

        print(
            f"[ARC-SEAT] Auth complete: Discord {discord_id} → "
            f"{char_name} ({char_id}) saved to arc_seat.db + arc_seat.json."
        )

        # ── 3. Schedule ESI pull on the discord.py main loop ─────────────────
        try:
            asyncio.run_coroutine_threadsafe(
                self._pull_character_esi(discord_id, char_id),
                self.bot.loop,
            )
        except Exception as e:
            print(f"[ARC-SEAT] Could not schedule ESI pull: {e}")

        return _html(
            "✅ Character Added",
            f"<strong>{char_name}</strong> has been added to your ARC-SEAT profile.",
        )

    async def _sync_characters_from_db(
        self, data: Dict[str, Any]
    ) -> None:
        """
        Scan arc_seat.db (seat_tokens) and import any character not yet
        present in arc_seat.json. This ensures arc_seat.json stays in
        sync with the token DB after restarts or manual DB edits.
        No overlay DB is read.
        """
        members = data.setdefault("members", {})
        changed = False

        for row in _seat_get_all_tokens_global():
            disc_id = row["discord_user_id"]
            key     = str(disc_id)

            if key not in members:
                members[key] = _default_member(disc_id)

            member = members[key]
            chars  = member.setdefault("characters", [])

            if not any(c["character_id"] == row["character_id"] for c in chars):
                char = _default_character(
                    character_id=   row["character_id"],
                    character_name= row["character_name"],
                    is_main=        not chars,
                )
                char["has_tokens"]      = True
                chars.append(char)
                member["verified"]      = True
                member["registered_at"] = member.get("registered_at") or _now_iso()
                changed = True
                print(
                    f"[ARC-SEAT] Synced '{row['character_name']}' "
                    f"(Discord {disc_id}) from arc_seat.db."
                )

        if changed:
            data["members"] = members
            await save_seat_data(data)

    # ── Token management ─────────────────────────────────────────────────────

    async def _ensure_valid_token(
        self,
        discord_id: int,
        char_id:    int,
    ) -> Optional[str]:
        """
        Returns a valid ESI access token for a specific character.
        Reads from arc_seat.db (seat_tokens table) only — no overlay dependency.
        Refreshes automatically and writes the new token back to arc_seat.db.
        """
        row = _seat_get_token(discord_id, char_id)
        if row is None:
            return None

        if row["expires_at"] > time.time() + 60:
            return row["access_token"]

        return await self._esi.refresh_token(
            discord_user_id= discord_id,
            character_id=    char_id,
            character_name=  row["character_name"],
            refresh_tok=     row["refresh_token"],
        )

    # ── ESI data pull ─────────────────────────────────────────────────────────

    async def _pull_character_esi(
        self,
        discord_id: int,
        char_id:    int,
    ) -> None:
        """Pull all ESI endpoints for a single character."""
        data   = await load_seat_data()
        key    = str(discord_id)
        member = data.get("members", {}).get(key)
        if not member:
            return

        char = next(
            (c for c in member.get("characters", []) if c["character_id"] == char_id),
            None,
        )
        if not char:
            return

        token = await self._ensure_valid_token(discord_id, char_id)
        cache = char.setdefault("cache", {})

        # ── Public (no token needed) ──────────────────────────────────────────
        pub = await self._esi.get(f"/characters/{char_id}/")
        if pub:
            char["corporation_id"]  = pub.get("corporation_id")
            char["alliance_id"]     = pub.get("alliance_id")
            char["security_status"] = round(float(pub.get("security_status", 0.0)), 2)
            char["birthday"]        = pub.get("birthday")

            # Resolve corp name
            corp_id = char.get("corporation_id")
            if corp_id:
                corp_info = await self._esi.get(f"/corporations/{corp_id}/")
                if corp_info:
                    char["corporation_name"] = corp_info.get("name", str(corp_id))

            # Resolve alliance name
            alliance_id = char.get("alliance_id")
            if alliance_id:
                all_info = await self._esi.get(f"/alliances/{alliance_id}/")
                if all_info:
                    char["alliance_name"] = all_info.get("name", str(alliance_id))

        # Corp history — public
        corp_history = await self._esi.get(
            f"/characters/{char_id}/corporationhistory/"
        )
        if corp_history is not None:
            cache["corp_history"] = corp_history

        # Check in-ARC-corp (main corp OR approved subsidiary)
        cfg = data.get("config", {})
        char["in_arc_corp"] = _is_approved_corp(char.get("corporation_id"), cfg)

        # zkillboard — public
        zkm = await self._esi.zkill_character(char_id, page=1)
        if zkm is not None:
            cache["killmails"] = zkm[:50]   # keep last 50

        if not token:
            # Can't pull authenticated endpoints without a token
            char["last_esi_pull"] = _now_iso()
            data["members"][key]  = member
            await save_seat_data(data)
            return

        # ── Authenticated endpoints ───────────────────────────────────────────
        async def _pull(path: str) -> Optional[Any]:
            return await self._esi.get(path, access_token=token)

        # Skills
        skills = await _pull(f"/characters/{char_id}/skills/")
        if skills:
            cache["skills"]   = skills
            char["total_sp"]  = int(skills.get("total_sp", 0))

        # Skill queue
        sq = await _pull(f"/characters/{char_id}/skillqueue/")
        if sq is not None:
            cache["skill_queue"] = sq

        # Wallet balance
        wb = await _pull(f"/characters/{char_id}/wallet/")
        if wb is not None:
            cache["wallet_balance"] = float(wb)

        # Wallet journal (last page)
        wj = await _pull(f"/characters/{char_id}/wallet/journal/")
        if wj is not None:
            cache["wallet_journal"] = wj[:200]   # keep last 200 entries

        # Assets
        assets = await _pull(f"/characters/{char_id}/assets/")
        if assets is not None:
            cache["assets"] = assets

        # Contacts
        contacts = await _pull(f"/characters/{char_id}/contacts/")
        if contacts is not None:
            cache["contacts"] = contacts

        # Standings
        standings = await _pull(f"/characters/{char_id}/standings/")
        if standings is not None:
            cache["standings"] = standings

        # Clones
        clones = await _pull(f"/characters/{char_id}/clones/")
        if clones is not None:
            cache["clones"] = clones

        # Implants
        implants = await _pull(f"/characters/{char_id}/implants/")
        if implants is not None:
            cache["implants"] = implants

        # Industry jobs
        jobs = await _pull(f"/characters/{char_id}/industry/jobs/")
        if jobs is not None:
            cache["industry_jobs"] = jobs

        char["last_esi_pull"] = _now_iso()
        char["has_tokens"]    = (_overlay_get_token(discord_id) is not None)
        data["members"][key]  = member
        await save_seat_data(data)
        print(f"[ARC-SEAT] ESI pull complete: {char.get('character_name')} ({char_id})")

        # Run spy scan after ESI pull
        await self._run_spy_scan(discord_id)

    # ── Spy scan ─────────────────────────────────────────────────────────────

    async def _run_spy_scan(self, discord_id: int) -> None:
        """Run the spy detection engine for a member and update their record."""
        if not self._spy_engine:
            return

        data   = await load_seat_data()
        key    = str(discord_id)
        member = data.get("members", {}).get(key)
        if not member:
            return

        flags, score, risk_level = self._spy_engine.scan_member(member)
        member["flags"]      = flags
        member["risk_score"] = score
        member["risk_level"] = risk_level
        data["members"][key] = member
        await save_seat_data(data)

        if flags:
            # Find the guild and post to watch-list
            for guild in self.bot.guilds:
                discord_member = guild.get_member(discord_id)
                if discord_member:
                    await self._update_watchlist_thread(
                        guild, discord_member, member, data
                    )
                    break

    # ── Corp sync ─────────────────────────────────────────────────────────────

    async def _sync_corp_for_member(
        self,
        guild:      discord.Guild,
        discord_id: int,
    ) -> None:
        """
        Re-check corp membership for all characters of a member.
        If no character is in ARC:
          - Remove ARC Security role only
          - Flag ARC Subsidized + rank roles for manual review in watch-list
        """
        data   = await load_seat_data()
        key    = str(discord_id)
        member = data.get("members", {}).get(key)
        if not member:
            return

        cfg         = data.get("config", {})
        arc_corp_id = cfg.get("arc_corp_id")
        if not arc_corp_id:
            return

        characters   = member.get("characters", [])
        any_in_corp  = False

        for char in characters:
            # Corp membership is a public ESI endpoint — no token needed
            pub = await self._esi.get(f"/characters/{char['character_id']}/")
            if pub:
                char["corporation_id"] = pub.get("corporation_id")
                char["in_arc_corp"]    = _is_approved_corp(char["corporation_id"], cfg)
                if char["in_arc_corp"]:
                    any_in_corp = True

        member["last_corp_check"] = _now_iso()
        data["members"][key]      = member
        await save_seat_data(data)

        discord_member = guild.get_member(discord_id)
        if not discord_member:
            return

        if not any_in_corp:
            # Collect ALL ARC roles held by this member for the prompt
            all_arc_roles = [
                r for r in discord_member.roles
                if r.name in {ARC_SECURITY_ROLE, ARC_SUBSIDIZED_ROLE} | ARC_RANK_ROLES
            ]

            # Update the watchlist thread (corp fail notice, no auto-removal)
            await self._update_watchlist_thread(
                guild, discord_member, member, data,
                corp_fail=True,
                rank_roles_held=all_arc_roles,
            )

            # Send the role-removal prompt with buttons into that same thread
            await self._send_role_removal_prompt(
                guild, discord_member, member, data, all_arc_roles
            )

            await self._log_to_hierarchy(
                guild,
                discord_member,
                roles_removed=[],
                rank_roles_flagged=[r.name for r in all_arc_roles],
            )

    # ── Role removal prompt ───────────────────────────────────────────────────

    async def _send_role_removal_prompt(
        self,
        guild:           discord.Guild,
        discord_member:  discord.Member,
        member_rec:      Dict[str, Any],
        data:            Dict[str, Any],
        roles_to_remove: List[discord.Role],
    ) -> None:
        """
        Post a role-removal decision embed with buttons into the member's
        watchlist thread.  Only PROMPT_AUTHORIZED_ROLES can interact.
        """
        if not roles_to_remove:
            return

        # Find or use the existing watchlist thread
        thread_id = member_rec.get("watch_list_thread_id")
        thread: Optional[discord.Thread] = None

        if thread_id:
            try:
                thread = guild.get_thread(thread_id)
                if thread is None:
                    thread = await guild.fetch_channel(thread_id)  # type: ignore[assignment]
            except Exception:
                thread = None

        if not isinstance(thread, discord.Thread):
            # No thread found — can't post prompt (watchlist channel will log it)
            print(
                f"[ARC-SEAT] Could not find watchlist thread for "
                f"{discord_member} — role removal prompt not sent."
            )
            return

        prompt_embed = discord.Embed(
            title=       "⚠️ Corp Check Failed — Action Required",
            description= (
                f"{discord_member.mention} has no character in an approved ARC corporation.\n\n"
                f"**Only** <roles with Administration Council or Corporation Leader> "
                f"may act on this.\n\n"
                f"Choose an action below:"
            ),
            color=       discord.Color.orange(),
            timestamp=   datetime.now(timezone.utc),
        )
        prompt_embed.add_field(
            name="Roles pending removal",
            value="\n".join(f"• {r.name}" for r in roles_to_remove),
            inline=False,
        )
        prompt_embed.set_footer(
            text="This prompt expires in 48 hours. If no action is taken, roles are retained."
        )

        view = RoleRemovalView(
            cog=             self,
            guild=           guild,
            target_member=   discord_member,
            roles_to_remove= roles_to_remove,
        )

        try:
            msg = await thread.send(embed=prompt_embed, view=view)
            view.prompt_msg = msg
            print(
                f"[ARC-SEAT] Role removal prompt sent for "
                f"{discord_member} in thread {thread.id}."
            )
        except Exception as e:
            print(f"[ARC-SEAT] Could not send role removal prompt: {e}")

    # ── Watch-list management ─────────────────────────────────────────────────

    async def _ensure_watchlist_channel(
        self,
        guild: discord.Guild,
        data:  Dict[str, Any],
    ) -> Optional[discord.ForumChannel]:
        """
        Returns the watch-list forum channel using the hardcoded channel ID.
        Never creates a new channel — if the channel is not found the bot
        logs a warning and returns None gracefully.
        """
        ch = guild.get_channel(WATCH_LIST_CHANNEL_ID)
        if isinstance(ch, discord.ForumChannel):
            return ch

        # Channel not in cache — try fetching it
        try:
            ch = await guild.fetch_channel(WATCH_LIST_CHANNEL_ID)
            if isinstance(ch, discord.ForumChannel):
                return ch
        except Exception:
            pass

        print(
            f"[ARC-SEAT] WARNING: Watch-list forum channel "
            f"{WATCH_LIST_CHANNEL_ID} not found in guild '{guild.name}'. "
            "Check that the channel ID is correct and the bot has access."
        )
        return None

    async def _update_watchlist_thread(
        self,
        guild:           discord.Guild,
        discord_member:  discord.Member,
        member_rec:      Dict[str, Any],
        data:            Dict[str, Any],
        corp_fail:       bool = False,
        rank_roles_held: Optional[List[discord.Role]] = None,
    ) -> None:
        """
        Create or update the watch-list forum thread for a flagged member.
        Thread title = member's display name.
        """
        ch = await self._ensure_watchlist_channel(guild, data)
        if not isinstance(ch, discord.ForumChannel):
            return

        flags      = member_rec.get("flags", [])
        risk_level = member_rec.get("risk_level", "UNKNOWN")
        risk_score = member_rec.get("risk_score", 0)
        risk_emoji = SpyDetectionEngine.risk_emoji(risk_level)

        # Build embed
        embed = discord.Embed(
            title=     f"{risk_emoji} {discord_member.display_name}  —  Risk: {risk_level}",
            color=     self._risk_colour(risk_level),
            timestamp= datetime.now(timezone.utc),
        )
        embed.add_field(
            name="Discord",
            value=discord_member.mention,
            inline=True,
        )
        embed.add_field(
            name="Risk Score",
            value=str(risk_score),
            inline=True,
        )
        embed.add_field(name="\u200b", value="\u200b", inline=True)

        # Characters
        chars = member_rec.get("characters", [])
        for char in chars:
            in_corp = char.get("in_arc_corp", False)
            corp    = char.get("corporation_name") or str(char.get("corporation_id", "?"))
            embed.add_field(
                name=  f"{'🌟' if char.get('is_main') else '👤'} {char['character_name']}",
                value= (
                    f"Corp: **{corp}**\n"
                    f"In ARC: {'✅' if in_corp else '❌'}\n"
                    f"SP: {char.get('total_sp', 0):,}"
                ),
                inline=True,
            )

        # Flags
        if flags:
            flag_lines = []
            for fl in flags[:15]:   # cap at 15 to stay under embed limits
                sev_icon = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(fl["severity"], "⬜")
                flag_lines.append(f"{sev_icon} **{fl['title']}**\n{fl['detail']}")
            embed.add_field(
                name=  f"⚠️ Flags Detected ({len(flags)})",
                value= "\n\n".join(flag_lines)[:1024],
                inline=False,
            )

        if corp_fail:
            embed.add_field(
                name=  "🚨 Corp Check Failed — Pending Review",
                value= (
                    "No character is currently in an approved ARC corporation.\n"
                    + (
                        f"**Roles held (pending decision):** "
                        f"{', '.join(r.name for r in (rank_roles_held or []))}"
                        if rank_roles_held else
                        "No ARC roles currently held."
                    )
                ),
                inline=False,
            )

        embed.set_footer(text=f"Discord ID: {discord_member.id} | Last scan: {_now_iso()[:19]} UTC")

        # Check for existing thread
        thread_id = member_rec.get("watch_list_thread_id")
        existing_thread = None
        if thread_id:
            try:
                existing_thread = guild.get_thread(thread_id)
                if existing_thread is None:
                    existing_thread = await guild.fetch_channel(thread_id)
            except Exception:
                existing_thread = None

        if isinstance(existing_thread, discord.Thread):
            # Update the first post
            try:
                async for msg in existing_thread.history(limit=1, oldest_first=True):
                    await msg.edit(embed=embed)
                    break
                else:
                    await existing_thread.send(embed=embed)
            except Exception as e:
                print(f"[ARC-SEAT] Watch-list thread update failed: {e}")
        else:
            # Create new thread
            thread_name = (
                f"{risk_emoji} {discord_member.display_name}"
                f"  [{risk_level}]"
            )[:100]
            try:
                thread, _ = await ch.create_thread(
                    name=    thread_name,
                    content= discord_member.mention,
                    embed=   embed,
                )
                member_rec["watch_list_thread_id"] = thread.id
                data["members"][str(discord_member.id)] = member_rec
                await save_seat_data(data)
            except Exception as e:
                print(f"[ARC-SEAT] Could not create watch-list thread: {e}")

    @staticmethod
    def _risk_colour(level: str) -> discord.Color:
        return {
            "HIGH":    discord.Color.red(),
            "MEDIUM":  discord.Color.yellow(),
            "LOW":     discord.Color.green(),
            "CLEAN":   discord.Color.green(),
            "UNKNOWN": discord.Color.greyple(),
        }.get(level, discord.Color.greyple())

    # ── Hierarchy log ─────────────────────────────────────────────────────────

    async def _log_to_hierarchy(
        self,
        guild:               discord.Guild,
        discord_member:      discord.Member,
        roles_removed:       List[str],
        rank_roles_flagged:  List[str],
        actioned_by:         Optional[discord.Member] = None,
        kept:                bool = False,
    ) -> None:
        ch = discord.utils.get(guild.text_channels, name=HIERARCHY_LOG_CH)
        if not ch:
            return

        if actioned_by:
            # Log a button decision result
            if kept:
                title  = "🔒 ARC-SEAT: Roles Kept — No Action Taken"
                colour = discord.Color.greyple()
            else:
                title  = "✅ ARC-SEAT: Roles Removed by Admin"
                colour = discord.Color.green()
        else:
            # Log the initial corp check failure (prompt sent)
            title  = "🚨 ARC-SEAT: Corp Check Failed — Awaiting Admin Decision"
            colour = discord.Color.orange()

        embed = discord.Embed(
            title=     title,
            color=     colour,
            timestamp= datetime.now(timezone.utc),
        )
        embed.add_field(name="Member", value=discord_member.mention, inline=True)

        if actioned_by:
            embed.add_field(
                name="Actioned by", value=actioned_by.mention, inline=True
            )

        if not actioned_by:
            embed.add_field(
                name="Status",
                value="Role removal prompt sent to watchlist thread. Awaiting admin decision.",
                inline=False,
            )

        if roles_removed:
            embed.add_field(
                name="🗑️ Roles removed",
                value=", ".join(roles_removed),
                inline=False,
            )
        if rank_roles_flagged:
            label = "🔒 Roles retained" if kept else "⚠️ Roles pending decision"
            embed.add_field(
                name=label,
                value=", ".join(rank_roles_flagged),
                inline=False,
            )

        try:
            await ch.send(embed=embed)
        except Exception:
            pass

    # ── Background tasks ─────────────────────────────────────────────────────

    @tasks.loop(seconds=TOKEN_REFRESH_INTERVAL)
    async def _token_refresh_loop(self) -> None:
        """
        Refresh any seat_tokens expiring within 5 minutes.
        Reads from and writes to arc_seat.db only — no overlay dependency.
        """
        soon = time.time() + 300
        for row in _seat_get_all_tokens_global():
            if row.get("expires_at", 0) > soon:
                continue
            await self._esi.refresh_token(
                discord_user_id= row["discord_user_id"],
                character_id=    row["character_id"],
                character_name=  row["character_name"],
                refresh_tok=     row["refresh_token"],
            )
            await asyncio.sleep(1)

    @_token_refresh_loop.before_loop
    async def _before_token_refresh(self) -> None:
        await self.bot.wait_until_ready()

    @tasks.loop(seconds=CORP_SYNC_INTERVAL)
    async def _corp_sync_loop(self) -> None:
        """Check corp membership for every registered member."""
        data = await load_seat_data()

        for guild in self.bot.guilds:
            for key in list(data.get("members", {}).keys()):
                try:
                    await self._sync_corp_for_member(guild, int(key))
                except Exception as e:
                    print(f"[ARC-SEAT] Corp sync error for {key}: {e}")
                await asyncio.sleep(2)   # rate-limit courtesy pause

    @_corp_sync_loop.before_loop
    async def _before_corp_sync(self) -> None:
        await self.bot.wait_until_ready()
        await asyncio.sleep(120)   # allow bot to settle before first run

    @tasks.loop(seconds=ESI_PULL_INTERVAL)
    async def _esi_pull_loop(self) -> None:
        """Full ESI pull for every registered character (every 6 h)."""
        data = await load_seat_data()

        for key, member in list(data.get("members", {}).items()):
            for char in member.get("characters", []):
                try:
                    await self._pull_character_esi(int(key), char["character_id"])
                except Exception as e:
                    print(
                        f"[ARC-SEAT] ESI pull error for "
                        f"{char.get('character_name')}: {e}"
                    )
                await asyncio.sleep(5)  # be kind to ESI rate limits

    @_esi_pull_loop.before_loop
    async def _before_esi_pull(self) -> None:
        await self.bot.wait_until_ready()
        await asyncio.sleep(180)   # stagger from other loops

    @tasks.loop(seconds=SKILL_SNAPSHOT_INTERVAL)
    async def _skill_snapshot_loop(self) -> None:
        """
        Take a daily SP snapshot per character for progression tracking.
        Stored under data['skill_snapshots'][discord_id][char_id] as a list
        of {timestamp, total_sp} dicts. Keeps last 90 snapshots.
        """
        data     = await load_seat_data()
        snaps    = data.setdefault("skill_snapshots", {})
        changed  = False
        now      = _now_iso()

        for key, member in data.get("members", {}).items():
            member_snaps = snaps.setdefault(key, {})
            for char in member.get("characters", []):
                total_sp = char.get("total_sp", 0)
                if not total_sp:
                    continue
                char_key = str(char["character_id"])
                char_snaps = member_snaps.setdefault(char_key, [])
                char_snaps.append({"timestamp": now, "total_sp": total_sp})
                # Keep last 90 snapshots
                if len(char_snaps) > 90:
                    member_snaps[char_key] = char_snaps[-90:]
                changed = True

        if changed:
            await save_seat_data(data)

    @_skill_snapshot_loop.before_loop
    async def _before_skill_snapshot(self) -> None:
        await self.bot.wait_until_ready()
        await asyncio.sleep(300)

    # ── Slash commands ────────────────────────────────────────────────────────

    @app_commands.command(
        name="seat_sync",
        description="Re-import your characters from ARC-SEAT into your profile (use after restart).",
    )
    async def seat_sync(self, interaction: discord.Interaction) -> None:
        """
        Scans arc_seat.db for any characters linked to this Discord user
        and imports them into arc_seat.json if missing. Useful after bot
        restarts or if the profile looks out of date.
        """
        await interaction.response.defer(ephemeral=True)

        rows = _seat_get_all_tokens(interaction.user.id)
        if not rows:
            await interaction.followup.send(
                "You have no EVE characters registered with ARC-SEAT.\n\n"
                "Use **`/seat_add_char`** to link your first character.",
                ephemeral=True,
            )
            return

        data    = await load_seat_data()
        key     = str(interaction.user.id)
        members = data.setdefault("members", {})

        if key not in members:
            members[key] = _default_member(interaction.user.id)

        member  = members[key]
        chars   = member.setdefault("characters", [])
        added   = 0

        for row in rows:
            if not any(c["character_id"] == row["character_id"] for c in chars):
                char = _default_character(
                    character_id=   row["character_id"],
                    character_name= row["character_name"],
                    is_main=        not chars,
                )
                char["has_tokens"]    = True
                chars.append(char)
                added += 1

        member["verified"]      = True
        member["registered_at"] = member.get("registered_at") or _now_iso()
        data["members"][key]    = member
        await save_seat_data(data)

        char_list = ", ".join(f"**{r['character_name']}**" for r in rows)
        await interaction.followup.send(
            f"✅ Synced {len(rows)} character(s): {char_list}\n"
            + (f"({added} newly imported)\n" if added else "")
            + "Use `/seat_status` to view your full profile.",
            ephemeral=True,
        )

    @app_commands.command(
        name="seat_add_char",
        description="Add an additional EVE Online character to your ARC-SEAT profile.",
    )
    async def seat_add_char(self, interaction: discord.Interaction) -> None:
        """
        Generates an EVE SSO link for an additional character.
        The token is stored in seat_tokens (separate from the overlay's eve_tokens),
        allowing unlimited characters per Discord account.
        """
        await interaction.response.defer(ephemeral=True)

        if not EVE_CLIENT_ID or not SEAT_CALLBACK_URL:
            await interaction.followup.send(
                "❌ `/seat_add_char` is not configured.\n"
                "An admin must set `SEAT_CALLBACK_URL` in Railway environment variables.\n"
                f"It should be: `https://your-railway-domain/seat/auth/callback`",
                ephemeral=True,
            )
            return

        import secrets
        from urllib.parse import urlencode

        state = secrets.token_hex(16)
        self._oauth_states[state] = {
            "discord_id": interaction.user.id,
            "expires":    time.time() + OAUTH_STATE_TTL,
        }

        params = {
            "response_type": "code",
            "client_id":     EVE_CLIENT_ID,
            "redirect_uri":  SEAT_CALLBACK_URL,
            "scope":         SEAT_SCOPES,
            "state":         state,
        }
        auth_url = f"{SSO_AUTH_URL}?{urlencode(params)}"

        embed = discord.Embed(
            title=       "➕ Add EVE Character",
            description= (
                "Click the link below to authorise an additional EVE character.\n\n"
                "This character will be added to your ARC-SEAT profile alongside "
                "any existing characters.\n\n"
                "⏱ This link expires in **10 minutes**.\n\n"
                f"🔗 [Authorise on EVE Online]({auth_url})"
            ),
            color= discord.Color.blurple(),
        )
        embed.set_footer(text="You can add as many characters as you have EVE accounts.")
        await interaction.followup.send(embed=embed, ephemeral=True)

    async def _resolve_missing_corp_names(
        self,
        m_rec: Dict[str, Any],
        data:  Dict[str, Any],
        key:   str,
    ) -> bool:
        """
        For any character that has a corporation_id but no corporation_name,
        do a live ESI lookup and fill in the name.  Also resolves alliance_name
        when missing.  Writes resolved names back to data[members][key] in-place
        and returns True if anything was updated (caller should save_seat_data).
        Safe to call when no ESI pull has happened yet — public endpoint, no token needed.
        """
        changed = False
        cfg     = data.get("config", {})

        for char in m_rec.get("characters", []):
            corp_id = char.get("corporation_id")

            # ── Corp name ────────────────────────────────────────────────────────
            if corp_id and not char.get("corporation_name"):
                corp_info = await self._esi.get(f"/corporations/{corp_id}/")
                if corp_info:
                    char["corporation_name"] = corp_info.get("name", str(corp_id))
                    # Also backfill in_arc_corp now that we have the corp_id
                    char["in_arc_corp"] = _is_approved_corp(corp_id, cfg)
                    changed = True

            # ── Alliance name ────────────────────────────────────────────────────
            alliance_id = char.get("alliance_id")
            if alliance_id and not char.get("alliance_name"):
                all_info = await self._esi.get(f"/alliances/{alliance_id}/")
                if all_info:
                    char["alliance_name"] = all_info.get("name", str(alliance_id))
                    changed = True

        if changed:
            data["members"][key] = m_rec

        return changed

    @app_commands.command(
        name="seat_status",
        description="View your ARC-SEAT registration and character status.",
    )
    async def seat_status(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        data   = await load_seat_data()
        key    = str(interaction.user.id)
        member = data.get("members", {}).get(key)

        if not member or not member.get("characters"):
            await interaction.followup.send(
                "You have no characters registered with ARC-SEAT.\n\n"
                "Use **`/seat_add_char`** to link your first EVE character.",
                ephemeral=True,
            )
            return

        # Resolve any missing corp/alliance names on-demand before display
        if await self._resolve_missing_corp_names(member, data, key):
            await save_seat_data(data)

        embed = discord.Embed(
            title= "🛡️ Your ARC-SEAT Profile",
            color= discord.Color.blurple(),
        )

        for char in member.get("characters", []):
            row        = _seat_get_token(interaction.user.id, char["character_id"])
            corp       = char.get("corporation_name") or str(char.get("corporation_id", "Unknown"))
            alliance   = char.get("alliance_name")
            in_corp    = "✅ In ARC" if char.get("in_arc_corp") else f"❌ {corp}"
            token_s    = "✅ Active" if row else "⚠️ Missing — run `/seat_add_char`"
            last       = (char.get("last_esi_pull") or "Never")[:19]
            corp_line  = f"Corp: **{corp}**"
            if alliance:
                corp_line += f"\nAlliance: **{alliance}**"
            embed.add_field(
                name=  f"{'🌟 Main' if char.get('is_main') else '👤 Alt'}: {char['character_name']}",
                value= (
                    f"{corp_line}\n"
                    f"Status: {in_corp}\n"
                    f"SP: {char.get('total_sp', 0):,}\n"
                    f"Token: {token_s}\n"
                    f"Last pull: {last} UTC"
                ),
                inline=True,
            )

        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(
        name="seat_whois",
        description="[Admin] Show all EVE characters linked to a Discord member.",
    )
    @app_commands.describe(member="The Discord member to look up.")
    async def seat_whois(
        self,
        interaction: discord.Interaction,
        member:      discord.Member,
    ) -> None:
        await interaction.response.defer(ephemeral=True)

        if not isinstance(interaction.user, discord.Member):
            await interaction.followup.send("Must be used in a server.", ephemeral=True)
            return

        # Admin or CREATOR_ROLES only
        allowed_roles = {
            "ARC Security Administration Council",
            "ARC Security Corporation Leader",
            "ARC General",
            "ARC Commander",
        }
        if not (
            interaction.user.guild_permissions.administrator
            or any(r.name in allowed_roles for r in interaction.user.roles)
        ):
            await interaction.followup.send("❌ Not authorised.", ephemeral=True)
            return

        data   = await load_seat_data()
        key    = str(member.id)
        m_rec  = data.get("members", {}).get(key)

        if not m_rec or not m_rec.get("characters"):
            await interaction.followup.send(
                f"{member.mention} has no characters registered in ARC-SEAT.",
                ephemeral=True,
            )
            return

        # Resolve any missing corp/alliance names on-demand before display
        if await self._resolve_missing_corp_names(m_rec, data, key):
            await save_seat_data(data)

        risk_emoji = SpyDetectionEngine.risk_emoji(m_rec.get("risk_level", "UNKNOWN"))

        embed = discord.Embed(
            title= f"🔍 Intelligence Profile: {member.display_name}",
            color= self._risk_colour(m_rec.get("risk_level", "UNKNOWN")),
        )
        embed.add_field(
            name=  "Risk Level",
            value= f"{risk_emoji} {m_rec.get('risk_level', 'UNKNOWN')} (score: {m_rec.get('risk_score', 0)})",
            inline=False,
        )

        for char in m_rec.get("characters", []):
            corp      = char.get("corporation_name") or str(char.get("corporation_id", "Unknown"))
            alliance  = char.get("alliance_name")
            in_c      = "✅" if char.get("in_arc_corp") else "❌"
            corp_line = f"Corp: **{corp}**  {in_c}"
            if alliance:
                corp_line += f"\nAlliance: **{alliance}**"
            embed.add_field(
                name=  f"{'🌟' if char.get('is_main') else '👤'} {char['character_name']}",
                value= (
                    f"{corp_line}\n"
                    f"SP: {char.get('total_sp', 0):,}\n"
                    f"Born: {(char.get('birthday') or '?')[:10]}\n"
                    f"Sec: {char.get('security_status', 0.0):.1f}"
                ),
                inline=True,
            )

        flags = m_rec.get("flags", [])
        if flags:
            flag_lines = [
                f"{'🔴' if f['severity']=='HIGH' else '🟡' if f['severity']=='MEDIUM' else '🟢'} "
                f"{f['title']}"
                for f in flags[:10]
            ]
            embed.add_field(
                name=  f"⚠️ Active Flags ({len(flags)})",
                value= "\n".join(flag_lines)[:1024],
                inline=False,
            )

        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(
        name="seat_skills",
        description="[Admin] Show skill snapshot progression for a member.",
    )
    @app_commands.describe(member="The Discord member to inspect.")
    async def seat_skills(
        self,
        interaction: discord.Interaction,
        member:      discord.Member,
    ) -> None:
        await interaction.response.defer(ephemeral=True)

        data   = await load_seat_data()
        key    = str(member.id)
        m_rec  = data.get("members", {}).get(key)

        if not m_rec:
            await interaction.followup.send("No data found.", ephemeral=True)
            return

        snaps_by_char = data.get("skill_snapshots", {}).get(key, {})
        embed = discord.Embed(
            title= f"📊 Skill Progression — {member.display_name}",
            color= discord.Color.blue(),
        )

        for char in m_rec.get("characters", []):
            cid     = str(char["character_id"])
            snaps   = snaps_by_char.get(cid, [])
            total   = char.get("total_sp", 0)
            cache   = char.get("cache", {})
            sq      = cache.get("skill_queue") or []

            # SP gained since oldest snapshot
            sp_gained = 0
            if len(snaps) >= 2:
                sp_gained = total - snaps[0].get("total_sp", total)

            # Next skill in queue
            next_skill = ""
            if sq:
                nxt = sq[0] if sq else None
                if nxt:
                    finish = nxt.get("finish_date", "")[:10]
                    next_skill = f"Queue head finishes: {finish}"

            embed.add_field(
                name=  f"{'🌟' if char.get('is_main') else '👤'} {char['character_name']}",
                value= (
                    f"Total SP: **{total:,}**\n"
                    + (f"SP gained (tracked): **+{sp_gained:,}**\n" if sp_gained else "")
                    + (f"Snapshots: {len(snaps)}\n" if snaps else "No snapshots yet\n")
                    + (f"{next_skill}" if next_skill else "")
                ),
                inline=True,
            )

            # Detailed skill levels for key spy-relevant skills
            skills_cache = cache.get("skills")
            if skills_cache:
                skill_map = {
                    sk["skill_id"]: sk.get("trained_skill_level", 0)
                    for sk in skills_cache.get("skills", [])
                }
                spy_lines = []
                for sid, sname in SPY_SKILL_IDS.items():
                    lvl = skill_map.get(sid, 0)
                    if lvl > 0:
                        spy_lines.append(f"{sname}: {lvl}")
                if spy_lines:
                    embed.add_field(
                        name=  f"🔍 Intel-Relevant Skills",
                        value= "\n".join(spy_lines),
                        inline=True,
                    )

        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(
        name="seat_scan",
        description="[Admin] Force a spy detection scan on a member.",
    )
    @app_commands.describe(member="The member to scan.")
    async def seat_scan(
        self,
        interaction: discord.Interaction,
        member:      discord.Member,
    ) -> None:
        await interaction.response.defer(ephemeral=True)

        allowed_roles = {
            "ARC Security Administration Council",
            "ARC Security Corporation Leader",
            "ARC General",
            "ARC Commander",
        }
        if not (
            isinstance(interaction.user, discord.Member)
            and (
                interaction.user.guild_permissions.administrator
                or any(r.name in allowed_roles for r in interaction.user.roles)
            )
        ):
            await interaction.followup.send("❌ Not authorised.", ephemeral=True)
            return

        await interaction.followup.send(
            f"⏳ Running full ESI pull + spy scan for {member.mention}...",
            ephemeral=True,
        )

        data   = await load_seat_data()
        key    = str(member.id)
        m_rec  = data.get("members", {}).get(key)

        if not m_rec:
            await interaction.followup.send("No SEAT record found.", ephemeral=True)
            return

        for char in m_rec.get("characters", []):
            await self._pull_character_esi(member.id, char["character_id"])

        data    = await load_seat_data()
        m_rec   = data.get("members", {}).get(key, {})
        flags   = m_rec.get("flags", [])
        risk    = m_rec.get("risk_level", "UNKNOWN")
        score   = m_rec.get("risk_score", 0)
        r_emoji = SpyDetectionEngine.risk_emoji(risk)

        result = (
            f"{r_emoji} **Scan complete for {member.display_name}**\n"
            f"Risk Level: **{risk}** (score: {score})\n"
            f"Flags found: **{len(flags)}**\n"
        )
        if flags:
            result += "\n".join(
                f"• {f['severity']}: {f['title']}" for f in flags[:5]
            )
            if len(flags) > 5:
                result += f"\n… and {len(flags) - 5} more — see watch-list thread."

        await interaction.followup.send(result, ephemeral=True)

    @app_commands.command(
        name="seat_verify_all",
        description="[Admin] Force a corp sync + spy scan for all registered members.",
    )
    async def seat_verify_all(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)

        if not (
            isinstance(interaction.user, discord.Member)
            and interaction.user.guild_permissions.administrator
        ):
            await interaction.followup.send(
                "❌ Administrator permission required.", ephemeral=True
            )
            return

        data  = await load_seat_data()
        count = len(data.get("members", {}))
        await interaction.followup.send(
            f"⏳ Running corp sync + scan for **{count}** member(s). "
            "This runs in the background — check #arc-hierarchy-log for results.",
            ephemeral=True,
        )

        for guild in self.bot.guilds:
            for key in list(data.get("members", {}).keys()):
                try:
                    await self._sync_corp_for_member(guild, int(key))
                    await asyncio.sleep(3)
                except Exception as e:
                    print(f"[ARC-SEAT] verify_all error for {key}: {e}")

    @app_commands.command(
        name="seat_hostile_corp",
        description="[Admin] Add or remove a corp from the hostile corps list.",
    )
    @app_commands.describe(
        action="add or remove",
        corp_id="The integer corporation ID from EVE.",
        corp_name="Human-readable name for the log (optional).",
    )
    async def seat_hostile_corp(
        self,
        interaction: discord.Interaction,
        action:      str,
        corp_id:     int,
        corp_name:   Optional[str] = None,
    ) -> None:
        await interaction.response.defer(ephemeral=True)

        if not (
            isinstance(interaction.user, discord.Member)
            and interaction.user.guild_permissions.administrator
        ):
            await interaction.followup.send(
                "❌ Administrator permission required.", ephemeral=True
            )
            return

        if action.lower() not in ("add", "remove"):
            await interaction.followup.send(
                "Action must be `add` or `remove`.", ephemeral=True
            )
            return

        data = await load_seat_data()
        cfg  = data.setdefault("config", {})
        lst  = cfg.setdefault("hostile_corps", [])

        if action.lower() == "add":
            if corp_id not in lst:
                lst.append(corp_id)
            msg = f"✅ Added corp `{corp_name or corp_id}` (ID {corp_id}) to hostile list."
        else:
            if corp_id in lst:
                lst.remove(corp_id)
            msg = f"✅ Removed corp `{corp_name or corp_id}` (ID {corp_id}) from hostile list."

        await save_seat_data(data)

        # Rebuild spy engine with new hostile corps list
        self._spy_engine = SpyDetectionEngine(
            hostile_corps=     cfg.get("hostile_corps",     []),
            hostile_alliances= cfg.get("hostile_alliances", []),
            arc_corp_id=       cfg.get("arc_corp_id"),
        )

        await interaction.followup.send(msg, ephemeral=True)

    @app_commands.command(
        name="seat_unlink",
        description="Remove a character from your ARC-SEAT profile.",
    )
    @app_commands.describe(character_name="Exact character name to unlink.")
    async def seat_unlink(
        self,
        interaction:    discord.Interaction,
        character_name: str,
    ) -> None:
        await interaction.response.defer(ephemeral=True)

        data   = await load_seat_data()
        key    = str(interaction.user.id)
        member = data.get("members", {}).get(key)

        if not member:
            await interaction.followup.send("No SEAT profile found.", ephemeral=True)
            return

        # Find the character to remove
        target = next(
            (c for c in member.get("characters", [])
             if c.get("character_name", "").lower() == character_name.strip().lower()),
            None,
        )
        if not target:
            await interaction.followup.send(
                f"Character `{character_name}` not found in your profile.",
                ephemeral=True,
            )
            return

        char_id = target["character_id"]

        # Remove from arc_seat.json
        member["characters"] = [
            c for c in member.get("characters", [])
            if c["character_id"] != char_id
        ]
        data["members"][key] = member
        await save_seat_data(data)

        # Remove from seat_tokens if present (does NOT touch overlay eve_tokens)
        _seat_delete_token(interaction.user.id, char_id)

        await interaction.followup.send(
            f"✅ `{character_name}` removed from your ARC-SEAT profile.\n"
            "_Note: if this was your main character linked via `/eve_link`, "
            "you may want to run `/eve_link` again to update that token separately._",
            ephemeral=True,
        )


# ============================================================
# SETUP
# ============================================================

async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ArcSeatCog(bot))
