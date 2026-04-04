# cogs/arc_seat.py
#
# ARC-SEAT  —  Autonomous EVE Intelligence & Member Tracking
# ===========================================================
#
# A self-contained SeAT equivalent built as a discord.py cog.
# Runs entirely inside the existing Railway-hosted bot — no extra server needed.
#
# AUTH FLOW
# ---------
# Reuses the existing overlay_api.py SSO flow.
# Members authenticate ONCE via /eve_link — no separate /seat_auth needed.
# arc_seat reads tokens directly from the overlay's SQLite database
# (MISSION_DB_PATH, default: /data/missions.db).
# The overlay's EVE_SCOPES have been expanded to cover all SEAT endpoints.
#
# ON STARTUP
# ----------
# arc_seat scans the overlay DB and auto-imports any character that has
# already authenticated via /eve_link.  New members just run /eve_link
# followed by /seat_sync.
#
# FEATURES
# --------
# 1.  Auto-import from overlay DB on startup
# 2.  /seat_sync   — register with SEAT after /eve_link
# 3.  Full ESI pull every 6 h per character
#     • Corp membership, corp history, character info (public)
#     • Skills + skill queue, wallet, assets, contacts, standings,
#       clones, implants, industry jobs  (authenticated)
#     • Killmails via zkillboard public API (no auth)
# 4.  Automated spy-detection scoring on every ESI pull
# 5.  Corp sync loop every 1 h
#     • Corp check fails → ARC Security auto-removed
#     • ARC Subsidized + rank roles → flagged for manual review only
# 6.  Skill snapshot every 24 h (SP progression history)
# 7.  Forum watch-list — one thread per flagged member
# 8.  Migration from ign_registry.json on first run
#
# RAILWAY ENV VARS  (no new vars needed beyond existing overlay setup)
# --------------------------------------------------------------------
#   EVE_CLIENT_ID      — already set for overlay
#   EVE_CLIENT_SECRET  — already set for overlay
#   EVE_CORP_ID        — integer ARC corp ID
#   MISSION_DB_PATH    — path to overlay SQLite DB (default: /data/missions.db)
#
# COMMANDS
# --------
#   /seat_sync          — register after /eve_link
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

# ============================================================
# ENV VARS
# ============================================================
EVE_CLIENT_ID     = os.getenv("EVE_CLIENT_ID",     "")
EVE_CLIENT_SECRET = os.getenv("EVE_CLIENT_SECRET", "")
ARC_CORP_ID_ENV   = int(os.getenv("EVE_CORP_ID",   "0") or "0") or None

# Path to the overlay's SQLite database — tokens are read from here.
# Must match MISSION_DB_PATH in overlay_api.py (default: /data/missions.db).
OVERLAY_DB_PATH = os.getenv("MISSION_DB_PATH", "/data/missions.db")

# ============================================================
# ESI / SSO ENDPOINTS
# ============================================================
ESI_BASE      = "https://esi.evetech.net/latest"
SSO_TOKEN_URL = "https://login.eveonline.com/v2/oauth/token"
ZKILL_BASE    = "https://zkillboard.com/api"

# NOTE: ESI scopes are defined in overlay_api.py (EVE_SCOPES).
# Members authorise once via /eve_link — arc_seat reads tokens from
# the same overlay SQLite database (OVERLAY_DB_PATH).

# ============================================================
# DISCORD CONFIG
# ============================================================
WATCH_LIST_CHANNEL_ID = 1461162252173316249   # existing forum channel — never recreated
ARC_SECURITY_ROLE     = "ARC Security"
ARC_SUBSIDIZED_ROLE   = "ARC Subsidized"
HIERARCHY_LOG_CH      = "arc-hierarchy-log"

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
            "arc_corp_id":         ARC_CORP_ID_ENV,
            "hostile_corps":       [],   # List[int]  corp IDs
            "hostile_alliances":   [],   # List[int]  alliance IDs
        },
        "oauth_states":   {},   # state_token → {discord_id, is_alt, expires}
        "skill_snapshots": {},  # str(discord_id) → {str(char_id) → [snapshots]}
    }


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
# OVERLAY DATABASE HELPERS
# ============================================================
# arc_seat reads EVE SSO tokens from the same SQLite database
# that overlay_api.py manages. This avoids duplicate auth flows —
# members authenticate once via /eve_link and both cogs share
# the same tokens.

import sqlite3 as _sqlite3


def _overlay_db_connect() -> _sqlite3.Connection:
    conn = _sqlite3.connect(OVERLAY_DB_PATH)
    conn.row_factory = _sqlite3.Row
    return conn


def _overlay_get_token(discord_user_id: int) -> Optional[Dict[str, Any]]:
    """
    Read the EVE token row for a Discord user from the overlay DB.
    Returns a plain dict or None if no row exists.
    """
    try:
        with _overlay_db_connect() as conn:
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
            row = conn.execute(
                "SELECT * FROM eve_tokens WHERE discord_user_id=?",
                (discord_user_id,),
            ).fetchone()
            if row is None:
                return None
            return dict(row)
    except Exception as e:
        print(f"[ARC-SEAT] Overlay DB read error: {e}")
        return None


def _overlay_save_token(
    discord_user_id: int,
    character_id:    int,
    character_name:  str,
    access_token:    str,
    refresh_token:   str,
    expires_in:      int,
) -> None:
    """Write a refreshed token back to the overlay DB."""
    try:
        with _overlay_db_connect() as conn:
            conn.execute("""
                INSERT INTO eve_tokens
                    (discord_user_id, character_id, character_name,
                     access_token, refresh_token, expires_at)
                VALUES (?,?,?,?,?,?)
                ON CONFLICT(discord_user_id) DO UPDATE SET
                    access_token=excluded.access_token,
                    refresh_token=excluded.refresh_token,
                    expires_at=excluded.expires_at
            """, (
                discord_user_id, character_id, character_name,
                access_token, refresh_token,
                time.time() + expires_in,
            ))
    except Exception as e:
        print(f"[ARC-SEAT] Overlay DB write error: {e}")


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

    async def refresh_token(
        self,
        discord_user_id: int,
        character_id:    int,
        character_name:  str,
        refresh_tok:     str,
    ) -> Optional[str]:
        """
        Refresh the EVE access token.
        Writes the new token back to the overlay DB so both cogs stay in sync.
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

            # Write back to overlay DB so /eve_link and arc_seat stay in sync
            _overlay_save_token(
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

        # Sync characters from overlay DB for any registered member
        # who doesn't yet have a character record (e.g. freshly migrated)
        await self._sync_characters_from_overlay(data)

        # Ensure watch-list channel is reachable
        for guild in self.bot.guilds:
            await self._ensure_watchlist_channel(guild, data)

        await save_seat_data(data)
        print(
            f"[ARC-SEAT] Ready. {len(data.get('members', {}))} member(s) tracked. "
            "Tokens shared with overlay_api via overlay DB."
        )

    async def _sync_characters_from_overlay(
        self, data: Dict[str, Any]
    ) -> None:
        """
        For every member in arc_seat.json, check the overlay DB.
        If the overlay has a token for that Discord user and arc_seat
        doesn't have a character record yet, create one automatically.
        This means a member who ran /eve_link is picked up by arc_seat
        without any extra steps.
        """
        members  = data.setdefault("members", {})
        changed  = False

        # Also scan overlay DB for any Discord user not yet in members
        try:
            with _overlay_db_connect() as conn:
                rows = conn.execute("SELECT * FROM eve_tokens").fetchall()
        except Exception:
            rows = []

        for row in rows:
            row      = dict(row)
            disc_id  = row["discord_user_id"]
            key      = str(disc_id)

            if key not in members:
                members[key] = _default_member(disc_id)

            member = members[key]
            chars  = member.setdefault("characters", [])

            # Check if this character is already registered
            if not any(c["character_id"] == row["character_id"] for c in chars):
                char = _default_character(
                    character_id=   row["character_id"],
                    character_name= row["character_name"],
                    is_main=        True,
                )
                char["has_tokens"] = True
                chars.append(char)
                member["verified"]      = True
                member["registered_at"] = member.get("registered_at") or _now_iso()
                changed = True
                print(
                    f"[ARC-SEAT] Auto-imported character '{row['character_name']}' "
                    f"(Discord {disc_id}) from overlay DB."
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
        Returns a valid ESI access token for the given Discord user.

        Tokens live in the overlay's SQLite DB (managed by overlay_api.py).
        The overlay only stores ONE token per Discord user (the main character).
        Alts without a stored token will only receive public ESI data.

        Flow:
          1. Read row from overlay DB.
          2. If the token is still valid → return it.
          3. If expired → refresh via EVE SSO and write back to overlay DB.
          4. If no row found → return None (user has not yet run /eve_link).
        """
        row = _overlay_get_token(discord_id)
        if row is None:
            return None

        # Token still valid
        if row["expires_at"] > time.time() + 60:
            return row["access_token"]

        # Expired — refresh
        new_access = await self._esi.refresh_token(
            discord_user_id= discord_id,
            character_id=    row["character_id"],
            character_name=  row["character_name"],
            refresh_tok=     row["refresh_token"],
        )
        return new_access

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

        # Check in-ARC-corp
        cfg          = data.get("config", {})
        arc_corp_id  = cfg.get("arc_corp_id")
        char["in_arc_corp"] = (
            bool(arc_corp_id) and char.get("corporation_id") == arc_corp_id
        )

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
                char["in_arc_corp"]    = (char["corporation_id"] == arc_corp_id)
                if char["in_arc_corp"]:
                    any_in_corp = True

        member["last_corp_check"] = _now_iso()
        data["members"][key]      = member
        await save_seat_data(data)

        discord_member = guild.get_member(discord_id)
        if not discord_member:
            return

        if not any_in_corp:
            # Auto-remove ARC Security only
            roles_to_remove = [
                r for r in discord_member.roles
                if r.name == ARC_SECURITY_ROLE
            ]
            if roles_to_remove:
                try:
                    await discord_member.remove_roles(
                        *roles_to_remove,
                        reason="ARC-SEAT: corp check failed — no character in ARC corp",
                    )
                    print(
                        f"[ARC-SEAT] Removed {[r.name for r in roles_to_remove]} "
                        f"from {discord_member} — not in ARC corp."
                    )
                except discord.Forbidden:
                    print(
                        f"[ARC-SEAT] Missing permissions to remove roles from "
                        f"{discord_member}."
                    )

            # Flag ARC Subsidized + rank roles for manual review (NOT auto-removed)
            roles_for_review = [
                r for r in discord_member.roles
                if r.name == ARC_SUBSIDIZED_ROLE or r.name in ARC_RANK_ROLES
            ]

            await self._update_watchlist_thread(
                guild, discord_member, member, data,
                corp_fail=True,
                rank_roles_held=roles_for_review,
            )

            await self._log_to_hierarchy(
                guild,
                discord_member,
                roles_removed=[r.name for r in roles_to_remove],
                rank_roles_flagged=[r.name for r in roles_for_review],
            )

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
            removed_txt = "ARC Security"
            embed.add_field(
                name=  "🚨 Corp Check Failed",
                value= (
                    f"**Auto-removed:** {removed_txt}\n"
                    + (
                        f"**⚠️ Roles requiring manual review (NOT auto-removed):** "
                        f"{', '.join(r.name for r in (rank_roles_held or []))}"
                        if rank_roles_held else ""
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
    ) -> None:
        ch = discord.utils.get(guild.text_channels, name=HIERARCHY_LOG_CH)
        if not ch:
            return

        embed = discord.Embed(
            title=     "🚨 ARC-SEAT: Corp Check Failed",
            color=     discord.Color.red(),
            timestamp= datetime.now(timezone.utc),
        )
        embed.add_field(name="Member",    value=discord_member.mention, inline=True)
        embed.add_field(name="Action",    value="Corp check failed — not in ARC corp", inline=False)
        if roles_removed:
            embed.add_field(
                name="✅ Auto-removed", value=", ".join(roles_removed), inline=False
            )
        if rank_roles_flagged:
            embed.add_field(
                name="⚠️ Rank roles — MANUAL REVIEW REQUIRED",
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
        Refresh any overlay DB tokens that are expiring within 5 minutes.
        This keeps the shared token store current for both overlay_api and arc_seat.
        """
        try:
            with _overlay_db_connect() as conn:
                rows = conn.execute("SELECT * FROM eve_tokens").fetchall()
        except Exception:
            return

        soon = time.time() + 300
        for row in rows:
            row = dict(row)
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
        description="Sync your EVE character with ARC-SEAT intelligence tracking.",
    )
    async def seat_sync(self, interaction: discord.Interaction) -> None:
        """
        Tells the member to use /eve_link if they haven't already, then
        immediately syncs their character from the overlay DB into arc_seat.
        """
        await interaction.response.defer(ephemeral=True)

        # Check if the overlay already has a token for this user
        row = _overlay_get_token(interaction.user.id)

        if row is None:
            await interaction.followup.send(
                "You don't have an EVE character linked yet.\n\n"
                "Use **`/eve_link`** to authorise your EVE character first, "
                "then run `/seat_sync` again to register it with ARC-SEAT.",
                ephemeral=True,
            )
            return

        # Import character into arc_seat if not already there
        data    = await load_seat_data()
        key     = str(interaction.user.id)
        members = data.setdefault("members", {})

        if key not in members:
            members[key] = _default_member(interaction.user.id)

        member = members[key]
        chars  = member.setdefault("characters", [])

        already_registered = any(
            c["character_id"] == row["character_id"] for c in chars
        )
        if not already_registered:
            char = _default_character(
                character_id=   row["character_id"],
                character_name= row["character_name"],
                is_main=        True,
            )
            char["has_tokens"]    = True
            chars.append(char)
            member["verified"]      = True
            member["registered_at"] = _now_iso()
            data["members"][key]    = member
            await save_seat_data(data)

        await interaction.followup.send(
            f"✅ **{row['character_name']}** is now registered with ARC-SEAT.\n"
            "An ESI pull will run in the background — "
            "use `/seat_status` to check your profile.",
            ephemeral=True,
        )

        # Trigger an immediate ESI pull
        asyncio.create_task(
            self._pull_character_esi(interaction.user.id, row["character_id"])
        )

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
                "**Step 1:** Run `/eve_link` to authorise your EVE character.\n"
                "**Step 2:** Run `/seat_sync` to register it with ARC-SEAT.",
                ephemeral=True,
            )
            return

        # Check token status from overlay DB
        overlay_row = _overlay_get_token(interaction.user.id)
        token_status = (
            "✅ Active (via `/eve_link`)" if overlay_row else
            "⚠️ No token — run `/eve_link` then `/seat_sync`"
        )

        embed = discord.Embed(
            title= "🛡️ Your ARC-SEAT Profile",
            color= discord.Color.blurple(),
        )
        embed.add_field(name="Token", value=token_status, inline=False)

        for char in member.get("characters", []):
            corp    = char.get("corporation_name") or str(char.get("corporation_id", "?"))
            in_corp = "✅ In ARC" if char.get("in_arc_corp") else "❌ Not in ARC"
            last    = (char.get("last_esi_pull") or "Never")[:19]
            embed.add_field(
                name=  f"{'🌟 Main' if char.get('is_main') else '👤 Alt'}: {char['character_name']}",
                value= (
                    f"Corp: **{corp}**\n"
                    f"Status: {in_corp}\n"
                    f"SP: {char.get('total_sp', 0):,}\n"
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
            corp  = char.get("corporation_name") or str(char.get("corporation_id", "?"))
            in_c  = "✅" if char.get("in_arc_corp") else "❌"
            embed.add_field(
                name=  f"{'🌟' if char.get('is_main') else '👤'} {char['character_name']}",
                value= (
                    f"Corp: **{corp}**  {in_c}\n"
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

        before = len(member.get("characters", []))
        member["characters"] = [
            c for c in member.get("characters", [])
            if c.get("character_name", "").lower() != character_name.strip().lower()
        ]
        after = len(member["characters"])

        if before == after:
            await interaction.followup.send(
                f"Character `{character_name}` not found in your profile.",
                ephemeral=True,
            )
            return

        data["members"][key] = member
        await save_seat_data(data)
        await interaction.followup.send(
            f"✅ `{character_name}` removed from your profile.", ephemeral=True
        )


# ============================================================
# SETUP
# ============================================================

async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ArcSeatCog(bot))
