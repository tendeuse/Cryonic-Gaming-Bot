# cogs/arc_seat.py
#
# ARC-SEAT  —  Autonomous EVE Intelligence & Member Tracking
# ===========================================================
#
# A self-contained SeAT equivalent built as a discord.py cog.
# Runs entirely inside the existing Railway-hosted bot — no extra server needed.
#
# FEATURES
# --------
# 1.  Per-member EVE SSO auth  (/seat_auth, /seat_add_alt)
#     • Separate ESI scopes from overlay_api.py
#     • Callback registered on the overlay FastAPI server (/seat/auth/callback)
#
# 2.  Full ESI data pull  (every 6 h per character)
#     • Corp membership, corp history, character info
#     • Skills + skill queue  (progression tracking)
#     • Wallet balance + journal
#     • Assets
#     • Contacts + standings
#     • Clone / implants
#     • Recent killmails  (via zkillboard public API)
#
# 3.  Automated spy-detection scoring on every pull
#     Flags: corp hopping, alt in hostile corp, young character, injected SP,
#            high spy skills, large ISK transfers, hostile contacts,
#            killed ARC members, widespread assets
#
# 4.  Corp sync loop  (every 1 h)
#     • Members who leave ARC corp → ARC Security removed; ARC Subsidized kept
#     • Rank roles (Lieutenant, Commander, etc.) → flagged for manual review
#     • Any flag or corp failure → forum watch-list thread created / updated
#
# 5.  Skill snapshot loop  (every 24 h)
#     • Stores daily SP total per character for progression graphs
#
# 6.  Migration from ign_registry.json
#     • Imports existing character names / IDs on first run
#     • Members must re-auth via /seat_auth to obtain fresh tokens
#
# REQUIREMENTS
# ------------
# overlay_api.py  must expose  self.app  on OverlayApiCog  (one-line change,
#   already applied — see overlay_api.py line ~833).
#
# Railway env vars (add these):
#   SEAT_CALLBACK_URL  — full public URL of the callback, e.g.
#                        https://your-app.up.railway.app/seat/auth/callback
#   EVE_CORP_ID        — integer corp ID for ARC Security (already set if
#                        ign_registration uses it)
#   EVE_CLIENT_ID      — same EVE developer app as overlay  (already set)
#   EVE_CLIENT_SECRET  — same EVE developer app as overlay  (already set)
#
# BACKWARD COMPATIBILITY
# ----------------------
# • Does not touch ign_registry.json  (read-only migration source)
# • Does not modify any other cog
# • Safe to add/remove without affecting other features

import asyncio
import base64
import io
import json
import os
import secrets
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlencode

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
SEAT_CALLBACK_URL = os.getenv("SEAT_CALLBACK_URL", "")
ARC_CORP_ID_ENV   = int(os.getenv("EVE_CORP_ID",   "0") or "0") or None

# ============================================================
# ESI / SSO ENDPOINTS
# ============================================================
ESI_BASE      = "https://esi.evetech.net/latest"
SSO_AUTH_URL  = "https://login.eveonline.com/v2/oauth/authorize"
SSO_TOKEN_URL = "https://login.eveonline.com/v2/oauth/token"
SSO_VERIFY_URL = "https://esi.evetech.net/verify/"
ZKILL_BASE    = "https://zkillboard.com/api"

# ============================================================
# ESI SCOPES  (comprehensive — separate from overlay scopes)
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

# OAuth state TTL
OAUTH_STATE_TTL = 600  # 10 min

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
        "access_token":    None,
        "refresh_token":   None,
        "token_expires":   0,
        "in_arc_corp":     False,
        "last_esi_pull":   None,
        "has_tokens":      False,
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

    async def verify_token(self, access_token: str) -> Optional[Dict[str, Any]]:
        """Verify access token → {CharacterID, CharacterName, …}."""
        sess = await self._sess()
        try:
            async with sess.get(
                SSO_VERIFY_URL,
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                return await r.json() if r.status == 200 else None
        except Exception as e:
            print(f"[ARC-SEAT] Token verify error: {e}")
            return None

    async def refresh_token(self, refresh_tok: str) -> Optional[Dict[str, Any]]:
        """Refresh access token → new token dict or None."""
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
                return await r.json() if r.status == 200 else None
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

        # In-memory OAuth state store (keyed by state token)
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

        # Register the OAuth callback on the overlay FastAPI app
        self._register_callback_route()

        # Ensure watch-list channel exists in each guild
        for guild in self.bot.guilds:
            await self._ensure_watchlist_channel(guild, data)

        await save_seat_data(data)
        print(
            f"[ARC-SEAT] Ready. {len(data.get('members', {}))} member(s) tracked."
        )

    def _register_callback_route(self) -> None:
        """
        Add /seat/auth/callback to the existing overlay FastAPI app.
        Requires overlay_api.py to expose self.app on OverlayApiCog (already done).
        """
        try:
            overlay_cog = (
                self.bot.get_cog("OverlayAPI")
                or self.bot.get_cog("OverlayApiCog")
            )
            app = getattr(overlay_cog, "app", None)
            if app is None:
                print(
                    "[ARC-SEAT] ⚠️  Could not find overlay FastAPI app. "
                    "OAuth callback not registered. "
                    "Ensure overlay_api.py exposes self.app on OverlayApiCog."
                )
                return

            # Register route on the FastAPI app instance
            app.add_api_route(
                "/seat/auth/callback",
                self._oauth_callback_handler,
                methods=["GET"],
            )
            print("[ARC-SEAT] OAuth callback route registered: /seat/auth/callback")

        except Exception as e:
            print(f"[ARC-SEAT] Could not register callback route: {e}")

    # ── OAuth callback handler (called by FastAPI) ────────────────────────────

    async def _oauth_callback_handler(self, code: str, state: str):
        """
        FastAPI GET /seat/auth/callback
        Called when EVE SSO redirects back after member authorisation.
        """
        from fastapi.responses import HTMLResponse

        def _html(title: str, body: str, colour: str = "#2ECC71") -> HTMLResponse:
            return HTMLResponse(f"""<!DOCTYPE html>
<html><head><title>ARC SEAT</title>
<style>body{{background:#0a1a2f;color:#ccd6f6;font-family:Consolas;
  display:flex;align-items:center;justify-content:center;height:100vh;margin:0}}
.box{{text-align:center;border:1px solid #1e3148;padding:40px;border-radius:8px}}
h1{{color:{colour}}}p{{color:#8a99aa}}</style></head>
<body><div class="box"><h1>{title}</h1><p>{body}</p>
<p>You can close this window and return to Discord.</p>
</div></body></html>""")

        # Validate state
        state_entry = self._oauth_states.pop(state, None)
        if state_entry is None or time.time() > state_entry["expires"]:
            return _html("❌ Auth Failed", "Invalid or expired state token.", "#E74C3C")

        discord_id = state_entry["discord_id"]
        is_alt     = state_entry.get("is_alt", False)

        # Exchange code for tokens
        tokens = await self._esi.exchange_code(code)
        if not tokens or "access_token" not in tokens:
            return _html("❌ Auth Failed", "Token exchange with EVE SSO failed.", "#E74C3C")

        # Verify and extract character info
        char_info = await self._esi.verify_token(tokens["access_token"])
        if not char_info:
            return _html("❌ Auth Failed", "Could not verify EVE character.", "#E74C3C")

        char_id   = int(char_info["CharacterID"])
        char_name = str(char_info["CharacterName"])

        # Save to data store
        data    = await load_seat_data()
        members = data.setdefault("members", {})
        key     = str(discord_id)

        if key not in members:
            members[key] = _default_member(discord_id)

        member   = members[key]
        chars    = member.setdefault("characters", [])
        is_first = not chars and not is_alt

        # Check if character already registered
        existing = next((c for c in chars if c["character_id"] == char_id), None)
        if existing:
            # Update tokens
            existing["access_token"]  = tokens["access_token"]
            existing["refresh_token"] = tokens["refresh_token"]
            existing["token_expires"] = int(time.time()) + int(tokens.get("expires_in", 1200))
            existing["has_tokens"]    = True
        else:
            char = _default_character(char_id, char_name, is_main=is_first)
            char["access_token"]  = tokens["access_token"]
            char["refresh_token"] = tokens["refresh_token"]
            char["token_expires"] = int(time.time()) + int(tokens.get("expires_in", 1200))
            char["has_tokens"]    = True
            chars.append(char)

        member["verified"]       = True
        member["registered_at"]  = member.get("registered_at") or _now_iso()
        data["members"][key]     = member
        await save_seat_data(data)

        print(
            f"[ARC-SEAT] Auth complete: Discord {discord_id} → "
            f"{'alt' if is_alt else 'main'} {char_name} ({char_id})"
        )

        # Schedule an immediate ESI pull for this character
        asyncio.create_task(self._pull_character_esi(discord_id, char_id))

        return _html(
            "✅ Character Linked",
            f"<strong>{char_name}</strong> has been linked to your Discord account.",
        )

    # ── Token management ─────────────────────────────────────────────────────

    async def _ensure_valid_token(
        self,
        data:      Dict[str, Any],
        discord_id: int,
        char_id:    int,
    ) -> Optional[str]:
        """
        Returns a valid access token for the given character, refreshing if needed.
        Saves updated token data to `data` (caller must save to disk).
        """
        key    = str(discord_id)
        member = data.get("members", {}).get(key)
        if not member:
            return None

        char = next(
            (c for c in member.get("characters", []) if c["character_id"] == char_id),
            None,
        )
        if not char or not char.get("refresh_token"):
            return None

        # Valid if not expiring within 60 s
        if char.get("token_expires", 0) > time.time() + 60:
            return char.get("access_token")

        # Refresh
        new_tokens = await self._esi.refresh_token(char["refresh_token"])
        if not new_tokens or "access_token" not in new_tokens:
            char["has_tokens"] = False
            return None

        char["access_token"]  = new_tokens["access_token"]
        char["refresh_token"] = new_tokens.get("refresh_token", char["refresh_token"])
        char["token_expires"] = int(time.time()) + int(new_tokens.get("expires_in", 1200))
        char["has_tokens"]    = True
        return char["access_token"]

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

        token = await self._ensure_valid_token(data, discord_id, char_id)
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
            token = await self._ensure_valid_token(data, discord_id, char["character_id"])
            pub   = await self._esi.get(f"/characters/{char['character_id']}/")
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
        """Refresh all tokens expiring in the next 5 minutes."""
        data    = await load_seat_data()
        changed = False
        soon    = time.time() + 300

        for key, member in data.get("members", {}).items():
            for char in member.get("characters", []):
                if not char.get("refresh_token"):
                    continue
                if char.get("token_expires", 0) > soon:
                    continue
                new_tokens = await self._esi.refresh_token(char["refresh_token"])
                if new_tokens and "access_token" in new_tokens:
                    char["access_token"]  = new_tokens["access_token"]
                    char["refresh_token"] = new_tokens.get("refresh_token", char["refresh_token"])
                    char["token_expires"] = int(time.time()) + int(new_tokens.get("expires_in", 1200))
                    char["has_tokens"]    = True
                    changed               = True
                else:
                    char["has_tokens"] = False
                    changed            = True

        if changed:
            await save_seat_data(data)

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
        name="seat_auth",
        description="Link your main EVE character to the ARC intelligence system.",
    )
    async def seat_auth(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)

        if not EVE_CLIENT_ID or not SEAT_CALLBACK_URL:
            await interaction.followup.send(
                "❌ ARC-SEAT is not configured. "
                "Ask an admin to set `EVE_CLIENT_ID`, `EVE_CLIENT_SECRET`, "
                "and `SEAT_CALLBACK_URL` in Railway.",
                ephemeral=True,
            )
            return

        state = secrets.token_hex(16)
        self._oauth_states[state] = {
            "discord_id": interaction.user.id,
            "is_alt":     False,
            "expires":    time.time() + OAUTH_STATE_TTL,
        }

        params = {
            "response_type": "code",
            "client_id":     EVE_CLIENT_ID,
            "redirect_uri":  SEAT_CALLBACK_URL,
            "scope":         SEAT_SCOPES,
            "state":         state,
        }
        url = f"{SSO_AUTH_URL}?{urlencode(params)}"

        embed = discord.Embed(
            title=       "🔗 Link your EVE character",
            description= (
                "Click the button below to authorise ARC-SEAT access to your EVE character.\n\n"
                "This grants ARC security officers access to:\n"
                "• Corp membership & history\n"
                "• Skills & skill queue\n"
                "• Wallet activity\n"
                "• Assets, contacts & standings\n"
                "• Clones & implants\n\n"
                "⏱ This link expires in **10 minutes**."
            ),
            color= discord.Color.blurple(),
        )

        view = discord.ui.View()
        view.add_item(discord.ui.Button(
            label= "Authorise on EVE Online",
            url=   url,
            style= discord.ButtonStyle.link,
            emoji= "🚀",
        ))
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)

    @app_commands.command(
        name="seat_add_alt",
        description="Register an additional EVE character (alt) to your profile.",
    )
    async def seat_add_alt(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)

        if not EVE_CLIENT_ID or not SEAT_CALLBACK_URL:
            await interaction.followup.send(
                "❌ ARC-SEAT is not configured.", ephemeral=True
            )
            return

        state = secrets.token_hex(16)
        self._oauth_states[state] = {
            "discord_id": interaction.user.id,
            "is_alt":     True,
            "expires":    time.time() + OAUTH_STATE_TTL,
        }

        params = {
            "response_type": "code",
            "client_id":     EVE_CLIENT_ID,
            "redirect_uri":  SEAT_CALLBACK_URL,
            "scope":         SEAT_SCOPES,
            "state":         state,
        }
        url = f"{SSO_AUTH_URL}?{urlencode(params)}"

        view = discord.ui.View()
        view.add_item(discord.ui.Button(
            label="Authorise Alt", url=url, style=discord.ButtonStyle.link, emoji="👤"
        ))
        await interaction.followup.send(
            "Click below to add an alt character. "
            "Link expires in **10 minutes**.",
            view=view,
            ephemeral=True,
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
                "You have no characters registered. Use `/seat_auth` to link your main.",
                ephemeral=True,
            )
            return

        embed = discord.Embed(
            title= "🛡️ Your ARC-SEAT Profile",
            color= discord.Color.blurple(),
        )
        for char in member.get("characters", []):
            corp    = char.get("corporation_name") or str(char.get("corporation_id", "?"))
            tokens  = "✅ Active" if char.get("has_tokens") else "⚠️ Needs re-auth"
            in_corp = "✅ In ARC" if char.get("in_arc_corp") else "❌ Not in ARC"
            last    = (char.get("last_esi_pull") or "Never")[:19]
            embed.add_field(
                name=  f"{'🌟 Main' if char.get('is_main') else '👤 Alt'}: {char['character_name']}",
                value= (
                    f"Corp: **{corp}**\n"
                    f"Status: {in_corp}\n"
                    f"SP: {char.get('total_sp', 0):,}\n"
                    f"Token: {tokens}\n"
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
