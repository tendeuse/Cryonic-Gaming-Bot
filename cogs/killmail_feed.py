# cogs/killmail_feed.py
#
# ESI-time authoritative ordering + robust scanning + ESI diagnostics
#
# Key behavior:
# - zKill corp feed can return no killmail_time (confirmed by your inspect earlier).
# - We therefore select/order/window strictly using ESI killmail_time.
#
# Posting logic:
# - Always aims to post the next 5 NEWEST (by ESI time), but sends OLDEST->NEWEST within that batch,
#   so the most recent appears last in Discord.
#
# Performance logic:
# - Scans through zKill IDs using a persistent scan_cursor to avoid rechecking the same IDs repeatedly.
# - Attempts up to MAX_ESI_ATTEMPTS_PER_CYCLE ESI calls per cycle.
# - If ESI returns 420/429, aborts so backoff can kick in.
#
# Debug:
# - /killmail_status
# - /killmail_inspect
# - /killmail_debug_next
# - /killmail_debug_esi (shows ESI status histogram)
# - /killmail_reload (restricted roles)

import discord
from discord.ext import commands, tasks
from discord import app_commands
from pathlib import Path
import aiohttp
import asyncio
import json
import datetime
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict

# =====================
# CONFIG
# =====================

KILLMAIL_CHANNEL_NAME = "kill-mail"
CORPORATION_ID = 98743131

PRIMARY_GUILD_ID: Optional[int] = None  # None = all guilds

POLL_SECONDS = 120
FAST_POLL_SECONDS_WHEN_BACKLOG_REMAINS = 60
MAX_POSTS_PER_CYCLE = 5
WINDOW_DAYS = 14

MAX_ZKILL_PAGES = 8
ZKILL_REQUEST_DELAY = 0.25

# Hard cap on ESI requests per cycle (increase if needed, but watch ESI error limit)
MAX_ESI_ATTEMPTS_PER_CYCLE = 200

USER_AGENT = "Cryonic Gaming bot/1.0 (contact: tendeuse on Discord)"
ESI_BASE = "https://esi.evetech.net/latest"
IMAGE_BASE = "https://images.evetech.net"

DATA_FILE = Path("data/killmail_feed.json")
DATA_FILE.parent.mkdir(parents=True, exist_ok=True)

MAX_NAME_CACHE = 10000
MAX_SYSTEM_CACHE = 5000
MAX_TYPE_CACHE = 10000

CEO_ROLE = "ARC Security Corporation Leader"
COUNCIL_ROLE = "ARC Security Administration Council"
LYCAN_ROLE = "Lycan King"


# =====================
# UTILITIES
# =====================

def utcnow() -> datetime.datetime:
    return datetime.datetime.utcnow()

def utcnow_iso() -> str:
    return utcnow().isoformat()

def cutoff_utc() -> datetime.datetime:
    return utcnow() - datetime.timedelta(days=WINDOW_DAYS)

def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

def save_json(path: Path, data: Dict[str, Any]) -> None:
    path.write_text(json.dumps(data, indent=4), encoding="utf-8")

def safe_int(x) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None

def clamp_dict(d: Dict[str, Any], max_items: int) -> Dict[str, Any]:
    if len(d) <= max_items:
        return d
    keys = list(d.keys())[-max_items:]
    return {k: d[k] for k in keys}

def parse_killmail_time(value: Any) -> Optional[datetime.datetime]:
    if not isinstance(value, str) or not value:
        return None
    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    try:
        dtv = datetime.datetime.fromisoformat(v)
        if dtv.tzinfo is not None:
            dtv = dtv.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return dtv
    except Exception:
        return None

def zkill_link(km_id: int) -> str:
    return f"https://zkillboard.com/kill/{km_id}/"

def esi_killmail_link(km_id: int, km_hash: str) -> str:
    return f"{ESI_BASE}/killmails/{km_id}/{km_hash}/"

def victim_ship_icon_url(type_id: int) -> str:
    return f"{IMAGE_BASE}/types/{type_id}/icon?size=64"

def type_render_url(type_id: int) -> str:
    return f"{IMAGE_BASE}/types/{type_id}/render?size=512"

def isk_value(zkm: Dict[str, Any]) -> Optional[float]:
    zkb = zkm.get("zkb") or {}
    val = zkb.get("totalValue")
    try:
        return float(val) if val is not None else None
    except Exception:
        return None

def member_has_role(member: discord.Member, role_name: str) -> bool:
    return any(r.name == role_name for r in getattr(member, "roles", []))

def require_killmail_admin():
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return False
        if (
            member_has_role(interaction.user, CEO_ROLE)
            or member_has_role(interaction.user, COUNCIL_ROLE)
            or member_has_role(interaction.user, LYCAN_ROLE)
        ):
            return True
        try:
            await interaction.response.send_message(
                f"❌ You must have **{CEO_ROLE}**, **{COUNCIL_ROLE}**, or **{LYCAN_ROLE}**.",
                ephemeral=True
            )
        except Exception:
            pass
        return False
    return app_commands.check(predicate)

async def safe_defer(interaction: discord.Interaction, *, ephemeral: bool = True) -> None:
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=ephemeral)
    except Exception:
        pass

async def safe_reply(
    interaction: discord.Interaction,
    content: Optional[str] = None,
    *,
    embed: Optional[discord.Embed] = None,
    ephemeral: bool = True,
) -> None:
    try:
        if interaction.response.is_done():
            await interaction.followup.send(content=content, embed=embed, ephemeral=ephemeral)
        else:
            await interaction.response.send_message(content=content, embed=embed, ephemeral=ephemeral)
    except Exception:
        pass


class ESIHTTPError(RuntimeError):
    def __init__(self, status: int, msg: str):
        super().__init__(msg)
        self.status = status


# =====================
# COG
# =====================

class KillmailFeed(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.session: Optional[aiohttp.ClientSession] = None
        self.lock = asyncio.Lock()

        self.state = load_json(DATA_FILE)

        # posted_map: { "killmail_id": "killmail_time_iso" } (ESI time)
        self.posted_map: Dict[str, str] = self.state.get("posted_map", {}) or {}

        # scan cursor so we don't keep enriching the same first N IDs
        self.scan_cursor: int = int(self.state.get("scan_cursor", 0) or 0)

        # caches
        self.name_cache: Dict[str, str] = self.state.get("name_cache", {}) or {}
        self.system_cache: Dict[str, Dict[str, Any]] = self.state.get("system_cache", {}) or {}
        self.type_cache: Dict[str, str] = self.state.get("type_cache", {}) or {}

        # diagnostics
        self.last_poll_utc: Optional[str] = self.state.get("last_poll_utc")
        self.last_send_attempt_utc: Optional[str] = self.state.get("last_send_attempt_utc")
        self.last_posted_id: Optional[str] = self.state.get("last_posted_id")
        self.last_posted_time: Optional[str] = self.state.get("last_posted_time")
        self.last_backlog_size: int = int(self.state.get("last_backlog_size", 0) or 0)
        self.last_fetch_pages: int = int(self.state.get("last_fetch_pages", 0) or 0)
        self.last_fetch_count: int = int(self.state.get("last_fetch_count", 0) or 0)

        self.last_enriched_count: int = int(self.state.get("last_enriched_count", 0) or 0)
        self.last_esi_attempts: int = int(self.state.get("last_esi_attempts", 0) or 0)
        self.last_esi_success: int = int(self.state.get("last_esi_success", 0) or 0)
        self.last_esi_status_hist: Dict[str, int] = self.state.get("last_esi_status_hist", {}) or {}
        self.last_esi_error: Optional[str] = self.state.get("last_esi_error")

        self.consecutive_failures: int = int(self.state.get("consecutive_failures", 0) or 0)
        self.last_error: Optional[str] = self.state.get("last_error")
        self.last_send_error: Optional[str] = self.state.get("last_send_error")
        self.send_failures: int = int(self.state.get("send_failures", 0) or 0)
        self.last_channel_id: Optional[int] = self.state.get("last_channel_id")

        # de-dupe within a single run
        self._posted_this_run: set[str] = set()

        self.killmail_loop.start()

    def cog_unload(self):
        self.killmail_loop.cancel()
        if self.session and not self.session.closed:
            asyncio.create_task(self.session.close())

    async def ensure_session(self):
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=35)
            self.session = aiohttp.ClientSession(timeout=timeout)

    def target_guilds(self) -> List[discord.Guild]:
        if PRIMARY_GUILD_ID is None:
            return list(self.bot.guilds)
        g = self.bot.get_guild(PRIMARY_GUILD_ID)
        return [g] if g else []

    async def ensure_channel(self, guild: discord.Guild) -> discord.TextChannel:
        ch = discord.utils.get(guild.text_channels, name=KILLMAIL_CHANNEL_NAME)
        if ch:
            return ch

        me = guild.me
        if me is None and self.bot.user is not None:
            me = guild.get_member(self.bot.user.id)

        overwrites = {
            guild.default_role: discord.PermissionOverwrite(send_messages=False, add_reactions=False, read_messages=True),
        }
        if me is not None:
            overwrites[me] = discord.PermissionOverwrite(send_messages=True, embed_links=True, read_messages=True)

        return await guild.create_text_channel(
            KILLMAIL_CHANNEL_NAME,
            overwrites=overwrites,
            reason="Killmail feed channel"
        )

    def _check_channel_perms(self, guild: discord.Guild, channel: discord.TextChannel) -> Optional[str]:
        me = guild.me
        if me is None and self.bot.user is not None:
            me = guild.get_member(self.bot.user.id)
        if me is None:
            return "Could not resolve bot member in guild."
        perms = channel.permissions_for(me)
        missing: List[str] = []
        if not perms.view_channel:
            missing.append("view_channel")
        if not perms.send_messages:
            missing.append("send_messages")
        if not perms.embed_links:
            missing.append("embed_links")
        return f"Missing permissions: {', '.join(missing)}" if missing else None

    # -------------------------
    # zKill
    # -------------------------

    async def fetch_zkill_page(self, page: int, *, mode: str) -> List[Dict[str, Any]]:
        await self.ensure_session()
        url = f"https://zkillboard.com/api/{mode}/corporationID/{CORPORATION_ID}/page/{page}/"
        headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"}
        async with self.session.get(url, headers=headers) as resp:
            if resp.status == 429:
                raise RuntimeError("Rate limited by zKill (429).")
            if resp.status >= 400:
                txt = await resp.text()
                raise RuntimeError(f"zKill HTTP {resp.status}: {txt[:200]}")
            data = await resp.json(content_type=None)
            return data if isinstance(data, list) else []

    async def fetch_zkill_one(self, killmail_id: int) -> Optional[Dict[str, Any]]:
        await self.ensure_session()
        url = f"https://zkillboard.com/api/killID/{killmail_id}/"
        headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"}
        async with self.session.get(url, headers=headers) as resp:
            if resp.status == 429:
                raise RuntimeError("Rate limited by zKill (429).")
            if resp.status >= 400:
                txt = await resp.text()
                raise RuntimeError(f"zKill HTTP {resp.status}: {txt[:200]}")
            data = await resp.json(content_type=None)
            if isinstance(data, list) and data and isinstance(data[0], dict):
                return data[0]
            return None

    def _extract_killmail_id(self, km: Dict[str, Any]) -> Optional[int]:
        for key in ("killmail_id", "killID", "kill_id", "id"):
            v = safe_int(km.get(key))
            if v:
                return v
        return None

    def _extract_hash(self, km: Dict[str, Any]) -> Optional[str]:
        zkb = km.get("zkb") or {}
        h = zkb.get("hash")
        if isinstance(h, str) and h:
            return h
        h2 = km.get("hash")
        if isinstance(h2, str) and h2:
            return h2
        return None

    async def _fetch_mode_pages(self, mode: str) -> Tuple[List[Dict[str, Any]], int]:
        out: List[Dict[str, Any]] = []
        pages_used = 0
        for page in range(1, MAX_ZKILL_PAGES + 1):
            rows = await self.fetch_zkill_page(page, mode=mode)
            pages_used = page
            if not rows:
                break
            out.extend(rows)
            if ZKILL_REQUEST_DELAY:
                await asyncio.sleep(ZKILL_REQUEST_DELAY)
        return out, pages_used

    async def fetch_zkill_merged(self) -> List[Dict[str, Any]]:
        kills, pk = await self._fetch_mode_pages("kills")
        losses, pl = await self._fetch_mode_pages("losses")

        merged: Dict[int, Dict[str, Any]] = {}
        for km in kills:
            kmid = self._extract_killmail_id(km)
            if kmid:
                merged[kmid] = km
        for km in losses:
            kmid = self._extract_killmail_id(km)
            if kmid and kmid not in merged:
                merged[kmid] = km

        self.last_fetch_pages = max(pk, pl)
        self.last_fetch_count = len(merged)
        return list(merged.values())

    # -------------------------
    # ESI
    # -------------------------

    async def esi_get_json(self, url: str) -> Any:
        await self.ensure_session()
        headers = {"User-Agent": USER_AGENT, "Accept": "application/json", "Accept-Encoding": "gzip"}
        async with self.session.get(url, headers=headers) as resp:
            status = resp.status
            if status in (420, 429):
                txt = await resp.text()
                raise ESIHTTPError(status, f"ESI rate/err limit {status}: {txt[:200]}")
            if status >= 400:
                txt = await resp.text()
                raise ESIHTTPError(status, f"ESI HTTP {status}: {txt[:200]}")
            return await resp.json(content_type=None)

    async def esi_post_json(self, url: str, payload: Any) -> Any:
        await self.ensure_session()
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Accept-Encoding": "gzip",
        }
        async with self.session.post(url, headers=headers, json=payload) as resp:
            status = resp.status
            if status in (420, 429):
                txt = await resp.text()
                raise ESIHTTPError(status, f"ESI rate/err limit {status}: {txt[:200]}")
            if status >= 400:
                txt = await resp.text()
                raise ESIHTTPError(status, f"ESI HTTP {status}: {txt[:200]}")
            return await resp.json(content_type=None)

    async def fetch_esi_killmail(self, km_id: int, km_hash: str) -> Dict[str, Any]:
        data = await self.esi_get_json(f"{ESI_BASE}/killmails/{km_id}/{km_hash}/")
        return data if isinstance(data, dict) else {}

    async def resolve_universe_names(self, ids: List[int]) -> None:
        uniq: List[int] = []
        seen = set()
        for i in ids:
            if isinstance(i, int) and i > 0 and i not in seen:
                uniq.append(i)
                seen.add(i)

        ask = [i for i in uniq if str(i) not in self.name_cache]
        if not ask:
            return

        result = await self.esi_post_json(f"{ESI_BASE}/universe/names/", ask)
        if not isinstance(result, list):
            return

        for row in result:
            _id = str(row.get("id"))
            _name = row.get("name")
            if _id and isinstance(_name, str) and _name:
                self.name_cache[_id] = _name

        self.name_cache = clamp_dict(self.name_cache, MAX_NAME_CACHE)

    async def resolve_system_info(self, system_id: int) -> Tuple[str, Optional[float]]:
        key = str(system_id)
        cached = self.system_cache.get(key)
        if isinstance(cached, dict) and "name" in cached:
            name = cached.get("name") or "Unknown system"
            sec = cached.get("security_status")
            try:
                sec = float(sec) if sec is not None else None
            except Exception:
                sec = None
            return name, sec

        data = await self.esi_get_json(f"{ESI_BASE}/universe/systems/{system_id}/")
        if isinstance(data, dict):
            name = data.get("name") or "Unknown system"
            sec = data.get("security_status")
            try:
                sec = float(sec) if sec is not None else None
            except Exception:
                sec = None
            self.system_cache[key] = {"name": name, "security_status": sec}
            if len(self.system_cache) > MAX_SYSTEM_CACHE:
                keys = list(self.system_cache.keys())[-MAX_SYSTEM_CACHE:]
                self.system_cache = {k: self.system_cache[k] for k in keys}
            return name, sec
        return "Unknown system", None

    async def resolve_type_name(self, type_id: int) -> str:
        key = str(type_id)
        if key in self.type_cache:
            return self.type_cache[key]
        data = await self.esi_get_json(f"{ESI_BASE}/universe/types/{type_id}/")
        name = (data or {}).get("name") if isinstance(data, dict) else None
        if isinstance(name, str) and name:
            self.type_cache[key] = name
            self.type_cache = clamp_dict(self.type_cache, MAX_TYPE_CACHE)
            return name
        return "Unknown type"

    def pick_final_blow_attacker(self, esikm: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not isinstance(esikm, dict):
            return {}
        attackers = esikm.get("attackers") or []
        if not isinstance(attackers, list) or not attackers:
            return {}
        for a in attackers:
            if a.get("final_blow") is True:
                return a
        # fallback: top damage
        best = None
        best_dmg = -1
        for a in attackers:
            dmg = safe_int(a.get("damage_done")) or 0
            if dmg > best_dmg:
                best_dmg = dmg
                best = a
        return best or attackers[0]

    def classify_mail(self, esikm: Optional[Dict[str, Any]]) -> str:
        if not isinstance(esikm, dict):
            return "UNKNOWN"
        victim = esikm.get("victim") or {}
        if safe_int(victim.get("corporation_id")) == CORPORATION_ID:
            return "LOSS"
        for a in (esikm.get("attackers") or []):
            if safe_int(a.get("corporation_id")) == CORPORATION_ID:
                return "KILL"
        return "INVOLVEMENT"

    async def enrich_one(self, zkm: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[datetime.datetime]]:
        kmid = self._extract_killmail_id(zkm)
        kmhash = self._extract_hash(zkm)
        if not kmid or not kmhash:
            return None, None, None

        esikm = await self.fetch_esi_killmail(kmid, kmhash)
        ktime = parse_killmail_time(esikm.get("killmail_time"))
        if not ktime:
            return None, None, None

        # Resolve names for victim + final blow
        ids: List[int] = []
        victim = (esikm.get("victim") or {})
        for k in ("character_id", "corporation_id", "alliance_id"):
            v = safe_int(victim.get(k))
            if v:
                ids.append(v)

        fb = self.pick_final_blow_attacker(esikm)
        for k in ("character_id", "corporation_id", "alliance_id"):
            v = safe_int(fb.get(k))
            if v:
                ids.append(v)

        await self.resolve_universe_names(ids)

        sys_id = safe_int(esikm.get("solar_system_id"))
        if sys_id:
            await self.resolve_system_info(sys_id)

        ship_id = safe_int(victim.get("ship_type_id"))
        if ship_id:
            await self.resolve_type_name(ship_id)

        fb_ship_id = safe_int(fb.get("ship_type_id"))
        if fb_ship_id:
            await self.resolve_type_name(fb_ship_id)

        return zkm, esikm, ktime

    # -------------------------
    # Embed
    # -------------------------

    def build_embed(self, zkm: Dict[str, Any], esikm: Dict[str, Any]) -> discord.Embed:
        kmid = self._extract_killmail_id(zkm) or 0
        kmhash = self._extract_hash(zkm) or ""
        z_url = zkill_link(kmid) if kmid else "https://zkillboard.com/"
        e_url = esi_killmail_link(kmid, kmhash) if (kmid and kmhash) else None

        victim = esikm.get("victim") or {}
        attackers = esikm.get("attackers") or []
        n_atk = len(attackers) if isinstance(attackers, list) else 0

        v_char_id = safe_int(victim.get("character_id"))
        v_corp_id = safe_int(victim.get("corporation_id"))
        v_alliance_id = safe_int(victim.get("alliance_id"))

        v_char_name = self.name_cache.get(str(v_char_id), "Unknown") if v_char_id else "Unknown"
        v_corp_name = self.name_cache.get(str(v_corp_id), "Unknown corp") if v_corp_id else "Unknown corp"
        v_alliance_name = self.name_cache.get(str(v_alliance_id), "None") if v_alliance_id else "None"

        ship_type_id = safe_int(victim.get("ship_type_id"))
        ship_name = self.type_cache.get(str(ship_type_id), "Unknown ship") if ship_type_id else "Unknown ship"

        system_id = safe_int(esikm.get("solar_system_id"))
        system_name = "Unknown system"
        sec_status = None
        if system_id:
            cached = self.system_cache.get(str(system_id)) or {}
            system_name = cached.get("name") or "Unknown system"
            sec_status = cached.get("security_status")

        fb = self.pick_final_blow_attacker(esikm)
        fb_char_id = safe_int(fb.get("character_id"))
        fb_corp_id = safe_int(fb.get("corporation_id"))
        fb_alliance_id = safe_int(fb.get("alliance_id"))
        fb_ship_type_id = safe_int(fb.get("ship_type_id"))

        fb_char_name = self.name_cache.get(str(fb_char_id), "Unknown") if fb_char_id else "Unknown"
        fb_corp_name = self.name_cache.get(str(fb_corp_id), "Unknown corp") if fb_corp_id else "Unknown corp"
        fb_alliance_name = self.name_cache.get(str(fb_alliance_id), "None") if fb_alliance_id else "None"
        fb_ship_name = self.type_cache.get(str(fb_ship_type_id), "Unknown ship") if fb_ship_type_id else "Unknown ship"

        val = isk_value(zkm)
        ktime = esikm.get("killmail_time")
        kdt = parse_killmail_time(ktime) or utcnow()

        tag = self.classify_mail(esikm)
        if tag == "KILL":
            color = discord.Color.green()
        elif tag == "LOSS":
            color = discord.Color.red()
        elif tag == "INVOLVEMENT":
            color = discord.Color.gold()
        else:
            color = discord.Color.light_grey()

        lines = [
            f"**Type:** {tag}",
            "",
            f"**Victim:** {v_char_name}",
            f"**Victim Corp:** {v_corp_name}",
            f"**Victim Alliance:** {v_alliance_name}",
            "",
            f"**Victim Ship:** {ship_name}",
            f"**System:** {system_name} (Sec: {sec_status if sec_status is not None else 'Unknown'})",
            f"**Attackers:** {n_atk}",
            "",
            f"**Final Blow:** {fb_char_name}",
            f"**Final Blow Ship:** {fb_ship_name}",
            f"**Final Blow Corp:** {fb_corp_name}",
            f"**Final Blow Alliance:** {fb_alliance_name}",
        ]

        if val is not None:
            lines.append("")
            lines.append(f"**Estimated ISK:** {val:,.0f}")

        if ktime:
            lines.append(f"**Time:** {ktime}")

        lines.append("")
        links = f"[zKillboard]({z_url})"
        if e_url:
            links += f" • [ESI]({e_url})"
        lines.append(links)

        emb = discord.Embed(
            title=f"{tag} — Killmail #{kmid}",
            url=z_url,
            description="\n".join(lines),
            color=color,
            timestamp=kdt,
        )
        emb.set_footer(text=f"Source: zKillboard + ESI | Window: last {WINDOW_DAYS}d")

        if ship_type_id:
            emb.set_thumbnail(url=victim_ship_icon_url(ship_type_id))
            emb.set_image(url=type_render_url(ship_type_id))

        return emb

    # -------------------------
    # Persistence / posted
    # -------------------------

    def _is_posted(self, kmid: int) -> bool:
        k = str(kmid)
        return (k in self.posted_map) or (k in self._posted_this_run)

    def _mark_posted(self, kmid: int, iso_time: str) -> None:
        k = str(kmid)
        self.posted_map[k] = iso_time
        self._posted_this_run.add(k)

    def _prune_posted(self):
        cut = cutoff_utc()
        keep: Dict[str, str] = {}
        for kmid, iso in self.posted_map.items():
            dtp = parse_killmail_time(iso)
            if dtp is None or dtp >= cut:
                keep[kmid] = iso
        self.posted_map = keep

    async def persist(self):
        self._prune_posted()
        self.name_cache = clamp_dict(self.name_cache, MAX_NAME_CACHE)
        self.type_cache = clamp_dict(self.type_cache, MAX_TYPE_CACHE)

        self.state.update({
            "posted_map": self.posted_map,
            "scan_cursor": self.scan_cursor,

            "name_cache": self.name_cache,
            "system_cache": self.system_cache,
            "type_cache": self.type_cache,

            "updated_utc": utcnow_iso(),
            "last_poll_utc": self.last_poll_utc,
            "last_send_attempt_utc": self.last_send_attempt_utc,
            "last_posted_id": self.last_posted_id,
            "last_posted_time": self.last_posted_time,
            "last_backlog_size": self.last_backlog_size,
            "last_fetch_pages": self.last_fetch_pages,
            "last_fetch_count": self.last_fetch_count,

            "last_enriched_count": self.last_enriched_count,
            "last_esi_attempts": self.last_esi_attempts,
            "last_esi_success": self.last_esi_success,
            "last_esi_status_hist": self.last_esi_status_hist,
            "last_esi_error": self.last_esi_error,

            "consecutive_failures": self.consecutive_failures,
            "last_error": self.last_error,
            "last_send_error": self.last_send_error,
            "send_failures": self.send_failures,
            "last_channel_id": self.last_channel_id,
        })
        save_json(DATA_FILE, self.state)

    # -------------------------
    # Core cycle
    # -------------------------

    async def post_cycle(self) -> Tuple[int, int]:
        self.last_poll_utc = utcnow_iso()
        self.last_esi_error = None
        self.last_error = None
        self.last_send_error = None

        cut = cutoff_utc()

        # Fetch merged IDs (kills+losses). Ordering from zKill cannot be trusted; we use ESI times later.
        rows = await self.fetch_zkill_merged()

        # Build unseen list by ID
        unseen: List[Dict[str, Any]] = []
        for km in rows:
            kmid = self._extract_killmail_id(km)
            if not kmid:
                continue
            if self._is_posted(kmid):
                continue
            unseen.append(km)

        remaining_unseen = len(unseen)
        self.last_backlog_size = remaining_unseen

        if remaining_unseen == 0:
            await self.persist()
            return 0, 0

        # Use persistent scan cursor to avoid re-checking the same IDs repeatedly
        n = len(unseen)
        if n == 0:
            await self.persist()
            return 0, 0
        if self.scan_cursor >= n:
            self.scan_cursor = 0

        # Scan until we have enough enriched items or hit ESI attempt cap
        status_hist = defaultdict(int)
        esi_attempts = 0
        esi_success = 0

        enriched: List[Tuple[Dict[str, Any], Dict[str, Any], datetime.datetime]] = []

        start = self.scan_cursor
        i = 0
        while i < n and esi_attempts < MAX_ESI_ATTEMPTS_PER_CYCLE and len(enriched) < (MAX_POSTS_PER_CYCLE * 4):
            idx = (start + i) % n
            zkm = unseen[idx]
            kmid = self._extract_killmail_id(zkm)
            if not kmid or self._is_posted(kmid):
                i += 1
                continue

            kmhash = self._extract_hash(zkm)
            if not kmhash:
                status_hist["no_hash"] += 1
                i += 1
                continue

            try:
                esi_attempts += 1
                z2, e2, kdt = await self.enrich_one(zkm)
                if not z2 or not e2 or not kdt:
                    status_hist["no_time"] += 1
                    i += 1
                    continue
                esi_success += 1
                status_hist["200"] += 1

                # Window check (ESI time)
                if kdt < cut:
                    status_hist["out_of_window"] += 1
                    i += 1
                    continue

                enriched.append((z2, e2, kdt))
                i += 1

            except ESIHTTPError as ee:
                status_hist[str(ee.status)] += 1
                self.last_esi_error = str(ee)

                # If rate/err limited, abort cycle so outer backoff can kick in
                if ee.status in (420, 429):
                    break

                i += 1
            except Exception as e:
                status_hist["exception"] += 1
                self.last_esi_error = f"{type(e).__name__}: {e}"
                i += 1

        # Move cursor forward by how far we scanned, so next cycle continues
        self.scan_cursor = (start + i) % n

        self.last_esi_attempts = esi_attempts
        self.last_esi_success = esi_success
        self.last_esi_status_hist = dict(status_hist)

        # If we have nothing enriched, do not mark posted; just persist and return
        if not enriched:
            self.last_enriched_count = 0
            await self.persist()
            return 0, remaining_unseen

        # Choose NEXT 5 NEWEST by ESI time
        # sort newest->oldest for selection
        enriched.sort(key=lambda x: (x[2], self._extract_killmail_id(x[0]) or 0), reverse=True)
        selected = enriched[:MAX_POSTS_PER_CYCLE]

        # Post OLDEST->NEWEST so most recent shows last
        selected.sort(key=lambda x: (x[2], self._extract_killmail_id(x[0]) or 0))

        self.last_enriched_count = len(selected)
        self.last_send_attempt_utc = utcnow_iso()

        posted_count = 0

        for guild in self.target_guilds():
            if guild is None:
                continue

            try:
                channel = await self.ensure_channel(guild)
                self.last_channel_id = channel.id
            except Exception as e:
                self.last_send_error = f"ensure_channel:{type(e).__name__}:{e}"
                self.send_failures += 1
                continue

            perm_err = self._check_channel_perms(guild, channel)
            if perm_err:
                self.last_send_error = perm_err
                self.send_failures += 1
                continue

            for zkm, esikm, kdt in selected:
                kmid = self._extract_killmail_id(zkm) or 0
                if kmid <= 0 or self._is_posted(kmid):
                    continue
                try:
                    await channel.send(embed=self.build_embed(zkm, esikm))
                    iso_time = str(esikm.get("killmail_time") or utcnow_iso())
                    self._mark_posted(kmid, iso_time)
                    self.last_posted_id = str(kmid)
                    self.last_posted_time = iso_time
                    posted_count += 1
                except Exception as e:
                    self.last_send_error = f"send:{type(e).__name__}:{e}"
                    self.send_failures += 1

        # Approx remaining (IDs only)
        remaining = 0
        for km in unseen:
            kmid = self._extract_killmail_id(km)
            if kmid and not self._is_posted(kmid):
                remaining += 1

        self.last_backlog_size = remaining
        await self.persist()
        return posted_count, remaining

    async def run_once_and_maybe_fast_followup(self):
        posted, remaining = await self.post_cycle()
        if remaining > 0 and posted > 0:
            await asyncio.sleep(FAST_POLL_SECONDS_WHEN_BACKLOG_REMAINS)
            await self.post_cycle()

    async def run_with_backoff(self):
        async with self.lock:
            try:
                await self.run_once_and_maybe_fast_followup()
                self.consecutive_failures = 0
                self.last_error = None
                await self.persist()
            except Exception as e:
                self.consecutive_failures += 1
                self.last_error = f"{type(e).__name__}: {e}"
                await self.persist()
                await asyncio.sleep(min(300, 20 * self.consecutive_failures))

    @tasks.loop(seconds=POLL_SECONDS)
    async def killmail_loop(self):
        await self.run_with_backoff()

    @killmail_loop.before_loop
    async def before_killmail_loop(self):
        await self.bot.wait_until_ready()

    # =====================
    # SLASH COMMANDS
    # =====================

    @app_commands.command(name="killmail_status", description="Show killmail feed status.")
    async def killmail_status(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        emb = discord.Embed(
            title="Killmail Feed Status",
            description=(
                f"**Corp ID:** {CORPORATION_ID}\n"
                f"**Window:** last {WINDOW_DAYS} days (ESI killmail_time)\n"
                f"**Channel:** #{KILLMAIL_CHANNEL_NAME}\n"
                f"**Primary Guild ID:** {PRIMARY_GUILD_ID or 'None (all guilds)'}\n\n"
                f"**Poll interval:** {POLL_SECONDS}s\n"
                f"**Max posts / cycle:** {MAX_POSTS_PER_CYCLE}\n"
                f"**Follow-up if backlog remains:** {FAST_POLL_SECONDS_WHEN_BACKLOG_REMAINS}s\n\n"
                f"**zKill pages (max):** {MAX_ZKILL_PAGES}\n"
                f"**zKill request delay:** {ZKILL_REQUEST_DELAY}s\n"
                f"**Max ESI attempts / cycle:** {MAX_ESI_ATTEMPTS_PER_CYCLE}\n"
                f"**Scan cursor:** {self.scan_cursor}\n\n"
                f"**Last fetch count (merged):** {self.last_fetch_count}\n"
                f"**Last fetch pages used:** {self.last_fetch_pages}\n\n"
                f"**Last poll (UTC):** {self.last_poll_utc or 'Never'}\n"
                f"**Last send attempt (UTC):** {self.last_send_attempt_utc or 'Never'}\n"
                f"**Last channel ID:** {self.last_channel_id or 'None'}\n"
                f"**Last posted ID:** {self.last_posted_id or 'None'}\n"
                f"**Last posted time:** {self.last_posted_time or 'None'}\n\n"
                f"**Last cycle ESI attempts:** {self.last_esi_attempts}\n"
                f"**Last cycle ESI success:** {self.last_esi_success}\n"
                f"**Last cycle enriched posted-set:** {self.last_enriched_count}\n"
                f"**Backlog (unposted IDs):** {self.last_backlog_size}\n\n"
                f"**Consecutive failures:** {self.consecutive_failures}\n"
                f"**Send failures:** {self.send_failures}\n"
                f"**Last error:** {self.last_error or 'None'}\n"
                f"**Last send error:** {self.last_send_error or 'None'}\n"
                f"**Last ESI error:** {self.last_esi_error or 'None'}\n"
            ),
            timestamp=utcnow(),
        )
        emb.set_footer(text="Cryonic Gaming bot — Killmail Feed")
        await safe_reply(interaction, embed=emb, ephemeral=True)

    @app_commands.command(name="killmail_debug_esi", description="Admin only: show ESI status histogram from last cycle.")
    @require_killmail_admin()
    async def killmail_debug_esi(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)
        hist = self.last_esi_status_hist or {}
        if not hist:
            await safe_reply(interaction, "No ESI histogram recorded yet.", ephemeral=True)
            return
        lines = [f"- `{k}`: {v}" for k, v in sorted(hist.items(), key=lambda kv: (-kv[1], kv[0]))]
        msg = (
            f"**Last ESI cycle histogram**\n"
            f"Attempts: {self.last_esi_attempts} | Success: {self.last_esi_success}\n"
            f"{chr(10).join(lines)}\n\n"
            f"Last ESI error: {self.last_esi_error or 'None'}"
        )
        await safe_reply(interaction, msg, ephemeral=True)

    @app_commands.command(name="killmail_inspect", description="Admin only: inspect zKill row structure.")
    @require_killmail_admin()
    async def killmail_inspect(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        rows = await self.fetch_zkill_merged()

        missing_id = 0
        missing_time = 0
        missing_hash = 0

        sample = rows[0] if rows else None
        for km in rows:
            kmid = self._extract_killmail_id(km)
            if not kmid:
                missing_id += 1
            if km.get("killmail_time") is None:
                missing_time += 1
            if not self._extract_hash(km):
                missing_hash += 1

        if not sample:
            await safe_reply(interaction, "No zKill rows returned.", ephemeral=True)
            return

        keys = sorted(list(sample.keys()))
        kmid = self._extract_killmail_id(sample)
        ktime = sample.get("killmail_time")
        zkb = sample.get("zkb") or {}
        h = self._extract_hash(sample)
        tv = isk_value(sample)

        msg = (
            f"**zKill structure check:**\n"
            f"- rows fetched (merged): {len(rows)}\n"
            f"- missing id: {missing_id}\n"
            f"- missing time: {missing_time}\n"
            f"- missing hash: {missing_hash}\n\n"
            f"**Sample row:**\n"
            f"- keys: {', '.join(keys)}\n"
            f"- extracted killmail_id: {kmid}\n"
            f"- killmail_time: {ktime} (type={type(ktime).__name__})\n"
            f"- zkb keys: {', '.join(sorted(list(zkb.keys())))}\n"
            f"- zkb.hash: {h}\n"
            f"- zkb.totalValue: {tv}\n"
        )
        await safe_reply(interaction, msg, ephemeral=True)

    @app_commands.command(name="killmail_debug_next", description="Admin only: run one post cycle immediately.")
    @require_killmail_admin()
    async def killmail_debug_next(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)
        posted, remaining = await self.post_cycle()
        await safe_reply(
            interaction,
            (
                f"post_cycle done. posted={posted} remaining={remaining}\n"
                f"esi_attempts={self.last_esi_attempts} esi_success={self.last_esi_success}\n"
                f"scan_cursor={self.scan_cursor}\n"
                f"last_send_error={self.last_send_error or 'None'} last_esi_error={self.last_esi_error or 'None'}"
            ),
            ephemeral=True
        )

    @app_commands.command(name="killmail_reload", description="Reload a killmail by kill ID and repost it (admin only).")
    @require_killmail_admin()
    async def killmail_reload(self, interaction: discord.Interaction, killmail_id: int):
        await safe_defer(interaction, ephemeral=True)
        kmid = safe_int(killmail_id)
        if not kmid:
            await safe_reply(interaction, "❌ Invalid killmail_id.", ephemeral=True)
            return

        zkm = await self.fetch_zkill_one(kmid)
        if not zkm:
            await safe_reply(interaction, f"❌ No zKill data for killmail_id={kmid}.", ephemeral=True)
            return

        kmhash = self._extract_hash(zkm)
        if not kmhash:
            await safe_reply(interaction, "❌ Missing zkb.hash; cannot fetch ESI killmail.", ephemeral=True)
            return

        try:
            esikm = await self.fetch_esi_killmail(kmid, kmhash)
        except Exception as e:
            await safe_reply(interaction, f"❌ ESI fetch failed: {type(e).__name__}: {e}", ephemeral=True)
            return

        if not isinstance(esikm, dict) or not esikm.get("killmail_time"):
            await safe_reply(interaction, "❌ ESI did not return a valid killmail.", ephemeral=True)
            return

        # Warm caches for display
        try:
            victim = (esikm.get("victim") or {})
            ids: List[int] = []
            for k in ("character_id", "corporation_id", "alliance_id"):
                v = safe_int(victim.get(k))
                if v:
                    ids.append(v)
            fb = self.pick_final_blow_attacker(esikm)
            for k in ("character_id", "corporation_id", "alliance_id"):
                v = safe_int(fb.get(k))
                if v:
                    ids.append(v)
            await self.resolve_universe_names(ids)

            sys_id = safe_int(esikm.get("solar_system_id"))
            if sys_id:
                await self.resolve_system_info(sys_id)

            ship_id = safe_int(victim.get("ship_type_id"))
            if ship_id:
                await self.resolve_type_name(ship_id)

            fb_ship_id = safe_int(fb.get("ship_type_id"))
            if fb_ship_id:
                await self.resolve_type_name(fb_ship_id)
        except Exception:
            pass

        if not interaction.guild:
            await safe_reply(interaction, "❌ Must be used in a server.", ephemeral=True)
            return

        ch = await self.ensure_channel(interaction.guild)
        perm_err = self._check_channel_perms(interaction.guild, ch)
        if perm_err:
            await safe_reply(interaction, f"❌ {perm_err}", ephemeral=True)
            return

        await ch.send(embed=self.build_embed(zkm, esikm))

        iso_time = str(esikm.get("killmail_time") or utcnow_iso())
        self._mark_posted(kmid, iso_time)
        self.last_posted_id = str(kmid)
        self.last_posted_time = iso_time
        await self.persist()

        await safe_reply(interaction, f"✅ Reloaded and reposted killmail {kmid}.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(KillmailFeed(bot))
