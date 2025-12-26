# cogs/killmail_feed.py
#
# Post ALL kills/losses in chronological order (oldest -> newest),
# without reposts, using ESI killmail_time as authoritative.
#
# Key design:
# - Fetch ALL pages from zKill (kills + losses) until empty.
# - Maintain a persistent killmail_time cache (km_time_cache) keyed by killmail_id,
#   so we can sort globally across cycles without re-fetching the same killmails.
# - Enrich (ESI fetch) in deterministic oldest-first order (by killmail_id ascending).
# - Post up to MAX_POSTS_PER_CYCLE, oldest->newest by cached ESI time.
#
# Debug:
# - /killmail_status
# - /killmail_debug_next
# - /killmail_debug_esi
# - /killmail_inspect
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

# Per your request
MAX_POSTS_PER_CYCLE = 15

# Fetch zKill pages until empty. Guard prevents runaway if upstream misbehaves.
HARD_MAX_PAGES: Optional[int] = 2000
ZKILL_REQUEST_DELAY = 0.25

# ESI protection. Increase to catch up faster.
MAX_ESI_ATTEMPTS_PER_CYCLE = 300

USER_AGENT = "Cryonic Gaming bot/1.0 (contact: tendeuse on Discord)"
ESI_BASE = "https://esi.evetech.net/latest"
IMAGE_BASE = "https://images.evetech.net"

DATA_FILE = Path("data/killmail_feed.json")
DATA_FILE.parent.mkdir(parents=True, exist_ok=True)

MAX_NAME_CACHE = 10000
MAX_SYSTEM_CACHE = 5000
MAX_TYPE_CACHE = 10000
MAX_KM_TIME_CACHE = 200000  # safety bound on cached times

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

        # posted_map: { "killmail_id": "killmail_time_iso" }
        self.posted_map: Dict[str, str] = self.state.get("posted_map", {}) or {}

        # killmail_time cache: { "killmail_id": "killmail_time_iso" } from ESI
        self.km_time_cache: Dict[str, str] = self.state.get("km_time_cache", {}) or {}

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
    # zKill (page until empty)
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

    async def fetch_zkill_all_merged(self) -> List[Dict[str, Any]]:
        merged: Dict[int, Dict[str, Any]] = {}
        pages_used = 0

        async def ingest(mode: str, page: int) -> int:
            rows = await self.fetch_zkill_page(page, mode=mode)
            if not rows:
                return 0
            for km in rows:
                kmid = self._extract_killmail_id(km)
                if kmid and kmid not in merged:
                    merged[kmid] = km
            return len(rows)

        page = 1
        while True:
            if HARD_MAX_PAGES is not None and page > HARD_MAX_PAGES:
                break

            kc = await ingest("kills", page)
            lc = await ingest("losses", page)
            pages_used = page

            if kc == 0 and lc == 0:
                break

            if ZKILL_REQUEST_DELAY:
                await asyncio.sleep(ZKILL_REQUEST_DELAY)

            page += 1

        self.last_fetch_pages = pages_used
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

        # Cache time for global ordering
        self.km_time_cache[str(kmid)] = str(esikm.get("killmail_time"))

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
        emb.set_footer(text="Source: zKillboard + ESI | Ordering: oldest→newest (ESI time)")

        if ship_type_id:
            emb.set_thumbnail(url=victim_ship_icon_url(ship_type_id))
            emb.set_image(url=type_render_url(ship_type_id))

        return emb

    # -------------------------
    # Dedup / persistence
    # -------------------------

    def _is_posted(self, kmid: int) -> bool:
        k = str(kmid)
        return (k in self.posted_map) or (k in self._posted_this_run)

    def _mark_posted(self, kmid: int, iso_time: str) -> None:
        k = str(kmid)
        self.posted_map[k] = iso_time
        self._posted_this_run.add(k)

    def _prune_time_cache(self) -> None:
        # Keep cache bounded (most recent entries)
        self.km_time_cache = clamp_dict(self.km_time_cache, MAX_KM_TIME_CACHE)

    async def persist(self):
        self.name_cache = clamp_dict(self.name_cache, MAX_NAME_CACHE)
        self.type_cache = clamp_dict(self.type_cache, MAX_TYPE_CACHE)
        self._prune_time_cache()

        self.state.update({
            "posted_map": self.posted_map,
            "km_time_cache": self.km_time_cache,

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

        rows = await self.fetch_zkill_all_merged()

        # Build unposted set
        unposted: List[Tuple[int, Dict[str, Any]]] = []
        for km in rows:
            kmid = self._extract_killmail_id(km)
            if not kmid:
                continue
            if self._is_posted(kmid):
                continue
            unposted.append((kmid, km))

        # Deterministic oldest-first scan by killmail_id
        unposted.sort(key=lambda t: t[0])
        self.last_backlog_size = len(unposted)

        if not unposted:
            await self.persist()
            return 0, 0

        status_hist = defaultdict(int)
        esi_attempts = 0
        esi_success = 0

        # 1) Fill time cache for oldest unposted IDs first (bounded)
        for kmid, km in unposted:
            if esi_attempts >= MAX_ESI_ATTEMPTS_PER_CYCLE:
                break
            # Skip if we already have time cached
            cached_iso = self.km_time_cache.get(str(kmid))
            if cached_iso and parse_killmail_time(cached_iso):
                continue

            kmhash = self._extract_hash(km)
            if not kmhash:
                status_hist["no_hash"] += 1
                continue

            try:
                esi_attempts += 1
                z2, e2, kdt = await self.enrich_one(km)
                if not z2 or not e2 or not kdt:
                    status_hist["no_time"] += 1
                    continue
                esi_success += 1
                status_hist["200"] += 1
            except ESIHTTPError as ee:
                status_hist[str(ee.status)] += 1
                self.last_esi_error = str(ee)
                if ee.status in (420, 429):
                    break
            except Exception as e:
                status_hist["exception"] += 1
                self.last_esi_error = f"{type(e).__name__}: {e}"

        self.last_esi_attempts = esi_attempts
        self.last_esi_success = esi_success
        self.last_esi_status_hist = dict(status_hist)

        # 2) From all unposted, select those with known times and pick global oldest 15
        timed: List[Tuple[datetime.datetime, int, Dict[str, Any]]] = []
        for kmid, km in unposted:
            iso = self.km_time_cache.get(str(kmid))
            dt = parse_killmail_time(iso) if iso else None
            if dt is None:
                continue
            timed.append((dt, kmid, km))

        timed.sort(key=lambda t: (t[0], t[1]))
        selected = timed[:MAX_POSTS_PER_CYCLE]
        self.last_enriched_count = len(selected)

        if not selected:
            # We have unposted IDs but none have time cached yet (or all cache entries invalid)
            await self.persist()
            return 0, self.last_backlog_size

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

            # Post strictly oldest->newest
            for dt, kmid, km in selected:
                if self._is_posted(kmid):
                    continue

                kmhash = self._extract_hash(km)
                if not kmhash:
                    continue

                try:
                    esikm = await self.fetch_esi_killmail(kmid, kmhash)
                    if not isinstance(esikm, dict) or not esikm.get("killmail_time"):
                        continue

                    await channel.send(embed=self.build_embed(km, esikm))

                    iso_time = str(esikm.get("killmail_time") or utcnow_iso())
                    self.km_time_cache[str(kmid)] = iso_time
                    self._mark_posted(kmid, iso_time)

                    self.last_posted_id = str(kmid)
                    self.last_posted_time = iso_time
                    posted_count += 1

                except Exception as e:
                    self.last_send_error = f"send:{type(e).__name__}:{e}"
                    self.send_failures += 1

        # Recompute remaining
        remaining = 0
        for kmid, _ in unposted:
            if not self._is_posted(kmid):
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
                f"**Channel:** #{KILLMAIL_CHANNEL_NAME}\n"
                f"**Ordering:** Oldest → Newest (ESI killmail_time)\n"
                f"**Max posts / cycle:** {MAX_POSTS_PER_CYCLE}\n\n"
                f"**zKill paging:** until empty\n"
                f"**Safety page guard:** {HARD_MAX_PAGES if HARD_MAX_PAGES is not None else 'None (disabled)'}\n"
                f"**zKill request delay:** {ZKILL_REQUEST_DELAY}s\n"
                f"**Max ESI attempts / cycle:** {MAX_ESI_ATTEMPTS_PER_CYCLE}\n\n"
                f"**Last fetch count (merged):** {self.last_fetch_count}\n"
                f"**Last fetch pages used:** {self.last_fetch_pages}\n\n"
                f"**Last poll (UTC):** {self.last_poll_utc or 'Never'}\n"
                f"**Last send attempt (UTC):** {self.last_send_attempt_utc or 'Never'}\n"
                f"**Last posted ID:** {self.last_posted_id or 'None'}\n"
                f"**Last posted time:** {self.last_posted_time or 'None'}\n\n"
                f"**Last cycle ESI attempts:** {self.last_esi_attempts}\n"
                f"**Last cycle ESI success:** {self.last_esi_success}\n"
                f"**Last cycle selected-to-post:** {self.last_enriched_count}\n"
                f"**Backlog (unposted IDs):** {self.last_backlog_size}\n\n"
                f"**Send failures:** {self.send_failures}\n"
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
                f"last_send_error={self.last_send_error or 'None'} last_esi_error={self.last_esi_error or 'None'}"
            ),
            ephemeral=True
        )

    @app_commands.command(name="killmail_inspect", description="Admin only: inspect zKill row structure.")
    @require_killmail_admin()
    async def killmail_inspect(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)
        rows = await self.fetch_zkill_all_merged()
        if not rows:
            await safe_reply(interaction, "No zKill rows returned.", ephemeral=True)
            return
        sample = rows[0]
        zkb = sample.get("zkb") or {}
        msg = (
            f"Sample keys: {', '.join(sorted(sample.keys()))}\n"
            f"killmail_id: {self._extract_killmail_id(sample)}\n"
            f"killmail_time field: {sample.get('killmail_time')} (type={type(sample.get('killmail_time')).__name__})\n"
            f"zkb keys: {', '.join(sorted(zkb.keys()))}\n"
            f"zkb.hash: {self._extract_hash(sample)}\n"
        )
        await safe_reply(interaction, msg, ephemeral=True)

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
        self.km_time_cache[str(kmid)] = iso_time
        self._mark_posted(kmid, iso_time)
        self.last_posted_id = str(kmid)
        self.last_posted_time = iso_time

        await self.persist()
        await safe_reply(interaction, f"✅ Reloaded and reposted killmail {kmid}.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(KillmailFeed(bot))
