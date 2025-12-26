# cogs/killmail_feed.py
#
# Full copy/paste version (with NEWEST batch selection + chronological posting)
# - Always selects the next 5 NEWEST unseen rows
# - Posts them OLDEST -> NEWEST so the most recent is posted last
# - 5 posts every 120s + one follow-up after 60s if backlog remains
# - Last 14 days window
# - Merges zKill kills + losses
# - Best-effort ESI enrichment; if enrichment fails, posts a minimal zKill-only embed (prevents deadlock)
# - Includes /killmail_inspect and /killmail_debug_next for debugging
# - /killmail_reload restricted to: ARC Security Administration Council, ARC Security Corporation Leader, Lycan King
# - Uses your User-Agent string

import discord
from discord.ext import commands, tasks
from discord import app_commands
from pathlib import Path
import aiohttp
import asyncio
import json
import datetime
from typing import Dict, Any, List, Optional, Tuple

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
    if " " in v and "T" not in v:
        v = v.replace(" ", "T")
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


# =====================
# COG
# =====================

class KillmailFeed(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.session: Optional[aiohttp.ClientSession] = None
        self.lock = asyncio.Lock()

        self.state = load_json(DATA_FILE)
        self.posted_map: Dict[str, str] = self.state.get("posted_map", {}) or {}

        self.name_cache: Dict[str, str] = self.state.get("name_cache", {}) or {}
        self.system_cache: Dict[str, Dict[str, Any]] = self.state.get("system_cache", {}) or {}
        self.type_cache: Dict[str, str] = self.state.get("type_cache", {}) or {}

        # diagnostics
        self.last_poll_utc: Optional[str] = self.state.get("last_poll_utc")
        self.last_posted_id: Optional[str] = self.state.get("last_posted_id")
        self.last_posted_time: Optional[str] = self.state.get("last_posted_time")
        self.last_backlog_size: int = int(self.state.get("last_backlog_size", 0) or 0)
        self.last_fetch_pages: int = int(self.state.get("last_fetch_pages", 0) or 0)
        self.last_fetch_count: int = int(self.state.get("last_fetch_count", 0) or 0)
        self.consecutive_failures: int = int(self.state.get("consecutive_failures", 0) or 0)
        self.last_error: Optional[str] = self.state.get("last_error")
        self.last_send_error: Optional[str] = self.state.get("last_send_error")
        self.last_send_attempt_utc: Optional[str] = self.state.get("last_send_attempt_utc")
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
    # zKill (kills + losses)
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

    async def _fetch_mode_window(self, mode: str) -> Tuple[List[Dict[str, Any]], int]:
        cut = cutoff_utc()
        out: List[Dict[str, Any]] = []
        pages_used = 0

        for page in range(1, MAX_ZKILL_PAGES + 1):
            rows = await self.fetch_zkill_page(page, mode=mode)
            pages_used = page
            if not rows:
                break

            out.extend(rows)

            times = [parse_killmail_time(r.get("killmail_time")) for r in rows]
            times = [t for t in times if t is not None]
            if times and min(times) < cut:
                break

            if ZKILL_REQUEST_DELAY:
                await asyncio.sleep(ZKILL_REQUEST_DELAY)

        filtered: List[Dict[str, Any]] = []
        for km in out:
            t = parse_killmail_time(km.get("killmail_time"))
            if t is None or t >= cut:
                filtered.append(km)

        return filtered, pages_used

    async def fetch_zkill_last_window(self) -> List[Dict[str, Any]]:
        kills, pk = await self._fetch_mode_window("kills")
        losses, pl = await self._fetch_mode_window("losses")

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
            if resp.status == 429:
                raise RuntimeError("ESI rate limited (429).")
            if resp.status >= 400:
                txt = await resp.text()
                raise RuntimeError(f"ESI HTTP {resp.status}: {txt[:200]}")
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
            if resp.status == 429:
                raise RuntimeError("ESI rate limited (429).")
            if resp.status >= 400:
                txt = await resp.text()
                raise RuntimeError(f"ESI HTTP {resp.status}: {txt[:200]}")
            return await resp.json(content_type=None)

    async def fetch_esi_killmail(self, km_id: int, km_hash: str) -> Dict[str, Any]:
        url = f"{ESI_BASE}/killmails/{km_id}/{km_hash}/"
        data = await self.esi_get_json(url)
        return data if isinstance(data, dict) else {}

    async def resolve_universe_names(self, ids: List[int]) -> None:
        uniq = []
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
        return attackers[0]

    def classify_mail(self, esikm: Optional[Dict[str, Any]]) -> str:
        if not isinstance(esikm, dict):
            return "UNKNOWN"
        victim = esikm.get("victim") or {}
        if safe_int(victim.get("corporation_id")) == CORPORATION_ID:
            return "LOSS"
        fb = self.pick_final_blow_attacker(esikm)
        if safe_int(fb.get("corporation_id")) == CORPORATION_ID:
            return "KILL"
        for a in (esikm.get("attackers") or []):
            if safe_int(a.get("corporation_id")) == CORPORATION_ID:
                return "INVOLVEMENT"
        return "UNKNOWN"

    async def enrich_one(self, zkm: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        kmid = self._extract_killmail_id(zkm)
        kmhash = self._extract_hash(zkm)
        if not kmid or not kmhash:
            return zkm, None

        try:
            esikm = await self.fetch_esi_killmail(kmid, kmhash)

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

            return zkm, esikm
        except Exception:
            return zkm, None

    def build_embed_full_or_minimal(self, zkm: Dict[str, Any], esikm: Optional[Dict[str, Any]]) -> discord.Embed:
        kmid = self._extract_killmail_id(zkm) or 0
        kmhash = self._extract_hash(zkm)

        z_url = zkill_link(kmid) if kmid else "https://zkillboard.com/"
        e_url = esi_killmail_link(kmid, kmhash) if (kmid and kmhash) else None

        val = isk_value(zkm)
        z_time = zkm.get("killmail_time")

        # Minimal fallback (no ESI)
        if not isinstance(esikm, dict):
            desc = [
                "**Note:** ESI enrichment unavailable (missing hash or ESI fetch failed).",
                f"**Killmail ID:** {kmid}",
            ]
            if z_time:
                desc.append(f"**Time:** {z_time}")
            if val is not None:
                desc.append(f"**Estimated ISK:** {val:,.0f}")
            desc.append("")
            desc.append(f"[zKillboard]({z_url})")
            if e_url:
                desc.append(f"[ESI]({e_url})")

            emb = discord.Embed(
                title=f"UNENRICHED — Killmail #{kmid}",
                url=z_url,
                description="\n".join(desc),
                color=discord.Color.blurple(),
                timestamp=parse_killmail_time(z_time) or utcnow(),
            )
            emb.set_footer(text=f"Source: zKillboard | Window: last {WINDOW_DAYS}d")
            return emb

        # Full embed (ESI)
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

        tag = self.classify_mail(esikm)
        color = discord.Color.green() if tag == "KILL" else discord.Color.red() if tag == "LOSS" else discord.Color.gold() if tag == "INVOLVEMENT" else discord.Color.blurple()

        ktime = esikm.get("killmail_time") or z_time
        kdt = parse_killmail_time(ktime) or utcnow()

        lines = [
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
    # State / de-dupe
    # -------------------------

    def _is_posted(self, kmid: int) -> bool:
        k = str(kmid)
        return k in self.posted_map or k in self._posted_this_run

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
            "name_cache": self.name_cache,
            "system_cache": self.system_cache,
            "type_cache": self.type_cache,

            "updated_utc": utcnow_iso(),
            "last_poll_utc": self.last_poll_utc,
            "last_posted_id": self.last_posted_id,
            "last_posted_time": self.last_posted_time,
            "last_backlog_size": self.last_backlog_size,
            "last_fetch_pages": self.last_fetch_pages,
            "last_fetch_count": self.last_fetch_count,
            "consecutive_failures": self.consecutive_failures,
            "last_error": self.last_error,
            "last_send_error": self.last_send_error,
            "last_send_attempt_utc": self.last_send_attempt_utc,
            "send_failures": self.send_failures,
            "last_channel_id": self.last_channel_id,
        })
        save_json(DATA_FILE, self.state)

    # -------------------------
    # Core loop
    # Selection: next 5 NEWEST unseen
    # Posting order: OLDEST -> NEWEST (so newest is posted last)
    # -------------------------

    def _ztime_key(self, km: Dict[str, Any]) -> Tuple[datetime.datetime, int]:
        t = parse_killmail_time(km.get("killmail_time")) or datetime.datetime.max
        kmid = self._extract_killmail_id(km) or 0
        return (t, kmid)

    async def post_cycle(self) -> Tuple[int, int]:
        self.last_poll_utc = utcnow_iso()
        cut = cutoff_utc()

        rows = await self.fetch_zkill_last_window()

        unseen: List[Dict[str, Any]] = []
        for km in rows:
            kmid = self._extract_killmail_id(km)
            if not kmid:
                continue
            if self._is_posted(kmid):
                continue
            t = parse_killmail_time(km.get("killmail_time"))
            if t is not None and t < cut:
                continue
            unseen.append(km)

        backlog = len(unseen)
        self.last_backlog_size = backlog
        if backlog == 0:
            await self.persist()
            return 0, 0

        # NEWEST batch selection:
        unseen.sort(key=self._ztime_key, reverse=True)                  # newest -> oldest
        to_post = list(reversed(unseen[:MAX_POSTS_PER_CYCLE]))          # post oldest -> newest within that newest batch

        posted_count = 0
        self.last_send_attempt_utc = utcnow_iso()

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

            for zkm in to_post:
                kmid = self._extract_killmail_id(zkm) or 0
                if kmid <= 0 or self._is_posted(kmid):
                    continue

                # Enrich best-effort, but do not block posting
                zkm2, esikm = await self.enrich_one(zkm)

                try:
                    await channel.send(embed=self.build_embed_full_or_minimal(zkm2, esikm))
                    best_time_iso = (esikm or {}).get("killmail_time") or zkm2.get("killmail_time") or utcnow_iso()
                    self._mark_posted(kmid, str(best_time_iso))
                    self.last_posted_id = str(kmid)
                    self.last_posted_time = str(best_time_iso)
                    posted_count += 1
                    self.last_send_error = None
                except Exception as e:
                    self.last_send_error = f"send:{type(e).__name__}:{e}"
                    self.send_failures += 1

        await self.persist()

        remaining = 0
        for km in unseen:
            kmid = self._extract_killmail_id(km)
            if kmid and not self._is_posted(kmid):
                remaining += 1

        self.last_backlog_size = remaining
        await self.persist()
        return posted_count, remaining

    async def run_once_and_maybe_fast_followup(self):
        async with self.lock:
            _, remaining = await self.post_cycle()

        if remaining > 0:
            await asyncio.sleep(FAST_POLL_SECONDS_WHEN_BACKLOG_REMAINS)
            async with self.lock:
                await self.post_cycle()

    async def run_with_backoff(self):
        try:
            await self.run_once_and_maybe_fast_followup()
            self.consecutive_failures = 0
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
    # COMMANDS
    # =====================

    @app_commands.command(name="killmail_status", description="Show killmail feed status.")
    async def killmail_status(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)
        emb = discord.Embed(
            title="Killmail Feed Status",
            description=(
                f"**Corp ID:** {CORPORATION_ID}\n"
                f"**Window:** last {WINDOW_DAYS} days\n"
                f"**Channel:** #{KILLMAIL_CHANNEL_NAME}\n"
                f"**Primary Guild ID:** {PRIMARY_GUILD_ID or 'None (all guilds)'}\n\n"
                f"**Poll interval:** {POLL_SECONDS}s\n"
                f"**Max posts / cycle:** {MAX_POSTS_PER_CYCLE}\n"
                f"**Follow-up if backlog remains:** {FAST_POLL_SECONDS_WHEN_BACKLOG_REMAINS}s\n\n"
                f"**zKill pages (max):** {MAX_ZKILL_PAGES}\n"
                f"**zKill request delay:** {ZKILL_REQUEST_DELAY}s\n\n"
                f"**Last fetch count (merged):** {self.last_fetch_count}\n"
                f"**Last fetch pages used:** {self.last_fetch_pages}\n"
                f"**Last poll (UTC):** {self.last_poll_utc or 'Never'}\n"
                f"**Last send attempt (UTC):** {self.last_send_attempt_utc or 'Never'}\n"
                f"**Last channel ID:** {self.last_channel_id or 'None'}\n"
                f"**Last posted ID:** {self.last_posted_id or 'None'}\n"
                f"**Backlog (unposted in window):** {self.last_backlog_size}\n"
                f"**Consecutive failures:** {self.consecutive_failures}\n"
                f"**Send failures:** {self.send_failures}\n"
                f"**Last error:** {self.last_error or 'None'}\n"
                f"**Last send error:** {self.last_send_error or 'None'}\n"
            ),
            timestamp=utcnow(),
        )
        emb.set_footer(text="Cryonic Gaming bot — Killmail Feed")
        await safe_reply(interaction, embed=emb, ephemeral=True)

    @app_commands.command(name="killmail_inspect", description="Admin only: inspect one raw zKill row (keys/id/time/hash).")
    @require_killmail_admin()
    async def killmail_inspect(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)
        rows = await self.fetch_zkill_last_window()
        cut = cutoff_utc()

        missing_id = 0
        missing_time = 0
        missing_hash = 0
        in_window = 0

        sample: Optional[Dict[str, Any]] = None
        for km in rows:
            kmid = self._extract_killmail_id(km)
            if not kmid:
                missing_id += 1
                continue

            t = parse_killmail_time(km.get("killmail_time"))
            if t is None:
                missing_time += 1
                in_window += 1
            else:
                if t < cut:
                    continue
                in_window += 1

            if not self._extract_hash(km):
                missing_hash += 1

            if sample is None and not self._is_posted(kmid):
                sample = km

        if sample is None and rows:
            sample = rows[0]

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
            f"**Window summary (last {WINDOW_DAYS}d logic):**\n"
            f"- rows fetched (merged): {len(rows)}\n"
            f"- rows counted in-window: {in_window}\n"
            f"- missing id: {missing_id}\n"
            f"- missing time: {missing_time}\n"
            f"- missing hash: {missing_hash}\n\n"
            f"**Sample row:**\n"
            f"- keys: {', '.join(keys[:40])}{' …' if len(keys) > 40 else ''}\n"
            f"- extracted killmail_id: {kmid}\n"
            f"- killmail_time: {ktime} (type={type(ktime).__name__})\n"
            f"- zkb keys: {', '.join(sorted(list(zkb.keys()))[:30])}\n"
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
            f"post_cycle done. posted={posted} remaining={remaining} last_send_error={self.last_send_error or 'None'}",
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

        zkm, esikm = await self.enrich_one(zkm)

        if not interaction.guild:
            await safe_reply(interaction, "❌ Must be used in a server.", ephemeral=True)
            return
        ch = await self.ensure_channel(interaction.guild)
        perm_err = self._check_channel_perms(interaction.guild, ch)
        if perm_err:
            await safe_reply(interaction, f"❌ {perm_err}", ephemeral=True)
            return

        await ch.send(embed=self.build_embed_full_or_minimal(zkm, esikm))
        best_time_iso = (esikm or {}).get("killmail_time") or zkm.get("killmail_time") or utcnow_iso()
        self._mark_posted(kmid, str(best_time_iso))
        self.last_posted_id = str(kmid)
        self.last_posted_time = str(best_time_iso)
        await self.persist()
        await safe_reply(interaction, f"✅ Reloaded and reposted killmail {kmid}.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(KillmailFeed(bot))
