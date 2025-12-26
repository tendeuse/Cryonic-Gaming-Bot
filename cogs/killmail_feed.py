# cogs/killmail_feed.py
#
# FIX: feed running but posting nothing
# - Adds selection diagnostics shown in /killmail_status
# - Adds fail-safe: if enrichment yields 0 items, post next 5 unseen using zKill-only (ESI=None)
# - Keeps strict chronological order (oldest -> newest; most recent LAST)
# - 5 posts per 120s + one follow-up after 60s if backlog remains
# - Kills + losses merged
# - Hard cutoff: last 14 days
# - /killmail_reload restricted + /killmail_debug_next restricted

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

CANDIDATE_ENRICH_LIMIT = 60

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

def get_killmail_id(km: Dict[str, Any]) -> Optional[int]:
    return safe_int(km.get("killmail_id"))

def get_killmail_hash(km: Dict[str, Any]) -> Optional[str]:
    zkb = km.get("zkb") or {}
    h = zkb.get("hash")
    if isinstance(h, str) and h:
        return h
    h2 = km.get("hash")
    if isinstance(h2, str) and h2:
        return h2
    return None

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

def format_sec_status(sec: Optional[float]) -> str:
    if sec is None:
        return "Unknown"
    return f"{sec:.1f}"

def isk_value(zkm: Dict[str, Any]) -> Optional[float]:
    zkb = zkm.get("zkb") or {}
    val = zkb.get("totalValue")
    try:
        return float(val) if val is not None else None
    except Exception:
        return None

def member_has_role(member: discord.Member, role_name: str) -> bool:
    return any(r.name == role_name for r in getattr(member, "roles", []))

def require_ceo():
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return False
        if member_has_role(interaction.user, CEO_ROLE):
            return True
        try:
            await interaction.response.send_message(f"❌ You must have the **{CEO_ROLE}** role.", ephemeral=True)
        except Exception:
            pass
        return False
    return app_commands.check(predicate)

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

        # Metrics / diagnostics
        self.consecutive_failures: int = int(self.state.get("consecutive_failures", 0) or 0)
        self.last_poll_utc: Optional[str] = self.state.get("last_poll_utc")
        self.last_posted_id: Optional[str] = self.state.get("last_posted_id")
        self.last_posted_time: Optional[str] = self.state.get("last_posted_time")
        self.last_backlog_size: int = int(self.state.get("last_backlog_size", 0) or 0)
        self.last_fetch_pages: int = int(self.state.get("last_fetch_pages", 0) or 0)
        self.last_fetch_count: int = int(self.state.get("last_fetch_count", 0) or 0)

        self.last_error: Optional[str] = self.state.get("last_error")
        self.last_send_error: Optional[str] = self.state.get("last_send_error")
        self.last_send_attempt_utc: Optional[str] = self.state.get("last_send_attempt_utc")
        self.send_failures: int = int(self.state.get("send_failures", 0) or 0)
        self.last_channel_id: Optional[int] = self.state.get("last_channel_id")

        # NEW: selector diagnostics
        self.last_unseen_count: int = int(self.state.get("last_unseen_count", 0) or 0)
        self.last_candidate_count: int = int(self.state.get("last_candidate_count", 0) or 0)
        self.last_enriched_count: int = int(self.state.get("last_enriched_count", 0) or 0)
        self.last_to_post_count: int = int(self.state.get("last_to_post_count", 0) or 0)

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
            return "Could not resolve bot member in guild (guild.me is None)."

        perms = channel.permissions_for(me)
        missing: List[str] = []
        if not perms.view_channel:
            missing.append("view_channel")
        if not perms.send_messages:
            missing.append("send_messages")
        if not perms.embed_links:
            missing.append("embed_links")

        if missing:
            return f"Missing permissions in #{channel.name}: {', '.join(missing)}"
        return None

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

    async def _fetch_zkill_last_window_for_mode(self, mode: str) -> Tuple[List[Dict[str, Any]], int]:
        cut = cutoff_utc()
        out: List[Dict[str, Any]] = []
        pages_used = 0

        for page in range(1, MAX_ZKILL_PAGES + 1):
            rows = await self.fetch_zkill_page(page, mode=mode)
            pages_used = page
            if not rows:
                break

            out.extend(rows)

            times = [parse_killmail_time(km.get("killmail_time")) for km in rows]
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
        kills: List[Dict[str, Any]] = []
        losses: List[Dict[str, Any]] = []
        pages_k = 0
        pages_l = 0
        err_parts: List[str] = []

        try:
            kills, pages_k = await self._fetch_zkill_last_window_for_mode("kills")
        except Exception as e:
            err_parts.append(f"kills:{type(e).__name__}:{e}")

        try:
            losses, pages_l = await self._fetch_zkill_last_window_for_mode("losses")
        except Exception as e:
            err_parts.append(f"losses:{type(e).__name__}:{e}")

        self.last_error = " | ".join(err_parts) if err_parts else None

        merged: Dict[int, Dict[str, Any]] = {}
        for km in kills:
            kmid = get_killmail_id(km)
            if kmid:
                merged[kmid] = km
        for km in losses:
            kmid = get_killmail_id(km)
            if kmid and kmid not in merged:
                merged[kmid] = km

        out = list(merged.values())
        self.last_fetch_pages = max(pages_k, pages_l)
        self.last_fetch_count = len(out)
        return out

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
        uniq: List[int] = []
        seen: set[int] = set()
        for i in ids:
            if isinstance(i, int) and i > 0 and i not in seen:
                seen.add(i)
                uniq.append(i)

        ask: List[int] = []
        for i in uniq:
            if str(i) not in self.name_cache:
                ask.append(i)

        if not ask:
            return

        url = f"{ESI_BASE}/universe/names/"
        result = await self.esi_post_json(url, ask)
        if not isinstance(result, list):
            return

        for row in result:
            try:
                _id = str(row.get("id"))
                _name = row.get("name")
                if _id and isinstance(_name, str) and _name:
                    self.name_cache[_id] = _name
            except Exception:
                continue

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

        url = f"{ESI_BASE}/universe/systems/{system_id}/"
        data = await self.esi_get_json(url)
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

        url = f"{ESI_BASE}/universe/types/{type_id}/"
        data = await self.esi_get_json(url)
        name = (data or {}).get("name") if isinstance(data, dict) else None
        if isinstance(name, str) and name:
            self.type_cache[key] = name
            self.type_cache = clamp_dict(self.type_cache, MAX_TYPE_CACHE)
            return name
        return "Unknown type"

    # -------------------------
    # Enrichment / embed
    # -------------------------

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
        return best or {}

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
        kmid = get_killmail_id(zkm)
        kmhash = get_killmail_hash(zkm)
        esikm = None

        if kmid and kmhash:
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

                system_id = safe_int(esikm.get("solar_system_id"))
                if system_id:
                    await self.resolve_system_info(system_id)

                ship_type_id = safe_int(victim.get("ship_type_id"))
                if ship_type_id:
                    await self.resolve_type_name(ship_type_id)

                fb_ship_type_id = safe_int(fb.get("ship_type_id"))
                if fb_ship_type_id:
                    await self.resolve_type_name(fb_ship_type_id)

            except Exception:
                esikm = None

        return zkm, esikm

    def best_time_dt(self, zkm: Dict[str, Any], esikm: Optional[Dict[str, Any]]) -> Optional[datetime.datetime]:
        t_raw = (esikm or {}).get("killmail_time") or zkm.get("killmail_time")
        return parse_killmail_time(t_raw)

    def build_embed(self, zkm: Dict[str, Any], esikm: Optional[Dict[str, Any]]) -> discord.Embed:
        kmid = get_killmail_id(zkm)
        kmhash = get_killmail_hash(zkm)

        z_url = zkill_link(kmid) if kmid else "https://zkillboard.com/"
        e_url = esi_killmail_link(kmid, kmhash) if (kmid and kmhash) else None

        victim = (esikm or {}).get("victim") or {}
        attackers = (esikm or {}).get("attackers") or []
        n_atk = len(attackers) if isinstance(attackers, list) else 0

        v_char_id = safe_int(victim.get("character_id"))
        v_corp_id = safe_int(victim.get("corporation_id"))
        v_alliance_id = safe_int(victim.get("alliance_id"))

        v_char_name = self.name_cache.get(str(v_char_id), "Unknown") if v_char_id else "Unknown"
        v_corp_name = self.name_cache.get(str(v_corp_id), "Unknown corp") if v_corp_id else "Unknown corp"
        v_alliance_name = self.name_cache.get(str(v_alliance_id), "None") if v_alliance_id else "None"

        ship_type_id = safe_int(victim.get("ship_type_id"))
        ship_name = self.type_cache.get(str(ship_type_id), "Unknown ship") if ship_type_id else "Unknown ship"

        system_id = safe_int((esikm or {}).get("solar_system_id"))
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
        ktime_raw = (esikm or {}).get("killmail_time") or zkm.get("killmail_time")
        ktime_dt = parse_killmail_time(ktime_raw) or utcnow()

        tag = self.classify_mail(esikm)
        if tag == "LOSS":
            color = discord.Color.red()
        elif tag == "KILL":
            color = discord.Color.green()
        elif tag == "INVOLVEMENT":
            color = discord.Color.gold()
        else:
            color = discord.Color.blurple()

        title = f"{tag} — Killmail #{kmid}" if kmid else f"{tag} — Killmail"

        lines = [
            f"**Victim:** {v_char_name}",
            f"**Victim Corp:** {v_corp_name}",
            f"**Victim Alliance:** {v_alliance_name}",
            "",
            f"**Victim Ship:** {ship_name}",
            f"**System:** {system_name} (Sec: {format_sec_status(sec_status)})",
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

        if ktime_raw:
            lines.append(f"**Time:** {ktime_raw}")

        links = f"[zKillboard]({z_url})"
        if e_url:
            links += f" • [ESI]({e_url})"
        lines.append("")
        lines.append(links)

        emb = discord.Embed(
            title=title,
            url=z_url,
            description="\n".join(lines),
            color=color,
            timestamp=ktime_dt,
        )
        emb.set_footer(text=f"Source: zKillboard + ESI | Window: last {WINDOW_DAYS}d")
        if ship_type_id:
            emb.set_thumbnail(url=victim_ship_icon_url(ship_type_id))
            emb.set_image(url=type_render_url(ship_type_id))
        return emb

    # -------------------------
    # Persistence / de-dupe
    # -------------------------

    def prune_posted_map(self):
        cut = cutoff_utc()
        keep: Dict[str, str] = {}
        for kmid, iso in self.posted_map.items():
            dtp = parse_killmail_time(iso)
            if dtp is None or dtp >= cut:
                keep[kmid] = iso
        self.posted_map = keep

    async def persist(self):
        self.prune_posted_map()
        self.name_cache = clamp_dict(self.name_cache, MAX_NAME_CACHE)
        self.type_cache = clamp_dict(self.type_cache, MAX_TYPE_CACHE)

        self.state["posted_map"] = self.posted_map
        self.state["updated_utc"] = utcnow_iso()
        self.state["consecutive_failures"] = self.consecutive_failures
        self.state["last_poll_utc"] = self.last_poll_utc
        self.state["last_posted_id"] = self.last_posted_id
        self.state["last_posted_time"] = self.last_posted_time
        self.state["last_backlog_size"] = self.last_backlog_size
        self.state["last_fetch_pages"] = self.last_fetch_pages
        self.state["last_fetch_count"] = self.last_fetch_count

        self.state["last_error"] = self.last_error
        self.state["last_send_error"] = self.last_send_error
        self.state["last_send_attempt_utc"] = self.last_send_attempt_utc
        self.state["send_failures"] = self.send_failures
        self.state["last_channel_id"] = self.last_channel_id

        self.state["last_unseen_count"] = self.last_unseen_count
        self.state["last_candidate_count"] = self.last_candidate_count
        self.state["last_enriched_count"] = self.last_enriched_count
        self.state["last_to_post_count"] = self.last_to_post_count

        self.state["name_cache"] = self.name_cache
        self.state["system_cache"] = self.system_cache
        self.state["type_cache"] = self.type_cache

        save_json(DATA_FILE, self.state)

    def _is_already_posted(self, kmid: int) -> bool:
        k = str(kmid)
        return (k in self.posted_map) or (k in self._posted_this_run)

    def _mark_posted(self, kmid: int, iso_time: str) -> None:
        k = str(kmid)
        self.posted_map[k] = iso_time
        self._posted_this_run.add(k)

    def _ztime_key(self, km: Dict[str, Any]) -> Tuple[datetime.datetime, int]:
        t = parse_killmail_time(km.get("killmail_time"))
        if t is None:
            t = datetime.datetime.max
        kmid = get_killmail_id(km) or 0
        return (t, int(kmid))

    # -------------------------
    # Posting cycle
    # -------------------------

    async def post_cycle(self) -> Tuple[int, int]:
        items = await self.fetch_zkill_last_window()
        self.last_poll_utc = utcnow_iso()
        cut = cutoff_utc()

        unseen: List[Dict[str, Any]] = []
        for km in items:
            kmid = get_killmail_id(km)
            if not kmid:
                continue
            t = parse_killmail_time(km.get("killmail_time"))
            if t is not None and t < cut:
                continue
            if self._is_already_posted(kmid):
                continue
            unseen.append(km)

        unseen.sort(key=self._ztime_key)

        self.last_unseen_count = len(unseen)
        self.last_backlog_size = len(unseen)

        if not unseen:
            self.last_candidate_count = 0
            self.last_enriched_count = 0
            self.last_to_post_count = 0
            await self.persist()
            return 0, 0

        candidates = unseen[:CANDIDATE_ENRICH_LIMIT]
        self.last_candidate_count = len(candidates)

        enriched: List[Tuple[datetime.datetime, int, Dict[str, Any], Optional[Dict[str, Any]]]] = []
        for zkm in candidates:
            kmid = get_killmail_id(zkm) or 0
            zkm2, esikm = await self.enrich_one(zkm)

            t_best = self.best_time_dt(zkm2, esikm)
            if t_best is None:
                # allow posting; put at the end of ordering deterministically
                t_best = datetime.datetime.max

            if t_best is not datetime.datetime.max and t_best < cut:
                self._mark_posted(int(kmid), (esikm or {}).get("killmail_time") or zkm2.get("killmail_time") or utcnow_iso())
                continue

            enriched.append((t_best, int(kmid), zkm2, esikm))

        # strict chronological by best time
        enriched.sort(key=lambda x: (x[0], x[1]))
        self.last_enriched_count = len(enriched)

        # NORMAL path
        to_post = enriched[:MAX_POSTS_PER_CYCLE]

        # FAIL-SAFE path: if enrichment yields nothing, still post the oldest unseen using zKill-only
        if not to_post:
            fallback = unseen[:MAX_POSTS_PER_CYCLE]
            to_post = []
            for km in fallback:
                kmid = get_killmail_id(km) or 0
                # best-effort time key
                t = parse_killmail_time(km.get("killmail_time")) or datetime.datetime.max
                to_post.append((t, int(kmid), km, None))

        self.last_to_post_count = len(to_post)

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

            for _, kmid, zkm2, esikm in to_post:
                if kmid <= 0:
                    continue
                if self._is_already_posted(kmid):
                    continue

                try:
                    await channel.send(embed=self.build_embed(zkm2, esikm))
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

        # Remaining backlog
        remaining = 0
        for km in unseen:
            kmid = get_killmail_id(km)
            if kmid and not self._is_already_posted(kmid):
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
            delay = min(300, 20 * self.consecutive_failures)
            await asyncio.sleep(delay)

    @tasks.loop(seconds=POLL_SECONDS)
    async def killmail_loop(self):
        await self.run_with_backoff()

    @killmail_loop.before_loop
    async def before_killmail_loop(self):
        await self.bot.wait_until_ready()

    # =====================
    # SLASH COMMANDS
    # =====================

    @app_commands.command(name="killmail_status", description="Show killmail feed status and selector diagnostics.")
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
                f"**zKill request delay:** {ZKILL_REQUEST_DELAY}s\n"
                f"**Candidate enrich limit:** {CANDIDATE_ENRICH_LIMIT}\n\n"
                f"**Last fetch count (merged):** {self.last_fetch_count}\n"
                f"**Last fetch pages used:** {self.last_fetch_pages}\n"
                f"**Last poll (UTC):** {self.last_poll_utc or 'Never'}\n"
                f"**Last send attempt (UTC):** {self.last_send_attempt_utc or 'Never'}\n"
                f"**Last channel ID:** {self.last_channel_id or 'None'}\n"
                f"**Last posted ID:** {self.last_posted_id or 'None'}\n"
                f"**Last posted time:** {self.last_posted_time or 'None'}\n\n"
                f"**Selector diagnostics:**\n"
                f"- Unseen: {self.last_unseen_count}\n"
                f"- Candidates: {self.last_candidate_count}\n"
                f"- Enriched: {self.last_enriched_count}\n"
                f"- To post: {self.last_to_post_count}\n\n"
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

    @app_commands.command(
        name="killmail_post_test",
        description="Admin only: post a simple test message in #kill-mail (checks permissions)."
    )
    @require_killmail_admin()
    async def killmail_post_test(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        if not interaction.guild:
            await safe_reply(interaction, "❌ Must be used in a server.", ephemeral=True)
            return

        try:
            ch = await self.ensure_channel(interaction.guild)
        except Exception as e:
            await safe_reply(interaction, f"❌ ensure_channel failed: {type(e).__name__}: {e}", ephemeral=True)
            return

        perm_err = self._check_channel_perms(interaction.guild, ch)
        if perm_err:
            await safe_reply(interaction, f"❌ {perm_err}", ephemeral=True)
            return

        try:
            await ch.send("Killmail feed test: bot can post here and embed permissions are OK.")
        except Exception as e:
            await safe_reply(interaction, f"❌ Send failed: {type(e).__name__}: {e}", ephemeral=True)
            return

        await safe_reply(interaction, f"✅ Posted test message in #{ch.name}.", ephemeral=True)

    @app_commands.command(
        name="killmail_debug_next",
        description="Admin only: show selector counts and attempt to post the next killmail."
    )
    @require_killmail_admin()
    async def killmail_debug_next(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        try:
            posted, remaining = await self.post_cycle()
        except Exception as e:
            await safe_reply(interaction, f"❌ post_cycle threw: {type(e).__name__}: {e}", ephemeral=True)
            return

        await safe_reply(
            interaction,
            (
                f"post_cycle() done.\n"
                f"- posted: {posted}\n"
                f"- remaining: {remaining}\n"
                f"- unseen: {self.last_unseen_count}\n"
                f"- candidates: {self.last_candidate_count}\n"
                f"- enriched: {self.last_enriched_count}\n"
                f"- to_post: {self.last_to_post_count}\n"
                f"- last_send_error: {self.last_send_error or 'None'}"
            ),
            ephemeral=True
        )

    @app_commands.command(
        name="killmail_clear_cache",
        description="CEO only: clear posted cache (will repost the last window again)."
    )
    @require_ceo()
    async def killmail_clear_cache(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)
        self.posted_map = {}
        self._posted_this_run = set()
        self.last_posted_id = None
        self.last_posted_time = None
        await self.persist()
        await safe_reply(
            interaction,
            f"Cleared posted cache. The bot will repost the last {WINDOW_DAYS} days (chronological; newest last).",
            ephemeral=True
        )

    @app_commands.command(
        name="killmail_reload",
        description="Reload a killmail by kill ID and repost it to #kill-mail (admin only)."
    )
    @require_killmail_admin()
    async def killmail_reload(self, interaction: discord.Interaction, killmail_id: int):
        await safe_defer(interaction, ephemeral=True)

        kmid = safe_int(killmail_id)
        if not kmid or kmid <= 0:
            await safe_reply(interaction, "❌ Invalid killmail_id.", ephemeral=True)
            return

        try:
            zkm = await self.fetch_zkill_one(kmid)
        except Exception as e:
            await safe_reply(interaction, f"❌ Fetch failed for {kmid}: {type(e).__name__}: {e}", ephemeral=True)
            return

        if not zkm:
            await safe_reply(interaction, f"❌ No data returned for killmail_id={kmid}.", ephemeral=True)
            return

        zkm, esikm = await self.enrich_one(zkm)
        t_best = self.best_time_dt(zkm, esikm)
        if t_best is not None and t_best < cutoff_utc():
            await safe_reply(interaction, f"❌ Killmail {kmid} is older than {WINDOW_DAYS} days; not reposting.", ephemeral=True)
            return

        if not interaction.guild:
            await safe_reply(interaction, "❌ This command must be used in a server.", ephemeral=True)
            return

        try:
            channel = await self.ensure_channel(interaction.guild)
            perm_err = self._check_channel_perms(interaction.guild, channel)
            if perm_err:
                await safe_reply(interaction, f"❌ {perm_err}", ephemeral=True)
                return

            await channel.send(embed=self.build_embed(zkm, esikm))
        except Exception as e:
            await safe_reply(interaction, f"❌ Post failed: {type(e).__name__}: {e}", ephemeral=True)
            return

        best_time_iso = (esikm or {}).get("killmail_time") or zkm.get("killmail_time") or utcnow_iso()
        self._mark_posted(kmid, str(best_time_iso))
        self.last_posted_id = str(kmid)
        self.last_posted_time = str(best_time_iso)
        await self.persist()

        await safe_reply(interaction, f"✅ Reloaded and reposted killmail **{kmid}** in #{KILLMAIL_CHANNEL_NAME}.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(KillmailFeed(bot))
