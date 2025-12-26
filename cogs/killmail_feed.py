# cogs/killmail_feed.py
# Updates per your requirements:
# - Always posts most recent LAST (chronological oldest -> newest).
# - Pulls/posts MAX_POSTS_PER_CYCLE (=5) per cycle.
# - Runs every POLL_SECONDS (=120s).
# - If backlog remains after posting 5, runs ONE additional cycle after 60s.
# - Never duplicates posts (posted_map persisted + in-memory de-dupe).
# - Hard cutoff: only last 14 days.

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

POLL_SECONDS = 120
FAST_POLL_SECONDS_WHEN_BACKLOG_REMAINS = 60
MAX_POSTS_PER_CYCLE = 5

# HARD LIMIT: do not look further than 2 weeks ago
WINDOW_DAYS = 14
MAX_ZKILL_PAGES = 20

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
    """
    Accepts ISO8601 strings such as 2025-12-24T12:34:56Z.
    Returns UTC-naive datetime for comparisons.
    """
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


# =====================
# COG
# =====================

class KillmailFeed(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.session: Optional[aiohttp.ClientSession] = None
        self.lock = asyncio.Lock()

        self.state = load_json(DATA_FILE)

        # posted_map: { "killmail_id": "iso_time" }
        self.posted_map: Dict[str, str] = self.state.get("posted_map", {}) or {}

        # caches
        self.name_cache: Dict[str, str] = self.state.get("name_cache", {}) or {}
        self.system_cache: Dict[str, Dict[str, Any]] = self.state.get("system_cache", {}) or {}
        self.type_cache: Dict[str, str] = self.state.get("type_cache", {}) or {}

        # metrics
        self.consecutive_failures: int = int(self.state.get("consecutive_failures", 0) or 0)
        self.last_poll_utc: Optional[str] = self.state.get("last_poll_utc")
        self.last_posted_id: Optional[str] = self.state.get("last_posted_id")
        self.last_posted_time: Optional[str] = self.state.get("last_posted_time")
        self.last_backlog_size: int = int(self.state.get("last_backlog_size", 0) or 0)
        self.last_fetch_pages: int = int(self.state.get("last_fetch_pages", 0) or 0)
        self.last_fetch_count: int = int(self.state.get("last_fetch_count", 0) or 0)

        # in-process de-dupe for a single run (handles edge cases before persist)
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

    # -------------------------
    # HTTP: zKill (paged)
    # -------------------------

    async def fetch_zkill_page(self, page: int) -> List[Dict[str, Any]]:
        await self.ensure_session()
        url = f"https://zkillboard.com/api/kills/corporationID/{CORPORATION_ID}/page/{page}/"
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

    async def fetch_zkill_last_window(self) -> List[Dict[str, Any]]:
        cut = cutoff_utc()
        out: List[Dict[str, Any]] = []
        pages_used = 0

        for page in range(1, MAX_ZKILL_PAGES + 1):
            rows = await self.fetch_zkill_page(page)
            pages_used = page
            if not rows:
                break

            out.extend(rows)

            times = []
            for km in rows:
                t = parse_killmail_time(km.get("killmail_time"))
                if t is not None:
                    times.append(t)
            if times:
                oldest = min(times)
                if oldest < cut:
                    break

        filtered: List[Dict[str, Any]] = []
        for km in out:
            t = parse_killmail_time(km.get("killmail_time"))
            if t is None or t >= cut:
                filtered.append(km)

        self.last_fetch_pages = pages_used
        self.last_fetch_count = len(filtered)
        return filtered

    # -------------------------
    # HTTP: ESI
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
        seen: set[int] = set()
        uniq: List[int] = []
        for i in ids:
            if isinstance(i, int) and i > 0 and i not in seen:
                seen.add(i)
                uniq.append(i)

        if not uniq:
            return

        ask: List[int] = []
        for i in uniq:
            key = str(i)
            if key not in self.name_cache:
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
    # Enrichment + classification
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
        v_corp = safe_int(victim.get("corporation_id"))
        if v_corp == CORPORATION_ID:
            return "LOSS"

        attackers = esikm.get("attackers") or []
        fb = self.pick_final_blow_attacker(esikm)
        fb_corp = safe_int(fb.get("corporation_id")) if isinstance(fb, dict) else None
        if fb_corp == CORPORATION_ID:
            return "KILL"

        for a in attackers if isinstance(attackers, list) else []:
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

                ids_to_resolve: List[int] = []
                victim = (esikm.get("victim") or {})
                for k in ("character_id", "corporation_id", "alliance_id"):
                    v = safe_int(victim.get(k))
                    if v:
                        ids_to_resolve.append(v)

                fb = self.pick_final_blow_attacker(esikm)
                for k in ("character_id", "corporation_id", "alliance_id"):
                    v = safe_int(fb.get(k))
                    if v:
                        ids_to_resolve.append(v)

                await self.resolve_universe_names(ids_to_resolve)

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
    # Persistence / pruning
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

        self.state["name_cache"] = self.name_cache
        self.state["system_cache"] = self.system_cache
        self.state["type_cache"] = self.type_cache

        save_json(DATA_FILE, self.state)

    # -------------------------
    # Core loop: always most recent last, 5 per cycle, fast follow-up if backlog remains
    # -------------------------

    def km_time_for_sort(self, km: Dict[str, Any]) -> datetime.datetime:
        t = parse_killmail_time(km.get("killmail_time"))
        return t if t is not None else utcnow()

    def _is_already_posted(self, kmid: int) -> bool:
        k = str(kmid)
        return (k in self.posted_map) or (k in self._posted_this_run)

    def _mark_posted(self, kmid: int, iso_time: str) -> None:
        k = str(kmid)
        self.posted_map[k] = iso_time
        self._posted_this_run.add(k)

    async def post_cycle(self) -> Tuple[int, int]:
        """
        Returns (posted_count, backlog_remaining_after_marking).
        """
        kills = await self.fetch_zkill_last_window()
        self.last_poll_utc = utcnow_iso()

        cut = cutoff_utc()

        # Collect unseen within window
        unseen: List[Dict[str, Any]] = []
        for km in kills:
            kmid = get_killmail_id(km)
            if not kmid:
                continue

            t = parse_killmail_time(km.get("killmail_time"))
            if t is not None and t < cut:
                continue

            if self._is_already_posted(kmid):
                continue

            unseen.append(km)

        # CHRONOLOGICAL: oldest -> newest (so newest is posted last)
        unseen.sort(key=self.km_time_for_sort)

        backlog_size = len(unseen)
        self.last_backlog_size = backlog_size

        if backlog_size == 0:
            await self.persist()
            return 0, 0

        batch = unseen[:MAX_POSTS_PER_CYCLE]

        # Enrich sequentially (simple, stable). If you want, we can parallelize with a semaphore.
        enriched: List[Tuple[Dict[str, Any], Optional[Dict[str, Any]]]] = []
        for zkm in batch:
            enriched.append(await self.enrich_one(zkm))

        posted_count = 0

        # Post to each guild's #kill-mail
        for guild in self.bot.guilds:
            try:
                channel = await self.ensure_channel(guild)
            except Exception as e:
                print(f"[killmail_feed] Failed to ensure channel in guild={guild.id}: {type(e).__name__}: {e}")
                continue

            for zkm, esikm in enriched:
                kmid = get_killmail_id(zkm)
                if not kmid:
                    continue
                # Double-check de-dupe right before sending
                if self._is_already_posted(kmid):
                    continue
                try:
                    await channel.send(embed=self.build_embed(zkm, esikm))
                    posted_count += 1

                    best_time = (esikm or {}).get("killmail_time") or zkm.get("killmail_time") or utcnow_iso()
                    self._mark_posted(kmid, str(best_time))
                    self.last_posted_id = str(kmid)
                    self.last_posted_time = str(best_time)

                except Exception as e:
                    print(f"[killmail_feed] Send failed guild={guild.id} kmid={kmid}: {type(e).__name__}: {e}")

        await self.persist()

        # Recompute backlog remaining using the current fetched list (cheap, accurate enough)
        remaining = 0
        for km in kills:
            kmid = get_killmail_id(km)
            if not kmid:
                continue
            t = parse_killmail_time(km.get("killmail_time"))
            if t is not None and t < cut:
                continue
            if self._is_already_posted(kmid):
                continue
            remaining += 1

        self.last_backlog_size = remaining
        await self.persist()
        return posted_count, remaining

    async def run_once_and_maybe_fast_followup(self):
        """
        One normal cycle; if backlog remains after posting 5, do exactly one follow-up after 60s.
        """
        async with self.lock:
            posted, remaining = await self.post_cycle()

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
            print(f"[killmail_feed] Cycle failed: {type(e).__name__}: {e}")
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

    @app_commands.command(name="killmail_status", description="Show killmail feed status (window, backlog, failures).")
    async def killmail_status(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        emb = discord.Embed(
            title="Killmail Feed Status",
            description=(
                f"**Corp ID:** {CORPORATION_ID}\n"
                f"**Window:** last {WINDOW_DAYS} days\n"
                f"**Channel:** #{KILLMAIL_CHANNEL_NAME}\n\n"
                f"**Poll interval:** {POLL_SECONDS}s\n"
                f"**Max posts / cycle:** {MAX_POSTS_PER_CYCLE}\n"
                f"**Follow-up if backlog remains:** {FAST_POLL_SECONDS_WHEN_BACKLOG_REMAINS}s\n\n"
                f"**zKill pages/cycle (max):** {MAX_ZKILL_PAGES}\n"
                f"**Last fetch pages used:** {self.last_fetch_pages}\n"
                f"**Last fetch kill count (window):** {self.last_fetch_count}\n\n"
                f"**Last poll (UTC):** {self.last_poll_utc or 'Never'}\n"
                f"**Last posted ID:** {self.last_posted_id or 'None'}\n"
                f"**Last posted time:** {self.last_posted_time or 'None'}\n"
                f"**Backlog (unposted in window):** {self.last_backlog_size}\n"
                f"**Consecutive failures:** {self.consecutive_failures}\n"
                f"**Remembered posted IDs (in window):** {len(self.posted_map)}\n"
            ),
            timestamp=utcnow(),
        )
        emb.set_footer(text="Cryonic Gaming bot — Killmail Feed")
        await safe_reply(interaction, embed=emb, ephemeral=True)

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
            f"Cleared posted cache. The bot will repost the last {WINDOW_DAYS} days (chronological, newest last).",
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

        # Enforce cutoff from zKill time
        t = parse_killmail_time(zkm.get("killmail_time"))
        if t is not None and t < cutoff_utc():
            await safe_reply(interaction, f"❌ Killmail {kmid} is older than {WINDOW_DAYS} days; not reposting.", ephemeral=True)
            return

        zkm, esikm = await self.enrich_one(zkm)

        # Enforce cutoff using ESI time if present
        t2 = parse_killmail_time((esikm or {}).get("killmail_time") or zkm.get("killmail_time"))
        if t2 is not None and t2 < cutoff_utc():
            await safe_reply(interaction, f"❌ Killmail {kmid} is older than {WINDOW_DAYS} days; not reposting.", ephemeral=True)
            return

        if not interaction.guild:
            await safe_reply(interaction, "❌ This command must be used in a server.", ephemeral=True)
            return

        try:
            channel = await self.ensure_channel(interaction.guild)
            await channel.send(embed=self.build_embed(zkm, esikm))
        except Exception as e:
            await safe_reply(interaction, f"❌ Post failed: {type(e).__name__}: {e}", ephemeral=True)
            return

        best_time = (esikm or {}).get("killmail_time") or zkm.get("killmail_time") or utcnow_iso()
        self._mark_posted(kmid, str(best_time))
        self.last_posted_id = str(kmid)
        self.last_posted_time = str(best_time)
        await self.persist()

        await safe_reply(interaction, f"✅ Reloaded and reposted killmail **{kmid}** in #{KILLMAIL_CHANNEL_NAME}.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(KillmailFeed(bot))
