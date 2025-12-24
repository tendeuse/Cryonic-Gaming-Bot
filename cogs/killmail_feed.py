# cogs/killmail_feed.py

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
FAST_POLL_SECONDS_WHEN_LIMIT_REACHED = 60
MAX_POSTS_PER_CYCLE = 5

ZKILL_URL = f"https://zkillboard.com/api/kills/corporationID/{CORPORATION_ID}/"
USER_AGENT = "Cryonic Gaming bot/1.0 (contact: tendeuse on Discord)"

ESI_BASE = "https://esi.evetech.net/latest"
IMAGE_BASE = "https://images.evetech.net"

DATA_FILE = Path("data/killmail_feed.json")
DATA_FILE.parent.mkdir(parents=True, exist_ok=True)

MAX_STORED_IDS = 3000
MAX_NAME_CACHE = 10000
MAX_SYSTEM_CACHE = 5000
MAX_TYPE_CACHE = 10000

# Admin-only role for reload command
CEO_ROLE = "ARC Security Corporation Leader"


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

def zkill_link(km_id: int) -> str:
    return f"https://zkillboard.com/kill/{km_id}/"

def esi_killmail_link(km_id: int, km_hash: str) -> str:
    return f"{ESI_BASE}/killmails/{km_id}/{km_hash}/"

def format_sec_status(sec: Optional[float]) -> str:
    if sec is None:
        return "Unknown"
    return f"{sec:.1f}"

def clamp_dict(d: Dict[str, Any], max_items: int) -> Dict[str, Any]:
    if len(d) <= max_items:
        return d
    keys = list(d.keys())[-max_items:]
    return {k: d[k] for k in keys}

def clamp_set_as_list(s: set, max_items: int) -> set:
    if len(s) <= max_items:
        return s
    items = list(s)[-max_items:]
    return set(items)

def isk_value(zkm: Dict[str, Any]) -> Optional[float]:
    zkb = zkm.get("zkb") or {}
    val = zkb.get("totalValue")
    try:
        return float(val) if val is not None else None
    except Exception:
        return None

def victim_ship_icon_url(type_id: int) -> str:
    return f"{IMAGE_BASE}/types/{type_id}/icon?size=64"

def type_render_url(type_id: int) -> str:
    return f"{IMAGE_BASE}/types/{type_id}/render?size=512"

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

        self.posted_ids = set(str(x) for x in self.state.get("posted_ids", []))
        self.bootstrapped: bool = bool(self.state.get("bootstrapped", False))

        # Caches
        self.name_cache: Dict[str, str] = self.state.get("name_cache", {}) or {}
        self.system_cache: Dict[str, Dict[str, Any]] = self.state.get("system_cache", {}) or {}
        self.type_cache: Dict[str, str] = self.state.get("type_cache", {}) or {}

        # metrics
        self.consecutive_failures: int = int(self.state.get("consecutive_failures", 0) or 0)
        self.last_poll_utc: Optional[str] = self.state.get("last_poll_utc")
        self.last_posted_id: Optional[str] = self.state.get("last_posted_id")
        self.last_backlog_size: int = int(self.state.get("last_backlog_size", 0) or 0)
        self.last_bootstrap_seeded: int = int(self.state.get("last_bootstrap_seeded", 0) or 0)

        self.killmail_loop.start()

    def cog_unload(self):
        self.killmail_loop.cancel()
        if self.session and not self.session.closed:
            asyncio.create_task(self.session.close())

    async def ensure_session(self):
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=25)
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
    # HTTP: zKill
    # -------------------------

    async def fetch_zkill(self) -> List[Dict[str, Any]]:
        await self.ensure_session()
        headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"}
        async with self.session.get(ZKILL_URL, headers=headers) as resp:
            if resp.status == 429:
                raise RuntimeError("Rate limited by zKill (429).")
            if resp.status >= 400:
                txt = await resp.text()
                raise RuntimeError(f"zKill HTTP {resp.status}: {txt[:200]}")
            data = await resp.json(content_type=None)
            return data if isinstance(data, list) else []

    async def fetch_zkill_one(self, killmail_id: int) -> Optional[Dict[str, Any]]:
        """
        zKill "single kill" endpoint returns a list with one item (typically).
        We use this to resolve the killmail hash reliably.
        """
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
            if isinstance(data, list) and data:
                row = data[0]
                return row if isinstance(row, dict) else None
            return None

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
        """
        ESI /universe/names requires UNIQUE IDs.
        This function dedupes while preserving order, then resolves only missing IDs.
        """
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
    # Enrichment helpers
    # -------------------------

    def infer_kill_or_loss(self, esikm: Optional[Dict[str, Any]]) -> Optional[str]:
        if not isinstance(esikm, dict):
            return None

        victim = esikm.get("victim") or {}
        v_corp = safe_int(victim.get("corporation_id"))
        if v_corp == CORPORATION_ID:
            return "LOSS"

        for a in (esikm.get("attackers") or []):
            a_corp = safe_int(a.get("corporation_id"))
            if a_corp == CORPORATION_ID:
                return "KILL"
        return "INVOLVED"

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

                # Dedupe IDs (ESI requires uniqueness)
                seen2: set[int] = set()
                ids_to_resolve_uniq: List[int] = []
                for _id in ids_to_resolve:
                    if isinstance(_id, int) and _id > 0 and _id not in seen2:
                        seen2.add(_id)
                        ids_to_resolve_uniq.append(_id)

                await self.resolve_universe_names(ids_to_resolve_uniq)

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
        ktime = (esikm or {}).get("killmail_time") or zkm.get("killmail_time")

        tag = self.infer_kill_or_loss(esikm)
        title = f"{tag} — Killmail #{kmid}" if (tag and kmid) else (f"Killmail #{kmid}" if kmid else "Killmail")

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

        if ktime:
            lines.append(f"**Time:** {ktime}")

        links = f"[zKillboard]({z_url})"
        if e_url:
            links += f" • [ESI]({e_url})"
        lines.append("")
        lines.append(links)

        emb = discord.Embed(
            title=title,
            url=z_url,
            description="\n".join(lines),
            timestamp=utcnow(),
        )
        emb.set_footer(text="Source: zKillboard + ESI")

        if ship_type_id:
            emb.set_thumbnail(url=victim_ship_icon_url(ship_type_id))
            emb.set_image(url=type_render_url(ship_type_id))

        return emb

    # -------------------------
    # Persistence
    # -------------------------

    async def persist(self):
        self.posted_ids = clamp_set_as_list(self.posted_ids, MAX_STORED_IDS)
        self.name_cache = clamp_dict(self.name_cache, MAX_NAME_CACHE)
        self.type_cache = clamp_dict(self.type_cache, MAX_TYPE_CACHE)

        self.state["posted_ids"] = list(self.posted_ids)
        self.state["bootstrapped"] = self.bootstrapped
        self.state["updated_utc"] = utcnow_iso()
        self.state["consecutive_failures"] = self.consecutive_failures
        self.state["last_poll_utc"] = self.last_poll_utc
        self.state["last_posted_id"] = self.last_posted_id
        self.state["last_backlog_size"] = self.last_backlog_size
        self.state["last_bootstrap_seeded"] = self.last_bootstrap_seeded

        self.state["name_cache"] = self.name_cache
        self.state["system_cache"] = self.system_cache
        self.state["type_cache"] = self.type_cache

        save_json(DATA_FILE, self.state)

    # -------------------------
    # Core loop
    # -------------------------

    async def post_cycle(self) -> Tuple[int, int]:
        kills = await self.fetch_zkill()
        self.last_poll_utc = utcnow_iso()

        # BOOTSTRAP: seed current feed, post nothing
        if not self.bootstrapped:
            seeded = 0
            for km in kills:
                kmid = get_killmail_id(km)
                if kmid:
                    self.posted_ids.add(str(kmid))
                    seeded += 1
            self.bootstrapped = True
            self.last_bootstrap_seeded = seeded
            self.last_backlog_size = 0
            await self.persist()
            return 0, 0

        unseen: List[Dict[str, Any]] = []
        for km in kills:
            kmid = get_killmail_id(km)
            if not kmid:
                continue
            if str(kmid) in self.posted_ids:
                continue
            unseen.append(km)

        backlog_size = len(unseen)
        self.last_backlog_size = backlog_size

        if backlog_size == 0:
            await self.persist()
            return 0, 0

        # Oldest first
        unseen.sort(key=lambda x: (safe_int(x.get("killmail_id")) or 0))
        to_post = unseen[:MAX_POSTS_PER_CYCLE]

        enriched: List[Tuple[Dict[str, Any], Optional[Dict[str, Any]]]] = []
        for zkm in to_post:
            enriched.append(await self.enrich_one(zkm))

        # Post to each guild
        for guild in self.bot.guilds:
            try:
                channel = await self.ensure_channel(guild)
            except Exception:
                continue

            for zkm, esikm in enriched:
                kmid = get_killmail_id(zkm)
                if not kmid:
                    continue
                try:
                    await channel.send(embed=self.build_embed(zkm, esikm))
                except Exception:
                    continue

        for zkm, _ in enriched:
            kmid = get_killmail_id(zkm)
            if kmid:
                self.posted_ids.add(str(kmid))
                self.last_posted_id = str(kmid)

        await self.persist()
        return len(enriched), backlog_size

    async def run_with_backoff(self):
        async with self.lock:
            try:
                posted, backlog = await self.post_cycle()
                self.consecutive_failures = 0
                await self.persist()

                if posted >= MAX_POSTS_PER_CYCLE and backlog > MAX_POSTS_PER_CYCLE:
                    await asyncio.sleep(FAST_POLL_SECONDS_WHEN_LIMIT_REACHED)
                    await self.post_cycle()
                    await self.persist()

            except Exception:
                self.consecutive_failures += 1
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

    @app_commands.command(name="killmail_status", description="Show killmail feed status (polling, backlog, failures).")
    async def killmail_status(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        last_poll = self.last_poll_utc or "Never"
        last_post = self.last_posted_id or "None"
        backlog = self.last_backlog_size
        fails = self.consecutive_failures
        boot = "Yes" if self.bootstrapped else "No"
        seeded = self.last_bootstrap_seeded

        emb = discord.Embed(
            title="Killmail Feed Status",
            description=(
                f"**Corp ID:** {CORPORATION_ID}\n"
                f"**zKill URL:** {ZKILL_URL}\n"
                f"**Channel:** #{KILLMAIL_CHANNEL_NAME}\n\n"
                f"**Poll interval:** {POLL_SECONDS}s\n"
                f"**Max posts / cycle:** {MAX_POSTS_PER_CYCLE}\n"
                f"**Fast follow-up if capped:** {FAST_POLL_SECONDS_WHEN_LIMIT_REACHED}s\n\n"
                f"**Bootstrapped:** {boot}\n"
                f"**Bootstrap seeded IDs:** {seeded}\n\n"
                f"**Last poll (UTC):** {last_poll}\n"
                f"**Last posted ID:** {last_post}\n"
                f"**Backlog (unseen at last poll):** {backlog}\n"
                f"**Consecutive failures:** {fails}\n"
            ),
            timestamp=utcnow(),
        )
        emb.set_footer(text="Cryonic Gaming bot — Version 1.0")
        await safe_reply(interaction, embed=emb, ephemeral=True)

    @app_commands.command(
        name="killmail_reload",
        description="Reload and repost a killmail by its killmail number (CEO only)."
    )
    @app_commands.describe(killmail_id="The killmail ID number from zKill (e.g. 123456789)")
    @require_ceo()
    async def killmail_reload(self, interaction: discord.Interaction, killmail_id: int):
        """
        Fetches zKill + ESI data for a specific killmail ID and reposts it to #kill-mail
        in the guild where the command was run. CEO role only.
        """
        await safe_defer(interaction, ephemeral=True)

        if not interaction.guild:
            await safe_reply(interaction, "This command must be used in a server.", ephemeral=True)
            return

        # Ensure killmail channel exists in THIS guild
        try:
            channel = await self.ensure_channel(interaction.guild)
        except Exception:
            await safe_reply(interaction, "I could not find or create the kill-mail channel.", ephemeral=True)
            return

        kmid = safe_int(killmail_id)
        if not kmid or kmid <= 0:
            await safe_reply(interaction, "Invalid killmail ID.", ephemeral=True)
            return

        # Fetch zKill single record to obtain hash reliably
        try:
            zkm = await self.fetch_zkill_one(kmid)
        except Exception as e:
            await safe_reply(interaction, f"Failed to fetch killmail from zKill. ({type(e).__name__})", ephemeral=True)
            return

        if not zkm:
            await safe_reply(interaction, "Killmail not found on zKillboard.", ephemeral=True)
            return

        kmhash = get_killmail_hash(zkm)
        if not kmhash:
            await safe_reply(interaction, "Killmail hash not available from zKillboard yet.", ephemeral=True)
            return

        # Enrich via ESI and update caches
        try:
            zkm_enriched, esikm = await self.enrich_one(zkm)
            emb = self.build_embed(zkm_enriched, esikm)
        except Exception as e:
            await safe_reply(interaction, f"Failed to enrich/build embed. ({type(e).__name__})", ephemeral=True)
            return

        # Post to channel
        try:
            await channel.send(embed=emb)
        except Exception:
            await safe_reply(interaction, "Failed to post the killmail embed (missing permissions?).", ephemeral=True)
            return

        # Manual repost tool: we do not mark as posted
        await safe_reply(
            interaction,
            f"Reposted killmail `{kmid}` to #{KILLMAIL_CHANNEL_NAME}.",
            ephemeral=True
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(KillmailFeed(bot))
