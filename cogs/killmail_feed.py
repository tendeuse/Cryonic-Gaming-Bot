# cogs/killmail_feed.py
#
# WebSocket-based killmail feed (zKillboard push + ESI enrichment)
# ----------------------------------------------------------------
# Architecture
# ============
# Primary delivery: zKillboard WebSocket (wss://zkillboard.com/websocket/)
#   - Subscribes to corporation:<corp_id> for each feed.
#   - Kills are pushed within seconds of zKill receiving them.
#   - No polling loop = no 60-second wait.
#
# Safety net: catchup poll every CATCHUP_POLL_MINUTES minutes
#   - Fetches the last CATCHUP_PAGES zKill pages (newest-first, frontier-aware).
#   - Catches anything the WebSocket missed during a disconnection window.
#
# Reconnection: exponential back-off (5s → 320s) with auto-resume.
#
# Feeds (both share one ESI session, separate dedup state):
#   main  corp 98743131  →  #kill-mail
#   hs    corp 98791781  →  #kill-mail-hs
#
# Slash commands:
#   /killmail_status          — live status for both feeds + WS state
#   /killmail_reload          — repost a specific kill by ID
#   /killmail_debug_next      — run one catchup scan immediately
#   /killmail_ws_reconnect    — force-reconnect the WebSocket

import os
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

PRIMARY_GUILD_ID: Optional[int] = None   # None = all guilds

# WebSocket
ZKILL_WS_URL        = "wss://zkillboard.com/websocket/"
WS_RECONNECT_BASE   = 5     # seconds, doubles each attempt up to WS_RECONNECT_MAX
WS_RECONNECT_MAX    = 320
WS_HEARTBEAT        = 30    # seconds between pings to keep connection alive

# Safety-net catchup poll (runs even when WebSocket is healthy)
CATCHUP_POLL_MINUTES = 10
CATCHUP_PAGES        = 3    # how many zKill pages to scan (newest-first, frontier-aware)
ZKILL_REQUEST_DELAY  = 0.15

# ESI
ESI_CONCURRENCY         = 8
ESI_REQUEST_DELAY       = 0.05
ESI_RETRY_FLOOR_SECONDS = 30

USER_AGENT = "Cryonic Gaming bot/1.0 (contact: tendeuse on Discord)"
ESI_BASE   = "https://esi.evetech.net/latest"
IMAGE_BASE = "https://images.evetech.net"

# Cache sizes
MAX_NAME_CACHE   = 10_000
MAX_SYSTEM_CACHE = 5_000
MAX_TYPE_CACHE   = 10_000
MAX_KM_CACHE     = 200_000

# ---------------------
# PERSISTENCE (Railway)
# ---------------------
PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)

DATA_FILES = {
    "main": PERSIST_ROOT / "killmail_feed.json",
    "hs":   PERSIST_ROOT / "killmail_feed_hs.json",
}

# =====================
# FEEDS
# =====================
FEEDS: Dict[str, Dict[str, Any]] = {
    "main": {
        "label":   "MAIN",
        "corp_id": 98743131,
        "channel": "kill-mail",
    },
    "hs": {
        "label":   "HS",
        "corp_id": 98791781,
        "channel": "kill-mail-hs",
    },
}

CEO_ROLE     = "ARC Security Corporation Leader"
COUNCIL_ROLE = "ARC Security Administration Council"
LYCAN_ROLE   = "Lycan King"


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
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=4), encoding="utf-8")
    tmp.replace(path)

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
                ephemeral=True,
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
    def __init__(self, status: int, msg: str, retry_after: Optional[int] = None):
        super().__init__(msg)
        self.status = status
        self.retry_after = retry_after


# =====================
# COG
# =====================

class KillmailFeed(commands.Cog):

    def __init__(self, bot: commands.Bot):
        self.bot     = bot
        self.session: Optional[aiohttp.ClientSession] = None

        # Per-feed dedup
        self.posted_map:    Dict[str, Dict[str, str]] = {}
        self.km_time_cache: Dict[str, Dict[str, str]] = {}

        # Shared ESI caches
        self.name_cache:   Dict[str, str]            = {}
        self.system_cache: Dict[str, Dict[str, Any]] = {}
        self.type_cache:   Dict[str, str]            = {}

        # Per-feed diagnostics
        self.diag: Dict[str, Dict[str, Any]] = {}

        # WebSocket state
        self._ws_task:              Optional[asyncio.Task] = None
        self._ws_reconnect_delay:   int                    = WS_RECONNECT_BASE
        self._ws_connected:         bool                   = False
        self._ws_last_message_utc:  Optional[str]          = None
        self._ws_total_received:    int                    = 0

        # Catchup lock (prevents overlapping scans)
        self._catchup_lock = asyncio.Lock()

        # Load persisted state
        for feed_key in FEEDS:
            st = load_json(DATA_FILES[feed_key])
            self.posted_map[feed_key]    = st.get("posted_map",    {}) or {}
            self.km_time_cache[feed_key] = st.get("km_time_cache", {}) or {}

            if not self.name_cache:
                self.name_cache   = st.get("name_cache",   {}) or {}
            if not self.system_cache:
                self.system_cache = st.get("system_cache", {}) or {}
            if not self.type_cache:
                self.type_cache   = st.get("type_cache",   {}) or {}

            self.diag[feed_key] = {
                "last_posted_id":     st.get("last_posted_id"),
                "last_posted_time":   st.get("last_posted_time"),
                "last_esi_error":     st.get("last_esi_error"),
                "last_send_error":    st.get("last_send_error"),
                "send_failures":      int(st.get("send_failures",     0) or 0),
                "ws_kills_received":  int(st.get("ws_kills_received",  0) or 0),
                "catchup_posted":     int(st.get("catchup_posted",     0) or 0),
            }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def cog_unload(self):
        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
        if self.catchup_poll.is_running():
            self.catchup_poll.cancel()
        if self.session and not self.session.closed:
            asyncio.create_task(self.session.close())

    @commands.Cog.listener()
    async def on_ready(self):
        await self._ensure_channels()
        self._start_websocket()
        if not self.catchup_poll.is_running():
            self.catchup_poll.start()

    def _start_websocket(self):
        if self._ws_task and not self._ws_task.done():
            return
        self._ws_task = asyncio.create_task(self._ws_run())

    # ------------------------------------------------------------------
    # HTTP session
    # ------------------------------------------------------------------

    async def _ensure_session(self):
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=40)
            self.session = aiohttp.ClientSession(timeout=timeout)

    # ------------------------------------------------------------------
    # Guild / channel helpers
    # ------------------------------------------------------------------

    def target_guilds(self) -> List[discord.Guild]:
        if PRIMARY_GUILD_ID is None:
            return list(self.bot.guilds)
        g = self.bot.get_guild(PRIMARY_GUILD_ID)
        return [g] if g else []

    async def _ensure_channels(self):
        for cfg in FEEDS.values():
            for guild in self.target_guilds():
                try:
                    await self._get_or_create_channel(guild, cfg["channel"])
                except Exception:
                    pass

    async def _get_or_create_channel(
        self, guild: discord.Guild, channel_name: str
    ) -> Optional[discord.TextChannel]:
        ch = discord.utils.get(guild.text_channels, name=channel_name)
        if ch:
            return ch
        try:
            me = guild.me or guild.get_member(self.bot.user.id)
            overwrites = {
                guild.default_role: discord.PermissionOverwrite(
                    send_messages=False, add_reactions=False, read_messages=True
                ),
            }
            if me:
                overwrites[me] = discord.PermissionOverwrite(
                    send_messages=True, embed_links=True, read_messages=True
                )
            return await guild.create_text_channel(
                channel_name, overwrites=overwrites, reason="Killmail feed channel"
            )
        except Exception:
            return None

    def _check_perms(self, guild: discord.Guild, ch: discord.TextChannel) -> Optional[str]:
        me = guild.me or guild.get_member(self.bot.user.id)
        if not me:
            return "Could not resolve bot member."
        perms = ch.permissions_for(me)
        missing = [p for p in ("view_channel", "send_messages", "embed_links")
                   if not getattr(perms, p)]
        return f"Missing permissions: {', '.join(missing)}" if missing else None

    # ------------------------------------------------------------------
    # WebSocket listener
    # ------------------------------------------------------------------

    async def _ws_run(self):
        """
        Persistent WebSocket loop with exponential back-off reconnection.
        Subscribes to all corp channels and dispatches incoming kills.
        """
        await self.bot.wait_until_ready()

        while True:
            try:
                await self._ws_connect_and_listen()
                # Clean server-side close → reset back-off
                self._ws_reconnect_delay = WS_RECONNECT_BASE
            except asyncio.CancelledError:
                return
            except Exception as exc:
                print(f"[killmail_ws] Connection error: {type(exc).__name__}: {exc}")

            self._ws_connected = False
            delay = self._ws_reconnect_delay
            print(f"[killmail_ws] Reconnecting in {delay}s …")
            await asyncio.sleep(delay)
            self._ws_reconnect_delay = min(self._ws_reconnect_delay * 2, WS_RECONNECT_MAX)

    async def _ws_connect_and_listen(self):
        await self._ensure_session()

        async with self.session.ws_connect(
            ZKILL_WS_URL,
            headers={"User-Agent": USER_AGENT},
            heartbeat=WS_HEARTBEAT,
            max_msg_size=0,
        ) as ws:
            self._ws_connected        = True
            self._ws_reconnect_delay  = WS_RECONNECT_BASE
            print(f"[killmail_ws] Connected to {ZKILL_WS_URL}")

            # Subscribe once per corp
            for feed_key, cfg in FEEDS.items():
                sub = json.dumps({"action": "sub", "channel": f"corporation:{cfg['corp_id']}"})
                await ws.send_str(sub)
                print(f"[killmail_ws] Subscribed corporation:{cfg['corp_id']} ({feed_key})")

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_ws_message(msg.data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    raise RuntimeError(f"WS error frame: {ws.exception()}")
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING):
                    break

    async def _handle_ws_message(self, raw: str):
        """Parse one pushed killmail and fire-and-forget enrichment."""
        try:
            data = json.loads(raw)
        except Exception:
            return
        if not isinstance(data, dict):
            return

        kmid = safe_int(data.get("killmail_id") or data.get("killID"))
        if not kmid:
            return

        zkb    = data.get("zkb") or {}
        kmhash = zkb.get("hash") or data.get("hash")
        if not isinstance(kmhash, str) or not kmhash:
            return

        self._ws_total_received     += 1
        self._ws_last_message_utc    = utcnow_iso()

        # Don't block the receive loop — enrich in background
        asyncio.create_task(self._process_ws_kill(kmid, kmhash, data))

    async def _process_ws_kill(self, kmid: int, kmhash: str, zkm: Dict[str, Any]):
        """Fetch ESI data then post to every feed this kill belongs to."""
        try:
            esikm = await self.fetch_esi_killmail(kmid, kmhash)
            if not isinstance(esikm, dict) or not esikm.get("killmail_time"):
                return
            await self.enrich_supporting_caches(esikm)
            iso_time = str(esikm["killmail_time"])
        except ESIHTTPError as e:
            if e.status in (420, 429):
                await asyncio.sleep(e.retry_after or ESI_RETRY_FLOOR_SECONDS)
            return
        except Exception as e:
            print(f"[killmail_ws] ESI error km {kmid}: {type(e).__name__}: {e}")
            return

        for feed_key, cfg in FEEDS.items():
            corp_id = int(cfg["corp_id"])
            tag     = self.classify_mail(esikm, corp_id)

            if tag not in ("KILL", "LOSS", "INVOLVEMENT"):
                continue
            if self._is_posted(feed_key, kmid):
                continue

            self._mark_posted(feed_key, kmid, iso_time)
            self.diag[feed_key]["ws_kills_received"] = (
                int(self.diag[feed_key].get("ws_kills_received", 0) or 0) + 1
            )

            embed = self.build_embed(zkm, esikm, corp_id=corp_id, feed_label=cfg["label"])

            for guild in self.target_guilds():
                ch = await self._get_or_create_channel(guild, cfg["channel"])
                if not ch:
                    continue
                err = self._check_perms(guild, ch)
                if err:
                    self.diag[feed_key]["last_send_error"] = err
                    continue
                try:
                    await ch.send(embed=embed)
                    self.diag[feed_key]["last_posted_id"]   = str(kmid)
                    self.diag[feed_key]["last_posted_time"] = iso_time
                except Exception as e:
                    self.diag[feed_key]["last_send_error"] = f"{type(e).__name__}: {e}"
                    self.diag[feed_key]["send_failures"] = (
                        int(self.diag[feed_key].get("send_failures", 0) or 0) + 1
                    )

            await self.persist(feed_key)

    # ------------------------------------------------------------------
    # Safety-net catchup poll
    # ------------------------------------------------------------------

    @tasks.loop(minutes=CATCHUP_POLL_MINUTES)
    async def catchup_poll(self):
        """
        Runs every CATCHUP_POLL_MINUTES regardless of WebSocket health.
        Posts anything missed during a disconnect window.
        """
        if self._catchup_lock.locked():
            return
        async with self._catchup_lock:
            for feed_key, cfg in FEEDS.items():
                try:
                    await self._catchup_feed(feed_key, cfg)
                except Exception as e:
                    print(f"[killmail_catchup] {feed_key} error: {type(e).__name__}: {e}")

    @catchup_poll.before_loop
    async def _before_catchup(self):
        await self.bot.wait_until_ready()

    async def _catchup_feed(self, feed_key: str, cfg: Dict[str, Any]):
        new_kills = await self._fetch_zkill_frontier(
            int(cfg["corp_id"]), feed_key, max_pages=CATCHUP_PAGES
        )
        if not new_kills:
            return

        # Parallel ESI enrichment
        sem = asyncio.Semaphore(ESI_CONCURRENCY)

        async def one(kmid: int, zkm: Dict[str, Any]):
            kmhash = self._extract_hash(zkm)
            if not kmhash:
                return None
            async with sem:
                try:
                    esikm = await self.fetch_esi_killmail(kmid, kmhash)
                    if not isinstance(esikm, dict) or not esikm.get("killmail_time"):
                        return None
                    ktime = parse_killmail_time(str(esikm["killmail_time"]))
                    if not ktime:
                        return None
                    self.km_time_cache[feed_key][str(kmid)] = str(esikm["killmail_time"])
                    await self.enrich_supporting_caches(esikm)
                    return (ktime, kmid, zkm, esikm)
                except ESIHTTPError as e:
                    if e.status in (420, 429):
                        raise
                    return None
                except Exception:
                    return None

        results = await asyncio.gather(
            *[one(kmid, zkm) for kmid, zkm in new_kills],
            return_exceptions=True,
        )
        for r in results:
            if isinstance(r, ESIHTTPError) and r.status in (420, 429):
                raise r

        enriched = sorted(
            [r for r in results if isinstance(r, tuple)],
            key=lambda t: (t[0], t[1]),
        )

        posted = 0
        corp_id    = int(cfg["corp_id"])
        feed_label = str(cfg["label"])

        for _dt, kmid, zkm, esikm in enriched:
            if self._is_posted(feed_key, kmid):
                continue
            iso_time = str(esikm["killmail_time"])
            self._mark_posted(feed_key, kmid, iso_time)
            embed = self.build_embed(zkm, esikm, corp_id=corp_id, feed_label=feed_label)

            for guild in self.target_guilds():
                ch = await self._get_or_create_channel(guild, cfg["channel"])
                if not ch:
                    continue
                if self._check_perms(guild, ch):
                    continue
                try:
                    await ch.send(embed=embed)
                    self.diag[feed_key]["last_posted_id"]   = str(kmid)
                    self.diag[feed_key]["last_posted_time"] = iso_time
                    posted += 1
                except Exception:
                    pass

        if posted:
            self.diag[feed_key]["catchup_posted"] = (
                int(self.diag[feed_key].get("catchup_posted", 0) or 0) + posted
            )
            print(f"[killmail_catchup] {feed_label}: {posted} missed kill(s) posted.")
            await self.persist(feed_key)

    # ------------------------------------------------------------------
    # zKillboard HTTP helpers
    # ------------------------------------------------------------------

    async def _fetch_zkill_page(
        self, corp_id: int, page: int, *, mode: str
    ) -> List[Dict[str, Any]]:
        await self._ensure_session()
        url     = f"https://zkillboard.com/api/{mode}/corporationID/{corp_id}/page/{page}/"
        headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"}
        async with self.session.get(url, headers=headers) as resp:
            if resp.status == 429:
                raise RuntimeError("Rate limited by zKill (429).")
            if resp.status >= 400:
                txt = await resp.text()
                raise RuntimeError(f"zKill HTTP {resp.status}: {txt[:200]}")
            data = await resp.json(content_type=None)
            return data if isinstance(data, list) else []

    async def _fetch_zkill_frontier(
        self, corp_id: int, feed_key: str, max_pages: int = CATCHUP_PAGES
    ) -> List[Tuple[int, Dict[str, Any]]]:
        """
        Pages through zKill newest-first, stopping per-endpoint as soon as a
        full page is entirely already-posted (frontier detection).
        Kills and losses pages are fetched in parallel per page number.
        Returns [(kmid, zkm)] for unposted kills only.
        """
        merged: Dict[int, Dict[str, Any]] = {}
        kills_done = losses_done = False

        for page in range(1, max_pages + 1):
            if kills_done and losses_done:
                break

            modes = []
            if not kills_done:
                modes.append("kills")
            if not losses_done:
                modes.append("losses")

            page_results = await asyncio.gather(
                *[self._fetch_zkill_page(corp_id, page, mode=m) for m in modes],
                return_exceptions=True,
            )

            for mode, rows in zip(modes, page_results):
                if isinstance(rows, BaseException) or not isinstance(rows, list) or not rows:
                    if mode == "kills":
                        kills_done = True
                    else:
                        losses_done = True
                    continue

                all_known = True
                for km in rows:
                    kmid = self._extract_killmail_id(km)
                    if not kmid:
                        continue
                    if not self._is_posted(feed_key, kmid):
                        all_known = False
                        merged[kmid] = km

                if all_known:
                    if mode == "kills":
                        kills_done = True
                    else:
                        losses_done = True

            if not (kills_done and losses_done) and ZKILL_REQUEST_DELAY:
                await asyncio.sleep(ZKILL_REQUEST_DELAY)

        return list(merged.items())

    async def _fetch_zkill_one(self, killmail_id: int) -> Optional[Dict[str, Any]]:
        await self._ensure_session()
        url     = f"https://zkillboard.com/api/killID/{killmail_id}/"
        headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"}
        async with self.session.get(url, headers=headers) as resp:
            if resp.status >= 400:
                return None
            data = await resp.json(content_type=None)
            if isinstance(data, list) and data and isinstance(data[0], dict):
                return data[0]
            return None

    # ------------------------------------------------------------------
    # ESI helpers
    # ------------------------------------------------------------------

    async def _esi_get(self, url: str) -> Any:
        await self._ensure_session()
        headers = {
            "User-Agent":      USER_AGENT,
            "Accept":          "application/json",
            "Accept-Encoding": "gzip",
        }
        async with self.session.get(url, headers=headers) as resp:
            status = resp.status
            ra     = resp.headers.get("Retry-After")
            retry  = safe_int(ra) if ra else None
            if status in (420, 429):
                txt = await resp.text()
                raise ESIHTTPError(status, f"ESI rate limit {status}: {txt[:200]}", retry)
            if status >= 400:
                txt = await resp.text()
                raise ESIHTTPError(status, f"ESI HTTP {status}: {txt[:200]}")
            data = await resp.json(content_type=None)
        if ESI_REQUEST_DELAY:
            await asyncio.sleep(ESI_REQUEST_DELAY)
        return data

    async def _esi_post(self, url: str, payload: Any) -> Any:
        await self._ensure_session()
        headers = {
            "User-Agent":      USER_AGENT,
            "Accept":          "application/json",
            "Content-Type":    "application/json",
            "Accept-Encoding": "gzip",
        }
        async with self.session.post(url, headers=headers, json=payload) as resp:
            status = resp.status
            ra     = resp.headers.get("Retry-After")
            retry  = safe_int(ra) if ra else None
            if status in (420, 429):
                txt = await resp.text()
                raise ESIHTTPError(status, f"ESI rate limit {status}: {txt[:200]}", retry)
            if status >= 400:
                txt = await resp.text()
                raise ESIHTTPError(status, f"ESI HTTP {status}: {txt[:200]}")
            data = await resp.json(content_type=None)
        if ESI_REQUEST_DELAY:
            await asyncio.sleep(ESI_REQUEST_DELAY)
        return data

    async def fetch_esi_killmail(self, km_id: int, km_hash: str) -> Dict[str, Any]:
        data = await self._esi_get(f"{ESI_BASE}/killmails/{km_id}/{km_hash}/")
        return data if isinstance(data, dict) else {}

    async def resolve_universe_names(self, ids: List[int]) -> None:
        ask = [i for i in set(ids) if i > 0 and str(i) not in self.name_cache]
        if not ask:
            return
        result = await self._esi_post(f"{ESI_BASE}/universe/names/", ask)
        if not isinstance(result, list):
            return
        for row in result:
            _id = str(row.get("id"))
            _nm = row.get("name")
            if _id and isinstance(_nm, str):
                self.name_cache[_id] = _nm
        self.name_cache = clamp_dict(self.name_cache, MAX_NAME_CACHE)

    async def resolve_system_info(self, system_id: int) -> Tuple[str, Optional[float]]:
        key    = str(system_id)
        cached = self.system_cache.get(key)
        if isinstance(cached, dict) and "name" in cached:
            try:
                sec = float(cached["security_status"]) if cached.get("security_status") is not None else None
            except Exception:
                sec = None
            return cached.get("name") or "Unknown system", sec

        data = await self._esi_get(f"{ESI_BASE}/universe/systems/{system_id}/")
        if isinstance(data, dict):
            name = data.get("name") or "Unknown system"
            try:
                sec = float(data["security_status"]) if data.get("security_status") is not None else None
            except Exception:
                sec = None
            self.system_cache[key] = {"name": name, "security_status": sec}
            self.system_cache = clamp_dict(self.system_cache, MAX_SYSTEM_CACHE)
            return name, sec
        return "Unknown system", None

    async def resolve_type_name(self, type_id: int) -> str:
        key = str(type_id)
        if key in self.type_cache:
            return self.type_cache[key]
        data = await self._esi_get(f"{ESI_BASE}/universe/types/{type_id}/")
        name = (data or {}).get("name") if isinstance(data, dict) else None
        if isinstance(name, str) and name:
            self.type_cache[key] = name
            self.type_cache = clamp_dict(self.type_cache, MAX_TYPE_CACHE)
            return name
        return "Unknown type"

    async def enrich_supporting_caches(self, esikm: Dict[str, Any]) -> None:
        if not isinstance(esikm, dict):
            return
        victim = esikm.get("victim") or {}
        fb     = self._pick_final_blow(esikm)

        ids: List[int] = []
        for k in ("character_id", "corporation_id", "alliance_id"):
            for src in (victim, fb):
                v = safe_int(src.get(k))
                if v:
                    ids.append(v)
        if ids:
            await self.resolve_universe_names(ids)

        sys_id = safe_int(esikm.get("solar_system_id"))
        if sys_id:
            await self.resolve_system_info(sys_id)

        for tid in filter(None, [safe_int(victim.get("ship_type_id")), safe_int(fb.get("ship_type_id"))]):
            await self.resolve_type_name(tid)

    # ------------------------------------------------------------------
    # Kill helpers
    # ------------------------------------------------------------------

    def _extract_killmail_id(self, km: Dict[str, Any]) -> Optional[int]:
        for key in ("killmail_id", "killID", "kill_id", "id"):
            v = safe_int(km.get(key))
            if v:
                return v
        return None

    def _extract_hash(self, km: Dict[str, Any]) -> Optional[str]:
        for src in (km.get("zkb") or {}, km):
            h = src.get("hash") if isinstance(src, dict) else None
            if isinstance(h, str) and h:
                return h
        return None

    def _pick_final_blow(self, esikm: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not isinstance(esikm, dict):
            return {}
        attackers = esikm.get("attackers") or []
        if not isinstance(attackers, list) or not attackers:
            return {}
        for a in attackers:
            if a.get("final_blow") is True:
                return a
        return max(attackers, key=lambda a: safe_int(a.get("damage_done")) or 0, default=attackers[0])

    def classify_mail(self, esikm: Optional[Dict[str, Any]], corp_id: int) -> str:
        if not isinstance(esikm, dict):
            return "UNKNOWN"
        victim = esikm.get("victim") or {}
        if safe_int(victim.get("corporation_id")) == corp_id:
            return "LOSS"
        for a in (esikm.get("attackers") or []):
            if safe_int(a.get("corporation_id")) == corp_id:
                return "KILL"
        return "NONE"

    def classify_space(self, system_id: Optional[int], sec_status: Optional[float]) -> str:
        if system_id and 31_000_000 <= system_id < 32_000_000:
            return "WH"
        if sec_status is None:
            return "Unknown"
        try:
            s = float(sec_status)
        except Exception:
            return "Unknown"
        if s >= 0.45:
            return "HS"
        if s > 0.0:
            return "LS"
        return "NS"

    # ------------------------------------------------------------------
    # Dedup / persistence
    # ------------------------------------------------------------------

    def _is_posted(self, feed_key: str, kmid: int) -> bool:
        return str(kmid) in self.posted_map[feed_key]

    def _mark_posted(self, feed_key: str, kmid: int, iso_time: str) -> None:
        self.posted_map[feed_key][str(kmid)] = iso_time

    async def persist(self, feed_key: str):
        self.name_cache   = clamp_dict(self.name_cache,   MAX_NAME_CACHE)
        self.system_cache = clamp_dict(self.system_cache, MAX_SYSTEM_CACHE)
        self.type_cache   = clamp_dict(self.type_cache,   MAX_TYPE_CACHE)
        self.km_time_cache[feed_key] = clamp_dict(self.km_time_cache[feed_key], MAX_KM_CACHE)

        d = self.diag[feed_key]
        save_json(DATA_FILES[feed_key], {
            "posted_map":        self.posted_map[feed_key],
            "km_time_cache":     self.km_time_cache[feed_key],
            "name_cache":        self.name_cache,
            "system_cache":      self.system_cache,
            "type_cache":        self.type_cache,
            "updated_utc":       utcnow_iso(),
            "last_posted_id":    d.get("last_posted_id"),
            "last_posted_time":  d.get("last_posted_time"),
            "last_esi_error":    d.get("last_esi_error"),
            "last_send_error":   d.get("last_send_error"),
            "send_failures":     d.get("send_failures",    0),
            "ws_kills_received": d.get("ws_kills_received", 0),
            "catchup_posted":    d.get("catchup_posted",    0),
        })

    # ------------------------------------------------------------------
    # Embed builder
    # ------------------------------------------------------------------

    def _linkify(self, name: str, url: Optional[str]) -> str:
        if url and isinstance(name, str) and name not in ("Unknown", "Unknown corp", "None", ""):
            return f"[{name}]({url})"
        return name or "Unknown"

    def build_embed(
        self,
        zkm: Dict[str, Any],
        esikm: Dict[str, Any],
        *,
        corp_id: int,
        feed_label: str,
    ) -> discord.Embed:
        kmid   = self._extract_killmail_id(zkm) or 0
        kmhash = self._extract_hash(zkm) or ""
        z_url  = zkill_link(kmid) if kmid else "https://zkillboard.com/"
        e_url  = esi_killmail_link(kmid, kmhash) if (kmid and kmhash) else None

        victim    = esikm.get("victim") or {}
        attackers = esikm.get("attackers") or []
        n_atk     = len(attackers) if isinstance(attackers, list) else 0

        def zk_char(i): return f"https://zkillboard.com/character/{i}/"   if i else None
        def zk_corp(i): return f"https://zkillboard.com/corporation/{i}/" if i else None
        def zk_ally(i): return f"https://zkillboard.com/alliance/{i}/"    if i else None

        v_char_id = safe_int(victim.get("character_id"))
        v_corp_id = safe_int(victim.get("corporation_id"))
        v_ally_id = safe_int(victim.get("alliance_id"))

        v_char = self._linkify(self.name_cache.get(str(v_char_id), "Unknown"),      zk_char(v_char_id))
        v_corp = self._linkify(self.name_cache.get(str(v_corp_id), "Unknown corp"), zk_corp(v_corp_id))
        v_ally = self._linkify(self.name_cache.get(str(v_ally_id), "None"),         zk_ally(v_ally_id))

        ship_id   = safe_int(victim.get("ship_type_id"))
        ship_name = self.type_cache.get(str(ship_id), "Unknown ship") if ship_id else "Unknown ship"

        sys_id     = safe_int(esikm.get("solar_system_id"))
        sys_name   = "Unknown system"
        sec_status: Optional[float] = None
        if sys_id:
            sc = self.system_cache.get(str(sys_id)) or {}
            sys_name = sc.get("name") or "Unknown system"
            try:
                sec_status = float(sc["security_status"]) if sc.get("security_status") is not None else None
            except Exception:
                pass
        space   = self.classify_space(sys_id, sec_status)
        sec_str = f"{sec_status:.2f}" if isinstance(sec_status, float) else "?"

        fb = self._pick_final_blow(esikm)
        fb_char_id = safe_int(fb.get("character_id"))
        fb_corp_id = safe_int(fb.get("corporation_id"))
        fb_ally_id = safe_int(fb.get("alliance_id"))
        fb_ship_id = safe_int(fb.get("ship_type_id"))

        fb_char = self._linkify(self.name_cache.get(str(fb_char_id), "Unknown"),      zk_char(fb_char_id))
        fb_corp = self._linkify(self.name_cache.get(str(fb_corp_id), "Unknown corp"), zk_corp(fb_corp_id))
        fb_ally = self._linkify(self.name_cache.get(str(fb_ally_id), "None"),         zk_ally(fb_ally_id))
        fb_ship = self.type_cache.get(str(fb_ship_id), "Unknown ship") if fb_ship_id else "Unknown ship"

        val   = isk_value(zkm)
        ktime = esikm.get("killmail_time")
        kdt   = parse_killmail_time(ktime) or utcnow()
        tag   = self.classify_mail(esikm, corp_id)

        color = {"KILL": discord.Color.green(), "LOSS": discord.Color.red(),
                 "INVOLVEMENT": discord.Color.gold()}.get(tag, discord.Color.light_grey())

        lines = [
            f"**Feed:** {feed_label}",
            "",
            f"**Type:** {tag}",
            "",
            f"**Victim:** {v_char}",
            f"**Corp:** {v_corp}",
            f"**Alliance:** {v_ally}",
            "",
            f"**Ship:** {ship_name}",
            f"**System:** {sys_name} ({space}, Sec: {sec_str})",
            f"**Attackers:** {n_atk}",
            "",
            f"**Final Blow:** {fb_char}",
            f"**Ship:** {fb_ship}",
            f"**Corp:** {fb_corp}",
            f"**Alliance:** {fb_ally}",
        ]
        if val is not None:
            lines += ["", f"**Est. ISK:** {val:,.0f}"]
        if ktime:
            lines.append(f"**Time:** {ktime}")
        lines += ["", f"[zKillboard]({z_url})" + (f" • [ESI]({e_url})" if e_url else "")]

        emb = discord.Embed(
            title=f"{tag} — Killmail #{kmid}",
            url=z_url,
            description="\n".join(lines),
            color=color,
            timestamp=kdt,
        )
        emb.set_footer(text="zKillboard WebSocket + ESI")
        if ship_id:
            emb.set_thumbnail(url=type_render_url(ship_id))
        return emb

    # ------------------------------------------------------------------
    # Slash commands
    # ------------------------------------------------------------------

    @app_commands.command(name="killmail_status", description="Show killmail feed status.")
    async def killmail_status(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        ws_icon = "🟢" if self._ws_connected else "🔴"
        lines = [
            f"**WebSocket:** {ws_icon} {'Connected' if self._ws_connected else 'Disconnected'}",
            f"**Last WS message (UTC):** {self._ws_last_message_utc or 'Never'}",
            f"**Total WS messages:** {self._ws_total_received}",
            f"**Catchup poll:** every {CATCHUP_POLL_MINUTES} min, last {CATCHUP_PAGES} page(s)",
            "",
        ]
        for feed_key, cfg in FEEDS.items():
            d = self.diag[feed_key]
            lines += [
                f"## {cfg['label']} ({feed_key})",
                f"Corp `{cfg['corp_id']}` → #{cfg['channel']}",
                f"WS kills received: **{d.get('ws_kills_received', 0)}**",
                f"Catchup posted: **{d.get('catchup_posted', 0)}**",
                f"Last posted ID: {d.get('last_posted_id') or 'None'}",
                f"Last posted time: {d.get('last_posted_time') or 'None'}",
                f"Send failures: {d.get('send_failures', 0)}",
                f"Last send error: {d.get('last_send_error') or 'None'}",
                f"Last ESI error: {d.get('last_esi_error') or 'None'}",
                "",
            ]

        emb = discord.Embed(
            title="Killmail Feed Status",
            description="\n".join(lines),
            timestamp=utcnow(),
        )
        emb.set_footer(text="Cryonic Gaming — WebSocket feed")
        await safe_reply(interaction, embed=emb, ephemeral=True)

    @app_commands.command(
        name="killmail_reload",
        description="Fetch and repost a specific killmail by ID (admin only).",
    )
    @require_killmail_admin()
    @app_commands.choices(feed=[
        app_commands.Choice(name="main", value="main"),
        app_commands.Choice(name="hs",   value="hs"),
    ])
    async def killmail_reload(
        self,
        interaction: discord.Interaction,
        killmail_id: int,
        feed: app_commands.Choice[str],
    ):
        await safe_defer(interaction, ephemeral=True)

        feed_key = feed.value
        cfg      = FEEDS.get(feed_key)
        if not cfg:
            await safe_reply(interaction, "❌ Invalid feed.", ephemeral=True)
            return

        kmid = safe_int(killmail_id)
        if not kmid:
            await safe_reply(interaction, "❌ Invalid killmail_id.", ephemeral=True)
            return

        zkm = await self._fetch_zkill_one(kmid)
        if not zkm:
            await safe_reply(interaction, f"❌ No zKill data for `{kmid}`.", ephemeral=True)
            return

        kmhash = self._extract_hash(zkm)
        if not kmhash:
            await safe_reply(interaction, "❌ Missing hash; cannot fetch ESI.", ephemeral=True)
            return

        try:
            esikm = await self.fetch_esi_killmail(kmid, kmhash)
        except Exception as e:
            await safe_reply(interaction, f"❌ ESI fetch failed: {type(e).__name__}: {e}", ephemeral=True)
            return

        if not isinstance(esikm, dict) or not esikm.get("killmail_time"):
            await safe_reply(interaction, "❌ ESI returned an invalid killmail.", ephemeral=True)
            return

        if not interaction.guild:
            await safe_reply(interaction, "❌ Must be used in a server.", ephemeral=True)
            return

        await self.enrich_supporting_caches(esikm)
        ch = await self._get_or_create_channel(interaction.guild, cfg["channel"])
        if not ch:
            await safe_reply(interaction, f"❌ Channel #{cfg['channel']} not found.", ephemeral=True)
            return
        err = self._check_perms(interaction.guild, ch)
        if err:
            await safe_reply(interaction, f"❌ {err}", ephemeral=True)
            return

        await ch.send(embed=self.build_embed(zkm, esikm, corp_id=int(cfg["corp_id"]), feed_label=cfg["label"]))
        iso_time = str(esikm["killmail_time"])
        self._mark_posted(feed_key, kmid, iso_time)
        self.diag[feed_key]["last_posted_id"]   = str(kmid)
        self.diag[feed_key]["last_posted_time"] = iso_time
        await self.persist(feed_key)

        await safe_reply(interaction, f"✅ Reloaded killmail `{kmid}` → **{cfg['label']}**.", ephemeral=True)

    @app_commands.command(
        name="killmail_debug_next",
        description="Run one catchup scan immediately for all feeds (admin only).",
    )
    @require_killmail_admin()
    async def killmail_debug_next(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        results: List[str] = []
        for feed_key, cfg in FEEDS.items():
            before = int(self.diag[feed_key].get("catchup_posted", 0) or 0)
            try:
                await self._catchup_feed(feed_key, cfg)
            except Exception as e:
                results.append(f"**{cfg['label']}**: error — {type(e).__name__}: {e}")
                continue
            after  = int(self.diag[feed_key].get("catchup_posted", 0) or 0)
            results.append(
                f"**{cfg['label']} ({feed_key})**: posted **{after - before}** new kill(s)\n"
                f"send_error={self.diag[feed_key].get('last_send_error') or 'None'}"
            )

        await safe_reply(interaction, "\n\n".join(results), ephemeral=True)

    @app_commands.command(
        name="killmail_ws_reconnect",
        description="Force-reconnect the zKillboard WebSocket (admin only).",
    )
    @require_killmail_admin()
    async def killmail_ws_reconnect(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(self._ws_task), timeout=3)
            except Exception:
                pass

        self._ws_reconnect_delay = WS_RECONNECT_BASE
        self._start_websocket()
        await safe_reply(interaction, "✅ WebSocket reconnect initiated.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(KillmailFeed(bot))
