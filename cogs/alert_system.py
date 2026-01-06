# cogs/alert_system.py
#
# Alert System + Wormhole Status
# Railway persistence via /data volume (or PERSIST_ROOT override).
#
# This version intentionally AVOIDS message EDITS (PATCH) for panels to prevent 429 PATCH storms.
# Panels are updated via DELETE + SEND (replace), with cooldowns.
#
# Features:
# - Per-guild state (no cross-guild panel ID collisions)
# - Persistent Views (buttons survive restart)
# - Status lights:
#     🟢 Normal
#     🟣 Lock-down
#     🔴 all other statuses
# - Buttons labeled with same lights and colored:
#     Normal = success (green)
#     Lock-down = primary (closest supported; Discord has no purple)
#     Others = danger (red)
# - Every status change pings ARC Security while ensuring only ONE bot ping message exists
# - Auto-danger from killmail embeds (supports "System: J220215 ..." and "Type: LOSS")
#
import os
import re
import discord
from discord.ext import commands
from discord import app_commands
from discord.ui import View, Button, Modal, TextInput
from pathlib import Path
import json
import datetime
import asyncio
from typing import Dict, Any, Optional, List, Tuple, Set
import io

# =====================
# CONFIG
# =====================

CHANNEL_NAME = "wormhole-status"
KILLMAIL_CHANNEL_NAME = "kill-mail"

ARC_SECURITY_ROLE = "ARC Security"

ALERT_SEND_ROLES = {
    "ARC Security Corporation Leader",
    "ARC Security Administration Council",
    "ARC General",
}

WH_STATUS_ROLES = {
    "ARC Security Corporation Leader",
    "ARC Security Administration Council",
    "ARC General",
    "ARC Commander",
    "ARC Officer",
}

HOME_SYSTEM_NAME = "J220215"
AUTO_DANGER_STATUS_VALUE = "Dangerous"
AUTO_DANGER_REVERT_VALUE = "Normal"
AUTO_DANGER_DURATION_SECONDS = 45 * 60

DM_DELAY_SECONDS = 1.2
DM_FAIL_ABORT_THRESHOLD = 25
DM_CONCURRENCY = 1

MAX_BROADCAST_HISTORY = 250

# Panel replacement cooldown (per guild) to prevent spam on reconnect loops
PANEL_REPLACE_COOLDOWN_SECONDS = 30.0

# Status panel replacement is debounced
STATUS_REFRESH_DEBOUNCE_SECONDS = 3.0

# =====================
# PERSISTENCE
# =====================

PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)
DATA_FILE = PERSIST_ROOT / "alert_system.json"

_file_lock = asyncio.Lock()

# =====================
# STATUS LIGHTS / HELPERS
# =====================

GREEN_LIGHT = "🟢"
PURPLE_LIGHT = "🟣"
RED_LIGHT = "🔴"

def utcnow() -> datetime.datetime:
    return datetime.datetime.utcnow()

def utcnow_iso() -> str:
    return utcnow().isoformat()

def normalize_status(value: str) -> str:
    v = (value or "").strip()
    if v.lower() == "lockdown":
        return "Lock-down"
    return v

def status_light(value: str) -> str:
    v = (value or "").strip().lower()
    if v == "normal":
        return GREEN_LIGHT
    if v in {"lock-down", "lockdown"}:
        return PURPLE_LIGHT
    return RED_LIGHT

def clamp_history(broadcasts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if len(broadcasts) <= MAX_BROADCAST_HISTORY:
        return broadcasts
    return broadcasts[-MAX_BROADCAST_HISTORY:]

def iso_to_dt(s: Optional[str]) -> Optional[datetime.datetime]:
    if not s:
        return None
    try:
        return datetime.datetime.fromisoformat(s)
    except Exception:
        return None

# =====================
# DEFAULT STATE (per-guild)
# =====================

def _default_guild_state() -> Dict[str, Any]:
    return {
        "opt_in": {},
        "broadcasts": [],
        "status": {"value": "Normal", "updated_utc": None, "updated_by": None},
        "panel_message_ids": {
            "channel_id": None,
            "alert": None,
            "status": None,
            "status_ping": None,
        },
        "auto_danger": {
            "active": False,
            "reset_at_utc": None,
            "last_trigger_utc": None,
            "last_trigger_killmail_id": None,
            "last_trigger_tag": None,
            "last_trigger_system": None,
        },
    }

def _default_state() -> Dict[str, Any]:
    return {"guilds": {}}

# =====================
# JSON IO
# =====================

def _safe_read_json(p: Path) -> Dict[str, Any]:
    try:
        if not p.exists():
            return {}
        txt = p.read_text(encoding="utf-8").strip()
        if not txt:
            return {}
        return json.loads(txt)
    except Exception:
        return {}

def _atomic_write_json(p: Path, d: Dict[str, Any]) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(d, indent=4), encoding="utf-8")
    tmp.replace(p)

async def load_state() -> Dict[str, Any]:
    async with _file_lock:
        s = _safe_read_json(DATA_FILE)
        if not isinstance(s, dict) or not s:
            return _default_state()

        # Migrate old flat schema -> per-guild
        if "guilds" not in s:
            migrated = _default_state()
            old_panel = (s.get("panel_message_ids") or {})
            old_gid = old_panel.get("guild_id")
            gid_key = str(old_gid) if old_gid else "unknown"

            gs = _default_guild_state()
            gs["opt_in"] = s.get("opt_in", {}) or {}
            gs["broadcasts"] = s.get("broadcasts", []) or []
            gs["status"] = s.get("status", gs["status"]) or gs["status"]
            gs["auto_danger"] = s.get("auto_danger", gs["auto_danger"]) or gs["auto_danger"]

            gs["panel_message_ids"]["channel_id"] = old_panel.get("channel_id")
            gs["panel_message_ids"]["alert"] = old_panel.get("alert")
            gs["panel_message_ids"]["status"] = old_panel.get("status")
            gs["panel_message_ids"]["status_ping"] = old_panel.get("status_ping")

            migrated["guilds"][gid_key] = gs
            s = migrated

        s.setdefault("guilds", {})
        if not isinstance(s["guilds"], dict):
            s["guilds"] = {}

        return s

async def save_state(state: Dict[str, Any]) -> None:
    async with _file_lock:
        _atomic_write_json(DATA_FILE, state)

# =====================
# PERMISSIONS
# =====================

def has_any_role(member: discord.Member, role_names: set) -> bool:
    return any(r.name in role_names for r in getattr(member, "roles", []))

def has_role(member: discord.Member, role_name: str) -> bool:
    return any(r.name == role_name for r in getattr(member, "roles", []))

def require_alert_sender_roles():
    async def predicate(interaction: discord.Interaction) -> bool:
        if not isinstance(interaction.user, discord.Member):
            return False
        return has_any_role(interaction.user, ALERT_SEND_ROLES)
    return app_commands.check(predicate)

# =====================
# MODALS
# =====================

class AlertMessageModal(Modal):
    def __init__(self, cog: "AlertSystemCog"):
        super().__init__(title="Send Alert to ARC Security (DM)")
        self.cog = cog
        self.msg = TextInput(
            label="Alert message",
            placeholder="Keep it short, clear, and operational.",
            required=True,
            max_length=1500,
            style=discord.TextStyle.paragraph,
        )
        self.add_item(self.msg)

    async def on_submit(self, interaction: discord.Interaction):
        await self.cog.handle_send_alert(interaction, str(self.msg.value))

# =====================
# VIEWS
# =====================

class AlertPanelView(View):
    def __init__(self, cog: "AlertSystemCog"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Alert", style=discord.ButtonStyle.danger, custom_id="arc_alert:send")
    async def alert_send(self, interaction: discord.Interaction, button: Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("This must be used in a server.", ephemeral=True)
            return
        if not has_any_role(interaction.user, ALERT_SEND_ROLES):
            await interaction.response.send_message("You do not have permission to send alerts.", ephemeral=True)
            return
        await interaction.response.send_modal(AlertMessageModal(self.cog))

    @discord.ui.button(label="Enable DMs", style=discord.ButtonStyle.success, custom_id="arc_alert:optin")
    async def opt_in(self, interaction: discord.Interaction, button: Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("This must be used in a server.", ephemeral=True)
            return
        if not has_role(interaction.user, ARC_SECURITY_ROLE):
            await interaction.response.send_message(f"Only members with `{ARC_SECURITY_ROLE}` can opt in.", ephemeral=True)
            return
        await self.cog.set_opt_in(interaction.guild, interaction.user, True)
        await interaction.response.send_message("You are now opted-in to ARC Security DM alerts.", ephemeral=True)

    @discord.ui.button(label="Disable DMs", style=discord.ButtonStyle.secondary, custom_id="arc_alert:optout")
    async def opt_out(self, interaction: discord.Interaction, button: Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("This must be used in a server.", ephemeral=True)
            return
        if not has_role(interaction.user, ARC_SECURITY_ROLE):
            await interaction.response.send_message(f"Only members with `{ARC_SECURITY_ROLE}` can opt out.", ephemeral=True)
            return
        await self.cog.set_opt_in(interaction.guild, interaction.user, False)
        await interaction.response.send_message("You are now opted-out of ARC Security DM alerts.", ephemeral=True)

class WormholeStatusView(View):
    def __init__(self, cog: "AlertSystemCog"):
        super().__init__(timeout=None)
        self.cog = cog

    async def _set(self, interaction: discord.Interaction, value: str):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("This must be used in a server.", ephemeral=True)
            return
        if not has_any_role(interaction.user, WH_STATUS_ROLES):
            await interaction.response.send_message("You do not have permission to update wormhole status.", ephemeral=True)
            return

        value = normalize_status(value)
        await self.cog.update_status(
            guild=interaction.guild,
            value=value,
            updated_by=f"{interaction.user} (ID: {interaction.user.id})",
            ping_role=True,
            context_note="Manual status change",
        )
        await interaction.response.send_message(f"Wormhole status set to `{value}`.", ephemeral=True)

    @discord.ui.button(label=f"{GREEN_LIGHT} Normal", style=discord.ButtonStyle.success, custom_id="wh_status:normal")
    async def normal(self, interaction: discord.Interaction, button: Button):
        await self._set(interaction, "Normal")

    @discord.ui.button(label=f"{RED_LIGHT} Dangerous", style=discord.ButtonStyle.danger, custom_id="wh_status:dangerous")
    async def dangerous(self, interaction: discord.Interaction, button: Button):
        await self._set(interaction, "Dangerous")

    @discord.ui.button(label=f"{RED_LIGHT} Enemy Fleet Spotted", style=discord.ButtonStyle.danger, custom_id="wh_status:enemy")
    async def enemy(self, interaction: discord.Interaction, button: Button):
        await self._set(interaction, "Enemy Fleet Spotted")

    # Discord does not have a purple button; primary is closest.
    @discord.ui.button(label=f"{PURPLE_LIGHT} Lock-down", style=discord.ButtonStyle.primary, custom_id="wh_status:lockdown")
    async def lockdown(self, interaction: discord.Interaction, button: Button):
        await self._set(interaction, "Lock-down")

    @discord.ui.button(label=f"{RED_LIGHT} Under attack", style=discord.ButtonStyle.danger, custom_id="wh_status:attack")
    async def attack(self, interaction: discord.Interaction, button: Button):
        await self._set(interaction, "Under attack")

# =====================
# COG
# =====================

class AlertSystemCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state: Dict[str, Any] = _default_state()

        self.bot.add_view(AlertPanelView(self))
        self.bot.add_view(WormholeStatusView(self))

        self._send_lock = asyncio.Lock()
        self._auto_setup_done: Set[int] = set()

        self._danger_reset_tasks: Dict[int, asyncio.Task] = {}
        self._panel_locks: Dict[int, asyncio.Lock] = {}

        self._status_refresh_tasks: Dict[int, asyncio.Task] = {}
        self._last_panel_replace_at: Dict[int, float] = {}

    def _gkey(self, guild: discord.Guild) -> str:
        return str(guild.id)

    def _ensure_guild_state(self, guild: discord.Guild) -> Dict[str, Any]:
        self.state.setdefault("guilds", {})
        gs = self.state["guilds"].get(self._gkey(guild))
        if not isinstance(gs, dict):
            gs = _default_guild_state()
            self.state["guilds"][self._gkey(guild)] = gs
        gs.setdefault("opt_in", {})
        gs.setdefault("broadcasts", [])
        gs.setdefault("status", {"value": "Normal", "updated_utc": None, "updated_by": None})
        gs.setdefault("panel_message_ids", _default_guild_state()["panel_message_ids"])
        gs.setdefault("auto_danger", _default_guild_state()["auto_danger"])
        gs["panel_message_ids"].setdefault("status_ping", None)
        return gs

    def _lock(self, guild_id: int) -> asyncio.Lock:
        if guild_id not in self._panel_locks:
            self._panel_locks[guild_id] = asyncio.Lock()
        return self._panel_locks[guild_id]

    async def cog_load(self):
        self.state = await load_state()
        for g in self.bot.guilds:
            await self._resume_auto_danger_timer_if_needed(g)

    def cog_unload(self):
        for t in list(self._danger_reset_tasks.values()):
            if t and not t.done():
                t.cancel()
        for t in list(self._status_refresh_tasks.values()):
            if t and not t.done():
                t.cancel()

    @commands.Cog.listener()
    async def on_ready(self):
        # Stagger to avoid bursts on reconnects
        for g in self.bot.guilds:
            if g.id in self._auto_setup_done:
                continue
            try:
                await self.upsert_panels(g, force=False)
            except Exception:
                pass
            self._auto_setup_done.add(g.id)
            await asyncio.sleep(1.0)

        for g in self.bot.guilds:
            try:
                await self._resume_auto_danger_timer_if_needed(g)
            except Exception:
                continue

    # =====================
    # Channels
    # =====================

    async def ensure_channel(self, guild: discord.Guild) -> discord.TextChannel:
        ch = discord.utils.get(guild.text_channels, name=CHANNEL_NAME)
        if ch:
            return ch

        overwrites = {
            guild.default_role: discord.PermissionOverwrite(send_messages=False, add_reactions=False),
        }
        if guild.me:
            overwrites[guild.me] = discord.PermissionOverwrite(send_messages=True, embed_links=True, read_messages=True)

        return await guild.create_text_channel(CHANNEL_NAME, overwrites=overwrites, reason="Wormhole status channel")

    # =====================
    # Embeds
    # =====================

    def build_alert_panel_embed(self, guild: discord.Guild) -> discord.Embed:
        role = discord.utils.get(guild.roles, name=ARC_SECURITY_ROLE)
        opted_in_count = 0
        if role:
            for m in role.members:
                if not m.bot and self.is_opted_in(guild, m.id):
                    opted_in_count += 1

        emb = discord.Embed(
            title="ARC Security Alert System",
            description=(
                "**Leadership:** Use `Alert` to DM an operational alert.\n"
                f"**Recipients:** Members with `{ARC_SECURITY_ROLE}` who have opted in.\n\n"
                f"**Opted-in recipients (current):** `{opted_in_count}`\n"
            ),
            timestamp=utcnow(),
        )
        emb.set_footer(text="Cryonic Gaming bot — Alerts")
        return emb

    def build_status_embed(self, guild: discord.Guild) -> discord.Embed:
        gs = self._ensure_guild_state(guild)
        st = gs.get("status", {}) or {}
        value = normalize_status(st.get("value", "Normal"))
        updated_utc = st.get("updated_utc") or "Never"
        updated_by = st.get("updated_by") or "N/A"

        ad = gs.get("auto_danger", {}) or {}
        ad_line = ""
        if ad.get("active"):
            ad_line = f"\n**Auto-Revert (UTC):** `{ad.get('reset_at_utc') or 'Unknown'}`"

        light = status_light(value)

        # "Increase size" effect: put status in TITLE (bigger than description)
        emb = discord.Embed(
            title=f"{light} Wormhole Status: {value}",
            description=(
                f"**Last Updated (UTC):** `{updated_utc}`\n"
                f"**Updated By:** `{updated_by}`\n"
                f"{ad_line}"
            ),
            timestamp=utcnow(),
        )
        emb.set_footer(text="Cryonic Gaming bot — Wormhole Status")
        return emb

    # =====================
    # Panel management (NO EDITS)
    # =====================

    async def _safe_delete_if_exists(self, ch: discord.TextChannel, msg_id: Optional[int]) -> None:
        if not msg_id:
            return
        try:
            msg = await ch.fetch_message(int(msg_id))
            if self.bot.user and msg.author.id == self.bot.user.id:
                await msg.delete()
        except Exception:
            return

    async def upsert_panels(self, guild: discord.Guild, force: bool = False):
        # Replace-by-delete+send only, guarded by cooldown.
        async with self._lock(guild.id):
            now = asyncio.get_event_loop().time()
            last = self._last_panel_replace_at.get(guild.id, 0.0)
            if not force and (now - last) < PANEL_REPLACE_COOLDOWN_SECONDS:
                return

            self._last_panel_replace_at[guild.id] = now

            gs = self._ensure_guild_state(guild)
            ch = await self.ensure_channel(guild)

            pm = gs.get("panel_message_ids", {}) or {}
            # Delete old panels if they exist (no PATCH)
            await self._safe_delete_if_exists(ch, pm.get("alert"))
            await self._safe_delete_if_exists(ch, pm.get("status"))

            # Recreate panels
            alert_msg = await ch.send(embed=self.build_alert_panel_embed(guild), view=AlertPanelView(self))
            status_msg = await ch.send(embed=self.build_status_embed(guild), view=WormholeStatusView(self))

            pm["channel_id"] = ch.id
            pm["alert"] = alert_msg.id
            pm["status"] = status_msg.id
            pm.setdefault("status_ping", None)

            gs["panel_message_ids"] = pm
            self.state["guilds"][self._gkey(guild)] = gs
            await save_state(self.state)

    async def _replace_status_panel(self, guild: discord.Guild):
        async with self._lock(guild.id):
            gs = self._ensure_guild_state(guild)
            ch = await self.ensure_channel(guild)
            pm = gs.get("panel_message_ids", {}) or {}

            # Delete existing status panel and send new one (no PATCH)
            await self._safe_delete_if_exists(ch, pm.get("status"))
            status_msg = await ch.send(embed=self.build_status_embed(guild), view=WormholeStatusView(self))
            pm["channel_id"] = ch.id
            pm["status"] = status_msg.id

            gs["panel_message_ids"] = pm
            self.state["guilds"][self._gkey(guild)] = gs
            await save_state(self.state)

    async def refresh_status_panel_debounced(self, guild: discord.Guild):
        existing = self._status_refresh_tasks.get(guild.id)
        if existing and not existing.done():
            return

        async def runner():
            try:
                await asyncio.sleep(STATUS_REFRESH_DEBOUNCE_SECONDS)
                await self._replace_status_panel(guild)
            except asyncio.CancelledError:
                return
            except Exception:
                return

        self._status_refresh_tasks[guild.id] = asyncio.create_task(runner())

    # =====================
    # Single ping message (one at a time)
    # =====================

    async def _replace_status_ping_message(
        self,
        *,
        guild: discord.Guild,
        status_value: str,
        updated_by: str,
        context_note: Optional[str],
        ping_role: bool,
    ):
        async with self._lock(guild.id):
            gs = self._ensure_guild_state(guild)
            ch = await self.ensure_channel(guild)
            pm = gs.get("panel_message_ids", {}) or {}

            await self._safe_delete_if_exists(ch, pm.get("status_ping"))

            role = discord.utils.get(guild.roles, name=ARC_SECURITY_ROLE)
            mention = role.mention if role else f"`{ARC_SECURITY_ROLE}`"

            status_value = normalize_status(status_value)
            light = status_light(status_value)

            emb = discord.Embed(
                title=f"{light} STATUS CHANGE: {status_value}",
                description=(
                    f"{mention if ping_role else ''}\n\n"
                    f"**New Status:** `{status_value}`\n"
                    f"**Updated By:** `{updated_by}`\n"
                    + (f"**Context:** {context_note}\n" if context_note else "")
                    + f"**Time (UTC):** `{utcnow().isoformat()}`\n"
                ),
                timestamp=utcnow(),
            )
            emb.set_footer(text="Cryonic Gaming bot — Status Notification")

            content = mention if (ping_role and role) else None
            allowed = discord.AllowedMentions(roles=[role] if role else [], everyone=False, users=False)

            new_msg = await ch.send(content=content, embed=emb, allowed_mentions=allowed)

            pm["channel_id"] = ch.id
            pm["status_ping"] = new_msg.id
            gs["panel_message_ids"] = pm
            self.state["guilds"][self._gkey(guild)] = gs
            await save_state(self.state)

    # =====================
    # Status update entrypoint
    # =====================

    async def update_status(
        self,
        *,
        guild: discord.Guild,
        value: str,
        updated_by: str,
        ping_role: bool = True,
        context_note: Optional[str] = None,
    ):
        value = normalize_status(value)

        gs = self._ensure_guild_state(guild)
        gs["status"] = {"value": value, "updated_utc": utcnow_iso(), "updated_by": updated_by}
        self.state["guilds"][self._gkey(guild)] = gs
        await save_state(self.state)

        # Ping message replaced immediately (delete old + send new)
        try:
            await self._replace_status_ping_message(
                guild=guild,
                status_value=value,
                updated_by=updated_by,
                context_note=context_note,
                ping_role=ping_role,
            )
        except Exception:
            pass

        # Status panel replaced on debounce (delete+send; no PATCH)
        await self.refresh_status_panel_debounced(guild)

    # =====================
    # Killmail integration
    # =====================

    def _parse_killmail_embed(self, emb: discord.Embed) -> Tuple[Optional[str], Optional[str], Optional[int]]:
        tag: Optional[str] = None
        kmid: Optional[int] = None
        system_name: Optional[str] = None

        title = (emb.title or "").strip()
        m = re.match(r"^(KILL|LOSS|INVOLVEMENT|UNKNOWN)\s+—\s+Killmail\s+#(\d+)\s*$", title, re.IGNORECASE)
        if m:
            tag = m.group(1).upper()
            try:
                kmid = int(m.group(2))
            except Exception:
                kmid = None
        else:
            m2 = re.search(r"#(\d+)", title)
            if m2:
                try:
                    kmid = int(m2.group(1))
                except Exception:
                    kmid = None
            mtag = re.match(r"^(KILL|LOSS|INVOLVEMENT|UNKNOWN)\b", title, re.IGNORECASE)
            if mtag:
                tag = mtag.group(1).upper()

        desc = emb.description or ""

        # System line variants:
        #   "**System:** J220215 ..."
        #   "System: J220215 (WH, Sec: -0.99)"
        m3 = re.search(r"\*\*System:\*\*\s*([^\n]+?)(?:\s*\(|\s*$)", desc, re.IGNORECASE)
        if m3:
            system_name = m3.group(1).strip()

        if not system_name:
            m4 = re.search(r"^System:\s*([^\s(]+)", desc, re.IGNORECASE | re.MULTILINE)
            if m4:
                system_name = m4.group(1).strip()

        # Type line variant:
        #   "Type: LOSS"
        if not tag:
            m5 = re.search(r"^Type:\s*(KILL|LOSS|INVOLVEMENT|UNKNOWN)\s*$", desc, re.IGNORECASE | re.MULTILINE)
            if m5:
                tag = m5.group(1).upper()

        return system_name, tag, kmid

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild or not self.bot.user:
            return
        if message.author.id != self.bot.user.id:
            return
        if not isinstance(message.channel, discord.TextChannel):
            return
        if message.channel.name != KILLMAIL_CHANNEL_NAME:
            return
        if not message.embeds:
            return

        emb = message.embeds[0]
        system_name, tag, kmid = self._parse_killmail_embed(emb)
        if not system_name or not tag:
            return
        if system_name != HOME_SYSTEM_NAME:
            return
        if tag not in {"KILL", "LOSS"}:
            return

        await self._trigger_auto_danger(
            guild=message.guild,
            system_name=system_name,
            tag=tag,
            killmail_id=kmid or 0
        )

    async def _trigger_auto_danger(self, *, guild: discord.Guild, system_name: str, tag: str, killmail_id: int):
        gs = self._ensure_guild_state(guild)

        reset_at = utcnow() + datetime.timedelta(seconds=AUTO_DANGER_DURATION_SECONDS)
        gs.setdefault("auto_danger", _default_guild_state()["auto_danger"])
        gs["auto_danger"].update({
            "active": True,
            "reset_at_utc": reset_at.isoformat(),
            "last_trigger_utc": utcnow_iso(),
            "last_trigger_killmail_id": killmail_id,
            "last_trigger_tag": tag,
            "last_trigger_system": system_name,
        })
        self.state["guilds"][self._gkey(guild)] = gs
        await save_state(self.state)

        old = self._danger_reset_tasks.get(guild.id)
        if old and not old.done():
            old.cancel()
        self._danger_reset_tasks[guild.id] = asyncio.create_task(self._auto_revert_after_delay(guild, reset_at))

        await self.update_status(
            guild=guild,
            value=AUTO_DANGER_STATUS_VALUE,
            updated_by=f"Auto: {tag} detected in {system_name} (Killmail {killmail_id})",
            ping_role=True,
            context_note=f"Auto-trigger from killmail {killmail_id}",
        )

    async def _auto_revert_after_delay(self, guild: discord.Guild, reset_at: datetime.datetime):
        try:
            delay = (reset_at - utcnow()).total_seconds()
            if delay > 0:
                await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return
        except Exception:
            try:
                await asyncio.sleep(AUTO_DANGER_DURATION_SECONDS)
            except Exception:
                return

        gs = self._ensure_guild_state(guild)
        ad = gs.get("auto_danger", {}) or {}
        if not ad.get("active"):
            return

        reset_at_utc = iso_to_dt(ad.get("reset_at_utc"))
        if reset_at_utc and utcnow() < reset_at_utc:
            return

        gs["auto_danger"].update({"active": False, "reset_at_utc": None})
        self.state["guilds"][self._gkey(guild)] = gs
        await save_state(self.state)

        await self.update_status(
            guild=guild,
            value=AUTO_DANGER_REVERT_VALUE,
            updated_by="Auto: danger timer elapsed",
            ping_role=True,
            context_note="Auto-revert after timer",
        )

    async def _resume_auto_danger_timer_if_needed(self, guild: discord.Guild):
        gs = self._ensure_guild_state(guild)
        ad = gs.get("auto_danger", {}) or {}
        if not ad.get("active"):
            return

        reset_at = iso_to_dt(ad.get("reset_at_utc"))
        if not reset_at:
            gs["auto_danger"]["active"] = False
            gs["auto_danger"]["reset_at_utc"] = None
            self.state["guilds"][self._gkey(guild)] = gs
            await save_state(self.state)
            return

        if utcnow() >= reset_at:
            gs["auto_danger"]["active"] = False
            gs["auto_danger"]["reset_at_utc"] = None
            self.state["guilds"][self._gkey(guild)] = gs
            await save_state(self.state)
            await self.update_status(
                guild=guild,
                value=AUTO_DANGER_REVERT_VALUE,
                updated_by="Auto: timer expired during downtime",
                ping_role=True,
                context_note="Auto-revert after downtime",
            )
            return

        old = self._danger_reset_tasks.get(guild.id)
        if old and not old.done():
            old.cancel()
        self._danger_reset_tasks[guild.id] = asyncio.create_task(self._auto_revert_after_delay(guild, reset_at))

    # =====================
    # Opt-in records
    # =====================

    def opt_in_records(self, guild: discord.Guild) -> Dict[str, Any]:
        gs = self._ensure_guild_state(guild)
        return gs.get("opt_in", {}) or {}

    def is_opted_in(self, guild: discord.Guild, user_id: int) -> bool:
        rec = self.opt_in_records(guild).get(str(user_id))
        return bool(rec and rec.get("current") is True)

    async def set_opt_in(self, guild: discord.Guild, member: discord.Member, enabled: bool):
        gs = self._ensure_guild_state(guild)
        recs = gs.get("opt_in", {}) or {}
        uid = str(member.id)
        now = utcnow_iso()

        rec = recs.get(uid) or {
            "current": False,
            "opted_in_at": None,
            "opted_out_at": None,
            "username_last_seen": str(member),
        }

        rec["username_last_seen"] = str(member)
        if enabled:
            if rec.get("current") is not True:
                rec["current"] = True
                rec["opted_in_at"] = now
        else:
            if rec.get("current") is True:
                rec["current"] = False
                rec["opted_out_at"] = now

        recs[uid] = rec
        gs["opt_in"] = recs
        self.state["guilds"][self._gkey(guild)] = gs
        await save_state(self.state)

    # =====================
    # Alert broadcast
    # =====================

    async def add_broadcast(self, guild: discord.Guild, broadcast: Dict[str, Any]):
        gs = self._ensure_guild_state(guild)
        b = gs.get("broadcasts", []) or []
        b.append(broadcast)
        gs["broadcasts"] = clamp_history(b)
        self.state["guilds"][self._gkey(guild)] = gs
        await save_state(self.state)

    async def handle_send_alert(self, interaction: discord.Interaction, message: str):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("This must be used in a server.", ephemeral=True)
            return
        if not has_any_role(interaction.user, ALERT_SEND_ROLES):
            await interaction.response.send_message("You do not have permission to send alerts.", ephemeral=True)
            return

        guild = interaction.guild
        role = discord.utils.get(guild.roles, name=ARC_SECURITY_ROLE)
        if role is None:
            await interaction.response.send_message(f"Role `{ARC_SECURITY_ROLE}` not found.", ephemeral=True)
            return

        recipients: List[discord.Member] = []
        recs = self.opt_in_records(guild)
        for m in role.members:
            if m.bot:
                continue
            if str(m.id) in recs:
                recs[str(m.id)]["username_last_seen"] = str(m)
            if self.is_opted_in(guild, m.id):
                recipients.append(m)

        gs = self._ensure_guild_state(guild)
        gs["opt_in"] = recs
        self.state["guilds"][self._gkey(guild)] = gs
        await save_state(self.state)

        if not recipients:
            await interaction.response.send_message(
                f"No opted-in members found in `{ARC_SECURITY_ROLE}`. They must click `Enable DMs` first.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            f"Sending alert to `{len(recipients)}` opted-in `{ARC_SECURITY_ROLE}` members...",
            ephemeral=True,
        )

        dm_embed = discord.Embed(
            title="ARC Security Alert",
            description=message,
            timestamp=utcnow(),
        )
        dm_embed.add_field(name="Sent By", value=f"{interaction.user} (ID: {interaction.user.id})", inline=False)
        dm_embed.add_field(name="Server", value=guild.name, inline=False)
        dm_embed.set_footer(text="Cryonic Gaming bot — Alerts")

        broadcast_id = utcnow_iso()
        deliveries: List[Dict[str, Any]] = []

        async with self._send_lock:
            sem = asyncio.Semaphore(DM_CONCURRENCY)
            failed = 0

            async def send_one(member: discord.Member):
                nonlocal failed
                async with sem:
                    try:
                        await member.send(embed=dm_embed)
                        deliveries.append({
                            "user_id": member.id,
                            "username": str(member),
                            "sent_utc": utcnow_iso(),
                        })
                    except (discord.Forbidden, discord.HTTPException):
                        failed += 1
                    await asyncio.sleep(DM_DELAY_SECONDS)

            for member in recipients:
                await send_one(member)
                if failed >= DM_FAIL_ABORT_THRESHOLD:
                    break

        await self.add_broadcast(guild, {
            "broadcast_id": broadcast_id,
            "created_utc": broadcast_id,
            "sender": f"{interaction.user} (ID: {interaction.user.id})",
            "guild_id": guild.id,
            "message_excerpt": (message[:200] + "…") if len(message) > 200 else message,
            "recipient_target_count": len(recipients),
            "delivered_count": len(deliveries),
            "deliveries": deliveries,
            "aborted_due_to_failures": failed >= DM_FAIL_ABORT_THRESHOLD,
            "fail_count": failed,
        })

        # Rebuild panels at end of alert flow (but cooldown applies)
        try:
            await self.upsert_panels(guild, force=True)
        except Exception:
            pass

    # =====================
    # Slash commands
    # =====================

    async def _send_report(self, interaction: discord.Interaction, title: str, body_text: str):
        if len(body_text) <= 3500:
            emb = discord.Embed(title=title, description=body_text, timestamp=utcnow())
            await interaction.response.send_message(embed=emb, ephemeral=True)
            return

        data = body_text.encode("utf-8")
        file = discord.File(io.BytesIO(data), filename="alert_report.txt")
        emb = discord.Embed(
            title=title,
            description="Report is attached as a text file (too long for an embed).",
            timestamp=utcnow(),
        )
        await interaction.response.send_message(embed=emb, file=file, ephemeral=True)

    @app_commands.command(name="alert_setup", description="Post/refresh the alert and wormhole status panels in #wormhole-status.")
    @require_alert_sender_roles()
    async def alert_setup(self, interaction: discord.Interaction):
        if not interaction.guild:
            await interaction.response.send_message("This must be used in a server.", ephemeral=True)
            return
        await self.upsert_panels(interaction.guild, force=True)
        await interaction.response.send_message("Panels posted/refreshed in #wormhole-status.", ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(AlertSystemCog(bot))
