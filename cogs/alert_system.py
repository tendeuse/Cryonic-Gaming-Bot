# cogs/alert_system.py
#
# Alert System + Wormhole Status
# Railway persistence via /data volume (or PERSIST_ROOT override).
#
# Key behavior:
# - On bot ready: for every guild, ensure #wormhole-status exists and panels are posted/refreshed
# - Persistent Views registered (so buttons keep working after restart)
#
# Updates in this version:
# 1) Auto-danger parsing supports your observed killmail embed format ("System: J220215 ...")
# 2) Every status change pings the "ARC Security" role while ensuring ONLY ONE bot ping message exists in the channel
# 3) Increased apparent size of status text by putting status in embed TITLE
# 4) Adds "status lights" beside statuses:
#    - 🟢 Normal
#    - 🟣 Lock-down
#    - 🔴 all other statuses
# 5) Buttons colors aligned:
#    - Normal = green (success)
#    - Lock-down = purple (Discord doesn't have purple button; using primary/blue as closest)
#    - All others = red (danger)
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
KILLMAIL_CHANNEL_NAME = "kill-mail"  # must match your killmail_feed.py

ARC_SECURITY_ROLE = "ARC Security"

# Alerts system permissions (KEEP RESTRICTED)
ALERT_SEND_ROLES = {
    "ARC Security Corporation Leader",
    "ARC Security Administration Council",
    "ARC General",
}

# Wormhole status permissions (EXPANDED)
WH_STATUS_ROLES = {
    "ARC Security Corporation Leader",
    "ARC Security Administration Council",
    "ARC General",
    "ARC Commander",
    "ARC Officer",
}

# Auto-danger config
HOME_SYSTEM_NAME = "J220215"
AUTO_DANGER_STATUS_VALUE = "Dangerous"
AUTO_DANGER_REVERT_VALUE = "Normal"
AUTO_DANGER_DURATION_SECONDS = 45 * 60  # 45 minutes

# =====================
# PERSISTENCE (Railway Volume)
# =====================

PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)

DATA_FILE = PERSIST_ROOT / "alert_system.json"

# DM pacing / safety
DM_DELAY_SECONDS = 1.2
DM_FAIL_ABORT_THRESHOLD = 25
DM_CONCURRENCY = 1

# How many broadcasts to keep in storage
MAX_BROADCAST_HISTORY = 250

# =====================
# STATUS LIGHTS / STYLES
# =====================

GREEN_LIGHT = "🟢"
PURPLE_LIGHT = "🟣"
RED_LIGHT = "🔴"

def status_light(value: str) -> str:
    v = (value or "").strip().lower()
    if v == "normal":
        return GREEN_LIGHT
    if v in {"lock-down", "lockdown"}:
        return PURPLE_LIGHT
    return RED_LIGHT

def normalize_status(value: str) -> str:
    # Keep canonical spelling consistent with your buttons/labels
    v = (value or "").strip()
    if v.lower() == "lockdown":
        return "Lock-down"
    return v

def is_normal(value: str) -> bool:
    return (value or "").strip().lower() == "normal"

def is_lockdown(value: str) -> bool:
    return (value or "").strip().lower() in {"lock-down", "lockdown"}

# =====================
# STORAGE HELPERS
# =====================

_file_lock = asyncio.Lock()

def utcnow() -> datetime.datetime:
    return datetime.datetime.utcnow()

def utcnow_iso() -> str:
    return utcnow().isoformat()

def _default_state() -> Dict[str, Any]:
    return {
        "opt_in": {},
        "broadcasts": [],
        "status": {"value": "Normal", "updated_utc": None, "updated_by": None},
        "panel_message_ids": {
            "alert": None,
            "status": None,
            "status_ping": None,  # only one tag message in channel
            "channel_id": None,
            "guild_id": None,
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
            s = _default_state()

        s.setdefault("opt_in", {})
        s.setdefault("broadcasts", [])
        s.setdefault("status", {"value": "Normal", "updated_utc": None, "updated_by": None})
        s.setdefault("panel_message_ids", _default_state()["panel_message_ids"])
        s.setdefault("auto_danger", _default_state()["auto_danger"])

        s["panel_message_ids"].setdefault("status_ping", None)
        return s

async def save_state(state: Dict[str, Any]) -> None:
    async with _file_lock:
        _atomic_write_json(DATA_FILE, state)

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

        await self.cog.set_opt_in(interaction.user, True)
        await interaction.response.send_message("You are now opted-in to ARC Security DM alerts.", ephemeral=True)

    @discord.ui.button(label="Disable DMs", style=discord.ButtonStyle.secondary, custom_id="arc_alert:optout")
    async def opt_out(self, interaction: discord.Interaction, button: Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("This must be used in a server.", ephemeral=True)
            return
        if not has_role(interaction.user, ARC_SECURITY_ROLE):
            await interaction.response.send_message(f"Only members with `{ARC_SECURITY_ROLE}` can opt out.", ephemeral=True)
            return

        await self.cog.set_opt_in(interaction.user, False)
        await interaction.response.send_message("You are now opted-out of ARC Security DM alerts.", ephemeral=True)

class WormholeStatusView(View):
    def __init__(self, cog: "AlertSystemCog"):
        super().__init__(timeout=None)
        self.cog = cog

        # NOTE: Discord UI button colors are limited to: primary (blue), secondary (grey),
        # success (green), danger (red). There is no true purple.
        # We keep the "purple" concept via 🟣 in labels and embed lights, and use primary (blue)
        # for Lock-down buttons as the closest supported style.

    async def _set(self, interaction: discord.Interaction, value: str):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("This must be used in a server.", ephemeral=True)
            return
        if not has_any_role(interaction.user, WH_STATUS_ROLES):
            await interaction.response.send_message(
                "You do not have permission to update wormhole status.",
                ephemeral=True
            )
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

        self._danger_reset_task: Optional[asyncio.Task] = None

    async def cog_load(self):
        self.state = await load_state()
        await self._resume_auto_danger_timer_if_needed()

    def cog_unload(self):
        if self._danger_reset_task and not self._danger_reset_task.done():
            self._danger_reset_task.cancel()

    @commands.Cog.listener()
    async def on_ready(self):
        for g in self.bot.guilds:
            if g.id in self._auto_setup_done:
                continue
            try:
                await self.upsert_panels(g)
            except Exception:
                pass
            self._auto_setup_done.add(g.id)

        await self._resume_auto_danger_timer_if_needed()

    # ==========================================================
    # Killmail integration: watch bot messages in #kill-mail and parse embeds
    # ==========================================================

    def _parse_killmail_embed(self, emb: discord.Embed) -> Tuple[Optional[str], Optional[str], Optional[int]]:
        """
        Returns (system_name, tag, killmail_id) parsed from your killmail_feed embed.

        Supports observed formatting:
          Title: "LOSS — Killmail #132426586"
          Description line: "System: J220215 (WH, Sec: -0.99)"
          Also supports: "**System:** J220215 ..."
        """
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

        # Prefer fields if present
        try:
            for f in (emb.fields or []):
                if (f.name or "").strip().lower() == "system":
                    raw = (f.value or "").strip()
                    mfs = re.match(r"^([^\s(]+)", raw)
                    if mfs:
                        system_name = mfs.group(1).strip()
                        break
        except Exception:
            pass

        desc = emb.description or ""

        if not system_name:
            m3 = re.search(r"\*\*System:\*\*\s*([^\n]+?)(?:\s*\(|\s*$)", desc, re.IGNORECASE)
            if m3:
                system_name = m3.group(1).strip()

        if not system_name:
            m4 = re.search(r"^System:\s*([^\s(]+)", desc, re.IGNORECASE | re.MULTILINE)
            if m4:
                system_name = m4.group(1).strip()

        if not tag:
            m5 = re.search(r"^Type:\s*(KILL|LOSS|INVOLVEMENT|UNKNOWN)\s*$", desc, re.IGNORECASE | re.MULTILINE)
            if m5:
                tag = m5.group(1).upper()

        return system_name, tag, kmid

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild:
            return
        if not self.bot.user:
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
        reset_at = utcnow() + datetime.timedelta(seconds=AUTO_DANGER_DURATION_SECONDS)
        reset_at_iso = reset_at.isoformat()

        self.state.setdefault("auto_danger", _default_state()["auto_danger"])
        self.state["auto_danger"].update({
            "active": True,
            "reset_at_utc": reset_at_iso,
            "last_trigger_utc": utcnow_iso(),
            "last_trigger_killmail_id": killmail_id,
            "last_trigger_tag": tag,
            "last_trigger_system": system_name,
        })
        await save_state(self.state)

        if self._danger_reset_task and not self._danger_reset_task.done():
            self._danger_reset_task.cancel()
        self._danger_reset_task = asyncio.create_task(self._auto_revert_after_delay(reset_at))

        await self.update_status(
            guild=guild,
            value=AUTO_DANGER_STATUS_VALUE,
            updated_by=f"Auto: {tag} detected in {system_name} (Killmail {killmail_id})",
            ping_role=True,
            context_note=f"Auto-trigger from killmail {killmail_id}",
        )

    async def _auto_revert_after_delay(self, reset_at: datetime.datetime):
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

        try:
            ad = self.state.get("auto_danger", {}) or {}
            if not ad.get("active"):
                return

            reset_at_utc = iso_to_dt(ad.get("reset_at_utc"))
            if reset_at_utc and utcnow() < reset_at_utc:
                return

            self.state["auto_danger"].update({"active": False, "reset_at_utc": None})
            await save_state(self.state)

            for g in self.bot.guilds:
                try:
                    await self.update_status(
                        guild=g,
                        value=AUTO_DANGER_REVERT_VALUE,
                        updated_by="Auto: danger timer elapsed",
                        ping_role=True,
                        context_note="Auto-revert after timer",
                    )
                except Exception:
                    continue
        except Exception:
            return

    async def _resume_auto_danger_timer_if_needed(self):
        ad = self.state.get("auto_danger", {}) or {}
        if not ad.get("active"):
            return

        reset_at = iso_to_dt(ad.get("reset_at_utc"))
        if not reset_at:
            self.state["auto_danger"]["active"] = False
            self.state["auto_danger"]["reset_at_utc"] = None
            await save_state(self.state)
            return

        if utcnow() >= reset_at:
            self.state["auto_danger"]["active"] = False
            self.state["auto_danger"]["reset_at_utc"] = None
            await save_state(self.state)
            for g in self.bot.guilds:
                try:
                    await self.update_status(
                        guild=g,
                        value=AUTO_DANGER_REVERT_VALUE,
                        updated_by="Auto: timer expired during downtime",
                        ping_role=True,
                        context_note="Auto-revert after downtime",
                    )
                except Exception:
                    continue
            return

        if self._danger_reset_task and not self._danger_reset_task.done():
            self._danger_reset_task.cancel()
        self._danger_reset_task = asyncio.create_task(self._auto_revert_after_delay(reset_at))

    # -------- Storage ops --------

    def opt_in_records(self) -> Dict[str, Any]:
        return self.state.get("opt_in", {}) or {}

    def is_opted_in(self, user_id: int) -> bool:
        rec = self.opt_in_records().get(str(user_id))
        return bool(rec and rec.get("current") is True)

    async def set_opt_in(self, member: discord.Member, enabled: bool):
        recs = self.opt_in_records()
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
        self.state["opt_in"] = recs
        await save_state(self.state)

    async def set_status_state_only(self, value: str, updated_by: str):
        value = normalize_status(value)
        self.state["status"] = {"value": value, "updated_utc": utcnow_iso(), "updated_by": updated_by}
        await save_state(self.state)

    async def add_broadcast(self, broadcast: Dict[str, Any]):
        b = self.state.get("broadcasts", []) or []
        b.append(broadcast)
        self.state["broadcasts"] = clamp_history(b)
        await save_state(self.state)

    # -------- Channel / Panels --------

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

    def build_alert_panel_embed(self, guild: discord.Guild) -> discord.Embed:
        role = discord.utils.get(guild.roles, name=ARC_SECURITY_ROLE)
        opted_in_count = 0
        if role:
            for m in role.members:
                if not m.bot and self.is_opted_in(m.id):
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

    def build_status_embed(self) -> discord.Embed:
        st = self.state.get("status", {}) or {}
        value = normalize_status(st.get("value", "Normal"))
        updated_utc = st.get("updated_utc") or "Never"
        updated_by = st.get("updated_by") or "N/A"

        ad = self.state.get("auto_danger", {}) or {}
        ad_line = ""
        if ad.get("active"):
            ad_line = f"\n**Auto-Revert (UTC):** `{ad.get('reset_at_utc') or 'Unknown'}`"

        light = status_light(value)

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

    async def upsert_panels(self, guild: discord.Guild):
        ch = await self.ensure_channel(guild)

        panel_ids = self.state.get("panel_message_ids", {}) or {}
        alert_msg_id = panel_ids.get("alert")
        status_msg_id = panel_ids.get("status")

        stored_gid = panel_ids.get("guild_id")
        stored_cid = panel_ids.get("channel_id")
        if stored_gid and int(stored_gid) != guild.id:
            alert_msg_id = None
            status_msg_id = None
            stored_cid = None

        alert_embed = self.build_alert_panel_embed(guild)
        if alert_msg_id and stored_cid and int(stored_cid) == ch.id:
            try:
                msg = await ch.fetch_message(int(alert_msg_id))
                await msg.edit(embed=alert_embed, view=AlertPanelView(self))
            except Exception:
                msg = await ch.send(embed=alert_embed, view=AlertPanelView(self))
                alert_msg_id = msg.id
        else:
            msg = await ch.send(embed=alert_embed, view=AlertPanelView(self))
            alert_msg_id = msg.id

        status_embed = self.build_status_embed()
        if status_msg_id and stored_cid and int(stored_cid) == ch.id:
            try:
                msg2 = await ch.fetch_message(int(status_msg_id))
                await msg2.edit(embed=status_embed, view=WormholeStatusView(self))
            except Exception:
                msg2 = await ch.send(embed=status_embed, view=WormholeStatusView(self))
                status_msg_id = msg2.id
        else:
            msg2 = await ch.send(embed=status_embed, view=WormholeStatusView(self))
            status_msg_id = msg2.id

        status_ping_id = panel_ids.get("status_ping")

        self.state["panel_message_ids"] = {
            "guild_id": guild.id,
            "channel_id": ch.id,
            "alert": alert_msg_id,
            "status": status_msg_id,
            "status_ping": status_ping_id,
        }
        await save_state(self.state)

    async def refresh_status_panel(self, guild: discord.Guild):
        panel_ids = self.state.get("panel_message_ids", {}) or {}
        channel_id = panel_ids.get("channel_id")
        status_msg_id = panel_ids.get("status")
        if not channel_id or not status_msg_id:
            return

        ch = guild.get_channel(int(channel_id))
        if not isinstance(ch, discord.TextChannel):
            return
        try:
            msg = await ch.fetch_message(int(status_msg_id))
            await msg.edit(embed=self.build_status_embed(), view=WormholeStatusView(self))
        except Exception:
            pass

    # -------- Single ping/tag message handling --------

    async def _replace_status_ping_message(
        self,
        *,
        guild: discord.Guild,
        status_value: str,
        updated_by: str,
        context_note: Optional[str],
        ping_role: bool,
    ):
        ch = await self.ensure_channel(guild)
        panel_ids = self.state.get("panel_message_ids", {}) or {}
        old_ping_id = panel_ids.get("status_ping")

        if old_ping_id:
            try:
                old_msg = await ch.fetch_message(int(old_ping_id))
                if self.bot.user and old_msg.author.id == self.bot.user.id:
                    await old_msg.delete()
            except Exception:
                pass

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

        self.state.setdefault("panel_message_ids", _default_state()["panel_message_ids"])
        self.state["panel_message_ids"]["status_ping"] = new_msg.id
        await save_state(self.state)

    # -------- Unified status update entrypoint (panel + ping) --------

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
        await self.set_status_state_only(value, updated_by)

        try:
            await self.upsert_panels(guild)
        except Exception:
            pass

        try:
            await self.refresh_status_panel(guild)
        except Exception:
            pass

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

    # -------- Alert broadcast --------

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
        recs = self.opt_in_records()
        for m in role.members:
            if m.bot:
                continue
            if str(m.id) in recs:
                recs[str(m.id)]["username_last_seen"] = str(m)
            if self.is_opted_in(m.id):
                recipients.append(m)

        self.state["opt_in"] = recs
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

        await self.add_broadcast({
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

        try:
            ch = await self.ensure_channel(guild)
            summary = discord.Embed(
                title="Alert Dispatch Summary",
                description=(
                    f"**Sender:** {interaction.user.mention}\n"
                    f"**Recipients (opted-in target):** `{len(recipients)}`\n"
                    f"**Delivered (successful):** `{len(deliveries)}`\n"
                    f"**Failed (DMs closed / errors):** `{failed}`\n"
                    f"**Broadcast ID (UTC):** `{broadcast_id}`"
                ),
                timestamp=utcnow(),
            )
            await ch.send(embed=summary)
        except Exception:
            pass

        await self.upsert_panels(guild)

    # =====================
    # REPORT GENERATION / SLASH COMMANDS
    # =====================

    def _format_opt_in_report_lines(self) -> List[str]:
        recs = self.opt_in_records()
        lines = []
        for uid, rec in recs.items():
            lines.append(
                f"{rec.get('username_last_seen','unknown')} | ID={uid} | "
                f"current={rec.get('current')} | opted_in_at={rec.get('opted_in_at')} | opted_out_at={rec.get('opted_out_at')}"
            )
        lines.sort()
        return lines

    def _filter_broadcasts_by_range(self, start_dt: datetime.datetime, end_dt: datetime.datetime) -> List[Dict[str, Any]]:
        out = []
        for b in (self.state.get("broadcasts", []) or []):
            dt = iso_to_dt(b.get("created_utc"))
            if not dt:
                continue
            if start_dt <= dt <= end_dt:
                out.append(b)
        return out

    def _build_broadcast_text(self, b: Dict[str, Any], include_deliveries: bool = True) -> str:
        lines = []
        lines.append(f"Broadcast ID (UTC): {b.get('broadcast_id')}")
        lines.append(f"Created UTC: {b.get('created_utc')}")
        lines.append(f"Sender: {b.get('sender')}")
        lines.append(f"Target recipients (opted-in): {b.get('recipient_target_count')}")
        lines.append(f"Delivered (successful): {b.get('delivered_count')}")
        lines.append(f"Failures: {b.get('fail_count')} | Aborted: {b.get('aborted_due_to_failures')}")
        lines.append(f"Message excerpt: {b.get('message_excerpt')}")
        lines.append("")
        if include_deliveries:
            lines.append("Successful deliveries:")
            deliveries = b.get("deliveries", []) or []
            for d in deliveries:
                lines.append(f"  - {d.get('username')} (ID: {d.get('user_id')}) @ {d.get('sent_utc')} UTC")
        return "\n".join(lines)

    async def _send_report(self, interaction: discord.Interaction, title: str, body_text: str):
        if len(body_text) <= 3500:
            emb = discord.Embed(
                title=title,
                description=body_text,
                timestamp=utcnow(),
            )
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
        await self.upsert_panels(interaction.guild)
        await interaction.response.send_message("Panels posted/refreshed in #wormhole-status.", ephemeral=True)

    @app_commands.command(name="alert_report", description="Generate alert delivery + opt-in audit report.")
    @require_alert_sender_roles()
    @app_commands.choices(
        mode=[
            app_commands.Choice(name="last_sent", value="last_sent"),
            app_commands.Choice(name="timeframe", value="timeframe"),
        ]
    )
    async def alert_report(
        self,
        interaction: discord.Interaction,
        mode: app_commands.Choice[str],
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        include_opt_in_audit: Optional[bool] = True,
    ):
        broadcasts = self.state.get("broadcasts", []) or []

        if mode.value == "last_sent":
            if not broadcasts:
                await interaction.response.send_message("No broadcasts have been sent yet.", ephemeral=True)
                return
            b = broadcasts[-1]
            text = self._build_broadcast_text(b, include_deliveries=True)

            if include_opt_in_audit:
                text += "\n\n---\nOpt-in audit (current + timestamps):\n"
                text += "\n".join(self._format_opt_in_report_lines())

            await self._send_report(interaction, "Alert Report — Last Sent", text)
            return

        if not date_from or not date_to:
            await interaction.response.send_message(
                "For timeframe reports, provide both `date_from` and `date_to` in YYYY-MM-DD format.",
                ephemeral=True,
            )
            return

        try:
            d_from = parse_yyyy_mm_dd(date_from)
            d_to = parse_yyyy_mm_dd(date_to)
            start_dt = datetime.datetime.combine(d_from, datetime.time.min)
            end_dt = datetime.datetime.combine(d_to, datetime.time.max)
        except ValueError as e:
            await interaction.response.send_message(str(e), ephemeral=True)
            return

        filtered = self._filter_broadcasts_by_range(start_dt, end_dt)
        if not filtered:
            await interaction.response.send_message("No broadcasts found in that timeframe.", ephemeral=True)
            return

        lines = []
        lines.append("Alert Report — Timeframe")
        lines.append(f"From (UTC): {start_dt.isoformat()}  To (UTC): {end_dt.isoformat()}")
        lines.append(f"Broadcasts found: {len(filtered)}")
        lines.append("")

        for b in filtered:
            lines.append(self._build_broadcast_text(b, include_deliveries=True))
            lines.append("\n" + ("-" * 60) + "\n")

        if include_opt_in_audit:
            lines.append("Opt-in audit (current + timestamps):")
            lines.extend(self._format_opt_in_report_lines())

        await self._send_report(interaction, "Alert Report — Timeframe", "\n".join(lines))

def parse_yyyy_mm_dd(s: str) -> datetime.date:
    try:
        return datetime.date.fromisoformat(s)
    except Exception:
        raise ValueError("Invalid date format. Use YYYY-MM-DD (e.g., 2025-12-22).")

async def setup(bot: commands.Bot):
    await bot.add_cog(AlertSystemCog(bot))
