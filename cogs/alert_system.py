# cogs/alert_system.py
#
# Alert System + Wormhole Status
# Railway persistence via /data volume (or PERSIST_ROOT override).
#
import os
import discord
from discord.ext import commands
from discord import app_commands
from discord.ui import View, Button, Modal, TextInput
from pathlib import Path
import json
import datetime
import asyncio
from typing import Dict, Any, Optional, List, Tuple
import io

# =====================
# CONFIG
# =====================

CHANNEL_NAME = "wormhole-status"

ARC_SECURITY_ROLE = "ARC Security"

ALERT_SEND_ROLES = {
    "ARC Security Corporation Leader",
    "ARC Security Administration Council",
    "ARC General",
}

# =====================
# PERSISTENCE (Railway Volume)
# =====================
# Mount your Railway Volume at /data.
# Optionally override with env var PERSIST_ROOT (e.g., "/data").
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
# STORAGE HELPERS
# =====================

_file_lock = asyncio.Lock()

def utcnow() -> datetime.datetime:
    return datetime.datetime.utcnow()

def utcnow_iso() -> str:
    return utcnow().isoformat()

def _default_state() -> Dict[str, Any]:
    return {
        "opt_in": {},  # user_id -> {current: bool, opted_in_at, opted_out_at, username_last_seen}
        "broadcasts": [],  # list of broadcasts with delivery logs
        "status": {"value": "Normal", "updated_utc": None, "updated_by": None},
        "panel_message_ids": {"alert": None, "status": None, "channel_id": None, "guild_id": None},
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
        s.setdefault("panel_message_ids", {"alert": None, "status": None, "channel_id": None, "guild_id": None})
        return s

async def save_state(state: Dict[str, Any]) -> None:
    async with _file_lock:
        _atomic_write_json(DATA_FILE, state)

def clamp_history(broadcasts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if len(broadcasts) <= MAX_BROADCAST_HISTORY:
        return broadcasts
    return broadcasts[-MAX_BROADCAST_HISTORY:]

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
# DATE PARSING (YYYY-MM-DD)
# =====================

def parse_yyyy_mm_dd(s: str) -> datetime.date:
    try:
        return datetime.date.fromisoformat(s)
    except Exception:
        raise ValueError("Invalid date format. Use YYYY-MM-DD (e.g., 2025-12-22).")

def date_range_to_datetimes(d_from: datetime.date, d_to: datetime.date) -> Tuple[datetime.datetime, datetime.datetime]:
    if d_to < d_from:
        raise ValueError("End date must be on or after start date.")
    start_dt = datetime.datetime.combine(d_from, datetime.time.min)
    end_dt = datetime.datetime.combine(d_to, datetime.time.max)
    return start_dt, end_dt

def iso_to_dt(s: Optional[str]) -> Optional[datetime.datetime]:
    if not s:
        return None
    try:
        return datetime.datetime.fromisoformat(s)
    except Exception:
        return None

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

    async def _set(self, interaction: discord.Interaction, value: str):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("This must be used in a server.", ephemeral=True)
            return
        if not has_any_role(interaction.user, ALERT_SEND_ROLES):
            await interaction.response.send_message("You do not have permission to update wormhole status.", ephemeral=True)
            return

        await self.cog.set_status(value, f"{interaction.user} (ID: {interaction.user.id})")
        await self.cog.refresh_status_panel(interaction.guild)
        await interaction.response.send_message(f"Wormhole status set to `{value}`.", ephemeral=True)

    @discord.ui.button(label="Normal", style=discord.ButtonStyle.success, custom_id="wh_status:normal")
    async def normal(self, interaction: discord.Interaction, button: Button):
        await self._set(interaction, "Normal")

    @discord.ui.button(label="Dangerous", style=discord.ButtonStyle.primary, custom_id="wh_status:dangerous")
    async def dangerous(self, interaction: discord.Interaction, button: Button):
        await self._set(interaction, "Dangerous")

    @discord.ui.button(label="Enemy Fleet Spotted", style=discord.ButtonStyle.danger, custom_id="wh_status:enemy")
    async def enemy(self, interaction: discord.Interaction, button: Button):
        await self._set(interaction, "Enemy Fleet Spotted")

    @discord.ui.button(label="Lock-down", style=discord.ButtonStyle.secondary, custom_id="wh_status:lockdown")
    async def lockdown(self, interaction: discord.Interaction, button: Button):
        await self._set(interaction, "Lock-down")

    @discord.ui.button(label="Under attack", style=discord.ButtonStyle.danger, custom_id="wh_status:attack")
    async def attack(self, interaction: discord.Interaction, button: Button):
        await self._set(interaction, "Under attack")

# =====================
# COG
# =====================

class AlertSystemCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state: Dict[str, Any] = _default_state()

        # Persistent views (safe to call at init)
        self.bot.add_view(AlertPanelView(self))
        self.bot.add_view(WormholeStatusView(self))

        self._send_lock = asyncio.Lock()

    async def cog_load(self):
        # Load persisted state from Railway volume on cog load
        self.state = await load_state()

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

    async def set_status(self, value: str, updated_by: str):
        self.state["status"] = {"value": value, "updated_utc": utcnow_iso(), "updated_by": updated_by}
        await save_state(self.state)

    async def add_broadcast(self, broadcast: Dict[str, Any]):
        b = self.state.get("broadcasts", []) or []
        b.append(broadcast)
        self.state["broadcasts"] = clamp_history(b)
        await save_state(self.state)

    async def persist_state(self):
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
        # Count opted-in among ARC Security members (best effort)
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
        value = st.get("value", "Normal")
        updated_utc = st.get("updated_utc") or "Never"
        updated_by = st.get("updated_by") or "N/A"

        emb = discord.Embed(
            title="Wormhole Status",
            description=(
                f"**Current Status:** `{value}`\n"
                f"**Last Updated (UTC):** `{updated_utc}`\n"
                f"**Updated By:** `{updated_by}`\n"
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

        alert_embed = self.build_alert_panel_embed(guild)
        if alert_msg_id:
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
        if status_msg_id:
            try:
                msg2 = await ch.fetch_message(int(status_msg_id))
                await msg2.edit(embed=status_embed, view=WormholeStatusView(self))
            except Exception:
                msg2 = await ch.send(embed=status_embed, view=WormholeStatusView(self))
                status_msg_id = msg2.id
        else:
            msg2 = await ch.send(embed=status_embed, view=WormholeStatusView(self))
            status_msg_id = msg2.id

        self.state["panel_message_ids"] = {
            "guild_id": guild.id,
            "channel_id": ch.id,
            "alert": alert_msg_id,
            "status": status_msg_id,
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

        # recipients: ARC Security, opted in, not bots
        recipients: List[discord.Member] = []
        recs = self.opt_in_records()
        for m in role.members:
            if m.bot:
                continue
            # update username_last_seen (audit)
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
            "deliveries": deliveries,  # successful only
            "aborted_due_to_failures": failed >= DM_FAIL_ABORT_THRESHOLD,
            "fail_count": failed,
        })

        # Summary in channel
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
    # REPORT GENERATION
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
        # Discord embed description limit ~4096
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

    # =====================
    # SLASH COMMANDS
    # =====================

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
        date_from: Optional[str] = None,  # YYYY-MM-DD
        date_to: Optional[str] = None,    # YYYY-MM-DD
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

        # timeframe mode
        if not date_from or not date_to:
            await interaction.response.send_message(
                "For timeframe reports, provide both `date_from` and `date_to` in YYYY-MM-DD format.",
                ephemeral=True,
            )
            return

        try:
            d_from = parse_yyyy_mm_dd(date_from)
            d_to = parse_yyyy_mm_dd(date_to)
            start_dt, end_dt = date_range_to_datetimes(d_from, d_to)
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

async def setup(bot: commands.Bot):
    await bot.add_cog(AlertSystemCog(bot))
