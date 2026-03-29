# cogs/scheduling.py
#
# Recruiter & Onboarder Scheduling + Invite Log + Role Self-Assign
# ================================================================
# Channels (auto-created):
#   #on-duty            — live embed showing who is currently on shift
#   #start-shift        — shift control panel (Start/End buttons per role)
#   #recruiter-lounge   — IGN invite checker, Recruiter-only
#   #role-requests      — self-assign panel (everyone can see)
#   #staff-approvals    — pending role approval inbox (approval roles only)
#
# Roles auto-created if missing:
#   Recruiter  —  can start Recruiter shifts
#   Onboarder  —  can start Onboarder shifts (also requires ARC Genesis)
#
# Slash commands:
#   /scheduling_setup   —  run/re-run setup (server owner or approval roles)
#   /scheduling_status  —  dump shift + pending-request snapshot (approval roles)

import asyncio
import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import discord
from discord.ext import commands
from discord import app_commands

# ============================================================
# CONFIG
# ============================================================

RECRUITER_ROLE = "Recruiter"
ONBOARDER_ROLE = "Onboarder"
GENESIS_ROLE   = "ARC Genesis"   # required alongside Onboarder for Onboarder shifts

APPROVAL_ROLES: set[str] = {
    "ARC Lieutenant",
    "ARC Commander",
    "ARC General",
    "ARC Security Administration Council",
    "ARC Security Corporation Leader",
}

ON_DUTY_CHANNEL      = "on-duty"
START_SHIFT_CHANNEL  = "start-shift"
RECRUITER_LOUNGE_CH  = "recruiter-lounge"
ROLE_REQUESTS_CH     = "role-requests"
STAFF_APPROVALS_CH   = "staff-approvals"

# ============================================================
# PATHS
# ============================================================

DATA_DIR   = Path(os.getenv("PERSIST_ROOT", "/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = DATA_DIR / "scheduling_state.json"
DB_FILE    = DATA_DIR / "recruits.db"

# ============================================================
# GENERIC UTILITIES
# ============================================================

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def utcnow_ts() -> int:
    return int(utcnow().timestamp())

def utcnow_iso() -> str:
    return utcnow().isoformat()

def fmt_duration(seconds: int) -> str:
    h, r = divmod(max(0, seconds), 3600)
    m, s = divmod(r, 60)
    return f"{h}h {m:02d}m {s:02d}s"

def atomic_write(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)

def safe_load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, type(default)) else default
    except Exception:
        return default

def has_role(member: discord.Member, name: str) -> bool:
    return any(r.name == name for r in member.roles)

def has_any_role(member: discord.Member, names: set[str]) -> bool:
    return any(r.name in names for r in member.roles)

def can_approve(member: discord.Member) -> bool:
    return member.guild_permissions.administrator or has_any_role(member, APPROVAL_ROLES)

# ============================================================
# SQLITE  (invite database)
# ============================================================

def _db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_FILE)
    con.row_factory = sqlite3.Row
    return con

def init_db() -> None:
    with _db() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS invites (
                ign         TEXT PRIMARY KEY COLLATE NOCASE,
                invited_by  TEXT NOT NULL,
                invited_at  TEXT NOT NULL
            )
        """)

def _check_ign_sync(ign: str) -> Optional[dict]:
    with _db() as con:
        row = con.execute("SELECT * FROM invites WHERE ign = ?", (ign.strip(),)).fetchone()
        return dict(row) if row else None

def _add_invite_sync(ign: str, invited_by: str) -> None:
    with _db() as con:
        con.execute(
            "INSERT OR REPLACE INTO invites (ign, invited_by, invited_at) VALUES (?,?,?)",
            (ign.strip(), invited_by, utcnow_iso()),
        )

async def check_ign(ign: str) -> Optional[dict]:
    return await asyncio.to_thread(_check_ign_sync, ign)

async def add_invite(ign: str, invited_by: str) -> None:
    await asyncio.to_thread(_add_invite_sync, ign, invited_by)

# ============================================================
# STATE MANAGEMENT
# ============================================================

def _default_guild_state() -> dict:
    return {
        "channel_ids": {
            "on_duty": None,
            "start_shift": None,
            "recruiter_lounge": None,
            "role_requests": None,
            "staff_approvals": None,
        },
        "message_ids": {
            "on_duty_embed": None,
            "shift_panel": None,
            "recruiter_panel": None,
            "role_request_panel": None,
        },
        "on_duty":         {"recruiter": {}, "onboarder": {}},
        "pending_requests": {},
    }

def ensure_guild(state: dict, guild_id: int) -> dict:
    gkey = str(guild_id)
    if gkey not in state["guilds"]:
        state["guilds"][gkey] = _default_guild_state()
    gs = state["guilds"][gkey]
    # Forward-compat: add any missing keys from the default
    default = _default_guild_state()
    for k, v in default.items():
        gs.setdefault(k, v)
    gs["channel_ids"].update({k: gs["channel_ids"].get(k) for k in default["channel_ids"]})
    gs["message_ids"].update({k: gs["message_ids"].get(k) for k in default["message_ids"]})
    gs["on_duty"].setdefault("recruiter", {})
    gs["on_duty"].setdefault("onboarder", {})
    return gs

# ============================================================
# EMBED BUILDERS
# ============================================================

def build_on_duty_embed(gs: dict) -> discord.Embed:
    on_duty = gs.get("on_duty", {"recruiter": {}, "onboarder": {}})
    embed = discord.Embed(
        title="📋 Currently On Duty",
        color=discord.Color.blurple(),
        timestamp=utcnow(),
    )
    for key, label, emoji in [
        ("recruiter", "Recruiters",  "🔵"),
        ("onboarder", "Onboarders",  "🟢"),
    ]:
        slot = on_duty.get(key, {})
        if slot:
            lines = [f"• <@{uid}> — on since <t:{ts}:t>" for uid, ts in slot.items()]
            embed.add_field(name=f"{emoji} {label}", value="\n".join(lines), inline=False)
        else:
            embed.add_field(name=f"{emoji} {label}", value="*(nobody on duty)*", inline=False)
    embed.set_footer(text="Updates automatically")
    return embed

def build_shift_panel_embed() -> discord.Embed:
    return discord.Embed(
        title="⏱️ Shift Control Panel",
        description=(
            "Press the button for your role to **start** or **end** your shift.\n\n"
            f"**Recruiter shift** — requires the `{RECRUITER_ROLE}` role.\n"
            f"**Onboarder shift** — requires `{ONBOARDER_ROLE}` **and** `{GENESIS_ROLE}`."
        ),
        color=discord.Color.dark_blue(),
    )

def build_recruiter_panel_embed() -> discord.Embed:
    return discord.Embed(
        title="🔍 Invite Verification",
        description=(
            "Check whether a player's in-game name is already in the invite list "
            "before sending a Corporation invitation.\n\n"
            "Click **Check IGN** to look up a name."
        ),
        color=discord.Color.blue(),
    )

def build_role_request_embed() -> discord.Embed:
    return discord.Embed(
        title="📝 Request a Role",
        description=(
            f"Click a button to request the **{RECRUITER_ROLE}** or **{ONBOARDER_ROLE}** role.\n"
            "A staff member will review and approve or deny your request."
        ),
        color=discord.Color.gold(),
    )

def build_approval_embed(member: discord.Member, role_name: str, req_id: str) -> discord.Embed:
    embed = discord.Embed(
        title="📥 Role Request",
        color=discord.Color.orange(),
        timestamp=utcnow(),
    )
    embed.add_field(name="Applicant",       value=f"{member.mention} (`{member}`)", inline=False)
    embed.add_field(name="Requested Role",  value=f"**{role_name}**",               inline=True)
    embed.add_field(name="Request ID",      value=f"`{req_id[:8]}`",                inline=True)
    embed.set_footer(text="Click Approve or Deny below")
    return embed

# ============================================================
# VIEWS
# ============================================================

class ShiftPanelView(discord.ui.View):
    """Persistent panel in #start-shift — one per guild."""

    def __init__(self, cog: "SchedulingCog", guild_id: int):
        super().__init__(timeout=None)
        self.cog      = cog
        self.guild_id = guild_id
        gid = str(guild_id)
        self.btn_sr.custom_id = f"sched:start_recruiter:{gid}"
        self.btn_er.custom_id = f"sched:end_recruiter:{gid}"
        self.btn_so.custom_id = f"sched:start_onboarder:{gid}"
        self.btn_eo.custom_id = f"sched:end_onboarder:{gid}"

    @discord.ui.button(label="▶ Start Recruiter Shift", style=discord.ButtonStyle.success,   custom_id="sched:sr:_", row=0)
    async def btn_sr(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_start_shift(interaction, "recruiter")

    @discord.ui.button(label="■ End Recruiter Shift",   style=discord.ButtonStyle.danger,    custom_id="sched:er:_", row=0)
    async def btn_er(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_end_shift(interaction, "recruiter")

    @discord.ui.button(label="▶ Start Onboarder Shift", style=discord.ButtonStyle.success,   custom_id="sched:so:_", row=1)
    async def btn_so(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_start_shift(interaction, "onboarder")

    @discord.ui.button(label="■ End Onboarder Shift",   style=discord.ButtonStyle.danger,    custom_id="sched:eo:_", row=1)
    async def btn_eo(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_end_shift(interaction, "onboarder")


class CheckIGNModal(discord.ui.Modal, title="Check Player IGN"):
    ign_input = discord.ui.TextInput(
        label="In-Game Name",
        placeholder="Enter the player's IGN exactly as it appears in-game",
        min_length=1,
        max_length=64,
        required=True,
    )

    def __init__(self, cog: "SchedulingCog"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        ign    = self.ign_input.value.strip()
        record = await check_ign(ign)

        if record:
            embed = discord.Embed(
                title="✅ Already Invited",
                description=f"**{ign}** is already in the invite list.",
                color=discord.Color.green(),
            )
            embed.add_field(name="Invited by", value=record["invited_by"],                              inline=True)
            embed.add_field(name="Invited at", value=record["invited_at"][:19].replace("T", " ") + " UTC", inline=True)
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            embed = discord.Embed(
                title="❌ Not Yet Invited",
                description=f"**{ign}** is not in the invite list.",
                color=discord.Color.red(),
            )
            await interaction.response.send_message(
                embed=embed,
                view=AddInviteView(ign, interaction.user),
                ephemeral=True,
            )


class AddInviteView(discord.ui.View):
    """Ephemeral — shown after a 'Not yet invited' check result."""

    def __init__(self, ign: str, requester: discord.User | discord.Member):
        super().__init__(timeout=120)
        self.ign       = ign
        self.requester = requester

    @discord.ui.button(label="Add to Invite List", style=discord.ButtonStyle.primary)
    async def add_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        button.disabled = True
        await add_invite(self.ign, str(self.requester))
        embed = discord.Embed(
            title="✅ Added to Invite List",
            description=f"**{self.ign}** has been recorded in the invite database.",
            color=discord.Color.green(),
        )
        embed.set_footer(text=f"Added by {self.requester.display_name}")
        await interaction.response.edit_message(embed=embed, view=self)


class RecruiterLoungeView(discord.ui.View):
    """Persistent panel in #recruiter-lounge — one per guild."""

    def __init__(self, cog: "SchedulingCog", guild_id: int):
        super().__init__(timeout=None)
        self.cog      = cog
        self.guild_id = guild_id
        self.check_btn.custom_id = f"sched:check_ign:{guild_id}"

    @discord.ui.button(label="🔍 Check IGN", style=discord.ButtonStyle.primary, custom_id="sched:check_ign:_")
    async def check_btn(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Must be used in a server.", ephemeral=True)
            return
        if not has_role(interaction.user, RECRUITER_ROLE):
            await interaction.response.send_message(
                f"❌ Only **{RECRUITER_ROLE}** members can use this tool.", ephemeral=True
            )
            return
        await interaction.response.send_modal(CheckIGNModal(self.cog))


class RoleRequestView(discord.ui.View):
    """Persistent self-assign panel in #role-requests — one per guild."""

    def __init__(self, cog: "SchedulingCog", guild_id: int):
        super().__init__(timeout=None)
        self.cog      = cog
        self.guild_id = guild_id
        self.req_rec.custom_id = f"sched:req_recruiter:{guild_id}"
        self.req_onb.custom_id = f"sched:req_onboarder:{guild_id}"

    @discord.ui.button(label=f"📋 Request Recruiter Role", style=discord.ButtonStyle.primary,   custom_id="sched:req_recruiter:_")
    async def req_rec(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_role_request(interaction, RECRUITER_ROLE)

    @discord.ui.button(label=f"📋 Request Onboarder Role", style=discord.ButtonStyle.secondary, custom_id="sched:req_onboarder:_")
    async def req_onb(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_role_request(interaction, ONBOARDER_ROLE)


class RoleApprovalView(discord.ui.View):
    """Persistent per-request Approve/Deny view in #staff-approvals."""

    def __init__(self, cog: "SchedulingCog", guild_id: int, req_id: str):
        super().__init__(timeout=None)
        self.cog      = cog
        self.guild_id = guild_id
        self.req_id   = req_id
        self.approve_btn.custom_id = f"sched:approve:{req_id}"
        self.deny_btn.custom_id    = f"sched:deny:{req_id}"

    @discord.ui.button(label="✅ Approve", style=discord.ButtonStyle.success, custom_id="sched:approve:_")
    async def approve_btn(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_approval(interaction, self.req_id, approved=True)

    @discord.ui.button(label="❌ Deny",    style=discord.ButtonStyle.danger,   custom_id="sched:deny:_")
    async def deny_btn(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self.cog.handle_approval(interaction, self.req_id, approved=False)

# ============================================================
# COG
# ============================================================

class SchedulingCog(commands.Cog):

    def __init__(self, bot: commands.Bot):
        self.bot     = bot
        self._lock   = asyncio.Lock()
        self._state: dict = safe_load_json(STATE_FILE, {"guilds": {}})
        self._state.setdefault("guilds", {})
        init_db()

    # ── Lifecycle ──────────────────────────────────────────────────────────

    async def cog_load(self):
        self.bot.loop.create_task(self._post_ready_init())

    async def _post_ready_init(self):
        await self.bot.wait_until_ready()
        for guild in self.bot.guilds:
            try:
                await self._setup_guild(guild)
            except Exception as exc:
                print(f"[scheduling] Setup failed for {guild.name} ({guild.id}): {exc}")

    async def cog_unload(self):
        async with self._lock:
            atomic_write(STATE_FILE, self._state)

    # ── State helpers ──────────────────────────────────────────────────────

    def _gs(self, guild_id: int) -> dict:
        return ensure_guild(self._state, guild_id)

    def _save(self):
        atomic_write(STATE_FILE, self._state)

    # ── Full guild setup ───────────────────────────────────────────────────

    async def _setup_guild(self, guild: discord.Guild):
        await self._ensure_roles(guild)
        await self._ensure_channels(guild)
        await self._ensure_panel_messages(guild)
        self._register_views(guild)
        async with self._lock:
            self._save()

    # ── Role creation ──────────────────────────────────────────────────────

    async def _ensure_roles(self, guild: discord.Guild):
        for name in (RECRUITER_ROLE, ONBOARDER_ROLE):
            if not discord.utils.get(guild.roles, name=name):
                try:
                    await guild.create_role(name=name, reason="Scheduling cog auto-setup")
                    print(f"[scheduling] Created role '{name}' in {guild.name}")
                except discord.Forbidden:
                    print(f"[scheduling] Cannot create role '{name}' — missing permissions")

    # ── Channel creation ───────────────────────────────────────────────────

    async def _ensure_channels(self, guild: discord.Guild):
        gs       = self._gs(guild.id)
        everyone = guild.default_role
        me       = guild.me

        bot_ow      = discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_messages=True, read_message_history=True, embed_links=True)
        readonly_ow = discord.PermissionOverwrite(view_channel=True, send_messages=False, read_message_history=True)
        hidden_ow   = discord.PermissionOverwrite(view_channel=False)

        approval_roles = [r for r in guild.roles if r.name in APPROVAL_ROLES]
        recruiter_role = discord.utils.get(guild.roles, name=RECRUITER_ROLE)
        onboarder_role = discord.utils.get(guild.roles, name=ONBOARDER_ROLE)

        async def get_or_create(name: str, ows: dict) -> Optional[discord.TextChannel]:
            ch = discord.utils.get(guild.text_channels, name=name)
            if ch is None:
                try:
                    ch = await guild.create_text_channel(name, overwrites=ows, reason="Scheduling cog auto-setup")
                    print(f"[scheduling] Created #{name} in {guild.name}")
                except discord.Forbidden:
                    print(f"[scheduling] Cannot create #{name} — missing permissions")
                    return None
            return ch

        # #on-duty — read-only for everyone
        ch = await get_or_create(ON_DUTY_CHANNEL, {everyone: readonly_ow, me: bot_ow})
        if ch:
            gs["channel_ids"]["on_duty"] = ch.id

        # #start-shift — visible to shift-eligible roles and approval roles
        shift_ows: dict = {everyone: hidden_ow, me: bot_ow}
        for role in [r for r in [recruiter_role, onboarder_role] + approval_roles if r]:
            shift_ows[role] = readonly_ow
        ch = await get_or_create(START_SHIFT_CHANNEL, shift_ows)
        if ch:
            gs["channel_ids"]["start_shift"] = ch.id

        # #recruiter-lounge — Recruiter role only (+ approval roles)
        lounge_ows: dict = {everyone: hidden_ow, me: bot_ow}
        if recruiter_role:
            lounge_ows[recruiter_role] = readonly_ow
        for role in approval_roles:
            lounge_ows[role] = readonly_ow
        ch = await get_or_create(RECRUITER_LOUNGE_CH, lounge_ows)
        if ch:
            gs["channel_ids"]["recruiter_lounge"] = ch.id

        # #role-requests — visible to everyone
        ch = await get_or_create(ROLE_REQUESTS_CH, {everyone: readonly_ow, me: bot_ow})
        if ch:
            gs["channel_ids"]["role_requests"] = ch.id

        # #staff-approvals — approval roles only
        approvals_ows: dict = {everyone: hidden_ow, me: bot_ow}
        for role in approval_roles:
            approvals_ows[role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True)
        ch = await get_or_create(STAFF_APPROVALS_CH, approvals_ows)
        if ch:
            gs["channel_ids"]["staff_approvals"] = ch.id

    # ── Panel messages ─────────────────────────────────────────────────────

    async def _ensure_panel_messages(self, guild: discord.Guild):
        gs   = self._gs(guild.id)
        cids = gs["channel_ids"]
        mids = gs["message_ids"]

        async def get_ch(key: str) -> Optional[discord.TextChannel]:
            cid = cids.get(key)
            return guild.get_channel(int(cid)) if cid else None  # type: ignore

        async def upsert(ch: discord.TextChannel, msg_key: str, embed: discord.Embed, view: Optional[discord.ui.View] = None) -> Optional[discord.Message]:
            mid = mids.get(msg_key)
            if mid:
                try:
                    msg = await ch.fetch_message(int(mid))
                    await msg.edit(embed=embed, view=view)
                    return msg
                except Exception:
                    pass
            try:
                msg = await ch.send(embed=embed, **({"view": view} if view else {}))
                mids[msg_key] = msg.id
                return msg
            except Exception as exc:
                print(f"[scheduling] Could not send panel to #{ch.name}: {exc}")
                return None

        if ch := await get_ch("on_duty"):
            await upsert(ch, "on_duty_embed", build_on_duty_embed(gs))

        if ch := await get_ch("start_shift"):
            await upsert(ch, "shift_panel", build_shift_panel_embed(), ShiftPanelView(self, guild.id))

        if ch := await get_ch("recruiter_lounge"):
            await upsert(ch, "recruiter_panel", build_recruiter_panel_embed(), RecruiterLoungeView(self, guild.id))

        if ch := await get_ch("role_requests"):
            await upsert(ch, "role_request_panel", build_role_request_embed(), RoleRequestView(self, guild.id))

    # ── View registration (called on ready + after setup) ─────────────────

    def _register_views(self, guild: discord.Guild):
        gs   = self._gs(guild.id)
        mids = gs["message_ids"]

        def add(view: discord.ui.View, msg_key: str):
            mid = mids.get(msg_key)
            try:
                if mid:
                    self.bot.add_view(view, message_id=int(mid))
                else:
                    self.bot.add_view(view)
            except Exception:
                pass

        add(ShiftPanelView(self, guild.id),       "shift_panel")
        add(RecruiterLoungeView(self, guild.id),   "recruiter_panel")
        add(RoleRequestView(self, guild.id),       "role_request_panel")

        # Per-request approval views
        for req_id, req in gs.get("pending_requests", {}).items():
            if req.get("status") == "pending":
                view = RoleApprovalView(self, guild.id, req_id)
                mid  = req.get("message_id")
                try:
                    if mid:
                        self.bot.add_view(view, message_id=int(mid))
                    else:
                        self.bot.add_view(view)
                except Exception:
                    pass

    # ── On-duty embed refresh ──────────────────────────────────────────────

    async def _refresh_on_duty(self, guild: discord.Guild):
        gs     = self._gs(guild.id)
        ch_id  = gs["channel_ids"].get("on_duty")
        msg_id = gs["message_ids"].get("on_duty_embed")
        if not ch_id or not msg_id:
            return
        ch = guild.get_channel(int(ch_id))
        if not isinstance(ch, discord.TextChannel):
            return
        try:
            msg = await ch.fetch_message(int(msg_id))
            await msg.edit(embed=build_on_duty_embed(gs))
        except Exception:
            pass

    # ── Shift handlers ─────────────────────────────────────────────────────

    async def handle_start_shift(self, interaction: discord.Interaction, shift_type: str):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Must be used in a server.", ephemeral=True)
            return

        member, guild = interaction.user, interaction.guild

        # Role validation
        if shift_type == "recruiter":
            if not has_role(member, RECRUITER_ROLE):
                await interaction.response.send_message(
                    f"❌ You need the **{RECRUITER_ROLE}** role to start a Recruiter shift.", ephemeral=True
                )
                return
        elif shift_type == "onboarder":
            missing = [n for n in (ONBOARDER_ROLE, GENESIS_ROLE) if not has_role(member, n)]
            if missing:
                await interaction.response.send_message(
                    f"❌ You need the **{', '.join(missing)}** role(s) to start an Onboarder shift.", ephemeral=True
                )
                return

        async with self._lock:
            gs   = self._gs(guild.id)
            slot = gs["on_duty"].setdefault(shift_type, {})
            if str(member.id) in slot:
                await interaction.response.send_message(
                    "❌ You are already on shift for this role.", ephemeral=True
                )
                return
            slot[str(member.id)] = utcnow_ts()
            self._save()

        label = "Recruiter" if shift_type == "recruiter" else "Onboarder"
        await interaction.response.send_message(f"🟢 **{label}** shift started. Welcome on duty!", ephemeral=True)
        await self._refresh_on_duty(guild)

    async def handle_end_shift(self, interaction: discord.Interaction, shift_type: str):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Must be used in a server.", ephemeral=True)
            return

        member, guild = interaction.user, interaction.guild

        async with self._lock:
            gs       = self._gs(guild.id)
            slot     = gs["on_duty"].get(shift_type, {})
            label    = "Recruiter" if shift_type == "recruiter" else "Onboarder"
            if str(member.id) not in slot:
                await interaction.response.send_message(
                    f"❌ You are not currently on a **{label}** shift.", ephemeral=True
                )
                return
            start_ts = slot.pop(str(member.id))
            gs["on_duty"][shift_type] = slot
            self._save()

        duration = utcnow_ts() - start_ts
        await interaction.response.send_message(
            f"🔴 **{label}** shift ended. Duration: **{fmt_duration(duration)}**", ephemeral=True
        )
        await self._refresh_on_duty(guild)

    # ── Role request handlers ──────────────────────────────────────────────

    async def handle_role_request(self, interaction: discord.Interaction, role_name: str):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Must be used in a server.", ephemeral=True)
            return

        member, guild = interaction.user, interaction.guild

        if has_role(member, role_name):
            await interaction.response.send_message(
                f"✅ You already have the **{role_name}** role.", ephemeral=True
            )
            return

        async with self._lock:
            gs      = self._gs(guild.id)
            pending = gs["pending_requests"]

            # Prevent duplicate pending requests
            for req in pending.values():
                if req.get("user_id") == member.id and req.get("role") == role_name and req.get("status") == "pending":
                    await interaction.response.send_message(
                        f"⏳ You already have a pending request for **{role_name}**.", ephemeral=True
                    )
                    return

            req_id = uuid.uuid4().hex
            pending[req_id] = {
                "user_id":      member.id,
                "role":         role_name,
                "status":       "pending",
                "message_id":   None,
                "channel_id":   None,
                "requested_at": utcnow_iso(),
            }
            self._save()

        # Post to #staff-approvals
        ch_id = self._gs(guild.id)["channel_ids"].get("staff_approvals")
        if not ch_id:
            await interaction.response.send_message(
                "❌ Staff approvals channel not found — ask an admin to run `/scheduling_setup`.", ephemeral=True
            )
            return

        ch = guild.get_channel(int(ch_id))
        if not isinstance(ch, discord.TextChannel):
            await interaction.response.send_message("❌ Staff approvals channel is unavailable.", ephemeral=True)
            return

        view = RoleApprovalView(self, guild.id, req_id)
        msg  = await ch.send(embed=build_approval_embed(member, role_name, req_id), view=view)

        async with self._lock:
            gs = self._gs(guild.id)
            gs["pending_requests"][req_id]["message_id"] = msg.id
            gs["pending_requests"][req_id]["channel_id"] = ch.id
            self._save()

        try:
            self.bot.add_view(view, message_id=msg.id)
        except Exception:
            pass

        await interaction.response.send_message(
            f"📥 Your request for **{role_name}** has been submitted. Staff will review it shortly.", ephemeral=True
        )

    async def handle_approval(self, interaction: discord.Interaction, req_id: str, *, approved: bool):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Must be used in a server.", ephemeral=True)
            return

        if not can_approve(interaction.user):
            await interaction.response.send_message(
                "❌ You do not have permission to approve role requests.", ephemeral=True
            )
            return

        guild = interaction.guild
        gs    = self._gs(guild.id)
        req   = gs.get("pending_requests", {}).get(req_id)

        if not req:
            await interaction.response.send_message("❌ Request not found.", ephemeral=True)
            return

        if req.get("status") != "pending":
            await interaction.response.send_message(
                f"⚠️ This request was already **{req.get('status')}**.", ephemeral=True
            )
            return

        role_name = req["role"]
        user_id   = int(req["user_id"])
        member    = guild.get_member(user_id)

        if approved:
            if member is None:
                try:
                    member = await guild.fetch_member(user_id)
                except Exception:
                    member = None

            if member:
                role = discord.utils.get(guild.roles, name=role_name)
                if role:
                    try:
                        await member.add_roles(role, reason=f"Approved by {interaction.user.display_name}")
                    except discord.Forbidden:
                        await interaction.response.send_message(
                            "❌ I lack permission to assign roles. Check my role hierarchy.", ephemeral=True
                        )
                        return

        # Update state
        async with self._lock:
            gs = self._gs(guild.id)
            gs["pending_requests"][req_id].update({
                "status":      "approved" if approved else "denied",
                "decided_by":  interaction.user.id,
                "decided_at":  utcnow_iso(),
            })
            self._save()

        # Disable the approval message buttons
        icon, verb, color = (
            ("✅", "approved", discord.Color.green())
            if approved else
            ("❌", "denied",   discord.Color.red())
        )
        if interaction.message and interaction.message.embeds:
            try:
                upd = interaction.message.embeds[0].copy()
                upd.colour = color
                upd.title  = f"{icon} Role Request — {verb.upper()}"
                upd.set_footer(text=f"{verb.capitalize()} by {interaction.user.display_name}")
                await interaction.message.edit(embed=upd, view=discord.ui.View())
            except Exception:
                pass

        await interaction.response.send_message(
            f"{icon} **{verb.capitalize()}** the **{role_name}** request for <@{user_id}>.", ephemeral=True
        )

        # Best-effort DM to the applicant
        target = member or guild.get_member(user_id)
        if target:
            try:
                dm = (
                    f"{'✅' if approved else '❌'} Your request for **{role_name}** in **{guild.name}** was "
                    f"**{verb}** by {interaction.user.display_name}."
                )
                await target.send(dm)
            except Exception:
                pass

    # ── Slash commands ─────────────────────────────────────────────────────

    @app_commands.command(
        name="scheduling_setup",
        description="Create all scheduling channels, roles, and panels (approval roles / server owner).",
    )
    async def scheduling_setup(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Must be used in a server.", ephemeral=True)
            return

        if (
            interaction.user.id != interaction.guild.owner_id
            and not can_approve(interaction.user)
        ):
            await interaction.response.send_message(
                "❌ Only the server owner or approval-role holders can run this.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)
        try:
            await self._setup_guild(interaction.guild)
            await interaction.followup.send("✅ Scheduling system set up successfully.", ephemeral=True)
        except Exception as exc:
            await interaction.followup.send(f"❌ Setup encountered an error: {exc}", ephemeral=True)

    @app_commands.command(
        name="scheduling_status",
        description="Show who is currently on duty and any pending role requests (approval roles only).",
    )
    async def scheduling_status(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Must be used in a server.", ephemeral=True)
            return

        if not can_approve(interaction.user):
            await interaction.response.send_message(
                "❌ Only approval-role holders can view the status summary.", ephemeral=True
            )
            return

        gs      = self._gs(interaction.guild.id)
        on_duty = gs.get("on_duty", {})
        pending = {k: v for k, v in gs.get("pending_requests", {}).items() if v.get("status") == "pending"}

        embed = discord.Embed(title="📊 Scheduling Status", color=discord.Color.blurple(), timestamp=utcnow())

        for key, label in [("recruiter", "Recruiters"), ("onboarder", "Onboarders")]:
            slot = on_duty.get(key, {})
            val  = "\n".join(f"• <@{uid}> since <t:{ts}:t>" for uid, ts in slot.items()) or "*(none)*"
            embed.add_field(name=label, value=val, inline=False)

        if pending:
            lines = [
                f"• <@{r['user_id']}> → **{r['role']}** (`{rid[:8]}`)"
                for rid, r in pending.items()
            ]
            embed.add_field(name=f"⏳ Pending Requests ({len(lines)})", value="\n".join(lines), inline=False)
        else:
            embed.add_field(name="⏳ Pending Requests", value="*(none)*", inline=False)

        await interaction.response.send_message(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    if bot.get_cog("SchedulingCog") is not None:
        return
    await bot.add_cog(SchedulingCog(bot))
