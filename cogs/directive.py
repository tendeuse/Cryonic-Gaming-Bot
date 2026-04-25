# cogs/directive.py
#
# ARC Directives — Task Management System
# ========================================
# - Permanent panel embed in #arc-directives with "Create Directive" button
# - CREATE access: General, Director, CEO
# - FINISH access: General, Director, CEO
# - COMMIT / RETRACT / COMPLETED access: determined per-task by minimum rank
# - All actions are logged to #directives-logs
# - Completed tasks track per-member completion counts (repeatable tasks)
# - Finishing a task starts a 30-minute countdown, then auto-deletes the embed
#
# Rank hierarchy (index 0 = highest):
#   0  CEO            → ARC Security Corporation Leader
#   1  Director       → ARC Security Administration Council
#   2  General        → ARC General
#   3  Fleet Commander→ ARC Commander
#   4  Lieutenant     → ARC Lieutenant
#   5  Petty Officer  → ARC Petty Officer

import os
import json
import asyncio
import datetime
import uuid
from pathlib import Path
from typing import Dict, Any, List, Optional

import discord
from discord.ext import commands, tasks
from discord import app_commands

# =====================
# CHANNELS
# =====================
DIRECTIVES_CHANNEL = "arc-directives"
LOG_CHANNEL        = "directives-logs"

# =====================
# PERSISTENCE
# =====================
PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)
DATA_FILE = PERSIST_ROOT / "directives.json"

# =====================
# RANK SYSTEM
# =====================
# Index 0 = highest authority. A member can interact with a task if their
# rank index is <= the task's min_rank_index (equal or higher authority).

RANK_ROLES: List[str] = [
    "ARC Security Corporation Leader",      # 0 — CEO
    "ARC Security Administration Council",  # 1 — Director
    "ARC General",                          # 2 — General
    "ARC Commander",                        # 3 — Fleet Commander
    "ARC Lieutenant",                       # 4 — Officer
    "ARC Petty Officer",                    # 5 — Petty Officer
]

RANK_DISPLAY: List[str] = [
    "CEO",
    "Director",
    "General",
    "Fleet Commander",
    "Lieutenant",
    "Petty Officer",
]

RANK_ROLE_TO_INDEX: Dict[str, int] = {role: i for i, role in enumerate(RANK_ROLES)}

# Minimum rank index required to CREATE / FINISH directives (General = 2)
CREATE_MIN_RANK_IDX = 2
FINISH_MIN_RANK_IDX = 2

DELETE_DELAY_SECONDS = 30 * 60   # 30 minutes after finishing


# =====================
# PERSISTENCE HELPERS
# =====================

_file_lock = asyncio.Lock()


def _atomic_write(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=4), encoding="utf-8")
    tmp.replace(path)


async def load_data() -> Dict[str, Any]:
    async with _file_lock:
        if not DATA_FILE.exists():
            return {"directives": {}, "panels": {}}
        try:
            txt = DATA_FILE.read_text(encoding="utf-8").strip()
            if not txt:
                return {"directives": {}, "panels": {}}
            data = json.loads(txt)
            data.setdefault("directives", {})
            data.setdefault("panels", {})
            return data
        except Exception:
            return {"directives": {}, "panels": {}}


async def save_data(data: Dict[str, Any]) -> None:
    async with _file_lock:
        _atomic_write(DATA_FILE, data)


def utcnow_iso() -> str:
    return datetime.datetime.utcnow().isoformat()


# =====================
# RANK PERMISSION HELPERS
# =====================

def member_rank_index(member: discord.Member) -> Optional[int]:
    """Return the index of the member's highest rank, or None if unranked."""
    for i, role_name in enumerate(RANK_ROLES):
        if any(r.name == role_name for r in member.roles):
            return i
    return None


def can_create(member: discord.Member) -> bool:
    idx = member_rank_index(member)
    return idx is not None and idx <= CREATE_MIN_RANK_IDX


def can_finish(member: discord.Member) -> bool:
    idx = member_rank_index(member)
    return idx is not None and idx <= FINISH_MIN_RANK_IDX


def can_interact(member: discord.Member, min_rank_idx: int) -> bool:
    """Can the member commit/retract/complete this task?"""
    idx = member_rank_index(member)
    return idx is not None and idx <= min_rank_idx


def parse_rank_input(text: str) -> Optional[int]:
    """
    Accept a rank as:
      - A digit 1–6  (1 = CEO, 6 = Petty Officer)
      - Display name (case-insensitive): "General", "Lieutenant", etc.
      - Role name    (case-insensitive): "ARC General", etc.
    Returns 0-based index or None if invalid.
    """
    text = text.strip()
    if text.isdigit():
        n = int(text)
        if 1 <= n <= 6:
            return n - 1
    for i, name in enumerate(RANK_DISPLAY):
        if text.lower() == name.lower():
            return i
    for i, role in enumerate(RANK_ROLES):
        if text.lower() == role.lower():
            return i
    return None


# =====================
# LOGGING
# =====================

async def _log(guild: discord.Guild, message: str) -> None:
    ch = discord.utils.get(guild.text_channels, name=LOG_CHANNEL)
    if not ch:
        try:
            ch = await guild.create_text_channel(LOG_CHANNEL)
        except Exception:
            return
    try:
        await ch.send(message[:2000])
    except Exception:
        pass


# =====================
# EMBED BUILDER
# =====================

def _resolve_name(guild: discord.Guild, uid: int) -> str:
    m = guild.get_member(uid)
    return m.display_name if m else f"<@{uid}>"


def build_directive_embed(
    directive: Dict[str, Any],
    guild: Optional[discord.Guild] = None,
) -> discord.Embed:
    status        = directive.get("status", "active")
    min_rank_idx  = int(directive.get("min_rank_index", 5))
    max_assignees = directive.get("max_assignees")         # None = unlimited
    assignees     = directive.get("assignees", [])
    completions   = directive.get("completions", {})       # {str(uid): int}
    task_text     = directive.get("task", "")
    d_id          = directive.get("id", "???")

    color = discord.Color.blue() if status == "active" else discord.Color.greyple()

    slot_str = (
        f"{len(assignees)} / {max_assignees}"
        if max_assignees
        else f"{len(assignees)} / ∞"
    )

    # Assignee list
    if assignees and guild:
        assignee_text = "\n".join(
            f"• {_resolve_name(guild, int(uid))}" for uid in assignees
        )
    else:
        assignee_text = "_(none)_"

    # Completion list
    if completions and guild:
        comp_text = "\n".join(
            f"• {_resolve_name(guild, int(uid))}: **{count}×**"
            for uid, count in completions.items()
        )
    else:
        comp_text = "_(none)_"

    status_str = "✅ Active" if status == "active" else "🏁 Finished"

    embed = discord.Embed(
        title="📋 Directive",
        description=f"```{task_text}```",
        color=color,
        timestamp=datetime.datetime.utcnow(),
    )
    embed.add_field(name="📊 Status",      value=status_str,                    inline=True)
    embed.add_field(name="👥 Slots",       value=slot_str,                      inline=True)
    embed.add_field(name="🔰 Min. Rank",  value=RANK_DISPLAY[min_rank_idx],    inline=True)
    embed.add_field(name="👤 Assigned Members", value=assignee_text,           inline=False)
    embed.add_field(name="✅ Completions",       value=comp_text,               inline=False)

    if status == "finished":
        finished_by = directive.get("finished_by")
        finished_at = (directive.get("finished_at") or "")[:19].replace("T", " ")
        who = (_resolve_name(guild, int(finished_by)) if (guild and finished_by)
               else (f"<@{finished_by}>" if finished_by else "Unknown"))
        embed.add_field(
            name="🏁 Finished By",
            value=f"{who} — `{finished_at} UTC`",
            inline=False,
        )
        embed.set_footer(text=f"Directive ID: {d_id} • Deleting in 30 minutes")
    else:
        created_by = directive.get("created_by")
        who = (_resolve_name(guild, int(created_by)) if (guild and created_by)
               else (f"<@{created_by}>" if created_by else "Unknown"))
        embed.set_footer(text=f"Directive ID: {d_id} • Created by {who}")

    return embed


# =====================
# VIEWS
# =====================

class PanelView(discord.ui.View):
    """Permanent panel in #arc-directives with the Create Directive button."""

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="📋 Create Directive",
        style=discord.ButtonStyle.primary,
        custom_id="directive:panel:create",
    )
    async def create_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Must be used in a server.", ephemeral=True)
            return

        if not can_create(interaction.user):
            await interaction.response.send_message(
                "❌ Only **Generals**, **Directors**, and **CEOs** can create directives.",
                ephemeral=True,
            )
            return

        await interaction.response.send_modal(CreateDirectiveModal())


class DirectiveView(discord.ui.View):
    """
    Attached to each live directive embed.
    The buttons have no callbacks — they are dispatched via on_interaction
    so they can look up live data without a cog reference baked in.
    """

    def __init__(self, directive_id: str, *, disabled: bool = False):
        super().__init__(timeout=None)

        def _btn(label: str, style: discord.ButtonStyle, action: str):
            b = discord.ui.Button(
                label=label,
                style=style,
                custom_id=f"directive:{action}:{directive_id}",
                disabled=disabled,
            )
            self.add_item(b)

        _btn("✅ Commit",           discord.ButtonStyle.success,   "commit")
        _btn("↩ Retract",           discord.ButtonStyle.secondary, "retract")
        _btn("🏆 Completed",        discord.ButtonStyle.primary,   "completed")
        _btn("🏁 Finish Directive", discord.ButtonStyle.danger,    "finish")


# =====================
# MODAL
# =====================

class CreateDirectiveModal(discord.ui.Modal, title="Create New Directive"):

    task_input = discord.ui.TextInput(
        label="Task Description",
        style=discord.TextStyle.paragraph,
        placeholder="Describe what needs to be done...",
        min_length=10,
        max_length=1000,
        required=True,
    )
    slots_input = discord.ui.TextInput(
        label="Max Assignees  (leave blank = unlimited)",
        placeholder="e.g. 3",
        required=False,
        max_length=5,
    )
    rank_input = discord.ui.TextInput(
        label="Minimum Rank Required",
        placeholder="1=CEO  2=Director  3=General  4=Fleet Cmdr  5=Lieutenant  6=Petty Officer",
        required=True,
        max_length=30,
    )

    async def on_submit(self, interaction: discord.Interaction):
        # --- Validate max slots ---
        max_slots: Optional[int] = None
        raw_slots = self.slots_input.value.strip()
        if raw_slots:
            if not raw_slots.isdigit() or int(raw_slots) < 1:
                await interaction.response.send_message(
                    "❌ Max Assignees must be a positive whole number, or leave it blank.",
                    ephemeral=True,
                )
                return
            max_slots = int(raw_slots)

        # --- Validate rank ---
        rank_idx = parse_rank_input(self.rank_input.value)
        if rank_idx is None:
            lines = "\n".join(f"  `{i+1}` — {name}" for i, name in enumerate(RANK_DISPLAY))
            await interaction.response.send_message(
                f"❌ Invalid rank. Enter a number 1–6 or a rank name:\n{lines}",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)

        guild = interaction.guild
        if not guild:
            await interaction.followup.send("❌ Must be used in a server.", ephemeral=True)
            return

        ch = discord.utils.get(guild.text_channels, name=DIRECTIVES_CHANNEL)
        if not ch:
            await interaction.followup.send(
                f"❌ Channel `#{DIRECTIVES_CHANNEL}` not found.", ephemeral=True
            )
            return

        # --- Build directive record ---
        d_id = uuid.uuid4().hex[:8]
        directive: Dict[str, Any] = {
            "id":            d_id,
            "task":          self.task_input.value.strip(),
            "max_assignees": max_slots,
            "min_rank_index": rank_idx,
            "created_by":    interaction.user.id,
            "created_at":    utcnow_iso(),
            "status":        "active",
            "assignees":     [],
            "completions":   {},
            "guild_id":      guild.id,
            "channel_id":    ch.id,
            "message_id":    None,
            "finished_at":   None,
            "finished_by":   None,
            "delete_at":     None,
        }

        embed = build_directive_embed(directive, guild)
        view  = DirectiveView(d_id)
        msg   = await ch.send(embed=embed, view=view)

        directive["message_id"] = msg.id

        data = await load_data()
        data["directives"][d_id] = directive
        await save_data(data)

        # Register for persistence immediately
        try:
            interaction.client.add_view(view, message_id=msg.id)
        except Exception:
            pass

        await _log(
            guild,
            f"📋 **Directive created** by {interaction.user.mention}\n"
            f"**ID:** `{d_id}`\n"
            f"**Task:** {directive['task'][:200]}\n"
            f"**Min Rank:** {RANK_DISPLAY[rank_idx]}\n"
            f"**Max Slots:** {max_slots or '∞'}",
        )
        await interaction.followup.send(
            f"✅ Directive `{d_id}` created in {ch.mention}.", ephemeral=True
        )


# =====================
# COG
# =====================

class DirectiveCog(commands.Cog):
    """ARC Directives — task management with rank-gated assignment."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # Per-directive asyncio locks to prevent race conditions
        self._locks: Dict[str, asyncio.Lock] = {}
        if not self.deletion_check.is_running():
            self.deletion_check.start()

    def cog_unload(self):
        self.deletion_check.cancel()

    def _lock(self, d_id: str) -> asyncio.Lock:
        if d_id not in self._locks:
            self._locks[d_id] = asyncio.Lock()
        return self._locks[d_id]

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    @commands.Cog.listener()
    async def on_ready(self):
        # Always register the panel view so the Create button survives restarts
        self.bot.add_view(PanelView())

        data = await load_data()

        # Register persistent views for active directives
        count = 0
        for d_id, directive in data.get("directives", {}).items():
            if directive.get("status") != "active":
                continue
            msg_id = directive.get("message_id")
            try:
                view = DirectiveView(d_id)
                if isinstance(msg_id, int):
                    self.bot.add_view(view, message_id=msg_id)
                else:
                    self.bot.add_view(view)
                count += 1
            except Exception:
                pass

        print(f"[directive] Registered {count} active directive view(s).")

        # Ensure the panel embed exists in every guild
        for guild in self.bot.guilds:
            await self._ensure_panel(guild, data)

    async def _ensure_panel(
        self, guild: discord.Guild, data: Optional[Dict[str, Any]] = None
    ) -> None:
        """Create or refresh the permanent panel embed in #arc-directives."""
        ch = discord.utils.get(guild.text_channels, name=DIRECTIVES_CHANNEL)
        if not ch:
            try:
                ch = await guild.create_text_channel(DIRECTIVES_CHANNEL)
            except Exception:
                return

        if data is None:
            data = await load_data()

        panels   = data.setdefault("panels", {})
        gkey     = str(guild.id)
        msg_id   = panels.get(gkey)

        embed = discord.Embed(
            title="📋 ARC Directives",
            description=(
                "Press **Create Directive** to post a new task for the corps.\n\n"
                "**Who can create:** General · Director · CEO\n"
                "**Who can take tasks:** Set per-directive by minimum rank\n\n"
                "Each directive supports unlimited completions — "
                "the system tracks how many times each member finishes the task."
            ),
            color=discord.Color.dark_blue(),
        )
        embed.set_footer(text="ARC Security — Directives System")
        view = PanelView()

        # Try to edit the existing message first
        if msg_id:
            try:
                existing = await ch.fetch_message(int(msg_id))
                await existing.edit(embed=embed, view=view)
                return
            except Exception:
                pass

        # Send a fresh panel
        try:
            msg = await ch.send(embed=embed, view=view)
            panels[gkey] = msg.id
            await save_data(data)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Button dispatcher (all directive action buttons)
    # ------------------------------------------------------------------

    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        if interaction.type != discord.InteractionType.component:
            return
        if not interaction.data or not isinstance(interaction.data, dict):
            return

        cid = str(interaction.data.get("custom_id", ""))

        # Panel button is handled by PanelView's own callback — skip it here
        if cid == "directive:panel:create":
            return

        # Only process our directive action buttons
        if not cid.startswith("directive:"):
            return

        parts = cid.split(":", 2)
        if len(parts) != 3:
            return

        _, action, d_id = parts
        if action not in ("commit", "retract", "completed", "finish"):
            return

        await self._handle_action(interaction, action, d_id)

    async def _handle_action(
        self,
        interaction: discord.Interaction,
        action: str,
        d_id: str,
    ) -> None:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Must be used in a server.", ephemeral=True)
            return

        guild  = interaction.guild
        member = interaction.user

        async with self._lock(d_id):
            data      = await load_data()
            directive = data.get("directives", {}).get(d_id)

            if not isinstance(directive, dict):
                await interaction.response.send_message(
                    "❌ Directive not found.", ephemeral=True
                )
                return

            if directive.get("status") != "active":
                await interaction.response.send_message(
                    "⚠️ This directive has already been finished.", ephemeral=True
                )
                return

            min_rank_idx = int(directive.get("min_rank_index", 5))

            # ---- Permission check ----
            if action == "finish":
                if not can_finish(member):
                    await interaction.response.send_message(
                        "❌ Only **Generals**, **Directors**, and **CEOs** can finish directives.",
                        ephemeral=True,
                    )
                    return
            else:
                if not can_interact(member, min_rank_idx):
                    await interaction.response.send_message(
                        f"❌ This task requires at least **{RANK_DISPLAY[min_rank_idx]}** rank.",
                        ephemeral=True,
                    )
                    return

            uid     = member.id
            uid_str = str(uid)
            assignees   = directive.setdefault("assignees",   [])
            completions = directive.setdefault("completions", {})

            # ---- Action logic ----

            if action == "commit":
                if uid in assignees:
                    await interaction.response.send_message(
                        "⚠️ You are already committed to this task.", ephemeral=True
                    )
                    return
                max_s = directive.get("max_assignees")
                if max_s and len(assignees) >= int(max_s):
                    await interaction.response.send_message(
                        f"❌ This task is full ({int(max_s)}/{int(max_s)} slots taken).",
                        ephemeral=True,
                    )
                    return
                assignees.append(uid)
                reply = "✅ You have committed to this task."
                log_msg = (
                    f"✅ **{member.display_name}** committed to directive `{d_id}`\n"
                    f"**Task:** {directive['task'][:150]}"
                )

            elif action == "retract":
                if uid not in assignees:
                    await interaction.response.send_message(
                        "⚠️ You are not committed to this task.", ephemeral=True
                    )
                    return
                assignees.remove(uid)
                reply = "↩ You have retracted from this task."
                log_msg = (
                    f"↩ **{member.display_name}** retracted from directive `{d_id}`\n"
                    f"**Task:** {directive['task'][:150]}"
                )

            elif action == "completed":
                count = completions.get(uid_str, 0) + 1
                completions[uid_str] = count
                reply = f"🏆 Completion recorded! You have completed this task **{count}×** total."
                log_msg = (
                    f"🏆 **{member.display_name}** completed directive `{d_id}` "
                    f"(**{count}× total**)\n"
                    f"**Task:** {directive['task'][:150]}"
                )

            elif action == "finish":
                directive["status"]      = "finished"
                directive["finished_at"] = utcnow_iso()
                directive["finished_by"] = uid
                delete_dt = datetime.datetime.utcnow() + datetime.timedelta(seconds=DELETE_DELAY_SECONDS)
                directive["delete_at"] = delete_dt.isoformat()

                # Build completion summary for log
                if completions:
                    comp_lines = []
                    for uid_s, c in completions.items():
                        comp_lines.append(
                            f"  • {_resolve_name(guild, int(uid_s))}: **{c}×**"
                        )
                    comp_summary = "\n".join(comp_lines)
                else:
                    comp_summary = "  _(none)_"

                reply = "🏁 Directive finished. The post will be deleted in 30 minutes."
                log_msg = (
                    f"🏁 **{member.display_name}** finished directive `{d_id}`\n"
                    f"**Task:** {directive['task'][:150]}\n"
                    f"**Completion totals:**\n{comp_summary}"
                )

            else:
                return   # should never reach here

            await save_data(data)

        # Respond to user and log (outside the lock, non-critical)
        await interaction.response.send_message(reply, ephemeral=True)
        await _log(guild, log_msg)
        await self._refresh_embed(guild, directive)

    # ------------------------------------------------------------------
    # Embed refresh helper
    # ------------------------------------------------------------------

    async def _refresh_embed(
        self, guild: discord.Guild, directive: Dict[str, Any]
    ) -> None:
        ch_id  = directive.get("channel_id")
        msg_id = directive.get("message_id")
        if not ch_id or not msg_id:
            return

        ch = guild.get_channel(int(ch_id))
        if not isinstance(ch, discord.TextChannel):
            return

        try:
            msg = await ch.fetch_message(int(msg_id))
        except Exception:
            return

        embed    = build_directive_embed(directive, guild)
        finished = directive.get("status") == "finished"
        view     = DirectiveView(directive["id"], disabled=finished)

        try:
            await msg.edit(embed=embed, view=view)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 30-minute deletion task
    # ------------------------------------------------------------------

    @tasks.loop(seconds=30)
    async def deletion_check(self):
        """
        Every 30 seconds: delete embed messages for directives whose
        30-minute post-finish window has elapsed, then remove them from storage.
        """
        now  = datetime.datetime.utcnow()
        data = await load_data()

        to_purge: List[str] = []
        for d_id, directive in data.get("directives", {}).items():
            if directive.get("status") != "finished":
                continue
            delete_at_str = directive.get("delete_at")
            if not delete_at_str:
                continue
            try:
                delete_at = datetime.datetime.fromisoformat(delete_at_str)
            except Exception:
                continue
            if now >= delete_at:
                to_purge.append(d_id)

        if not to_purge:
            return

        for d_id in to_purge:
            directive = data["directives"].get(d_id)
            if not directive:
                continue

            guild_id = directive.get("guild_id")
            ch_id    = directive.get("channel_id")
            msg_id   = directive.get("message_id")

            if guild_id and ch_id and msg_id:
                guild = self.bot.get_guild(int(guild_id))
                if guild:
                    ch = guild.get_channel(int(ch_id))
                    if isinstance(ch, discord.TextChannel):
                        try:
                            msg = await ch.fetch_message(int(msg_id))
                            await msg.delete()
                        except Exception:
                            pass

            del data["directives"][d_id]
            print(f"[directive] Purged finished directive {d_id}.")

        await save_data(data)

    @deletion_check.before_loop
    async def _before_deletion_check(self):
        await self.bot.wait_until_ready()

    # ------------------------------------------------------------------
    # Admin slash command — force refresh panel
    # ------------------------------------------------------------------

    @app_commands.command(
        name="directive_setup",
        description="Re-post or refresh the Directives panel (CEO / Director / General only).",
    )
    async def directive_setup(self, interaction: discord.Interaction):
        if not isinstance(interaction.user, discord.Member) or not can_create(interaction.user):
            await interaction.response.send_message(
                "❌ Only Generals, Directors, and CEOs can use this.", ephemeral=True
            )
            return
        await interaction.response.defer(ephemeral=True)
        await self._ensure_panel(interaction.guild)
        await interaction.followup.send(
            f"✅ Panel refreshed in `#{DIRECTIVES_CHANNEL}`.", ephemeral=True
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(DirectiveCog(bot))
