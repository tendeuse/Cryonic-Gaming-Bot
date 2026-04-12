# cogs/meeting.py
#
# Officer Meeting Planner
# =======================
#
# FEATURES
# --------
# 1. /meeting  — opens a creation modal (leadership only).
#    Accepts title, description, date & time in EVE Time (UTC).
#    Posts a persistent embed in #officer-meeting pinging @ARC Security.
#
# 2. Embed time display  — uses Discord <t:{unix}:F> timestamps so every
#    member sees the meeting time converted to their own local timezone
#    automatically.  No server-side timezone conversion is needed.
#
# 3. "📝 Add Topic" button  — anyone can submit an agenda item.
#    Each topic is appended to the embed and attributed to the submitter.
#
# 4. "⚙️ Manage Meeting" button  — restricted to leadership roles.
#    Opens a pre-filled modal allowing edits to title, description, and
#    date/time.  Leadership can also type "CANCEL" in the title field to
#    cancel the meeting entirely (disables both buttons, marks embed red).
#
# PERSISTENCE
# -----------
# • Meeting data is stored in /data/meetings.json keyed by UUID.
# • Persistent views are re-registered on every on_ready so buttons survive
#   bot restarts.
# • Cancelled or fully-closed meetings keep their views registered but with
#   all buttons disabled — Discord requires the view to stay registered for
#   the disabled state to render correctly after a restart.
#
# SERVER SETUP COMPATIBILITY
# --------------------------
# REQUIRED_CHANNELS and REQUIRED_ROLES are declared at module level so
# server_setup.py auto-creates anything that's missing.

import asyncio
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import discord
from discord.ext import commands
from discord import app_commands


# ============================================================
# CONFIG
# ============================================================

MEETING_CHANNEL  = "officer-meeting"
PING_ROLE        = "ARC Security"

MEETINGS_PATH    = "/data/meetings.json"

# Roles that may create and manage meetings
LEADERSHIP_ROLES: Set[str] = {
    "ARC Security Administration Council",
    "ARC Security Corporation Leader",
}

# Picked up by server_setup.py auto-scanner
REQUIRED_CHANNELS: List[str] = [MEETING_CHANNEL]
REQUIRED_ROLES:    List[str] = [PING_ROLE]


# ============================================================
# HELPERS
# ============================================================

def _has_leadership(member: discord.Member) -> bool:
    return any(r.name in LEADERSHIP_ROLES for r in member.roles)


def _parse_eve_dt(date_str: str, time_str: str) -> Optional[datetime]:
    """
    Parse date (YYYY-MM-DD) and time (HH:MM) strings in EVE Time (UTC).
    Returns an aware datetime or None on failure.
    """
    try:
        dt = datetime.strptime(
            f"{date_str.strip()} {time_str.strip()}", "%Y-%m-%d %H:%M"
        ).replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


# ============================================================
# STORAGE
# ============================================================

_mtg_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _mtg_lock
    if _mtg_lock is None:
        _mtg_lock = asyncio.Lock()
    return _mtg_lock


def _atomic_write(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


async def _load_meetings() -> Dict[str, Any]:
    async with _get_lock():
        if not os.path.exists(MEETINGS_PATH):
            return {}
        try:
            with open(MEETINGS_PATH, "r", encoding="utf-8") as f:
                txt = f.read().strip()
            if not txt:
                return {}
            data = json.loads(txt)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}


async def _save_meetings(data: Dict[str, Any]) -> None:
    async with _get_lock():
        _atomic_write(MEETINGS_PATH, data)


# ============================================================
# EMBED BUILDER  (single source of truth)
# ============================================================

def _build_meeting_embed(mtg: Dict[str, Any]) -> discord.Embed:
    """
    Builds (or rebuilds) the full meeting embed from stored data.
    Called on initial post and on every subsequent update.
    """
    status    = mtg.get("status", "active")
    cancelled = (status == "cancelled")

    color = discord.Color.red() if cancelled else discord.Color.blurple()

    title = str(mtg.get("title", "Officer Meeting"))
    if cancelled:
        title = f"~~{title}~~ — CANCELLED"

    embed = discord.Embed(
        title=       title,
        description= str(mtg.get("description", "")),
        color=       color,
    )

    # ── Time field ────────────────────────────────────────────────────────────
    # Discord <t:{unix}:F> renders as "Saturday, 14 June 2025 20:00" in the
    # viewer's own local timezone.  <t:{unix}:R> adds a live relative countdown.
    unix = mtg.get("eve_timestamp")
    if unix:
        embed.add_field(
            name="🕒 Meeting Time",
            value=(
                f"**EVE Time:** <t:{unix}:F>\n"
                f"**Your Time:** <t:{unix}:F>  (<t:{unix}:R>)"
            ),
            inline=False,
        )
    else:
        embed.add_field(name="🕒 Meeting Time", value="*(not set)*", inline=False)

    # ── Agenda / Topics ───────────────────────────────────────────────────────
    topics: List[Dict] = mtg.get("topics", [])
    if topics:
        lines = []
        for i, t in enumerate(topics, start=1):
            submitter = t.get("submitted_by_name", "Unknown")
            text      = t.get("topic", "")
            lines.append(f"**{i}.** {text}  _(by {submitter})_")
        # Embed field values cap at 1024 chars; chunk if needed
        value = "\n".join(lines)
        if len(value) > 1024:
            value = value[:1020] + "…"
        embed.add_field(name="📋 Agenda", value=value, inline=False)
    else:
        embed.add_field(
            name="📋 Agenda",
            value="*(no topics yet — click **📝 Add Topic** to contribute)*",
            inline=False,
        )

    # ── Footer ────────────────────────────────────────────────────────────────
    creator_name = mtg.get("creator_name", "Leadership")
    embed.set_footer(text=f"Scheduled by {creator_name}  •  Meeting ID: {mtg.get('meeting_id', '?')[:8]}")

    return embed


# ============================================================
# MODALS
# ============================================================

class MeetingCreateModal(discord.ui.Modal, title="Schedule a Meeting"):
    """
    Step 1 of /meeting — collects all meeting details in one modal.
    """

    mtg_title = discord.ui.TextInput(
        label="Meeting Title",
        max_length=100,
        required=True,
        placeholder="e.g. Monthly Officer Debrief",
    )
    description = discord.ui.TextInput(
        label="Description / Agenda Summary",
        style=discord.TextStyle.paragraph,
        max_length=800,
        required=False,
        placeholder="Brief summary of what will be covered…",
    )
    date = discord.ui.TextInput(
        label="Date (EVE Time — YYYY-MM-DD)",
        max_length=10,
        min_length=10,
        required=True,
        placeholder="e.g. 2025-07-20",
    )
    time = discord.ui.TextInput(
        label="Time (EVE Time — HH:MM, 24-hour UTC)",
        max_length=5,
        min_length=4,
        required=True,
        placeholder="e.g. 20:00",
    )

    def __init__(self, cog: "MeetingCog"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction) -> None:
        dt = _parse_eve_dt(self.date.value, self.time.value)
        if dt is None:
            await interaction.response.send_message(
                "❌ Invalid date or time format.\n"
                "Date must be `YYYY-MM-DD` and time must be `HH:MM` (24-hour EVE Time / UTC).",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)
        await self.cog._post_meeting(
            interaction=  interaction,
            title=        self.mtg_title.value.strip(),
            description=  self.description.value.strip(),
            eve_timestamp=int(dt.timestamp()),
        )


class AddTopicModal(discord.ui.Modal, title="Add a Discussion Topic"):
    """
    Submitted by any member.  The topic is appended to the meeting agenda.
    """

    topic = discord.ui.TextInput(
        label="Your Topic",
        style=discord.TextStyle.paragraph,
        max_length=300,
        required=True,
        placeholder="Describe the topic you'd like to address…",
    )

    def __init__(self, cog: "MeetingCog", meeting_id: str):
        super().__init__()
        self.cog        = cog
        self.meeting_id = meeting_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.cog._add_topic(
            interaction= interaction,
            meeting_id=  self.meeting_id,
            topic_text=  self.topic.value.strip(),
        )


class ManageMeetingModal(discord.ui.Modal, title="Manage Meeting"):
    """
    Leadership-only.  Pre-filled with current values.
    Typing 'CANCEL' (case-insensitive) in the Title field cancels the meeting.
    """

    # Fields are populated with current values in __init__
    mtg_title = discord.ui.TextInput(
        label='Title  (type "CANCEL" to cancel the meeting)',
        max_length=100,
        required=True,
    )
    description = discord.ui.TextInput(
        label="Description",
        style=discord.TextStyle.paragraph,
        max_length=800,
        required=False,
    )
    date = discord.ui.TextInput(
        label="Date (EVE Time — YYYY-MM-DD)",
        max_length=10,
        min_length=10,
        required=True,
    )
    time = discord.ui.TextInput(
        label="Time (EVE Time — HH:MM, 24-hour UTC)",
        max_length=5,
        min_length=4,
        required=True,
    )

    def __init__(
        self,
        cog:        "MeetingCog",
        meeting_id: str,
        current:    Dict[str, Any],
    ):
        super().__init__()
        self.cog        = cog
        self.meeting_id = meeting_id

        # Pre-fill with current values so leadership only changes what they need
        self.mtg_title.default  = current.get("title", "")
        self.description.default = current.get("description", "")

        unix = current.get("eve_timestamp")
        if unix:
            dt = datetime.fromtimestamp(unix, tz=timezone.utc)
            self.date.default = dt.strftime("%Y-%m-%d")
            self.time.default = dt.strftime("%H:%M")
        else:
            self.date.default = ""
            self.time.default = ""

    async def on_submit(self, interaction: discord.Interaction) -> None:
        # Check for cancellation intent
        if self.mtg_title.value.strip().upper() == "CANCEL":
            await self.cog._cancel_meeting(interaction, self.meeting_id)
            return

        dt = _parse_eve_dt(self.date.value, self.time.value)
        if dt is None:
            await interaction.response.send_message(
                "❌ Invalid date or time format.\n"
                "Date must be `YYYY-MM-DD` and time must be `HH:MM` (24-hour EVE Time / UTC).",
                ephemeral=True,
            )
            return

        await self.cog._update_meeting(
            interaction=  interaction,
            meeting_id=   self.meeting_id,
            new_title=    self.mtg_title.value.strip(),
            new_desc=     self.description.value.strip(),
            new_timestamp=int(dt.timestamp()),
        )


# ============================================================
# PERSISTENT VIEW
# ============================================================

class MeetingView(discord.ui.View):
    """
    Two-button persistent view attached to every meeting embed.

    • "📝 Add Topic"      — any member, opens AddTopicModal
    • "⚙️ Manage Meeting" — leadership only, opens ManageMeetingModal

    Both buttons have the meeting_id baked into their custom_id so the view
    can be re-registered after a bot restart with no extra lookup.

    cancelled=True  → both buttons are pre-disabled (re-registration of
                       closed meetings after restart).
    """

    def __init__(self, meeting_id: str, cancelled: bool = False):
        super().__init__(timeout=None)
        self.meeting_id = meeting_id
        self._cancelled = cancelled

        # Build buttons with stable, unique custom_ids
        add_btn = discord.ui.Button(
            label=     "📝 Add Topic",
            style=     discord.ButtonStyle.secondary,
            custom_id= f"mtg_topic:{meeting_id}",
            disabled=  cancelled,
        )
        add_btn.callback = self._add_topic_cb
        self.add_item(add_btn)

        mgr_btn = discord.ui.Button(
            label=     "⚙️ Manage Meeting",
            style=     discord.ButtonStyle.primary,
            custom_id= f"mtg_manage:{meeting_id}",
            disabled=  cancelled,
        )
        mgr_btn.callback = self._manage_cb
        self.add_item(mgr_btn)

    # ── Button callbacks ──────────────────────────────────────────────────────

    async def _add_topic_cb(self, interaction: discord.Interaction) -> None:
        cog: Optional["MeetingCog"] = interaction.client.cogs.get("MeetingCog")  # type: ignore
        if cog is None:
            await interaction.response.send_message(
                "❌ Meeting cog unavailable.", ephemeral=True
            )
            return
        await interaction.response.send_modal(
            AddTopicModal(cog, self.meeting_id)
        )

    async def _manage_cb(self, interaction: discord.Interaction) -> None:
        cog: Optional["MeetingCog"] = interaction.client.cogs.get("MeetingCog")  # type: ignore
        if cog is None:
            await interaction.response.send_message(
                "❌ Meeting cog unavailable.", ephemeral=True
            )
            return

        # Leadership gate
        if not isinstance(interaction.user, discord.Member) or \
                not _has_leadership(interaction.user):
            await interaction.response.send_message(
                "❌ Only **ARC Security Corporation Leader** or "
                "**ARC Security Administration Council** can manage meetings.",
                ephemeral=True,
            )
            return

        # Load current values to pre-fill the modal
        meetings = await _load_meetings()
        mtg      = meetings.get(self.meeting_id)
        if not mtg:
            await interaction.response.send_message(
                "⚠️ Meeting record not found.", ephemeral=True
            )
            return

        await interaction.response.send_modal(
            ManageMeetingModal(cog, self.meeting_id, mtg)
        )


# ============================================================
# COG
# ============================================================

class MeetingCog(commands.Cog, name="MeetingCog"):

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    # ----------------------------------------------------------------
    # on_ready — re-register all persistent views
    # ----------------------------------------------------------------

    @commands.Cog.listener()
    async def on_ready(self) -> None:
        meetings = await _load_meetings()
        count    = 0
        for meeting_id, mtg in meetings.items():
            if not isinstance(mtg, dict):
                continue
            cancelled = mtg.get("status") == "cancelled"
            view      = MeetingView(meeting_id, cancelled=cancelled)
            msg_id    = mtg.get("message_id")
            try:
                if isinstance(msg_id, int):
                    self.bot.add_view(view, message_id=msg_id)
                else:
                    self.bot.add_view(view)
                count += 1
            except Exception as e:
                print(
                    f"[meeting] Could not re-register view for meeting "
                    f"{meeting_id[:8]}: {e}"
                )
        print(f"[meeting] Re-registered {count} meeting view(s).")

    # ----------------------------------------------------------------
    # Internal helpers
    # ----------------------------------------------------------------

    async def _post_meeting(
        self,
        interaction:   discord.Interaction,
        title:         str,
        description:   str,
        eve_timestamp: int,
    ) -> None:
        """Create the meeting record, post the embed, save to disk."""
        guild = interaction.guild
        if not guild:
            await interaction.followup.send("Must be used in a server.", ephemeral=True)
            return

        ch = discord.utils.get(guild.text_channels, name=MEETING_CHANNEL)
        if not ch:
            await interaction.followup.send(
                f"❌ Channel `#{MEETING_CHANNEL}` not found. "
                "Run `/server_setup` to create it.",
                ephemeral=True,
            )
            return

        meeting_id = str(uuid.uuid4())
        mtg: Dict[str, Any] = {
            "meeting_id":   meeting_id,
            "guild_id":     guild.id,
            "channel_id":   ch.id,
            "message_id":   None,
            "title":        title,
            "description":  description,
            "eve_timestamp":eve_timestamp,
            "creator_id":   interaction.user.id,
            "creator_name": interaction.user.display_name,
            "topics":       [],
            "status":       "active",
            "created_at":   datetime.now(timezone.utc).isoformat(),
        }

        # Resolve ping
        ping_role = discord.utils.get(guild.roles, name=PING_ROLE)
        ping_str  = ping_role.mention if ping_role else f"@{PING_ROLE}"

        view = MeetingView(meeting_id)

        try:
            msg = await ch.send(
                content=          ping_str,
                embed=            _build_meeting_embed(mtg),
                view=             view,
                allowed_mentions= discord.AllowedMentions(roles=True),
            )
        except discord.Forbidden:
            await interaction.followup.send(
                f"❌ I don't have permission to post in {ch.mention}.",
                ephemeral=True,
            )
            return
        except Exception as e:
            await interaction.followup.send(
                f"❌ Failed to post meeting: `{e}`", ephemeral=True
            )
            return

        mtg["message_id"] = msg.id

        # Register view persistently
        try:
            self.bot.add_view(view, message_id=msg.id)
        except Exception:
            pass

        # Persist to disk
        meetings = await _load_meetings()
        meetings[meeting_id] = mtg
        await _save_meetings(meetings)

        await interaction.followup.send(
            f"✅ Meeting **{title}** scheduled in {ch.mention}.",
            ephemeral=True,
        )

    async def _add_topic(
        self,
        interaction: discord.Interaction,
        meeting_id:  str,
        topic_text:  str,
    ) -> None:
        """Append a topic to the meeting and refresh the embed."""
        meetings = await _load_meetings()
        mtg      = meetings.get(meeting_id)

        if not mtg or not isinstance(mtg, dict):
            await interaction.response.send_message(
                "⚠️ Meeting record not found.", ephemeral=True
            )
            return

        if mtg.get("status") == "cancelled":
            await interaction.response.send_message(
                "⚠️ This meeting has been cancelled — topics cannot be added.",
                ephemeral=True,
            )
            return

        # Append topic
        mtg.setdefault("topics", []).append({
            "submitted_by_id":   interaction.user.id,
            "submitted_by_name": interaction.user.display_name,
            "topic":             topic_text,
            "at":                datetime.now(timezone.utc).isoformat(),
        })

        meetings[meeting_id] = mtg
        await _save_meetings(meetings)

        # Refresh the embed
        await self._refresh_embed(interaction.guild, mtg)

        await interaction.response.send_message(
            "✅ Your topic has been added to the agenda!", ephemeral=True
        )

    async def _update_meeting(
        self,
        interaction:   discord.Interaction,
        meeting_id:    str,
        new_title:     str,
        new_desc:      str,
        new_timestamp: int,
    ) -> None:
        """Apply leadership edits to the meeting and refresh the embed."""
        meetings = await _load_meetings()
        mtg      = meetings.get(meeting_id)

        if not mtg or not isinstance(mtg, dict):
            await interaction.response.send_message(
                "⚠️ Meeting record not found.", ephemeral=True
            )
            return

        mtg["title"]         = new_title
        mtg["description"]   = new_desc
        mtg["eve_timestamp"] = new_timestamp
        mtg["last_edited_by"]   = interaction.user.display_name
        mtg["last_edited_at"]   = datetime.now(timezone.utc).isoformat()

        meetings[meeting_id] = mtg
        await _save_meetings(meetings)

        await self._refresh_embed(interaction.guild, mtg)

        await interaction.response.send_message(
            "✅ Meeting updated successfully.", ephemeral=True
        )

    async def _cancel_meeting(
        self,
        interaction: discord.Interaction,
        meeting_id:  str,
    ) -> None:
        """Mark the meeting as cancelled, disable buttons, update embed."""
        meetings = await _load_meetings()
        mtg      = meetings.get(meeting_id)

        if not mtg or not isinstance(mtg, dict):
            await interaction.response.send_message(
                "⚠️ Meeting record not found.", ephemeral=True
            )
            return

        if mtg.get("status") == "cancelled":
            await interaction.response.send_message(
                "⚠️ This meeting is already cancelled.", ephemeral=True
            )
            return

        mtg["status"]          = "cancelled"
        mtg["cancelled_by"]    = interaction.user.display_name
        mtg["cancelled_at"]    = datetime.now(timezone.utc).isoformat()

        meetings[meeting_id] = mtg
        await _save_meetings(meetings)

        # Refresh embed with disabled buttons
        guild = interaction.guild
        ch    = guild.get_channel(mtg.get("channel_id")) if guild else None
        if isinstance(ch, discord.TextChannel):
            msg_id = mtg.get("message_id")
            if isinstance(msg_id, int):
                try:
                    msg = await ch.fetch_message(msg_id)
                    cancelled_view = MeetingView(meeting_id, cancelled=True)
                    await msg.edit(
                        embed= _build_meeting_embed(mtg),
                        view=  cancelled_view,
                    )
                except Exception as e:
                    print(f"[meeting] Could not update cancelled embed for {meeting_id[:8]}: {e}")

        await interaction.response.send_message(
            "❌ Meeting has been **cancelled**. The embed has been updated.",
            ephemeral=True,
        )

    async def _refresh_embed(
        self,
        guild:      Optional[discord.Guild],
        mtg:        Dict[str, Any],
    ) -> None:
        """Fetch the meeting message and edit the embed in place."""
        if not guild:
            return
        ch = guild.get_channel(mtg.get("channel_id"))
        if not isinstance(ch, discord.TextChannel):
            return
        msg_id = mtg.get("message_id")
        if not isinstance(msg_id, int):
            return
        try:
            msg  = await ch.fetch_message(msg_id)
            view = MeetingView(
                mtg["meeting_id"],
                cancelled=(mtg.get("status") == "cancelled"),
            )
            await msg.edit(embed=_build_meeting_embed(mtg), view=view)
        except Exception as e:
            print(
                f"[meeting] Embed refresh failed for meeting "
                f"{mtg.get('meeting_id', '?')[:8]}: {e}"
            )

    # ----------------------------------------------------------------
    # /meeting
    # ----------------------------------------------------------------

    @app_commands.command(
        name="meeting",
        description="Schedule an officer meeting in #officer-meeting (leadership only).",
    )
    async def meeting(self, interaction: discord.Interaction) -> None:
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message(
                "Must be used in a server.", ephemeral=True
            )
            return

        if not _has_leadership(interaction.user):
            await interaction.response.send_message(
                "❌ Only **ARC Security Corporation Leader** or "
                "**ARC Security Administration Council** can schedule meetings.",
                ephemeral=True,
            )
            return

        await interaction.response.send_modal(MeetingCreateModal(self))


# ============================================================
# SETUP
# ============================================================

async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(MeetingCog(bot))
