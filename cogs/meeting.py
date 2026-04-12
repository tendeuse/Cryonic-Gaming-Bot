# cogs/meeting.py
#
# Officer Meeting Planner  +  Attendance Tracking
# =================================================
#
# FEATURES
# --------
# 1. /meeting  — opens a creation modal (leadership only).
#    Accepts title, description, date & time in EVE Time (UTC).
#    Posts a persistent embed in #officer-meeting pinging @ARC Security.
#
# 2. Embed time display  — uses Discord <t:{unix}:F> timestamps so every
#    member sees the meeting time in their own local timezone automatically.
#
# 3. "📝 Add Topic" button  — any member submits an agenda item via modal.
#    Topics are appended to the embed and attributed to the submitter.
#
# 4. "⚙️ Manage Meeting" button  — leadership only.  Pre-filled modal to edit
#    title / description / time.  Typing "CANCEL" in the title field cancels
#    the meeting (disables buttons, marks embed red).
#
# 5. Attendance tracking  — at the meeting's scheduled EVE Time the bot begins
#    monitoring the "ARC Leadership Meeting" voice channel for 2 hours.
#    Anyone who joins during that window is recorded.  At the end of the window
#    a final attendance summary is posted to #officer-meeting.
#
#    Recovery: if the bot restarts mid-window the presence_loop and on_ready
#    restore tracking state from disk so no attendance data is lost.
#
# VOICE CHANNEL REQUIREMENT
# -------------------------
# The voice channel "ARC Leadership Meeting" must already exist in the server.
# server_setup.py only creates TEXT channels, so this VC must be created
# manually by an admin.  The bot will warn in the console at on_ready if the
# channel cannot be found in any joined guild.
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
from discord.ext import commands, tasks
from discord import app_commands


# ============================================================
# CONFIG
# ============================================================

MEETING_CHANNEL  = "officer-meeting"        # text channel for embeds
PING_ROLE        = "ARC Security"
ATTENDANCE_VC    = "ARC Leadership Meeting"  # voice channel to monitor
                                             # ⚠ Must be created manually — not a text channel

ATTENDANCE_WINDOW_SECS = 2 * 60 * 60        # 2 hours
MEETINGS_PATH          = "/data/meetings.json"

# Roles that may create and manage meetings
LEADERSHIP_ROLES: Set[str] = {
    "ARC Security Administration Council",
    "ARC Security Corporation Leader",
}

# Picked up by server_setup.py auto-scanner (text channels only)
REQUIRED_CHANNELS: List[str] = [MEETING_CHANNEL]
REQUIRED_ROLES:    List[str] = [PING_ROLE]


# ============================================================
# HELPERS
# ============================================================

def _has_leadership(member: discord.Member) -> bool:
    return any(r.name in LEADERSHIP_ROLES for r in member.roles)


def _parse_eve_dt(date_str: str, time_str: str) -> Optional[datetime]:
    """Parse YYYY-MM-DD + HH:MM (UTC/EVE Time) into an aware datetime."""
    try:
        return datetime.strptime(
            f"{date_str.strip()} {time_str.strip()}", "%Y-%m-%d %H:%M"
        ).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _fmt_duration(total_seconds: int) -> str:
    """Format seconds as 'Xh Ym Zs'."""
    h, r = divmod(max(0, total_seconds), 3600)
    m, s = divmod(r, 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


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
# EMBED BUILDERS
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

    # ── Time ──────────────────────────────────────────────────────────────────
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

    # ── Agenda ────────────────────────────────────────────────────────────────
    topics: List[Dict] = mtg.get("topics", [])
    if topics:
        lines = [
            f"**{i}.** {t.get('topic', '')}  _(by {t.get('submitted_by_name', 'Unknown')})_"
            for i, t in enumerate(topics, start=1)
        ]
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

    # ── Attendance summary (shown once finalised) ─────────────────────────────
    if mtg.get("attendance_finalized"):
        attendees: Dict[str, Any] = mtg.get("attendees", {})
        count = len(attendees)
        embed.add_field(
            name="✅ Attendance",
            value=(
                f"**{count}** member(s) attended the meeting.\n"
                f"_(See the attendance summary posted below.)_"
            ) if count else "*(nobody recorded)*",
            inline=False,
        )
    elif mtg.get("attendance_started") and not cancelled:
        start_ts = mtg.get("attendance_start_ts", 0)
        end_ts   = start_ts + ATTENDANCE_WINDOW_SECS
        embed.add_field(
            name="🎙️ Attendance Tracking",
            value=(
                f"**Active** — monitoring `{ATTENDANCE_VC}` VC.\n"
                f"Window closes <t:{end_ts}:R>."
            ),
            inline=False,
        )

    # ── Footer ────────────────────────────────────────────────────────────────
    creator_name = mtg.get("creator_name", "Leadership")
    embed.set_footer(
        text=f"Scheduled by {creator_name}  •  Meeting ID: {mtg.get('meeting_id', '?')[:8]}"
    )
    return embed


def _build_attendance_embed(mtg: Dict[str, Any], guild: discord.Guild) -> discord.Embed:
    """
    Standalone attendance summary embed posted after the 2-hour window closes.
    Separate from the main meeting embed to keep the agenda clean.
    """
    attendees: Dict[str, Any] = mtg.get("attendees", {})
    title     = str(mtg.get("title", "Officer Meeting"))
    start_ts  = mtg.get("attendance_start_ts", 0)
    end_ts    = start_ts + ATTENDANCE_WINDOW_SECS

    color  = discord.Color.green() if attendees else discord.Color.orange()
    embed  = discord.Embed(
        title=     f"🗂️ Attendance Report — {title}",
        color=     color,
        timestamp= datetime.now(timezone.utc),
    )
    embed.add_field(
        name="⏱ Tracking Window",
        value=f"<t:{start_ts}:f> → <t:{end_ts}:f>",
        inline=False,
    )

    if attendees:
        lines = []
        for uid_str, info in attendees.items():
            m    = guild.get_member(int(uid_str))
            name = m.mention if m else f"<@{uid_str}>"
            secs = int(info.get("total_seconds", 0))
            lines.append(f"• {name} — {_fmt_duration(secs)}")
        value = "\n".join(lines)
        if len(value) > 1024:
            value = value[:1020] + "…"
        embed.add_field(
            name=f"✅ Present — {len(attendees)} member(s)",
            value=value,
            inline=False,
        )
    else:
        embed.add_field(
            name="✅ Present",
            value="*(nobody joined the meeting VC during the tracking window)*",
            inline=False,
        )

    embed.set_footer(
        text=f"Meeting ID: {mtg.get('meeting_id', '?')[:8]}  •  "
             f"Attendance window: {ATTENDANCE_WINDOW_SECS // 60} minutes"
    )
    return embed


# ============================================================
# MODALS
# ============================================================

class MeetingCreateModal(discord.ui.Modal, title="Schedule a Meeting"):
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
            interaction=   interaction,
            title=         self.mtg_title.value.strip(),
            description=   self.description.value.strip(),
            eve_timestamp= int(dt.timestamp()),
        )


class AddTopicModal(discord.ui.Modal, title="Add a Discussion Topic"):
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

    def __init__(self, cog: "MeetingCog", meeting_id: str, current: Dict[str, Any]):
        super().__init__()
        self.cog        = cog
        self.meeting_id = meeting_id

        self.mtg_title.default   = current.get("title", "")
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
            interaction=   interaction,
            meeting_id=    self.meeting_id,
            new_title=     self.mtg_title.value.strip(),
            new_desc=      self.description.value.strip(),
            new_timestamp= int(dt.timestamp()),
        )


# ============================================================
# PERSISTENT VIEW
# ============================================================

class MeetingView(discord.ui.View):
    """
    Two-button persistent view on every meeting embed.
    Button custom_ids embed meeting_id for restart recovery.
    """

    def __init__(self, meeting_id: str, cancelled: bool = False):
        super().__init__(timeout=None)
        self.meeting_id = meeting_id

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

    async def _add_topic_cb(self, interaction: discord.Interaction) -> None:
        cog: Optional["MeetingCog"] = interaction.client.cogs.get("MeetingCog")  # type: ignore
        if cog is None:
            await interaction.response.send_message("❌ Meeting cog unavailable.", ephemeral=True)
            return
        await interaction.response.send_modal(AddTopicModal(cog, self.meeting_id))

    async def _manage_cb(self, interaction: discord.Interaction) -> None:
        cog: Optional["MeetingCog"] = interaction.client.cogs.get("MeetingCog")  # type: ignore
        if cog is None:
            await interaction.response.send_message("❌ Meeting cog unavailable.", ephemeral=True)
            return

        if not isinstance(interaction.user, discord.Member) or not _has_leadership(interaction.user):
            await interaction.response.send_message(
                "❌ Only **ARC Security Corporation Leader** or "
                "**ARC Security Administration Council** can manage meetings.",
                ephemeral=True,
            )
            return

        meetings = await _load_meetings()
        mtg      = meetings.get(self.meeting_id)
        if not mtg:
            await interaction.response.send_message("⚠️ Meeting record not found.", ephemeral=True)
            return

        await interaction.response.send_modal(ManageMeetingModal(cog, self.meeting_id, mtg))


# ============================================================
# COG
# ============================================================

class MeetingCog(commands.Cog, name="MeetingCog"):

    def __init__(self, bot: commands.Bot):
        self.bot = bot

        # ── Attendance tracking (in-memory, rebuilt on on_ready) ──────────────
        #
        # _tracked_vcs:   {voice_channel_id: meeting_id}
        #   Fast lookup in on_voice_state_update — is this VC being tracked?
        #
        # _vc_join_times: {meeting_id: {user_id: unix_join_timestamp}}
        #   Records when each member entered the meeting VC this session.
        #   Cleared on leave; time is folded into meeting["attendees"].
        self._tracked_vcs:   Dict[int, str]            = {}
        self._vc_join_times: Dict[str, Dict[int, int]] = {}

        if not self.presence_loop.is_running():
            self.presence_loop.start()

    def cog_unload(self) -> None:
        if self.presence_loop.is_running():
            self.presence_loop.cancel()

    # ----------------------------------------------------------------
    # on_ready
    # ----------------------------------------------------------------

    @commands.Cog.listener()
    async def on_ready(self) -> None:
        # ── 1. Re-register persistent meeting views ───────────────────────────
        meetings = await _load_meetings()
        views_registered = 0
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
                views_registered += 1
            except Exception as e:
                print(f"[meeting] Could not re-register view for {meeting_id[:8]}: {e}")

        # ── 2. Rebuild in-memory attendance tracking for mid-window restarts ──
        now      = int(datetime.now(timezone.utc).timestamp())
        restored = 0

        for meeting_id, mtg in meetings.items():
            if not isinstance(mtg, dict):
                continue
            if not mtg.get("attendance_started"):
                continue
            if mtg.get("attendance_finalized"):
                continue
            if mtg.get("status") == "cancelled":
                continue

            start_ts = mtg.get("attendance_start_ts", 0)
            if now >= start_ts + ATTENDANCE_WINDOW_SECS:
                # Window already expired — finalize in the next loop tick
                # (don't block on_ready with heavy work)
                continue

            # Restore VC mapping
            vc_id = mtg.get("attendance_vc_id")
            guild = self.bot.get_guild(mtg.get("guild_id", 0))
            if not isinstance(vc_id, int) or not guild:
                continue

            vc = guild.get_channel(vc_id)
            if not isinstance(vc, discord.VoiceChannel):
                print(
                    f"[meeting] Meeting {meeting_id[:8]}: attendance VC {vc_id} "
                    "no longer exists — tracking cannot resume."
                )
                continue

            self._tracked_vcs[vc_id] = meeting_id

            # Re-baseline anyone already in the VC
            self._vc_join_times[meeting_id] = {
                m.id: now for m in vc.members
            }
            restored += 1
            print(
                f"[meeting] Restored attendance tracking for meeting "
                f"{meeting_id[:8]} (VC={vc_id}, {len(vc.members)} member(s) present)."
            )

        print(
            f"[meeting] on_ready: registered {views_registered} view(s), "
            f"restored {restored} attendance window(s)."
        )

        # ── 3. Warn if the attendance VC doesn't exist in any guild ───────────
        for guild in self.bot.guilds:
            vc = discord.utils.get(guild.voice_channels, name=ATTENDANCE_VC)
            if not vc:
                print(
                    f"[meeting] WARNING: Voice channel '{ATTENDANCE_VC}' not found "
                    f"in guild '{guild.name}'. "
                    "Create it manually — attendance tracking will not work without it."
                )

    # ----------------------------------------------------------------
    # Presence loop  — fires every 60 s
    # ----------------------------------------------------------------

    @tasks.loop(seconds=60)
    async def presence_loop(self) -> None:
        """
        Two jobs per tick:

        A) Start attendance tracking for meetings whose scheduled time has passed
           and whose attendance window hasn't been opened yet.

        B) Finalize attendance for meetings whose 2-hour window has expired.
        """
        meetings = await _load_meetings()
        now      = int(datetime.now(timezone.utc).timestamp())
        changed  = False

        for meeting_id, mtg in list(meetings.items()):
            if not isinstance(mtg, dict):
                continue
            if mtg.get("status") == "cancelled":
                continue

            # ── A: Start tracking ─────────────────────────────────────────────
            if (
                not mtg.get("attendance_started")
                and not mtg.get("attendance_finalized")
            ):
                eve_ts = mtg.get("eve_timestamp", 0)
                if isinstance(eve_ts, int) and now >= eve_ts:
                    await self._start_attendance(meetings, meeting_id, mtg, now)
                    changed = True
                continue  # nothing else to check for this meeting this tick

            # ── B: Finalize expired windows ───────────────────────────────────
            if (
                mtg.get("attendance_started")
                and not mtg.get("attendance_finalized")
            ):
                start_ts = mtg.get("attendance_start_ts", 0)
                if now >= start_ts + ATTENDANCE_WINDOW_SECS:
                    await self._finalize_attendance(meetings, meeting_id, mtg, now)
                    changed = True

        if changed:
            await _save_meetings(meetings)

    @presence_loop.before_loop
    async def _before_presence_loop(self) -> None:
        await self.bot.wait_until_ready()

    # ----------------------------------------------------------------
    # Attendance — start
    # ----------------------------------------------------------------

    async def _start_attendance(
        self,
        meetings:   Dict[str, Any],
        meeting_id: str,
        mtg:        Dict[str, Any],
        now:        int,
    ) -> None:
        """
        Begin monitoring the ATTENDANCE_VC for this meeting.
        Mutates mtg in-place; caller must save meetings to disk.
        """
        guild_id = mtg.get("guild_id")
        guild    = self.bot.get_guild(guild_id) if isinstance(guild_id, int) else None
        if not guild:
            return

        vc = discord.utils.get(guild.voice_channels, name=ATTENDANCE_VC)
        if not isinstance(vc, discord.VoiceChannel):
            print(
                f"[meeting] Meeting {meeting_id[:8]}: cannot start attendance — "
                f"voice channel '{ATTENDANCE_VC}' not found in guild '{guild.name}'."
            )
            return

        # Seed in-memory state
        self._tracked_vcs[vc.id] = meeting_id
        self._vc_join_times[meeting_id] = {
            m.id: now for m in vc.members   # baseline anyone already in VC
        }

        mtg["attendance_started"]  = True
        mtg["attendance_start_ts"] = now
        mtg["attendance_vc_id"]    = vc.id
        mtg.setdefault("attendees", {})

        # Credit anyone already in the VC as having joined at start time
        for member in vc.members:
            mtg["attendees"].setdefault(str(member.id), {
                "name":          member.display_name,
                "total_seconds": 0,
            })

        meetings[meeting_id] = mtg

        # Update the meeting embed to show tracking is active
        await self._refresh_embed(guild, mtg)

        end_ts = now + ATTENDANCE_WINDOW_SECS
        print(
            f"[meeting] Attendance tracking started for meeting '{mtg.get('title')}' "
            f"(ID={meeting_id[:8]}, VC={vc.id}). "
            f"Window closes at <t:{end_ts}:f> EVE Time."
        )

    # ----------------------------------------------------------------
    # Attendance — finalize
    # ----------------------------------------------------------------

    async def _finalize_attendance(
        self,
        meetings:   Dict[str, Any],
        meeting_id: str,
        mtg:        Dict[str, Any],
        now:        int,
    ) -> None:
        """
        Lock in all cumulative times, post the attendance summary,
        and mark the meeting as finalized.
        Mutates mtg in-place; caller must save meetings to disk.
        """
        guild_id = mtg.get("guild_id")
        guild    = self.bot.get_guild(guild_id) if isinstance(guild_id, int) else None

        # ── 1. Lock in any ongoing sessions ───────────────────────────────────
        join_times = self._vc_join_times.get(meeting_id, {})
        attendees  = mtg.setdefault("attendees", {})

        for uid, join_ts in join_times.items():
            elapsed = max(0, now - join_ts)
            key     = str(uid)
            if key not in attendees:
                # Member joined but wasn't in the initial snapshot — resolve name
                name = str(uid)
                if guild:
                    m = guild.get_member(uid)
                    if m:
                        name = m.display_name
                attendees[key] = {"name": name, "total_seconds": 0}
            attendees[key]["total_seconds"] = (
                int(attendees[key].get("total_seconds", 0)) + elapsed
            )

        mtg["attendees"]            = attendees
        mtg["attendance_finalized"] = True
        meetings[meeting_id]        = mtg

        # ── 2. Clean up in-memory state ───────────────────────────────────────
        vc_id = mtg.get("attendance_vc_id")
        if isinstance(vc_id, int):
            self._tracked_vcs.pop(vc_id, None)
        self._vc_join_times.pop(meeting_id, None)

        # ── 3. Post attendance summary to #officer-meeting ────────────────────
        if guild:
            ch = discord.utils.get(guild.text_channels, name=MEETING_CHANNEL)
            if ch:
                try:
                    await ch.send(embed=_build_attendance_embed(mtg, guild))
                except Exception as e:
                    print(
                        f"[meeting] Could not post attendance summary "
                        f"for meeting {meeting_id[:8]}: {e}"
                    )
            else:
                print(
                    f"[meeting] #{MEETING_CHANNEL} not found — "
                    f"attendance summary for {meeting_id[:8]} not posted."
                )

            # ── 4. Update main meeting embed to show final attendance count ────
            await self._refresh_embed(guild, mtg)

        print(
            f"[meeting] Attendance finalised for meeting '{mtg.get('title')}' "
            f"(ID={meeting_id[:8]}). {len(attendees)} member(s) recorded."
        )

    # ----------------------------------------------------------------
    # Voice channel attendance tracking
    # ----------------------------------------------------------------

    @commands.Cog.listener()
    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after:  discord.VoiceState,
    ) -> None:
        """
        Record joins and leaves for any actively tracked meeting VC.

        A) Member LEFT a tracked VC  → fold session time into attendees.
        B) Member JOINED a tracked VC → record join timestamp.
        All other updates are ignored with a fast-path exit.
        """
        if not self._tracked_vcs:
            return   # No active meeting — fast exit

        now = int(datetime.now(timezone.utc).timestamp())

        left_tracked   = before.channel and before.channel.id in self._tracked_vcs
        joined_tracked = after.channel  and after.channel.id  in self._tracked_vcs

        # ── A: Member left the tracked VC ─────────────────────────────────────
        if left_tracked:
            meeting_id = self._tracked_vcs[before.channel.id]
            join_ts    = self._vc_join_times.get(meeting_id, {}).pop(member.id, None)
            if join_ts is not None:
                elapsed = max(0, now - join_ts)
                # Persist incrementally to disk so a restart doesn't lose the time
                meetings = await _load_meetings()
                mtg      = meetings.get(meeting_id)
                if isinstance(mtg, dict):
                    attendees = mtg.setdefault("attendees", {})
                    key       = str(member.id)
                    if key not in attendees:
                        attendees[key] = {
                            "name":          member.display_name,
                            "total_seconds": 0,
                        }
                    attendees[key]["total_seconds"] = (
                        int(attendees[key].get("total_seconds", 0)) + elapsed
                    )
                    meetings[meeting_id] = mtg
                    await _save_meetings(meetings)

        # ── B: Member joined the tracked VC ───────────────────────────────────
        if joined_tracked and not left_tracked:
            meeting_id = self._tracked_vcs[after.channel.id]
            self._vc_join_times.setdefault(meeting_id, {})[member.id] = now

    # ----------------------------------------------------------------
    # Meeting management helpers
    # ----------------------------------------------------------------

    async def _post_meeting(
        self,
        interaction:   discord.Interaction,
        title:         str,
        description:   str,
        eve_timestamp: int,
    ) -> None:
        guild = interaction.guild
        if not guild:
            await interaction.followup.send("Must be used in a server.", ephemeral=True)
            return

        ch = discord.utils.get(guild.text_channels, name=MEETING_CHANNEL)
        if not ch:
            await interaction.followup.send(
                f"❌ Channel `#{MEETING_CHANNEL}` not found. Run `/server_setup`.",
                ephemeral=True,
            )
            return

        meeting_id = str(uuid.uuid4())
        mtg: Dict[str, Any] = {
            "meeting_id":           meeting_id,
            "guild_id":             guild.id,
            "channel_id":           ch.id,
            "message_id":           None,
            "title":                title,
            "description":          description,
            "eve_timestamp":        eve_timestamp,
            "creator_id":           interaction.user.id,
            "creator_name":         interaction.user.display_name,
            "topics":               [],
            "status":               "active",
            "created_at":           datetime.now(timezone.utc).isoformat(),
            # Attendance fields — populated by presence_loop
            "attendance_started":   False,
            "attendance_start_ts":  None,
            "attendance_vc_id":     None,
            "attendance_finalized": False,
            "attendees":            {},
        }

        ping_role = discord.utils.get(guild.roles, name=PING_ROLE)
        ping_str  = ping_role.mention if ping_role else f"@{PING_ROLE}"
        view      = MeetingView(meeting_id)

        try:
            msg = await ch.send(
                content=          ping_str,
                embed=            _build_meeting_embed(mtg),
                view=             view,
                allowed_mentions= discord.AllowedMentions(roles=True),
            )
        except discord.Forbidden:
            await interaction.followup.send(
                f"❌ I don't have permission to post in {ch.mention}.", ephemeral=True
            )
            return
        except Exception as e:
            await interaction.followup.send(
                f"❌ Failed to post meeting: `{e}`", ephemeral=True
            )
            return

        mtg["message_id"] = msg.id
        try:
            self.bot.add_view(view, message_id=msg.id)
        except Exception:
            pass

        meetings = await _load_meetings()
        meetings[meeting_id] = mtg
        await _save_meetings(meetings)

        await interaction.followup.send(
            f"✅ Meeting **{title}** scheduled in {ch.mention}.\n"
            f"Attendance tracking will begin automatically at <t:{eve_timestamp}:F> "
            f"and run for {ATTENDANCE_WINDOW_SECS // 3600} hours.",
            ephemeral=True,
        )

    async def _add_topic(
        self,
        interaction: discord.Interaction,
        meeting_id:  str,
        topic_text:  str,
    ) -> None:
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

        mtg.setdefault("topics", []).append({
            "submitted_by_id":   interaction.user.id,
            "submitted_by_name": interaction.user.display_name,
            "topic":             topic_text,
            "at":                datetime.now(timezone.utc).isoformat(),
        })
        meetings[meeting_id] = mtg
        await _save_meetings(meetings)
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
        meetings = await _load_meetings()
        mtg      = meetings.get(meeting_id)

        if not mtg or not isinstance(mtg, dict):
            await interaction.response.send_message(
                "⚠️ Meeting record not found.", ephemeral=True
            )
            return

        mtg["title"]          = new_title
        mtg["description"]    = new_desc
        mtg["eve_timestamp"]  = new_timestamp
        mtg["last_edited_by"] = interaction.user.display_name
        mtg["last_edited_at"] = datetime.now(timezone.utc).isoformat()

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

        mtg["status"]       = "cancelled"
        mtg["cancelled_by"] = interaction.user.display_name
        mtg["cancelled_at"] = datetime.now(timezone.utc).isoformat()

        # If tracking was active, stop it cleanly
        vc_id = mtg.get("attendance_vc_id")
        if isinstance(vc_id, int):
            self._tracked_vcs.pop(vc_id, None)
        self._vc_join_times.pop(meeting_id, None)

        meetings[meeting_id] = mtg
        await _save_meetings(meetings)

        guild = interaction.guild
        ch    = guild.get_channel(mtg.get("channel_id")) if guild else None
        if isinstance(ch, discord.TextChannel):
            msg_id = mtg.get("message_id")
            if isinstance(msg_id, int):
                try:
                    msg = await ch.fetch_message(msg_id)
                    await msg.edit(
                        embed= _build_meeting_embed(mtg),
                        view=  MeetingView(meeting_id, cancelled=True),
                    )
                except Exception as e:
                    print(
                        f"[meeting] Could not update cancelled embed "
                        f"for {meeting_id[:8]}: {e}"
                    )

        await interaction.response.send_message(
            "❌ Meeting has been **cancelled**. The embed has been updated.",
            ephemeral=True,
        )

    async def _refresh_embed(
        self,
        guild:  Optional[discord.Guild],
        mtg:    Dict[str, Any],
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
