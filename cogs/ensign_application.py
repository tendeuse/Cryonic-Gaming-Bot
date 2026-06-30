# cogs/ensign_application.py
#
# Ensign Role Application
# =======================
# - Posts a persistent "Apply for Ensign" button panel in a channel.
# - Clicking the button opens a DM with the applicant and asks the application
#   questions ONE AT A TIME — the applicant answers each by replying in the DM.
# - When finished, the bot builds a Q&A .txt file (named after the applicant)
#   and delivers it BOTH:
#       • as a DM to the configured recipient (RECIPIENT_USER_ID), and
#       • to the #ensign-applications review channel (auto-created if missing).
# - The panel survives restarts (stored message ID; no duplicate on reconnect).
# - One active application per user at a time; "cancel" aborts; sessions time out.
#
# RESTART-SAFE INTERVIEWS:
#   In-progress DM interviews are persisted to disk after every answer. The Q&A
#   is driven by an on_message listener (not an in-memory wait_for), so a restart
#   never drops a session. On boot the bot re-sends the current question to any
#   unfinished applicant, and a background sweeper expires idle sessions.
#
# Manual command:
#   /ensign_setup  — post / refresh the application panel (leadership only)

import asyncio
import io
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import discord
from discord.ext import commands, tasks
from discord import app_commands

from . import db
from .uiutil import edit_if_changed

# ============================================================
# CONFIG
# ============================================================

# Channel where the "Apply for Ensign" panel is posted (members click here).
PANEL_CHANNEL_NAME = "ensign-applications"

# Separate, leadership-only channel where completed application transcripts
# (the Q&A logs) are posted for review.
REVIEW_CHANNEL_NAME = "ensign-app-review"

# Discord user ID that receives the completed Q&A file via DM.
# Leave as 0 to skip the DM (the review channel still gets the file).
# Can also be overridden via the ENSIGN_APP_RECIPIENT_ID environment variable.
RECIPIENT_USER_ID = int(os.getenv("ENSIGN_APP_RECIPIENT_ID", "306935804054208523"))

# Roles allowed to run /ensign_setup.
SETUP_ROLES: tuple[str, ...] = (
    "ARC Security Administration Council",
    "ARC Security Corporation Leader",
)

# How long (seconds) an application may sit idle (no new answer) before the
# bot expires it. Measured from the applicant's last reply.
SESSION_TIMEOUT = 60 * 60 * 2  # 2 hours

# How often (minutes) the background sweeper checks for idle sessions.
SWEEP_INTERVAL_MIN = 10

# Word an applicant can send at any prompt to abort the application.
CANCEL_WORD = "cancel"

# The application questions, asked one at a time in order.
QUESTIONS: List[str] = [
    "Discord username and ID (e.g., username#0000 or @username)",
    "How old are you?",
    "What timezone are you in?",
    "How long have you been in ARC Security?",
    "How many hours per week can you realistically dedicate to server duties?",
    "Have you ever held a moderation/officer role on this or another server? "
    "If so, describe your experience.",
    "Ensigns are required to host a minimum of 3 events/classes per week. "
    "Are you able to commit to this? If yes, what types of events/classes would "
    "you propose hosting?",
    "What type of event would you most like to run, and how would you organize it?",
    "How would you approach training and onboarding new members so they feel "
    "welcomed and understand the rules?",
    "Describe a time you had to handle a conflict or disagreement between members. "
    "How did you resolve it?",
    "What do you think makes a good Ensign/officer, and why do you think "
    "you'd be a good fit?",
    "Are there any times you're regularly unavailable (work, school, etc.) that "
    "we should know about?",
    "Anything else you'd like us to know?",
]

# ============================================================
# PERSISTENCE  (panel message id only)
# ============================================================

PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)
DATA_FILE = PERSIST_ROOT / "ensign_application.json"

_file_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _file_lock
    if _file_lock is None:
        _file_lock = asyncio.Lock()
    return _file_lock


def _atomic_write(data: Dict[str, Any]) -> None:
    db.kv_save("ensign_application", data)


async def _load() -> Dict[str, Any]:
    async with _get_lock():
        try:
            data = await asyncio.to_thread(
                db.kv_load, "ensign_application", {"panels": {}, "sessions": {}}
            )
            data.setdefault("panels", {})
            data.setdefault("sessions", {})
            return data
        except Exception as e:
            print(f"[ensign_application] Data load error: {e} — using defaults")
            return {"panels": {}, "sessions": {}}


async def _save(data: Dict[str, Any]) -> None:
    async with _get_lock():
        await asyncio.to_thread(_atomic_write, data)


# ============================================================
# HELPERS
# ============================================================

def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _has_any_role(member: discord.Member, role_names: tuple[str, ...]) -> bool:
    return any(r.name in role_names for r in member.roles)


def _safe_filename(name: str) -> str:
    cleaned = "".join(c if (c.isalnum() or c in "-_.") else "_" for c in name).strip("_")
    return (cleaned[:50] or "applicant")


def _build_transcript(applicant: discord.abc.User, answers: List[str]) -> str:
    lines: List[str] = [
        "=" * 60,
        "  ENSIGN ROLE APPLICATION",
        f"  Applicant : {applicant} (ID: {applicant.id})",
        f"  Submitted : {_utcnow()}",
        "=" * 60,
        "",
    ]
    for idx, (question, answer) in enumerate(zip(QUESTIONS, answers), start=1):
        lines.append(f"Q{idx}. {question}")
        lines.append(f"A{idx}. {answer if answer.strip() else '<no answer>'}")
        lines.append("")
    lines.append("=" * 60)
    lines.append("  END OF APPLICATION")
    lines.append("=" * 60)
    return "\n".join(lines)


# ============================================================
# PANEL EMBED + VIEW
# ============================================================

def _build_panel_embed() -> discord.Embed:
    embed = discord.Embed(
        title="🎖️ Ensign Role Application",
        description=(
            "Interested in becoming an **Ensign**? Ensigns organize and "
            "lead corporation activities and help train new pilots "
            "(minimum **3 events/classes per week**).\n\n"
            "Click **Apply for Ensign** below and the bot will message you "
            "the application questions **one at a time** here in your DMs — just "
            "reply to each.\n\n"
            f"You can type **`{CANCEL_WORD}`** at any time to stop.\n"
            "⚠️ Make sure your DMs are open so the bot can reach you."
        ),
        color=discord.Color.blurple(),
    )
    embed.set_footer(text="Cryonic Gaming — Ensign Applications")
    return embed


class EnsignPanelView(discord.ui.View):
    """Persistent panel view. Stable custom_id so it survives restarts."""

    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Apply for Ensign",
        style=discord.ButtonStyle.primary,
        emoji="🎖️",
        custom_id="ensign_application:apply",
    )
    async def apply(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ) -> None:
        cog: Optional["EnsignApplicationCog"] = interaction.client.cogs.get(
            "EnsignApplicationCog"
        )  # type: ignore
        if cog is None:
            await interaction.response.send_message(
                "❌ The application system is currently unavailable.", ephemeral=True
            )
            return
        await cog._handle_apply(interaction)


# ============================================================
# COG
# ============================================================

class EnsignApplicationCog(commands.Cog, name="EnsignApplicationCog"):
    """DM-based Ensign role application with a persistent button panel."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        # Serializes read-modify-write of session state across handlers.
        self._session_lock = asyncio.Lock()
        # Guard so unfinished interviews are re-prompted once per process start,
        # not on every on_ready (which also fires on reconnect/resume).
        self._resumed = False

    # ----------------------------------------------------------------
    # Lifecycle — background sweeper for idle sessions
    # ----------------------------------------------------------------

    async def cog_load(self) -> None:
        self.session_sweeper.start()

    async def cog_unload(self) -> None:
        self.session_sweeper.cancel()

    # ----------------------------------------------------------------
    # on_ready — register view, ensure panel, resume unfinished interviews
    # ----------------------------------------------------------------

    @commands.Cog.listener()
    async def on_ready(self) -> None:
        self.bot.add_view(EnsignPanelView())

        data = await _load()
        for guild in self.bot.guilds:
            try:
                await self._ensure_panel(guild, data)
            except Exception as e:
                print(f"[ensign_application] Panel setup error in '{guild.name}': {e}")
        await _save(data)

        if not self._resumed:
            self._resumed = True
            await self._resume_sessions()

    async def _resume_sessions(self) -> None:
        """Re-send the current question to anyone mid-application after a restart."""
        async with self._session_lock:
            data = await _load()
            sessions = dict(data.get("sessions", {}))

        for key, sess in sessions.items():
            try:
                idx = int(sess.get("index", 0))
                if idx >= len(QUESTIONS):
                    continue  # malformed/finished — leave for the sweeper
                user = self.bot.get_user(int(key)) or await self.bot.fetch_user(int(key))
                if user is None:
                    continue
                dm = user.dm_channel or await user.create_dm()
                await dm.send(
                    "👋 I'm back online — let's pick up your **Ensign "
                    "application** right where we left off."
                )
                await self._send_question(dm, idx)
            except Exception as e:
                print(f"[ensign_application] Could not resume session {key}: {e}")

    # ----------------------------------------------------------------
    # Channel helpers
    # ----------------------------------------------------------------

    async def _ensure_panel_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        ch = discord.utils.get(guild.text_channels, name=PANEL_CHANNEL_NAME)
        if ch:
            return ch

        bot_member = guild.get_member(self.bot.user.id) if self.bot.user else None
        overwrites: Dict[Any, discord.PermissionOverwrite] = {
            guild.default_role: discord.PermissionOverwrite(
                send_messages=False,
                add_reactions=False,
                view_channel=True,
            )
        }
        if bot_member:
            overwrites[bot_member] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                embed_links=True,
                attach_files=True,
                manage_messages=True,
            )
        try:
            ch = await guild.create_text_channel(
                PANEL_CHANNEL_NAME,
                overwrites=overwrites,
                reason="Ensign Application System — panel channel",
            )
            print(f"[ensign_application] Created #{PANEL_CHANNEL_NAME} in '{guild.name}'.")
            return ch
        except discord.Forbidden:
            print(
                f"[ensign_application] Cannot create #{PANEL_CHANNEL_NAME} "
                f"in '{guild.name}' — missing permissions."
            )
            return None

    async def _get_review_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        """Find or create the leadership-only review channel for transcripts."""
        ch = discord.utils.get(guild.text_channels, name=REVIEW_CHANNEL_NAME)
        if ch:
            return ch

        # Hidden from @everyone; visible to leadership roles + the bot.
        bot_member = guild.get_member(self.bot.user.id) if self.bot.user else None
        overwrites: Dict[Any, discord.PermissionOverwrite] = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False)
        }
        for role in guild.roles:
            if role.name in SETUP_ROLES:
                overwrites[role] = discord.PermissionOverwrite(
                    view_channel=True,
                    read_message_history=True,
                )
        if bot_member:
            overwrites[bot_member] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                embed_links=True,
                attach_files=True,
            )
        try:
            ch = await guild.create_text_channel(
                REVIEW_CHANNEL_NAME,
                overwrites=overwrites,
                reason="Ensign Application System — review channel",
            )
            print(f"[ensign_application] Created #{REVIEW_CHANNEL_NAME} in '{guild.name}'.")
            return ch
        except discord.Forbidden:
            print(
                f"[ensign_application] Cannot create #{REVIEW_CHANNEL_NAME} "
                f"in '{guild.name}' — missing permissions."
            )
            return None

    # ----------------------------------------------------------------
    # Panel management
    # ----------------------------------------------------------------

    async def _ensure_panel(self, guild: discord.Guild, data: Dict[str, Any]) -> None:
        ch = await self._ensure_panel_channel(guild)
        if ch is None:
            return

        panels = data.setdefault("panels", {})
        gkey = str(guild.id)
        panel_rec = panels.get(gkey, {})
        msg_id = panel_rec.get("message_id")

        embed = _build_panel_embed()
        view = EnsignPanelView()

        if msg_id:
            try:
                existing = await ch.fetch_message(int(msg_id))
                await edit_if_changed(existing, embed=embed, view=view)
                return
            except (discord.NotFound, discord.HTTPException):
                pass  # deleted — post a fresh one

        try:
            msg = await ch.send(embed=embed, view=view)
            panels[gkey] = {"channel_id": ch.id, "message_id": msg.id}
            data["panels"] = panels
            print(f"[ensign_application] Panel posted in '{guild.name}' #{ch.name}.")
        except discord.Forbidden:
            print(
                f"[ensign_application] Cannot post panel in #{ch.name} "
                f"in '{guild.name}' — missing permissions."
            )

    # ----------------------------------------------------------------
    # Apply handler — opens a persistent DM Q&A session
    # ----------------------------------------------------------------

    async def _send_question(self, dm: discord.abc.Messageable, index: int) -> None:
        await dm.send(
            f"**Question {index + 1} of {len(QUESTIONS)}**\n{QUESTIONS[index]}"
        )

    async def _handle_apply(self, interaction: discord.Interaction) -> None:
        user = interaction.user
        guild = interaction.guild

        # Ack immediately so we never blow the 3s interaction window while we
        # open the DM and write session state to disk.
        await interaction.response.defer(ephemeral=True)

        async with self._session_lock:
            data = await _load()
            sessions = data.setdefault("sessions", {})

            if str(user.id) in sessions:
                await interaction.followup.send(
                    "❌ You already have an application in progress — check your DMs.",
                    ephemeral=True,
                )
                return

            # Open the DM first so we can fail gracefully on closed DMs (and we
            # only persist a session once we know we can reach the applicant).
            try:
                dm = user.dm_channel or await user.create_dm()
                await dm.send(
                    embed=discord.Embed(
                        title="🎖️ Ensign Role Application",
                        description=(
                            "Thanks for applying! I'll ask you a series of questions "
                            "**one at a time** — just reply to each one.\n\n"
                            f"There are **{len(QUESTIONS)}** questions. Your progress "
                            "is saved as you go, so even if the bot restarts you can "
                            "continue right where you left off.\n"
                            f"Type **`{CANCEL_WORD}`** at any point to stop.\n\n"
                            "Let's begin! 👇"
                        ),
                        color=discord.Color.blurple(),
                    )
                )
            except discord.Forbidden:
                await interaction.followup.send(
                    "❌ I couldn't DM you. Please enable **Direct Messages** from server "
                    "members (Privacy Settings) and click the button again.",
                    ephemeral=True,
                )
                return

            now = _utcnow()
            sessions[str(user.id)] = {
                "user_id": user.id,
                "user_name": str(user),
                "guild_id": guild.id if guild else None,
                "answers": [],
                "index": 0,
                "started_at": now,
                "updated_at": now,
            }
            await _save(data)

        await interaction.followup.send(
            "📬 Check your DMs — I've started your Ensign application there.",
            ephemeral=True,
        )
        await self._send_question(dm, 0)

    # ----------------------------------------------------------------
    # on_message — drives each DM answer (restart-safe; reads session from disk)
    # ----------------------------------------------------------------

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot or not isinstance(message.channel, discord.DMChannel):
            return

        async with self._session_lock:
            data = await _load()
            sessions = data.setdefault("sessions", {})
            sess = sessions.get(str(message.author.id))
            if not sess:
                return  # not an applicant — ignore

            content = message.content.strip()

            if content.lower() == CANCEL_WORD:
                sessions.pop(str(message.author.id), None)
                await _save(data)
                await message.channel.send(
                    "❌ Application cancelled. No worries — apply again anytime!"
                )
                return

            # Record the answer and advance.
            sess["answers"].append(content or "<no answer>")
            sess["index"] = len(sess["answers"])
            sess["updated_at"] = _utcnow()

            if sess["index"] < len(QUESTIONS):
                next_index = sess["index"]
                await _save(data)
                await self._send_question(message.channel, next_index)
                return

            # Finished — capture what we need, remove the session, then deliver.
            answers = list(sess["answers"])
            guild_id = sess.get("guild_id")
            sessions.pop(str(message.author.id), None)
            await _save(data)

        guild = self.bot.get_guild(guild_id) if guild_id else None
        try:
            await self._deliver(message.author, answers, guild)
            await message.channel.send(
                "✅ **Application submitted!** Thank you — your responses have been "
                "sent to leadership for review. Good luck! 🎖️"
            )
        except Exception as e:
            print(
                f"[ensign_application] Delivery error for "
                f"{message.author} ({message.author.id}): {e}"
            )
            await message.channel.send(
                "⚠️ Your answers were recorded but I hit a problem delivering them. "
                "Please contact leadership so they can follow up."
            )

    # ----------------------------------------------------------------
    # Background sweeper — expire idle sessions
    # ----------------------------------------------------------------

    @tasks.loop(minutes=SWEEP_INTERVAL_MIN)
    async def session_sweeper(self) -> None:
        async with self._session_lock:
            data = await _load()
            sessions = data.setdefault("sessions", {})
            now = datetime.now(timezone.utc)
            expired: List[int] = []

            for key, sess in list(sessions.items()):
                try:
                    updated = datetime.fromisoformat(sess.get("updated_at"))
                except Exception:
                    updated = now
                if (now - updated).total_seconds() > SESSION_TIMEOUT:
                    expired.append(int(key))
                    sessions.pop(key, None)

            if expired:
                await _save(data)

        for user_id in expired:
            try:
                user = self.bot.get_user(user_id) or await self.bot.fetch_user(user_id)
                if user is not None:
                    dm = user.dm_channel or await user.create_dm()
                    await dm.send(
                        "⏰ Your Ensign application timed out due to inactivity "
                        "and has been cancelled. Click the button again to restart."
                    )
            except Exception:
                pass

    @session_sweeper.before_loop
    async def before_session_sweeper(self) -> None:
        await self.bot.wait_until_ready()

    # ----------------------------------------------------------------
    # Delivery — DM the recipient + post to the review channel
    # ----------------------------------------------------------------

    async def _deliver(
        self,
        user: discord.abc.User,
        answers: List[str],
        guild: Optional[discord.Guild],
    ) -> None:
        transcript = _build_transcript(user, answers)
        filename = f"ensign_application_{_safe_filename(str(user))}.txt"
        caption = (
            f"🎖️ **New Ensign Application** — `{user}` (ID: `{user.id}`)"
        )

        def make_file() -> discord.File:
            return discord.File(io.BytesIO(transcript.encode("utf-8")), filename=filename)

        delivered = False

        # 1) DM the configured recipient.
        if RECIPIENT_USER_ID:
            try:
                recipient = self.bot.get_user(RECIPIENT_USER_ID) or await self.bot.fetch_user(
                    RECIPIENT_USER_ID
                )
                if recipient is not None:
                    await recipient.send(content=caption, file=make_file())
                    delivered = True
            except Exception as e:
                print(f"[ensign_application] Could not DM recipient {RECIPIENT_USER_ID}: {e}")

        # 2) Post to the review channel.
        if guild is not None:
            try:
                review = await self._get_review_channel(guild)
                if review is not None:
                    await review.send(content=caption, file=make_file())
                    delivered = True
            except Exception as e:
                print(f"[ensign_application] Could not post to review channel: {e}")

        if not delivered:
            print(
                f"[ensign_application] WARNING: application from {user} ({user.id}) "
                "could not be delivered to a recipient DM or a review channel."
            )

    # ----------------------------------------------------------------
    # /ensign_setup — post / refresh the panel
    # ----------------------------------------------------------------

    @app_commands.command(
        name="ensign_setup",
        description="Post or refresh the Ensign application panel (leadership only).",
    )
    async def ensign_setup(self, interaction: discord.Interaction) -> None:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message(
                "Must be used in a server.", ephemeral=True
            )
            return

        if not _has_any_role(interaction.user, SETUP_ROLES):
            await interaction.response.send_message(
                "❌ Only **ARC Security Administration Council** or "
                "**ARC Security Corporation Leader** can use this command.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)

        data = await _load()
        # Force a fresh post by clearing the stored message id for this guild.
        data.setdefault("panels", {}).pop(str(interaction.guild.id), None)
        await self._ensure_panel(interaction.guild, data)
        await _save(data)

        await interaction.followup.send(
            f"✅ Ensign application panel refreshed in `#{PANEL_CHANNEL_NAME}`.",
            ephemeral=True,
        )


# ============================================================
# SETUP
# ============================================================

async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(EnsignApplicationCog(bot))
