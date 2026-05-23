# cogs/corp_transfer.py
#
# ARC Security Corp Transfer Application Ticket System
# =====================================================
# - Posts a persistent "Apply to ARC Security" button panel in #corp-transfer
# - Clicking "Apply" opens a DM conversation where the bot asks each of the
#   16 application questions one at a time; the applicant replies to each
# - After all answers are submitted a private ticket channel is created,
#   visible only to:
#     • The applicant
#     • ARC Security Administration Council
#     • ARC Security Corporation Leader
#     • ARC General                          ← added vs appeal_ticket.py
# - The ticket auto-posts the applicant's event-participation history and
#   test/certification status by reading the same data files arc_seat.py
#   writes to (roles on the member, signature_tagging_attempts.json,
#   /data/events.json) — no cross-cog dependency required
# - Each user may only have one open ticket at a time
# - Tickets have a Close button (creator or staff only)
# - Panel survives restarts (stored message ID; no duplicate on reconnect)
# - All data persists to /data/corp_transfer_tickets.json

import asyncio
import io
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import discord
from discord import app_commands
from discord.ext import commands

# ============================================================
# CONFIG
# ============================================================

TRANSFER_CHANNEL_NAME  = "corp-transfer"
TICKET_CATEGORY_NAME   = "Tickets"
ADMIN_LOG_CHANNEL_NAME = "transfer-application"

# Roles with access to every ticket
STAFF_ROLES: tuple[str, ...] = (
    "ARC Security Administration Council",
    "ARC Security Corporation Leader",
    "ARC General",
)

# Roles allowed to run /transfer_setup
SETUP_ROLES: tuple[str, ...] = (
    "ARC Security Administration Council",
    "ARC Security Corporation Leader",
)

# ============================================================
# PERSISTENCE
# ============================================================

PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)
DATA_FILE = PERSIST_ROOT / "corp_transfer_tickets.json"

_file_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _file_lock
    if _file_lock is None:
        _file_lock = asyncio.Lock()
    return _file_lock


def _atomic_write(data: Dict[str, Any]) -> None:
    PERSIST_ROOT.mkdir(parents=True, exist_ok=True)
    tmp = DATA_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    tmp.replace(DATA_FILE)


async def _load() -> Dict[str, Any]:
    async with _get_lock():
        if not DATA_FILE.exists():
            return {"panels": {}, "tickets": {}}
        try:
            with open(DATA_FILE, encoding="utf-8") as f:
                raw = f.read().strip()
            if not raw:
                return {"panels": {}, "tickets": {}}
            data = json.loads(raw)
            data.setdefault("panels", {})
            data.setdefault("tickets", {})
            return data
        except Exception as e:
            print(f"[corp_transfer] Data load error: {e} — using defaults")
            return {"panels": {}, "tickets": {}}


async def _save(data: Dict[str, Any]) -> None:
    async with _get_lock():
        _atomic_write(data)


# ============================================================
# HELPERS
# ============================================================

def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _has_any_role(member: discord.Member, role_names: tuple[str, ...]) -> bool:
    return any(r.name in role_names for r in member.roles)


def _staff_roles(guild: discord.Guild) -> List[discord.Role]:
    """Resolve staff role objects from the guild — no hardcoded IDs."""
    return [r for r in guild.roles if r.name in STAFF_ROLES]


# ============================================================
# APPLICATION QUESTIONS
# ============================================================
# Ordered list of (answer_key, prompt) tuples — one DM per question.

APPLICATION_QUESTIONS: List[tuple[str, str]] = [
    ("char_name", "**[1 / 16]** What is your **Main Character / Discord Name**?"),
    ("q1",  "**[2 / 16]** How long have you been playing EVE?"),
    ("q2",  "**[3 / 16]** How long have you been a member of **ARC Subsidized**?"),
    ("q3",  "**[4 / 16]** What is your **Timezone / Play schedule** like?"),
    ("q4",  "**[5 / 16]** Do you have a history in any other corporations in EVE?\n*(List any previous corps and briefly explain)*"),
    ("q5",  "**[6 / 16]** Have you completed the **WH Skill Plan** to enter ARC Security?\n*(Yes / No / Partially — please explain)*"),
    ("q6",  "**[7 / 16]** Have you attended any of our **organised classes and/or fleets**?\n*(Which ones? How many?)*"),
    ("q7",  "**[8 / 16]** Why do you want to join a **Wormhole corporation**?"),
    ("q8",  "**[9 / 16]** What makes you think **ARC Security** is the right Wormhole Corporation for you?"),
    ("q9",  "**[10 / 16]** Tell us what you **bring to the table** as a member of ARC Security."),
    ("q10", "**[11 / 16]** What are your **goals** personally in EVE and the Corporation?"),
    ("q11", "**[12 / 16]** Tell us about an **exciting or educational experience** you had in ARC Subsidized."),
    ("q12", "**[13 / 16]** Do you have members of ARC Security who would **support your move**?\n*(Names, or \"None that I'm aware of\")*"),
    ("q13", "**[14 / 16]** Are you willing to come to the **defense of our home system / alliance** when needed?\n*(Yes / No / Comments — real life first is understood)*"),
    ("q14", "**[15 / 16]** Are you willing to **delay personal skill goals** for corp PvP fits?\n*(Yes / No / Comments)*"),
    ("q15", "**[16 / 16]** Share a little about **yourself outside of EVE** — as much or as little as you'd like. 🙂"),
]

# ============================================================
# IN-MEMORY APPLICATION STATE
# ============================================================
# Keyed by Discord user ID.
# Each entry: {"step": int, "guild_id": int, "answers": {key: str}}
# Cleared once the ticket is created (or on error).

_pending_applications: Dict[int, Dict[str, Any]] = {}


# ============================================================
# PANEL EMBED
# ============================================================

def _build_panel_embed() -> discord.Embed:
    embed = discord.Embed(
        title="🚀 Apply to ARC Security",
        description=(
            "Ready to make the jump into wormhole space with ARC Security? "
            "Click the **Apply** button below to begin your application.\n\n"
            "The bot will **send you a DM** and ask you **16 short questions** "
            "one at a time — simply reply to each message. Once all questions are "
            "answered a private application ticket will be opened for review by "
            "**ARC Security leadership**.\n\n"
            "⚠️ Make sure your **DMs are open** for members of this server.\n"
            "⚠️ You may only have **one open application** at a time."
        ),
        color=discord.Color.orange(),
    )
    embed.set_footer(text="ARC Security — Corp Transfer Application System")
    return embed


# ============================================================
# EMBED SIZE HELPER
# ============================================================

def _embed_size(embed: discord.Embed) -> int:
    """
    Return the approximate total character count of a Discord embed.
    Discord enforces a hard 6 000-character limit across all text fields
    combined (title + description + all field names + all field values +
    footer text + author name).
    """
    total = 0
    if embed.title:
        total += len(embed.title)
    if embed.description:
        total += len(embed.description)
    for field in embed.fields:
        total += len(field.name) + len(field.value)
    if embed.footer and embed.footer.text:
        total += len(embed.footer.text)
    if embed.author and embed.author.name:
        total += len(embed.author.name)
    return total


# ============================================================
# TICKET EMBEDS (posted inside the new ticket channel)
# ============================================================
# Returns a *list* of embeds.  When all 16 answers fit inside the
# 6 000-character Discord limit they come back as a single-element list;
# when they do not, answers overflow into one or more plain continuation
# embeds so that every send() call stays under the limit.

def _build_ticket_embeds(
    creator: discord.Member,
    answers: Dict[str, str],
) -> List[discord.Embed]:
    """
    Build one or more embeds for the application ticket.

    Discord enforces a hard 6 000-character total across every text field in
    an embed.  With 16 free-text answers that limit is easily exceeded, so
    this function tracks the running size and spills overflow fields into
    plain "continuation" embeds rather than letting a single send() call
    blow past the limit.
    """
    EMBED_LIMIT = 5_900  # 100-char safety margin below Discord's 6 000 cap

    # ── First embed — header + as many answer fields as fit ──────────────────
    first = discord.Embed(
        title="📋 ARC Security Application",
        description=(
            f"Application submitted by {creator.mention}.\n\n"
            "**ARC Security Administration Council**, "
            "**ARC Security Corporation Leader**, and "
            "**ARC General** will review this application and respond "
            "as soon as possible.\n\n"
            "When the review is complete, press **Close Ticket** to close this ticket."
        ),
        color=discord.Color.orange(),
        timestamp=datetime.now(timezone.utc),
    )
    first.set_footer(
        text=f"Application opened by {creator.display_name} • {creator.id}"
    )

    embeds: List[discord.Embed] = [first]
    current = first

    # ── Application answers ──────────────────────────────────────────────────
    # The answers dict uses short keys (q0 … q15); map to display labels.
    LABELS: Dict[str, str] = {
        "char_name": "Main Character / Discord Name",
        "q1":  "1. How long have you been playing EVE?",
        "q2":  "2. How long have you been a member of ARC Subsidized?",
        "q3":  "3. What is your Timezone / Play schedule like?",
        "q4":  "4. Do you have a history in any other corporations in EVE?",
        "q5":  "5. Have you completed the Required Skill Plan(s) to enter ARC Security?",
        "q6":  "6. Have you attended any of our organised classes and/or fleets?",
        "q7":  "7. Why do you want to join a Wormhole corporation?",
        "q8":  "8. What makes you think ARC Security is the right Wormhole Corporation?",
        "q9":  "9. Tell us what you bring to the table as a member of ARC Security.",
        "q10": "10. What are your goals personally in EVE and the Corporation?",
        "q11": "11. Tell us about an exciting or educational experience in ARC Subsidized.",
        "q12": "12. Do you have members of ARC Security who would support your move?",
        "q13": "13. Are you willing to come to the defense of our home system / alliance?",
        "q14": "14. Are you willing to delay personal skill goals for corp PvP fits?",
        "q15": "15. Share a little about yourself outside of EVE.",
    }

    for key, label in LABELS.items():
        value = answers.get(key, "_No answer provided._") or "_No answer provided._"
        # Discord embed field values are capped at 1 024 characters
        if len(value) > 1020:
            value = value[:1020] + "…"

        field_cost = len(label) + len(value)

        # If adding this field would push the current embed over the limit,
        # open a fresh continuation embed and write into that instead.
        if _embed_size(current) + field_cost > EMBED_LIMIT:
            continuation = discord.Embed(
                title="📋 Application Answers (cont.)",
                color=discord.Color.orange(),
            )
            embeds.append(continuation)
            current = continuation

        current.add_field(name=label, value=value, inline=False)

    return embeds


# ============================================================
# VIEWS
# ============================================================

class TransferPanelView(discord.ui.View):
    """
    Persistent view attached to the panel message in #corp-transfer.
    Stable custom_id survives bot restarts without needing the message_id.
    """

    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Apply to ARC Security",
        style=discord.ButtonStyle.success,
        emoji="🚀",
        custom_id="corp_transfer:open",
    )
    async def open_application(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ) -> None:
        cog: Optional["CorpTransferCog"] = interaction.client.cogs.get("CorpTransferCog")  # type: ignore
        if cog is None:
            await interaction.response.send_message(
                "❌ Application system is currently unavailable.", ephemeral=True
            )
            return
        await cog._handle_open_application(interaction)


class TicketView(discord.ui.View):
    """
    Persistent view posted inside each ticket channel.
    The channel_id is baked into the custom_id so the cog can look up
    the correct ticket after a restart without extra state.
    """

    def __init__(self, channel_id: int) -> None:
        super().__init__(timeout=None)
        self.channel_id = channel_id

        close_btn = discord.ui.Button(
            label="Close Ticket",
            style=discord.ButtonStyle.danger,
            emoji="🔒",
            custom_id=f"corp_transfer:close:{channel_id}",
        )
        close_btn.callback = self._close_callback  # type: ignore
        self.add_item(close_btn)

    async def _close_callback(self, interaction: discord.Interaction) -> None:
        cog: Optional["CorpTransferCog"] = interaction.client.cogs.get("CorpTransferCog")  # type: ignore
        if cog is None:
            await interaction.response.send_message(
                "❌ Ticket system is currently unavailable.", ephemeral=True
            )
            return
        await cog._handle_close_ticket(interaction, self.channel_id)


# ============================================================
# TRANSCRIPT HELPER
# ============================================================

async def _build_transcript(
    channel: discord.TextChannel, ticket: Dict[str, Any]
) -> str:
    lines: List[str] = [
        "=" * 60,
        "  ARC SECURITY APPLICATION TRANSCRIPT",
        f"  Applicant : {ticket.get('creator_name', 'Unknown')} (ID: {ticket.get('creator_id', '?')})",
        f"  Channel   : #{channel.name}",
        f"  Opened    : {ticket.get('opened_at', 'Unknown')}",
        f"  Closed    : {_utcnow()}",
        "=" * 60,
        "",
    ]

    messages: List[discord.Message] = []
    async for msg in channel.history(limit=None, oldest_first=True):
        messages.append(msg)

    for msg in messages:
        timestamp = msg.created_at.strftime("%Y-%m-%d %H:%M:%S UTC")
        author    = f"{msg.author.display_name} ({msg.author})"
        content   = msg.content or ""

        for embed in msg.embeds:
            parts: List[str] = []
            if embed.title:
                parts.append(f"[Embed Title: {embed.title}]")
            if embed.description:
                parts.append(f"[Embed: {embed.description}]")
            for field in embed.fields:
                parts.append(f"[Field — {field.name}: {field.value}]")
            if parts:
                content = (content + "\n" + "\n".join(parts)).strip()

        for att in msg.attachments:
            content = (content + f"\n[Attachment: {att.filename} — {att.url}]").strip()

        if not content:
            content = "<no text content>"

        lines.append(f"[{timestamp}] {author}")
        lines.append(f"  {content}")
        lines.append("")

    lines.append("=" * 60)
    lines.append("  END OF TRANSCRIPT")
    lines.append("=" * 60)
    return "\n".join(lines)


# ============================================================
# COG
# ============================================================

class CorpTransferCog(commands.Cog, name="CorpTransferCog"):
    """ARC Security Corp Transfer Application — private ticket channels for applicants."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    # ----------------------------------------------------------------
    # on_ready — ensure panel + re-register persistent views
    # ----------------------------------------------------------------

    @commands.Cog.listener()
    async def on_ready(self) -> None:
        self.bot.add_view(TransferPanelView())

        data    = await _load()
        tickets = data.get("tickets", {})
        registered = 0
        stale: List[str] = []

        for channel_id_str, ticket in list(tickets.items()):
            try:
                channel_id = int(channel_id_str)
            except ValueError:
                stale.append(channel_id_str)
                continue

            channel = self.bot.get_channel(channel_id)
            if channel is None:
                stale.append(channel_id_str)
                continue

            view = TicketView(channel_id)
            self.bot.add_view(view, message_id=None)
            registered += 1

        if stale:
            for key in stale:
                tickets.pop(key, None)
            data["tickets"] = tickets
            await _save(data)
            print(f"[corp_transfer] Cleaned up {len(stale)} stale ticket record(s).")

        print(f"[corp_transfer] Re-registered {registered} open ticket view(s).")

        for guild in self.bot.guilds:
            try:
                await self._ensure_panel(guild, data)
            except Exception as e:
                print(f"[corp_transfer] Panel setup error in '{guild.name}': {e}")

        await _save(data)

    # ----------------------------------------------------------------
    # Channel / category helpers
    # ----------------------------------------------------------------

    async def _ensure_transfer_channel(
        self, guild: discord.Guild
    ) -> discord.TextChannel:
        ch = discord.utils.get(guild.text_channels, name=TRANSFER_CHANNEL_NAME)
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
                manage_messages=True,
                manage_channels=True,
            )

        ch = await guild.create_text_channel(
            TRANSFER_CHANNEL_NAME,
            overwrites=overwrites,
            reason="Corp Transfer Application System — panel channel",
        )
        print(f"[corp_transfer] Created #{TRANSFER_CHANNEL_NAME} in '{guild.name}'.")
        return ch

    async def _get_or_create_ticket_category(
        self, guild: discord.Guild
    ) -> Optional[discord.CategoryChannel]:
        category = discord.utils.get(guild.categories, name=TICKET_CATEGORY_NAME)
        if category:
            return category
        try:
            category = await guild.create_category(
                TICKET_CATEGORY_NAME,
                reason="Corp Transfer Application System — ticket category",
            )
            return category
        except discord.Forbidden:
            print(
                f"[corp_transfer] Cannot create '{TICKET_CATEGORY_NAME}' category "
                f"in '{guild.name}' — missing permissions."
            )
            return None

    # ----------------------------------------------------------------
    # Panel management
    # ----------------------------------------------------------------

    async def _ensure_panel(
        self, guild: discord.Guild, data: Dict[str, Any]
    ) -> None:
        ch = await self._ensure_transfer_channel(guild)

        panels    = data.setdefault("panels", {})
        gkey      = str(guild.id)
        panel_rec = panels.get(gkey, {})
        msg_id    = panel_rec.get("message_id")

        embed = _build_panel_embed()
        view  = TransferPanelView()

        if msg_id:
            try:
                existing = await ch.fetch_message(int(msg_id))
                await existing.edit(embed=embed, view=view)
                return
            except (discord.NotFound, discord.HTTPException):
                pass

        try:
            msg = await ch.send(embed=embed, view=view)
            panels[gkey] = {
                "channel_id": ch.id,
                "message_id": msg.id,
            }
            data["panels"] = panels
            print(f"[corp_transfer] Panel posted in '{guild.name}' #{ch.name}.")
        except discord.Forbidden:
            print(
                f"[corp_transfer] Cannot post panel in #{ch.name} "
                f"in '{guild.name}' — missing permissions."
            )

    # ----------------------------------------------------------------
    # "Apply" button handler — starts the DM interview
    # ----------------------------------------------------------------

    async def _handle_open_application(
        self, interaction: discord.Interaction
    ) -> None:
        """Called when a member clicks the Apply button."""
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message(
                "❌ This must be used inside the server.", ephemeral=True
            )
            return

        guild  = interaction.guild
        member = interaction.user

        # ── One-ticket-per-user guard ─────────────────────────────────────────
        data    = await _load()
        tickets = data.get("tickets", {})

        for ch_id_str, ticket in tickets.items():
            if (
                ticket.get("creator_id") == member.id
                and ticket.get("guild_id") == guild.id
            ):
                existing_ch = guild.get_channel(int(ch_id_str))
                if existing_ch is not None:
                    await interaction.response.send_message(
                        f"❌ You already have an open application ticket: {existing_ch.mention}\n"
                        "Please use your existing ticket or close it before starting a new one.",
                        ephemeral=True,
                    )
                    return
                # Channel gone — clean up the stale record
                tickets.pop(ch_id_str, None)
                data["tickets"] = tickets
                await _save(data)
                break

        # ── Guard: already in progress ────────────────────────────────────────
        if member.id in _pending_applications:
            await interaction.response.send_message(
                "⚠️ You already have an application in progress in your DMs! "
                "Please finish answering there, or type `cancel` to start over.",
                ephemeral=True,
            )
            return

        # ── Open a DM and send the first question ─────────────────────────────
        try:
            dm = await member.create_dm()
            await dm.send(
                "👋 **Welcome to the ARC Security Corp Transfer Application!**\n\n"
                "I'll ask you **16 questions** one at a time. Simply reply to each "
                "message here in this DM.\n"
                "Type `cancel` at any time to abort the application.\n\n"
                "─────────────────────────────\n"
                + APPLICATION_QUESTIONS[0][1]
            )
        except discord.Forbidden:
            await interaction.response.send_message(
                "❌ I couldn't send you a DM. Please **enable DMs from server members** "
                "(User Settings → Privacy & Safety) and try again.",
                ephemeral=True,
            )
            return

        # Initialise state
        _pending_applications[member.id] = {
            "step":     0,
            "guild_id": guild.id,
            "answers":  {},
        }

        await interaction.response.send_message(
            "📬 Check your DMs — I've sent you the first question!",
            ephemeral=True,
        )

    # ----------------------------------------------------------------
    # DM listener — advances the application one question at a time
    # ----------------------------------------------------------------

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        """Handle applicant replies inside DMs."""
        # Only care about DMs from real users
        if message.guild is not None:
            return
        if message.author.bot:
            return

        uid = message.author.id
        state = _pending_applications.get(uid)
        if state is None:
            return  # not an active applicant

        # ── Cancel ───────────────────────────────────────────────────────────
        if message.content.strip().lower() == "cancel":
            _pending_applications.pop(uid, None)
            await message.channel.send(
                "❌ Application cancelled. You can start over by clicking **Apply** "
                "in the server whenever you're ready."
            )
            return

        # ── Record the answer for the current step ────────────────────────────
        step    = state["step"]
        key, _  = APPLICATION_QUESTIONS[step]
        answer  = message.content.strip()

        if not answer:
            await message.channel.send("⚠️ Please send a non-empty reply.")
            return

        state["answers"][key] = answer
        next_step = step + 1
        state["step"] = next_step

        # ── More questions? ───────────────────────────────────────────────────
        if next_step < len(APPLICATION_QUESTIONS):
            _, prompt = APPLICATION_QUESTIONS[next_step]
            await message.channel.send(prompt)
            return

        # ── All questions answered — create the ticket ────────────────────────
        answers = dict(state["answers"])
        guild_id = state["guild_id"]
        _pending_applications.pop(uid, None)

        await message.channel.send(
            "✅ All questions answered! Creating your application ticket now — "
            "please wait a moment…"
        )

        guild = self.bot.get_guild(guild_id)
        if guild is None:
            await message.channel.send(
                "❌ Could not find the server. Please contact an admin."
            )
            return

        member = guild.get_member(uid)
        if member is None:
            try:
                member = await guild.fetch_member(uid)
            except discord.HTTPException:
                await message.channel.send(
                    "❌ Could not find you in the server. Please contact an admin."
                )
                return

        await self._create_ticket_from_dm(member, guild, answers, message.channel)

    # ----------------------------------------------------------------
    # Ticket creation — called after all DM questions are answered
    # ----------------------------------------------------------------

    async def _create_ticket_from_dm(
        self,
        member: discord.Member,
        guild: discord.Guild,
        answers: Dict[str, str],
        dm_channel: discord.DMChannel,
    ) -> None:
        """Create the private ticket channel and post the application inside it."""

        # ── Build permission overwrites ───────────────────────────────────────
        bot_member = guild.get_member(self.bot.user.id) if self.bot.user else None

        overwrites: Dict[Any, discord.PermissionOverwrite] = {
            guild.default_role: discord.PermissionOverwrite(
                view_channel=False,
                send_messages=False,
                read_message_history=False,
            )
        }

        # Applicant
        overwrites[member] = discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            embed_links=True,
            attach_files=True,
        )

        # Staff roles (Admin Council + Corp Leader + ARC General)
        for role in _staff_roles(guild):
            overwrites[role] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                embed_links=True,
                attach_files=True,
                manage_messages=True,
            )

        # Bot itself
        if bot_member:
            overwrites[bot_member] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                embed_links=True,
                manage_channels=True,
                manage_messages=True,
            )

        # ── Create the ticket channel ─────────────────────────────────────────
        category = await self._get_or_create_ticket_category(guild)

        safe_name = "".join(
            c if (c.isalnum() or c == "-") else "-"
            for c in member.display_name.lower()
        ).strip("-")[:20] or "member"

        channel_name = f"transfer-{safe_name}"

        try:
            ticket_channel = await guild.create_text_channel(
                channel_name,
                category=category,
                overwrites=overwrites,
                reason=f"Corp transfer application by {member} ({member.id})",
            )
        except discord.Forbidden:
            await dm_channel.send(
                "❌ I don't have permission to create ticket channels. "
                "Please contact an admin."
            )
            return
        except discord.HTTPException as e:
            await dm_channel.send(f"❌ Failed to create ticket channel: `{e}`")
            return

        # ── Post the ticket embed + Close button ──────────────────────────────
        view = TicketView(ticket_channel.id)
        try:
            self.bot.add_view(view)
        except Exception:
            pass

        ticket_embeds  = _build_ticket_embeds(member, answers)
        staff_mentions = " ".join(
            role.mention for role in _staff_roles(guild) if role
        )

        # Send the first embed with the staff pings and the Close button.
        # Any overflow continuation embeds are sent as plain follow-ups so
        # that no single message exceeds Discord's 6 000-char embed limit.
        await ticket_channel.send(
            content=staff_mentions or None,
            embed=ticket_embeds[0],
            view=view,
            allowed_mentions=discord.AllowedMentions(roles=True),
        )
        for extra_embed in ticket_embeds[1:]:
            await ticket_channel.send(embed=extra_embed)

        # ── Pull ARC-SEAT data and post as follow-up embeds ──────────────────
        await self._post_seat_data(ticket_channel, member)

        # ── Persist the ticket record ─────────────────────────────────────────
        data    = await _load()
        tickets = data.get("tickets", {})
        tickets[str(ticket_channel.id)] = {
            "creator_id":   member.id,
            "creator_name": str(member),
            "guild_id":     guild.id,
            "channel_id":   ticket_channel.id,
            "opened_at":    _utcnow(),
        }
        data["tickets"] = tickets
        await _save(data)

        await dm_channel.send(
            f"✅ Your application ticket has been created: **#{ticket_channel.name}**\n"
            "ARC Security leadership will review it and get back to you there."
        )

    # ----------------------------------------------------------------
    # Pull ARC-SEAT data into the ticket (self-contained — no cog import)
    # ----------------------------------------------------------------

    async def _post_seat_data(
        self,
        channel: discord.TextChannel,
        member: discord.Member,
    ) -> None:
        """
        Read the same data sources that arc_seat.py writes to and post
        the applicant's test/cert status and event participation as embeds.
        No cross-cog dependency — the files are read directly.
        """
        # ── Test & Certification Status ───────────────────────────────────────
        try:
            tests_embed = self._build_tests_embed(member)
            await channel.send(embed=tests_embed)
        except Exception as e:
            print(f"[corp_transfer] Failed to build tests embed: {e}")
            await channel.send(
                f"⚠️ Could not retrieve test/certification data: `{e}`"
            )

        # ── Event Participation History ───────────────────────────────────────
        try:
            events_embed = self._build_events_embed(member.id)
            await channel.send(embed=events_embed)
        except Exception as e:
            print(f"[corp_transfer] Failed to build events embed: {e}")
            await channel.send(
                f"⚠️ Could not retrieve event participation data: `{e}`"
            )

    # ----------------------------------------------------------------
    # Tests & certifications embed
    # (mirrors arc_seat.py _build_tests_embed — reads the same sources)
    # ----------------------------------------------------------------

    def _build_tests_embed(self, member: discord.Member) -> discord.Embed:
        """
        Build an embed showing the applicant's status on all three in-bot tests.

        Data sources (read-only):
        • Roles on the member  — role presence/absence is the canonical record
          of test completion, because each test removes or grants a role on pass.
        • signature_tagging_attempts.json  — attempt counts per day per user.
          Located at ./signature_tagging_attempts.json (project root, as written
          by signature_tagging_test.py).

        Tests:
        ┌──────────────────────────┬────────────────────────────┬─────────────┐
        │ Test                     │ Pass evidence              │ Fail/pending│
        ├──────────────────────────┼────────────────────────────┼─────────────┤
        │ Onboarding Test          │ "Onboarding" role ABSENT   │ role PRESENT│
        │ Corp Rules Test          │ "Newbro" role ABSENT       │ role PRESENT│
        │ Signature Tagging Test   │ "Exploration Certified"    │ role ABSENT │
        └──────────────────────────┴────────────────────────────┴─────────────┘
        """
        member_role_names = {r.name for r in member.roles}

        # ── Onboarding Test ───────────────────────────────────────────────────
        has_onboarding = "Onboarding" in member_role_names
        onboarding_status = (
            "❌ Not yet passed  (`Onboarding` role still present)"
            if has_onboarding
            else "✅ Passed  (role removed)"
        )

        # ── Corp Rules Test ───────────────────────────────────────────────────
        has_newbro = "Newbro" in member_role_names
        corp_rules_status = (
            "❌ Not yet passed  (`Newbro` role still present)"
            if has_newbro
            else "✅ Passed  (role removed)"
        )

        # ── Signature Tagging Test ────────────────────────────────────────────
        has_cert = "Exploration Certified" in member_role_names
        sig_status = (
            "✅ Passed  (`Exploration Certified` granted)"
            if has_cert
            else "❌ Not yet passed"
        )

        # ── Signature tagging attempt history from JSON ───────────────────────
        sig_attempts_total = 0
        sig_attempts_detail: List[str] = []
        attempts_path = Path("signature_tagging_attempts.json")
        try:
            if attempts_path.exists():
                raw = json.loads(attempts_path.read_text(encoding="utf-8"))
                uid_str = str(member.id)
                for day, day_data in sorted(raw.items(), reverse=True):
                    if uid_str in day_data:
                        count = int(day_data[uid_str])
                        sig_attempts_total += count
                        sig_attempts_detail.append(f"`{day}` — {count} attempt(s)")
        except Exception:
            pass  # file missing or malformed — silently skip

        if sig_attempts_total > 0:
            attempts_str = f"{sig_attempts_total} lifetime attempt(s)"
            if sig_attempts_detail:
                shown = sig_attempts_detail[:5]
                attempts_str += "\n" + "\n".join(shown)
                if len(sig_attempts_detail) > 5:
                    attempts_str += f"\n_(+ {len(sig_attempts_detail) - 5} more day(s))_"
        else:
            attempts_str = "0 — no attempts recorded"

        embed = discord.Embed(
            title="📝 Test & Certification Status",
            color=discord.Color.blurple(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(
            name="🎓 Onboarding Test",
            value=onboarding_status,
            inline=False,
        )
        embed.add_field(
            name="📋 Corp Rules Test",
            value=corp_rules_status,
            inline=False,
        )
        embed.add_field(
            name="🗺️ Signature Tagging Test",
            value=sig_status,
            inline=False,
        )
        embed.add_field(
            name="🗺️ Signature Tagging — Attempt History",
            value=attempts_str,
            inline=False,
        )
        embed.set_footer(
            text=(
                "Pass evidence is inferred from Discord roles. "
                "Onboarding/Corp Rules: role removed on pass. "
                "Sig Tagging: role granted on pass."
            )
        )
        return embed

    # ----------------------------------------------------------------
    # Event participation embed
    # (mirrors arc_seat.py _build_events_embed — reads /data/events.json)
    # ----------------------------------------------------------------

    def _build_events_embed(self, discord_id: int) -> discord.Embed:
        """
        Build an embed showing the applicant's event RSVP and attendance history.

        Data source: /data/events.json (written by event_creator.py).
        Read-only; no lock required.

        For each event the member appeared in (any RSVP or vc_qualified):
        • Shows title, scheduled time (<t:unix:d>), and their RSVP button(s).
        • Marks with ✅ if they are in vc_qualified (attended ≥ 15 min in VC).
        • Marks with 📋 if they RSVPd but did not qualify (or event is still active).
        • Sorted newest-first; capped at 25 events to stay under embed limits.
        """
        uid     = discord_id
        uid_str = str(uid)

        events_data: Dict[str, Any] = {}
        events_path = PERSIST_ROOT / "events.json"
        try:
            if events_path.exists():
                raw = events_path.read_text(encoding="utf-8").strip()
                if raw:
                    events_data = json.loads(raw)
        except Exception:
            pass

        member_events: List[Dict[str, Any]] = []

        for event_id, event in events_data.items():
            if not isinstance(event, dict):
                continue

            ts        = int(event.get("timestamp", 0) or 0)
            title     = str(event.get("title", "Untitled"))
            qualified = event.get("vc_qualified") or []
            roles     = event.get("roles") or {}

            # RSVP buttons this member clicked (excluding Decline)
            rsvp_buttons: List[str] = []
            for btn_name, btn_users in roles.items():
                if btn_name.lower() == "decline":
                    continue
                if isinstance(btn_users, list) and uid in btn_users:
                    rsvp_buttons.append(btn_name)

            is_qualified = uid in qualified

            if not rsvp_buttons and not is_qualified:
                continue

            # Cumulative VC time
            cum_times   = event.get("vc_cumulative_times") or {}
            vc_secs     = int(cum_times.get(uid_str, cum_times.get(uid, 0)) or 0)
            vc_time_str = ""
            if vc_secs > 0:
                mins = vc_secs // 60
                secs = vc_secs % 60
                vc_time_str = f" ({mins}m {secs}s in VC)"

            member_events.append({
                "ts":          ts,
                "title":       title,
                "qualified":   is_qualified,
                "rsvp":        rsvp_buttons,
                "vc_time_str": vc_time_str,
                "active":      bool(event.get("active", True)) and not bool(event.get("closed", False)),
            })

        # Sort newest-first, cap at 25
        member_events.sort(key=lambda e: e["ts"], reverse=True)
        member_events = member_events[:25]

        embed = discord.Embed(
            title="⚔️ Event Participation History",
            color=discord.Color.gold(),
            timestamp=datetime.now(timezone.utc),
        )

        if not member_events:
            embed.description = "_No event participation recorded._"
            embed.set_footer(text="Source: /data/events.json")
            return embed

        qualified_count = sum(1 for e in member_events if e["qualified"])
        rsvp_count      = len(member_events)

        embed.description = (
            f"**Events found:** {rsvp_count}  |  "
            f"**Qualified (≥15 min VC):** {qualified_count}"
        )

        # Build field lines and pack into ≤1 000-char chunks
        lines: List[str] = []
        for ev in member_events:
            ts_str   = f"<t:{ev['ts']}:d>" if ev["ts"] else "?"
            icon     = "✅" if ev["qualified"] else ("📋" if not ev["active"] else "📌")
            rsvp_str = ", ".join(ev["rsvp"]) if ev["rsvp"] else "No RSVP"
            lines.append(
                f"{icon} **{ev['title']}** — {ts_str}\n"
                f"  RSVP: {rsvp_str}{ev['vc_time_str']}"
            )

        FIELD_LIMIT = 1000
        buf       = ""
        field_idx = 0
        for line in lines:
            sep       = "\n\n" if buf else ""
            candidate = buf + sep + line
            if len(candidate) > FIELD_LIMIT:
                if buf:
                    embed.add_field(
                        name="Events" if field_idx == 0 else "Events (cont.)",
                        value=buf,
                        inline=False,
                    )
                    field_idx += 1
                buf = line
            else:
                buf = candidate
        if buf:
            embed.add_field(
                name="Events" if field_idx == 0 else "Events (cont.)",
                value=buf,
                inline=False,
            )

        embed.set_footer(
            text=(
                "✅ = Qualified (≥15 min in event VC)  "
                "📋 = RSVPd, event closed  "
                "📌 = RSVPd, event still active  "
                "| Source: /data/events.json"
            )
        )
        return embed

    # ----------------------------------------------------------------
    # Close ticket handler  (called by the in-channel Close button)
    # ----------------------------------------------------------------

    async def _handle_close_ticket(
        self,
        interaction: discord.Interaction,
        channel_id: int,
    ) -> None:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message(
                "❌ Must be used in a server.", ephemeral=True
            )
            return

        guild  = interaction.guild
        member = interaction.user

        data    = await _load()
        tickets = data.get("tickets", {})
        ticket  = tickets.get(str(channel_id))

        if ticket is None:
            if not _has_any_role(member, STAFF_ROLES):
                await interaction.response.send_message(
                    "❌ Ticket record not found.", ephemeral=True
                )
                return
        else:
            is_creator = ticket.get("creator_id") == member.id
            is_staff   = _has_any_role(member, STAFF_ROLES)
            if not (is_creator or is_staff):
                await interaction.response.send_message(
                    "❌ Only the applicant or ARC Security leadership can close this ticket.",
                    ephemeral=True,
                )
                return

        await interaction.response.send_message(
            "🔒 Closing this ticket in **5 seconds**…", ephemeral=False
        )

        await asyncio.sleep(5)

        channel = guild.get_channel(channel_id)
        if isinstance(channel, discord.TextChannel):
            # ── Build & upload transcript to #transfer-application ────────────
            try:
                transcript_text = await _build_transcript(channel, ticket or {})

                creator_name: str = (ticket or {}).get("creator_name", "unknown")
                safe_filename = "".join(
                    c if (c.isalnum() or c in "-_.") else "_"
                    for c in creator_name
                ).strip("_")[:50] or "unknown"
                filename = f"transfer_{safe_filename}.txt"

                transcript_bytes = transcript_text.encode("utf-8")
                transcript_file  = discord.File(
                    fp=io.BytesIO(transcript_bytes),
                    filename=filename,
                )

                admin_channel = discord.utils.get(
                    guild.text_channels, name=ADMIN_LOG_CHANNEL_NAME
                )
                if admin_channel is not None:
                    await admin_channel.send(
                        content=(
                            f"📋 **Corp Transfer Application transcript** — `{creator_name}` "
                            f"(closed by {member.mention})"
                        ),
                        file=transcript_file,
                    )
                else:
                    print(
                        f"[corp_transfer] Could not find #{ADMIN_LOG_CHANNEL_NAME} "
                        f"in '{guild.name}' — transcript not saved."
                    )
            except Exception as e:
                print(f"[corp_transfer] Transcript error: {e}")

            # ── Clean up persistence ──────────────────────────────────────────
            tickets.pop(str(channel_id), None)
            data["tickets"] = tickets
            await _save(data)

            # ── Delete the ticket channel ─────────────────────────────────────
            try:
                await channel.delete(
                    reason=f"Corp transfer application closed by {member} ({member.id})"
                )
            except discord.Forbidden:
                pass
            except discord.HTTPException as e:
                print(f"[corp_transfer] Channel delete error: {e}")
        else:
            tickets.pop(str(channel_id), None)
            data["tickets"] = tickets
            await _save(data)

    # ----------------------------------------------------------------
    # /transfer_close — manual staff-only ticket closer
    # ----------------------------------------------------------------

    @app_commands.command(
        name="transfer_close",
        description="Manually close a corp-transfer ticket channel (staff only).",
    )
    @app_commands.describe(
        channel=(
            "The ticket channel to close — defaults to the current channel if omitted."
        )
    )
    async def transfer_close(
        self,
        interaction: discord.Interaction,
        channel: Optional[discord.TextChannel] = None,
    ) -> None:
        """
        Force-closes a transfer ticket regardless of whether a persistence
        record exists.  Designed to recover half-opened tickets (e.g. the
        channel was created but the embed send crashed before the record
        was written) as well as normally-opened ones.

        Steps
        -----
        1. Resolve the target channel (argument or current channel).
        2. Staff-role gate.
        3. Try to save a transcript to #transfer-application (soft-fail).
        4. Erase the persistence record if one exists.
        5. Clear any stuck in-flight DM application state for the creator.
        6. Delete the channel.
        """
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message(
                "❌ Must be used in a server.", ephemeral=True
            )
            return

        if not _has_any_role(interaction.user, STAFF_ROLES):
            await interaction.response.send_message(
                "❌ Only ARC Security leadership can use this command.",
                ephemeral=True,
            )
            return

        guild  = interaction.guild
        closer = interaction.user

        # ── Resolve target channel ────────────────────────────────────────────
        target: Optional[discord.TextChannel] = channel
        if target is None:
            if isinstance(interaction.channel, discord.TextChannel):
                target = interaction.channel
            else:
                await interaction.response.send_message(
                    "❌ Run this command inside a ticket channel, or pass the "
                    "`channel` argument.",
                    ephemeral=True,
                )
                return

        await interaction.response.defer(ephemeral=True)

        # ── Look up persistence record (may not exist for half-opened tickets) ─
        data    = await _load()
        tickets = data.get("tickets", {})
        ticket  = tickets.get(str(target.id))  # may be None

        creator_name: str = (ticket or {}).get("creator_name", target.name)
        creator_id: Optional[int] = (ticket or {}).get("creator_id")

        # ── Attempt transcript ────────────────────────────────────────────────
        transcript_saved = False
        try:
            transcript_text  = await _build_transcript(target, ticket or {})
            safe_filename    = "".join(
                c if (c.isalnum() or c in "-_.") else "_" for c in creator_name
            ).strip("_")[:50] or "unknown"
            transcript_file  = discord.File(
                fp=io.BytesIO(transcript_text.encode("utf-8")),
                filename=f"transfer_{safe_filename}.txt",
            )
            admin_channel = discord.utils.get(
                guild.text_channels, name=ADMIN_LOG_CHANNEL_NAME
            )
            if admin_channel is not None:
                note = " *(half-opened — no ticket record)*" if ticket is None else ""
                await admin_channel.send(
                    content=(
                        f"📋 **Corp Transfer Application transcript** — `{creator_name}`"
                        f"{note} (force-closed by {closer.mention})"
                    ),
                    file=transcript_file,
                )
                transcript_saved = True
            else:
                print(
                    f"[corp_transfer] /transfer_close: #{ADMIN_LOG_CHANNEL_NAME} "
                    f"not found in '{guild.name}' — transcript skipped."
                )
        except Exception as e:
            print(f"[corp_transfer] /transfer_close transcript error: {e}")

        # ── Clear persistence record ──────────────────────────────────────────
        had_record = str(target.id) in tickets
        tickets.pop(str(target.id), None)
        data["tickets"] = tickets
        await _save(data)

        # ── Clear any in-flight DM application state for the creator ──────────
        cleared_pending = False
        if creator_id is not None and creator_id in _pending_applications:
            _pending_applications.pop(creator_id, None)
            cleared_pending = True

        # ── Delete the channel ────────────────────────────────────────────────
        deleted = False
        try:
            await target.delete(
                reason=(
                    f"Corp transfer ticket force-closed by "
                    f"{closer} ({closer.id}) via /transfer_close"
                )
            )
            deleted = True
        except discord.Forbidden:
            await interaction.followup.send(
                "⚠️ Persistence record cleared but I lack permission to delete "
                f"{target.mention} — please delete it manually.",
                ephemeral=True,
            )
            return
        except discord.HTTPException as e:
            await interaction.followup.send(
                f"⚠️ Persistence record cleared but channel deletion failed: `{e}`",
                ephemeral=True,
            )
            return

        # ── Summary ───────────────────────────────────────────────────────────
        parts: List[str] = [f"✅ **#{target.name}** has been closed."]
        parts.append(
            f"📄 Transcript: {'saved to ' + f'`#{ADMIN_LOG_CHANNEL_NAME}`' if transcript_saved else '⚠️ could not be saved (channel may have been empty or unreachable)'}"
        )
        parts.append(
            f"🗂️ Persistence record: {'removed' if had_record else 'none found (half-opened ticket)'}"
        )
        if cleared_pending:
            parts.append("🧹 In-flight DM application state cleared for the applicant.")

        await interaction.followup.send("\n".join(parts), ephemeral=True)

    # ----------------------------------------------------------------
    # /transfer_setup — manual panel refresh (leadership only)
    # ----------------------------------------------------------------

    @app_commands.command(
        name="transfer_setup",
        description="Post or refresh the ARC Security application panel (leadership only).",
    )
    async def transfer_setup(self, interaction: discord.Interaction) -> None:
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

        data   = await _load()
        panels = data.setdefault("panels", {})
        panels.pop(str(interaction.guild.id), None)
        data["panels"] = panels

        await self._ensure_panel(interaction.guild, data)
        await _save(data)

        await interaction.followup.send(
            f"✅ Corp Transfer panel refreshed in `#{TRANSFER_CHANNEL_NAME}`.",
            ephemeral=True,
        )


# ============================================================
# SETUP
# ============================================================

async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(CorpTransferCog(bot))