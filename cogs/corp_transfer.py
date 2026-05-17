# cogs/corp_transfer.py
#
# ARC Security Corp Transfer Application Ticket System
# =====================================================
# - Posts a persistent "Apply to ARC Security" button panel in #corp-transfer
# - Clicking "Apply" walks the applicant through 4 modal dialogs (Discord
#   allows a maximum of 5 fields per modal, so 16 fields span 4 modals)
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
# IN-MEMORY APPLICATION STATE
# ============================================================
# Stores partial modal answers while the user works through all 4 modals.
# Keyed by Discord user ID.  Cleared once the ticket is created (or on error).

_pending_applications: Dict[int, Dict[str, str]] = {}


# ============================================================
# PANEL EMBED
# ============================================================

def _build_panel_embed() -> discord.Embed:
    embed = discord.Embed(
        title="🚀 Apply to ARC Security",
        description=(
            "Ready to make the jump into wormhole space with ARC Security? "
            "Click the **Apply** button below to begin your application.\n\n"
            "A short series of questions will appear — please answer each one "
            "honestly and thoughtfully. Once submitted, a private application "
            "ticket will be opened for review by **ARC Security leadership**.\n\n"
            "⚠️ You may only have **one open application** at a time."
        ),
        color=discord.Color.orange(),
    )
    embed.set_footer(text="ARC Security — Corp Transfer Application System")
    return embed


# ============================================================
# TICKET EMBED (posted inside the new ticket channel)
# ============================================================

def _build_ticket_embed(
    creator: discord.Member,
    answers: Dict[str, str],
) -> discord.Embed:
    embed = discord.Embed(
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
    embed.set_footer(
        text=f"Application opened by {creator.display_name} • {creator.id}"
    )

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
        embed.add_field(name=label, value=value, inline=False)

    return embed


# ============================================================
# MODALS  (4 modals × ≤5 fields each = 16 fields total)
# ============================================================

class ApplicationModal1(discord.ui.Modal, title="ARC Security Application — Part 1 of 4"):
    """Character name + Questions 1–4."""

    char_name = discord.ui.TextInput(
        label="Main Character / Discord Name",
        placeholder="Your EVE main character name and/or Discord username",
        style=discord.TextStyle.short,
        max_length=200,
        required=True,
    )
    q1 = discord.ui.TextInput(
        label="1. How long have you been playing EVE?",
        placeholder="e.g. 3 years, since 2019 …",
        style=discord.TextStyle.short,
        max_length=300,
        required=True,
    )
    q2 = discord.ui.TextInput(
        label="2. How long in ARC Subsidized?",
        placeholder="e.g. 6 months",
        style=discord.TextStyle.short,
        max_length=300,
        required=True,
    )
    q3 = discord.ui.TextInput(
        label="3. Timezone / Play schedule?",
        placeholder="e.g. EU TZ, evenings weekdays + weekends",
        style=discord.TextStyle.short,
        max_length=300,
        required=True,
    )
    q4 = discord.ui.TextInput(
        label="4. History in other EVE corporations?",
        placeholder="List any previous corps and briefly explain",
        style=discord.TextStyle.paragraph,
        max_length=1000,
        required=True,
    )

    def __init__(self, cog: "CorpTransferCog") -> None:
        super().__init__()
        self._cog = cog

    async def on_submit(self, interaction: discord.Interaction) -> None:
        uid = interaction.user.id
        _pending_applications[uid] = {
            "char_name": self.char_name.value,
            "q1": self.q1.value,
            "q2": self.q2.value,
            "q3": self.q3.value,
            "q4": self.q4.value,
        }
        # Immediately open the next modal
        await interaction.response.send_modal(ApplicationModal2(self._cog))

    async def on_error(
        self, interaction: discord.Interaction, error: Exception
    ) -> None:
        print(f"[corp_transfer] Modal1 error: {error}")
        await interaction.response.send_message(
            "❌ An error occurred processing Part 1. Please try again.", ephemeral=True
        )


class ApplicationModal2(discord.ui.Modal, title="ARC Security Application — Part 2 of 4"):
    """Questions 5–9."""

    q5 = discord.ui.TextInput(
        label="5. Completed the Required Skill Plan(s)?",
        placeholder="Yes / No / Partially — please explain",
        style=discord.TextStyle.short,
        max_length=500,
        required=True,
    )
    q6 = discord.ui.TextInput(
        label="6. Attended organised classes / fleets?",
        placeholder="Which ones? How many?",
        style=discord.TextStyle.paragraph,
        max_length=500,
        required=True,
    )
    q7 = discord.ui.TextInput(
        label="7. Why do you want to join a Wormhole corp?",
        style=discord.TextStyle.paragraph,
        max_length=800,
        required=True,
    )
    q8 = discord.ui.TextInput(
        label="8. Why is ARC Security the right Wormhole corp?",
        style=discord.TextStyle.paragraph,
        max_length=800,
        required=True,
    )
    q9 = discord.ui.TextInput(
        label="9. What do you bring to ARC Security?",
        style=discord.TextStyle.paragraph,
        max_length=800,
        required=True,
    )

    def __init__(self, cog: "CorpTransferCog") -> None:
        super().__init__()
        self._cog = cog

    async def on_submit(self, interaction: discord.Interaction) -> None:
        uid = interaction.user.id
        existing = _pending_applications.setdefault(uid, {})
        existing.update({
            "q5": self.q5.value,
            "q6": self.q6.value,
            "q7": self.q7.value,
            "q8": self.q8.value,
            "q9": self.q9.value,
        })
        await interaction.response.send_modal(ApplicationModal3(self._cog))

    async def on_error(
        self, interaction: discord.Interaction, error: Exception
    ) -> None:
        print(f"[corp_transfer] Modal2 error: {error}")
        await interaction.response.send_message(
            "❌ An error occurred processing Part 2. Please try again.", ephemeral=True
        )


class ApplicationModal3(discord.ui.Modal, title="ARC Security Application — Part 3 of 4"):
    """Questions 10–14."""

    q10 = discord.ui.TextInput(
        label="10. Goals in EVE and the Corporation?",
        style=discord.TextStyle.paragraph,
        max_length=800,
        required=True,
    )
    q11 = discord.ui.TextInput(
        label="11. Exciting / educational ARC Subsidized experience?",
        style=discord.TextStyle.paragraph,
        max_length=800,
        required=True,
    )
    q12 = discord.ui.TextInput(
        label="12. ARC Security members who'd support your move?",
        placeholder="Names, or 'None that I'm aware of'",
        style=discord.TextStyle.paragraph,
        max_length=500,
        required=True,
    )
    q13 = discord.ui.TextInput(
        label="13. Willing to defend home / alliance as needed?",
        placeholder="Yes / No / Comments (real life first is understood)",
        style=discord.TextStyle.short,
        max_length=300,
        required=True,
    )
    q14 = discord.ui.TextInput(
        label="14. Willing to delay personal skills for corp PvP fits?",
        placeholder="Yes / No / Comments",
        style=discord.TextStyle.short,
        max_length=300,
        required=True,
    )

    def __init__(self, cog: "CorpTransferCog") -> None:
        super().__init__()
        self._cog = cog

    async def on_submit(self, interaction: discord.Interaction) -> None:
        uid = interaction.user.id
        existing = _pending_applications.setdefault(uid, {})
        existing.update({
            "q10": self.q10.value,
            "q11": self.q11.value,
            "q12": self.q12.value,
            "q13": self.q13.value,
            "q14": self.q14.value,
        })
        await interaction.response.send_modal(ApplicationModal4(self._cog))

    async def on_error(
        self, interaction: discord.Interaction, error: Exception
    ) -> None:
        print(f"[corp_transfer] Modal3 error: {error}")
        await interaction.response.send_message(
            "❌ An error occurred processing Part 3. Please try again.", ephemeral=True
        )


class ApplicationModal4(discord.ui.Modal, title="ARC Security Application — Part 4 of 4"):
    """Question 15 — final modal, triggers ticket creation."""

    q15 = discord.ui.TextInput(
        label="15. Tell us about yourself outside of EVE.",
        placeholder="Share as much or as little as you'd like us to know.",
        style=discord.TextStyle.paragraph,
        max_length=1000,
        required=True,
    )

    def __init__(self, cog: "CorpTransferCog") -> None:
        super().__init__()
        self._cog = cog

    async def on_submit(self, interaction: discord.Interaction) -> None:
        uid = interaction.user.id
        existing = _pending_applications.setdefault(uid, {})
        existing["q15"] = self.q15.value

        # All answers collected — hand off to the cog to create the ticket
        answers = dict(existing)
        _pending_applications.pop(uid, None)

        await interaction.response.defer(ephemeral=True)
        await self._cog._create_ticket(interaction, answers)

    async def on_error(
        self, interaction: discord.Interaction, error: Exception
    ) -> None:
        print(f"[corp_transfer] Modal4 error: {error}")
        await interaction.response.send_message(
            "❌ An error occurred processing Part 4. Please try again.", ephemeral=True
        )


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
    # "Apply" button handler — opens Modal 1
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

        # Open the first modal (the interaction response must be a modal)
        await interaction.response.send_modal(ApplicationModal1(self))

    # ----------------------------------------------------------------
    # Ticket creation — called after all 4 modals are submitted
    # ----------------------------------------------------------------

    async def _create_ticket(
        self,
        interaction: discord.Interaction,
        answers: Dict[str, str],
    ) -> None:
        """Create the private ticket channel and post the application inside it."""
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.followup.send(
                "❌ This must be used inside the server.", ephemeral=True
            )
            return

        guild  = interaction.guild
        member = interaction.user

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
            await interaction.followup.send(
                "❌ I don't have permission to create ticket channels. "
                "Please contact an admin.",
                ephemeral=True,
            )
            return
        except discord.HTTPException as e:
            await interaction.followup.send(
                f"❌ Failed to create ticket channel: `{e}`",
                ephemeral=True,
            )
            return

        # ── Post the ticket embed + Close button ──────────────────────────────
        view = TicketView(ticket_channel.id)
        try:
            self.bot.add_view(view)
        except Exception:
            pass

        ticket_embed   = _build_ticket_embed(member, answers)
        staff_mentions = " ".join(
            role.mention for role in _staff_roles(guild) if role
        )

        await ticket_channel.send(
            content=staff_mentions or None,
            embed=ticket_embed,
            view=view,
            allowed_mentions=discord.AllowedMentions(roles=True),
        )

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

        await interaction.followup.send(
            f"✅ Your application ticket has been created: {ticket_channel.mention}\n"
            "Leadership will review it and get back to you there.",
            ephemeral=True,
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
    # Close ticket handler
    # ----------------------------------------------------------------

    # ----------------------------------------------------------------
    # Close ticket handler
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