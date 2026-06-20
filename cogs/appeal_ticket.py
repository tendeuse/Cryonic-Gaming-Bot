# cogs/appeal_ticket.py
#
# Appeal Ticket System
# ====================
# - Posts a persistent "Appeal" button panel in #appeal
# - Clicking "Appeal" creates a private ticket channel visible only to:
#     • The ticket creator
#     • ARC Security Administration Council
#     • ARC Security Corporation Leader
# - Each user may only have one open ticket at a time
# - Tickets have a Close button (creator or leadership only)
# - Panel survives restarts (stored message ID; no duplicate on reconnect)
# - All data persists to /data/appeal_tickets.json

import asyncio
import io
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import discord
from discord.ext import commands
from discord import app_commands

from . import db

# ============================================================
# CONFIG
# ============================================================

APPEAL_CHANNEL_NAME  = "appeal"
TICKET_CATEGORY_NAME = "Tickets"
ADMIN_LOG_CHANNEL_NAME = "administration"

# Roles with access to every ticket
STAFF_ROLES: tuple[str, ...] = (
    "ARC Security Administration Council",
    "ARC Security Corporation Leader",
)

# Roles allowed to run /appeal_setup
SETUP_ROLES: tuple[str, ...] = (
    "ARC Security Administration Council",
    "ARC Security Corporation Leader",
)

# ============================================================
# PERSISTENCE
# ============================================================

PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)
DATA_FILE = PERSIST_ROOT / "appeal_tickets.json"

_file_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _file_lock
    if _file_lock is None:
        _file_lock = asyncio.Lock()
    return _file_lock


def _atomic_write(data: Dict[str, Any]) -> None:
    db.kv_save("appeal_tickets", data)


async def _load() -> Dict[str, Any]:
    async with _get_lock():
        try:
            data = db.kv_load("appeal_tickets", {"panels": {}, "tickets": {}})
            data.setdefault("panels", {})
            data.setdefault("tickets", {})
            return data
        except Exception as e:
            print(f"[appeal_ticket] Data load error: {e} — using defaults")
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
# PANEL EMBED
# ============================================================

def _build_panel_embed() -> discord.Embed:
    embed = discord.Embed(
        title="📬 Submit an Appeal",
        description=(
            "If you wish to appeal a decision, submit a ban appeal, or raise a concern "
            "with ARC Security leadership, click the **Appeal** button below.\n\n"
            "A private ticket will be created that is visible only to you and "
            "**ARC Security leadership**.\n\n"
            "Please be clear and concise in your appeal."
        ),
        color=discord.Color.blurple(),
    )
    embed.set_footer(text="ARC Security — Appeal System")
    return embed


# ============================================================
# TICKET EMBED (posted inside the new ticket channel)
# ============================================================

def _build_ticket_embed(creator: discord.Member) -> discord.Embed:
    embed = discord.Embed(
        title="🎫 Appeal Ticket Opened",
        description=(
            f"Welcome {creator.mention}.\n\n"
            "Please describe your appeal or concern in detail below. "
            "**ARC Security Administration Council** and "
            "**ARC Security Corporation Leader** will review your ticket "
            "and respond as soon as possible.\n\n"
            "When the matter is resolved, press **Close Ticket** to close this ticket."
        ),
        color=discord.Color.green(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_footer(text=f"Ticket opened by {creator.display_name} • {creator.id}")
    return embed


# ============================================================
# VIEWS
# ============================================================

class AppealPanelView(discord.ui.View):
    """
    Persistent view attached to the panel message in #appeal.
    Uses a stable custom_id so it survives bot restarts without needing
    the message_id for re-registration.
    """

    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Appeal",
        style=discord.ButtonStyle.primary,
        emoji="📬",
        custom_id="appeal_ticket:open",
    )
    async def open_appeal(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ) -> None:
        cog: Optional["AppealTicketCog"] = interaction.client.cogs.get("AppealTicketCog")  # type: ignore
        if cog is None:
            await interaction.response.send_message(
                "❌ Ticket system is currently unavailable.", ephemeral=True
            )
            return
        await cog._handle_open_ticket(interaction)


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
            custom_id=f"appeal_ticket:close:{channel_id}",
        )
        close_btn.callback = self._close_callback  # type: ignore
        self.add_item(close_btn)

    async def _close_callback(self, interaction: discord.Interaction) -> None:
        cog: Optional["AppealTicketCog"] = interaction.client.cogs.get("AppealTicketCog")  # type: ignore
        if cog is None:
            await interaction.response.send_message(
                "❌ Ticket system is currently unavailable.", ephemeral=True
            )
            return
        await cog._handle_close_ticket(interaction, self.channel_id)


# ============================================================
# TRANSCRIPT HELPER
# ============================================================

async def _build_transcript(channel: discord.TextChannel, ticket: Dict[str, Any]) -> str:
    """
    Scrape all messages in *channel* and return a formatted plain-text transcript.
    Messages are returned oldest-first.
    """
    lines: List[str] = [
        "=" * 60,
        f"  APPEAL TICKET TRANSCRIPT",
        f"  Player  : {ticket.get('creator_name', 'Unknown')} (ID: {ticket.get('creator_id', '?')})",
        f"  Channel : #{channel.name}",
        f"  Opened  : {ticket.get('opened_at', 'Unknown')}",
        f"  Closed  : {_utcnow()}",
        "=" * 60,
        "",
    ]

    messages: List[discord.Message] = []
    async for msg in channel.history(limit=None, oldest_first=True):
        messages.append(msg)

    for msg in messages:
        timestamp = msg.created_at.strftime("%Y-%m-%d %H:%M:%S UTC")
        author = f"{msg.author.display_name} ({msg.author})"

        # Message content
        content = msg.content or ""

        # Embeds — summarise inline
        for embed in msg.embeds:
            parts = []
            if embed.title:
                parts.append(f"[Embed Title: {embed.title}]")
            if embed.description:
                parts.append(f"[Embed: {embed.description}]")
            if parts:
                content = (content + "\n" + "\n".join(parts)).strip()

        # Attachments
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

class AppealTicketCog(commands.Cog, name="AppealTicketCog"):
    """Appeal Ticket System — private ticket channels for member appeals."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    # ----------------------------------------------------------------
    # on_ready — ensure panel + re-register persistent views
    # ----------------------------------------------------------------

    @commands.Cog.listener()
    async def on_ready(self) -> None:
        # Register the panel view globally (stable custom_id — no message_id needed)
        self.bot.add_view(AppealPanelView())

        data = await _load()

        # Re-register TicketView for every open ticket that still exists
        tickets = data.get("tickets", {})
        registered = 0
        stale: List[str] = []

        for channel_id_str, ticket in list(tickets.items()):
            try:
                channel_id = int(channel_id_str)
            except ValueError:
                stale.append(channel_id_str)
                continue

            # Check if the channel still exists in any guild
            channel = self.bot.get_channel(channel_id)
            if channel is None:
                stale.append(channel_id_str)
                continue

            view = TicketView(channel_id)
            self.bot.add_view(view, message_id=None)
            registered += 1

        # Clean up stale ticket records (channels that were manually deleted)
        if stale:
            for key in stale:
                tickets.pop(key, None)
            data["tickets"] = tickets
            await _save(data)
            print(f"[appeal_ticket] Cleaned up {len(stale)} stale ticket record(s).")

        print(f"[appeal_ticket] Re-registered {registered} open ticket view(s).")

        # Ensure the panel is posted in every guild
        for guild in self.bot.guilds:
            try:
                await self._ensure_panel(guild, data)
            except Exception as e:
                print(f"[appeal_ticket] Panel setup error in '{guild.name}': {e}")

        await _save(data)

    # ----------------------------------------------------------------
    # Channel / category helpers
    # ----------------------------------------------------------------

    async def _ensure_appeal_channel(
        self, guild: discord.Guild
    ) -> discord.TextChannel:
        """Find or create the #appeal channel."""
        ch = discord.utils.get(guild.text_channels, name=APPEAL_CHANNEL_NAME)
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
            APPEAL_CHANNEL_NAME,
            overwrites=overwrites,
            reason="Appeal Ticket System — panel channel",
        )
        print(f"[appeal_ticket] Created #{APPEAL_CHANNEL_NAME} in '{guild.name}'.")
        return ch

    async def _get_or_create_ticket_category(
        self, guild: discord.Guild
    ) -> Optional[discord.CategoryChannel]:
        """Find or create a 'Tickets' category for ticket channels."""
        category = discord.utils.get(guild.categories, name=TICKET_CATEGORY_NAME)
        if category:
            return category
        try:
            category = await guild.create_category(
                TICKET_CATEGORY_NAME,
                reason="Appeal Ticket System — ticket category",
            )
            return category
        except discord.Forbidden:
            print(
                f"[appeal_ticket] Cannot create '{TICKET_CATEGORY_NAME}' category "
                f"in '{guild.name}' — missing permissions."
            )
            return None

    # ----------------------------------------------------------------
    # Panel management
    # ----------------------------------------------------------------

    async def _ensure_panel(
        self, guild: discord.Guild, data: Dict[str, Any]
    ) -> None:
        """
        Post or refresh the persistent panel embed in #appeal.
        Avoids re-posting if the panel message already exists.
        """
        ch = await self._ensure_appeal_channel(guild)

        panels    = data.setdefault("panels", {})
        gkey      = str(guild.id)
        panel_rec = panels.get(gkey, {})
        msg_id    = panel_rec.get("message_id")

        embed = _build_panel_embed()
        view  = AppealPanelView()

        # Try to edit the existing panel message
        if msg_id:
            try:
                existing = await ch.fetch_message(int(msg_id))
                await existing.edit(embed=embed, view=view)
                return
            except (discord.NotFound, discord.HTTPException):
                # Message was deleted — fall through to post a fresh one
                pass

        # Post a new panel
        try:
            msg = await ch.send(embed=embed, view=view)
            panels[gkey] = {
                "channel_id": ch.id,
                "message_id": msg.id,
            }
            data["panels"] = panels
            print(f"[appeal_ticket] Panel posted in '{guild.name}' #{ch.name}.")
        except discord.Forbidden:
            print(
                f"[appeal_ticket] Cannot post panel in #{ch.name} "
                f"in '{guild.name}' — missing permissions."
            )

    # ----------------------------------------------------------------
    # Open ticket handler
    # ----------------------------------------------------------------

    async def _handle_open_ticket(self, interaction: discord.Interaction) -> None:
        """Called when a member clicks the Appeal button."""
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message(
                "❌ This must be used inside the server.", ephemeral=True
            )
            return

        guild  = interaction.guild
        member = interaction.user

        await interaction.response.defer(ephemeral=True)

        data    = await _load()
        tickets = data.get("tickets", {})

        # ── One-ticket-per-user guard ─────────────────────────────────────────
        for ch_id_str, ticket in tickets.items():
            if ticket.get("creator_id") == member.id and ticket.get("guild_id") == guild.id:
                # Verify the channel still exists
                existing_ch = guild.get_channel(int(ch_id_str))
                if existing_ch is not None:
                    await interaction.followup.send(
                        f"❌ You already have an open ticket: {existing_ch.mention}\n"
                        "Please use your existing ticket or close it before opening a new one.",
                        ephemeral=True,
                    )
                    return
                # Channel gone — clean up the stale record
                tickets.pop(ch_id_str, None)
                data["tickets"] = tickets
                await _save(data)
                break

        # ── Build permission overwrites ────────────────────────────────────────
        bot_member = guild.get_member(self.bot.user.id) if self.bot.user else None

        # Start with deny-all for @everyone
        overwrites: Dict[Any, discord.PermissionOverwrite] = {
            guild.default_role: discord.PermissionOverwrite(
                view_channel=False,
                send_messages=False,
                read_message_history=False,
            )
        }

        # Ticket creator
        overwrites[member] = discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            embed_links=True,
            attach_files=True,
        )

        # Staff roles
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

        # Sanitise display name for channel name (Discord only allows a-z, 0-9, hyphens)
        safe_name = "".join(
            c if (c.isalnum() or c == "-") else "-"
            for c in member.display_name.lower()
        ).strip("-")[:20] or "member"

        channel_name = f"appeal-{safe_name}"

        try:
            ticket_channel = await guild.create_text_channel(
                channel_name,
                category=category,
                overwrites=overwrites,
                reason=f"Appeal ticket opened by {member} ({member.id})",
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

        # ── Post the ticket embed + Close button inside the new channel ────────
        view = TicketView(ticket_channel.id)
        try:
            self.bot.add_view(view)
        except Exception:
            pass

        ticket_embed = _build_ticket_embed(member)
        staff_mentions = " ".join(
            role.mention for role in _staff_roles(guild) if role
        )

        await ticket_channel.send(
            content=staff_mentions or None,
            embed=ticket_embed,
            view=view,
            allowed_mentions=discord.AllowedMentions(roles=True),
        )

        # ── Persist the ticket record ─────────────────────────────────────────
        tickets[str(ticket_channel.id)] = {
            "creator_id": member.id,
            "creator_name": str(member),
            "guild_id": guild.id,
            "channel_id": ticket_channel.id,
            "opened_at": _utcnow(),
        }
        data["tickets"] = tickets
        await _save(data)

        await interaction.followup.send(
            f"✅ Your appeal ticket has been created: {ticket_channel.mention}",
            ephemeral=True,
        )

    # ----------------------------------------------------------------
    # Close ticket handler
    # ----------------------------------------------------------------

    async def _handle_close_ticket(
        self,
        interaction: discord.Interaction,
        channel_id: int,
    ) -> None:
        """Called when the Close Ticket button is pressed."""
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
            # Ticket record missing — allow staff to still delete the channel
            if not _has_any_role(member, STAFF_ROLES):
                await interaction.response.send_message(
                    "❌ Ticket record not found.", ephemeral=True
                )
                return
        else:
            # Permission check: creator or staff
            is_creator = (ticket.get("creator_id") == member.id)
            is_staff   = _has_any_role(member, STAFF_ROLES)
            if not (is_creator or is_staff):
                await interaction.response.send_message(
                    "❌ Only the ticket creator or ARC Security leadership can close this ticket.",
                    ephemeral=True,
                )
                return

        await interaction.response.send_message(
            "🔒 Closing this ticket in **5 seconds**…", ephemeral=False
        )

        await asyncio.sleep(5)

        channel = guild.get_channel(channel_id)
        if isinstance(channel, discord.TextChannel):
            # ── Build & upload transcript to #administration ───────────────────
            try:
                transcript_text = await _build_transcript(channel, ticket or {})

                # Determine a safe filename from the creator's name
                creator_name: str = (ticket or {}).get("creator_name", "unknown")
                safe_filename = "".join(
                    c if (c.isalnum() or c in "-_.") else "_"
                    for c in creator_name
                ).strip("_")[:50] or "unknown"
                filename = f"{safe_filename}.txt"

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
                            f"📋 **Appeal transcript** — `{creator_name}` "
                            f"(closed by {member.mention})"
                        ),
                        file=transcript_file,
                    )
                else:
                    print(
                        f"[appeal_ticket] Could not find #{ADMIN_LOG_CHANNEL_NAME} "
                        f"in '{guild.name}' — transcript not saved."
                    )
            except Exception as e:
                print(f"[appeal_ticket] Transcript error: {e}")

            # ── Clean up persistence ───────────────────────────────────────────
            tickets.pop(str(channel_id), None)
            data["tickets"] = tickets
            await _save(data)

            # ── Delete the ticket channel ──────────────────────────────────────
            try:
                await channel.delete(
                    reason=f"Appeal ticket closed by {member} ({member.id})"
                )
            except discord.Forbidden:
                # Channel already gone or permission issue — nothing to do
                pass
            except discord.HTTPException as e:
                print(f"[appeal_ticket] Channel delete error: {e}")
        else:
            # Channel already gone — still clean up the record
            tickets.pop(str(channel_id), None)
            data["tickets"] = tickets
            await _save(data)

    # ----------------------------------------------------------------
    # /appeal_setup — manual panel refresh
    # ----------------------------------------------------------------

    @app_commands.command(
        name="appeal_setup",
        description="Post or refresh the Appeal ticket panel in #appeal (leadership only).",
    )
    async def appeal_setup(self, interaction: discord.Interaction) -> None:
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
        # Force re-post by clearing the stored message ID for this guild
        panels = data.setdefault("panels", {})
        panels.pop(str(interaction.guild.id), None)
        data["panels"] = panels

        await self._ensure_panel(interaction.guild, data)
        await _save(data)

        await interaction.followup.send(
            f"✅ Appeal panel refreshed in `#{APPEAL_CHANNEL_NAME}`.", ephemeral=True
        )


# ============================================================
# SETUP
# ============================================================

async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(AppealTicketCog(bot))