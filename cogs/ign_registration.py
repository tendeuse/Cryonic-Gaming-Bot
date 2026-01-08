import os
import discord
from discord.ext import commands
from discord import app_commands
from discord.ui import View, Button, Modal, TextInput
from pathlib import Path
import json
import datetime
from typing import Dict, Any, List, Optional

# =====================
# CONFIG
# =====================

# Railway volume path:
# - Default to /data (Railway Volume mount)
# - Allow override via env var if you ever change mount point
PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)

DATA_FILE = PERSIST_ROOT / "ign_registry.json"

# Panel channel where members click Register
REQUEST_PANEL_CHANNEL_NAME = "request-to-access-locations"

# Staff processing channel where requests are posted
ACCESS_CHANNEL_NAME = "location_access"

ARC_SECURITY_ROLE = "ARC Security"

ALLOWED_ROLES = {
    "ARC Security Corporation Leader",
    "ARC Security Administration Council",
    "ARC General",
    "ARC Commander",
    "ARC Officer",
}

MAX_IGNS_PER_USER = 10

PANEL_EMBED_TEXT = "Please register all the alts that you want to be provided with the Wormhole access"
PANEL_BUTTON_LABEL = "Register"

# =====================
# STORAGE
# =====================

def utcnow_iso() -> str:
    return datetime.datetime.utcnow().isoformat()

def load_state() -> Dict[str, Any]:
    if not DATA_FILE.exists():
        return {"users": {}, "requests": {}, "leave_warnings": {}, "panels": {}}
    try:
        s = json.loads(DATA_FILE.read_text(encoding="utf-8"))
        s.setdefault("users", {})
        s.setdefault("requests", {})
        s.setdefault("leave_warnings", {})
        s.setdefault("panels", {})  # per-guild panel message tracking
        return s
    except Exception:
        return {"users": {}, "requests": {}, "leave_warnings": {}, "panels": {}}

def save_state(state: Dict[str, Any]) -> None:
    # Ensure volume directory exists
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)

    # Atomic write to reduce chance of corruption on restart/deploy
    tmp = DATA_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=4), encoding="utf-8")
    tmp.replace(DATA_FILE)

def normalize_ign(s: str) -> str:
    return " ".join((s or "").strip().split())

def split_igns(raw: str) -> List[str]:
    if not raw:
        return []
    parts: List[str] = []
    for chunk in raw.replace("\n", ",").split(","):
        ign = normalize_ign(chunk)
        if ign:
            parts.append(ign)

    seen = set()
    out: List[str] = []
    for ign in parts:
        key = ign.lower()
        if key not in seen:
            seen.add(key)
            out.append(ign)
    return out

# =====================
# PERMISSION CHECKS
# =====================

def has_any_role(member: discord.Member, role_names: set) -> bool:
    return any(r.name in role_names for r in getattr(member, "roles", []))

def had_role_name(member: discord.Member, role_name: str) -> bool:
    return any(r.name == role_name for r in getattr(member, "roles", []))

def require_roles():
    async def predicate(interaction: discord.Interaction) -> bool:
        if not isinstance(interaction.user, discord.Member):
            return False
        return has_any_role(interaction.user, ALLOWED_ROLES)
    return app_commands.check(predicate)

# =====================
# INTERACTION SAFETY HELPERS
# =====================

async def safe_defer(interaction: discord.Interaction, ephemeral: bool = True) -> None:
    """Ack quickly; prevents 10062 if event loop is busy."""
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=ephemeral)
    except Exception:
        pass

async def safe_reply(
    interaction: discord.Interaction,
    content: Optional[str] = None,
    *,
    embed: Optional[discord.Embed] = None,
    ephemeral: bool = True,
) -> None:
    """Send a reply regardless of whether we already deferred/responded."""
    try:
        if interaction.response.is_done():
            await interaction.followup.send(content=content, embed=embed, ephemeral=ephemeral)
        else:
            await interaction.response.send_message(content=content, embed=embed, ephemeral=ephemeral)
    except Exception:
        pass

# =====================
# UI: MODAL
# =====================

class RegisterIGNModal(Modal):
    def __init__(self, cog: "IGNRegistrationCog"):
        super().__init__(title="Register IGN")
        self.cog = cog
        self.igns = TextInput(
            label="Enter your EVE IGN(s)",
            placeholder="Example: ARC Tendeuse A (or multiple: IGN1, IGN2)",
            required=True,
            max_length=400,
            style=discord.TextStyle.paragraph,
        )
        self.add_item(self.igns)

    async def on_submit(self, interaction: discord.Interaction):
        # Modal submission also needs fast ACK
        await safe_defer(interaction, ephemeral=True)

        raw = str(self.igns.value)
        igns = split_igns(raw)

        if not igns:
            await safe_reply(interaction, "No valid IGN was provided.", ephemeral=True)
            return

        await self.cog.handle_register_submission(interaction, igns)

# =====================
# UI: PERSISTENT VIEWS
# =====================

class RegisterPanelView(View):
    """Persistent panel shown in #request-to-access-locations with a Register button."""
    def __init__(self, cog: "IGNRegistrationCog"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label=PANEL_BUTTON_LABEL,
        style=discord.ButtonStyle.primary,
        custom_id="ign_panel:register",
    )
    async def open_register(self, interaction: discord.Interaction, button: Button):
        # Must be immediate; no defer here (modal must be the initial response)
        try:
            if interaction.response.is_done():
                await safe_reply(interaction, "Please click the button again.", ephemeral=True)
                return
            await interaction.response.send_modal(RegisterIGNModal(self.cog))
        except discord.NotFound:
            return
        except Exception:
            await safe_reply(interaction, "Failed to open the registration modal.", ephemeral=True)

class AccessRequestView(View):
    """Two-button staff workflow for access requests (Added / Revert)."""
    def __init__(self, cog: "IGNRegistrationCog"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Added to Access List",
        style=discord.ButtonStyle.success,
        custom_id="ign_access:added",
    )
    async def mark_added(self, interaction: discord.Interaction, button: Button):
        await self.cog.handle_access_button(interaction, action="added")

    @discord.ui.button(
        label="Revert to Pending",
        style=discord.ButtonStyle.secondary,
        custom_id="ign_access:revert",
    )
    async def revert_pending(self, interaction: discord.Interaction, button: Button):
        await self.cog.handle_access_button(interaction, action="revert")

class LeaveWarningView(View):
    """Single-button workflow for offboarding warnings (mark removed + purge data)."""
    def __init__(self, cog: "IGNRegistrationCog"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Marked Removed (Purge Data)",
        style=discord.ButtonStyle.danger,
        custom_id="ign_leave:purge",
    )
    async def mark_removed_purge(self, interaction: discord.Interaction, button: Button):
        await self.cog.handle_leave_purge_button(interaction)

# =====================
# COG
# =====================

class IGNRegistrationCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state = load_state()

        # Register persistent views for restart-safe buttons
        self.bot.add_view(RegisterPanelView(self))
        self.bot.add_view(AccessRequestView(self))
        self.bot.add_view(LeaveWarningView(self))

        # Prevent double-run on reconnects within same process
        self._panels_ensured_once: bool = False

    # -------------------------
    # Channel ensure
    # -------------------------

    async def ensure_request_panel_channel(self, guild: discord.Guild) -> discord.TextChannel:
        ch = discord.utils.get(guild.text_channels, name=REQUEST_PANEL_CHANNEL_NAME)
        if ch:
            return ch

        me = guild.me
        if me is None and self.bot.user is not None:
            me = guild.get_member(self.bot.user.id)

        overwrites = {
            guild.default_role: discord.PermissionOverwrite(
                read_messages=True,
                send_messages=False,
                add_reactions=False,
            )
        }
        if me is not None:
            overwrites[me] = discord.PermissionOverwrite(
                read_messages=True,
                send_messages=True,
                embed_links=True,
                manage_messages=True,
            )

        return await guild.create_text_channel(
            REQUEST_PANEL_CHANNEL_NAME,
            overwrites=overwrites,
            reason="Channel for location access request panel",
        )

    async def ensure_access_channel(self, guild: discord.Guild) -> discord.TextChannel:
        ch = discord.utils.get(guild.text_channels, name=ACCESS_CHANNEL_NAME)
        if ch:
            return ch

        me = guild.me
        if me is None and self.bot.user is not None:
            me = guild.get_member(self.bot.user.id)

        overwrites = {
            guild.default_role: discord.PermissionOverwrite(
                send_messages=False,
                add_reactions=False,
                read_messages=True,
            )
        }
        if me is not None:
            overwrites[me] = discord.PermissionOverwrite(
                send_messages=True,
                embed_links=True,
                read_messages=True,
                manage_messages=True,
            )

        return await guild.create_text_channel(
            ACCESS_CHANNEL_NAME,
            overwrites=overwrites,
            reason="Channel for location access list requests",
        )

    # -------------------------
    # Panel management
    # -------------------------

    def get_panel_record(self, guild_id: int) -> Dict[str, Any]:
        panels = self.state.setdefault("panels", {})
        return panels.setdefault(str(guild_id), {"channel_id": None, "message_id": None, "updated_utc": utcnow_iso()})

    async def ensure_register_panel_message(self, guild: discord.Guild) -> None:
        """
        Ensures there is exactly one persistent panel message in #request-to-access-locations.
        Stores message_id in state so we can edit/replace it if deleted.
        """
        ch = await self.ensure_request_panel_channel(guild)

        rec = self.get_panel_record(guild.id)
        rec["channel_id"] = ch.id

        embed = discord.Embed(
            description=PANEL_EMBED_TEXT,
            timestamp=datetime.datetime.utcnow(),
        )
        embed.set_footer(text="Wormhole Access Registration")

        # Try to reuse existing message if present
        msg_id = rec.get("message_id")
        if msg_id:
            try:
                msg = await ch.fetch_message(int(msg_id))
                await msg.edit(embed=embed, view=RegisterPanelView(self))
                rec["updated_utc"] = utcnow_iso()
                save_state(self.state)
                return
            except Exception:
                # message missing or cannot be fetched; fall through to recreate
                pass

        # Create a new panel message
        try:
            msg = await ch.send(embed=embed, view=RegisterPanelView(self))
            rec["message_id"] = msg.id
            rec["updated_utc"] = utcnow_iso()
            save_state(self.state)
        except Exception:
            pass

    # -------------------------
    # Embeds
    # -------------------------

    def build_request_embed(
        self,
        discord_user_id: int,
        discord_user_tag: str,
        igns: List[str],
        status: str,
        message_id: Optional[int] = None,
    ) -> discord.Embed:
        desc = (
            f"**Discord:** <@{discord_user_id}> (`{discord_user_tag}`)\n"
            f"**IGN(s):** {', '.join(igns)}\n"
            f"**Status:** `{status}`\n"
        )
        if message_id:
            desc += f"\n**Request Message ID:** `{message_id}`"

        emb = discord.Embed(
            title="Location Access Request",
            description=desc,
            timestamp=datetime.datetime.utcnow(),
        )
        emb.set_footer(text="IGN Registration Workflow")
        return emb

    def build_leave_warning_embed(
        self,
        member_tag: str,
        member_id: int,
        igns: List[str],
        status: str,
        warning_id: Optional[int] = None,
        purged_by: Optional[str] = None,
    ) -> discord.Embed:
        desc = (
            f"**Departed Member:** `{member_tag}` (ID: `{member_id}`)\n"
            f"**Character(s) to remove:** {', '.join(igns)}\n"
            f"**Status:** `{status}`\n"
        )
        if warning_id:
            desc += f"\n**Warning Message ID:** `{warning_id}`"
        if purged_by:
            desc += f"\n**Processed By:** {purged_by}"
        desc += "\n\nAction required: remove these character names from the in-game access list."

        emb = discord.Embed(
            title="Member Left — Remove From Access List",
            description=desc,
            timestamp=datetime.datetime.utcnow(),
        )
        emb.set_footer(text="IGN Registration / Offboarding")
        return emb

    # -------------------------
    # State helpers
    # -------------------------

    def get_user_record(self, user_id: int) -> Dict[str, Any]:
        users = self.state.setdefault("users", {})
        return users.setdefault(
            str(user_id),
            {"igns": [], "created_utc": utcnow_iso(), "updated_utc": utcnow_iso()},
        )

    def add_user_igns(self, user_id: int, new_igns: List[str]) -> List[str]:
        rec = self.get_user_record(user_id)
        existing = rec.get("igns", [])
        merged = existing[:]

        existing_lower = {x.lower() for x in existing}
        for ign in new_igns:
            if ign.lower() not in existing_lower:
                merged.append(ign)
                existing_lower.add(ign.lower())

        merged = merged[:MAX_IGNS_PER_USER]
        rec["igns"] = merged
        rec["updated_utc"] = utcnow_iso()
        save_state(self.state)
        return merged

    def delete_user_record(self, user_id: int) -> Optional[List[str]]:
        users = self.state.setdefault("users", {})
        rec = users.pop(str(user_id), None)
        if rec:
            save_state(self.state)
            return rec.get("igns", [])
        save_state(self.state)
        return None

    def remove_user_ign(self, user_id: int, ign: Optional[str] = None) -> List[str]:
        users = self.state.setdefault("users", {})
        key = str(user_id)
        if key not in users:
            return []

        if ign is None:
            removed = users[key].get("igns", [])
            users.pop(key, None)
            save_state(self.state)
            return removed

        ign_n = normalize_ign(ign)
        current = users[key].get("igns", [])
        remaining = [x for x in current if x.lower() != ign_n.lower()]
        users[key]["igns"] = remaining
        users[key]["updated_utc"] = utcnow_iso()

        if not remaining:
            users.pop(key, None)

        save_state(self.state)
        return remaining

    def upsert_request(self, message_id: int, payload: Dict[str, Any]) -> None:
        reqs = self.state.setdefault("requests", {})
        reqs[str(message_id)] = payload
        save_state(self.state)

    def get_request(self, message_id: int) -> Optional[Dict[str, Any]]:
        return self.state.get("requests", {}).get(str(message_id))

    def update_request_status(self, message_id: int, status: str, actor_id: int) -> None:
        req = self.get_request(message_id)
        if not req:
            return
        req["status"] = status
        req["updated_utc"] = utcnow_iso()
        req["updated_by"] = actor_id
        self.upsert_request(message_id, req)

    def upsert_leave_warning(self, message_id: int, payload: Dict[str, Any]) -> None:
        lw = self.state.setdefault("leave_warnings", {})
        lw[str(message_id)] = payload
        save_state(self.state)

    def get_leave_warning(self, message_id: int) -> Optional[Dict[str, Any]]:
        return self.state.get("leave_warnings", {}).get(str(message_id))

    def update_leave_warning(self, message_id: int, status: str, processed_by: str) -> None:
        lw = self.get_leave_warning(message_id)
        if not lw:
            return
        lw["status"] = status
        lw["processed_by"] = processed_by
        lw["updated_utc"] = utcnow_iso()
        self.upsert_leave_warning(message_id, lw)

    # -------------------------
    # Core handlers
    # -------------------------

    async def handle_register_submission(self, interaction: discord.Interaction, igns: List[str]):
        if not interaction.guild:
            await safe_reply(interaction, "This must be used in a server.", ephemeral=True)
            return

        merged = self.add_user_igns(interaction.user.id, igns)

        ch = await self.ensure_access_channel(interaction.guild)

        temp_embed = self.build_request_embed(
            discord_user_id=interaction.user.id,
            discord_user_tag=str(interaction.user),
            igns=merged,
            status="PENDING",
            message_id=None,
        )

        msg = await ch.send(embed=temp_embed, view=AccessRequestView(self))

        final_embed = self.build_request_embed(
            discord_user_id=interaction.user.id,
            discord_user_tag=str(interaction.user),
            igns=merged,
            status="PENDING",
            message_id=msg.id,
        )
        await msg.edit(embed=final_embed)

        self.upsert_request(
            msg.id,
            {
                "guild_id": interaction.guild.id,
                "channel_id": ch.id,
                "message_id": msg.id,
                "discord_user_id": interaction.user.id,
                "discord_user_tag": str(interaction.user),
                "igns": merged,
                "status": "PENDING",
                "created_utc": utcnow_iso(),
                "updated_utc": utcnow_iso(),
                "updated_by": None,
            },
        )

        await safe_reply(
            interaction,
            "IGN registration submitted. Staff will process it in #location_access.",
            ephemeral=True,
        )

    async def handle_access_button(self, interaction: discord.Interaction, action: str):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await safe_reply(interaction, "This action must be used in a server.", ephemeral=True)
            return
        if not has_any_role(interaction.user, ALLOWED_ROLES):
            await safe_reply(interaction, "You do not have permission to use these buttons.", ephemeral=True)
            return

        await safe_defer(interaction, ephemeral=True)

        message = interaction.message
        if not message:
            await safe_reply(interaction, "Unable to read the request message.", ephemeral=True)
            return

        req = self.get_request(message.id)
        if not req:
            await safe_reply(interaction, "This request is not recognized (missing from storage).", ephemeral=True)
            return

        new_status = "ADDED" if action == "added" else "PENDING"
        self.update_request_status(message.id, new_status, interaction.user.id)

        emb = self.build_request_embed(
            discord_user_id=int(req["discord_user_id"]),
            discord_user_tag=str(req.get("discord_user_tag", "unknown")),
            igns=req.get("igns", []),
            status=new_status,
            message_id=message.id,
        )

        try:
            await message.edit(embed=emb, view=AccessRequestView(self))
        except Exception:
            pass

        await safe_reply(interaction, f"Request updated to `{new_status}`.", ephemeral=True)

    async def handle_leave_purge_button(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await safe_reply(interaction, "This action must be used in a server.", ephemeral=True)
            return
        if not has_any_role(interaction.user, ALLOWED_ROLES):
            await safe_reply(interaction, "You do not have permission to use this button.", ephemeral=True)
            return

        await safe_defer(interaction, ephemeral=True)

        msg = interaction.message
        if not msg:
            await safe_reply(interaction, "Unable to read the warning message.", ephemeral=True)
            return

        lw = self.get_leave_warning(msg.id)
        if not lw:
            await safe_reply(interaction, "This warning is not recognized (missing from storage).", ephemeral=True)
            return

        departed_user_id = int(lw["discord_user_id"])
        igns = lw.get("igns", [])

        self.delete_user_record(departed_user_id)

        processed_by = f"{interaction.user} (ID: {interaction.user.id})"
        self.update_leave_warning(msg.id, status="REMOVED", processed_by=processed_by)

        new_embed = self.build_leave_warning_embed(
            member_tag=str(lw.get("discord_user_tag", "unknown")),
            member_id=departed_user_id,
            igns=igns,
            status="REMOVED",
            warning_id=msg.id,
            purged_by=processed_by,
        )
        try:
            await msg.edit(embed=new_embed, view=LeaveWarningView(self))
        except Exception:
            pass

        await safe_reply(
            interaction,
            "Marked as removed and purged the member's IGN data from the bot.",
            ephemeral=True,
        )

    # -------------------------
    # Events
    # -------------------------

    @commands.Cog.listener()
    async def on_ready(self):
        # Ensure the panel exists once per process startup
        if self._panels_ensured_once:
            return
        self._panels_ensured_once = True

        for guild in list(getattr(self.bot, "guilds", [])):
            try:
                await self.ensure_register_panel_message(guild)
            except Exception:
                pass

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        if not had_role_name(member, ARC_SECURITY_ROLE):
            return

        rec = self.state.get("users", {}).get(str(member.id))
        if not rec:
            return

        igns = rec.get("igns", [])
        if not igns:
            return

        try:
            ch = await self.ensure_access_channel(member.guild)

            temp = self.build_leave_warning_embed(
                member_tag=str(member),
                member_id=member.id,
                igns=igns,
                status="PENDING_REMOVAL",
                warning_id=None,
                purged_by=None,
            )
            msg = await ch.send(embed=temp, view=LeaveWarningView(self))

            final = self.build_leave_warning_embed(
                member_tag=str(member),
                member_id=member.id,
                igns=igns,
                status="PENDING_REMOVAL",
                warning_id=msg.id,
                purged_by=None,
            )
            await msg.edit(embed=final, view=LeaveWarningView(self))

            self.upsert_leave_warning(
                msg.id,
                {
                    "guild_id": member.guild.id,
                    "channel_id": ch.id,
                    "message_id": msg.id,
                    "discord_user_id": member.id,
                    "discord_user_tag": str(member),
                    "igns": igns,
                    "status": "PENDING_REMOVAL",
                    "created_utc": utcnow_iso(),
                    "updated_utc": utcnow_iso(),
                    "processed_by": None,
                },
            )

        except Exception:
            pass

    # -------------------------
    # Slash commands (staff only)
    # -------------------------

    @app_commands.command(name="unregister_ign", description="Remove a member's IGN(s) from their Discord link.")
    @require_roles()
    async def unregister_ign(self, interaction: discord.Interaction, member: discord.Member, ign: Optional[str] = None):
        # ACK immediately (prevents 10062 under load)
        await safe_defer(interaction, ephemeral=True)

        users = self.state.get("users", {})
        if str(member.id) not in users:
            await safe_reply(interaction, "That member has no registered IGN(s).", ephemeral=True)
            return

        if ign is None:
            removed = users[str(member.id)].get("igns", [])
            self.remove_user_ign(member.id, ign=None)
            await safe_reply(
                interaction,
                f"Removed all IGNs for {member.mention}: {', '.join(removed) if removed else '(none)'}",
                ephemeral=True,
            )
            return

        before = users[str(member.id)].get("igns", [])
        remaining = self.remove_user_ign(member.id, ign=ign)
        if len(remaining) == len(before):
            await safe_reply(interaction, "No matching IGN found to remove.", ephemeral=True)
        else:
            await safe_reply(
                interaction,
                f"Removed `{normalize_ign(ign)}` for {member.mention}. Remaining: {', '.join(remaining) if remaining else '(none)'}",
                ephemeral=True,
            )

    @app_commands.command(name="list_ign", description="List the registered IGN(s) for a member.")
    @require_roles()
    async def list_ign(self, interaction: discord.Interaction, member: discord.Member):
        # ACK immediately (prevents 10062 under load)
        await safe_defer(interaction, ephemeral=True)

        rec = self.state.get("users", {}).get(str(member.id))
        if not rec or not rec.get("igns"):
            await safe_reply(interaction, "No registered IGN(s) for that member.", ephemeral=True)
            return

        igns = rec.get("igns", [])
        emb = discord.Embed(
            title="Registered IGN(s)",
            description=f"**Member:** {member.mention} (`{member}`)\n**IGN(s):** {', '.join(igns)}",
            timestamp=datetime.datetime.utcnow(),
        )
        emb.set_footer(text="IGN Registration")
        await safe_reply(interaction, embed=emb, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(IGNRegistrationCog(bot))
