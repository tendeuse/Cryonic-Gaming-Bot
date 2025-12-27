# cogs/help_cog.py
import asyncio
import discord
from discord import app_commands
from discord.ext import commands
from typing import Optional

# =====================
# CONFIG
# =====================
RECRUITMENT_CANDIDATE_ROLE = "Recruitment Candidate"
RECRUITMENT_NOTIFY_CHANNEL = "support-staff-recruitment"

DONOR_ROLE_NAMES = {"1", "2", "3", "4", "5", "6"}
DONOR_NOTIFY_CHANNEL = "support-staff-recruitment"

RECRUITER_LOG_CHANNEL = "recruiter-log"

PURGE_ALLOWED_ROLES = {
    "Lycan King",
    "ARC Security Corporation Leader",
}

# Safety caps so the command can't accidentally nuke a whole channel.
PURGE_MAX_AMOUNT = 500
# How far back to scan for matching messages. Must be >= amount because user-filtering may skip messages.
PURGE_MAX_SCAN = 5000

# Throttle for old-message single deletes
DELETE_THROTTLE_EVERY = 20
DELETE_THROTTLE_SLEEP = 1.0


def _get_text_channel_by_name(guild: discord.Guild, name: str) -> Optional[discord.TextChannel]:
    for ch in guild.text_channels:
        if ch.name == name:
            return ch
    lname = name.lower()
    for ch in guild.text_channels:
        if ch.name.lower() == lname:
            return ch
    return None


def _role_names(role_list: list[discord.Role]) -> set[str]:
    return {r.name for r in role_list}


def _has_any_role(member: discord.Member, role_names: set[str]) -> bool:
    return any(r.name in role_names for r in getattr(member, "roles", []))


class PurgeConfirmView(discord.ui.View):
    def __init__(
        self,
        cog: "HelpCog",
        requester_id: int,
        channel_id: int,
        amount: int,
        target_user_id: Optional[int],
    ):
        super().__init__(timeout=60)
        self.cog = cog
        self.requester_id = requester_id
        self.channel_id = channel_id
        self.amount = amount
        self.target_user_id = target_user_id
        self._resolved = False

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # Only the requester can confirm/cancel.
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message(
                "Only the command requester can use these buttons.",
                ephemeral=True,
            )
            return False
        return True

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._resolved:
            return
        self._resolved = True

        # Disable buttons immediately
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="Purge confirmed. Deleting messages...", view=self)

        # Execute purge
        deleted = await self.cog._execute_purge(
            interaction=interaction,
            channel_id=self.channel_id,
            amount=self.amount,
            target_user_id=self.target_user_id,
        )

        # Report
        target_txt = f" from <@{self.target_user_id}>" if self.target_user_id else ""
        await interaction.followup.send(
            f"Completed purge: deleted **{deleted}** message(s){target_txt} in <#{self.channel_id}>.",
            ephemeral=True,
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._resolved:
            return
        self._resolved = True
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="Purge cancelled.", view=self)

    async def on_timeout(self):
        if self._resolved:
            return
        for item in self.children:
            item.disabled = True
        # Cannot edit message without a handle; caller message stays as-is if timed out.


class HelpCog(commands.Cog):
    """Help command + role recognition + recruiter logging + purge command"""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # Invite cache: guild_id -> {invite_code: uses}
        self.invite_uses: dict[int, dict[str, int]] = {}

    # =====================
    # /HELP
    # =====================
    @app_commands.command(
        name="help",
        description="Show all available slash commands organized by category",
    )
    async def help(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title="Bot Commands",
            description="List of available slash commands organized by category (cog):",
            color=discord.Color.blue(),
        )

        cog_commands: dict[str, list[str]] = {}

        for cmd in self.bot.tree.walk_commands():
            cog_name = getattr(cmd, "cog_name", None) or "No Category"
            qn = getattr(cmd, "qualified_name", cmd.name)
            desc = (cmd.description or "No description").strip()
            cog_commands.setdefault(cog_name, []).append(f"/{qn} — {desc}")

        for cog_name in sorted(cog_commands.keys(), key=str.lower):
            cmds = sorted(cog_commands[cog_name], key=str.lower)
            chunk = ""
            for line in cmds:
                if len(chunk) + len(line) + 1 > 1024:
                    embed.add_field(name=cog_name, value=chunk.rstrip(), inline=False)
                    chunk = ""
                chunk += line + "\n"
            if chunk.strip():
                embed.add_field(name=cog_name, value=chunk.rstrip(), inline=False)

        await interaction.response.send_message(embed=embed, ephemeral=True)

    # =====================
    # /PURGE (with confirmation + optional user filter + no 14-day limitation)
    # =====================
    @app_commands.command(
        name="purge",
        description="Delete messages in this channel (optionally only from a specific user)",
    )
    @app_commands.describe(
        amount=f"Number of messages to delete (1–{PURGE_MAX_AMOUNT})",
        user="If provided, only delete messages from this user",
    )
    async def purge(
        self,
        interaction: discord.Interaction,
        amount: int,
        user: Optional[discord.Member] = None,
    ):
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
            return

        if amount < 1 or amount > PURGE_MAX_AMOUNT:
            await interaction.response.send_message(
                f"Amount must be between 1 and {PURGE_MAX_AMOUNT}.",
                ephemeral=True,
            )
            return

        if not _has_any_role(interaction.user, PURGE_ALLOWED_ROLES):
            await interaction.response.send_message(
                "You do not have permission to use this command.",
                ephemeral=True,
            )
            return

        if not isinstance(interaction.channel, (discord.TextChannel, discord.Thread)):
            await interaction.response.send_message("This command can only be used in text channels/threads.", ephemeral=True)
            return

        target_user_id = user.id if user else None

        target_txt = f" from {user.mention}" if user else ""
        msg = (
            f"Confirm purge of **{amount}** message(s){target_txt} in {interaction.channel.mention}?\n"
            f"This will also delete messages older than 14 days (may take longer)."
        )

        view = PurgeConfirmView(
            cog=self,
            requester_id=interaction.user.id,
            channel_id=interaction.channel.id,
            amount=amount,
            target_user_id=target_user_id,
        )

        await interaction.response.send_message(msg, ephemeral=True, view=view)

    async def _execute_purge(
        self,
        interaction: discord.Interaction,
        channel_id: int,
        amount: int,
        target_user_id: Optional[int],
    ) -> int:
        """
        Deletes up to `amount` messages in channel_id, optionally filtered by author (target_user_id).
        Bulk deletes where possible; falls back to single deletes for older messages.
        Returns number deleted.
        """
        channel = interaction.guild.get_channel(channel_id)
        if channel is None:
            # Try fetch for threads or cache misses
            try:
                channel = await interaction.guild.fetch_channel(channel_id)
            except Exception:
                return 0

        if not isinstance(channel, (discord.TextChannel, discord.Thread)):
            return 0

        deleted_count = 0
        scanned = 0
        to_delete: list[discord.Message] = []

        # Collect candidates (newest -> oldest)
        async for msg in channel.history(limit=PURGE_MAX_SCAN, oldest_first=False):
            scanned += 1
            if target_user_id and msg.author.id != target_user_id:
                continue

            # Don’t delete the confirmation message itself (ephemeral won't be in history anyway)
            to_delete.append(msg)
            if len(to_delete) >= amount:
                break

        if not to_delete:
            return 0

        # Partition for bulk vs single delete:
        # Bulk delete fails for messages older than 14 days, so we attempt bulk only on a recent batch,
        # then single-delete the rest.
        # Use created_at timestamps vs utcnow.
        now = discord.utils.utcnow()
        recent: list[discord.Message] = []
        old: list[discord.Message] = []

        fourteen_days = 14 * 24 * 60 * 60
        for m in to_delete:
            age = (now - m.created_at).total_seconds()
            if age < fourteen_days:
                recent.append(m)
            else:
                old.append(m)

        # Bulk delete recent messages (best-effort)
        if recent:
            # discord.py's purge() can do filtering, but we already collected exactly what we want.
            # Use delete_messages for a single bulk request (up to 100).
            # If >100, do it in chunks.
            for i in range(0, len(recent), 100):
                chunk = recent[i : i + 100]
                try:
                    await channel.delete_messages(chunk, reason=f"Purge by {interaction.user}")
                    deleted_count += len(chunk)
                except discord.HTTPException:
                    # If bulk fails, fall back to single deletes
                    for m in chunk:
                        try:
                            await m.delete(reason=f"Purge by {interaction.user}")
                            deleted_count += 1
                        except discord.HTTPException:
                            pass

        # Single-delete old messages (throttled)
        if old:
            for idx, m in enumerate(old, start=1):
                try:
                    await m.delete(reason=f"Purge by {interaction.user}")
                    deleted_count += 1
                except discord.HTTPException:
                    pass

                if idx % DELETE_THROTTLE_EVERY == 0:
                    await asyncio.sleep(DELETE_THROTTLE_SLEEP)

        return deleted_count

    # =====================
    # INVITE CACHE
    # =====================
    async def _refresh_invites_for_guild(self, guild: discord.Guild):
        try:
            invites = await guild.invites()
        except (discord.Forbidden, discord.HTTPException):
            self.invite_uses[guild.id] = {}
            return

        self.invite_uses[guild.id] = {inv.code: (inv.uses or 0) for inv in invites}

    @commands.Cog.listener()
    async def on_ready(self):
        for guild in self.bot.guilds:
            await self._refresh_invites_for_guild(guild)

    @commands.Cog.listener()
    async def on_invite_create(self, invite: discord.Invite):
        if invite.guild:
            await self._refresh_invites_for_guild(invite.guild)

    @commands.Cog.listener()
    async def on_invite_delete(self, invite: discord.Invite):
        if invite.guild:
            await self._refresh_invites_for_guild(invite.guild)

    # =====================
    # ROLE RECOGNITION
    # =====================
    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        before_roles = _role_names(before.roles)
        after_roles = _role_names(after.roles)
        gained = after_roles - before_roles

        if not gained:
            return

        guild = after.guild

        if RECRUITMENT_CANDIDATE_ROLE in gained:
            ch = _get_text_channel_by_name(guild, RECRUITMENT_NOTIFY_CHANNEL)
            if ch:
                await ch.send(f"{after.mention} has an interest in recruiting.")

        donor_gained = gained.intersection(DONOR_ROLE_NAMES)
        if donor_gained:
            ch = _get_text_channel_by_name(guild, DONOR_NOTIFY_CHANNEL)
            if ch:
                for role_name in sorted(donor_gained, key=str.lower):
                    await ch.send(f"{after.mention} just received the **{role_name}** donor role! 🎉")

    # =====================
    # RECRUITER LOG ON JOIN
    # =====================
    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        guild = member.guild
        log_ch = _get_text_channel_by_name(guild, RECRUITER_LOG_CHANNEL)
        if not log_ch:
            return

        before = self.invite_uses.get(guild.id, {}).copy()

        try:
            invites = await guild.invites()
        except discord.Forbidden:
            embed = discord.Embed(
                title="New Recruit Joined",
                description="Invite tracking unavailable (missing Manage Server permission).",
                color=discord.Color.orange(),
            )
            embed.add_field(name="Recruit", value=f"{member.mention}\n`{member}`", inline=False)
            await log_ch.send(embed=embed)
            return
        except discord.HTTPException:
            return

        used_invite = None
        for inv in invites:
            if (inv.uses or 0) > before.get(inv.code, 0):
                used_invite = inv
                break

        self.invite_uses[guild.id] = {inv.code: (inv.uses or 0) for inv in invites}

        embed = discord.Embed(title="New Recruit Joined", color=discord.Color.green())
        embed.add_field(name="Recruit", value=f"{member.mention}\n`{member}`", inline=False)

        if used_invite and used_invite.inviter:
            embed.add_field(
                name="Recruiter (Invite Used)",
                value=f"{used_invite.inviter.mention}\n`{used_invite.inviter}`",
                inline=False,
            )
            embed.add_field(name="Invite Code", value=f"`{used_invite.code}`", inline=True)
            if used_invite.uses is not None:
                embed.add_field(name="Invite Uses", value=str(used_invite.uses), inline=True)
        else:
            embed.add_field(
                name="Recruiter (Invite Used)",
                value="Unknown (vanity URL, expired invite, or no invite increase detected).",
                inline=False,
            )

        await log_ch.send(embed=embed)


async def setup(bot: commands.Bot):
    await bot.add_cog(HelpCog(bot))
