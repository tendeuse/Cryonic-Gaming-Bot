# cogs/help_cog.py
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
DONOR_NOTIFY_CHANNEL = "support-staff-donors"  # Change if you want a different channel

RECRUITER_LOG_CHANNEL = "recruiter-log"


def _get_text_channel_by_name(guild: discord.Guild, name: str) -> Optional[discord.TextChannel]:
    # Prefer exact name match; case-insensitive fallback.
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


class HelpCog(commands.Cog):
    """Cog that provides a slash-only /help command and role/invite logging."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # Invite cache: guild_id -> {invite_code: uses}
        self.invite_uses: dict[int, dict[str, int]] = {}

    # =====================
    # /HELP
    # =====================
    @app_commands.command(
        name="help",
        description="Show all available slash commands organized by category"
    )
    async def help(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title="Bot Commands",
            description="List of available slash commands organized by category (cog):",
            color=discord.Color.blue()
        )

        cog_commands: dict[str, list[str]] = {}

        # Slash commands only
        for cmd in self.bot.tree.walk_commands():
            cog_name = getattr(cmd, "cog_name", None) or "No Category"

            # Prefer qualified name for grouped commands: arc roster, arc join, etc.
            qn = getattr(cmd, "qualified_name", cmd.name)
            desc = (cmd.description or "No description").strip()

            cog_commands.setdefault(cog_name, []).append(f"/{qn} — {desc}")

        # Stable ordering
        for cog_name in sorted(cog_commands.keys(), key=str.lower):
            cmds = sorted(cog_commands[cog_name], key=str.lower)
            # Discord embed field limit is 1024 chars; chunk if needed
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
    # INVITE CACHE
    # =====================
    async def _refresh_invites_for_guild(self, guild: discord.Guild) -> None:
        """
        Refresh cached invite uses for a guild.
        Requires Manage Guild permission for the bot to fetch invites.
        """
        try:
            invites = await guild.invites()
        except discord.Forbidden:
            # Bot doesn't have permission to read invites.
            self.invite_uses[guild.id] = {}
            return
        except discord.HTTPException:
            return

        self.invite_uses[guild.id] = {inv.code: (inv.uses or 0) for inv in invites}

    @commands.Cog.listener()
    async def on_ready(self):
        # Build invite cache for all guilds
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
        """
        Detect roles gained and announce:
        - Recruitment Candidate -> support-staff-recruitment
        - Donor roles 1..6 -> configured donor channel
        """
        if before.guild is None or after.guild is None:
            return

        before_roles = _role_names(before.roles)
        after_roles = _role_names(after.roles)

        gained = after_roles - before_roles
        if not gained:
            return

        guild = after.guild

        # Recruitment Candidate role gained
        if RECRUITMENT_CANDIDATE_ROLE in gained:
            ch = _get_text_channel_by_name(guild, RECRUITMENT_NOTIFY_CHANNEL)
            if ch:
                await ch.send(f"{after.mention} has an interest in recruiting.")

        # Donor role gained (1..6)
        donor_gained = gained.intersection(DONOR_ROLE_NAMES)
        if donor_gained:
            ch = _get_text_channel_by_name(guild, DONOR_NOTIFY_CHANNEL)
            if ch:
                # If they gained multiple donor roles at once, announce each (rare but safe)
                for role_name in sorted(donor_gained, key=str.lower):
                    await ch.send(f"{after.mention} just received the **{role_name}** donor role! 🎉")

    # =====================
    # RECRUITER LOG ON JOIN
    # =====================
    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        """
        Attempt to identify which invite was used and log:
        - Recruit tag
        - Recruiter tag (invite creator)
        in #recruiter-log
        """
        guild = member.guild
        log_ch = _get_text_channel_by_name(guild, RECRUITER_LOG_CHANNEL)
        if not log_ch:
            return

        # If we cannot read invites, we still log the recruit.
        before = self.invite_uses.get(guild.id, {}).copy()

        try:
            invites = await guild.invites()
        except discord.Forbidden:
            embed = discord.Embed(
                title="New Recruit Joined",
                description="Could not read invites (missing Manage Server permission).",
                color=discord.Color.orange()
            )
            embed.add_field(name="Recruit", value=f"{member.mention}\n`{member}`", inline=False)
            await log_ch.send(embed=embed)
            return
        except discord.HTTPException:
            return

        used_invite: Optional[discord.Invite] = None

        # Find which invite increased in uses
        for inv in invites:
            old_uses = before.get(inv.code, 0)
            new_uses = inv.uses or 0
            if new_uses > old_uses:
                used_invite = inv
                break

        # Update cache now that someone joined
        self.invite_uses[guild.id] = {inv.code: (inv.uses or 0) for inv in invites}

        embed = discord.Embed(
            title="New Recruit Joined",
            color=discord.Color.green()
        )
        embed.add_field(name="Recruit", value=f"{member.mention}\n`{member}`", inline=False)

        if used_invite and used_invite.inviter:
            recruiter = used_invite.inviter
            embed.add_field(name="Recruiter (Invite Used)", value=f"{recruiter.mention}\n`{recruiter}`", inline=False)
            embed.add_field(name="Invite Code", value=f"`{used_invite.code}`", inline=True)
            if used_invite.uses is not None:
                embed.add_field(name="Invite Uses", value=str(used_invite.uses), inline=True)
        else:
            embed.add_field(
                name="Recruiter (Invite Used)",
                value="Unknown (vanity URL, expired invite, or no invite increase detected).",
                inline=False
            )

        await log_ch.send(embed=embed)


async def setup(bot: commands.Bot):
    await bot.add_cog(HelpCog(bot))
