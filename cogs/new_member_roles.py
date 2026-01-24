import discord
from discord.ext import commands, tasks
from discord import app_commands
import asyncio
import sqlite3
import os
from datetime import datetime, timedelta

DB_PATH = "/data/auto_roles.db"
NEW_MEMBER_ROLE = "New Member"
SECURITY_ROLE = "ARC Security"
SCHEDULING_ROLE = "Scheduling"
ONBOARDING_ROLE = "Onboarding"
GENESIS_ROLE = "ARC Genesis"
DELAY_SECONDS = 3600  # 1 hour


def init_db():
    os.makedirs("/data", exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_members (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                join_time TEXT NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
            """
        )
        conn.commit()


class AutoRoles(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        init_db()
        self.check_pending.start()

    def cog_unload(self):
        self.check_pending.cancel()

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        join_time = datetime.utcnow().isoformat()
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO pending_members VALUES (?, ?, ?)",
                (member.guild.id, member.id, join_time),
            )
            conn.commit()

    @tasks.loop(minutes=1)
    async def check_pending(self):
        await self.bot.wait_until_ready()
        now = datetime.utcnow()
        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute("SELECT guild_id, user_id, join_time FROM pending_members").fetchall()

        for guild_id, user_id, join_time in rows:
            guild = self.bot.get_guild(guild_id)
            if not guild:
                continue

            member = guild.get_member(user_id)
            if not member:
                self._delete_entry(guild_id, user_id)
                continue

            jt = datetime.fromisoformat(join_time)
            if (now - jt).total_seconds() >= DELAY_SECONDS:
                role = discord.utils.get(guild.roles, name=NEW_MEMBER_ROLE)
                if role and role not in member.roles:
                    try:
                        await member.add_roles(role, reason="Auto New Member role after 1h")
                    except discord.Forbidden:
                        pass
                self._delete_entry(guild_id, user_id)

    def _delete_entry(self, guild_id: int, user_id: int):
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "DELETE FROM pending_members WHERE guild_id = ? AND user_id = ?",
                (guild_id, user_id),
            )
            conn.commit()

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        before_roles = {r.id for r in before.roles}
        after_roles = {r.id for r in after.roles}

        removed = before_roles - after_roles
        if not removed:
            return

        security_role = discord.utils.get(before.guild.roles, name=SECURITY_ROLE)
        if not security_role or security_role.id not in before_roles:
            return

        scheduling = discord.utils.get(before.guild.roles, name=SCHEDULING_ROLE)
        onboarding = discord.utils.get(before.guild.roles, name=ONBOARDING_ROLE)

        roles_to_add = []
        if scheduling:
            roles_to_add.append(scheduling)
        if onboarding:
            roles_to_add.append(onboarding)

        if roles_to_add:
            try:
                await after.add_roles(*roles_to_add, reason="ARC Security role removal follow-up")
            except discord.Forbidden:
                pass

    def _has_genesis(self, member: discord.Member) -> bool:
        return any(r.name == GENESIS_ROLE for r in member.roles)

    @app_commands.command(name="remove_scheduling", description="Remove Scheduling role from a member")
    async def remove_scheduling(self, interaction: discord.Interaction, member: discord.Member):
        if not self._has_genesis(interaction.user):
            await interaction.response.send_message("Insufficient permissions.", ephemeral=True)
            return

        role = discord.utils.get(interaction.guild.roles, name=SCHEDULING_ROLE)
        if role and role in member.roles:
            await member.remove_roles(role, reason="Removed via slash command")
            await interaction.response.send_message("Scheduling role removed.", ephemeral=True)
        else:
            await interaction.response.send_message("Member does not have Scheduling role.", ephemeral=True)

    @app_commands.command(name="remove_onboarding", description="Remove Onboarding role from a member")
    async def remove_onboarding(self, interaction: discord.Interaction, member: discord.Member):
        if not self._has_genesis(interaction.user):
            await interaction.response.send_message("Insufficient permissions.", ephemeral=True)
            return

        role = discord.utils.get(interaction.guild.roles, name=ONBOARDING_ROLE)
        if role and role in member.roles:
            await member.remove_roles(role, reason="Removed via slash command")
            await interaction.response.send_message("Onboarding role removed.", ephemeral=True)
        else:
            await interaction.response.send_message("Member does not have Onboarding role.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(AutoRoles(bot))
