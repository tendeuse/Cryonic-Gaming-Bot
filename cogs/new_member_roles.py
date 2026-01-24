import discord
from discord.ext import commands, tasks
import asyncio
import json
import os
import time
from typing import Dict

PERSIST_ROOT = os.getenv("PERSIST_ROOT", "/data")
DATA_FILE = os.path.join(PERSIST_ROOT, "new_member_tracker.json")

NEW_MEMBER_ROLE = "New Member"
ARC_SECURITY_ROLE = "ARC Security"
SCHEDULING_ROLE = "Scheduling"
ONBOARDING_ROLE = "Onboarding"
ARC_GENESIS_ROLE = "ARC Genesis"

DELAY_SECONDS = 3600  # 1 hour


class NewMemberRoles(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.data: Dict[str, Dict[str, float]] = {}
        self._load()
        self.role_check_loop.start()

    def cog_unload(self):
        self.role_check_loop.cancel()
        self._save()

    # ---------------------------
    # Persistence
    # ---------------------------
    def _load(self):
        os.makedirs(PERSIST_ROOT, exist_ok=True)
        if os.path.isfile(DATA_FILE):
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                self.data = json.load(f)
        else:
            self.data = {}

    def _save(self):
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2)

    # ---------------------------
    # Member Join Tracking
    # ---------------------------
    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        guild_id = str(member.guild.id)
        self.data.setdefault(guild_id, {})
        self.data[guild_id][str(member.id)] = time.time()
        self._save()

    # ---------------------------
    # Background Role Assignment
    # ---------------------------
    @tasks.loop(minutes=1)
    async def role_check_loop(self):
        now = time.time()

        for guild_id, members in list(self.data.items()):
            guild = self.bot.get_guild(int(guild_id))
            if not guild:
                continue

            role = discord.utils.get(guild.roles, name=NEW_MEMBER_ROLE)
            if not role:
                continue

            for member_id, join_time in list(members.items()):
                if now - join_time < DELAY_SECONDS:
                    continue

                member = guild.get_member(int(member_id))
                if not member:
                    del members[member_id]
                    continue

                if role not in member.roles:
                    try:
                        await member.add_roles(role, reason="Auto New Member assignment after 1 hour")
                    except discord.Forbidden:
                        pass

                del members[member_id]

        self._save()

    @role_check_loop.before_loop
    async def before_role_check_loop(self):
        await self.bot.wait_until_ready()

    # ---------------------------
    # Manual Role Removal Logic
    # ---------------------------
    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        removed_roles = set(before.roles) - set(after.roles)

        new_member_role = discord.utils.get(after.guild.roles, name=NEW_MEMBER_ROLE)
        arc_security_role = discord.utils.get(after.guild.roles, name=ARC_SECURITY_ROLE)
        scheduling_role = discord.utils.get(after.guild.roles, name=SCHEDULING_ROLE)
        onboarding_role = discord.utils.get(after.guild.roles, name=ONBOARDING_ROLE)

        if not all([new_member_role, arc_security_role, scheduling_role, onboarding_role]):
            return

        if new_member_role in removed_roles and arc_security_role in after.roles:
            roles_to_add = []
            if scheduling_role not in after.roles:
                roles_to_add.append(scheduling_role)
            if onboarding_role not in after.roles:
                roles_to_add.append(onboarding_role)

            if roles_to_add:
                try:
                    await after.add_roles(*roles_to_add, reason="ARC Security follow-up roles")
                except discord.Forbidden:
                    pass

    # ---------------------------
    # Permission Check
    # ---------------------------
    def _has_arc_genesis(self, member: discord.Member) -> bool:
        return any(r.name == ARC_GENESIS_ROLE for r in member.roles)

    # ---------------------------
    # Commands
    # ---------------------------
    @commands.hybrid_command(name="remove_scheduling")
    async def remove_scheduling(self, ctx: commands.Context):
        if not self._has_arc_genesis(ctx.author):
            await ctx.reply("You do not have permission to use this command.", ephemeral=True)
            return

        role = discord.utils.get(ctx.guild.roles, name=SCHEDULING_ROLE)
        if role and role in ctx.author.roles:
            await ctx.author.remove_roles(role, reason="Self-service Scheduling removal")
            await ctx.reply("Scheduling role removed.", ephemeral=True)

    @commands.hybrid_command(name="remove_onboarding")
    async def remove_onboarding(self, ctx: commands.Context):
        if not self._has_arc_genesis(ctx.author):
            await ctx.reply("You do not have permission to use this command.", ephemeral=True)
            return

        role = discord.utils.get(ctx.guild.roles, name=ONBOARDING_ROLE)
        if role and role in ctx.author.roles:
            await ctx.author.remove_roles(role, reason="Self-service Onboarding removal")
            await ctx.reply("Onboarding role removed.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(NewMemberRoles(bot))
