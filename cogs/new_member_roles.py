# cogs/new_member_roles.py
# discord.py 2.x
# Handles:
# 1) Auto-assign "New Member" role 1 hour after join
# 2) On manual removal of "New Member", if member has "ARC Security",
#    grant "Scheduling" and "Onboarding"
# 3) Commands for "ARC Genesis" to remove Scheduling / Onboarding from others
# 4) Persistent state via /data volume (Railway-compatible)

import discord
from discord.ext import commands, tasks
import asyncio
import json
import os
from datetime import datetime, timedelta, timezone

PERSIST_ROOT = os.getenv("PERSIST_ROOT", "/data")
STATE_FILE = os.path.join(PERSIST_ROOT, "new_member_state.json")

NEW_MEMBER_ROLE = "New Member"
SECURITY_ROLE = "ARC Security"
SCHEDULING_ROLE = "Scheduling"
ONBOARDING_ROLE = "Onboarding"
GENESIS_ROLE = "ARC Genesis"

DELAY_SECONDS = 3600  # 1 hour


def utcnow():
    return datetime.now(timezone.utc)


class NewMemberRoles(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state = {"pending": {}}  # guild_id -> user_id -> timestamp
        self._load_state()
        self.role_task.start()

    def cog_unload(self):
        self.role_task.cancel()
        self._save_state()

    # ---------------- Persistence ----------------
    def _load_state(self):
        if not os.path.exists(PERSIST_ROOT):
            os.makedirs(PERSIST_ROOT, exist_ok=True)
        if os.path.isfile(STATE_FILE):
            try:
                with open(STATE_FILE, "r", encoding="utf-8") as f:
                    self.state = json.load(f)
            except Exception:
                self.state = {"pending": {}}

    def _save_state(self):
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(self.state, f)

    # ---------------- Helpers ----------------
    def _get_role(self, guild: discord.Guild, name: str):
        return discord.utils.get(guild.roles, name=name)

    def _member_has_role(self, member: discord.Member, role_name: str) -> bool:
        return any(r.name == role_name for r in member.roles)

    # ---------------- Events ----------------
    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        guild_id = str(member.guild.id)
        user_id = str(member.id)
        self.state.setdefault("pending", {}).setdefault(guild_id, {})[user_id] = utcnow().isoformat()
        self._save_state()

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        # Detect manual removal of New Member role
        before_roles = {r.name for r in before.roles}
        after_roles = {r.name for r in after.roles}

        if NEW_MEMBER_ROLE in before_roles and NEW_MEMBER_ROLE not in after_roles:
            # Role was removed
            if SECURITY_ROLE in before_roles:
                sched = self._get_role(after.guild, SCHEDULING_ROLE)
                onboard = self._get_role(after.guild, ONBOARDING_ROLE)
                to_add = [r for r in (sched, onboard) if r and r not in after.roles]
                if to_add:
                    await after.add_roles(*to_add, reason="ARC Security -> onboarding flow")

    # ---------------- Background Task ----------------
    @tasks.loop(seconds=60)
    async def role_task(self):
        now = utcnow()
        changed = False

        for guild_id, users in list(self.state.get("pending", {}).items()):
            guild = self.bot.get_guild(int(guild_id))
            if not guild:
                continue

            role = self._get_role(guild, NEW_MEMBER_ROLE)
            if not role:
                continue

            for user_id, ts in list(users.items()):
                joined_at = datetime.fromisoformat(ts)
                if now - joined_at >= timedelta(seconds=DELAY_SECONDS):
                    member = guild.get_member(int(user_id))
                    if member and not self._member_has_role(member, NEW_MEMBER_ROLE):
                        try:
                            await member.add_roles(role, reason="Automatic New Member assignment")
                        except discord.Forbidden:
                            pass
                    users.pop(user_id, None)
                    changed = True

        if changed:
            self._save_state()

    @role_task.before_loop
    async def before_role_task(self):
        await self.bot.wait_until_ready()

    # ---------------- Commands ----------------
    def _genesis_check(self, member: discord.Member) -> bool:
        return self._member_has_role(member, GENESIS_ROLE)

    @commands.command(name="remove_scheduling")
    async def remove_scheduling(self, ctx: commands.Context, member: discord.Member):
        if not self._genesis_check(ctx.author):
            return await ctx.send("You do not have permission to use this command.")

        role = self._get_role(ctx.guild, SCHEDULING_ROLE)
        if role and role in member.roles:
            await member.remove_roles(role, reason="ARC Genesis command")
            await ctx.send(f"Scheduling role removed from {member.mention}.")
        else:
            await ctx.send("Member does not have the Scheduling role.")

    @commands.command(name="remove_onboarding")
    async def remove_onboarding(self, ctx: commands.Context, member: discord.Member):
        if not self._genesis_check(ctx.author):
            return await ctx.send("You do not have permission to use this command.")

        role = self._get_role(ctx.guild, ONBOARDING_ROLE)
        if role and role in member.roles:
            await member.remove_roles(role, reason="ARC Genesis command")
            await ctx.send(f"Onboarding role removed from {member.mention}.")
        else:
            await ctx.send("Member does not have the Onboarding role.")


async def setup(bot: commands.Bot):
    await bot.add_cog(NewMemberRoles(bot))
