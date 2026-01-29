import discord
from discord.ext import commands, tasks
from discord import app_commands
import asyncio
import json
import time
import os

DATA_DIR = "/data"
DATA_FILE = os.path.join(DATA_DIR, "member_roles.json")

NEW_MEMBER_ROLE = "New Member"
SECURITY_ROLE = "ARC Security"
SCHEDULING_ROLE = "Scheduling"
ONBOARDING_ROLE = "Onboarding"
GENESIS_ROLE = "ARC Genesis"

LOG_CHANNEL_NAME = "roles-log"

DELAY_SECONDS = 3600  # 1 hour


# ----------------- Persistence -----------------

def ensure_data():
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(DATA_FILE):
        with open(DATA_FILE, "w") as f:
            json.dump(
                {
                    "pending": {},     # user_id -> unix timestamp
                    "rewarded": []     # [user_id]
                },
                f,
                indent=2
            )


def load_data():
    ensure_data()
    with open(DATA_FILE, "r") as f:
        return json.load(f)


def save_data(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)


# ----------------- Cog -----------------

class NewMemberRoles(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        ensure_data()
        self.timer_task.start()
        bot.loop.create_task(self.bootstrap_existing_members())

    def cog_unload(self):
        self.timer_task.cancel()

    # ---------- Utilities ----------

    def get_role(self, guild: discord.Guild, name: str):
        return discord.utils.get(guild.roles, name=name)

    def get_log_channel(self, guild: discord.Guild):
        return discord.utils.get(guild.text_channels, name=LOG_CHANNEL_NAME)

    async def log(self, guild: discord.Guild, message: str):
        channel = self.get_log_channel(guild)
        if channel:
            await channel.send(message)

    # ---------- Bootstrap Existing Members ----------

    async def bootstrap_existing_members(self):
        """Ensure existing members are safely marked to avoid retroactive triggers."""
        await self.bot.wait_until_ready()
        data = load_data()
        changed = False

        for guild in self.bot.guilds:
            nm_role = self.get_role(guild, NEW_MEMBER_ROLE)
            if not nm_role:
                continue

            for member in guild.members:
                if nm_role in member.roles:
                    uid = str(member.id)
                    if uid not in data["rewarded"]:
                        data["rewarded"].append(uid)
                        changed = True

        if changed:
            save_data(data)

    # ---------- Member Join Handling ----------

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        data = load_data()
        data["pending"][str(member.id)] = int(time.time()) + DELAY_SECONDS
        save_data(data)

    @tasks.loop(seconds=60)
    async def timer_task(self):
        data = load_data()
        now = int(time.time())
        changed = False

        for user_id, due in list(data["pending"].items()):
            if now >= due:
                member = None
                for guild in self.bot.guilds:
                    member = guild.get_member(int(user_id))
                    if member:
                        break

                if member:
                    role = self.get_role(member.guild, NEW_MEMBER_ROLE)
                    if role and role not in member.roles:
                        try:
                            await member.add_roles(role, reason="Auto New Member role")
                            await self.log(
                                member.guild,
                                f"🟢 **New Member added** to {member.mention} (auto)"
                            )
                        except discord.Forbidden:
                            pass

                del data["pending"][user_id]
                changed = True

        if changed:
            save_data(data)

    # ---------- Role Removal Detection ----------

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        before_roles = {r.name for r in before.roles}
        after_roles = {r.name for r in after.roles}

        if NEW_MEMBER_ROLE in before_roles and NEW_MEMBER_ROLE not in after_roles:
            # Find who removed the role
            actor = "Unknown"
            try:
                async for entry in after.guild.audit_logs(
                    limit=5,
                    action=discord.AuditLogAction.member_role_update
                ):
                    if entry.target.id == after.id:
                        actor = entry.user.mention
                        break
            except discord.Forbidden:
                actor = "Audit log unavailable"

            await self.log(
                after.guild,
                f"🔴 **New Member removed** from {after.mention} by {actor}"
            )

            if SECURITY_ROLE not in before_roles:
                return

            data = load_data()
            uid = str(after.id)

            if uid in data["rewarded"]:
                return

            sched = self.get_role(after.guild, SCHEDULING_ROLE)
            onboard = self.get_role(after.guild, ONBOARDING_ROLE)

            roles_to_add = [r for r in (sched, onboard) if r]

            if roles_to_add:
                try:
                    await after.add_roles(
                        *roles_to_add,
                        reason="Security onboarding reward"
                    )
                except discord.Forbidden:
                    return

            data["rewarded"].append(uid)
            save_data(data)

    # ---------- Permission Check ----------

    def genesis_only():
        async def predicate(interaction: discord.Interaction):
            return any(r.name == GENESIS_ROLE for r in interaction.user.roles)
        return app_commands.check(predicate)

    # ---------- Slash Commands ----------

    @app_commands.command(
        name="remove_scheduling",
        description="Remove Scheduling role from a member"
    )
    @genesis_only()
    async def remove_scheduling(
        self,
        interaction: discord.Interaction,
        member: discord.Member
    ):
        role = self.get_role(interaction.guild, SCHEDULING_ROLE)
        if role and role in member.roles:
            await member.remove_roles(
                role,
                reason=f"Removed by {interaction.user}"
            )
            await interaction.response.send_message(
                f"Scheduling role removed from {member.mention}.",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                "Member does not have Scheduling role.",
                ephemeral=True
            )

    @app_commands.command(
        name="remove_onboarding",
        description="Remove Onboarding role from a member"
    )
    @genesis_only()
    async def remove_onboarding(
        self,
        interaction: discord.Interaction,
        member: discord.Member
    ):
        role = self.get_role(interaction.guild, ONBOARDING_ROLE)
        if role and role in member.roles:
            await member.remove_roles(
                role,
                reason=f"Removed by {interaction.user}"
            )
            await interaction.response.send_message(
                f"Onboarding role removed from {member.mention}.",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                "Member does not have Onboarding role.",
                ephemeral=True
            )


async def setup(bot: commands.Bot):
    await bot.add_cog(NewMemberRoles(bot))
