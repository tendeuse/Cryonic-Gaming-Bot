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
SUBSIDIZED_ROLE = "ARC Subsidized"
SCHEDULING_ROLE = "Scheduling"
ONBOARDING_ROLE = "Onboarding"

# ✅ NEW TRIGGER ROLE
EVE_ROLE = "EVE online"

# Permission roles
GENESIS_ROLE = "ARC Genesis"
DIRECTOR_ROLE = "ARC Security Administration Council"
CEO_ROLE = "ARC Security Corporation Leader"

LOG_CHANNEL_NAME = "roles-log"

DELAY_SECONDS = 3600  # 1 hour


# ----------------- Persistence -----------------

def ensure_data():
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(DATA_FILE):
        with open(DATA_FILE, "w") as f:
            json.dump(
                {
                    "pending": {},       # user_id -> unix timestamp
                    "rewarded": [],      # [user_id] (New Member removal reward)
                    "eve_awarded": []    # ✅ [user_id] (EVE role trigger processed)
                },
                f,
                indent=2
            )


def load_data():
    ensure_data()
    with open(DATA_FILE, "r") as f:
        data = json.load(f)
    # ✅ backfill keys safely for existing files
    if "pending" not in data or not isinstance(data["pending"], dict):
        data["pending"] = {}
    if "rewarded" not in data or not isinstance(data["rewarded"], list):
        data["rewarded"] = []
    if "eve_awarded" not in data or not isinstance(data["eve_awarded"], list):
        data["eve_awarded"] = []
    return data


def save_data(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)


# ----------------- Cog -----------------

class NewMemberRoles(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        ensure_data()
        self._data_lock = asyncio.Lock()

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

    def user_has_any_role(self, member: discord.Member, role_names: set[str]) -> bool:
        return any(r.name in role_names for r in member.roles)

    # ---------- Core Rule: Subsidy Swap ----------

    async def enforce_subsidy_swap(self, member: discord.Member, reason: str = "Auto subsidy swap"):
        """
        If member has Onboarding + ARC Security, replace ARC Security with ARC Subsidized.
        Safe to call repeatedly; it only acts when needed.
        """
        guild = member.guild
        onboarding = self.get_role(guild, ONBOARDING_ROLE)
        security = self.get_role(guild, SECURITY_ROLE)
        subsidized = self.get_role(guild, SUBSIDIZED_ROLE)

        if not onboarding or not security or not subsidized:
            return

        has_onboarding = onboarding in member.roles
        has_security = security in member.roles
        has_subsidized = subsidized in member.roles

        if not (has_onboarding and has_security):
            return

        try:
            if not has_subsidized:
                await member.add_roles(subsidized, reason=reason)

            if security in member.roles:
                await member.remove_roles(security, reason=reason)

            await self.log(
                guild,
                f"🟣 **Subsidy swap**: {member.mention} had **{ONBOARDING_ROLE}** + **{SECURITY_ROLE}** → "
                f"removed **{SECURITY_ROLE}**, added **{SUBSIDIZED_ROLE}**."
            )
        except discord.Forbidden:
            await self.log(
                guild,
                f"⚠️ **Subsidy swap failed** for {member.mention}: missing permissions or role hierarchy issue."
            )
        except discord.HTTPException as e:
            await self.log(
                guild,
                f"⚠️ **Subsidy swap failed** for {member.mention}: HTTP error: {e}"
            )

    # ---------- ✅ NEW: EVE Role Flow ----------

    async def handle_eve_role_added(self, member: discord.Member, reason: str = "EVE role trigger"):
        """
        When member gets EVE online role:
          1) Add ARC Subsidized
          2) If they had Security OR Subsidized -> add Scheduling + Onboarding
          3) Run subsidy swap again (Security removed if Onboarding now present)
        One-time per user (tracked by eve_awarded in JSON).
        """
        guild = member.guild
        eve = self.get_role(guild, EVE_ROLE)
        subsidized = self.get_role(guild, SUBSIDIZED_ROLE)
        security = self.get_role(guild, SECURITY_ROLE)
        sched = self.get_role(guild, SCHEDULING_ROLE)
        onboard = self.get_role(guild, ONBOARDING_ROLE)

        if not eve or eve not in member.roles:
            return
        if not subsidized:
            return

        uid = str(member.id)

        async with self._data_lock:
            data = load_data()
            if uid in data["eve_awarded"]:
                return
            # mark processed immediately to avoid double-processing on rapid updates
            data["eve_awarded"].append(uid)
            save_data(data)

        # Step 1: ensure subsidized
        try:
            if subsidized not in member.roles:
                await member.add_roles(subsidized, reason=reason)
        except discord.Forbidden:
            await self.log(guild, f"⚠️ Could not add **{SUBSIDIZED_ROLE}** to {member.mention} (permissions/hierarchy).")
            return
        except discord.HTTPException as e:
            await self.log(guild, f"⚠️ Could not add **{SUBSIDIZED_ROLE}** to {member.mention}: {e}")
            return

        # Step 2: if they had Security OR Subsidized, add Scheduling + Onboarding
        # (After step 1, they definitely have Subsidized; this condition will be true
        # for anyone in the program.)
        has_security = (security in member.roles) if security else False
        has_subsidized = subsidized in member.roles

        roles_to_add = []
        if (has_security or has_subsidized):
            if sched and sched not in member.roles:
                roles_to_add.append(sched)
            if onboard and onboard not in member.roles:
                roles_to_add.append(onboard)

        if roles_to_add:
            try:
                await member.add_roles(*roles_to_add, reason=f"{reason} (add Scheduling + Onboarding)")
            except discord.Forbidden:
                await self.log(guild, f"⚠️ Could not add Scheduling/Onboarding to {member.mention} (permissions/hierarchy).")
            except discord.HTTPException as e:
                await self.log(guild, f"⚠️ Could not add Scheduling/Onboarding to {member.mention}: {e}")

        # Step 3: run swap again (if Onboarding present and Security present -> remove Security)
        await self.enforce_subsidy_swap(member, reason=f"{reason} (post-onboarding swap)")

        # Log summary
        try:
            await self.log(
                guild,
                f"🟦 **EVE trigger**: {member.mention} received **{EVE_ROLE}** → added **{SUBSIDIZED_ROLE}**"
                f"{' + Scheduling + Onboarding' if roles_to_add else ''}."
            )
        except Exception:
            pass

    # ---------- Bootstrap Existing Members ----------

    async def bootstrap_existing_members(self):
        """Mark existing New Member holders as rewarded (no retro triggers) AND enforce subsidy swap AND enforce EVE rule."""
        await self.bot.wait_until_ready()

        async with self._data_lock:
            data = load_data()

        changed = False

        for guild in self.bot.guilds:
            nm_role = self.get_role(guild, NEW_MEMBER_ROLE)
            eve_role = self.get_role(guild, EVE_ROLE)

            # 1) Avoid retroactive New Member reward triggers
            if nm_role:
                for member in guild.members:
                    if nm_role in member.roles:
                        uid = str(member.id)
                        if uid not in data["rewarded"]:
                            data["rewarded"].append(uid)
                            changed = True

            # 2) Enforce subsidy swap for all members
            for member in guild.members:
                await self.enforce_subsidy_swap(member, reason="Bootstrap subsidy swap")

            # 3) ✅ Enforce EVE rule for existing EVE holders (ensure Subsidized),
            #    but do NOT auto-add Scheduling/Onboarding unless you want that retroactively.
            #    We will only ensure Subsidized is present without marking eve_awarded,
            #    so future EVE add events still work properly.
            if eve_role:
                subsidized = self.get_role(guild, SUBSIDIZED_ROLE)
                if subsidized:
                    for member in guild.members:
                        if eve_role in member.roles and subsidized not in member.roles:
                            try:
                                await member.add_roles(subsidized, reason="Bootstrap: EVE holders get Subsidized")
                                await self.log(guild, f"🟦 **Bootstrap**: Added **{SUBSIDIZED_ROLE}** to {member.mention} (has **{EVE_ROLE}**).")
                            except Exception:
                                pass

        if changed:
            async with self._data_lock:
                save_data(data)

    # ---------- Member Join Handling ----------

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        async with self._data_lock:
            data = load_data()
            data["pending"][str(member.id)] = int(time.time()) + DELAY_SECONDS
            save_data(data)

    @tasks.loop(seconds=60)
    async def timer_task(self):
        async with self._data_lock:
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

                    await self.enforce_subsidy_swap(member, reason="Timer task subsidy swap")

                del data["pending"][user_id]
                changed = True

        if changed:
            async with self._data_lock:
                save_data(data)

    # ---------- Role Update Handling (New Member + EVE Trigger + Swap Hook) ----------

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        before_roles = {r.name for r in before.roles}
        after_roles = {r.name for r in after.roles}

        # Always enforce the swap rule on any update
        await self.enforce_subsidy_swap(after, reason="Role update subsidy swap")

        # ✅ NEW: detect EVE role being added
        if (EVE_ROLE in after_roles) and (EVE_ROLE not in before_roles):
            await self.handle_eve_role_added(after, reason="EVE role added")

        # Existing: New Member removed logic
        if NEW_MEMBER_ROLE in before_roles and NEW_MEMBER_ROLE not in after_roles:
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

            async with self._data_lock:
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

            # Swap again after reward
            await self.enforce_subsidy_swap(after, reason="Post-reward subsidy swap")

            async with self._data_lock:
                data = load_data()
                data["rewarded"].append(uid)
                save_data(data)

    # ---------- Permission Check ----------

    def leadership_only():
        allowed = {GENESIS_ROLE, DIRECTOR_ROLE, CEO_ROLE}
        async def predicate(interaction: discord.Interaction):
            if not interaction.user or not isinstance(interaction.user, discord.Member):
                return False
            return any(r.name in allowed for r in interaction.user.roles)
        return app_commands.check(predicate)

    def genesis_only():
        async def predicate(interaction: discord.Interaction):
            return any(r.name == GENESIS_ROLE for r in interaction.user.roles)
        return app_commands.check(predicate)

    # ---------- Slash Commands ----------

    @app_commands.command(
        name="transfer_to_security",
        description="Transfer a member from ARC Subsidized to ARC Security"
    )
    @leadership_only()
    async def transfer_to_security(
        self,
        interaction: discord.Interaction,
        member: discord.Member
    ):
        guild = interaction.guild
        if not guild:
            await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
            return

        security = self.get_role(guild, SECURITY_ROLE)
        subsidized = self.get_role(guild, SUBSIDIZED_ROLE)

        if not security or not subsidized:
            await interaction.response.send_message(
                f"Missing roles: ensure **{SECURITY_ROLE}** and **{SUBSIDIZED_ROLE}** exist.",
                ephemeral=True
            )
            return

        if subsidized not in member.roles:
            await interaction.response.send_message(
                f"{member.mention} does not have **{SUBSIDIZED_ROLE}**.",
                ephemeral=True
            )
            return

        onboarding = self.get_role(guild, ONBOARDING_ROLE)
        if onboarding and onboarding in member.roles:
            await interaction.response.send_message(
                f"⚠️ {member.mention} still has **{ONBOARDING_ROLE}**. "
                f"Your auto-rule will re-swap them back to **{SUBSIDIZED_ROLE}** unless you remove **{ONBOARDING_ROLE}** first.",
                ephemeral=True
            )

        try:
            await member.add_roles(security, reason=f"Transfer to security by {interaction.user}")
            await member.remove_roles(subsidized, reason=f"Transfer to security by {interaction.user}")

            await self.log(
                guild,
                f"🟦 **Transfer**: {interaction.user.mention} transferred {member.mention} "
                f"from **{SUBSIDIZED_ROLE}** → **{SECURITY_ROLE}**."
            )

            await interaction.response.send_message(
                f"✅ Transferred {member.mention} to **{SECURITY_ROLE}** (removed **{SUBSIDIZED_ROLE}**).",
                ephemeral=True
            )
        except discord.Forbidden:
            await interaction.response.send_message(
                "❌ I don't have permission (check role hierarchy / Manage Roles).",
                ephemeral=True
            )
        except discord.HTTPException as e:
            await interaction.response.send_message(
                f"❌ Discord API error: {e}",
                ephemeral=True
            )

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
