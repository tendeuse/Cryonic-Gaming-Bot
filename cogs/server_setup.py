# cogs/server_setup.py
import discord
from discord.ext import commands

# =====================
# ARC Hierarchy Roles (must exist)
# =====================
CEO_ROLE = "ARC Security Corporation Leader"
DIRECTOR_ROLE = "ARC Security Administration Council"
GENERAL_ROLE = "ARC General"
COMMANDER_ROLE = "ARC Commander"
OFFICER_ROLE = "ARC Officer"
SECURITY_ROLE = "ARC Security"

ARC_HIERARCHY_ROLES = [
    CEO_ROLE,
    DIRECTOR_ROLE,
    GENERAL_ROLE,
    COMMANDER_ROLE,
    OFFICER_ROLE,
    SECURITY_ROLE,
]

# =====================
# Other Roles
# =====================
NEWBRO_ROLE = "Newbro"
UNITLESS_ROLE = "Unitless"

OTHER_ROLES = [
    "Shop Steward",
    "World of Warcraft",
    "Eve Online",
    NEWBRO_ROLE,
    "Exploration Certified",
    UNITLESS_ROLE,
]

ROLES = ARC_HIERARCHY_ROLES + OTHER_ROLES

# =====================
# Channels to ensure exist
# =====================
CHANNELS = [
    "ap-eve-shop",
    "ap-shop-access",
    "ap-reports",
    "video-submissions",
    "ap-shop-orders",
    "ap-check",
    "exploration-test",
    "corp-rules-test",
    "wormhole-status",
    "kill-mail",
    "arc-hierarchy-log",
    "member-join-logs-points-distribute",
    "location_access",
    "wormhole-status",
]

class ServerSetup(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    # ---------------------
    # Initial server setup
    # ---------------------
    @commands.Cog.listener()
    async def on_ready(self):
        for guild in self.bot.guilds:
            # Ensure roles exist
            for role_name in ROLES:
                if discord.utils.get(guild.roles, name=role_name) is None:
                    try:
                        await guild.create_role(
                            name=role_name,
                            reason="ServerSetup: ensure required roles exist"
                        )
                    except (discord.Forbidden, discord.HTTPException):
                        pass

            # Ensure channels exist
            for channel_name in CHANNELS:
                if discord.utils.get(guild.text_channels, name=channel_name) is None:
                    try:
                        await guild.create_text_channel(
                            channel_name,
                            reason="ServerSetup: ensure required channels exist"
                        )
                    except (discord.Forbidden, discord.HTTPException):
                        pass

    # -------------------------------------------------
    # Role hook: ARC Security → add Newbro + Unitless
    # -------------------------------------------------
    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        # Detect ARC Security role being newly added
        before_roles = {r.id for r in before.roles}
        after_roles = {r.id for r in after.roles}

        if before_roles == after_roles:
            return

        guild = after.guild
        security_role = discord.utils.get(guild.roles, name=SECURITY_ROLE)

        if not security_role or security_role.id not in after_roles:
            return

        # If member already had ARC Security before, ignore
        if security_role.id in before_roles:
            return

        newbro_role = discord.utils.get(guild.roles, name=NEWBRO_ROLE)
        unitless_role = discord.utils.get(guild.roles, name=UNITLESS_ROLE)

        roles_to_add = []
        if newbro_role and newbro_role not in after.roles:
            roles_to_add.append(newbro_role)

        if unitless_role and unitless_role not in after.roles:
            roles_to_add.append(unitless_role)

        if roles_to_add:
            try:
                await after.add_roles(
                    *roles_to_add,
                    reason="ARC Security granted: auto-assign Newbro and Unitless"
                )
            except (discord.Forbidden, discord.HTTPException):
                pass

async def setup(bot: commands.Bot):
    await bot.add_cog(ServerSetup(bot))
