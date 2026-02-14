import os
import asyncio
import traceback
import inspect
from pathlib import Path

import discord
from discord.ext import commands
from discord import app_commands


# =====================
# CONFIG
# =====================
TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN is not set in environment variables.")

# Your main guild where you want instant command availability
GUILD_ID = 1444318058419322983

# Roles allowed to use /sync (optional)
ADMIN_SYNC_ROLE_NAMES = {
    # "ARC Security Corporation Leader",
    # "ARC Security Administration Council",
}

# Optional: guild IDs to clear old command duplicates from
CLEANUP_GUILD_IDS = [
    # 781978392894505020,
]

# =====================
# INTENTS
# =====================
intents = discord.Intents.all()


def is_admin_or_allowed_role(member: discord.Member) -> bool:
    if member.guild_permissions.administrator:
        return True
    if ADMIN_SYNC_ROLE_NAMES:
        return any(r.name in ADMIN_SYNC_ROLE_NAMES for r in getattr(member, "roles", []))
    return False


async def maybe_await(result):
    if inspect.isawaitable(result):
        return await result
    return result


# =====================
# /sync ADMIN COMMAND
# =====================
class SyncCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="sync",
        description="Admin-only: sync slash commands globally or to a specific guild.",
    )
    async def sync(
        self,
        interaction: discord.Interaction,
        guild_id: str | None = None,
        clean_guild: bool = False,
        also_global: bool = False,
    ):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
            return

        if not is_admin_or_allowed_role(interaction.user) and interaction.user.id != interaction.guild.owner_id:
            await interaction.response.send_message("You do not have permission to use /sync.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        if not guild_id:
            gid = GUILD_ID
        else:
            try:
                gid = int(str(guild_id).strip())
            except ValueError:
                await interaction.followup.send("Invalid guild_id.", ephemeral=True)
                return

        guild_obj = discord.Object(id=gid)

        try:
            if clean_guild:
                self.bot.tree.clear_commands(guild=guild_obj)

            self.bot.tree.copy_global_to(guild=guild_obj)
            synced = await self.bot.tree.sync(guild=guild_obj)

            msg = f"Guild sync complete for `{gid}`. Synced `{len(synced)}` command(s)."

            if also_global:
                gsynced = await self.bot.tree.sync()
                msg += f"\nAlso global synced `{len(gsynced)}` command(s)."

            await interaction.followup.send(msg, ephemeral=True)

        except Exception as e:
            traceback.print_exception(type(e), e, e.__traceback__)
            await interaction.followup.send("Sync failed. Check bot logs.", ephemeral=True)


# =====================
# BOT
# =====================
class MyBot(commands.Bot):
    async def setup_hook(self):

        # ---- Ensure cogs package exists ----
        cogs_dir = Path("cogs")
        if not cogs_dir.exists():
            print("No 'cogs' folder found.")
            return

        init_py = cogs_dir / "__init__.py"
        if not init_py.exists():
            init_py.write_text("# auto\n", encoding="utf-8")

        # ---- Load cogs ----
        loaded = []
        failed = []

        for filename in sorted(os.listdir(cogs_dir)):
            if not filename.endswith(".py") or filename.startswith("__"):
                continue

            ext = f"cogs.{filename[:-3]}"
            try:
                await self.load_extension(ext)
                print(f"Loaded cog: {ext}")
                loaded.append(ext)
            except Exception as e:
                print(f"Failed to load {ext}: {e}")
                traceback.print_exception(type(e), e, e.__traceback__)
                failed.append(ext)

        print(f"[COGS] Loaded ({len(loaded)}): {loaded}")
        if failed:
            print(f"[COGS] FAILED ({len(failed)}): {failed}")

        # ---- Add /sync ----
        await self.add_cog(SyncCog(self))

        # ---- Resolve application ID ----
        app_info = await self.application_info()
        self._connection.application_id = app_info.id

        # ---- Guild sync for instant availability ----
        guild_obj = discord.Object(id=GUILD_ID)
        self.tree.copy_global_to(guild=guild_obj)
        gsynced = await self.tree.sync(guild=guild_obj)
        print(f"Synced {len(gsynced)} guild commands to {GUILD_ID}")

        # ---- Optional global sync ----
        synced = await self.tree.sync()
        print(f"Synced {len(synced)} global commands.")


bot = MyBot(command_prefix="!", intents=intents)


@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} ({bot.user.id})")


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: Exception):
    traceback.print_exception(type(error), error, error.__traceback__)
    try:
        if interaction.response.is_done():
            await interaction.followup.send("Command failed. Check logs.", ephemeral=True)
        else:
            await interaction.response.send_message("Command failed. Check logs.", ephemeral=True)
    except Exception:
        pass


bot.run(TOKEN)
