import os
import traceback
import inspect
from pathlib import Path

import discord
from discord.ext import commands
from discord import app_commands


TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN is not set in environment variables.")

GUILD_ID = 1444318058419322983

ADMIN_SYNC_ROLE_NAMES = {
    # "ARC Security Corporation Leader",
    # "ARC Security Administration Council",
}

intents = discord.Intents.all()


def is_admin_or_allowed_role(member: discord.Member) -> bool:
    if member.guild_permissions.administrator:
        return True
    if ADMIN_SYNC_ROLE_NAMES:
        return any(r.name in ADMIN_SYNC_ROLE_NAMES for r in getattr(member, "roles", []))
    return False


class SyncCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="sync",
        description="Admin-only: sync slash commands to the main guild (fast) and optionally global (slow).",
    )
    async def sync(
        self,
        interaction: discord.Interaction,
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

        guild_obj = discord.Object(id=GUILD_ID)

        try:
            if clean_guild:
                self.bot.tree.clear_commands(guild=guild_obj)

            self.bot.tree.copy_global_to(guild=guild_obj)
            synced = await self.bot.tree.sync(guild=guild_obj)
            msg = f"✅ Guild sync complete. Synced `{len(synced)}` command(s) to `{GUILD_ID}`."

            if also_global:
                gsynced = await self.bot.tree.sync()
                msg += f"\n✅ Global sync complete. Synced `{len(gsynced)}` command(s). (May take time to appear.)"

            await interaction.followup.send(msg, ephemeral=True)

        except Exception as e:
            traceback.print_exception(type(e), e, e.__traceback__)
            await interaction.followup.send("❌ Sync failed. Check bot logs.", ephemeral=True)


class MyBot(commands.Bot):
    async def setup_hook(self):
        print("[BOOT] setup_hook() start")

        try:
            # ---- Ensure cogs package exists ----
            cogs_dir = Path("cogs")
            print("[BOOT] CWD:", os.getcwd())
            print("[BOOT] cogs_dir exists:", cogs_dir.exists(), "is_dir:", cogs_dir.is_dir())

            if not cogs_dir.exists() or not cogs_dir.is_dir():
                print("[BOOT] No /cogs folder found; skipping cog loading.")
                return

            init_py = cogs_dir / "__init__.py"
            if not init_py.exists():
                init_py.write_text("# auto\n", encoding="utf-8")
                print("[BOOT] Created cogs/__init__.py")

            # ---- Directory listing (THIS is what proves what Railway deployed) ----
            listing = sorted([p.name for p in cogs_dir.iterdir()])
            print("[COGS] Directory listing:", listing)

            # ---- Load cogs ----
            loaded = []
            failed = []

            for filename in sorted(os.listdir(cogs_dir)):
                if not filename.endswith(".py") or filename.startswith("__"):
                    continue

                ext = f"cogs.{filename[:-3]}"
                print(f"[COGS] Attempting load: {ext}")
                try:
                    await self.load_extension(ext)
                    print(f"[COGS] Loaded: {ext}")
                    loaded.append(ext)
                except Exception as e:
                    print(f"[COGS] FAILED: {ext} -> {type(e).__name__}: {e}")
                    traceback.print_exception(type(e), e, e.__traceback__)
                    failed.append(ext)

            print(f"[COGS] Loaded ({len(loaded)}): {loaded}")
            if failed:
                print(f"[COGS] FAILED ({len(failed)}): {failed}")

            # ---- Add /sync ----
            try:
                await self.add_cog(SyncCog(self))
                print("[BOOT] Loaded internal cog: SyncCog (/sync)")
            except Exception as e:
                print("[BOOT] Failed to add SyncCog:", e)
                traceback.print_exception(type(e), e, e.__traceback__)

            # ---- Resolve application ID ----
            try:
                app_info = await self.application_info()
                self._connection.application_id = app_info.id
                print(f"[BOOT] Application ID resolved: {app_info.id}")
            except Exception as e:
                print("[BOOT] application_info() failed:", e)
                traceback.print_exception(type(e), e, e.__traceback__)
                return

            # ---- Guild sync (fast, immediate in your server) ----
            try:
                guild_obj = discord.Object(id=GUILD_ID)
                self.tree.copy_global_to(guild=guild_obj)
                synced_guild = await self.tree.sync(guild=guild_obj)
                print(f"[SYNC] Guild synced {len(synced_guild)} commands to {GUILD_ID}.")
            except Exception as e:
                print("[SYNC] Guild sync failed:", e)
                traceback.print_exception(type(e), e, e.__traceback__)

            # ---- Global sync (optional; keep if you want) ----
            try:
                synced_global = await self.tree.sync()
                print(f"[SYNC] Global synced {len(synced_global)} commands.")
            except Exception as e:
                print("[SYNC] Global sync failed:", e)
                traceback.print_exception(type(e), e, e.__traceback__)

            print("[BOOT] setup_hook() complete")

        except Exception as e:
            print("[BOOT] FATAL in setup_hook():", e)
            traceback.print_exception(type(e), e, e.__traceback__)


bot = MyBot(command_prefix="!", intents=intents)


@bot.event
async def on_ready():
    print(f"[READY] Logged in as {bot.user} (ID: {bot.user.id})")


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: Exception):
    print("[APP] Command error:", error)
    traceback.print_exception(type(error), error, error.__traceback__)
    try:
        if interaction.response.is_done():
            await interaction.followup.send("Command failed. Check bot logs.", ephemeral=True)
        else:
            await interaction.response.send_message("Command failed. Check bot logs.", ephemeral=True)
    except Exception:
        pass


bot.run(TOKEN)
