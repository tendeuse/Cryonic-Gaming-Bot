# bot.py (fixed loader + fixed command sync)
import os
import asyncio
import traceback
import inspect
from pathlib import Path

import discord
from discord.ext import commands
from discord import app_commands
from aiohttp import web  # keepalive server


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

# Optional: guild IDs to clear old command duplicates from (rarely needed)
CLEANUP_GUILD_IDS = [
    # 781978392894505020,
]

# Keepalive port (Railway often sets PORT)
KEEPALIVE_HOST = "0.0.0.0"
KEEPALIVE_PORT = int(os.getenv("PORT", "8080"))


# =====================
# INTENTS
# =====================
# If you truly need everything, keep .all(). Otherwise default+members is safer.
intents = discord.Intents.all()


def is_admin_or_allowed_role(member: discord.Member) -> bool:
    if member.guild_permissions.administrator:
        return True
    if ADMIN_SYNC_ROLE_NAMES:
        return any(r.name in ADMIN_SYNC_ROLE_NAMES for r in getattr(member, "roles", []))
    return False


async def maybe_await(result):
    """Await if the returned value is awaitable."""
    if inspect.isawaitable(result):
        return await result
    return result


async def safe_remove_cog(bot: commands.Bot, name: str) -> None:
    """
    Discord.py variants differ: remove_cog may be sync or async.
    This helper works for both.
    """
    try:
        if bot.get_cog(name) is None:
            return
        await maybe_await(bot.remove_cog(name))
    except Exception:
        pass


# =====================
# KEEP-ALIVE HTTP SERVER
# =====================
async def _handle_root(request):
    return web.Response(text="OK")


async def start_keepalive_server(host: str = KEEPALIVE_HOST, port: int = KEEPALIVE_PORT):
    app = web.Application()
    app.router.add_get("/", _handle_root)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, host, port)
    await site.start()

    print(f"Keepalive web server running on http://{host}:{port}/")


# =====================
# /sync ADMIN COMMAND
# =====================
class SyncCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="sync",
        description="Admin-only: sync slash commands globally or to a specific guild. If omitted, syncs the main guild.",
    )
    @app_commands.describe(
        guild_id="Optional: target guild ID to sync. If omitted, syncs the main guild instantly.",
        clean_guild="Optional: if true, clears guild commands first (useful to remove duplicates).",
        also_global="Optional: if true, also perform a global sync (slower to propagate).",
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

        # Default: sync the main guild for instant availability
        if not guild_id:
            gid = GUILD_ID
        else:
            try:
                gid = int(str(guild_id).strip())
            except ValueError:
                await interaction.followup.send("Invalid guild_id. It must be a numeric Discord guild ID.", ephemeral=True)
                return

        guild_obj = discord.Object(id=gid)

        try:
            if clean_guild:
                self.bot.tree.clear_commands(guild=guild_obj)

            # Important: copy global to guild so global commands appear instantly there too
            self.bot.tree.copy_global_to(guild=guild_obj)

            synced = await self.bot.tree.sync(guild=guild_obj)
            msg = f"Guild sync complete for `{gid}`. Synced `{len(synced)}` command(s)."
            if clean_guild:
                msg += " (Guild commands were cleared first.)"

            if also_global:
                gsynced = await self.bot.tree.sync()
                msg += f"\nAlso global synced `{len(gsynced)}` command(s). (Propagation may take time.)"

            await interaction.followup.send(msg, ephemeral=True)

        except Exception as e:
            traceback.print_exception(type(e), e, e.__traceback__)
            await interaction.followup.send(f"Sync failed for `{gid}`. Check bot logs.", ephemeral=True)


# =====================
# BOT
# =====================
class MyBot(commands.Bot):
    async def setup_hook(self):
        # ---- Start keepalive HTTP server ----
        try:
            asyncio.create_task(start_keepalive_server())
        except Exception as e:
            print(f"Failed to start keepalive server: {e}")
            traceback.print_exception(type(e), e, e.__traceback__)

        # ---- Ensure cogs package exists ----
        cogs_dir = Path("cogs")
        if not cogs_dir.exists() or not cogs_dir.is_dir():
            print("No 'cogs' folder found; skipping cog loading.")
        else:
            init_py = cogs_dir / "__init__.py"
            if not init_py.exists():
                try:
                    init_py.write_text("# Auto-created so cogs can be imported as a package.\n", encoding="utf-8")
                    print("Created cogs/__init__.py")
                except Exception as e:
                    print(f"Could not create cogs/__init__.py: {e}")

            # ---- Load cogs from /cogs ----
            loaded = []
            failed = []

            for filename in sorted(os.listdir(cogs_dir)):
                if not filename.endswith(".py") or filename.startswith("__"):
                    continue

                ext = f"cogs.{filename[:-3]}"
                try:
                    if ext in self.extensions:
                        await self.reload_extension(ext)
                        print(f"Reloaded cog: {ext}")
                    else:
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

        # ---- Add /sync command cog ----
        try:
            if self.get_cog("SyncCog") is None:
                await self.add_cog(SyncCog(self))
                print("Loaded internal cog: SyncCog (/sync)")
        except Exception as e:
            print(f"Failed to add SyncCog: {e}")
            traceback.print_exception(type(e), e, e.__traceback__)

        # ---- Resolve correct application_id ----
        # Usually not needed on modern discord.py, but keep since you had it.
        try:
            app_info = await self.application_info()
            self._connection.application_id = app_info.id
            print(f"Application ID resolved as: {app_info.id}")
        except Exception as e:
            print(f"Failed to fetch application info: {e}")
            traceback.print_exception(type(e), e, e.__traceback__)
            return

        # ---- OPTIONAL CLEANUP: remove old guild commands ----
        for gid in CLEANUP_GUILD_IDS:
            try:
                guild_obj = discord.Object(id=int(gid))
                self.tree.clear_commands(guild=guild_obj)
                cleared = await self.tree.sync(guild=guild_obj)
                print(f"Cleanup: cleared guild commands for {gid} (remaining synced: {len(cleared)}).")
            except Exception as e:
                print(f"Cleanup failed for guild {gid}: {e}")
                traceback.print_exception(type(e), e, e.__traceback__)

        # ---- IMPORTANT: GUILD SYNC for instant command availability ----
        # This is what makes /setup_shifts appear when it's registered as a guild command.
        try:
            guild_obj = discord.Object(id=GUILD_ID)

            # Copy global commands into the guild so global commands also appear instantly there
            self.tree.copy_global_to(guild=guild_obj)

            synced_guild = await self.tree.sync(guild=guild_obj)
            print(f"Synced {len(synced_guild)} guild slash commands to {GUILD_ID}.")
        except Exception as e:
            print(f"Guild command sync failed for {GUILD_ID}: {e}")
            traceback.print_exception(type(e), e, e.__traceback__)

        # ---- GLOBAL SYNC (optional) ----
        # Global sync can take time to propagate; keep it if you want commands outside the main guild.
        try:
            synced = await self.tree.sync()
            print(f"Synced {len(synced)} global slash commands.")
        except Exception as e:
            print(f"Global command sync failed: {e}")
            traceback.print_exception(type(e), e, e.__traceback__)


bot = MyBot(command_prefix="!", intents=intents)


@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")
    print("Bot is ready.")


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: Exception):
    print("=== APP COMMAND ERROR ===")
    print(f"Command: {getattr(interaction.command, 'name', 'unknown')}")
    print(f"User: {interaction.user} ({interaction.user.id})")
    print(f"Guild: {getattr(interaction.guild, 'id', None)}")
    traceback.print_exception(type(error), error, error.__traceback__)

    try:
        if interaction.response.is_done():
            await interaction.followup.send(
                "Command failed. Staff can check bot logs for details.",
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                "Command failed. Staff can check bot logs for details.",
                ephemeral=True,
            )
    except Exception:
        pass


bot.run(TOKEN)
