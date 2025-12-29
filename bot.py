import os
import asyncio
import traceback
import discord
from discord.ext import commands
from discord import app_commands
from aiohttp import web  # keepalive server

TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN is not set in environment variables.")

intents = discord.Intents.all()

ADMIN_SYNC_ROLE_NAMES = {
    # "ARC Security Corporation Leader",
    # "ARC Security Administration Council",
}

CLEANUP_GUILD_IDS = [
    # 781978392894505020,
]


def is_admin_or_allowed_role(member: discord.Member) -> bool:
    if member.guild_permissions.administrator:
        return True
    if ADMIN_SYNC_ROLE_NAMES:
        return any(r.name in ADMIN_SYNC_ROLE_NAMES for r in getattr(member, "roles", []))
    return False


# =====================
# KEEP-ALIVE HTTP SERVER
# (useful on Replit; harmless elsewhere)
# =====================

async def _handle_root(request):
    return web.Response(text="OK")


async def start_keepalive_server(host: str = "0.0.0.0", port: int = 8080):
    app = web.Application()
    app.router.add_get("/", _handle_root)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, host, port)
    await site.start()

    print(f"Keepalive web server running on http://{host}:{port}/")


class SyncCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="sync",
        description="Admin-only: sync slash commands globally, or sync/clean a specific guild by ID.",
    )
    @app_commands.describe(
        guild_id="Optional: target guild ID to sync. If omitted, syncs globally.",
        clean_guild="Optional: if true, clears guild commands first (useful to remove duplicates from past dev sync).",
    )
    async def sync(
        self,
        interaction: discord.Interaction,
        guild_id: str | None = None,
        clean_guild: bool = False,
    ):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
            return

        if not is_admin_or_allowed_role(interaction.user):
            await interaction.response.send_message("You do not have permission to use /sync.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        if not guild_id:
            try:
                synced = await self.bot.tree.sync()
                await interaction.followup.send(
                    f"Global sync complete. Synced `{len(synced)}` command(s).",
                    ephemeral=True
                )
            except Exception as e:
                traceback.print_exception(type(e), e, e.__traceback__)
                await interaction.followup.send("Global sync failed. Check bot logs.", ephemeral=True)
            return

        try:
            gid = int(str(guild_id).strip())
        except ValueError:
            await interaction.followup.send("Invalid guild_id. It must be a numeric Discord guild ID.", ephemeral=True)
            return

        guild = discord.Object(id=gid)

        try:
            if clean_guild:
                self.bot.tree.clear_commands(guild=guild)

            synced = await self.bot.tree.sync(guild=guild)
            msg = f"Guild sync complete for `{gid}`. Synced `{len(synced)}` command(s)."
            if clean_guild:
                msg += " (Guild commands were cleared first.)"
            await interaction.followup.send(msg, ephemeral=True)

        except Exception as e:
            traceback.print_exception(type(e), e, e.__traceback__)
            await interaction.followup.send(f"Guild sync failed for `{gid}`. Check bot logs.", ephemeral=True)


class MyBot(commands.Bot):
    async def setup_hook(self):
        # ---- Start keepalive HTTP server ----
        try:
            asyncio.create_task(start_keepalive_server())
        except Exception as e:
            print(f"Failed to start keepalive server: {e}")
            traceback.print_exception(type(e), e, e.__traceback__)

        # ---- Load cogs from /cogs ----
        cogs_folder = "cogs"
        if not os.path.isdir(cogs_folder):
            print("No 'cogs' folder found; skipping cog loading.")
            return

        for filename in sorted(os.listdir(cogs_folder)):
            if not filename.endswith(".py") or filename.startswith("__"):
                continue

            ext = f"{cogs_folder}.{filename[:-3]}"
            try:
                # load_extension already prevents double-loading in one process
                await self.load_extension(ext)
                print(f"Loaded cog: {ext}")
            except Exception as e:
                print(f"Failed to load {ext}: {e}")
                traceback.print_exception(type(e), e, e.__traceback__)

        # ---- Add /sync command cog ----
        try:
            await self.add_cog(SyncCog(self))
            print("Loaded internal cog: SyncCog (/sync)")
        except Exception as e:
            print(f"Failed to add SyncCog: {e}")
            traceback.print_exception(type(e), e, e.__traceback__)

        # ---- Resolve correct application_id ----
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
                guild = discord.Object(id=int(gid))
                self.tree.clear_commands(guild=guild)
                cleared = await self.tree.sync(guild=guild)
                print(f"Cleanup: cleared guild commands for {gid} (remaining synced: {len(cleared)}).")
            except Exception as e:
                print(f"Cleanup failed for guild {gid}: {e}")
                traceback.print_exception(type(e), e, e.__traceback__)

        # ---- GLOBAL SYNC ----
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
