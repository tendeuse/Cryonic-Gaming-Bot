import asyncio
import gc
import logging
import os
import time
import tracemalloc
import traceback
from pathlib import Path

import discord
from discord.ext import commands
from discord import app_commands


# ---------------------------------------------------------------------------
# Optional diagnostics — both off by default, zero overhead unless enabled.
#   MEM_PROBE=1       — log RSS + top object types every 2 min, plus (after a
#                       10-min warm-up baseline) the top allocating file:line
#                       by growth since baseline — names the leak's source.
#   RL_ORIGIN_PROBE=1 — pinpoint which cog/line triggers each new 429 source.
# ---------------------------------------------------------------------------
class _RateLimitOriginLogger(logging.Handler):
    """Pinpoint which cog/line triggers each 429, by capturing the live call
    stack at the moment discord.py logs 'We are being rate limited'. Throttled
    to once per 30s per originating frame so it doesn't add to the log spam
    it's diagnosing. Enable with RL_ORIGIN_PROBE=1."""

    def __init__(self):
        super().__init__()
        self._last_seen: dict[str, float] = {}

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = record.getMessage()
            if "rate limited" not in msg:
                return
            frames = [
                f for f in traceback.extract_stack()
                if ("\\cogs\\" in f.filename or "/cogs/" in f.filename or f.filename.endswith("bot.py"))
                and "uiutil.py" not in f.filename
            ]
            if not frames:
                return
            origin = frames[-1]
            key = f"{origin.filename}:{origin.lineno}"
            now = time.monotonic()
            if now - self._last_seen.get(key, 0) < 30:
                return
            self._last_seen[key] = now
            chain = " <- ".join(
                f"{Path(f.filename).name}:{f.lineno}:{f.name}" for f in frames[-4:][::-1]
            )
            print(f"[RLORIGIN] {msg.split(chr(10))[0][:140]} | call chain: {chain}")
        except Exception:
            pass


def _rss_mb() -> int:
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) // 1024
    except Exception:
        pass
    return -1


def _tune_glibc_malloc() -> None:
    """Reduce glibc heap retention so RSS tracks the live working set.

    Python's many small transient allocations (e.g. json.loads of a big ESI
    assets response) fragment glibc's per-arena heaps; the freed memory isn't
    returned, inflating RSS. Cap arenas and lower the trim threshold so free()
    hands memory back. Best-effort; no-op off glibc. (MALLOC_ARENA_MAX=2 as an
    env var is even more effective since it applies before libc init.)"""
    try:
        import ctypes
        libc = ctypes.CDLL("libc.so.6")
        libc.mallopt(-8, 2)        # M_ARENA_MAX = 2
        libc.mallopt(-1, 131072)   # M_TRIM_THRESHOLD = 128 KB
    except Exception:
        pass


_tune_glibc_malloc()


def _malloc_trim() -> None:
    """Return freed heap memory to the OS. Python frees large transient
    allocations (e.g. a character's full ESI assets/killmails parsed during the
    arc_seat ESI pull) but glibc retains the heap, inflating RSS — the probe
    showed ~1.3 GB of RSS with only ~tens of MB of live objects. malloc_trim
    hands that back. No-op off glibc (e.g. local Windows)."""
    try:
        import ctypes
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass


async def _malloc_trim_loop() -> None:
    while True:
        await asyncio.sleep(60)
        _malloc_trim()


async def _mem_probe(bot: commands.Bot) -> None:
    import collections

    tracemalloc.start(10)  # 10 frames of traceback per allocation
    baseline = None
    elapsed = 0
    BASELINE_AT = 600  # snapshot a baseline 10 min in, after startup settles

    while True:
        try:
            objs = gc.get_objects()
            by_type = collections.Counter(type(o).__name__ for o in objs).most_common(8)
            members = sum((g.member_count or 0) for g in bot.guilds)
            cached = len(getattr(bot, "cached_messages", []) or [])
            top = ", ".join(f"{n}={c}" for n, c in by_type)
            # Pending asyncio tasks + their coroutine names: if a loop spawns
            # edit/fetch tasks faster than Discord's rate limit lets them finish,
            # they pile up here — that backlog is the leak driving the OOM.
            try:
                tasks = asyncio.all_tasks()
                tcoros = collections.Counter()
                for t in tasks:
                    try:
                        tcoros[t.get_coro().__qualname__] += 1
                    except Exception:
                        pass
                ttop = ", ".join(f"{n}={c}" for n, c in tcoros.most_common(6))
            except Exception:
                tasks, ttop = [], "?"
            print(
                f"[MEMPROBE] rss={_rss_mb()}MB objs={len(objs)} tasks={len(tasks)} "
                f"guilds={len(bot.guilds)} members~{members} cached_msgs={cached}\n"
                f"           top_tasks: {ttop}\n"
                f"           top_objs:  {top}"
            )

            # Once warmed up, snapshot a baseline; every cycle after that, diff
            # against it and print the file:line whose allocations grew the
            # most — that names the leak's source directly (unlike the
            # gc-by-type counts above, which show WHAT is growing but not
            # WHERE it's allocated).
            if baseline is None and elapsed >= BASELINE_AT:
                baseline = tracemalloc.take_snapshot()
                print("[MEMPROBE] tracemalloc baseline captured.")
            elif baseline is not None:
                snap = tracemalloc.take_snapshot()
                diffs = snap.compare_to(baseline, "lineno")
                lines = "\n           ".join(str(d) for d in diffs[:8])
                print(f"[MEMPROBE] tracemalloc growth since baseline:\n           {lines}")
        except Exception as e:
            print(f"[MEMPROBE] error: {e}")
        await asyncio.sleep(120)
        elapsed += 120


if os.getenv("RL_ORIGIN_PROBE"):
    logging.getLogger("discord.http").addHandler(_RateLimitOriginLogger())
    print("[RLORIGIN] enabled — will print the calling cog/line for each new 429 source.")

TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN is not set in environment variables.")

# IMPORTANT:
# - DEV_GUILD_ID is optional. Set it in Railway only if you want fast guild sync.
# - If not set (or 0), guild sync is skipped entirely (no more 50001 tracebacks).
DEV_GUILD_ID = int(os.getenv("DEV_GUILD_ID", "0"))

ADMIN_SYNC_ROLE_NAMES = {
    # "ARC Security Corporation Leader",
    # "ARC Security Administration Council",
}

# Explicit intents instead of Intents.all(). Dropping `presences` (which the
# bot never reads — no member.status/activity usage anywhere) stops discord.py
# from caching presence state for every member, a large and needless RAM cost.
# Keep the privileged members + message_content intents (member tracking,
# anti-scam, AP-from-chat) plus the default set (guilds, messages, reactions,
# voice_states, ...).
intents = discord.Intents.default()
intents.members = True
intents.message_content = True


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
        description="Admin-only: sync slash commands to the current server (fast) and optionally global (slow).",
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

        # Sync to the guild you ran /sync in (prevents Missing Access issues)
        guild_obj = discord.Object(id=interaction.guild.id)

        try:
            if clean_guild:
                self.bot.tree.clear_commands(guild=guild_obj)

            self.bot.tree.copy_global_to(guild=guild_obj)
            synced = await self.bot.tree.sync(guild=guild_obj)
            msg = f"✅ Guild sync complete. Synced `{len(synced)}` command(s) to `{interaction.guild.id}`."

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
            # ---- Initialise the SQLite schema BEFORE loading cogs ----
            # All persisted state lives in the SQLite DB on the /data volume.
            # Create every table up front so no cog races to do it.
            try:
                from cogs import db
                db.init_db()
                print("[BOOT] SQLite schema initialised.")
            except Exception as e:
                print("[BOOT] FATAL: could not initialise the database. "
                      "Check the /data volume is mounted and writable.")
                traceback.print_exception(type(e), e, e.__traceback__)
                raise

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

            # ---- Load cogs (idempotent: skip if already loaded) ----
            loaded = []
            failed = []

            # Shared helper modules in cogs/ that are NOT Discord cogs
            # (imported via `from . import db`, no setup() entry point).
            NON_COG_MODULES = {"db", "uiutil"}

            for filename in sorted(os.listdir(cogs_dir)):
                if not filename.endswith(".py") or filename.startswith("__"):
                    continue
                if filename[:-3] in NON_COG_MODULES:
                    continue

                ext = f"cogs.{filename[:-3]}"
                print(f"[COGS] Attempting load: {ext}")

                try:
                    if ext in self.extensions:
                        print(f"[COGS] SKIP (already loaded): {ext}")
                        continue

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

            # ---- Add /sync (idempotent) ----
            try:
                if self.get_cog("SyncCog") is None:
                    await self.add_cog(SyncCog(self))
                    print("[BOOT] Loaded internal cog: SyncCog (/sync)")
                else:
                    print("[BOOT] SyncCog already loaded; skipping.")
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

            # ---- Guild sync (OPTIONAL; only if DEV_GUILD_ID is set & accessible) ----
            if DEV_GUILD_ID:
                guild_obj = self.get_guild(DEV_GUILD_ID)
                if guild_obj is None:
                    print(f"[SYNC] Skipping guild sync: bot cannot access guild {DEV_GUILD_ID} (wrong ID or bot not in server).")
                else:
                    try:
                        self.tree.copy_global_to(guild=guild_obj)
                        synced_guild = await self.tree.sync(guild=guild_obj)
                        print(f"[SYNC] Guild synced {len(synced_guild)} commands to {DEV_GUILD_ID}.")
                    except discord.Forbidden:
                        print(f"[SYNC] Guild sync forbidden for guild {DEV_GUILD_ID}: Missing Access (50001).")
                    except Exception as e:
                        print(f"[SYNC] Guild sync failed for guild {DEV_GUILD_ID}: {type(e).__name__}: {e}")
                        traceback.print_exception(type(e), e, e.__traceback__)
            else:
                print("[SYNC] Guild sync skipped (DEV_GUILD_ID not set).")

            # ---- Global sync (safe default) ----
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
    if not getattr(bot, "_trim_started", False):
        bot._trim_started = True
        bot.loop.create_task(_malloc_trim_loop())
        print("[BOOT] malloc_trim loop started (returns freed heap to the OS every 60s).")
    if os.getenv("MEM_PROBE") and not getattr(bot, "_memprobe_started", False):
        bot._memprobe_started = True
        bot.loop.create_task(_mem_probe(bot))
        print("[MEMPROBE] enabled (logging every 120s).")


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
