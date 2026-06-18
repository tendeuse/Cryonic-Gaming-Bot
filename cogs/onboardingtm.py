# cogs/onboardingtm.py
#
# Onboarding Ticket Monitor
# =========================
# Watches the Tickety ticket threads opened under a fixed parent channel and
# posts escalating notices against a 15-day completion deadline:
#
#   Day  8  — First Notice
#   Day 12  — Second Notice (deadline approaching)
#   Day 15+ — Final Notice  (pings the responsible admin)
#
# Tickets are tracked AUTOMATICALLY:
#   • A thread created under PARENT_CHANNEL_ID is registered on creation.
#   • Threads opened while the bot was offline are picked up by a startup scan.
#   • A thread that is deleted, locked, or archived is auto-untracked
#     (the usual ways a ticket bot signals a close).
#
# Each notice fires exactly once per ticket. Notices use independent checks, so
# a notice that became due while the bot was offline is still sent on the next
# pass instead of being skipped.
#
# Manual fallbacks (admins / approval roles):
#   /onboarding_open   <thread_id>  — manually start tracking a thread
#   /onboarding_close  <thread_id>  — manually stop tracking a thread
#   /onboarding_list                — list every tracked ticket

import asyncio
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

import discord
from discord.ext import commands, tasks
from discord import app_commands

# ============================================================
# CONFIG
# ============================================================

# Parent channel whose threads are Tickety onboarding tickets.
PARENT_CHANNEL_ID = 1463203696417706076

# Channel where all onboarding notices are posted.
WARNING_CHANNEL_ID = 1448591790860144681

# Responsible admin (new ARC Ainu / Broen) — pinged on the Day 15 final notice.
ADMIN_ID = 415519873972699147

# Deadline / notice thresholds (days from the thread's creation).
# The ticket deadline is 15 days; the final admin notice fires on day 14.
DEADLINE_DAYS = 15
DAY8_THRESHOLD = 8
DAY12_THRESHOLD = 12
FINAL_NOTICE_DAY = 14

CHECK_INTERVAL_HOURS = 1

# Roles with admin rights over the onboarding monitor (manage the commands).
# These roles are NEVER pinged by the cog — admin rights only, no notifications.
# Server administrators are also always allowed.
ADMIN_ROLES: set[str] = {
    "ARC Security Corporation Leader",
    "ARC Security Administration Council",
}

# ============================================================
# PATHS
# ============================================================

DATA_DIR = Path(os.getenv("PERSIST_ROOT", "/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_FILE = DATA_DIR / "onboarding_tickets.db"

# ============================================================
# GENERIC UTILITIES
# ============================================================

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def has_any_role(member: discord.Member, names: set[str]) -> bool:
    return any(r.name in names for r in getattr(member, "roles", []))

def can_manage(member: discord.Member) -> bool:
    return member.guild_permissions.administrator or has_any_role(member, ADMIN_ROLES)

# ============================================================
# SQLITE  (ticket database)
# ============================================================

def _db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_FILE)
    con.row_factory = sqlite3.Row
    return con

def init_db() -> None:
    with _db() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS tickets (
                ticket_id  TEXT PRIMARY KEY,
                name       TEXT NOT NULL DEFAULT '',
                open_time  TEXT NOT NULL,
                end_time   TEXT NOT NULL,
                day8_sent  INTEGER NOT NULL DEFAULT 0,
                day12_sent INTEGER NOT NULL DEFAULT 0,
                day15_sent INTEGER NOT NULL DEFAULT 0
            )
        """)
        # Forward-compat migration for older DBs without the name column.
        cols = {r["name"] for r in con.execute("PRAGMA table_info(tickets)").fetchall()}
        if "name" not in cols:
            con.execute("ALTER TABLE tickets ADD COLUMN name TEXT NOT NULL DEFAULT ''")

def _register_ticket_sync(ticket_id: str, name: str, open_time: str, end_time: str) -> bool:
    """Insert a ticket. Returns True if newly inserted, False if it already existed."""
    with _db() as con:
        cur = con.execute(
            "INSERT OR IGNORE INTO tickets (ticket_id, name, open_time, end_time) VALUES (?, ?, ?, ?)",
            (ticket_id, name, open_time, end_time),
        )
        return cur.rowcount > 0

def _close_ticket_sync(ticket_id: str) -> bool:
    """Returns True if a row was removed."""
    with _db() as con:
        cur = con.execute("DELETE FROM tickets WHERE ticket_id = ?", (ticket_id,))
        return cur.rowcount > 0

def _list_tickets_sync() -> List[dict]:
    with _db() as con:
        rows = con.execute(
            "SELECT ticket_id, name, open_time, end_time, day8_sent, day12_sent, day15_sent "
            "FROM tickets ORDER BY open_time ASC"
        ).fetchall()
        return [dict(r) for r in rows]

def _mark_sent_sync(ticket_id: str, column: str) -> None:
    # column is only ever passed from a fixed allow-list below.
    with _db() as con:
        con.execute(f"UPDATE tickets SET {column} = 1 WHERE ticket_id = ?", (ticket_id,))

# Async wrappers (keep the event loop unblocked, matching the other cogs).

async def register_ticket(ticket_id: str, name: str, open_time: str, end_time: str) -> bool:
    return await asyncio.to_thread(_register_ticket_sync, ticket_id, name, open_time, end_time)

async def close_ticket(ticket_id: str) -> bool:
    return await asyncio.to_thread(_close_ticket_sync, ticket_id)

async def list_tickets() -> List[dict]:
    return await asyncio.to_thread(_list_tickets_sync)

async def mark_sent(ticket_id: str, column: str) -> None:
    await asyncio.to_thread(_mark_sent_sync, ticket_id, column)

# ============================================================
# COG
# ============================================================

class OnboardingTMCog(commands.Cog):

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        init_db()

    # ── Lifecycle ──────────────────────────────────────────────────────────

    async def cog_load(self):
        self.ticket_checker_loop.start()
        self.bot.loop.create_task(self._post_ready_reconcile())

    async def cog_unload(self):
        self.ticket_checker_loop.cancel()

    async def _post_ready_reconcile(self):
        """Register any active ticket threads opened while the bot was offline."""
        await self.bot.wait_until_ready()
        parent = self.bot.get_channel(PARENT_CHANNEL_ID)
        if not isinstance(parent, (discord.TextChannel, discord.ForumChannel)):
            print(f"[onboardingtm] Parent channel {PARENT_CHANNEL_ID} not found or not thread-capable.")
            return

        # Cached active threads + a live fetch to be safe.
        threads = list(parent.threads)
        try:
            active = await parent.guild.active_threads()
            threads += [t for t in active if t.parent_id == PARENT_CHANNEL_ID]
        except Exception:
            pass

        seen: set[int] = set()
        for thread in threads:
            if thread.id in seen:
                continue
            seen.add(thread.id)
            if thread.locked:  # already closed by Tickety — don't re-track
                continue
            await self._register_thread(thread)

    # ── Auto-tracking via thread events ────────────────────────────────────

    async def _register_thread(self, thread: discord.Thread) -> bool:
        created = thread.created_at or utcnow()
        end_time = created + timedelta(days=DEADLINE_DAYS)
        return await register_ticket(
            str(thread.id),
            thread.name or "",
            created.isoformat(),
            end_time.strftime("%Y-%m-%d %H:%M UTC"),
        )

    @commands.Cog.listener()
    async def on_thread_create(self, thread: discord.Thread):
        if thread.parent_id != PARENT_CHANNEL_ID:
            return
        await self._register_thread(thread)

    @commands.Cog.listener()
    async def on_thread_update(self, before: discord.Thread, after: discord.Thread):
        if after.parent_id != PARENT_CHANNEL_ID:
            return
        # Tickety locks the thread when a ticket is closed. Key off the lock
        # transition specifically — NOT archive, since Discord auto-archives
        # idle threads (which would untrack an open ticket prematurely).
        if after.locked and not before.locked:
            await close_ticket(str(after.id))

    @commands.Cog.listener()
    async def on_thread_delete(self, thread: discord.Thread):
        if thread.parent_id != PARENT_CHANNEL_ID:
            return
        await close_ticket(str(thread.id))

    @commands.Cog.listener()
    async def on_raw_thread_delete(self, payload: discord.RawThreadDeleteEvent):
        if payload.parent_id != PARENT_CHANNEL_ID:
            return
        await close_ticket(str(payload.thread_id))

    # ── Embed builder ──────────────────────────────────────────────────────

    def _notice_embed(
        self,
        *,
        title: str,
        color: discord.Color,
        ticket_id: str,
        end_time: str,
        body: str,
    ) -> discord.Embed:
        emb = discord.Embed(title=title, description=body, color=color, timestamp=utcnow())
        emb.add_field(name="Thread / Ticket", value=f"<#{ticket_id}>", inline=True)
        emb.add_field(name="Target Completion (UTC)", value=end_time, inline=True)
        emb.set_footer(text="Cryonic Gaming bot — Onboarding Ticket Monitor")
        return emb

    # ── Slash commands (manual fallbacks) ──────────────────────────────────

    @app_commands.command(
        name="onboarding_open",
        description="Manually register an onboarding thread for 15-day deadline tracking.",
    )
    @app_commands.describe(thread="The ticket thread to start tracking.")
    async def onboarding_open(self, interaction: discord.Interaction, thread: discord.Thread):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
            return
        if not can_manage(interaction.user):
            await interaction.response.send_message("You do not have permission to manage onboarding tickets.", ephemeral=True)
            return

        inserted = await self._register_thread(thread)
        if not inserted:
            await interaction.response.send_message(
                f"⚠️ {thread.mention} is already being tracked.", ephemeral=True
            )
            return

        end_time = (thread.created_at or utcnow()) + timedelta(days=DEADLINE_DAYS)
        await interaction.response.send_message(
            f"✅ {thread.mention} is now being tracked.\n"
            f"📅 Expected Completion Date: **{end_time.strftime('%Y-%m-%d %H:%M UTC')}**",
            ephemeral=True,
        )

    @app_commands.command(
        name="onboarding_close",
        description="Manually stop tracking an onboarding thread.",
    )
    @app_commands.describe(thread="The ticket thread to stop tracking.")
    async def onboarding_close(self, interaction: discord.Interaction, thread: discord.Thread):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
            return
        if not can_manage(interaction.user):
            await interaction.response.send_message("You do not have permission to manage onboarding tickets.", ephemeral=True)
            return

        removed = await close_ticket(str(thread.id))
        if not removed:
            await interaction.response.send_message(
                f"⚠️ {thread.mention} was not being tracked.", ephemeral=True
            )
            return

        await interaction.response.send_message(
            f"🔒 {thread.mention} has been removed from tracking.", ephemeral=True
        )

    @app_commands.command(
        name="onboarding_list",
        description="List every onboarding thread currently being tracked.",
    )
    async def onboarding_list(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
            return
        if not can_manage(interaction.user):
            await interaction.response.send_message("You do not have permission to view onboarding tickets.", ephemeral=True)
            return

        tickets = await list_tickets()
        if not tickets:
            await interaction.response.send_message("No onboarding tickets are currently being tracked.", ephemeral=True)
            return

        now = utcnow()
        lines = []
        for t in tickets:
            try:
                days_open = (now - datetime.fromisoformat(t["open_time"])).days
            except ValueError:
                days_open = 0
            flags = [f for f, on in (("D8", t["day8_sent"]), ("D12", t["day12_sent"]), ("D15", t["day15_sent"])) if on]
            flag_str = f" [{', '.join(flags)}]" if flags else ""
            lines.append(f"• <#{t['ticket_id']}> — {days_open}d open — due {t['end_time']}{flag_str}")

        emb = discord.Embed(
            title="📋 Tracked Onboarding Tickets",
            description="\n".join(lines),
            color=discord.Color.blurple(),
            timestamp=now,
        )
        emb.set_footer(text="Cryonic Gaming bot — Onboarding Ticket Monitor")
        await interaction.response.send_message(embed=emb, ephemeral=True)

    # ── Background monitor ─────────────────────────────────────────────────

    @tasks.loop(hours=CHECK_INTERVAL_HOURS)
    async def ticket_checker_loop(self):
        channel = self.bot.get_channel(WARNING_CHANNEL_ID)
        if channel is None:
            print(f"[onboardingtm] Warning channel {WARNING_CHANNEL_ID} not found; skipping pass.")
            return

        tickets = await list_tickets()
        now = utcnow()

        for t in tickets:
            ticket_id = t["ticket_id"]
            end_time = t["end_time"]
            try:
                open_time = datetime.fromisoformat(t["open_time"])
            except ValueError:
                continue
            days_open = (now - open_time).days

            # ---- DAY 8 — First Notice ----
            if days_open >= DAY8_THRESHOLD and not t["day8_sent"]:
                emb = self._notice_embed(
                    title="⏰ Day 8 Onboarding Reminder",
                    color=discord.Color.gold(),
                    ticket_id=ticket_id,
                    end_time=end_time,
                    body=(
                        "Morning\n\n"
                        f"This is your Day 8 reminder to complete your onboarding ticket before **{end_time}**.\n\n"
                        "Please make sure all required steps are finished by the deadline.\n\n"
                        "Thank you for your attention to this matter, and fly safe among the stars!"
                    ),
                )
                await channel.send(embed=emb)
                await mark_sent(ticket_id, "day8_sent")

            # ---- DAY 12 — Second Notice ----
            if days_open >= DAY12_THRESHOLD and not t["day12_sent"]:
                emb = self._notice_embed(
                    title="⚠️ Second Notice — 12 Days Open",
                    color=discord.Color.orange(),
                    ticket_id=ticket_id,
                    end_time=end_time,
                    body=f"Onboarding ticket <#{ticket_id}> has reached **12 days**. Deadline approaching!",
                )
                await channel.send(embed=emb)
                await mark_sent(ticket_id, "day12_sent")

            # ---- DAY 14 — Final Notice (admin ping, day before deadline) ----
            if days_open >= FINAL_NOTICE_DAY and not t["day15_sent"]:
                emb = self._notice_embed(
                    title="🚨 Final Notice — Deadline Approaching",
                    color=discord.Color.red(),
                    ticket_id=ticket_id,
                    end_time=end_time,
                    body=(
                        f"<@{ADMIN_ID}> (new ARC Ainu / Broen), onboarding ticket <#{ticket_id}> "
                        f"has been open **{days_open} days** — the 15-day deadline ({end_time}) is nearly up."
                    ),
                )
                await channel.send(
                    content=f"<@{ADMIN_ID}>",
                    embed=emb,
                    allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
                )
                await mark_sent(ticket_id, "day15_sent")

    @ticket_checker_loop.before_loop
    async def before_ticket_checker_loop(self):
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot):
    await bot.add_cog(OnboardingTMCog(bot))
