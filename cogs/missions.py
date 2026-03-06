"""
cogs/missions.py  —  Cryonic Gaming Bot
Mission management cog using SQLite stored on Railway /data volume.

Drop this file into your /cogs folder — bot.py will auto-load it.

Railway setup:
  - Add a Volume mounted at /data in your Railway service settings.
  - No extra env vars needed; DB path defaults to /data/missions.db
  - Locally, set MISSION_DB_PATH=./data/missions.db (created automatically)

Channel behaviour:
  - The bot posts missions to #eve-missions (defined by MISSION_CHANNEL_NAME).
  - If that channel is deleted, the bot recreates it automatically on next use.
  - Requires the "Manage Channels" permission in your server.

Access control:
  - Only "ARC Security Administration Council" and "ARC Security Corporation Leader"
    (plus server administrators) can create, cancel, or force-assign missions.
"""

import os
import sqlite3
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Railway volume is mounted at /data — fallback to ./data for local dev
_DB_DIR = Path(os.getenv("MISSION_DB_PATH", "/data/missions.db")).parent
_DB_PATH = Path(os.getenv("MISSION_DB_PATH", "/data/missions.db"))

# Roles allowed to create / manage missions.
# Administrators are always allowed regardless of roles.
MISSION_MANAGER_ROLES: set[str] = {
    "ARC Security Administration Council",
    "ARC Security Corporation Leader",
}

# The bot will look for a channel with this exact name.
# If it doesn't exist in the guild, it will be created automatically.
MISSION_CHANNEL_NAME: str = "eve-missions"


# ---------------------------------------------------------------------------
# Status colours for embeds
# ---------------------------------------------------------------------------

STATUS_COLOUR = {
    "open":        discord.Colour.blue(),
    "in_progress": discord.Colour.orange(),
    "completed":   discord.Colour.green(),
    "cancelled":   discord.Colour.red(),
}

STATUS_EMOJI = {
    "open":        "🔵",
    "in_progress": "🟠",
    "completed":   "✅",
    "cancelled":   "❌",
}


# ---------------------------------------------------------------------------
# Database helper
# ---------------------------------------------------------------------------

class MissionDB:
    """Thin synchronous SQLite wrapper (discord.py runs in asyncio but
    SQLite calls are fast enough for a bot — no aiosqlite needed)."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._init_schema()

    # ---- Internal ----

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")   # safer concurrent reads
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_schema(self):
        with self._connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS missions (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    title       TEXT    NOT NULL,
                    description TEXT    NOT NULL DEFAULT '',
                    reward      TEXT    NOT NULL DEFAULT '',
                    status      TEXT    NOT NULL DEFAULT 'open'
                                CHECK(status IN ('open','in_progress','completed','cancelled')),
                    created_by  INTEGER NOT NULL,   -- Discord user ID
                    assigned_to INTEGER,             -- Discord user ID (nullable)
                    guild_id    INTEGER NOT NULL,
                    created_at  TEXT    NOT NULL,
                    updated_at  TEXT    NOT NULL
                );
            """)

    # ---- CRUD ----

    def create_mission(
        self,
        title: str,
        description: str,
        reward: str,
        created_by: int,
        guild_id: int,
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                """INSERT INTO missions
                   (title, description, reward, created_by, guild_id, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (title, description, reward, created_by, guild_id, now, now),
            )
            return cur.lastrowid

    def get_mission(self, mission_id: int) -> Optional[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM missions WHERE id = ?", (mission_id,)
            ).fetchone()

    def list_missions(
        self,
        guild_id: int,
        status: Optional[str] = None,
    ) -> list[sqlite3.Row]:
        with self._connect() as conn:
            if status:
                return conn.execute(
                    "SELECT * FROM missions WHERE guild_id=? AND status=? ORDER BY id DESC",
                    (guild_id, status),
                ).fetchall()
            return conn.execute(
                "SELECT * FROM missions WHERE guild_id=? AND status != 'cancelled' ORDER BY id DESC",
                (guild_id,),
            ).fetchall()

    def assign_mission(self, mission_id: int, user_id: int) -> bool:
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                """UPDATE missions SET assigned_to=?, status='in_progress', updated_at=?
                   WHERE id=? AND status='open'""",
                (user_id, now, mission_id),
            )
            return cur.rowcount > 0

    def complete_mission(self, mission_id: int, user_id: int) -> bool:
        """Only the assignee (or an admin via the cog) can complete."""
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                """UPDATE missions SET status='completed', updated_at=?
                   WHERE id=? AND assigned_to=? AND status='in_progress'""",
                (now, mission_id, user_id),
            )
            return cur.rowcount > 0

    def cancel_mission(self, mission_id: int) -> bool:
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                """UPDATE missions SET status='cancelled', updated_at=?
                   WHERE id=? AND status NOT IN ('completed','cancelled')""",
                (now, mission_id),
            )
            return cur.rowcount > 0


# ---------------------------------------------------------------------------
# Permission helper
# ---------------------------------------------------------------------------

def can_manage_missions(member: discord.Member) -> bool:
    if member.guild_permissions.administrator:
        return True
    if MISSION_MANAGER_ROLES:
        return any(r.name in MISSION_MANAGER_ROLES for r in member.roles)
    return False


# ---------------------------------------------------------------------------
# Embed builder
# ---------------------------------------------------------------------------

def mission_embed(row: sqlite3.Row, guild: discord.Guild) -> discord.Embed:
    status = row["status"]
    embed = discord.Embed(
        title=f"{STATUS_EMOJI.get(status, '❓')}  Mission #{row['id']} — {row['title']}",
        description=row["description"] or "*No description provided.*",
        colour=STATUS_COLOUR.get(status, discord.Colour.default()),
    )
    embed.add_field(name="💰 Reward", value=row["reward"] or "*None specified*", inline=True)
    embed.add_field(name="📊 Status", value=status.replace("_", " ").title(), inline=True)

    # Try to show display names from the guild's member cache
    creator = guild.get_member(row["created_by"])
    embed.add_field(
        name="📋 Created by",
        value=creator.display_name if creator else f"<@{row['created_by']}>",
        inline=True,
    )

    if row["assigned_to"]:
        assignee = guild.get_member(row["assigned_to"])
        embed.add_field(
            name="👤 Assigned to",
            value=assignee.display_name if assignee else f"<@{row['assigned_to']}>",
            inline=True,
        )

    embed.set_footer(text=f"Created {row['created_at'][:10]}  •  Last update {row['updated_at'][:10]}")
    return embed


# ---------------------------------------------------------------------------
# Modal — create mission form
# ---------------------------------------------------------------------------

class MissionCreateModal(discord.ui.Modal, title="Create a New Mission"):
    mission_title = discord.ui.TextInput(
        label="Mission Title",
        placeholder="e.g. Patrol the Jita undock",
        max_length=100,
    )
    description = discord.ui.TextInput(
        label="Description",
        style=discord.TextStyle.paragraph,
        placeholder="Describe the mission objectives…",
        max_length=1000,
        required=False,
    )
    reward = discord.ui.TextInput(
        label="Reward (ISK / items)",
        placeholder="e.g. 500M ISK + faction ammo",
        max_length=200,
        required=False,
    )

    def __init__(self, db: MissionDB, post_channel: Optional[discord.TextChannel]):
        super().__init__()
        self.db = db
        self.post_channel = post_channel

    async def on_submit(self, interaction: discord.Interaction):
        mission_id = self.db.create_mission(
            title=self.mission_title.value.strip(),
            description=self.description.value.strip(),
            reward=self.reward.value.strip(),
            created_by=interaction.user.id,
            guild_id=interaction.guild.id,
        )
        row = self.db.get_mission(mission_id)
        embed = mission_embed(row, interaction.guild)
        embed.set_author(name="✨ New mission created!")

        await interaction.response.send_message(embed=embed, ephemeral=False)

        # Also post to the dedicated mission channel if configured
        if self.post_channel and self.post_channel.id != interaction.channel_id:
            try:
                await self.post_channel.send(embed=embed)
            except discord.Forbidden:
                pass  # bot lacks perms — silent fail

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        traceback.print_exception(type(error), error, error.__traceback__)
        await interaction.response.send_message(
            "❌ Failed to create mission. Check bot logs.", ephemeral=True
        )


# ---------------------------------------------------------------------------
# Cog
# ---------------------------------------------------------------------------

class MissionCog(commands.Cog, name="Missions"):
    """EVE Online mission management — create, assign, complete, list."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db = MissionDB(_DB_PATH)
        print(f"[MissionCog] DB ready at {_DB_PATH}")

    # ------------------------------------------------------------------
    # Channel helper — finds #eve-missions by name, creates it if missing
    # ------------------------------------------------------------------
    async def _ensure_mission_channel(self, guild: discord.Guild) -> discord.TextChannel:
        """Return the #eve-missions channel, creating it (with a topic) if it was deleted."""
        # Search by name first
        existing = discord.utils.get(guild.text_channels, name=MISSION_CHANNEL_NAME)
        if existing:
            return existing

        # Channel is missing — recreate it
        print(f"[MissionCog] #{MISSION_CHANNEL_NAME} not found in {guild.name}, creating it…")
        try:
            channel = await guild.create_text_channel(
                name=MISSION_CHANNEL_NAME,
                topic="📋 EVE Online mission board — managed by the bot. Do not delete this channel.",
                reason="MissionCog: auto-recreated missing mission channel.",
            )
            print(f"[MissionCog] Created #{MISSION_CHANNEL_NAME} (id={channel.id}) in {guild.name}.")
            return channel
        except discord.Forbidden:
            print(f"[MissionCog] WARNING: Missing 'Manage Channels' permission in {guild.name}.")
            raise
        except Exception as e:
            print(f"[MissionCog] ERROR creating channel: {e}")
            raise

    # ------------------------------------------------------------------ /mission create
    @app_commands.command(name="mission_create", description="Create a new EVE mission (opens a form).")
    async def mission_create(self, interaction: discord.Interaction):
        if not can_manage_missions(interaction.user):
            await interaction.response.send_message(
                "🔒 You need the **ARC Security Administration Council** or "
                "**ARC Security Corporation Leader** role to create missions.",
                ephemeral=True,
            )
            return

        try:
            post_channel = await self._ensure_mission_channel(interaction.guild)
        except discord.Forbidden:
            await interaction.response.send_message(
                "⚠️ I'm missing the **Manage Channels** permission — "
                f"please recreate `#{MISSION_CHANNEL_NAME}` manually or grant me that permission.",
                ephemeral=True,
            )
            return

        modal = MissionCreateModal(self.db, post_channel)
        await interaction.response.send_modal(modal)

    # ------------------------------------------------------------------ /mission list
    @app_commands.command(name="mission_list", description="List active missions.")
    @app_commands.describe(status="Filter by status (default: all active)")
    @app_commands.choices(status=[
        app_commands.Choice(name="Open",        value="open"),
        app_commands.Choice(name="In Progress", value="in_progress"),
        app_commands.Choice(name="Completed",   value="completed"),
        app_commands.Choice(name="All active",  value="__all__"),
    ])
    async def mission_list(
        self,
        interaction: discord.Interaction,
        status: app_commands.Choice[str] = None,
    ):
        filter_status = None if (status is None or status.value == "__all__") else status.value
        rows = self.db.list_missions(interaction.guild.id, filter_status)

        if not rows:
            await interaction.response.send_message(
                "📭 No missions found for this filter.", ephemeral=True
            )
            return

        # Show up to 10 missions in a single response
        embeds = [mission_embed(r, interaction.guild) for r in rows[:10]]
        header = f"📋 **{len(rows)} mission(s) found**" + (
            f" — showing first 10" if len(rows) > 10 else ""
        )
        await interaction.response.send_message(content=header, embeds=embeds, ephemeral=True)

    # ------------------------------------------------------------------ /mission assign
    @app_commands.command(name="mission_assign", description="Assign yourself (or another pilot) to an open mission.")
    @app_commands.describe(mission_id="The mission ID to assign", pilot="Leave empty to assign yourself")
    async def mission_assign(
        self,
        interaction: discord.Interaction,
        mission_id: int,
        pilot: Optional[discord.Member] = None,
    ):
        target = pilot or interaction.user

        # Only managers can assign to other people
        if pilot and not can_manage_missions(interaction.user):
            await interaction.response.send_message(
                "🔒 Only **ARC Security Administration Council** or "
                "**ARC Security Corporation Leader** can assign missions to other pilots.",
                ephemeral=True,
            )
            return

        success = self.db.assign_mission(mission_id, target.id)
        if not success:
            row = self.db.get_mission(mission_id)
            if row is None:
                msg = f"❌ Mission `#{mission_id}` not found."
            else:
                msg = f"❌ Mission `#{mission_id}` is **{row['status']}** and cannot be assigned."
            await interaction.response.send_message(msg, ephemeral=True)
            return

        row = self.db.get_mission(mission_id)
        embed = mission_embed(row, interaction.guild)
        embed.set_author(name=f"👤 {target.display_name} assigned to mission!")
        await interaction.response.send_message(embed=embed)

    # ------------------------------------------------------------------ /mission complete
    @app_commands.command(name="mission_complete", description="Mark your assigned mission as completed.")
    @app_commands.describe(mission_id="The mission ID you completed")
    async def mission_complete(self, interaction: discord.Interaction, mission_id: int):
        # Admins/managers can complete any mission; others only their own
        row = self.db.get_mission(mission_id)
        if row is None:
            await interaction.response.send_message(f"❌ Mission `#{mission_id}` not found.", ephemeral=True)
            return

        is_manager = can_manage_missions(interaction.user)
        is_assignee = row["assigned_to"] == interaction.user.id

        if not is_manager and not is_assignee:
            await interaction.response.send_message(
                "🔒 You can only complete missions assigned to you.", ephemeral=True
            )
            return

        # For managers, force-complete regardless of assignee
        if is_manager and not is_assignee:
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc).isoformat()
            import sqlite3 as _sq
            with self.db._connect() as conn:
                conn.execute(
                    "UPDATE missions SET status='completed', updated_at=? WHERE id=?",
                    (now, mission_id),
                )
        else:
            self.db.complete_mission(mission_id, interaction.user.id)

        row = self.db.get_mission(mission_id)
        embed = mission_embed(row, interaction.guild)
        embed.set_author(name="✅ Mission completed!")
        await interaction.response.send_message(embed=embed)

    # ------------------------------------------------------------------ /mission cancel
    @app_commands.command(name="mission_cancel", description="Cancel a mission (managers only).")
    @app_commands.describe(mission_id="The mission ID to cancel")
    async def mission_cancel(self, interaction: discord.Interaction, mission_id: int):
        if not can_manage_missions(interaction.user):
            await interaction.response.send_message(
                "🔒 Only **ARC Security Administration Council** or "
                "**ARC Security Corporation Leader** can cancel missions.",
                ephemeral=True,
            )
            return

        success = self.db.cancel_mission(mission_id)
        if not success:
            row = self.db.get_mission(mission_id)
            if row is None:
                msg = f"❌ Mission `#{mission_id}` not found."
            else:
                msg = f"❌ Mission `#{mission_id}` is already **{row['status']}**."
            await interaction.response.send_message(msg, ephemeral=True)
            return

        row = self.db.get_mission(mission_id)
        embed = mission_embed(row, interaction.guild)
        embed.set_author(name="❌ Mission cancelled.")
        await interaction.response.send_message(embed=embed)

    # ------------------------------------------------------------------ /mission info
    @app_commands.command(name="mission_info", description="Show details for a specific mission.")
    @app_commands.describe(mission_id="The mission ID to inspect")
    async def mission_info(self, interaction: discord.Interaction, mission_id: int):
        row = self.db.get_mission(mission_id)
        if row is None:
            await interaction.response.send_message(f"❌ Mission `#{mission_id}` not found.", ephemeral=True)
            return
        embed = mission_embed(row, interaction.guild)
        await interaction.response.send_message(embed=embed, ephemeral=True)


# ---------------------------------------------------------------------------
# Required by discord.py cog loader
# ---------------------------------------------------------------------------

async def setup(bot: commands.Bot):
    await bot.add_cog(MissionCog(bot))
    print("[MissionCog] Cog registered.")
