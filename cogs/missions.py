"""
cogs/missions.py  —  Cryonic Gaming Bot
Mission management cog using SQLite stored on Railway /data volume.

Drop this file into your /cogs folder — bot.py will auto-load it.

Railway setup:
  - Add a Volume mounted at /data in your Railway service settings.
  - DB path defaults to /data/missions.db
  - Locally, set MISSION_DB_PATH=./data/missions.db (auto-created)

Channel behaviour:
  - #mission-control  → persistent button panel, recreated if deleted/missing
  - #eve-missions     → mission post feed, recreated if deleted/missing
  - Both channels are created automatically on bot ready or if deleted.
  - Requires "Manage Channels" permission in your server.

Access control:
  - "ARC Security Administration Council" and "ARC Security Corporation Leader"
    (plus server administrators) can create, cancel, and force-manage missions.
  - All members can assign themselves and complete their own missions.
"""

import os
import sqlite3
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import discord
from discord.ext import commands


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_DB_PATH = Path(os.getenv("MISSION_DB_PATH", "/data/missions.db"))

MISSION_MANAGER_ROLES: set[str] = {
    "ARC Security Administration Council",
    "ARC Security Corporation Leader",
}

MISSION_CONTROL_CHANNEL = "mission-control"   # button panel
MISSION_FEED_CHANNEL    = "eve-missions"       # mission post feed


# ---------------------------------------------------------------------------
# Status helpers
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
# Permission helper
# ---------------------------------------------------------------------------

def can_manage_missions(member: discord.Member) -> bool:
    if member.guild_permissions.administrator:
        return True
    return any(r.name in MISSION_MANAGER_ROLES for r in member.roles)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

class MissionDB:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
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
                    created_by  INTEGER NOT NULL,
                    assigned_to INTEGER,
                    guild_id    INTEGER NOT NULL,
                    created_at  TEXT    NOT NULL,
                    updated_at  TEXT    NOT NULL
                );
            """)

    def create_mission(self, title, description, reward, created_by, guild_id) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                "INSERT INTO missions (title,description,reward,created_by,guild_id,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (title, description, reward, created_by, guild_id, now, now),
            )
            return cur.lastrowid

    def get_mission(self, mission_id: int) -> Optional[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute("SELECT * FROM missions WHERE id=?", (mission_id,)).fetchone()

    def list_missions(self, guild_id: int, status: Optional[str] = None) -> list:
        with self._connect() as conn:
            if status:
                return conn.execute(
                    "SELECT * FROM missions WHERE guild_id=? AND status=? ORDER BY id DESC",
                    (guild_id, status),
                ).fetchall()
            return conn.execute(
                "SELECT * FROM missions WHERE guild_id=? AND status!='cancelled' ORDER BY id DESC",
                (guild_id,),
            ).fetchall()

    def assign_mission(self, mission_id: int, user_id: int) -> bool:
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE missions SET assigned_to=?,status='in_progress',updated_at=? "
                "WHERE id=? AND status='open'",
                (user_id, now, mission_id),
            )
            return cur.rowcount > 0

    def complete_mission(self, mission_id: int, user_id: int, force: bool = False) -> bool:
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            if force:
                cur = conn.execute(
                    "UPDATE missions SET status='completed',updated_at=? "
                    "WHERE id=? AND status='in_progress'",
                    (now, mission_id),
                )
            else:
                cur = conn.execute(
                    "UPDATE missions SET status='completed',updated_at=? "
                    "WHERE id=? AND assigned_to=? AND status='in_progress'",
                    (now, mission_id, user_id),
                )
            return cur.rowcount > 0

    def cancel_mission(self, mission_id: int) -> bool:
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE missions SET status='cancelled',updated_at=? "
                "WHERE id=? AND status NOT IN ('completed','cancelled')",
                (now, mission_id),
            )
            return cur.rowcount > 0


# ---------------------------------------------------------------------------
# Embed builder
# ---------------------------------------------------------------------------

def mission_embed(row: sqlite3.Row, guild: discord.Guild) -> discord.Embed:
    status = row["status"]
    embed  = discord.Embed(
        title=f"{STATUS_EMOJI.get(status,'❓')}  Mission #{row['id']} — {row['title']}",
        description=row["description"] or "*No description provided.*",
        colour=STATUS_COLOUR.get(status, discord.Colour.default()),
    )
    embed.add_field(name="💰 Reward", value=row["reward"] or "*None*", inline=True)
    embed.add_field(name="📊 Status", value=status.replace("_", " ").title(), inline=True)

    creator  = guild.get_member(row["created_by"])
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
    embed.set_footer(text=f"Created {row['created_at'][:10]}  •  Updated {row['updated_at'][:10]}")
    return embed


# ---------------------------------------------------------------------------
# Modals
# ---------------------------------------------------------------------------

class MissionCreateModal(discord.ui.Modal, title="✨ Create a New Mission"):
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

    def __init__(self, db: MissionDB, feed_channel: Optional[discord.TextChannel]):
        super().__init__()
        self.db           = db
        self.feed_channel = feed_channel

    async def on_submit(self, interaction: discord.Interaction):
        mid   = self.db.create_mission(
            title=self.mission_title.value.strip(),
            description=self.description.value.strip(),
            reward=self.reward.value.strip(),
            created_by=interaction.user.id,
            guild_id=interaction.guild.id,
        )
        row   = self.db.get_mission(mid)
        embed = mission_embed(row, interaction.guild)
        embed.set_author(name="✨ New mission created!")
        await interaction.response.send_message(embed=embed, ephemeral=True)
        if self.feed_channel:
            try:
                await self.feed_channel.send(embed=embed)
            except discord.Forbidden:
                pass

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        traceback.print_exception(type(error), error, error.__traceback__)
        await interaction.response.send_message("❌ Failed to create mission.", ephemeral=True)


class MissionIDModal(discord.ui.Modal):
    mission_id = discord.ui.TextInput(
        label="Mission ID",
        placeholder="e.g. 42",
        min_length=1,
        max_length=6,
    )

    def __init__(self, title: str, db: MissionDB, action: str,
                 feed_channel: Optional[discord.TextChannel]):
        super().__init__(title=title)
        self.db           = db
        self.action       = action   # "assign" | "complete" | "cancel" | "info"
        self.feed_channel = feed_channel

    async def on_submit(self, interaction: discord.Interaction):
        try:
            mid = int(self.mission_id.value.strip())
        except ValueError:
            await interaction.response.send_message("❌ Please enter a valid numeric ID.", ephemeral=True)
            return

        row    = self.db.get_mission(mid)
        member = interaction.user

        if row is None:
            await interaction.response.send_message(f"❌ Mission `#{mid}` not found.", ephemeral=True)
            return

        # ---- ASSIGN ------------------------------------------------
        if self.action == "assign":
            ok = self.db.assign_mission(mid, member.id)
            if not ok:
                await interaction.response.send_message(
                    f"❌ Mission `#{mid}` is **{row['status']}** and cannot be assigned.",
                    ephemeral=True)
                return
            row   = self.db.get_mission(mid)
            embed = mission_embed(row, interaction.guild)
            embed.set_author(name=f"👤 {member.display_name} assigned to mission!")
            await interaction.response.send_message(embed=embed, ephemeral=True)
            if self.feed_channel:
                try: await self.feed_channel.send(embed=embed)
                except discord.Forbidden: pass

        # ---- COMPLETE ----------------------------------------------
        elif self.action == "complete":
            is_manager  = can_manage_missions(member)
            is_assignee = row["assigned_to"] == member.id
            if not is_manager and not is_assignee:
                await interaction.response.send_message(
                    "🔒 You can only complete missions assigned to you.", ephemeral=True)
                return
            ok = self.db.complete_mission(mid, member.id, force=is_manager)
            if not ok:
                await interaction.response.send_message(
                    f"❌ Mission `#{mid}` cannot be completed (status: **{row['status']}**).",
                    ephemeral=True)
                return
            row   = self.db.get_mission(mid)
            embed = mission_embed(row, interaction.guild)
            embed.set_author(name="✅ Mission completed!")
            await interaction.response.send_message(embed=embed, ephemeral=True)
            if self.feed_channel:
                try: await self.feed_channel.send(embed=embed)
                except discord.Forbidden: pass

        # ---- CANCEL ------------------------------------------------
        elif self.action == "cancel":
            if not can_manage_missions(member):
                await interaction.response.send_message(
                    "🔒 Only **ARC Security Administration Council** or "
                    "**ARC Security Corporation Leader** can cancel missions.",
                    ephemeral=True)
                return
            ok = self.db.cancel_mission(mid)
            if not ok:
                await interaction.response.send_message(
                    f"❌ Mission `#{mid}` is already **{row['status']}**.", ephemeral=True)
                return
            row   = self.db.get_mission(mid)
            embed = mission_embed(row, interaction.guild)
            embed.set_author(name="❌ Mission cancelled.")
            await interaction.response.send_message(embed=embed, ephemeral=True)
            if self.feed_channel:
                try: await self.feed_channel.send(embed=embed)
                except discord.Forbidden: pass

        # ---- INFO --------------------------------------------------
        elif self.action == "info":
            embed = mission_embed(row, interaction.guild)
            await interaction.response.send_message(embed=embed, ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        traceback.print_exception(type(error), error, error.__traceback__)
        await interaction.response.send_message("❌ Action failed. Check bot logs.", ephemeral=True)


# ---------------------------------------------------------------------------
# Persistent Button Panel  (custom_id prefix: "mc_")
# Timeout=None + persistent custom_ids = survives bot restarts
# ---------------------------------------------------------------------------

class MissionControlView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    def _cog(self, interaction: discord.Interaction) -> "MissionCog":
        return interaction.client.get_cog("Missions")

    async def _feed(self, interaction: discord.Interaction) -> Optional[discord.TextChannel]:
        cog = self._cog(interaction)
        return await cog._ensure_channel(
            interaction.guild, MISSION_FEED_CHANNEL,
            "📡 EVE Online mission feed — managed by the bot. Do not delete.",
        )

    # ---------------------------------------------------------------- CREATE
    @discord.ui.button(label="✨ Create Mission", style=discord.ButtonStyle.success,
                       custom_id="mc_create", row=0)
    async def btn_create(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not can_manage_missions(interaction.user):
            await interaction.response.send_message(
                "🔒 You need the **ARC Security Administration Council** or "
                "**ARC Security Corporation Leader** role to create missions.",
                ephemeral=True)
            return
        try:
            feed = await self._feed(interaction)
        except discord.Forbidden:
            await interaction.response.send_message(
                f"⚠️ I'm missing **Manage Channels** permission — "
                f"please create `#{MISSION_FEED_CHANNEL}` manually.", ephemeral=True)
            return
        await interaction.response.send_modal(
            MissionCreateModal(self._cog(interaction).db, feed))

    # ---------------------------------------------------------------- LIST
    @discord.ui.button(label="📋 List Missions", style=discord.ButtonStyle.primary,
                       custom_id="mc_list", row=0)
    async def btn_list(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog  = self._cog(interaction)
        rows = cog.db.list_missions(interaction.guild.id)
        if not rows:
            await interaction.response.send_message("📭 No active missions.", ephemeral=True)
            return
        embeds = [mission_embed(r, interaction.guild) for r in rows[:10]]
        header = f"📋 **{len(rows)} mission(s)**" + (" — showing first 10" if len(rows) > 10 else "")
        await interaction.response.send_message(content=header, embeds=embeds, ephemeral=True)

    # ---------------------------------------------------------------- ASSIGN
    @discord.ui.button(label="👤 Assign to Mission", style=discord.ButtonStyle.secondary,
                       custom_id="mc_assign", row=1)
    async def btn_assign(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog  = self._cog(interaction)
        try:
            feed = await self._feed(interaction)
        except discord.Forbidden:
            feed = None
        await interaction.response.send_modal(
            MissionIDModal("👤 Assign to Mission", cog.db, "assign", feed))

    # ---------------------------------------------------------------- COMPLETE
    @discord.ui.button(label="✅ Complete Mission", style=discord.ButtonStyle.success,
                       custom_id="mc_complete", row=1)
    async def btn_complete(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog  = self._cog(interaction)
        try:
            feed = await self._feed(interaction)
        except discord.Forbidden:
            feed = None
        await interaction.response.send_modal(
            MissionIDModal("✅ Complete Mission", cog.db, "complete", feed))

    # ---------------------------------------------------------------- CANCEL
    @discord.ui.button(label="❌ Cancel Mission", style=discord.ButtonStyle.danger,
                       custom_id="mc_cancel", row=1)
    async def btn_cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog  = self._cog(interaction)
        try:
            feed = await self._feed(interaction)
        except discord.Forbidden:
            feed = None
        await interaction.response.send_modal(
            MissionIDModal("❌ Cancel Mission", cog.db, "cancel", feed))

    # ---------------------------------------------------------------- INFO
    @discord.ui.button(label="🔍 Mission Info", style=discord.ButtonStyle.secondary,
                       custom_id="mc_info", row=2)
    async def btn_info(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = self._cog(interaction)
        await interaction.response.send_modal(
            MissionIDModal("🔍 Mission Info", cog.db, "info", None))


# ---------------------------------------------------------------------------
# Cog
# ---------------------------------------------------------------------------

class MissionCog(commands.Cog, name="Missions"):
    """EVE Online mission management — persistent button panel in #mission-control."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db  = MissionDB(_DB_PATH)
        # Register persistent view so buttons survive restarts
        bot.add_view(MissionControlView())
        print(f"[MissionCog] DB ready at {_DB_PATH}")

    # ------------------------------------------------------------------
    # Channel helper — find by exact name, create if missing
    # ------------------------------------------------------------------
    async def _ensure_channel(
        self,
        guild: discord.Guild,
        name: str,
        topic: str,
    ) -> discord.TextChannel:
        existing = discord.utils.get(guild.text_channels, name=name)
        if existing:
            return existing

        print(f"[MissionCog] #{name} missing in '{guild.name}' — creating…")
        ch = await guild.create_text_channel(
            name=name,
            topic=topic,
            reason=f"MissionCog: auto-recreated missing channel #{name}.",
        )
        print(f"[MissionCog] Created #{name} (id={ch.id})")
        return ch

    # ------------------------------------------------------------------
    # Panel helper — post (or repost) the persistent control panel
    # ------------------------------------------------------------------
    async def _ensure_panel(self, guild: discord.Guild):
        ctrl = await self._ensure_channel(
            guild, MISSION_CONTROL_CHANNEL,
            "🎮 EVE Mission Control — use the buttons below to manage missions.",
        )

        # Check recent history — if our panel message is still there, skip
        async for msg in ctrl.history(limit=20):
            if msg.author == guild.me and msg.components:
                return   # panel already present

        # Build and post a fresh panel embed
        embed = discord.Embed(
            title="🛸  EVE Mission Control",
            description=(
                "Use the buttons below to manage EVE Online missions.\n\n"
                "**✨ Create Mission** — Post a new mission *(managers only)*\n"
                "**📋 List Missions** — View all active missions\n"
                "**👤 Assign** — Assign yourself to an open mission\n"
                "**✅ Complete** — Mark your mission as done\n"
                "**❌ Cancel** — Cancel a mission *(managers only)*\n"
                "**🔍 Info** — Look up details for any mission\n"
            ),
            colour=discord.Colour.from_rgb(0, 180, 212),
        )
        embed.set_footer(text="Cryonic Gaming  •  Mission Board  •  Buttons work after restarts")
        await ctrl.send(embed=embed, view=MissionControlView())
        print(f"[MissionCog] Control panel posted in #{MISSION_CONTROL_CHANNEL}")

    # ------------------------------------------------------------------
    # on_ready — create both channels and post panel for every guild
    # ------------------------------------------------------------------
    @commands.Cog.listener()
    async def on_ready(self):
        for guild in self.bot.guilds:
            try:
                await self._ensure_channel(
                    guild, MISSION_FEED_CHANNEL,
                    "📡 EVE Online mission feed — managed by the bot. Do not delete.",
                )
                await self._ensure_panel(guild)
            except discord.Forbidden:
                print(f"[MissionCog] Missing 'Manage Channels' in '{guild.name}' — skipping setup.")
            except Exception as e:
                print(f"[MissionCog] Setup error in '{guild.name}': {e}")
                traceback.print_exception(type(e), e, e.__traceback__)

    # ------------------------------------------------------------------
    # on_guild_channel_delete — instantly recreate if either channel is deleted
    # ------------------------------------------------------------------
    @commands.Cog.listener()
    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel):
        if channel.name not in (MISSION_CONTROL_CHANNEL, MISSION_FEED_CHANNEL):
            return

        print(f"[MissionCog] #{channel.name} was deleted in '{channel.guild.name}' — recreating…")
        try:
            if channel.name == MISSION_FEED_CHANNEL:
                await self._ensure_channel(
                    channel.guild, MISSION_FEED_CHANNEL,
                    "📡 EVE Online mission feed — managed by the bot. Do not delete.",
                )
            else:
                await self._ensure_panel(channel.guild)
        except discord.Forbidden:
            print(f"[MissionCog] Cannot recreate #{channel.name} — missing permissions.")
        except Exception as e:
            print(f"[MissionCog] Error recreating #{channel.name}: {e}")
            traceback.print_exception(type(e), e, e.__traceback__)


# ---------------------------------------------------------------------------
# Required by discord.py cog loader
# ---------------------------------------------------------------------------

async def setup(bot: commands.Bot):
    await bot.add_cog(MissionCog(bot))
    print("[MissionCog] Cog registered.")
    # NOTE: OverlayApiCog is loaded separately via cogs/overlay_api.py
