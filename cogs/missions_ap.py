# filename: cogs/missions_ap.py
# Discord bot cog: AP (Activity Points) system for EVE missions.
# - Receives mission_complete calls from the overlay
# - Stores AP per character/user in SQLite
# - /ap command: show AP balance
# - /ap_leaderboard: show top pilots
# - /ap_redeem: placeholder for spending AP
#
# AP table schema:
#   CREATE TABLE IF NOT EXISTS ap_ledger (
#       id            INTEGER PRIMARY KEY AUTOINCREMENT,
#       discord_id    TEXT NOT NULL,
#       character_name TEXT NOT NULL,
#       mission_name  TEXT NOT NULL,
#       faction       TEXT NOT NULL DEFAULT '',
#       level         INTEGER NOT NULL DEFAULT 4,
#       standing_gain REAL NOT NULL DEFAULT 0,
#       ap            INTEGER NOT NULL DEFAULT 50,
#       recorded_at   TEXT NOT NULL
#   );

import discord
from discord.ext import commands
from discord import app_commands
import sqlite3
import os
from datetime import datetime, timezone

DB_PATH = os.environ.get("MISSION_DB_PATH", "/data/missions.db")

def get_db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("""
        CREATE TABLE IF NOT EXISTS ap_ledger (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            discord_id     TEXT NOT NULL,
            character_name TEXT NOT NULL,
            mission_name   TEXT NOT NULL,
            faction        TEXT NOT NULL DEFAULT '',
            level          INTEGER NOT NULL DEFAULT 4,
            standing_gain  REAL NOT NULL DEFAULT 0,
            ap             INTEGER NOT NULL DEFAULT 50,
            recorded_at    TEXT NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS char_discord_map (
            character_name TEXT PRIMARY KEY,
            discord_id     TEXT NOT NULL
        )
    """)
    con.commit()
    return con

def get_discord_id_for_char(con: sqlite3.Connection, character_name: str) -> str | None:
    row = con.execute(
        "SELECT discord_id FROM char_discord_map WHERE character_name=?",
        (character_name,)
    ).fetchone()
    return row["discord_id"] if row else None

def get_total_ap(con: sqlite3.Connection, discord_id: str) -> int:
    row = con.execute(
        "SELECT COALESCE(SUM(ap),0) as total FROM ap_ledger WHERE discord_id=?",
        (discord_id,)
    ).fetchone()
    return row["total"] if row else 0

def get_mission_count(con: sqlite3.Connection, discord_id: str) -> int:
    row = con.execute(
        "SELECT COUNT(*) as cnt FROM ap_ledger WHERE discord_id=?",
        (discord_id,)
    ).fetchone()
    return row["cnt"] if row else 0


class MissionsAP(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    # ── /ap ───────────────────────────────────────────────────────────────
    @app_commands.command(name="ap", description="Show your Activity Points balance from EVE missions")
    async def ap(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        with get_db() as con:
            discord_id  = str(interaction.user.id)
            total       = get_total_ap(con, discord_id)
            count       = get_mission_count(con, discord_id)
            # Recent missions
            recent = con.execute(
                "SELECT mission_name, faction, ap, recorded_at FROM ap_ledger "
                "WHERE discord_id=? ORDER BY recorded_at DESC LIMIT 5",
                (discord_id,)
            ).fetchall()

        embed = discord.Embed(
            title="⚡ Activity Points — Mission Tracker",
            colour=discord.Colour.gold(),
        )
        embed.add_field(name="Total AP",       value=f"**{total:,}**",  inline=True)
        embed.add_field(name="Missions Done",  value=f"**{count}**",    inline=True)
        embed.add_field(name="AP per Mission", value="**50**",          inline=True)

        if recent:
            lines = []
            for r in recent:
                ts = r["recorded_at"][:16].replace("T", " ")
                lines.append(f"`{ts}`  {r['mission_name']} [{r['faction']}]  +{r['ap']} AP")
            embed.add_field(name="Recent Missions", value="\n".join(lines), inline=False)
        else:
            embed.add_field(
                name="No missions recorded yet",
                value="Complete missions in EVE — the overlay detects them automatically and credits AP here.",
                inline=False
            )
        embed.set_footer(text="AP are credited automatically when the Cryonic Overlay detects a mission completion.")
        await interaction.followup.send(embed=embed, ephemeral=True)

    # ── /ap_leaderboard ───────────────────────────────────────────────────
    @app_commands.command(name="ap_leaderboard", description="Top pilots by Activity Points this month")
    async def ap_leaderboard(self, interaction: discord.Interaction):
        await interaction.response.defer()
        with get_db() as con:
            rows = con.execute("""
                SELECT discord_id, character_name,
                       SUM(ap) as total_ap, COUNT(*) as missions
                FROM ap_ledger
                GROUP BY discord_id
                ORDER BY total_ap DESC
                LIMIT 10
            """).fetchall()

        embed = discord.Embed(
            title="🏆 Mission AP Leaderboard",
            colour=discord.Colour.gold(),
        )
        if not rows:
            embed.description = "No missions recorded yet. Get flying!"
        else:
            medals = ["🥇", "🥈", "🥉"] + ["🔹"] * 7
            lines  = []
            for i, row in enumerate(rows):
                try:
                    member = interaction.guild.get_member(int(row["discord_id"]))
                    name   = member.display_name if member else row["character_name"]
                except Exception:
                    name = row["character_name"]
                lines.append(
                    f"{medals[i]} **{name}**  —  {row['total_ap']:,} AP  ({row['missions']} missions)"
                )
            embed.description = "\n".join(lines)
        embed.set_footer(text="AP earned by completing EVE Online missions tracked by Cryonic Overlay")
        await interaction.followup.send(embed=embed)

    # ── /link_character ───────────────────────────────────────────────────
    @app_commands.command(
        name="link_character",
        description="Link your EVE character name to your Discord for AP tracking"
    )
    @app_commands.describe(character_name="Your exact EVE character name")
    async def link_character(self, interaction: discord.Interaction, character_name: str):
        await interaction.response.defer(ephemeral=True)
        with get_db() as con:
            con.execute(
                "INSERT INTO char_discord_map(character_name, discord_id) VALUES(?,?) "
                "ON CONFLICT(character_name) DO UPDATE SET discord_id=excluded.discord_id",
                (character_name, str(interaction.user.id))
            )
            con.commit()
        await interaction.followup.send(
            f"✅ **{character_name}** linked to your Discord account.\n"
            "AP from this character's missions will now appear in `/ap`.",
            ephemeral=True
        )

    # ── Internal: record a mission completion (called from overlay_api.py) ─
    @staticmethod
    def record_mission(character_name: str, mission_name: str, faction: str,
                       level: int, standing_gain: float, ap: int):
        with get_db() as con:
            discord_id = get_discord_id_for_char(con, character_name) or "unknown"
            con.execute(
                "INSERT INTO ap_ledger(discord_id, character_name, mission_name, "
                "faction, level, standing_gain, ap, recorded_at) VALUES(?,?,?,?,?,?,?,?)",
                (discord_id, character_name, mission_name, faction, level,
                 standing_gain, ap, datetime.now(timezone.utc).isoformat())
            )
            con.commit()
        return discord_id


async def setup(bot: commands.Bot):
    await bot.add_cog(MissionsAP(bot))
