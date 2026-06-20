# cogs/backup_cog.py

import asyncio
import io
import json

import discord
from discord.ext import commands
from discord import app_commands

from . import db

# --- CONFIGURATION ---
# Your unique ID for strict ownership
OWNER_ID = 306935804054208523


class BackupCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="export_volume",
        description="[OWNER ONLY] Dumps the entire MySQL database (kv_store + tables) as a JSON backup."
    )
    async def export_volume(self, interaction: discord.Interaction):
        # 1. STRICT OWNER CHECK
        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message("❌ Unauthorized. Only the bot owner can use this command.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        try:
            # 2. DUMP MYSQL (off the event loop)
            dump = await asyncio.to_thread(db.export_all)
            payload = json.dumps(dump, indent=2, default=str, ensure_ascii=False).encode("utf-8")

            kv_count    = len(dump.get("kv_store", {}))
            table_count = sum(len(v) for v in dump.get("tables", {}).values())

            # 3. UPLOAD TO DISCORD
            file = discord.File(fp=io.BytesIO(payload), filename="mysql_backup.json")
            await interaction.followup.send(
                f"✅ **Backup Complete!**\n"
                f"Documents: `{kv_count}` kv keys · Rows: `{table_count}` across "
                f"`{len(dump.get('tables', {}))}` tables.",
                file=file,
                ephemeral=True,
            )

        except Exception as e:
            print(f"Backup Error: {e}")
            await interaction.followup.send(f"❌ An error occurred during backup: {e}", ephemeral=True)

    @app_commands.command(
        name="list_volume",
        description="[OWNER ONLY] Lists kv_store keys and table row counts in MySQL."
    )
    async def list_volume(self, interaction: discord.Interaction):
        # 1. STRICT OWNER CHECK
        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message("❌ Unauthorized.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        try:
            summary = await asyncio.to_thread(db.list_summary)

            kv_lines    = "\n".join(summary["kv_keys"]) or "(none)"
            table_lines = "\n".join(f"{t}: {n}" for t, n in summary["table_counts"].items())

            msg = (
                "**MySQL kv_store keys:**\n```\n" + kv_lines + "\n```\n"
                "**Table row counts:**\n```\n" + table_lines + "\n```"
            )
            # Discord 2000-char cap safety
            if len(msg) > 1900:
                msg = msg[:1900] + "\n…(truncated)```"
            await interaction.followup.send(msg, ephemeral=True)

        except Exception as e:
            await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(BackupCog(bot))
