# cogs/backup_cog.py

import discord
import os
import zipfile
import io
import asyncio
from discord.ext import commands
from discord import app_commands
from pathlib import Path

# --- CONFIGURATION ---
# Your unique ID for strict ownership
OWNER_ID = 306935804054208523 
# Path to your Railway Volume
PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))

class BackupCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="export_volume",
        description="[OWNER ONLY] Zips and downloads all files from the persistent volume."
    )
    async def export_volume(self, interaction: discord.Interaction):
        # 1. STRICT OWNER CHECK
        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message("❌ Unauthorized. Only the bot owner can use this command.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        try:
            # 2. VERIFY DIRECTORY
            if not PERSIST_ROOT.exists():
                await interaction.followup.send(f"❌ Error: Volume path `{PERSIST_ROOT}` does not exist.")
                return

            # 3. ZIP FILES IN MEMORY
            zip_buffer = io.BytesIO()
            files_found = 0
            
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
                for root, dirs, files in os.walk(PERSIST_ROOT):
                    for file in files:
                        # Skip hidden system files if necessary
                        if file.startswith('.'):
                            continue
                            
                        file_path = Path(root) / file
                        # Create a clean internal path for the zip
                        arcname = file_path.relative_to(PERSIST_ROOT)
                        zip_file.write(file_path, arcname=arcname)
                        files_found += 1
                
            if files_found == 0:
                await interaction.followup.send("⚠️ Volume is empty. Nothing to backup.")
                return

            # 4. UPLOAD TO DISCORD
            zip_buffer.seek(0)
            file = discord.File(fp=zip_buffer, filename="railway_volume_backup.zip")

            await interaction.followup.send(
                f"✅ **Backup Complete!**\nFound `{files_found}` files. Downloading...", 
                file=file, 
                ephemeral=True
            )

        except Exception as e:
            print(f"Backup Error: {e}")
            await interaction.followup.send(f"❌ An error occurred during backup: {e}", ephemeral=True)

    @app_commands.command(
        name="list_volume",
        description="[OWNER ONLY] Lists all files currently in the persistent volume."
    )
    async def list_volume(self, interaction: discord.Interaction):
        # 1. STRICT OWNER CHECK
        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message("❌ Unauthorized.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        try:
            files_list = []
            for root, dirs, files in os.walk(PERSIST_ROOT):
                for file in files:
                    if not file.startswith('.'):
                        rel_path = Path(root).relative_to(PERSIST_ROOT) / file
                        files_list.append(str(rel_path))

            if not files_list:
                await interaction.followup.send("The volume is empty.")
            else:
                # Format as a code block for readability
                msg = "**Files in Volume:**\n```\n" + "\n".join(files_list) + "\n```"
                await interaction.followup.send(msg, ephemeral=True)

        except Exception as e:
            await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(BackupCog(bot))