# cogs/help_cog.py
import discord
from discord import app_commands
from discord.ext import commands

class HelpCog(commands.Cog):
    """Cog that provides a slash-only /help command (no prefix commands shown)."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="help",
        description="Show all available slash commands organized by category"
    )
    async def help(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title="Bot Commands",
            description="List of available slash commands organized by category (cog):",
            color=discord.Color.blue()
        )

        cog_commands: dict[str, list[str]] = {}

        # Slash commands only
        for cmd in self.bot.tree.walk_commands():
            cog_name = getattr(cmd, "cog_name", None) or "No Category"

            # Prefer qualified name for grouped commands: arc roster, arc join, etc.
            qn = getattr(cmd, "qualified_name", cmd.name)
            desc = (cmd.description or "No description").strip()

            cog_commands.setdefault(cog_name, []).append(f"/{qn} — {desc}")

        # Stable ordering
        for cog_name in sorted(cog_commands.keys(), key=str.lower):
            cmds = sorted(cog_commands[cog_name], key=str.lower)
            # Discord embed field limit is 1024 chars; chunk if needed
            chunk = ""
            for line in cmds:
                if len(chunk) + len(line) + 1 > 1024:
                    embed.add_field(name=cog_name, value=chunk.rstrip(), inline=False)
                    chunk = ""
                chunk += line + "\n"
            if chunk.strip():
                embed.add_field(name=cog_name, value=chunk.rstrip(), inline=False)

        await interaction.response.send_message(embed=embed, ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(HelpCog(bot))
