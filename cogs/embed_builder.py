import json
import discord
from discord import app_commands
from discord.ext import commands

class EmbedPreviewView(discord.ui.View):
    def __init__(self, embeds: list[discord.Embed], author: discord.User):
        super().__init__(timeout=300)
        self.embeds = embeds
        self.author = author
        self.sent = False

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.author.id

    @discord.ui.button(label="Send", style=discord.ButtonStyle.green)
    async def send_button(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        await interaction.channel.send(embeds=self.embeds)
        self.sent = True
        self.disable_all_items()
        await interaction.response.edit_message(
            content="✅ Embed sent successfully.",
            view=self
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.red)
    async def cancel_button(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        self.disable_all_items()
        await interaction.response.edit_message(
            content="❌ Embed cancelled.",
            view=self
        )

    async def on_timeout(self):
        if not self.sent:
            self.disable_all_items()

class EmbedBuilder(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="embed",
        description="Preview and send embeds from raw Discord JSON"
    )
    @app_commands.describe(
        json_payload="Paste a valid Discord embed JSON payload"
    )
    async def embed(
        self,
        interaction: discord.Interaction,
        json_payload: str
    ):
        await interaction.response.defer(ephemeral=True)

        try:
            payload = json.loads(json_payload)
        except json.JSONDecodeError as e:
            await interaction.followup.send(
                f"❌ **Invalid JSON**\n```{e}```",
                ephemeral=True
            )
            return

        if "embeds" not in payload or not isinstance(payload["embeds"], list):
            await interaction.followup.send(
                "❌ JSON must contain an **`embeds`** array.",
                ephemeral=True
            )
            return

        try:
            embeds = [discord.Embed.from_dict(e) for e in payload["embeds"]]
        except Exception as e:
            await interaction.followup.send(
                f"❌ **Embed build error**\n```{e}```",
                ephemeral=True
            )
            return

        if len(embeds) > 10:
            await interaction.followup.send(
                "❌ Discord allows a maximum of **10 embeds per message**.",
                ephemeral=True
            )
            return

        view = EmbedPreviewView(embeds, interaction.user)

        await interaction.followup.send(
            content="📝 **Embed Preview**\nReview below, then choose an action:",
            embeds=embeds,
            view=view,
            ephemeral=True
        )

async def setup(bot: commands.Bot):
    await bot.add_cog(EmbedBuilder(bot))
