import json
import discord
from discord import app_commands
from discord.ext import commands


class RolePingModal(discord.ui.Modal, title="Add Role Ping"):
    role_input = discord.ui.TextInput(
        label="Role to ping",
        placeholder="Role mention, role ID, everyone, or here",
        required=True,
        max_length=50
    )

    def __init__(self, view: "EmbedPreviewView"):
        super().__init__()
        self.view = view

    async def on_submit(self, interaction: discord.Interaction):
        value = self.role_input.value.strip()

        if value.lower() in ("everyone", "here"):
            self.view.role_ping = f"@{value.lower()}"
        elif value.isdigit():
            self.view.role_ping = f"<@&{value}>"
        else:
            self.view.role_ping = value  # assume valid mention

        await interaction.response.edit_message(
            content=f"🔔 Role ping set to: {self.view.role_ping}",
            view=self.view
        )


class EmbedPreviewView(discord.ui.View):
    def __init__(self, embeds: list[discord.Embed], author: discord.User):
        super().__init__(timeout=300)
        self.embeds = embeds
        self.author = author
        self.sent = False
        self.role_ping: str | None = None

    # 🔒 Author-only interaction
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author.id:
            await interaction.response.send_message(
                "❌ You cannot interact with this embed preview.",
                ephemeral=True
            )
            return False
        return True

    # ✅ FIX: this method did not exist before
    def disable_all_items(self):
        for item in self.children:
            item.disabled = True

    @discord.ui.button(label="Send", style=discord.ButtonStyle.green)
    async def send_button(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        content = self.role_ping or None

        await interaction.channel.send(
            content=content,
            embeds=self.embeds,
            allowed_mentions=discord.AllowedMentions(
                everyone=True,
                roles=True
            )
        )

        self.sent = True
        self.disable_all_items()

        await interaction.response.edit_message(
            content="✅ Embed sent successfully.",
            view=self
        )

    @discord.ui.button(label="Add Role Ping", style=discord.ButtonStyle.blurple)
    async def role_button(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        await interaction.response.send_modal(RolePingModal(self))

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
