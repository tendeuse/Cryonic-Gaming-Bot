import json
import os
import discord
from discord import app_commands
from discord.ext import commands


# ──────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────
AUTHORIZED_ROLE_NAME = "ARC Security Corporation Leader"
DATA_FILE = "data.json"


# ──────────────────────────────────────────────
# Persistence helpers
# ──────────────────────────────────────────────

# Module-level mirror of data.json["sent_embeds"].
# Populated by load_sent_embeds() at cog setup and updated on every send.
# Structure: { message_id (int): {"author_id": int, "channel_id": int} }
sent_embeds: dict[int, dict] = {}


def load_sent_embeds() -> dict[int, dict]:
    """Read sent_embeds from data.json. Returns an empty dict if missing."""
    if not os.path.exists(DATA_FILE):
        return {}
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {int(k): v for k, v in data.get("sent_embeds", {}).items()}
    except (json.JSONDecodeError, OSError):
        return {}


def save_sent_embeds() -> None:
    """Write sent_embeds back into data.json, preserving all other keys."""
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            data = {}
    except (json.JSONDecodeError, OSError):
        data = {}

    data["sent_embeds"] = {str(k): v for k, v in sent_embeds.items()}

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)


# ──────────────────────────────────────────────
# Permission helper
# ──────────────────────────────────────────────
def has_edit_permission(interaction: discord.Interaction, author_id: int) -> bool:
    """
    Returns True if the interacting user is:
      - The original embed author, OR
      - A guild member holding the AUTHORIZED_ROLE_NAME role.
    """
    if interaction.user.id == author_id:
        return True
    if isinstance(interaction.user, discord.Member):
        return any(r.name == AUTHORIZED_ROLE_NAME for r in interaction.user.roles)
    return False


# ──────────────────────────────────────────────
# Edit modal  (opened by the persistent button)
# ──────────────────────────────────────────────
class EditEmbedModal(discord.ui.Modal, title="Edit Sent Embed"):
    json_input = discord.ui.TextInput(
        label="New Embed JSON",
        style=discord.TextStyle.paragraph,
        placeholder='{"embeds": [{"title": "Updated title", "description": "..."}]}',
        required=True,
        max_length=4000
    )

    def __init__(self, message: discord.Message, author_id: int):
        super().__init__()
        self.message = message
        self.author_id = author_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            payload = json.loads(self.json_input.value.strip())
        except json.JSONDecodeError as e:
            await interaction.response.send_message(
                f"❌ **Invalid JSON**\n```{e}```",
                ephemeral=True
            )
            return

        if "embeds" not in payload or not isinstance(payload["embeds"], list):
            await interaction.response.send_message(
                "❌ JSON must contain an **`embeds`** array.",
                ephemeral=True
            )
            return

        try:
            embeds = [discord.Embed.from_dict(e) for e in payload["embeds"]]
        except Exception as e:
            await interaction.response.send_message(
                f"❌ **Embed build error**\n```{e}```",
                ephemeral=True
            )
            return

        if len(embeds) > 10:
            await interaction.response.send_message(
                "❌ Discord allows a maximum of **10 embeds** per message.",
                ephemeral=True
            )
            return

        try:
            await self.message.edit(embeds=embeds)
            await interaction.response.send_message(
                "✅ Embed updated successfully.",
                ephemeral=True
            )
        except discord.Forbidden:
            await interaction.response.send_message(
                "❌ I don't have permission to edit that message.",
                ephemeral=True
            )
        except discord.NotFound:
            await interaction.response.send_message(
                "❌ Message not found — was it deleted?",
                ephemeral=True
            )


# ──────────────────────────────────────────────
# Persistent view — lives ON the sent embed message
# ──────────────────────────────────────────────
class SentEmbedView(discord.ui.View):
    """
    Attached to every embed message the bot posts to the channel.

    Rules for persistence across restarts:
      - timeout=None (no expiry)
      - custom_id must be a static string (same value every time)
      - bot.add_view(SentEmbedView()) must be called in setup() before on_ready
    """

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="✏️ Edit Embed",
        style=discord.ButtonStyle.blurple,
        custom_id="sent_embed:edit"    # static — required for persistence
    )
    async def edit_button(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        msg_id = interaction.message.id
        record = sent_embeds.get(msg_id)

        # Record missing: message predates this feature or data was cleared
        if record is None:
            await interaction.response.send_message(
                "❌ No record found for this embed.\n"
                "This can happen if the bot was updated after the message was originally sent.",
                ephemeral=True
            )
            return

        # Permission check
        if not has_edit_permission(interaction, record["author_id"]):
            await interaction.response.send_message(
                f"❌ Only the original sender or a **{AUTHORIZED_ROLE_NAME}** can edit this embed.",
                ephemeral=True
            )
            return

        # interaction.message IS the sent embed message — pass it straight to the modal
        await interaction.response.send_modal(
            EditEmbedModal(interaction.message, record["author_id"])
        )


# ──────────────────────────────────────────────
# Ephemeral preview view  (shown only to the sender)
# ──────────────────────────────────────────────
class EmbedPreviewView(discord.ui.View):
    def __init__(self, embeds: list[discord.Embed], author: discord.User | discord.Member):
        super().__init__(timeout=300)
        self.embeds = embeds
        self.author = author
        self.sent = False
        self.role_pings: list[str] = []

    # Only the original sender may interact with their own preview
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author.id:
            await interaction.response.send_message(
                "❌ You cannot interact with this embed preview.",
                ephemeral=True
            )
            return False
        return True

    def disable_all_items(self):
        for item in self.children:
            item.disabled = True

    def build_preview_content(self) -> str:
        base = "📝 **Embed Preview** — Review below, then choose an action:"
        if self.role_pings:
            base += f"\n🔔 **Will ping:** {' '.join(self.role_pings)}"
        else:
            base += "\n🔔 **No role pings selected.**"
        return base

    def build_send_content(self) -> str | None:
        return " ".join(self.role_pings) if self.role_pings else None

    # ── Row 0: Native Discord role multi-selector ──────────────────
    @discord.ui.select(
        cls=discord.ui.RoleSelect,
        placeholder="🔔 Select roles to ping (optional, multi-select)…",
        min_values=0,
        max_values=20,
        row=0
    )
    async def role_select(
        self,
        interaction: discord.Interaction,
        select: discord.ui.RoleSelect
    ):
        self.role_pings = [role.mention for role in select.values]
        await interaction.response.edit_message(
            content=self.build_preview_content(),
            view=self
        )

    # ── Row 1: Send / Cancel ───────────────────────────────────────
    @discord.ui.button(label="✅ Send", style=discord.ButtonStyle.green, row=1)
    async def send_button(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        # Post the embed to the channel with the persistent Edit button attached
        sent_message = await interaction.channel.send(
            content=self.build_send_content(),
            embeds=self.embeds,
            view=SentEmbedView(),
            allowed_mentions=discord.AllowedMentions(everyone=True, roles=True)
        )
        self.sent = True

        # Save record to data.json so the Edit button survives restarts
        sent_embeds[sent_message.id] = {
            "author_id": self.author.id,
            "channel_id": interaction.channel.id,
        }
        save_sent_embeds()

        # Close the ephemeral preview cleanly
        self.disable_all_items()
        await interaction.response.edit_message(
            content=(
                f"✅ Embed sent!\n"
                f"The **✏️ Edit Embed** button is attached to the message.\n"
                f"Only you or a **{AUTHORIZED_ROLE_NAME}** can use it."
            ),
            view=self
        )

    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.red, row=1)
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
        self.disable_all_items()


# ──────────────────────────────────────────────
# Cog
# ──────────────────────────────────────────────
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
            content=view.build_preview_content(),
            embeds=embeds,
            view=view,
            ephemeral=True
        )


# ──────────────────────────────────────────────
# Setup  (called by bot.load_extension)
# ──────────────────────────────────────────────
async def setup(bot: commands.Bot):
    global sent_embeds

    # 1. Load persisted records BEFORE registering the view
    sent_embeds = load_sent_embeds()
    print(f"[EmbedBuilder] Loaded {len(sent_embeds)} sent embed record(s) from {DATA_FILE}.")

    # 2. Register the persistent view.
    #    This MUST happen before on_ready fires so discord.py can route
    #    button interactions from old messages to this handler after a restart.
    bot.add_view(SentEmbedView())
    print("[EmbedBuilder] Persistent SentEmbedView registered.")

    # 3. Add the cog
    await bot.add_cog(EmbedBuilder(bot))
