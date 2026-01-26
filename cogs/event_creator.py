import discord
from discord.ext import commands
from discord import app_commands
from datetime import datetime, timezone
import json
import os
import uuid

DATA_PATH = "/data/events.json"
ANNOUNCEMENT_CHANNEL = "eve-announcements-as"
TEMP_ROLE_NAME = "Event Participant"
SECURITY_PING_ROLE = "ARC Security"

CREATOR_ROLES = {
    "ARC Officer",
    "ARC Commander",
    "ARC General",
    "ARC Security Administration Council",
    "ARC Security Corporation Leader"
}

RSVP_TYPES = {"accept", "damage", "logi", "salvager", "tentative", "decline"}
ROLE_ASSIGN_TYPES = {"accept", "damage", "logi", "salvager"}


# -------------------- Persistence --------------------

def load_data():
    if not os.path.exists(DATA_PATH):
        return {}
    with open(DATA_PATH, "r") as f:
        return json.load(f)


def save_data(data):
    with open(DATA_PATH, "w") as f:
        json.dump(data, f, indent=2)


# -------------------- CREATE EVENT MODAL --------------------

class CreateEventModal(discord.ui.Modal, title="Create Event"):
    name = discord.ui.TextInput(label="Event Name", max_length=100)

    description = discord.ui.TextInput(
        label="Description",
        style=discord.TextStyle.paragraph,
        max_length=1000
    )

    datetime_utc = discord.ui.TextInput(
        label="Date & Time (UTC)",
        placeholder="YYYY-MM-DD HH:MM"
    )

    buttons = discord.ui.TextInput(
        label="Buttons (comma-separated)",
        placeholder="Accept, Damage, Logi, Salvager, Tentative, Decline",
        required=False
    )

    redirect_url = discord.ui.TextInput(
        label="Redirect URL (optional)",
        placeholder="https://...",
        required=False
    )

    def __init__(self, creator_id):
        super().__init__()
        self.creator_id = creator_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            event_dt = datetime.strptime(
                self.datetime_utc.value.strip(),
                "%Y-%m-%d %H:%M"
            ).replace(tzinfo=timezone.utc)
        except ValueError:
            await interaction.response.send_message(
                "Invalid date format. Use YYYY-MM-DD HH:MM (UTC).",
                ephemeral=True
            )
            return

        selected_buttons = {
            b.strip().lower()
            for b in self.buttons.value.split(",")
            if b.strip()
        } & RSVP_TYPES

        event_id = str(uuid.uuid4())
        timestamp = int(event_dt.timestamp())

        embed = discord.Embed(
            title=self.name.value,
            description=self.description.value,
            color=discord.Color.blue()
        )

        embed.add_field(
            name="🕒 Time",
            value=f"<t:{timestamp}:F>\n<t:{timestamp}:R>",
            inline=False
        )

        if selected_buttons:
            embed.add_field(
                name="📊 Fleet Signup",
                value="\n".join(f"{b.title()}: 0" for b in selected_buttons),
                inline=False
            )

        channel = discord.utils.get(
            interaction.guild.text_channels,
            name=ANNOUNCEMENT_CHANNEL
        )

        if not channel:
            await interaction.response.send_message(
                "Announcement channel not found.",
                ephemeral=True
            )
            return

        # 🔔 ARC Security Ping (ONCE)
        security_role = discord.utils.get(
            interaction.guild.roles,
            name=SECURITY_PING_ROLE
        )

        if security_role:
            await channel.send(security_role.mention)

        view = EventView(event_id, selected_buttons, self.redirect_url.value.strip())
        msg = await channel.send(embed=embed, view=view)

        data = load_data()
        data[event_id] = {
            "creator": self.creator_id,
            "timestamp": timestamp,
            "channel": channel.id,
            "message": msg.id,
            "roles": {b.title(): [] for b in selected_buttons},
            "redirect_url": self.redirect_url.value.strip(),
            "active": True
        }
        save_data(data)

        await interaction.response.send_message(
            "Event created successfully and ARC Security has been notified.",
            ephemeral=True
        )


# -------------------- EDIT EVENT MODAL --------------------

class EditEventModal(discord.ui.Modal, title="Edit Event"):
    description = discord.ui.TextInput(
        label="New Description",
        style=discord.TextStyle.paragraph,
        max_length=1000
    )

    datetime_utc = discord.ui.TextInput(
        label="New Date & Time (UTC)",
        placeholder="YYYY-MM-DD HH:MM"
    )

    def __init__(self, event_id):
        super().__init__()
        self.event_id = event_id

    async def on_submit(self, interaction: discord.Interaction):
        data = load_data()
        event = data[self.event_id]

        try:
            event_dt = datetime.strptime(
                self.datetime_utc.value.strip(),
                "%Y-%m-%d %H:%M"
            ).replace(tzinfo=timezone.utc)
        except ValueError:
            await interaction.response.send_message(
                "Invalid date format.",
                ephemeral=True
            )
            return

        event["timestamp"] = int(event_dt.timestamp())

        channel = interaction.guild.get_channel(event["channel"])
        msg = await channel.fetch_message(event["message"])
        embed = msg.embeds[0]

        embed.description = self.description.value
        embed.set_field_at(
            0,
            name="🕒 Time",
            value=f"<t:{event['timestamp']}:F>\n<t:{event['timestamp']}:R>",
            inline=False
        )

        await msg.edit(embed=embed)
        save_data(data)

        await interaction.response.send_message(
            "Event updated.",
            ephemeral=True
        )


# -------------------- EVENT VIEW --------------------

class EventView(discord.ui.View):
    def __init__(self, event_id, buttons, redirect_url):
        super().__init__(timeout=None)
        self.event_id = event_id

        for b in buttons:
            self.add_item(RSVPButton(b.title()))

        if redirect_url:
            self.add_item(
                discord.ui.Button(
                    label="External Signup",
                    url=redirect_url,
                    style=discord.ButtonStyle.link
                )
            )

        self.add_item(ManageEventButton())


class RSVPButton(discord.ui.Button):
    def __init__(self, rsvp_type):
        super().__init__(label=rsvp_type, style=discord.ButtonStyle.primary)
        self.rsvp_type = rsvp_type.lower()

    async def callback(self, interaction: discord.Interaction):
        data = load_data()
        event = data[self.view.event_id]
        uid = interaction.user.id

        guild = interaction.guild
        temp_role = discord.utils.get(guild.roles, name=TEMP_ROLE_NAME)

        for users in event["roles"].values():
            if uid in users:
                users.remove(uid)

        event["roles"][self.rsvp_type.title()].append(uid)

        if self.rsvp_type in ROLE_ASSIGN_TYPES and temp_role:
            await interaction.user.add_roles(temp_role)
        elif temp_role:
            await interaction.user.remove_roles(temp_role)

        save_data(data)
        await interaction.response.send_message(
            f"Registered as **{self.rsvp_type.title()}**.",
            ephemeral=True
        )


class ManageEventButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            label="⚙ Manage Event",
            style=discord.ButtonStyle.secondary
        )

    async def callback(self, interaction: discord.Interaction):
        data = load_data()
        event = data[self.view.event_id]

        if (
            interaction.user.id != event["creator"]
            and not interaction.user.guild_permissions.administrator
        ):
            await interaction.response.send_message(
                "You are not authorized to manage this event.",
                ephemeral=True
            )
            return

        await interaction.response.send_modal(
            EditEventModal(self.view.event_id)
        )


# -------------------- COG --------------------

class EventCreator(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    def can_create(self, member):
        return any(role.name in CREATOR_ROLES for role in member.roles)

    @app_commands.command(name="create_event", description="Create a new event")
    async def create_event(self, interaction: discord.Interaction):
        if not self.can_create(interaction.user):
            await interaction.response.send_message(
                "You are not authorized to create events.",
                ephemeral=True
            )
            return

        await interaction.response.send_modal(
            CreateEventModal(interaction.user.id)
        )


async def setup(bot):
    await bot.add_cog(EventCreator(bot))
