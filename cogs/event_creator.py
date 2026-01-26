import discord
from discord.ext import commands, tasks
from discord import app_commands
from datetime import datetime, timezone, timedelta
import json
import os
import uuid

DATA_PATH = "/data/events.json"
ANNOUNCEMENT_CHANNEL = "eve-announcements-as"
TEMP_ROLE_NAME = "Event Participant"

CREATOR_ROLES = {
    "ARC Officer",
    "ARC Commander",
    "ARC General",
    "ARC Security Administration Council",
    "ARC Security Corporation Leader"
}

RSVP_TYPES = ["Accept", "Damage", "Logi", "Salvager", "Tentative", "Decline"]
ROLE_ASSIGN_TYPES = {"Accept", "Damage", "Logi", "Salvager"}


# -------------------- Persistence --------------------

def load_data():
    if not os.path.exists(DATA_PATH):
        return {}
    with open(DATA_PATH, "r") as f:
        return json.load(f)


def save_data(data):
    with open(DATA_PATH, "w") as f:
        json.dump(data, f, indent=2)


# -------------------- UI --------------------

class RSVPSelect(discord.ui.Select):
    def __init__(self):
        options = [discord.SelectOption(label=r) for r in RSVP_TYPES]
        super().__init__(
            placeholder="Select RSVP buttons to include",
            min_values=1,
            max_values=len(options),
            options=options
        )


class ButtonConfigView(discord.ui.View):
    def __init__(self):
        super().__init__()
        self.rsvp_buttons = []
        self.redirect = False
        self.add_item(RSVPSelect())

    @discord.ui.button(label="Add Redirect Button", style=discord.ButtonStyle.secondary)
    async def redirect_btn(self, interaction, _):
        self.redirect = True
        await interaction.response.send_message(
            "Redirect button enabled. You will be asked for the URL.",
            ephemeral=True
        )

    async def interaction_check(self, interaction):
        if interaction.data.get("values"):
            self.rsvp_buttons = interaction.data["values"]
            await interaction.response.defer()
            self.stop()
        return True


class RedirectModal(discord.ui.Modal, title="Redirect Button URL"):
    url = discord.ui.TextInput(label="URL", placeholder="https://...")

    def __init__(self):
        super().__init__()
        self.value = None

    async def on_submit(self, interaction):
        self.value = self.url.value
        await interaction.response.defer()
        self.stop()


# -------------------- MODALS --------------------

class EventModal(discord.ui.Modal, title="Create Event"):
    name = discord.ui.TextInput(label="Event Name")
    description = discord.ui.TextInput(label="Description", style=discord.TextStyle.paragraph)
    datetime_utc = discord.ui.TextInput(
        label="Date & Time (UTC)",
        placeholder="YYYY-MM-DD HH:MM"
    )

    def __init__(self, creator_id, rsvp_buttons, redirect_url):
        super().__init__()
        self.creator_id = creator_id
        self.rsvp_buttons = rsvp_buttons
        self.redirect_url = redirect_url

    async def on_submit(self, interaction):
        event_dt = datetime.strptime(
            self.datetime_utc.value,
            "%Y-%m-%d %H:%M"
        ).replace(tzinfo=timezone.utc)

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

        rsvp_field = "\n".join(f"{k}: 0" for k in self.rsvp_buttons)
        if rsvp_field:
            embed.add_field(name="📊 Fleet Signup", value=rsvp_field, inline=False)

        channel = discord.utils.get(
            interaction.guild.text_channels,
            name=ANNOUNCEMENT_CHANNEL
        )

        view = EventView(event_id, self.rsvp_buttons, self.redirect_url)
        msg = await channel.send(embed=embed, view=view)

        data = load_data()
        data[event_id] = {
            "creator": self.creator_id,
            "timestamp": timestamp,
            "channel": channel.id,
            "message": msg.id,
            "roles": {k: [] for k in self.rsvp_buttons},
            "redirect_url": self.redirect_url,
            "reminders": {},
            "active": True
        }
        save_data(data)

        await interaction.response.send_message("Event created.", ephemeral=True)


# -------------------- VIEW --------------------

class EventView(discord.ui.View):
    def __init__(self, event_id, rsvp_buttons, redirect_url):
        super().__init__(timeout=None)
        self.event_id = event_id

        for r in rsvp_buttons:
            self.add_item(RSVPButton(r))

        if redirect_url:
            self.add_item(
                discord.ui.Button(
                    label="External Signup",
                    url=redirect_url,
                    style=discord.ButtonStyle.link
                )
            )

        self.add_item(AdminButton())


class RSVPButton(discord.ui.Button):
    def __init__(self, rsvp_type):
        super().__init__(
            label=rsvp_type,
            style=discord.ButtonStyle.primary
        )
        self.rsvp_type = rsvp_type

    async def callback(self, interaction):
        data = load_data()
        event = data[self.view.event_id]
        uid = interaction.user.id

        guild = interaction.guild
        temp_role = discord.utils.get(guild.roles, name=TEMP_ROLE_NAME)

        for lst in event["roles"].values():
            if uid in lst:
                lst.remove(uid)

        event["roles"][self.rsvp_type].append(uid)

        if self.rsvp_type in ROLE_ASSIGN_TYPES and temp_role:
            await interaction.user.add_roles(temp_role)
        elif temp_role:
            await interaction.user.remove_roles(temp_role)

        save_data(data)
        await update_embed(interaction, self.view.event_id)
        await interaction.response.send_message(
            f"Registered as **{self.rsvp_type}**.",
            ephemeral=True
        )


class AdminButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="⚙ Manage Event", style=discord.ButtonStyle.secondary)

    async def callback(self, interaction):
        data = load_data()
        event = data[self.view.event_id]

        if (
            interaction.user.id != event["creator"]
            and not interaction.user.guild_permissions.administrator
        ):
            await interaction.response.send_message("Not authorized.", ephemeral=True)
            return

        await interaction.response.send_message(
            "Admin actions:",
            view=AdminView(self.view.event_id),
            ephemeral=True
        )


class AdminView(discord.ui.View):
    def __init__(self, event_id):
        super().__init__(timeout=60)
        self.event_id = event_id

    @discord.ui.button(label="Cancel Event", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction, _):
        data = load_data()
        event = data[self.event_id]
        event["active"] = False

        channel = interaction.guild.get_channel(event["channel"])
        msg = await channel.fetch_message(event["message"])

        embed = msg.embeds[0]
        embed.color = discord.Color.red()
        embed.add_field(name="❌ Status", value="Event cancelled.", inline=False)

        await msg.edit(embed=embed, view=None)
        save_data(data)

        await interaction.response.send_message("Event cancelled.", ephemeral=True)


# -------------------- HELPERS --------------------

async def update_embed(interaction, event_id):
    data = load_data()
    event = data[event_id]

    channel = interaction.guild.get_channel(event["channel"])
    msg = await channel.fetch_message(event["message"])

    embed = msg.embeds[0]
    value = "\n".join(
        f"{k}: {len(v)}" for k, v in event["roles"].items()
    )

    embed.set_field_at(1, name="📊 Fleet Signup", value=value, inline=False)
    await msg.edit(embed=embed)


# -------------------- COG --------------------

class EventCreator(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    def can_create(self, member):
        return any(r.name in CREATOR_ROLES for r in member.roles)

    @app_commands.command(name="create_event")
    async def create_event(self, interaction):
        if not self.can_create(interaction.user):
            await interaction.response.send_message(
                "You are not authorized to create events.",
                ephemeral=True
            )
            return

        config = ButtonConfigView()
        await interaction.response.send_message(
            "Configure event buttons:",
            view=config,
            ephemeral=True
        )
        await config.wait()

        redirect_url = None
        if config.redirect:
            modal = RedirectModal()
            await interaction.followup.send_modal(modal)
            await modal.wait()
            redirect_url = modal.value

        await interaction.followup.send_modal(
            EventModal(
                interaction.user.id,
                config.rsvp_buttons,
                redirect_url
            )
        )


async def setup(bot):
    await bot.add_cog(EventCreator(bot))
