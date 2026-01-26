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


# -------------------- Persistence --------------------

def load_data():
    if not os.path.exists(DATA_PATH):
        return {}
    with open(DATA_PATH, "r") as f:
        return json.load(f)


def save_data(data):
    with open(DATA_PATH, "w") as f:
        json.dump(data, f, indent=2)


# -------------------- UI COMPONENTS --------------------

class ButtonSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label="RSVP Buttons", value="rsvp"),
            discord.SelectOption(label="Reminder Button", value="remind"),
            discord.SelectOption(label="External Link Button", value="external")
        ]
        super().__init__(
            placeholder="Select buttons to include",
            min_values=1,
            max_values=3,
            options=options
        )


class ButtonSelectView(discord.ui.View):
    def __init__(self):
        super().__init__()
        self.selected = []
        self.add_item(ButtonSelect())

    async def interaction_check(self, interaction):
        self.selected = interaction.data["values"]
        await interaction.response.defer()
        self.stop()
        return True


class ReminderSelect(discord.ui.Select):
    def __init__(self, event_id):
        self.event_id = event_id
        options = [
            discord.SelectOption(label="5 minutes", value="5"),
            discord.SelectOption(label="15 minutes", value="15"),
            discord.SelectOption(label="30 minutes", value="30"),
            discord.SelectOption(label="1 hour", value="60")
        ]
        super().__init__(
            placeholder="Reminder time",
            options=options
        )

    async def callback(self, interaction):
        data = load_data()
        data[self.event_id]["reminders"][str(interaction.user.id)] = int(self.values[0])
        save_data(data)
        await interaction.response.send_message(
            "Reminder scheduled.",
            ephemeral=True
        )


class ReminderView(discord.ui.View):
    def __init__(self, event_id):
        super().__init__(timeout=60)
        self.add_item(ReminderSelect(event_id))


# -------------------- MODALS --------------------

class EventModal(discord.ui.Modal, title="Create Event"):
    name = discord.ui.TextInput(label="Event Name")
    description = discord.ui.TextInput(
        label="Description",
        style=discord.TextStyle.paragraph
    )
    datetime_utc = discord.ui.TextInput(
        label="Date & Time (UTC)",
        placeholder="YYYY-MM-DD HH:MM"
    )

    def __init__(self, bot, buttons, creator_id):
        super().__init__()
        self.bot = bot
        self.buttons = buttons
        self.creator_id = creator_id

    async def on_submit(self, interaction):
        try:
            event_dt = datetime.strptime(
                self.datetime_utc.value,
                "%Y-%m-%d %H:%M"
            ).replace(tzinfo=timezone.utc)
        except ValueError:
            await interaction.response.send_message(
                "Invalid datetime format.",
                ephemeral=True
            )
            return

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
        embed.add_field(
            name="📊 RSVP",
            value="✅ 0 | ❌ 0",
            inline=False
        )

        channel = discord.utils.get(
            interaction.guild.text_channels,
            name=ANNOUNCEMENT_CHANNEL
        )

        view = EventView(event_id, self.buttons)
        msg = await channel.send(embed=embed, view=view)

        data = load_data()
        data[event_id] = {
            "creator": self.creator_id,
            "timestamp": timestamp,
            "channel": channel.id,
            "message": msg.id,
            "attending": [],
            "declined": [],
            "reminders": {},
            "active": True
        }
        save_data(data)

        await interaction.response.send_message(
            "Event created.",
            ephemeral=True
        )


class EditModal(discord.ui.Modal, title="Edit Event"):
    description = discord.ui.TextInput(
        label="New Description",
        style=discord.TextStyle.paragraph
    )
    datetime_utc = discord.ui.TextInput(
        label="New Date & Time (UTC)",
        placeholder="YYYY-MM-DD HH:MM"
    )

    def __init__(self, event_id):
        super().__init__()
        self.event_id = event_id

    async def on_submit(self, interaction):
        data = load_data()
        event = data[self.event_id]

        event_dt = datetime.strptime(
            self.datetime_utc.value,
            "%Y-%m-%d %H:%M"
        ).replace(tzinfo=timezone.utc)

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
    def __init__(self, event_id, buttons):
        super().__init__(timeout=None)
        self.event_id = event_id

        if "rsvp" in buttons:
            self.add_item(RSVPButton("attend", "✅ Attend"))
            self.add_item(RSVPButton("decline", "❌ Decline"))

        if "remind" in buttons:
            self.add_item(RemindButton())

        self.add_item(AdminButton())


class RSVPButton(discord.ui.Button):
    def __init__(self, rsvp_type, label):
        super().__init__(label=label, style=discord.ButtonStyle.primary)
        self.rsvp_type = rsvp_type

    async def callback(self, interaction):
        data = load_data()
        event = data[self.view.event_id]
        uid = interaction.user.id

        guild = interaction.guild
        role = discord.utils.get(guild.roles, name=TEMP_ROLE_NAME)

        for lst in ("attending", "declined"):
            if uid in event[lst]:
                event[lst].remove(uid)

        event["attending" if self.rsvp_type == "attend" else "declined"].append(uid)

        if self.rsvp_type == "attend" and role:
            await interaction.user.add_roles(role)
        elif role:
            await interaction.user.remove_roles(role)

        save_data(data)
        await update_embed(interaction, self.view.event_id)
        await interaction.response.send_message(
            "RSVP updated.",
            ephemeral=True
        )


class RemindButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="🔔 Remind Me", style=discord.ButtonStyle.secondary)

    async def callback(self, interaction):
        await interaction.response.send_message(
            "Select reminder time:",
            view=ReminderView(self.view.event_id),
            ephemeral=True
        )


class AdminButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            label="⚙ Manage Event",
            style=discord.ButtonStyle.secondary
        )

    async def callback(self, interaction):
        data = load_data()
        event = data[self.view.event_id]

        if (
            interaction.user.id != event["creator"]
            and not interaction.user.guild_permissions.administrator
        ):
            await interaction.response.send_message(
                "Not authorized.",
                ephemeral=True
            )
            return

        await interaction.response.send_message(
            "Choose action:",
            view=AdminView(self.view.event_id),
            ephemeral=True
        )


class AdminView(discord.ui.View):
    def __init__(self, event_id):
        super().__init__(timeout=60)
        self.event_id = event_id

    @discord.ui.button(label="Edit Event", style=discord.ButtonStyle.primary)
    async def edit(self, interaction, _):
        await interaction.response.send_modal(EditModal(self.event_id))

    @discord.ui.button(label="Cancel Event", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction, _):
        data = load_data()
        event = data[self.event_id]
        event["active"] = False

        channel = interaction.guild.get_channel(event["channel"])
        msg = await channel.fetch_message(event["message"])

        embed = msg.embeds[0]
        embed.color = discord.Color.red()
        embed.add_field(
            name="❌ Status",
            value="This event has been cancelled.",
            inline=False
        )

        await msg.edit(embed=embed, view=None)
        save_data(data)

        await interaction.response.send_message(
            "Event cancelled.",
            ephemeral=True
        )


# -------------------- TASKS --------------------

async def update_embed(interaction, event_id):
    data = load_data()
    event = data[event_id]

    channel = interaction.guild.get_channel(event["channel"])
    msg = await channel.fetch_message(event["message"])

    embed = msg.embeds[0]
    embed.set_field_at(
        1,
        name="📊 RSVP",
        value=f"✅ {len(event['attending'])} | ❌ {len(event['declined'])}",
        inline=False
    )

    await msg.edit(embed=embed)


class EventCreator(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.cleanup_loop.start()
        self.reminder_loop.start()

    def cog_unload(self):
        self.cleanup_loop.cancel()
        self.reminder_loop.cancel()

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

        view = ButtonSelectView()
        await interaction.response.send_message(
            "Select buttons:",
            view=view,
            ephemeral=True
        )
        await view.wait()

        await interaction.followup.send_modal(
            EventModal(self.bot, view.selected, interaction.user.id)
        )

    @tasks.loop(minutes=1)
    async def reminder_loop(self):
        data = load_data()
        now = datetime.now(timezone.utc)

        for event_id, event in data.items():
            event_time = datetime.fromtimestamp(event["timestamp"], timezone.utc)
            for uid, minutes in list(event["reminders"].items()):
                if now + timedelta(minutes=minutes) >= event_time:
                    user = self.bot.get_user(int(uid))
                    if user:
                        await user.send(
                            f"Reminder: Event starts <t:{event['timestamp']}:R>"
                        )
                    del event["reminders"][uid]

        save_data(data)

    @tasks.loop(minutes=5)
    async def cleanup_loop(self):
        data = load_data()
        now = datetime.now(timezone.utc)

        for event in data.values():
            if now.timestamp() > event["timestamp"]:
                guild = self.bot.guilds[0]
                role = discord.utils.get(guild.roles, name=TEMP_ROLE_NAME)
                if role:
                    for uid in event["attending"]:
                        member = guild.get_member(uid)
                        if member:
                            await member.remove_roles(role)

        save_data(data)


async def setup(bot):
    await bot.add_cog(EventCreator(bot))
