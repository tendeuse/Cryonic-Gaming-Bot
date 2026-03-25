# cogs/event_creator.py
import asyncio
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

import discord
from discord.ext import commands
from discord import app_commands

DATA_PATH = "/data/events.json"

SECURITY_ONLY_CHANNEL = "wh-op-sec-events"
PUBLIC_CHANNEL = "eve-announcements"

SECURITY_PING_ROLE = "ARC Security"
SUBSIDIZED_PING_ROLE = "ARC Subsidized"

# ---------------- STORAGE ----------------

_lock: Optional[asyncio.Lock] = None

def _ensure_lock():
    global _lock
    if _lock is None:
        _lock = asyncio.Lock()
    return _lock

async def load_events():
    lock = _ensure_lock()
    async with lock:
        if not os.path.exists(DATA_PATH):
            return {}
        with open(DATA_PATH, "r") as f:
            return json.load(f)

async def save_events(data):
    lock = _ensure_lock()
    async with lock:
        os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)
        with open(DATA_PATH, "w") as f:
            json.dump(data, f, indent=2)

# ---------------- HELPERS ----------------

def resolve_channel(guild, target):
    name = SECURITY_ONLY_CHANNEL if target == "security_only" else PUBLIC_CHANNEL
    return discord.utils.get(guild.text_channels, name=name)

def resolve_ping(guild, target):
    roles = []
    sec = discord.utils.get(guild.roles, name=SECURITY_PING_ROLE)
    sub = discord.utils.get(guild.roles, name=SUBSIDIZED_PING_ROLE)

    if sec:
        roles.append(sec.mention)
    if target == "public" and sub:
        roles.append(sub.mention)

    return " ".join(roles)

# ---------------- EMBED ----------------

def build_embed(event):
    embed = discord.Embed(
        title=event["title"],
        description=event["description"],
        color=discord.Color.blue()
    )

    embed.add_field(
        name="Time",
        value=f"<t:{event['timestamp']}:F>\n<t:{event['timestamp']}:R>",
        inline=False
    )

    for role, users in event["roles"].items():
        cap = event.get("capacities", {}).get(role)
        count = len(users)

        name = f"{role} ({count}/{cap})" if cap else f"{role} ({count})"
        value = "\n".join(f"<@{u}>" for u in users) or "_(none)_"

        embed.add_field(name=name, value=value, inline=False)

    return embed

async def refresh(bot, event_id):
    data = await load_events()
    event = data.get(event_id)
    if not event:
        return

    guild = bot.get_guild(event["guild_id"])
    channel = guild.get_channel(event["channel"])
    msg = await channel.fetch_message(event["message"])

    await msg.edit(
        embed=build_embed(event),
        view=EventView(event_id, event["buttons"])
    )

# ---------------- STEP 1 VIEW ----------------

class AudienceSelectView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id

    async def interaction_check(self, interaction: discord.Interaction):
        return interaction.user.id == self.user_id

    @discord.ui.button(label="ARC Security Only", style=discord.ButtonStyle.danger)
    async def security_only(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(EventModal(interaction.user.id, "security_only"))

    @discord.ui.button(label="Security + Subsidized", style=discord.ButtonStyle.success)
    async def public(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(EventModal(interaction.user.id, "public"))

# ---------------- MODAL ----------------

class EventModal(discord.ui.Modal, title="Create Event"):
    name = discord.ui.TextInput(label="Event Name")
    description = discord.ui.TextInput(label="Description", style=discord.TextStyle.paragraph)
    time = discord.ui.TextInput(label="UTC Time (YYYY-MM-DD HH:MM)")
    buttons = discord.ui.TextInput(label="Buttons (Logi:5 supported)")

    def __init__(self, creator_id, target):
        super().__init__()
        self.creator_id = creator_id
        self.target = target

    async def on_submit(self, interaction: discord.Interaction):
        try:
            dt = datetime.strptime(self.time.value, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        except:
            await interaction.response.send_message("Invalid date.", ephemeral=True)
            return

        raw = self.buttons.value.split(",")
        buttons = []
        caps = {}

        for r in raw:
            r = r.strip()
            if ":" in r:
                n, c = r.split(":")
                n = n.title()
                buttons.append(n)
                caps[n] = int(c)
            else:
                buttons.append(r.title())

        event_id = str(uuid.uuid4())
        guild = interaction.guild
        channel = resolve_channel(guild, self.target)

        event = {
            "title": self.name.value,
            "description": self.description.value,
            "timestamp": int(dt.timestamp()),
            "guild_id": guild.id,
            "channel": channel.id,
            "message": None,
            "buttons": buttons,
            "capacities": caps,
            "roles": {b: [] for b in buttons},
            "creator": self.creator_id
        }

        msg = await channel.send(
            content=resolve_ping(guild, self.target),
            embed=build_embed(event),
            view=EventView(event_id, buttons)
        )

        event["message"] = msg.id

        data = await load_events()
        data[event_id] = event
        await save_events(data)

        interaction.client.add_view(EventView(event_id, buttons), message_id=msg.id)

        await interaction.response.send_message("Event created.", ephemeral=True)

# ---------------- RSVP BUTTON ----------------

class RSVPButton(discord.ui.Button):
    def __init__(self, event_id, name, row):
        self.name = name
        super().__init__(
            label=name,
            style=self.style_map(name),
            custom_id=f"rsvp:{event_id}:{name}",
            row=row
        )

    def style_map(self, name):
        name = name.lower()
        if name == "accept":
            return discord.ButtonStyle.success
        if name == "decline":
            return discord.ButtonStyle.danger
        if name == "tentative":
            return discord.ButtonStyle.secondary
        return discord.ButtonStyle.primary

    async def callback(self, interaction: discord.Interaction):
        data = await load_events()
        event = data[self.view.event_id]

        uid = interaction.user.id

        # Remove from all
        for r in event["roles"]:
            if uid in event["roles"][r]:
                event["roles"][r].remove(uid)

        cap = event.get("capacities", {}).get(self.name)
        if cap and len(event["roles"][self.name]) >= cap:
            await interaction.response.send_message("Role full.", ephemeral=True)
            return

        event["roles"][self.name].append(uid)

        await save_events(data)
        await refresh(interaction.client, self.view.event_id)

        await interaction.response.send_message(f"Registered as {self.name}", ephemeral=True)

# ---------------- ADMIN BUTTONS ----------------

class AdminView(discord.ui.View):
    def __init__(self, event_id):
        super().__init__(timeout=60)
        self.event_id = event_id

    @discord.ui.button(label="Edit Event", style=discord.ButtonStyle.secondary)
    async def edit(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(EditModal(self.event_id))

    @discord.ui.button(label="Delete Event", style=discord.ButtonStyle.danger)
    async def delete(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = await load_events()
        event = data.get(self.event_id)

        guild = interaction.guild
        channel = guild.get_channel(event["channel"])
        msg = await channel.fetch_message(event["message"])

        await msg.delete()

        del data[self.event_id]
        await save_events(data)

        await interaction.response.send_message("Event deleted.", ephemeral=True)

class EditModal(discord.ui.Modal, title="Edit Event"):
    description = discord.ui.TextInput(label="New Description", style=discord.TextStyle.paragraph)

    def __init__(self, event_id):
        super().__init__()
        self.event_id = event_id

    async def on_submit(self, interaction: discord.Interaction):
        data = await load_events()
        event = data[self.event_id]

        event["description"] = self.description.value

        await save_events(data)
        await refresh(interaction.client, self.event_id)

        await interaction.response.send_message("Updated.", ephemeral=True)

# ---------------- VIEW ----------------

class EventView(discord.ui.View):
    def __init__(self, event_id, buttons):
        super().__init__(timeout=None)
        self.event_id = event_id

        for i, b in enumerate(buttons):
            btn = RSVPButton(event_id, b, i // 5)
            self.add_item(btn)

        self.add_item(AdminButton(event_id))

class AdminButton(discord.ui.Button):
    def __init__(self, event_id):
        super().__init__(
            label="Manage",
            style=discord.ButtonStyle.secondary,
            row=4
        )
        self.event_id = event_id

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_message(
            "Admin Panel:",
            view=AdminView(self.event_id),
            ephemeral=True
        )

# ---------------- DISABLE FULL BUTTONS ----------------

async def disable_full_buttons(view, event):
    for item in view.children:
        if isinstance(item, RSVPButton):
            cap = event.get("capacities", {}).get(item.name)
            if cap and len(event["roles"][item.name]) >= cap:
                item.disabled = True

# ---------------- COG ----------------

class EventCreator(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="create_event")
    async def create_event(self, interaction: discord.Interaction):
        await interaction.response.send_message(
            "Select event audience:",
            view=AudienceSelectView(interaction.user.id),
            ephemeral=True
        )

    @commands.Cog.listener()
    async def on_ready(self):
        data = await load_events()
        for eid, event in data.items():
            try:
                view = EventView(eid, event["buttons"])
                await disable_full_buttons(view, event)

                self.bot.add_view(view, message_id=event["message"])
            except:
                pass

async def setup(bot):
    await bot.add_cog(EventCreator(bot))