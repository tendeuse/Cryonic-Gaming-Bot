# cogs/event_creator.py
import asyncio
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Set

import discord
from discord.ext import commands
from discord import app_commands

DATA_PATH = "/data/events.json"

SECURITY_ONLY_CHANNEL = "wh-op-sec-events"
PUBLIC_CHANNEL = "eve-announcements"

SECURITY_PING_ROLE = "ARC Security"
SUBSIDIZED_PING_ROLE = "ARC Subsidized"

DISPLAY_ORDER = ["Accept", "Damage", "Logi", "Salvager", "Tentative", "Decline"]

# -------------------- STORAGE --------------------

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
        try:
            with open(DATA_PATH, "r") as f:
                return json.load(f)
        except:
            return {}

async def save_events(data):
    lock = _ensure_lock()
    async with lock:
        os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)
        with open(DATA_PATH, "w") as f:
            json.dump(data, f, indent=2)

# -------------------- HELPERS --------------------

def resolve_target_channel_name(target: str):
    return SECURITY_ONLY_CHANNEL if target == "security_only" else PUBLIC_CHANNEL

def resolve_ping_mentions(guild: discord.Guild, target: str):
    roles = []
    sec = discord.utils.get(guild.roles, name=SECURITY_PING_ROLE)
    sub = discord.utils.get(guild.roles, name=SUBSIDIZED_PING_ROLE)

    if sec:
        roles.append(sec.mention)
    if target == "public" and sub:
        roles.append(sub.mention)

    return " ".join(roles)

# -------------------- EMBED --------------------

def build_event_embed(event):
    embed = discord.Embed(
        title=event["title"],
        description=event["description"],
        color=discord.Color.blue(),
        timestamp=datetime.now(timezone.utc)
    )

    embed.add_field(
        name="🕒 Time",
        value=f"<t:{event['timestamp']}:F>\n<t:{event['timestamp']}:R>",
        inline=False
    )

    capacities = event.get("capacities", {})
    roles = event.get("roles", {})

    for role, users in roles.items():
        cap = capacities.get(role)
        count = len(users)

        if cap:
            name = f"{role} ({count}/{cap})"
        else:
            name = f"{role} ({count})"

        value = "\n".join(f"<@{u}>" for u in users) or "_(none)_"
        embed.add_field(name=name, value=value, inline=False)

    return embed

async def refresh_event_message(bot, event_id):
    data = await load_events()
    event = data.get(event_id)
    if not event:
        return

    guild = bot.get_guild(event["guild_id"])
    channel = guild.get_channel(event["channel"])
    msg = await channel.fetch_message(event["message"])

    view = EventView(event_id, event["enabled_buttons"])
    embed = build_event_embed(event)

    await msg.edit(embed=embed, view=view)

# -------------------- MODAL --------------------

class EventInfoModal(discord.ui.Modal, title="Create Event"):
    name = discord.ui.TextInput(label="Event Name")
    description = discord.ui.TextInput(label="Description", style=discord.TextStyle.paragraph)
    time = discord.ui.TextInput(label="Date UTC (YYYY-MM-DD HH:MM)")
    buttons = discord.ui.TextInput(
        label="Buttons (use : for cap)",
        placeholder="Accept, Damage, Logi:5, Decline"
    )

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
        capacities = {}
        seen = set()

        for r in raw:
            r = r.strip()
            if ":" in r:
                name, cap = r.split(":")
                name = name.title()
                capacities[name] = int(cap)
            else:
                name = r.title()

            if name.lower() not in seen:
                seen.add(name.lower())
                buttons.append(name)

        if len(buttons) > 25:
            await interaction.response.send_message("Max 25 buttons.", ephemeral=True)
            return

        guild = interaction.guild
        channel = discord.utils.get(
            guild.text_channels,
            name=resolve_target_channel_name(self.target)
        )

        event_id = str(uuid.uuid4())

        event = {
            "title": self.name.value,
            "description": self.description.value,
            "timestamp": int(dt.timestamp()),
            "creator": self.creator_id,
            "guild_id": guild.id,
            "channel": channel.id,
            "message": None,
            "enabled_buttons": buttons,
            "capacities": capacities,
            "roles": {b: [] for b in buttons}
        }

        msg = await channel.send(
            content=resolve_ping_mentions(guild, self.target),
            embed=build_event_embed(event),
            view=EventView(event_id, buttons)
        )

        event["message"] = msg.id

        data = await load_events()
        data[event_id] = event
        await save_events(data)

        interaction.client.add_view(EventView(event_id, buttons), message_id=msg.id)

        await interaction.response.send_message("Event created.", ephemeral=True)

# -------------------- BUTTON --------------------

class RSVPButton(discord.ui.Button):
    def __init__(self, event_id, name, row=0):
        self.name = name
        safe = "".join(c if c.isalnum() else "_" for c in name.lower())

        super().__init__(
            label=name,
            style=self.get_style(name),
            custom_id=f"rsvp:{event_id}:{safe}",
            row=row
        )

    def get_style(self, name):
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
        event = data.get(self.view.event_id)

        uid = interaction.user.id
        roles = event["roles"]
        caps = event.get("capacities", {})

        cap = caps.get(self.name)
        current = roles.get(self.name, [])

        if cap and len(current) >= cap:
            await interaction.response.send_message(
                f"{self.name} is full.",
                ephemeral=True
            )
            return

        for r in roles:
            if uid in roles[r]:
                roles[r].remove(uid)

        roles[self.name].append(uid)

        await save_events(data)
        await refresh_event_message(interaction.client, self.view.event_id)

        await interaction.response.send_message(f"Registered as {self.name}", ephemeral=True)

# -------------------- VIEW --------------------

class EventView(discord.ui.View):
    def __init__(self, event_id, buttons):
        super().__init__(timeout=None)
        self.event_id = event_id

        for i, b in enumerate(buttons):
            self.add_item(RSVPButton(event_id, b, row=i // 5))

# -------------------- COG --------------------

class EventCreator(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="create_event")
    async def create_event(self, interaction: discord.Interaction):
        await interaction.response.send_modal(
            EventInfoModal(interaction.user.id, "public")
        )

    @commands.Cog.listener()
    async def on_ready(self):
        data = await load_events()
        for event_id, event in data.items():
            try:
                self.bot.add_view(
                    EventView(event_id, event["enabled_buttons"]),
                    message_id=event["message"]
                )
            except:
                pass

async def setup(bot):
    await bot.add_cog(EventCreator(bot))