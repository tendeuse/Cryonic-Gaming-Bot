# cogs/event_creator.py
#
# Event creator + RSVP system
# + Presence confirmation DMs at event time:
#   - At/after event time, the event creator receives 1 DM per participant:
#       "Was this participant present?" + Yes/No buttons
#   - For each "Yes":
#       - Event creator gets +5 AP
#       - Event creator gets +10% of that participant's AP earnings for the next 24h
#         (Applied by APTracking via /data/ap_boosts.json)
#       - NOT applicable if creator has role:
#           "ARC Security Administration Council" OR "ARC Security Corporation Leader"
#   - Logs confirmed participant list to #arc-hierarchy-log
#
# Double-confirm prevention:
#   - If a participant already has a presence value recorded for this event,
#     pressing buttons again will NOT award again (no changes).
#
# Boost stacking rules:
#   - Boosts do NOT stack.
#   - If an active boost already exists for (participant -> creator), it EXTENDS expiry only.
#
# FIXES INCLUDED:
#   - Stable custom_id values for persistent buttons
#   - Safer event posting with permission checks + surfaced errors
#   - Re-register persistent event views on startup
#   - More defensive message/channel fetches
#   - Prevent duplicate RSVP entries
#   - Better logging / diagnostics

import asyncio
import json
import os
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Set

import discord
from discord.ext import commands, tasks
from discord import app_commands

DATA_PATH = "/data/events.json"
BOOSTS_PATH = "/data/ap_boosts.json"  # consumed by ap_tracking.py

ANNOUNCEMENT_CHANNEL = "eve-announcements-as"
TEMP_ROLE_NAME = "Event Participant"
SECURITY_PING_ROLE = "ARC Security"

HIERARCHY_LOG_CH = "arc-hierarchy-log"

CREATOR_ROLES = {
    "ARC Petty Officer",
    "ARC Lieutenant",
    "ARC Commander",
    "ARC General",
    "ARC Security Administration Council",
    "ARC Security Corporation Leader",
}

# If creator has ANY of these roles, they do NOT receive the presence-confirm bonuses
PRESENCE_BONUS_EXCLUDED_ROLES = {
    "ARC Security Administration Council",
    "ARC Security Corporation Leader",
}

RSVP_TYPES = {"accept", "damage", "logi", "salvager", "tentative", "decline"}
ROLE_ASSIGN_TYPES = {"accept", "damage", "logi", "salvager"}

_lock: Optional[asyncio.Lock] = None


# -------------------- Persistence (atomic-ish) --------------------

def _ensure_lock() -> asyncio.Lock:
    global _lock
    if _lock is None:
        _lock = asyncio.Lock()
    return _lock


def _ensure_file(path: str, default_obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(default_obj, f, indent=2)


async def _load_json(path: str, default_obj: Any):
    lock = _ensure_lock()
    async with lock:
        _ensure_file(path, default_obj)
        try:
            with open(path, "r", encoding="utf-8") as f:
                txt = f.read().strip()
            if not txt:
                return default_obj
            data = json.loads(txt)
            return data if isinstance(data, type(default_obj)) else default_obj
        except json.JSONDecodeError:
            try:
                if os.path.exists(path):
                    os.replace(path, path + ".bak")
            except Exception:
                pass
            _ensure_file(path, default_obj)
            return default_obj
        except Exception:
            return default_obj


async def _save_json(path: str, data: Any):
    lock = _ensure_lock()
    async with lock:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)


async def load_events() -> Dict[str, Any]:
    data = await _load_json(DATA_PATH, {})
    return data if isinstance(data, dict) else {}


async def save_events(data: Dict[str, Any]) -> None:
    await _save_json(DATA_PATH, data)


async def load_boosts() -> Dict[str, Any]:
    data = await _load_json(BOOSTS_PATH, {"participants": {}})
    if not isinstance(data, dict):
        return {"participants": {}}
    data.setdefault("participants", {})
    if not isinstance(data["participants"], dict):
        data["participants"] = {}
    return data


async def save_boosts(data: Dict[str, Any]) -> None:
    await _save_json(BOOSTS_PATH, data)


# -------------------- Helpers --------------------

def has_role(member: discord.Member, role_name: str) -> bool:
    return any(r.name == role_name for r in member.roles)


def has_any_role(member: discord.Member, role_names: Set[str]) -> bool:
    return any(r.name in role_names for r in member.roles)


def get_bot_member(guild: discord.Guild, bot_user_id: Optional[int]) -> Optional[discord.Member]:
    if not bot_user_id:
        return None
    return guild.get_member(bot_user_id)


async def ensure_hierarchy_log_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    ch = discord.utils.get(guild.text_channels, name=HIERARCHY_LOG_CH)
    if ch:
        return ch
    try:
        return await guild.create_text_channel(HIERARCHY_LOG_CH)
    except Exception:
        return None


def compute_participants(event: Dict[str, Any]) -> List[int]:
    roles_map = event.get("roles", {})
    if not isinstance(roles_map, dict):
        return []

    participants: Set[int] = set()
    for role_name, ids in roles_map.items():
        if str(role_name).strip().lower() == "decline":
            continue
        if not isinstance(ids, list):
            continue
        for uid in ids:
            if isinstance(uid, int):
                participants.add(uid)

    return sorted(participants)


def current_confirmed_ids(event: Dict[str, Any]) -> List[int]:
    presence = event.get("presence", {})
    if not isinstance(presence, dict):
        return []

    out = []
    for k, v in presence.items():
        if v is True:
            try:
                out.append(int(k))
            except Exception:
                pass
    return sorted(set(out))


def build_signup_field_value(event: Dict[str, Any]) -> str:
    roles = event.get("roles", {})
    if not isinstance(roles, dict):
        return "_No signups yet._"

    lines = []
    for role_name, ids in roles.items():
        count = len(ids) if isinstance(ids, list) else 0
        lines.append(f"{role_name}: {count}")

    return "\n".join(lines) if lines else "_No signups yet._"


async def log_confirmed_participants(
    guild: discord.Guild,
    *,
    event_title: str,
    event_id: str,
    confirmed_ids: List[int],
) -> None:
    ch = await ensure_hierarchy_log_channel(guild)
    if not ch:
        return

    lines = []
    mentions = []
    for uid in confirmed_ids:
        m = guild.get_member(uid)
        if not m:
            continue
        mentions.append(m.mention)
        lines.append(f"- {m.display_name} ({m.mention})")

    embed = discord.Embed(
        title="Event Presence Confirmed",
        description=(
            f"**Event:** {event_title}\n"
            f"**Event ID:** `{event_id}`\n"
            f"**Confirmed present:** {len(lines)}\n\n"
            + ("\n".join(lines) if lines else "_(none)_")
        ),
        color=discord.Color.green(),
        timestamp=datetime.now(timezone.utc),
    )

    content = (" ".join(mentions))[:1800] if mentions else ""
    try:
        await ch.send(content=content, embed=embed)
    except Exception:
        pass


# -------------------- Presence DM View --------------------

class PresenceConfirmView(discord.ui.View):
    def __init__(self, *, event_id: str, participant_id: int):
        super().__init__(timeout=48 * 3600)
        self.event_id = event_id
        self.participant_id = participant_id

        yes_btn = discord.ui.Button(
            label="Yes",
            style=discord.ButtonStyle.success,
            custom_id=f"presence_yes:{event_id}:{participant_id}",
        )
        no_btn = discord.ui.Button(
            label="No",
            style=discord.ButtonStyle.danger,
            custom_id=f"presence_no:{event_id}:{participant_id}",
        )

        yes_btn.callback = self.yes_callback
        no_btn.callback = self.no_callback

        self.add_item(yes_btn)
        self.add_item(no_btn)

    async def _award_creator_5ap(
        self,
        guild: discord.Guild,
        creator: discord.Member,
        participant: discord.Member,
        event_title: str
    ) -> bool:
        try:
            from cogs.ap_tracking import award_ap_with_bonuses  # type: ignore
        except Exception:
            return False

        try:
            await award_ap_with_bonuses(
                guild=guild,
                earner=creator,
                base_amount=5.0,
                source="event presence confirmation",
                reason=f"Confirmed {participant.display_name} present for '{event_title}'",
                log=True,
                actor=None,
                distribution_embed=True,
                distribution_title="Event Creator Bonus",
            )
            return True
        except Exception:
            return False

    async def _register_or_extend_boost(self, *, creator_id: int, participant_id: int, event_id: str) -> bool:
        """
        Boost stacking rules:
          - No stacking.
          - If active boost already exists for (participant -> creator), extend expiry only.
        """
        try:
            boosts = await load_boosts()
            participants = boosts.setdefault("participants", {})
            if not isinstance(participants, dict):
                participants = {}
                boosts["participants"] = participants

            key = str(participant_id)
            lst = participants.setdefault(key, [])
            if not isinstance(lst, list):
                lst = []
                participants[key] = lst

            now = int(datetime.now(timezone.utc).timestamp())
            new_expires = now + int(timedelta(hours=24).total_seconds())

            found = False
            for entry in lst:
                if not isinstance(entry, dict):
                    continue
                if entry.get("beneficiary") != creator_id:
                    continue
                entry["percent"] = 0.10
                old_expires = int(entry.get("expires", 0) or 0)
                entry["expires"] = max(old_expires, new_expires)
                entry["event_id"] = str(event_id)
                found = True
                break

            if not found:
                lst.append({
                    "beneficiary": creator_id,
                    "percent": 0.10,
                    "expires": new_expires,
                    "event_id": str(event_id),
                })

            await save_boosts(boosts)
            return True
        except Exception:
            return False

    async def _handle(self, interaction: discord.Interaction, present: bool):
        events = await load_events()
        event = events.get(self.event_id)
        if not isinstance(event, dict):
            await interaction.response.send_message("Event not found.", ephemeral=True)
            return

        creator_id = event.get("creator")
        guild_id = event.get("guild_id")

        if not isinstance(creator_id, int) or interaction.user.id != creator_id:
            await interaction.response.send_message("Only the event creator can confirm presence.", ephemeral=True)
            return

        if not isinstance(guild_id, int):
            await interaction.response.send_message("Could not resolve guild for this event.", ephemeral=True)
            return

        guild = interaction.client.get_guild(guild_id)
        if not guild:
            await interaction.response.send_message("Guild not found.", ephemeral=True)
            return

        presence = event.setdefault("presence", {})
        if not isinstance(presence, dict):
            presence = {}
            event["presence"] = presence

        key = str(self.participant_id)
        if key in presence:
            for child in self.children:
                if isinstance(child, discord.ui.Button):
                    child.disabled = True
            try:
                await interaction.response.edit_message(
                    content="Already recorded for this participant (no changes applied).",
                    view=self
                )
            except Exception:
                await interaction.response.send_message(
                    "Already recorded for this participant (no changes applied).",
                    ephemeral=True
                )
            return

        presence[key] = bool(present)
        event["presence_updated_utc"] = datetime.now(timezone.utc).isoformat()

        asked = event.setdefault("presence_dm_sent_to", [])
        if not isinstance(asked, list):
            asked = []
            event["presence_dm_sent_to"] = asked
        if self.participant_id not in asked:
            asked.append(self.participant_id)

        bonus_ap = False
        boost_ok = False

        creator_member = guild.get_member(creator_id)
        participant_member = guild.get_member(self.participant_id)

        if present and creator_member and participant_member:
            if not has_any_role(creator_member, PRESENCE_BONUS_EXCLUDED_ROLES):
                bonus_ap = await self._award_creator_5ap(
                    guild,
                    creator_member,
                    participant_member,
                    str(event.get("title") or "Event")
                )
                boost_ok = await self._register_or_extend_boost(
                    creator_id=creator_member.id,
                    participant_id=participant_member.id,
                    event_id=self.event_id
                )

        await save_events(events)

        try:
            await log_confirmed_participants(
                guild,
                event_title=str(event.get("title") or "Event"),
                event_id=self.event_id,
                confirmed_ids=current_confirmed_ids(event),
            )
        except Exception:
            pass

        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True

        lines = [f"Saved: **{'Present' if present else 'Not present'}**."]
        if present:
            if creator_member and has_any_role(creator_member, PRESENCE_BONUS_EXCLUDED_ROLES):
                lines.append("No bonuses applied (creator excluded by role).")
            else:
                lines.append("Creator bonus: **+5 AP** applied." if bonus_ap else "Creator bonus: **+5 AP** (could not apply).")
                lines.append("Boost: **+10% of participant AP for 24h** registered/extended." if boost_ok else "Boost: could not register/extend.")

        try:
            await interaction.response.edit_message(content="\n".join(lines), view=self)
        except Exception:
            try:
                await interaction.response.send_message("\n".join(lines), ephemeral=True)
            except Exception:
                pass

    async def yes_callback(self, interaction: discord.Interaction):
        await self._handle(interaction, True)

    async def no_callback(self, interaction: discord.Interaction):
        await self._handle(interaction, False)


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

    def __init__(self, creator_id: int):
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

        if not selected_buttons:
            selected_buttons = {"accept", "damage", "logi", "salvager", "tentative", "decline"}

        event_id = str(uuid.uuid4())
        timestamp = int(event_dt.timestamp())

        embed = discord.Embed(
            title=self.name.value,
            description=self.description.value,
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )

        embed.add_field(
            name="🕒 Time",
            value=f"<t:{timestamp}:F>\n<t:{timestamp}:R>",
            inline=False
        )

        embed.add_field(
            name="📊 Fleet Signup",
            value="\n".join(f"{b.title()}: 0" for b in sorted(selected_buttons)),
            inline=False
        )

        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
            return

        channel = discord.utils.get(guild.text_channels, name=ANNOUNCEMENT_CHANNEL)
        if not channel:
            await interaction.response.send_message(
                f"Announcement channel `#{ANNOUNCEMENT_CHANNEL}` not found.",
                ephemeral=True
            )
            return

        bot_member = get_bot_member(guild, interaction.client.user.id if interaction.client.user else None)
        if not bot_member:
            await interaction.response.send_message("Could not resolve bot member in this guild.", ephemeral=True)
            return

        perms = channel.permissions_for(bot_member)
        if not perms.view_channel:
            await interaction.response.send_message(
                f"I do not have permission to view {channel.mention}.",
                ephemeral=True
            )
            return

        if not perms.send_messages:
            await interaction.response.send_message(
                f"I do not have permission to send messages in {channel.mention}.",
                ephemeral=True
            )
            return

        if not perms.embed_links:
            await interaction.response.send_message(
                f"I do not have permission to embed links in {channel.mention}.",
                ephemeral=True
            )
            return

        view = EventView(event_id, selected_buttons, self.redirect_url.value.strip())

        try:
            security_role = discord.utils.get(guild.roles, name=SECURITY_PING_ROLE)
            content = security_role.mention if security_role else None

            msg = await channel.send(
                content=content,
                embed=embed,
                view=view,
                allowed_mentions=discord.AllowedMentions(roles=True)
            )
        except Exception as e:
            await interaction.response.send_message(
                f"Failed to post event message in {channel.mention}: `{type(e).__name__}: {e}`",
                ephemeral=True
            )
            return

        data = await load_events()
        data[event_id] = {
            "title": self.name.value,
            "creator": self.creator_id,
            "timestamp": timestamp,
            "guild_id": guild.id,
            "channel": channel.id,
            "message": msg.id,
            "roles": {b.title(): [] for b in sorted(selected_buttons)},
            "redirect_url": self.redirect_url.value.strip(),
            "active": True,
            "presence": {},
            "presence_dm_sent_to": [],
            "presence_started": False,
            "presence_started_utc": None,
            "created_utc": datetime.now(timezone.utc).isoformat(),
        }
        await save_events(data)

        await interaction.response.send_message(
            f"Event created successfully in {channel.mention}.",
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

    def __init__(self, event_id: str):
        super().__init__()
        self.event_id = event_id

    async def on_submit(self, interaction: discord.Interaction):
        data = await load_events()
        event = data.get(self.event_id)
        if not isinstance(event, dict):
            await interaction.response.send_message("Event not found.", ephemeral=True)
            return

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

        event["timestamp"] = int(event_dt.timestamp())

        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Guild not found.", ephemeral=True)
            return

        channel = guild.get_channel(event.get("channel"))
        if not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message("Event channel missing.", ephemeral=True)
            await save_events(data)
            return

        try:
            msg = await channel.fetch_message(event["message"])
        except Exception as e:
            await interaction.response.send_message(
                f"Could not fetch event message: `{type(e).__name__}: {e}`",
                ephemeral=True
            )
            await save_events(data)
            return

        if not msg.embeds:
            await interaction.response.send_message("Event message has no embed to edit.", ephemeral=True)
            return

        old_embed = msg.embeds[0]
        embed = discord.Embed.from_dict(old_embed.to_dict())
        embed.description = self.description.value

        if len(embed.fields) >= 1:
            embed.set_field_at(
                0,
                name="🕒 Time",
                value=f"<t:{event['timestamp']}:F>\n<t:{event['timestamp']}:R>",
                inline=False
            )
        else:
            embed.add_field(
                name="🕒 Time",
                value=f"<t:{event['timestamp']}:F>\n<t:{event['timestamp']}:R>",
                inline=False
            )

        try:
            await msg.edit(embed=embed)
        except Exception as e:
            await interaction.response.send_message(
                f"Failed to edit event message: `{type(e).__name__}: {e}`",
                ephemeral=True
            )
            return

        await save_events(data)
        await interaction.response.send_message("Event updated.", ephemeral=True)


# -------------------- EVENT VIEW --------------------

class EventView(discord.ui.View):
    def __init__(self, event_id: str, buttons: Set[str], redirect_url: str):
        super().__init__(timeout=None)
        self.event_id = event_id

        for b in sorted(buttons):
            self.add_item(RSVPButton(event_id, b.title()))

        if redirect_url:
            self.add_item(
                discord.ui.Button(
                    label="External Signup",
                    url=redirect_url,
                    style=discord.ButtonStyle.link
                )
            )

        self.add_item(ManageEventButton(event_id))


class RSVPButton(discord.ui.Button):
    def __init__(self, event_id: str, rsvp_type: str):
        super().__init__(
            label=rsvp_type,
            style=discord.ButtonStyle.primary,
            custom_id=f"event_rsvp:{event_id}:{rsvp_type.lower()}",
        )
        self.rsvp_type = rsvp_type.lower()

    async def callback(self, interaction: discord.Interaction):
        if not self.view or not isinstance(self.view, EventView):
            await interaction.response.send_message("Invalid event view.", ephemeral=True)
            return

        data = await load_events()
        event = data.get(self.view.event_id)
        if not isinstance(event, dict):
            await interaction.response.send_message("Event not found.", ephemeral=True)
            return

        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Guild not found.", ephemeral=True)
            return

        member = interaction.user
        if not isinstance(member, discord.Member):
            await interaction.response.send_message("Member not found.", ephemeral=True)
            return

        uid = member.id
        temp_role = discord.utils.get(guild.roles, name=TEMP_ROLE_NAME)

        roles_map = event.setdefault("roles", {})
        if not isinstance(roles_map, dict):
            roles_map = {}
            event["roles"] = roles_map

        for users in roles_map.values():
            if isinstance(users, list):
                while uid in users:
                    users.remove(uid)

        roles_map.setdefault(self.rsvp_type.title(), [])
        if uid not in roles_map[self.rsvp_type.title()]:
            roles_map[self.rsvp_type.title()].append(uid)

        try:
            if temp_role:
                if self.rsvp_type in ROLE_ASSIGN_TYPES:
                    await member.add_roles(temp_role, reason="Event RSVP role assignment")
                else:
                    await member.remove_roles(temp_role, reason="Event RSVP role removal")
        except Exception:
            pass

        await save_events(data)

        channel = guild.get_channel(event.get("channel"))
        if isinstance(channel, discord.TextChannel):
            try:
                msg = await channel.fetch_message(event["message"])
                if msg.embeds:
                    embed = discord.Embed.from_dict(msg.embeds[0].to_dict())
                    if len(embed.fields) >= 2:
                        embed.set_field_at(
                            1,
                            name="📊 Fleet Signup",
                            value=build_signup_field_value(event),
                            inline=False
                        )
                    else:
                        embed.add_field(
                            name="📊 Fleet Signup",
                            value=build_signup_field_value(event),
                            inline=False
                        )
                    await msg.edit(embed=embed, view=self.view)
            except Exception:
                pass

        await interaction.response.send_message(
            f"Registered as **{self.rsvp_type.title()}**.",
            ephemeral=True
        )


class ManageEventButton(discord.ui.Button):
    def __init__(self, event_id: str):
        super().__init__(
            label="⚙ Manage Event",
            style=discord.ButtonStyle.secondary,
            custom_id=f"event_manage:{event_id}",
        )

    async def callback(self, interaction: discord.Interaction):
        if not self.view or not isinstance(self.view, EventView):
            await interaction.response.send_message("Invalid event view.", ephemeral=True)
            return

        data = await load_events()
        event = data.get(self.view.event_id)
        if not isinstance(event, dict):
            await interaction.response.send_message("Event not found.", ephemeral=True)
            return

        if (
            interaction.user.id != event.get("creator")
            and not interaction.user.guild_permissions.administrator
        ):
            await interaction.response.send_message(
                "You are not authorized to manage this event.",
                ephemeral=True
            )
            return

        await interaction.response.send_modal(EditEventModal(self.view.event_id))


# -------------------- COG --------------------

class EventCreator(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._views_registered = False
        if not self.presence_loop.is_running():
            self.presence_loop.start()

    def cog_unload(self):
        if self.presence_loop.is_running():
            self.presence_loop.cancel()

    def can_create(self, member: discord.Member) -> bool:
        return any(role.name in CREATOR_ROLES for role in member.roles)

    async def register_persistent_views(self):
        if self._views_registered:
            return

        data = await load_events()
        for event_id, event in data.items():
            if not isinstance(event, dict):
                continue
            if not event.get("active", True):
                continue

            roles_map = event.get("roles", {})
            buttons = {
                str(k).strip().lower()
                for k in roles_map.keys()
                if str(k).strip().lower() in RSVP_TYPES
            }
            if not buttons:
                buttons = {"accept", "damage", "logi", "salvager", "tentative", "decline"}

            redirect_url = str(event.get("redirect_url") or "")
            try:
                self.bot.add_view(EventView(event_id, buttons, redirect_url))
            except Exception as e:
                print(f"[event_creator] Failed to add persistent view for {event_id}: {type(e).__name__}: {e}")

        self._views_registered = True
        print(f"[event_creator] Registered persistent views for {len(data)} event(s).")

    @app_commands.command(name="create_event", description="Create a new event")
    async def create_event(self, interaction: discord.Interaction):
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
            return

        if not self.can_create(interaction.user):
            await interaction.response.send_message(
                "You are not authorized to create events.",
                ephemeral=True
            )
            return

        await interaction.response.send_modal(CreateEventModal(interaction.user.id))

    @tasks.loop(seconds=60)
    async def presence_loop(self):
        data = await load_events()
        if not data:
            return

        now_ts = int(datetime.now(timezone.utc).timestamp())
        changed = False

        for event_id, event in list(data.items()):
            if not isinstance(event, dict):
                continue
            if not event.get("active", True):
                continue

            ts = event.get("timestamp")
            if not isinstance(ts, int):
                continue
            if now_ts < ts:
                continue
            if event.get("presence_started") is True:
                continue

            guild_id = event.get("guild_id")
            creator_id = event.get("creator")
            if not isinstance(guild_id, int) or not isinstance(creator_id, int):
                continue

            guild = self.bot.get_guild(guild_id)
            if not guild:
                continue

            creator = guild.get_member(creator_id)
            if not creator:
                continue

            participants = compute_participants(event)

            event["presence_started"] = True
            event["presence_started_utc"] = datetime.now(timezone.utc).isoformat()
            changed = True

            sent_list = event.setdefault("presence_dm_sent_to", [])
            if not isinstance(sent_list, list):
                sent_list = []
                event["presence_dm_sent_to"] = sent_list

            if not participants:
                continue

            for pid in participants:
                if pid in sent_list:
                    continue

                member = guild.get_member(pid)
                sent_list.append(pid)
                changed = True

                if not member:
                    continue

                try:
                    dm = await creator.create_dm()
                    embed = discord.Embed(
                        title="Was this participant present?",
                        description=(
                            f"**Event:** {event.get('title', 'Event')}\n"
                            f"**Participant:** {member.display_name} ({member.mention})\n"
                            f"**Event time:** <t:{ts}:F>"
                        ),
                        color=discord.Color.blurple(),
                        timestamp=datetime.now(timezone.utc),
                    )
                    view = PresenceConfirmView(event_id=event_id, participant_id=pid)
                    await dm.send(embed=embed, view=view)
                except discord.Forbidden:
                    print(f"[event_creator] Could not DM creator {creator_id} for event {event_id} (DMs closed).")
                    break
                except Exception as e:
                    print(f"[event_creator] Failed sending presence DM for event {event_id}, participant {pid}: {type(e).__name__}: {e}")
                    continue

        if changed:
            await save_events(data)

    @presence_loop.before_loop
    async def _before_presence_loop(self):
        await self.bot.wait_until_ready()

    @commands.Cog.listener()
    async def on_ready(self):
        try:
            await self.register_persistent_views()
        except Exception as e:
            print(f"[event_creator] Persistent view registration failed: {type(e).__name__}: {e}")

        for g in self.bot.guilds:
            try:
                await ensure_hierarchy_log_channel(g)
            except Exception:
                pass


async def setup(bot: commands.Bot):
    await bot.add_cog(EventCreator(bot))