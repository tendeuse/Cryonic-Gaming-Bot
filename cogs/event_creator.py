# cogs/event_creator.py
#
# Full-featured Event Creator + RSVP system
# ==========================================
#
# RESTORED FEATURES (from previous version)
# ------------------------------------------
# 1. Role-gated /create_event  (CREATOR_ROLES)
# 2. Two-step modal: Step 1 (details + button count) → Step 2 (one field per button name)
# 3. Per-button capacity limits  (old "Logi:5" events continue to work)
# 4. Custom RSVP button names — no hardcoded filter
# 5. Audience shown on embed + correct ping routing
# 6. Presence loop: DMs creator at event time to confirm who showed up
# 7. AP reward:  +5 AP per confirmed participant (paid to creator)
# 8. Earnings boost: +10% of participant's AP for 24 h (paid to creator)
# 9. CEO / Director excluded from AP bonuses
# 10. Confirmed participants logged to #arc-hierarchy-log
# 11. Edit event: change description + time after posting (via Manage button)
# 12. Creator display-name in embed footer
# 13. Correct persistent-view registration (for_registration flag strips link button)
#
# NEW FEATURE
# -----------
# /event_log [member]  — participation report (ephemeral; text-file if too long)
#   • No member: every event with its full signup list, newest first
#   • With member: every event that member signed up for + confirmation status
#
# BACKWARD COMPATIBILITY — no migration needed
# --------------------------------------------
# • Old events that use "buttons" key → normalised transparently
# • Old events that use "closed=True" → normalised to "active=False"
# • Old events missing presence/target/redirect fields → safe defaults added at
#   read-time; values written back lazily on first interaction
# • Capacity limits from old events ("Logi:5") continue to work unchanged
# • Existing messages keep working because RSVPButton custom_id generation is
#   identical to the simple version ("rsvp:<event_id>:<safe_name>")

import asyncio
import io
import json
import os
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set

import discord
from discord.ext import commands, tasks
from discord import app_commands

# ============================================================
# PATHS
# ============================================================
DATA_PATH   = "/data/events.json"
BOOSTS_PATH = "/data/ap_boosts.json"

# ============================================================
# CONFIG
# ============================================================
SECURITY_ONLY_CHANNEL = "wh-op-sec-events"
PUBLIC_CHANNEL        = "eve-announcements"

TEMP_ROLE_NAME        = "Event Participant"
SECURITY_PING_ROLE    = "ARC Security"
SUBSIDIZED_PING_ROLE  = "ARC Subsidized"
HIERARCHY_LOG_CH      = "arc-hierarchy-log"

# Only these roles may run /create_event
CREATOR_ROLES: Set[str] = {
    "ARC Petty Officer",
    "ARC Lieutenant",
    "ARC Commander",
    "ARC General",
    "ARC Security Administration Council",
    "ARC Security Corporation Leader",
}

# These roles receive no AP bonus when they are the event creator
PRESENCE_BONUS_EXCLUDED_ROLES: Set[str] = {
    "ARC Security Administration Council",
    "ARC Security Corporation Leader",
}

# Clicking these button names grants the "Event Participant" temp-role
ROLE_ASSIGN_TYPES: Set[str] = {"accept", "damage", "logi", "salvager"}

# Preferred display order for known button types
DISPLAY_ORDER: List[str] = ["Accept", "Damage", "Logi", "Salvager", "Tentative", "Decline"]


# ============================================================
# PERSISTENCE
# ============================================================
_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _lock
    if _lock is None:
        _lock = asyncio.Lock()
    return _lock


def _atomic_write(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


async def load_events() -> Dict[str, Any]:
    async with _get_lock():
        if not os.path.exists(DATA_PATH):
            return {}
        try:
            with open(DATA_PATH, "r", encoding="utf-8") as f:
                txt = f.read().strip()
            if not txt:
                return {}
            data = json.loads(txt)
            return data if isinstance(data, dict) else {}
        except json.JSONDecodeError:
            try:
                os.replace(DATA_PATH, DATA_PATH + ".bak")
            except Exception:
                pass
            return {}
        except Exception:
            return {}


async def save_events(data: Dict[str, Any]) -> None:
    async with _get_lock():
        _atomic_write(DATA_PATH, data)


async def load_boosts() -> Dict[str, Any]:
    async with _get_lock():
        if not os.path.exists(BOOSTS_PATH):
            return {"participants": {}}
        try:
            with open(BOOSTS_PATH, "r", encoding="utf-8") as f:
                txt = f.read().strip()
            if not txt:
                return {"participants": {}}
            data = json.loads(txt)
            if not isinstance(data, dict):
                return {"participants": {}}
            data.setdefault("participants", {})
            return data
        except Exception:
            return {"participants": {}}


async def save_boosts(data: Dict[str, Any]) -> None:
    async with _get_lock():
        _atomic_write(BOOSTS_PATH, data)


# ============================================================
# EVENT NORMALIZATION  (non-destructive — only adds missing keys)
# ============================================================

def normalize_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure every expected key exists, regardless of which version created the event.

    Old format:  buttons, capacities, closed
    New format:  enabled_buttons, active, target, redirect_url,
                 presence, presence_dm_sent_to, presence_started, created_utc

    This is SAFE to call multiple times — it never overwrites existing data.
    Normalised keys are written back the next time the event is saved
    (lazy migration — no up-front data conversion required).
    """
    # ── button list ──────────────────────────────────────────────────────────
    # Always keep BOTH keys in sync so old code paths and new code paths work.
    if "enabled_buttons" not in event:
        event["enabled_buttons"] = list(event.get("buttons", []))
    if "buttons" not in event:
        event["buttons"] = list(event.get("enabled_buttons", []))

    # ── status flags ─────────────────────────────────────────────────────────
    if "active" not in event:
        event["active"] = not bool(event.get("closed", False))
    if "closed" not in event:
        event["closed"] = not bool(event.get("active", True))

    # ── presence ─────────────────────────────────────────────────────────────
    event.setdefault("presence",            {})
    event.setdefault("presence_dm_sent_to", [])
    event.setdefault("presence_started",    False)
    event.setdefault("presence_started_utc", None)

    # ── other new fields ─────────────────────────────────────────────────────
    event.setdefault("target",       "security_only")
    event.setdefault("redirect_url", "")
    event.setdefault("link_only",    False)
    event.setdefault("created_utc",  None)
    event.setdefault("capacities",   {})

    # ── ensure roles dict has an entry for every button ───────────────────────
    roles = event.setdefault("roles", {})
    for btn in event["enabled_buttons"]:
        roles.setdefault(btn, [])

    return event


# ============================================================
# HELPERS
# ============================================================

def has_any_role(member: discord.Member, role_names: Set[str]) -> bool:
    return any(r.name in role_names for r in member.roles)


def get_enabled_button_titles(event: Dict[str, Any]) -> List[str]:
    """Return the ordered list of RSVP button labels for an event."""
    enabled = event.get("enabled_buttons") or event.get("buttons", [])
    if isinstance(enabled, list) and enabled:
        seen:   Set[str]  = set()
        result: List[str] = []
        for b in enabled:
            t = str(b).strip().title()
            if t and t.lower() not in seen:
                seen.add(t.lower())
                result.append(t)
        if result:
            return sorted(
                result,
                key=lambda x: DISPLAY_ORDER.index(x) if x in DISPLAY_ORDER else 999,
            )
    return ["Accept", "Decline"]


def compute_participants(event: Dict[str, Any]) -> List[int]:
    """User IDs of everyone who signed up, excluding Decline."""
    roles   = event.get("roles", {})
    enabled = set(get_enabled_button_titles(event))
    out:    Set[int] = set()
    for role_name, ids in roles.items():
        if role_name.title() == "Decline":
            continue
        if role_name.title() not in enabled:
            continue
        for uid in ids:
            if isinstance(uid, int):
                out.add(uid)
    return sorted(out)


def current_confirmed_ids(event: Dict[str, Any]) -> List[int]:
    presence = event.get("presence", {})
    return sorted({int(k) for k, v in presence.items() if v is True})


def target_label(target: str) -> str:
    return {
        "security_only": "Security Only",
        "public":         "Security + Subsidized",
    }.get(target, "Security Only")


def resolve_channel(
    guild: discord.Guild, target: str
) -> Optional[discord.TextChannel]:
    name = SECURITY_ONLY_CHANNEL if target == "security_only" else PUBLIC_CHANNEL
    return discord.utils.get(guild.text_channels, name=name)


def resolve_ping(guild: discord.Guild, target: str) -> str:
    parts = []
    sec = discord.utils.get(guild.roles, name=SECURITY_PING_ROLE)
    sub = discord.utils.get(guild.roles, name=SUBSIDIZED_PING_ROLE)
    if sec:
        parts.append(sec.mention)
    if target == "public" and sub:
        parts.append(sub.mention)
    return " ".join(parts)


def chunk_lines(lines: List[str], max_len: int = 1000) -> List[str]:
    if not lines:
        return ["_(none)_"]
    chunks:  List[str] = []
    current: str       = ""
    for line in lines:
        candidate = (current + "\n" + line).lstrip("\n") if current else line
        if len(candidate) > max_len:
            if current:
                chunks.append(current)
            current = line
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks or ["_(none)_"]


# ============================================================
# EMBED BUILDER
# ============================================================

def build_embed(
    event: Dict[str, Any],
    guild: Optional[discord.Guild] = None,
) -> discord.Embed:
    """
    Builds the event embed.  Works with both old (buttons/closed) and
    new (enabled_buttons/active) event records.
    """
    event    = normalize_event(event)
    is_active = event.get("active", True) and not event.get("closed", False)

    embed = discord.Embed(
        title=       str(event.get("title", "Event")),
        description= str(event.get("description", "")),
        color=       discord.Color.blue() if is_active else discord.Color.dark_grey(),
        timestamp=   datetime.now(timezone.utc),
    )

    # Time
    ts = event.get("timestamp", 0)
    if ts:
        embed.add_field(
            name="🕒 Time",
            value=f"<t:{ts}:F>\n<t:{ts}:R>",
            inline=False,
        )

    # Audience + buttons summary (inline pair)
    embed.add_field(
        name="📡 Audience",
        value=target_label(event.get("target", "security_only")),
        inline=True,
    )
    buttons = get_enabled_button_titles(event)
    embed.add_field(
        name="🎛 RSVP Options",
        value=", ".join(buttons) if buttons else "None",
        inline=True,
    )

    # Per-button attendance fields
    roles      = event.get("roles", {})
    capacities = event.get("capacities", {})

    for btn in buttons:
        users = roles.get(btn, [])
        cap   = capacities.get(btn)
        count = len(users)

        field_name = f"{btn} ({count}/{cap})" if cap else f"{btn} ({count})"

        if guild:
            lines = []
            for uid in users:
                m = guild.get_member(uid)
                lines.append(f"- {m.display_name}" if m else f"- <@{uid}>")
        else:
            lines = [f"- <@{uid}>" for uid in users]

        for i, chunk in enumerate(chunk_lines(lines, 900)):
            embed.add_field(
                name=  field_name if i == 0 else f"{btn} (cont.)",
                value= chunk,
                inline=False,
            )

    # Creator in footer
    creator_id = event.get("creator")
    if creator_id:
        name = str(creator_id)
        if guild:
            m = guild.get_member(int(creator_id))
            if m:
                name = m.display_name
        embed.set_footer(text=f"Created by {name}")

    if not is_active:
        embed.title = (embed.title or "Event") + " [CLOSED]"

    return embed


# ============================================================
# REFRESH
# ============================================================

async def refresh(bot: commands.Bot, event_id: str) -> None:
    data  = await load_events()
    event = data.get(event_id)
    if not isinstance(event, dict):
        return

    event = normalize_event(event)

    guild_id = event.get("guild_id")
    if not guild_id:
        print(f"[event_creator] Event {event_id} missing guild_id — skipping refresh")
        return

    guild = bot.get_guild(guild_id)
    if not guild:
        return

    channel = guild.get_channel(event.get("channel"))
    if not isinstance(channel, discord.TextChannel):
        return

    try:
        msg = await channel.fetch_message(event["message"])
    except Exception:
        return

    buttons     = get_enabled_button_titles(event)
    redirect    = event.get("redirect_url", "")
    link_only   = bool(event.get("link_only", False))
    capacities  = event.get("capacities", {})
    roles       = event.get("roles", {})
    is_active   = event.get("active", True) and not event.get("closed", False)

    view = EventView(event_id, buttons, redirect, link_only=link_only)

    # Disable capacity-full buttons
    for item in view.children:
        if isinstance(item, RSVPButton):
            cap = capacities.get(item.label)
            if cap and len(roles.get(item.label, [])) >= int(cap):
                item.disabled = True

    # Disable everything if closed
    if not is_active:
        for item in view.children:
            item.disabled = True

    try:
        await msg.edit(embed=build_embed(event, guild), view=view)
        # Re-register persistent view so it survives the next restart
        try:
            bot.add_view(
                EventView(event_id, buttons, "", link_only=link_only, for_registration=True),
                message_id=event["message"],
            )
        except Exception:
            pass
    except Exception as e:
        print(f"[event_creator] refresh failed for {event_id}: {e}")


# ============================================================
# AP / BOOST HELPERS
# ============================================================

async def award_creator_5ap(
    guild:       discord.Guild,
    creator:     discord.Member,
    participant: discord.Member,
    event_title: str,
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


async def register_or_extend_boost(
    *,
    creator_id:    int,
    participant_id: int,
    event_id:      str,
) -> bool:
    try:
        boosts    = await load_boosts()
        parts     = boosts.setdefault("participants", {})
        key       = str(participant_id)
        lst       = parts.setdefault(key, [])
        if not isinstance(lst, list):
            lst = []
            parts[key] = lst

        now     = int(datetime.now(timezone.utc).timestamp())
        new_exp = now + int(timedelta(hours=24).total_seconds())

        found = False
        for entry in lst:
            if not isinstance(entry, dict):
                continue
            if entry.get("beneficiary") != creator_id:
                continue
            entry["percent"] = 0.10
            entry["expires"] = max(int(entry.get("expires", 0) or 0), new_exp)
            entry["event_id"] = event_id
            found = True
            break

        if not found:
            lst.append({
                "beneficiary": creator_id,
                "percent":     0.10,
                "expires":     new_exp,
                "event_id":    event_id,
            })

        await save_boosts(boosts)
        return True
    except Exception:
        return False


# ============================================================
# HIERARCHY LOG
# ============================================================

async def ensure_hierarchy_log_channel(
    guild: discord.Guild,
) -> Optional[discord.TextChannel]:
    ch = discord.utils.get(guild.text_channels, name=HIERARCHY_LOG_CH)
    if ch:
        return ch
    try:
        return await guild.create_text_channel(HIERARCHY_LOG_CH)
    except Exception:
        return None


async def log_confirmed_participants(
    guild:       discord.Guild,
    *,
    event_title: str,
    event_id:    str,
    confirmed_ids: List[int],
) -> None:
    ch = await ensure_hierarchy_log_channel(guild)
    if not ch:
        return

    lines    = []
    mentions = []
    for uid in confirmed_ids:
        m = guild.get_member(uid)
        if m:
            mentions.append(m.mention)
            lines.append(f"- {m.display_name} ({m.mention})")
        else:
            lines.append(f"- <@{uid}>")

    embed = discord.Embed(
        title=       "Event Presence Confirmed",
        description= (
            f"**Event:** {event_title}\n"
            f"**Event ID:** `{event_id}`\n"
            f"**Confirmed present:** {len(lines)}\n\n"
            + ("\n".join(lines) if lines else "_(none)_")
        ),
        color=     discord.Color.green(),
        timestamp= datetime.now(timezone.utc),
    )
    content = (" ".join(mentions))[:1800] if mentions else ""
    try:
        await ch.send(content=content, embed=embed)
    except Exception:
        pass


# ============================================================
# PRESENCE CONFIRM VIEW  (DM buttons sent to the event creator)
# ============================================================

class PresenceConfirmView(discord.ui.View):
    """
    Sent to the event creator via DM for each participant.
    Timeout=48h — not persistent (same behaviour as the previous full version).
    If the bot restarts mid-window the buttons stop responding, which is a
    known Discord limitation for non-persistent DM views.
    """

    def __init__(self, *, event_id: str, participant_id: int):
        super().__init__(timeout=48 * 3600)
        self.event_id       = event_id
        self.participant_id = participant_id

        yes = discord.ui.Button(
            label="Yes", style=discord.ButtonStyle.success,
            custom_id=f"presence_yes:{event_id}:{participant_id}",
        )
        no = discord.ui.Button(
            label="No", style=discord.ButtonStyle.danger,
            custom_id=f"presence_no:{event_id}:{participant_id}",
        )
        yes.callback = self.yes_callback
        no.callback  = self.no_callback
        self.add_item(yes)
        self.add_item(no)

    async def _handle(self, interaction: discord.Interaction, present: bool) -> None:
        events = await load_events()
        event  = events.get(self.event_id)
        if not isinstance(event, dict):
            await interaction.response.send_message("Event not found.", ephemeral=True)
            return

        event = normalize_event(event)

        creator_id = event.get("creator")
        guild_id   = event.get("guild_id")

        if not isinstance(creator_id, int) or interaction.user.id != creator_id:
            await interaction.response.send_message(
                "Only the event creator can confirm presence.", ephemeral=True
            )
            return

        guild = interaction.client.get_guild(guild_id) if isinstance(guild_id, int) else None
        if not guild:
            await interaction.response.send_message("Guild not found.", ephemeral=True)
            return

        presence = event.setdefault("presence", {})
        key      = str(self.participant_id)

        # Already recorded
        if key in presence:
            for child in self.children:
                if isinstance(child, discord.ui.Button):
                    child.disabled = True
            try:
                await interaction.response.edit_message(
                    content="Already recorded (no changes made).", view=self
                )
            except Exception:
                await interaction.response.send_message(
                    "Already recorded.", ephemeral=True
                )
            return

        presence[key]          = present
        event["presence"]      = presence
        events[self.event_id]  = event
        await save_events(events)

        bonus_ap  = False
        boost_ok  = False
        creator_m = guild.get_member(creator_id)
        part_m    = guild.get_member(self.participant_id)

        if present and creator_m and part_m:
            if not has_any_role(creator_m, PRESENCE_BONUS_EXCLUDED_ROLES):
                bonus_ap = await award_creator_5ap(
                    guild, creator_m, part_m,
                    str(event.get("title", "Event")),
                )
                boost_ok = await register_or_extend_boost(
                    creator_id=    creator_m.id,
                    participant_id=part_m.id,
                    event_id=      self.event_id,
                )

        try:
            await log_confirmed_participants(
                guild,
                event_title=    str(event.get("title", "Event")),
                event_id=       self.event_id,
                confirmed_ids=  current_confirmed_ids(event),
            )
        except Exception:
            pass

        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True

        lines = [f"Saved: **{'Present' if present else 'Not present'}**."]
        if present:
            if creator_m and has_any_role(creator_m, PRESENCE_BONUS_EXCLUDED_ROLES):
                lines.append("No bonuses (creator excluded by role).")
            else:
                lines.append(
                    "Creator bonus: **+5 AP** "
                    + ("applied." if bonus_ap else "(could not apply — ap_tracking missing?).")
                )
                lines.append(
                    "Boost: **+10% for 24 h** "
                    + ("registered." if boost_ok else "(could not register).")
                )
        try:
            await interaction.response.edit_message(content="\n".join(lines), view=self)
        except Exception:
            await interaction.response.send_message("\n".join(lines), ephemeral=True)

    async def yes_callback(self, interaction: discord.Interaction):
        await self._handle(interaction, True)

    async def no_callback(self, interaction: discord.Interaction):
        await self._handle(interaction, False)


# ============================================================
# CREATE EVENT — STEP 1: EVENT INFO MODAL
# ============================================================

class EventInfoModal(discord.ui.Modal, title="Create Event — Step 1 of 2"):
    """
    Collects event details.
    button_count field:
      • Enter 1–5  → that many RSVP buttons (step 2 asks for names)
      • Leave blank OR enter 0 when a Redirect URL is provided
        → RSVP buttons are replaced by the link alone (link-only mode)
    """

    event_name = discord.ui.TextInput(
        label="Event Name", max_length=100, required=True,
    )
    description = discord.ui.TextInput(
        label="Description",
        style=discord.TextStyle.paragraph,
        max_length=1000, required=True,
    )
    datetime_utc = discord.ui.TextInput(
        label="Date & Time (UTC)  —  YYYY-MM-DD HH:MM",
        placeholder="e.g. 2025-06-15 20:00",
        required=True,
    )
    button_count = discord.ui.TextInput(
        label="RSVP Buttons (1–5, or 0 to use link only)",
        placeholder="0 = link only,  3 = three RSVP buttons",
        min_length=1, max_length=1, required=True,
    )
    redirect_url = discord.ui.TextInput(
        label="Redirect URL  (leave blank if none)",
        placeholder="https://...",
        required=False,
    )

    def __init__(self, creator_id: int, target: str):
        super().__init__()
        self.creator_id = creator_id
        self.target     = target

    async def on_submit(self, interaction: discord.Interaction):
        raw = self.button_count.value.strip()
        if not raw.isdigit() or not (0 <= int(raw) <= 5):
            await interaction.response.send_message(
                "❌ Button count must be **0 – 5**.\n"
                "`0` = link-only mode (requires a Redirect URL).\n"
                "`1–5` = that many named RSVP buttons.",
                ephemeral=True,
            )
            return

        count       = int(raw)
        link_only   = (count == 0)
        redir       = self.redirect_url.value.strip()

        if link_only and not redir:
            await interaction.response.send_message(
                "❌ Button count 0 (link-only) requires a **Redirect URL**.",
                ephemeral=True,
            )
            return

        try:
            dt = datetime.strptime(
                self.datetime_utc.value.strip(), "%Y-%m-%d %H:%M"
            ).replace(tzinfo=timezone.utc)
        except ValueError:
            await interaction.response.send_message(
                "❌ Invalid date. Use `YYYY-MM-DD HH:MM` (UTC).", ephemeral=True
            )
            return

        partial = {
            "creator_id":   self.creator_id,
            "target":       self.target,
            "event_name":   self.event_name.value.strip(),
            "description":  self.description.value.strip(),
            "timestamp":    int(dt.timestamp()),
            "redirect_url": redir,
            "button_count": count,
            "link_only":    link_only,
        }

        if link_only:
            # Skip step 2 — post directly with just the link button
            await interaction.response.send_modal(ButtonNamesModal(partial))
        else:
            await interaction.response.send_modal(ButtonNamesModal(partial))


# ============================================================
# CREATE EVENT — STEP 2: BUTTON NAMES MODAL (DYNAMICALLY BUILT)
# ============================================================

class ButtonNamesModal(discord.ui.Modal):
    """
    Dynamic modal:
    • link_only=True  →  title says "Step 1 of 1", no fields added, posts directly.
    • link_only=False →  one field per button; called as "Step 2 of 2".

    Discord allows up to 5 components in a modal, matching the 1–5 button limit.
    When link_only is True, on_submit is reached immediately (no extra fields).
    """

    def __init__(self, partial: Dict[str, Any]):
        count     = int(partial.get("button_count", 0))
        link_only = bool(partial.get("link_only", False))

        if link_only:
            super().__init__(title="Create Event — Confirm (Link Only)")
        else:
            super().__init__(title=f"Create Event — Step 2 of 2  ({count} buttons)")

        self.partial   = partial
        self._inputs: List[discord.ui.TextInput] = []

        if not link_only:
            examples = ["Accept", "Damage", "Logi", "Salvager", "Tentative"]
            for i in range(count):
                ph  = examples[i] if i < len(examples) else f"Button {i + 1}"
                inp = discord.ui.TextInput(
                    label=       f"Button {i + 1} name",
                    placeholder= ph,
                    min_length=1, max_length=25, required=True,
                )
                self.add_item(inp)
                self._inputs.append(inp)
        else:
            # Discord requires at least one component in a modal.
            # Add a read-only-style confirmation field.
            confirm = discord.ui.TextInput(
                label="Confirm  (type anything to continue)",
                placeholder="ok",
                min_length=1, max_length=10, required=True,
            )
            self.add_item(confirm)

    async def on_submit(self, interaction: discord.Interaction):
        link_only = bool(self.partial.get("link_only", False))

        if link_only:
            names: List[str] = []   # no RSVP buttons
        else:
            seen:  Set[str]  = set()
            names = []
            for inp in self._inputs:
                name = inp.value.strip().title()
                if name and name.lower() not in seen:
                    seen.add(name.lower())
                    names.append(name)

            if not names:
                await interaction.response.send_message(
                    "❌ No valid button names provided.", ephemeral=True
                )
                return

        guild = interaction.guild
        if not guild:
            await interaction.response.send_message("Must be used in a server.", ephemeral=True)
            return

        channel = resolve_channel(guild, self.partial["target"])
        if not channel:
            ch_name = (
                SECURITY_ONLY_CHANNEL
                if self.partial["target"] == "security_only"
                else PUBLIC_CHANNEL
            )
            await interaction.response.send_message(
                f"❌ Channel `#{ch_name}` not found.", ephemeral=True
            )
            return

        # Permission check
        bot_m = (
            guild.get_member(interaction.client.user.id)
            if interaction.client.user else None
        )
        if bot_m:
            p = channel.permissions_for(bot_m)
            if not (p.view_channel and p.send_messages and p.embed_links):
                await interaction.response.send_message(
                    f"❌ I'm missing permissions in {channel.mention}.", ephemeral=True
                )
                return

        event_id  = str(uuid.uuid4())
        redir     = self.partial.get("redirect_url", "")

        # Write BOTH old key (buttons) and new key (enabled_buttons) for compat.
        event: Dict[str, Any] = {
            "title":               self.partial["event_name"],
            "description":         self.partial["description"],
            "creator":             self.partial["creator_id"],
            "timestamp":           self.partial["timestamp"],
            "guild_id":            guild.id,
            "channel":             channel.id,
            "channel_name":        channel.name,
            "message":             None,
            "target":              self.partial["target"],
            "buttons":             names,
            "enabled_buttons":     names,
            "link_only":           link_only,
            "capacities":          {},
            "roles":               {n: [] for n in names},
            "active":              True,
            "closed":              False,
            "presence":            {},
            "presence_dm_sent_to": [],
            "presence_started":    False,
            "presence_started_utc":None,
            "redirect_url":        redir,
            "created_utc":         datetime.now(timezone.utc).isoformat(),
        }

        view = EventView(event_id, names, redir, link_only=link_only)

        try:
            msg = await channel.send(
                content= resolve_ping(guild, self.partial["target"]) or None,
                embed=   build_embed(event, guild),
                view=    view,
                allowed_mentions=discord.AllowedMentions(roles=True),
            )
        except Exception as e:
            await interaction.response.send_message(
                f"❌ Failed to post in {channel.mention}: `{e}`", ephemeral=True
            )
            return

        event["message"] = msg.id

        data = await load_events()
        data[event_id] = event
        await save_events(data)

        try:
            interaction.client.add_view(
                EventView(event_id, names, "", link_only=link_only, for_registration=True),
                message_id=msg.id,
            )
        except Exception:
            pass

        mode_str = (
            f"🔗 Link-only (no RSVP buttons) — [{redir}]({redir})"
            if link_only
            else f"Buttons: {', '.join(f'**{n}**' for n in names)}"
        )
        await interaction.response.send_message(
            f"✅ Event created in {channel.mention}.\n"
            f"Audience: **{target_label(self.partial['target'])}**\n"
            f"{mode_str}",
            ephemeral=True,
        )


# ============================================================
# EDIT EVENT MODAL
# ============================================================

# ============================================================
# EDIT MODALS  (one per concern — modals are capped at 5 fields)
# ============================================================

class EditInfoModal(discord.ui.Modal, title="Edit Event — Info"):
    """Edit title, description, date/time, and redirect URL."""

    new_title = discord.ui.TextInput(
        label="Title",
        max_length=100, required=True,
    )
    new_description = discord.ui.TextInput(
        label="Description",
        style=discord.TextStyle.paragraph,
        max_length=1000, required=True,
    )
    new_time = discord.ui.TextInput(
        label="Date & Time (UTC)  —  YYYY-MM-DD HH:MM",
        placeholder="e.g. 2025-06-15 20:00",
        required=True,
    )
    new_redirect = discord.ui.TextInput(
        label="Redirect URL  (leave blank to remove)",
        placeholder="https://...",
        required=False,
    )
    link_only_flag = discord.ui.TextInput(
        label='Link-only mode? Type "yes" or "no"',
        placeholder="no",
        min_length=2, max_length=3, required=False,
    )

    def __init__(self, event_id: str, event: Dict[str, Any]):
        super().__init__()
        self.event_id = event_id
        # Pre-fill current values
        self.new_title.default       = str(event.get("title", ""))
        self.new_description.default = str(event.get("description", ""))
        ts = event.get("timestamp", 0)
        if ts:
            self.new_time.default = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        self.new_redirect.default  = str(event.get("redirect_url", "") or "")
        self.link_only_flag.default = "yes" if event.get("link_only") else "no"

    async def on_submit(self, interaction: discord.Interaction):
        try:
            dt = datetime.strptime(
                self.new_time.value.strip(), "%Y-%m-%d %H:%M"
            ).replace(tzinfo=timezone.utc)
        except ValueError:
            await interaction.response.send_message(
                "❌ Invalid date. Use `YYYY-MM-DD HH:MM` (UTC).", ephemeral=True
            )
            return

        raw_lo = (self.link_only_flag.value or "no").strip().lower()
        if raw_lo not in ("yes", "no", ""):
            await interaction.response.send_message(
                '❌ Link-only must be `yes` or `no`.', ephemeral=True
            )
            return

        link_only = (raw_lo == "yes")
        redir     = self.new_redirect.value.strip()

        if link_only and not redir:
            await interaction.response.send_message(
                "❌ Link-only mode requires a Redirect URL.", ephemeral=True
            )
            return

        data  = await load_events()
        event = data.get(self.event_id)
        if not isinstance(event, dict):
            await interaction.response.send_message("Event not found.", ephemeral=True)
            return

        event["title"]        = self.new_title.value.strip()
        event["description"]  = self.new_description.value.strip()
        event["timestamp"]    = int(dt.timestamp())
        event["redirect_url"] = redir
        event["link_only"]    = link_only

        data[self.event_id] = event
        await save_events(data)
        await refresh(interaction.client, self.event_id)
        await interaction.response.send_message("✅ Event info updated.", ephemeral=True)


class EditButtonCountModal(discord.ui.Modal, title="Edit Buttons — How Many?"):
    """Step 1 of 2 for editing buttons: choose the new count."""

    count = discord.ui.TextInput(
        label="How many RSVP buttons?  (1 – 5, or 0 = link-only)",
        placeholder="e.g. 3",
        min_length=1, max_length=1, required=True,
    )

    def __init__(self, event_id: str):
        super().__init__()
        self.event_id = event_id

    async def on_submit(self, interaction: discord.Interaction):
        raw = self.count.value.strip()
        if not raw.isdigit() or not (0 <= int(raw) <= 5):
            await interaction.response.send_message(
                "❌ Enter 0 – 5.", ephemeral=True
            )
            return

        n = int(raw)
        if n == 0:
            # Switching to link-only — remove RSVP buttons immediately
            data  = await load_events()
            event = data.get(self.event_id)
            if not isinstance(event, dict):
                await interaction.response.send_message("Event not found.", ephemeral=True)
                return
            event["link_only"]     = True
            event["buttons"]       = []
            event["enabled_buttons"] = []
            data[self.event_id]    = event
            await save_events(data)
            await refresh(interaction.client, self.event_id)
            await interaction.response.send_message(
                "✅ Switched to link-only mode. All RSVP buttons removed.", ephemeral=True
            )
            return

        await interaction.response.send_modal(
            EditButtonNamesModal(self.event_id, n)
        )


class EditButtonNamesModal(discord.ui.Modal):
    """Step 2 of 2: one field per new button name, pre-filled if possible."""

    def __init__(self, event_id: str, count: int):
        super().__init__(title=f"Edit Buttons — Names  ({count} buttons)")
        self.event_id = event_id
        self._inputs: List[discord.ui.TextInput] = []

        examples = ["Accept", "Damage", "Logi", "Salvager", "Tentative"]
        for i in range(count):
            ph  = examples[i] if i < len(examples) else f"Button {i + 1}"
            inp = discord.ui.TextInput(
                label=       f"Button {i + 1} name",
                placeholder= ph,
                min_length=1, max_length=25, required=True,
            )
            self.add_item(inp)
            self._inputs.append(inp)

    async def on_submit(self, interaction: discord.Interaction):
        seen:  Set[str]  = set()
        names: List[str] = []
        for inp in self._inputs:
            name = inp.value.strip().title()
            if name and name.lower() not in seen:
                seen.add(name.lower())
                names.append(name)

        if not names:
            await interaction.response.send_message(
                "❌ No valid button names provided.", ephemeral=True
            )
            return

        data  = await load_events()
        event = data.get(self.event_id)
        if not isinstance(event, dict):
            await interaction.response.send_message("Event not found.", ephemeral=True)
            return

        # Preserve existing signups for buttons whose name didn't change
        old_roles = event.get("roles", {})
        new_roles: Dict[str, List] = {}
        for n in names:
            new_roles[n] = old_roles.get(n, [])

        # Also reset capacities to only include the new names
        old_caps = event.get("capacities", {})
        new_caps = {n: old_caps[n] for n in names if n in old_caps}

        event["buttons"]        = names
        event["enabled_buttons"] = names
        event["roles"]          = new_roles
        event["capacities"]     = new_caps
        event["link_only"]      = False

        data[self.event_id] = event
        await save_events(data)
        await refresh(interaction.client, self.event_id)
        await interaction.response.send_message(
            f"✅ Buttons updated: {', '.join(f'**{n}**' for n in names)}",
            ephemeral=True,
        )


class EditCapacitiesModal(discord.ui.Modal, title="Edit Capacities"):
    """
    One field per button, pre-filled with current cap (or empty = unlimited).
    Format: just a number, or leave blank.
    If there are more than 5 buttons Discord won't allow 5+ fields, so we
    pack them into a single multi-line field instead.
    """

    packed = discord.ui.TextInput(
        label="Button caps (one per line: ButtonName:Cap or ButtonName)",
        style=discord.TextStyle.paragraph,
        placeholder="Accept:10\nDamage:5\nLogi",
        required=False,
    )

    def __init__(self, event_id: str, event: Dict[str, Any]):
        super().__init__()
        self.event_id = event_id
        buttons  = get_enabled_button_titles(event)
        caps     = event.get("capacities", {})
        lines    = []
        for b in buttons:
            c = caps.get(b)
            lines.append(f"{b}:{c}" if c else b)
        self.packed.default = "\n".join(lines)

    async def on_submit(self, interaction: discord.Interaction):
        data  = await load_events()
        event = data.get(self.event_id)
        if not isinstance(event, dict):
            await interaction.response.send_message("Event not found.", ephemeral=True)
            return

        buttons = get_enabled_button_titles(event)
        btn_set = {b.lower() for b in buttons}
        new_caps: Dict[str, int] = {}

        for line in self.packed.value.splitlines():
            line = line.strip()
            if not line:
                continue
            if ":" in line:
                name_raw, cap_raw = line.split(":", 1)
                name_raw = name_raw.strip().title()
                if name_raw.lower() not in btn_set:
                    continue
                try:
                    cap = int(cap_raw.strip())
                    if cap > 0:
                        new_caps[name_raw] = cap
                except ValueError:
                    pass

        event["capacities"] = new_caps
        data[self.event_id] = event
        await save_events(data)
        await refresh(interaction.client, self.event_id)

        summary = ", ".join(
            f"{b}:{c}" for b, c in new_caps.items()
        ) if new_caps else "all unlimited"
        await interaction.response.send_message(
            f"✅ Capacities updated: {summary}", ephemeral=True
        )


# ============================================================
# ADMIN VIEW  (full management panel)
# ============================================================

class AdminView(discord.ui.View):
    """
    Full management panel shown ephemerally when the creator / admin
    clicks the Manage button.

    Buttons:
      ✏️ Edit Info        — title, description, datetime, redirect URL, link-only flag
      🔘 Edit Buttons     — rename / add / remove RSVP buttons (2-step like creation)
      🔢 Edit Capacities  — per-button slot limits
      🔒 Close / 🔓 Reopen — toggle event open/closed state
      🗑 Delete           — permanently remove the event and its message
    """

    def __init__(self, event_id: str, event: Dict[str, Any]):
        super().__init__(timeout=120)
        self.event_id = event_id
        self.event    = event
        is_active = event.get("active", True) and not event.get("closed", False)

        # Dynamic label for the close/reopen button
        self._toggle_label = "🔓 Reopen Event" if not is_active else "🔒 Close Event"
        self._toggle_style = (
            discord.ButtonStyle.success if not is_active else discord.ButtonStyle.secondary
        )

    @discord.ui.button(label="✏️ Edit Info", style=discord.ButtonStyle.primary, row=0)
    async def edit_info(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        data  = await load_events()
        event = data.get(self.event_id)
        if not isinstance(event, dict):
            await interaction.response.send_message("Event not found.", ephemeral=True)
            return
        await interaction.response.send_modal(EditInfoModal(self.event_id, event))

    @discord.ui.button(label="🔘 Edit Buttons", style=discord.ButtonStyle.primary, row=0)
    async def edit_buttons(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        await interaction.response.send_modal(EditButtonCountModal(self.event_id))

    @discord.ui.button(label="🔢 Edit Capacities", style=discord.ButtonStyle.primary, row=0)
    async def edit_capacities(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        data  = await load_events()
        event = data.get(self.event_id)
        if not isinstance(event, dict):
            await interaction.response.send_message("Event not found.", ephemeral=True)
            return
        await interaction.response.send_modal(EditCapacitiesModal(self.event_id, event))

    @discord.ui.button(label="🔒 Close / 🔓 Reopen", style=discord.ButtonStyle.secondary, row=1)
    async def toggle_active(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        data  = await load_events()
        event = data.get(self.event_id)
        if not isinstance(event, dict):
            await interaction.response.send_message("Event not found.", ephemeral=True)
            return

        is_active = event.get("active", True) and not event.get("closed", False)
        event["active"] = not is_active
        event["closed"] = is_active       # flip

        data[self.event_id] = event
        await save_events(data)
        await refresh(interaction.client, self.event_id)

        state = "closed" if is_active else "reopened"
        await interaction.response.send_message(
            f"✅ Event **{state}**.", ephemeral=True
        )

    @discord.ui.button(label="🗑 Delete Event", style=discord.ButtonStyle.danger, row=1)
    async def delete(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        data  = await load_events()
        event = data.get(self.event_id)
        if not event:
            await interaction.response.send_message("Event not found.", ephemeral=True)
            return

        event = normalize_event(event)
        guild = interaction.guild
        if guild:
            channel = guild.get_channel(event.get("channel"))
            if isinstance(channel, discord.TextChannel):
                try:
                    msg = await channel.fetch_message(event["message"])
                    await msg.delete()
                except Exception:
                    pass

        del data[self.event_id]
        await save_events(data)
        await interaction.response.send_message("✅ Event deleted.", ephemeral=True)



class AudienceSelectView(discord.ui.View):
    def __init__(self, user_id: int):
        super().__init__(timeout=300)   # 5 minutes — was 60 s
        self.user_id = user_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id

    async def on_check_failure(self, interaction: discord.Interaction) -> None:
        # Without this, Discord shows a silent "Interaction Failed" to non-creators.
        await interaction.response.send_message(
            "This menu belongs to someone else.", ephemeral=True
        )

    @discord.ui.button(label="ARC Security Only", style=discord.ButtonStyle.danger)
    async def security_only(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        await interaction.response.send_modal(
            EventInfoModal(interaction.user.id, "security_only")
        )

    @discord.ui.button(label="Security + Subsidized", style=discord.ButtonStyle.success)
    async def public(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        await interaction.response.send_modal(
            EventInfoModal(interaction.user.id, "public")
        )


# ============================================================
# RSVP BUTTON
# ============================================================

class RSVPButton(discord.ui.Button):
    def __init__(self, event_id: str, name: str, row: int = 0):
        self.name = name
        # Sanitise custom_id: alphanumeric + underscores only
        # This is the SAME formula as the simple version so existing
        # messages' button custom_ids continue to match.
        safe = "".join(c if c.isalnum() else "_" for c in name.lower())
        super().__init__(
            label=     name,
            style=     self._style(name),
            custom_id= f"rsvp:{event_id}:{safe}",
            row=       row,
        )

    @staticmethod
    def _style(name: str) -> discord.ButtonStyle:
        n = name.lower()
        if n == "accept":    return discord.ButtonStyle.success
        if n == "decline":   return discord.ButtonStyle.danger
        if n == "tentative": return discord.ButtonStyle.secondary
        return discord.ButtonStyle.primary

    async def callback(self, interaction: discord.Interaction):
        if not self.view or not isinstance(self.view, EventView):
            await interaction.response.send_message("Invalid view.", ephemeral=True)
            return

        # Defer immediately — load_events() is async I/O that can exceed
        # Discord's 3-second acknowledgement window (error 10062).
        await interaction.response.defer(ephemeral=True)

        data  = await load_events()
        event = data.get(self.view.event_id)
        if not isinstance(event, dict):
            await interaction.followup.send("Event not found.", ephemeral=True)
            return

        event    = normalize_event(event)
        is_active = event.get("active", True) and not event.get("closed", False)

        if not is_active:
            await interaction.followup.send(
                "This event is closed.", ephemeral=True
            )
            return

        if not isinstance(interaction.user, discord.Member):
            await interaction.followup.send(
                "Must be used in a server.", ephemeral=True
            )
            return

        uid  = interaction.user.id
        name = self.name

        # Mutual exclusion: remove user from every other role
        for r in event["roles"]:
            lst = event["roles"][r]
            if uid in lst:
                lst.remove(uid)

        # Capacity check
        cap = event.get("capacities", {}).get(name)
        if cap and len(event["roles"].get(name, [])) >= int(cap):
            await interaction.followup.send(
                f"**{name}** is full ({cap}/{cap}).", ephemeral=True
            )
            return

        event["roles"].setdefault(name, []).append(uid)

        # Temp role assignment for known types only;
        # custom button names leave the role untouched.
        guild     = interaction.guild
        temp_role = (
            discord.utils.get(guild.roles, name=TEMP_ROLE_NAME) if guild else None
        )
        if temp_role:
            try:
                if name.lower() in ROLE_ASSIGN_TYPES:
                    await interaction.user.add_roles(
                        temp_role, reason="Event RSVP"
                    )
                elif name.lower() == "decline":
                    await interaction.user.remove_roles(
                        temp_role, reason="Event RSVP decline"
                    )
            except Exception:
                pass

        data[self.view.event_id] = event
        await save_events(data)
        await refresh(interaction.client, self.view.event_id)
        await interaction.followup.send(
            f"Registered as **{name}**.", ephemeral=True
        )


# ============================================================
# MANAGE + ADMIN
# ============================================================

class ManageEventButton(discord.ui.Button):
    def __init__(self, event_id: str):
        super().__init__(
            label="Manage", style=discord.ButtonStyle.secondary,
            custom_id=f"manage:{event_id}", row=4,
        )
        self.event_id = event_id

    async def callback(self, interaction: discord.Interaction):
        # Defer immediately — load_events() is async I/O that can exceed
        # Discord's 3-second acknowledgement window (error 10062).
        await interaction.response.defer(ephemeral=True)

        data  = await load_events()
        event = data.get(self.event_id)
        if not isinstance(event, dict):
            await interaction.followup.send("Event not found.", ephemeral=True)
            return

        is_creator = interaction.user.id == event.get("creator")
        is_admin   = (
            isinstance(interaction.user, discord.Member)
            and interaction.user.guild_permissions.administrator
        )
        if not (is_creator or is_admin):
            await interaction.followup.send("Not authorized.", ephemeral=True)
            return

        event = normalize_event(event)
        is_active = event.get("active", True) and not event.get("closed", False)

        lines = [
            f"**Title:** {event.get('title', '?')}",
            f"**Status:** {'🟢 Active' if is_active else '🔴 Closed'}",
            f"**Time:** <t:{event.get('timestamp', 0)}:F>",
            f"**Audience:** {target_label(event.get('target', 'security_only'))}",
            f"**Buttons:** {', '.join(get_enabled_button_titles(event)) or '_(link-only)_'}",
            f"**Redirect URL:** {event.get('redirect_url') or '_(none)_'}",
        ]

        await interaction.followup.send(
            "\n".join(lines),
            view=AdminView(self.event_id, event),
            ephemeral=True,
        )


# (Duplicate AdminView removed — the full AdminView above is the active one)


# ============================================================
# EVENT VIEW
# ============================================================

class EventView(discord.ui.View):
    def __init__(
        self,
        event_id:      str,
        buttons:       List[str],
        redirect_url:  str = "",
        *,
        link_only:        bool = False,
        for_registration: bool = False,
    ):
        """
        link_only=True        → skip all RSVP buttons; show only the link + Manage.
        for_registration=True → omit the link button (no custom_id → breaks
                                 is_persistent(); always True when calling bot.add_view).
        """
        super().__init__(timeout=None)
        self.event_id = event_id

        if not link_only:
            def _order(x: str) -> int:
                t = x.title()
                return DISPLAY_ORDER.index(t) if t in DISPLAY_ORDER else 999

            for i, b in enumerate(sorted(buttons, key=_order)):
                row = min(i // 5, 3)
                self.add_item(RSVPButton(event_id, b.title(), row=row))

        if redirect_url and not for_registration:
            self.add_item(
                discord.ui.Button(
                    label="🔗 External Signup" if link_only else "External Signup",
                    url=redirect_url,
                    style=discord.ButtonStyle.link,
                    row=4,
                )
            )

        self.add_item(ManageEventButton(event_id))


# ============================================================
# COG
# ============================================================

class EventCreator(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot                 = bot
        self._views_registered   = False
        if not self.presence_loop.is_running():
            self.presence_loop.start()

    def cog_unload(self):
        if self.presence_loop.is_running():
            self.presence_loop.cancel()

    def _can_create(self, member: discord.Member) -> bool:
        return any(r.name in CREATOR_ROLES for r in member.roles)

    # ----------------------------------------------------------------
    # on_ready
    # ----------------------------------------------------------------

    @commands.Cog.listener()
    async def on_ready(self):
        # Guard against double-registration on reconnects
        if self._views_registered:
            return
        self._views_registered = True

        data       = await load_events()
        registered = 0

        for event_id, event in data.items():
            if not isinstance(event, dict):
                continue

            event = normalize_event(event)

            if not event.get("guild_id"):
                continue

            is_active = event.get("active", True) and not event.get("closed", False)
            if not is_active:
                continue

            msg_id   = event.get("message")
            buttons  = get_enabled_button_titles(event)
            redirect = event.get("redirect_url", "")
            link_only = bool(event.get("link_only", False))

            try:
                view = EventView(event_id, buttons, redirect, link_only=link_only, for_registration=True)
                if isinstance(msg_id, int):
                    self.bot.add_view(view, message_id=msg_id)
                else:
                    self.bot.add_view(view)
                registered += 1
            except Exception as e:
                print(
                    f"[event_creator] View registration failed for {event_id}: "
                    f"{type(e).__name__}: {e}"
                )

        print(
            f"[event_creator] Registered {registered}/{len(data)} "
            "persistent event view(s)."
        )

        for guild in self.bot.guilds:
            try:
                await ensure_hierarchy_log_channel(guild)
            except Exception:
                pass

    # ----------------------------------------------------------------
    # Presence loop
    # ----------------------------------------------------------------

    @tasks.loop(seconds=60)
    async def presence_loop(self):
        """
        Fires every 60 s.  When an event's timestamp is reached and
        presence_started is False, DMs the creator once per participant.

        Safety for old events
        ---------------------
        • Old events with closed=True are skipped (active=False after normalise).
        • Old events without guild_id are skipped (can't resolve guild/members).
        • Events without participants produce no DMs.
        """
        data   = await load_events()
        now_ts = int(datetime.now(timezone.utc).timestamp())
        changed = False

        for event_id, event in list(data.items()):
            if not isinstance(event, dict):
                continue

            event = normalize_event(event)

            is_active = event.get("active", True) and not event.get("closed", False)
            if not is_active:
                continue
            if event.get("presence_started") is True:
                continue

            ts = event.get("timestamp")
            if not isinstance(ts, int) or now_ts < ts:
                continue

            guild_id   = event.get("guild_id")
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

            event["presence_started"]      = True
            event["presence_started_utc"]  = datetime.now(timezone.utc).isoformat()
            data[event_id]                 = event
            changed                        = True

            sent_list = event.setdefault("presence_dm_sent_to", [])

            for pid in participants:
                if pid in sent_list:
                    continue

                member = guild.get_member(pid)
                sent_list.append(pid)

                if not member:
                    continue

                try:
                    dm    = await creator.create_dm()
                    embed = discord.Embed(
                        title="Was this participant present?",
                        description=(
                            f"**Event:** {event.get('title', 'Event')}\n"
                            f"**Participant:** {member.display_name} ({member.mention})\n"
                            f"**Event time:** <t:{ts}:F>"
                        ),
                        color=     discord.Color.blurple(),
                        timestamp= datetime.now(timezone.utc),
                    )
                    await dm.send(
                        embed=embed,
                        view=PresenceConfirmView(
                            event_id=event_id, participant_id=pid
                        ),
                    )
                except discord.Forbidden:
                    print(
                        f"[event_creator] Cannot DM creator {creator_id} "
                        "(DMs closed)."
                    )
                    break
                except Exception as e:
                    print(
                        f"[event_creator] DM error for {event_id}/{pid}: "
                        f"{type(e).__name__}: {e}"
                    )

        if changed:
            await save_events(data)

    @presence_loop.before_loop
    async def _before_presence_loop(self):
        await self.bot.wait_until_ready()

    # ----------------------------------------------------------------
    # /create_event
    # ----------------------------------------------------------------

    @app_commands.command(name="create_event", description="Create a new event")
    async def create_event(self, interaction: discord.Interaction):
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message(
                "Must be used in a server.", ephemeral=True
            )
            return
        if not self._can_create(interaction.user):
            await interaction.response.send_message(
                "❌ You are not authorized to create events.", ephemeral=True
            )
            return
        await interaction.response.send_message(
            "Select event audience:",
            view=AudienceSelectView(interaction.user.id),
            ephemeral=True,
        )

    # ----------------------------------------------------------------
    # /event_log
    # ----------------------------------------------------------------

    @app_commands.command(
        name="event_log",
        description="Show participation log — who signed up for which events.",
    )
    @app_commands.describe(
        member="Optional: filter to a single member's participation history.",
    )
    async def event_log(
        self,
        interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
    ):
        await interaction.response.defer(ephemeral=True)

        data = await load_events()
        if not data:
            await interaction.followup.send("No events found.", ephemeral=True)
            return

        guild = interaction.guild

        # Sort all events by timestamp descending (newest first)
        sorted_events = sorted(
            [(eid, e) for eid, e in data.items() if isinstance(e, dict)],
            key=lambda kv: kv[1].get("timestamp", 0),
            reverse=True,
        )

        if member:
            # ── per-member report ──────────────────────────────────────────
            uid   = member.id
            lines: List[str] = []

            for event_id, event in sorted_events:
                event = normalize_event(event)
                title = str(event.get("title", "Untitled"))
                ts    = event.get("timestamp", 0)

                buttons_found: List[str] = []
                for btn, users in event.get("roles", {}).items():
                    if uid in users:
                        confirmed = event.get("presence", {}).get(str(uid))
                        tag = (
                            " ✅" if confirmed is True
                            else " ❌" if confirmed is False
                            else ""
                        )
                        buttons_found.append(f"{btn}{tag}")

                if buttons_found:
                    date_str = f"<t:{ts}:d>" if ts else "?"
                    lines.append(
                        f"• **{title}** ({date_str}) — {', '.join(buttons_found)}"
                    )

            if not lines:
                await interaction.followup.send(
                    f"{member.mention} has no participation records.", ephemeral=True
                )
                return

            header = (
                f"📋 Participation log for **{member.display_name}** "
                f"— {len(lines)} event(s)\n\n"
            )
            body = "\n".join(lines)

        else:
            # ── all-events report ──────────────────────────────────────────
            sections: List[str] = []

            for event_id, event in sorted_events:
                event    = normalize_event(event)
                title    = str(event.get("title", "Untitled"))
                ts       = event.get("timestamp", 0)
                is_active = event.get("active", True) and not event.get("closed", False)
                status   = "🟢" if is_active else "🔴"
                date_str = f"<t:{ts}:d>" if ts else "?"

                part_lines: List[str] = []
                for btn, users in event.get("roles", {}).items():
                    for uid in users:
                        if guild:
                            m        = guild.get_member(uid)
                            name_str = m.display_name if m else f"ID:{uid}"
                        else:
                            name_str = f"ID:{uid}"

                        confirmed = event.get("presence", {}).get(str(uid))
                        conf_tag  = (
                            " ✅" if confirmed is True
                            else " ❌" if confirmed is False
                            else ""
                        )
                        part_lines.append(f"  • {name_str} ({btn}{conf_tag})")

                if part_lines:
                    sections.append(
                        f"{status} **{title}** — {date_str}\n"
                        + "\n".join(part_lines)
                    )

            if not sections:
                await interaction.followup.send(
                    "No participation records found.", ephemeral=True
                )
                return

            header = (
                f"📋 **Event Participation Log**  "
                f"({len(sorted_events)} event(s))\n\n"
            )
            body = "\n\n".join(sections)

        full = header + body

        # Send inline if short enough; otherwise attach as a plain-text file
        if len(full) <= 2000:
            await interaction.followup.send(full, ephemeral=True)
        else:
            buf = io.BytesIO(full.encode("utf-8"))
            await interaction.followup.send(
                (
                    f"📋 Participation log for **{member.display_name}**"
                    if member
                    else "📋 Event Participation Log"
                )
                + " — too long to display inline, see attached file.",
                file=discord.File(buf, filename="participation_log.txt"),
                ephemeral=True,
            )


async def setup(bot: commands.Bot):
    await bot.add_cog(EventCreator(bot))
