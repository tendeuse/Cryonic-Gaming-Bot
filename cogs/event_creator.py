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
from typing import Any, Dict, List, Optional, Set, Tuple

import discord
from discord.ext import commands, tasks
from discord import app_commands

from . import db

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
    "ARC Ensign",
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

# Voice-channel attendance tracking
ARC_MAIN_VC          = "ARC  Main (EVE Online)"      # destination VC after event ends
EVENT_VC_MIN_SECONDS = 15 * 60         # 900 s  — minimum cumulative time to qualify
VC_PRECREATE_SECONDS = 5 * 60          # 300 s  — create the event VC this long before start

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


def _key(path: str) -> str:
    # Old filename stem becomes the MySQL kv_store key ("events"/"ap_boosts").
    return os.path.splitext(os.path.basename(path))[0]


def _atomic_write(path: str, data: Any) -> None:
    db.kv_save(_key(path), data)


async def load_events() -> Dict[str, Any]:
    async with _get_lock():
        try:
            data = await asyncio.to_thread(db.kv_load, _key(DATA_PATH), {})
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}


async def save_events(data: Dict[str, Any]) -> None:
    async with _get_lock():
        await asyncio.to_thread(_atomic_write, DATA_PATH, data)


async def save_event(event_id: str, event: Dict[str, Any]) -> None:
    """
    Atomically read-modify-write a single event record.

    Holds the lock for the entire load → patch → save cycle, so this
    coroutine can never race with presence_loop or any other concurrent
    save and accidentally overwrite a freshly-set presence_started flag.
    """
    async with _get_lock():
        try:
            data: Dict[str, Any] = await asyncio.to_thread(db.kv_load, _key(DATA_PATH), {})
            if not isinstance(data, dict):
                data = {}
        except Exception:
            data = {}
        data[event_id] = event
        await asyncio.to_thread(_atomic_write, DATA_PATH, data)


async def load_boosts() -> Dict[str, Any]:
    async with _get_lock():
        try:
            data = await asyncio.to_thread(db.kv_load, _key(BOOSTS_PATH), {"participants": {}})
            if not isinstance(data, dict):
                return {"participants": {}}
            data.setdefault("participants", {})
            return data
        except Exception:
            return {"participants": {}}


async def save_boosts(data: Dict[str, Any]) -> None:
    async with _get_lock():
        await asyncio.to_thread(_atomic_write, BOOSTS_PATH, data)


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

    # ── voice-channel attendance (new system) ─────────────────────────────────
    event.setdefault("event_vc_id",         None)   # int: created event VC
    event.setdefault("vc_cumulative_times", {})     # {str(uid): seconds}
    event.setdefault("vc_qualified",        [])     # [uid, ...] hit 15-min threshold

    # ── other new fields ─────────────────────────────────────────────────────
    event.setdefault("target",       "security_only")
    event.setdefault("redirect_url", "")
    event.setdefault("link_only",    False)
    event.setdefault("created_utc",  None)
    event.setdefault("capacities",   {})

    # ── Directive tracking + participation gating (added by the Directive cog) ─
    event.setdefault("restrict_role",          None)   # role name required to RSVP
    event.setdefault("directive_officer_id",   None)
    event.setdefault("directive_focus",        None)
    event.setdefault("directive_cycle_start",  None)

    # ── ensure roles dict has an entry for every button ───────────────────────
    roles = event.setdefault("roles", {})
    for btn in event["enabled_buttons"]:
        roles.setdefault(btn, [])

    return event


# ============================================================
# HELPERS
# ============================================================

def _short_event_id(event_id: str) -> str:
    """
    Return a compact, human-readable Event ID from the internal UUID.

    Takes the first 6 hex characters (case-insensitive in UUID, uppercased
    here) and prefixes them with "EVT-", e.g. "EVT-550E84".
    This gives 16^6 = ~16.7 M unique values — more than enough for a single
    Discord guild, and short enough for admins to type into a slash command.
    """
    return "EVT-" + event_id.replace("-", "").upper()[:6]


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


def compute_pull_targets(event: Dict[str, Any]) -> List[int]:
    """
    User IDs the bot should pull into / track inside the event VC.

    This is the RSVP'd participants PLUS the event creator (host).  The creator
    is never auto-RSVP'd, so without this they would never be pulled into the
    event VC and would fail the directive's "creator attended" check even after
    running the op themselves.
    """
    targets: Set[int] = set(compute_participants(event))
    creator = event.get("creator")
    if isinstance(creator, int):
        targets.add(creator)
    return sorted(targets)


def current_confirmed_ids(event: Dict[str, Any]) -> List[int]:
    presence = event.get("presence", {})
    return sorted({int(k) for k, v in presence.items() if v is True})


def current_absent_ids(event: Dict[str, Any]) -> List[int]:
    """IDs of participants the creator explicitly marked as NOT present."""
    presence = event.get("presence", {})
    return sorted({int(k) for k, v in presence.items() if v is False})


def current_qualified_ids(event: Dict[str, Any]) -> List[int]:
    """
    IDs of members who accumulated >= EVENT_VC_MIN_SECONDS in the event VC.
    Reads from the persisted vc_qualified list (set at finalize time) or
    falls back to computing directly from vc_cumulative_times.
    """
    qualified = event.get("vc_qualified")
    if isinstance(qualified, list) and qualified:
        return sorted(int(x) for x in qualified if isinstance(x, int))
    # Fallback: compute from cumulative times
    cumulative = event.get("vc_cumulative_times", {})
    return sorted(
        int(k) for k, v in cumulative.items()
        if isinstance(v, (int, float)) and v >= EVENT_VC_MIN_SECONDS
    )


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

    # Creator + Event ID in footer
    creator_id = event.get("creator")
    short_id   = event.get("short_id", "")
    if creator_id:
        name = str(creator_id)
        if guild:
            m = guild.get_member(int(creator_id))
            if m:
                name = m.display_name
        footer_parts = [f"Created by {name}"]
        if short_id:
            footer_parts.append(f"Event ID: {short_id}")
        embed.set_footer(text=" · ".join(footer_parts))

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


def _build_vc_overwrites(
    guild:      discord.Guild,
    target:     str,
    bot_member: Optional[discord.Member] = None,
) -> Dict[Any, discord.PermissionOverwrite]:
    """
    Build permission overwrites for the auto-created event voice channel.

    Access policy
    -------------
    • @everyone        → cannot see or connect  (hidden from unauthorised members)
    • ARC Security     → can see and connect    (always — all events include security)
    • ARC Subsidized   → can see and connect    (only when target == "public")
    • Bot member       → full channel control   (view, connect, move_members, manage)

    Compatibility with Discord native Scheduled Events
    ---------------------------------------------------
    Discord's own scheduled-event system lets admins link an event to an
    existing VC.  Our auto-created VC is a brand-new channel that is never
    registered as a Discord scheduled event, so there is zero overlap.
    The on_voice_state_update listener only fires for VCs present in
    _vc_event_map, so moves triggered by Discord's own scheduled events
    (which point to different VCs entirely) are silently ignored.

    Returns an empty dict (no overwrites) if required roles are missing —
    Discord falls back to category-inherited permissions and a warning is
    printed so ops can investigate.
    """
    overwrites: Dict[Any, discord.PermissionOverwrite] = {}

    # ── Deny everyone by default ─────────────────────────────────────────────
    overwrites[guild.default_role] = discord.PermissionOverwrite(
        view_channel=False,
        connect=False,
    )

    # ── ARC Security — always allowed ────────────────────────────────────────
    sec_role = discord.utils.get(guild.roles, name=SECURITY_PING_ROLE)
    if sec_role:
        overwrites[sec_role] = discord.PermissionOverwrite(
            view_channel=True,
            connect=True,
        )
    else:
        print(
            f"[event_creator] WARNING: Role '{SECURITY_PING_ROLE}' not found — "
            "event VC will not restrict access correctly."
        )

    # ── ARC Subsidized — only for public-audience events ─────────────────────
    if target == "public":
        sub_role = discord.utils.get(guild.roles, name=SUBSIDIZED_PING_ROLE)
        if sub_role:
            overwrites[sub_role] = discord.PermissionOverwrite(
                view_channel=True,
                connect=True,
            )
        else:
            print(
                f"[event_creator] WARNING: Role '{SUBSIDIZED_PING_ROLE}' not found — "
                "subsidized members cannot join the public event VC."
            )

    # ── Bot itself — needs move_members + manage_channels to do its job ───────
    if bot_member:
        overwrites[bot_member] = discord.PermissionOverwrite(
            view_channel=True,
            connect=True,
            move_members=True,
            manage_channels=True,
        )

    return overwrites


async def log_confirmed_participants(
    guild:         discord.Guild,
    *,
    event_title:   str,
    event_id:      str,
    qualified_ids: List[int],
    rsvp_ids:      List[int],
    cum_times:     Optional[Dict[str, int]] = None,
) -> None:
    """
    Posts the final attendance summary to #arc-hierarchy-log.

    ✅  Qualified (≥15 min in event VC) — these members earned AP.
    ⏱   RSVP'd but did not reach the threshold — shown for transparency.

    Decline-button people are never included (excluded upstream by
    compute_participants).

    cum_times: {str(user_id): total_seconds} — when supplied, each member's
    cumulative VC time is appended to their entry (e.g. "— 23 min 14 s").
    """
    ch = await ensure_hierarchy_log_channel(guild)
    if not ch:
        return

    qualified_set = set(qualified_ids)
    _cum = cum_times or {}

    def _fmt_time(uid: int) -> str:
        """Return a compact 'X min Y s' string for a member's cumulative time."""
        secs  = int(_cum.get(str(uid), _cum.get(uid, 0)))  # accept str or int key
        mins  = secs // 60
        rem_s = secs % 60
        if mins == 0:
            return f"{rem_s}s"
        if rem_s == 0:
            return f"{mins} min"
        return f"{mins} min {rem_s}s"

    def _names(ids: List[int], show_time: bool = False) -> str:
        lines = []
        for uid in ids:
            m        = guild.get_member(uid)
            name_str = f"{m.display_name} ({m.mention})" if m else f"<@{uid}>"
            if show_time and _cum:
                name_str += f" — _{_fmt_time(uid)}_"
            lines.append(f"- {name_str}")
        return "\n".join(lines) if lines else "_(none)_"

    # RSVP'd members who didn't qualify
    no_threshold = [uid for uid in rsvp_ids if uid not in qualified_set]

    embed = discord.Embed(
        title=     "📋 Fleet Attendance — Final",
        color=     discord.Color.green() if qualified_ids else discord.Color.orange(),
        timestamp= datetime.now(timezone.utc),
    )
    embed.add_field(name="Fleet",    value=event_title,     inline=True)
    embed.add_field(name="Event ID", value=f"`{event_id}`", inline=True)
    embed.add_field(name="\u200b",   value="\u200b",        inline=True)

    embed.add_field(
        name=  f"✅ Qualified — {len(qualified_ids)} member(s)  (≥15 min)",
        value= _names(qualified_ids, show_time=True),
        inline=False,
    )
    if no_threshold:
        embed.add_field(
            name=  f"⏱ RSVP'd — did not reach threshold ({len(no_threshold)})",
            value= _names(no_threshold, show_time=True),
            inline=False,
        )

    # Mention only qualified members
    mentions = []
    for uid in qualified_ids:
        m = guild.get_member(uid)
        if m:
            mentions.append(m.mention)
    content = (" ".join(mentions))[:1800] if mentions else ""

    try:
        await ch.send(content=content, embed=embed)
    except Exception:
        pass


# ============================================================
# EVENT DONE VIEW  (single DM button sent to the event creator)
# ============================================================

class EventDoneView(discord.ui.View):
    """
    Sent to the event creator once via DM when the event VC is created.
    A single "✅ Mark Event as Done" button triggers finalization:
      • Cumulative VC times are locked in.
      • Members ≥ EVENT_VC_MIN_SECONDS are qualified and receive AP.
      • Everyone still in the event VC is moved to ARC Main.
      • The event VC is deleted.
      • Results are logged to #arc-hierarchy-log.

    Timeout = 24 h.  If the bot restarts the button stops responding
    (known Discord limitation for non-persistent DM views) — the creator
    can use /close_event as a fallback (future work).
    """

    def __init__(self, event_id: str, bot: commands.Bot):
        super().__init__(timeout=86_400)   # 24 hours
        self.event_id = event_id
        self.bot      = bot

    @discord.ui.button(
        label="✅ Mark Event as Done",
        style=discord.ButtonStyle.success,
        custom_id="event_done_btn",          # NOTE: not truly persistent (DM view)
    )
    async def done(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        # Defer immediately — load_events() is async I/O that can easily
        # exceed Discord's 3-second acknowledgement window, which is exactly
        # what caused the "Unknown Message" / "Interaction Failed" error
        # members were seeing when pressing this button.
        await interaction.response.defer()

        data  = await load_events()
        event = data.get(self.event_id)
        if not isinstance(event, dict):
            await interaction.followup.send("Event not found.", ephemeral=True)
            return

        if interaction.user.id != event.get("creator"):
            await interaction.followup.send(
                "Only the event creator can mark this event as done.",
                ephemeral=True,
            )
            return

        is_active = event.get("active", True) and not event.get("closed", False)
        if not is_active:
            await interaction.followup.send(
                "This event is already closed.", ephemeral=True
            )
            return

        # Disable the button so it can't be double-clicked while finalizing
        button.disabled = True
        button.label    = "⏳ Finalizing…"
        try:
            await interaction.edit_original_response(view=self)
        except Exception:
            pass   # DM edit can fail if the message is old — non-fatal

        # Delegate all finalization logic to the cog
        cog = self.bot.cogs.get("EventCreator")
        if cog:
            await cog._finalize_event(
                interaction=interaction,
                event_id=   self.event_id,
            )
        else:
            try:
                await interaction.followup.send(
                    "❌ EventCreator cog not found.", ephemeral=True
                )
            except Exception:
                pass


# ============================================================
# SHARED EVENT POSTING HELPER
# ============================================================

async def _do_post_event(
    interaction: discord.Interaction,
    partial:     Dict[str, Any],
    names:       List[str],
    extra:       Optional[Dict[str, Any]] = None,
) -> None:
    """
    Validates channel/permissions, builds the event record, posts the message,
    saves to disk, and sends the creator a confirmation followup.

    The caller MUST have already deferred the interaction before calling this
    (interaction.response.defer(ephemeral=True)) — this function uses followup.

    extra: optional extra keys merged into the event record before it is posted
    and saved. Used by the Directive cog to tag an event for weekly progress
    tracking (directive_officer_id / directive_focus / directive_cycle_start)
    and to gate RSVP participation (restrict_role).
    """
    link_only = bool(partial.get("link_only", False))
    guild     = interaction.guild
    if not guild:
        await interaction.followup.send("Must be used in a server.", ephemeral=True)
        return

    channel = resolve_channel(guild, partial["target"])
    if not channel:
        ch_name = (
            SECURITY_ONLY_CHANNEL
            if partial["target"] == "security_only"
            else PUBLIC_CHANNEL
        )
        await interaction.followup.send(
            f"❌ Channel `#{ch_name}` not found.", ephemeral=True
        )
        return

    bot_m = (
        guild.get_member(interaction.client.user.id)
        if interaction.client.user else None
    )
    if bot_m:
        p = channel.permissions_for(bot_m)
        if not (p.view_channel and p.send_messages and p.embed_links):
            await interaction.followup.send(
                f"❌ I'm missing permissions in {channel.mention}.", ephemeral=True
            )
            return

    event_id = str(uuid.uuid4())
    redir    = partial.get("redirect_url", "")

    event: Dict[str, Any] = {
        "title":                partial["event_name"],
        "description":          partial["description"],
        "creator":              partial["creator_id"],
        "timestamp":            partial["timestamp"],
        "guild_id":             guild.id,
        "channel":              channel.id,
        "channel_name":         channel.name,
        "message":              None,
        "target":               partial["target"],
        "buttons":              names,
        "enabled_buttons":      names,
        "link_only":            link_only,
        "capacities":           {},
        "roles":                {n: [] for n in names},
        "active":               True,
        "closed":               False,
        "presence":             {},
        "presence_dm_sent_to":  [],
        "presence_started":     False,
        "presence_started_utc": None,
        "redirect_url":         redir,
        "created_utc":          datetime.now(timezone.utc).isoformat(),
        "short_id":             _short_event_id(event_id),
    }

    # Merge caller-supplied extra fields (e.g. Directive tracking tags) before
    # the record is posted and persisted.
    if extra:
        event.update(extra)

    view = EventView(event_id, names, redir, link_only=link_only)

    try:
        msg = await channel.send(
            content=          resolve_ping(guild, partial["target"]) or None,
            embed=            build_embed(event, guild),
            view=             view,
            allowed_mentions= discord.AllowedMentions(roles=True),
        )
    except Exception as e:
        await interaction.followup.send(
            f"❌ Failed to post in {channel.mention}: `{e}`", ephemeral=True
        )
        return

    event["message"] = msg.id

    # Use the atomic single-event save so we never overwrite a
    # presence_started=True flag that presence_loop may have just written
    # for another event between our earlier load and this save.
    await save_event(event_id, event)

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
    await interaction.followup.send(
        f"✅ Event created in {channel.mention}.\n"
        f"Audience: **{target_label(partial['target'])}**\n"
        f"{mode_str}",
        ephemeral=True,
    )


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

        # Discord does NOT allow responding to a modal with another modal (error 50035).
        # For link-only: all data is in hand, defer and post directly.
        # For normal mode: defer and present a "Step 2" button so the user can
        #   open ButtonNamesModal from a component interaction (allowed by Discord).
        await interaction.response.defer(ephemeral=True)

        if link_only:
            await _do_post_event(interaction, partial, [])
        else:
            await interaction.followup.send(
                f"✅ **Step 1 complete!**\n"
                f"Event: **{partial['event_name']}** — {count} RSVP button(s)\n\n"
                f"Click below to name your buttons.",
                view=Step2ButtonView(partial),
                ephemeral=True,
            )


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
                # Pure validation — no async before this, send_message is safe.
                await interaction.response.send_message(
                    "❌ No valid button names provided.", ephemeral=True
                )
                return

        # Defer now — _do_post_event does channel.send + load/save (async I/O).
        await interaction.response.defer(ephemeral=True)
        await _do_post_event(interaction, self.partial, names)


# ============================================================
# CREATE EVENT — STEP 2 BRIDGE VIEW
# ============================================================

class Step2ButtonView(discord.ui.View):
    """
    Sent ephemerally after Step 1 (EventInfoModal) completes.
    Discord forbids modal→modal chaining, so we bridge via a button:
      EventInfoModal.on_submit → defer → this view
      user clicks "Set Button Names" → send_modal(ButtonNamesModal)  ← allowed
    """

    def __init__(self, partial: Dict[str, Any]):
        super().__init__(timeout=300)
        self.partial = partial

    @discord.ui.button(label="Set Button Names →", style=discord.ButtonStyle.primary)
    async def proceed(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        await interaction.response.send_modal(ButtonNamesModal(self.partial))


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
        # Validate with pure Python first — no I/O before the first response
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

        # All validation passed — defer before any async I/O
        await interaction.response.defer(ephemeral=True)

        data  = await load_events()
        event = data.get(self.event_id)
        if not isinstance(event, dict):
            await interaction.followup.send("Event not found.", ephemeral=True)
            return

        event["title"]        = self.new_title.value.strip()
        event["description"]  = self.new_description.value.strip()
        event["timestamp"]    = int(dt.timestamp())
        event["redirect_url"] = redir
        event["link_only"]    = link_only

        data[self.event_id] = event
        await save_events(data)
        await refresh(interaction.client, self.event_id)
        await interaction.followup.send("✅ Event info updated.", ephemeral=True)


class EditButtonCountModal(discord.ui.Modal, title="Edit Buttons — How Many?"):
    """Step 1 of 2 for editing buttons: choose the new count."""

    count = discord.ui.TextInput(
        label="Number of buttons (0 = link-only, 1–5)",
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
            # Switching to link-only — defer before I/O
            await interaction.response.defer(ephemeral=True)
            data  = await load_events()
            event = data.get(self.event_id)
            if not isinstance(event, dict):
                await interaction.followup.send("Event not found.", ephemeral=True)
                return
            event["link_only"]       = True
            event["buttons"]         = []
            event["enabled_buttons"] = []
            data[self.event_id]      = event
            await save_events(data)
            await refresh(interaction.client, self.event_id)
            await interaction.followup.send(
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

        # Validation passed — defer before I/O
        await interaction.response.defer(ephemeral=True)

        data  = await load_events()
        event = data.get(self.event_id)
        if not isinstance(event, dict):
            await interaction.followup.send("Event not found.", ephemeral=True)
            return

        # Preserve existing signups for buttons whose name didn't change
        old_roles = event.get("roles", {})
        new_roles: Dict[str, List] = {}
        for n in names:
            new_roles[n] = old_roles.get(n, [])

        # Also reset capacities to only include the new names
        old_caps = event.get("capacities", {})
        new_caps = {n: old_caps[n] for n in names if n in old_caps}

        event["buttons"]         = names
        event["enabled_buttons"] = names
        event["roles"]           = new_roles
        event["capacities"]      = new_caps
        event["link_only"]       = False

        data[self.event_id] = event
        await save_events(data)
        await refresh(interaction.client, self.event_id)
        await interaction.followup.send(
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
        # Defer immediately — load_events/save_events/refresh are all async I/O
        await interaction.response.defer(ephemeral=True)

        data  = await load_events()
        event = data.get(self.event_id)
        if not isinstance(event, dict):
            await interaction.followup.send("Event not found.", ephemeral=True)
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
        await interaction.followup.send(
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
        # Defer immediately — load/save/refresh are async I/O
        await interaction.response.defer(ephemeral=True)

        data  = await load_events()
        event = data.get(self.event_id)
        if not isinstance(event, dict):
            await interaction.followup.send("Event not found.", ephemeral=True)
            return

        is_active = event.get("active", True) and not event.get("closed", False)
        event["active"] = not is_active
        event["closed"] = is_active       # flip

        data[self.event_id] = event
        await save_events(data)
        await refresh(interaction.client, self.event_id)

        state = "closed" if is_active else "reopened"
        await interaction.followup.send(
            f"✅ Event **{state}**.", ephemeral=True
        )

    @discord.ui.button(label="✅ Mark Event as Done", style=discord.ButtonStyle.success, row=2)
    async def finalize(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        # Restart-proof alternative to the DM "Mark Event as Done" button:
        # this panel is re-opened fresh from the persistent Manage button, so
        # the finalize path survives bot restarts (the DM view does not).
        #
        # Defer as a message-update (no ephemeral) so edit_original_response
        # disables this button on the panel itself — mirrors EventDoneView.done.
        await interaction.response.defer()

        data  = await load_events()
        event = data.get(self.event_id)
        if not isinstance(event, dict):
            await interaction.followup.send("Event not found.", ephemeral=True)
            return

        is_active = event.get("active", True) and not event.get("closed", False)
        if not is_active:
            await interaction.followup.send(
                "This event is already closed.", ephemeral=True
            )
            return

        # Disable to prevent a double-click paying AP twice while finalizing.
        button.disabled = True
        button.label    = "⏳ Finalizing…"
        try:
            await interaction.edit_original_response(view=self)
        except Exception:
            pass

        cog = interaction.client.cogs.get("EventCreator")
        if not cog:
            await interaction.followup.send(
                "❌ EventCreator cog not found.", ephemeral=True
            )
            return

        # Locks in VC times, awards AP to qualified attendees, moves members to
        # ARC Main, deletes the event VC, logs results, and closes the event.
        await cog._finalize_event(
            interaction= interaction,
            event_id=    self.event_id,
        )

    @discord.ui.button(label="🗑 Delete Event", style=discord.ButtonStyle.danger, row=1)
    async def delete(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        # Defer immediately — fetch_message, msg.delete, and save_events are all
        # network/disk I/O that can easily exceed 3 seconds under load.
        await interaction.response.defer(ephemeral=True)

        data  = await load_events()
        event = data.get(self.event_id)
        if not event:
            await interaction.followup.send("Event not found.", ephemeral=True)
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
        await interaction.followup.send("✅ Event deleted.", ephemeral=True)



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

        # Participation gating — directive ops restrict RSVP to a single role.
        restrict_role = event.get("restrict_role")
        if restrict_role and not any(
            r.name == restrict_role for r in interaction.user.roles
        ):
            await interaction.followup.send(
                f"❌ Only **{restrict_role}** members can sign up for this op.",
                ephemeral=True,
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
        self.bot               = bot
        self._views_registered = False

        # ── In-memory voice-channel attendance tracking ───────────────────────
        # These are rebuilt from disk on on_ready / on bot restart.
        #
        # _vc_event_map:   {voice_channel_id: event_id}
        #   Fast lookup: is this VC one of ours?
        #
        # _vc_join_times:  {event_id: {user_id: unix_join_timestamp}}
        #   Records when a member *entered* the event VC this session.
        #   Cleared when they leave; their time is folded into _vc_cumulative.
        #
        # _vc_cumulative:  {event_id: {user_id: total_seconds_so_far}}
        #   Accumulated VC time since tracking began.
        #   Persisted to disk every 2 min by _vc_save_loop.
        #
        # _vc_pulled:      {event_id: set(user_id, ...)}
        #   Members who have already been pulled into the event VC once.
        #   Once a member is in this set they are NEVER force-moved again,
        #   so leaving the event VC is always voluntary.
        self._vc_event_map:  Dict[int, str]            = {}
        self._vc_join_times: Dict[str, Dict[int, int]] = {}
        self._vc_cumulative: Dict[str, Dict[int, int]] = {}
        self._vc_pulled:     Dict[str, Set[int]]       = {}

        if not self.presence_loop.is_running():
            self.presence_loop.start()
        if not self._vc_save_loop.is_running():
            self._vc_save_loop.start()

    def cog_unload(self):
        if self.presence_loop.is_running():
            self.presence_loop.cancel()
        if self._vc_save_loop.is_running():
            self._vc_save_loop.cancel()

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
        vc_rebuilt = 0
        needs_save = False

        for event_id, event in data.items():
            if not isinstance(event, dict):
                continue

            event = normalize_event(event)

            if not event.get("guild_id"):
                continue

            is_active = event.get("active", True) and not event.get("closed", False)
            if not is_active:
                continue

            # ── Rebuild VC tracking state for events that were mid-session ────
            vc_id = event.get("event_vc_id")
            if isinstance(vc_id, int):
                guild = self.bot.get_guild(event["guild_id"])
                vc    = guild.get_channel(vc_id) if guild else None
                if isinstance(vc, discord.VoiceChannel):
                    # Restore in-memory maps
                    self._vc_event_map[vc_id] = event_id
                    # Load persisted cumulative times
                    persisted = event.get("vc_cumulative_times", {})
                    self._vc_cumulative[event_id] = {
                        int(k): int(v) for k, v in persisted.items()
                        if str(k).isdigit()
                    }
                    # Re-baseline join times for anyone already in the VC
                    now = int(datetime.now(timezone.utc).timestamp())
                    self._vc_join_times[event_id] = {
                        m.id: now for m in vc.members
                    }
                    vc_rebuilt += 1
                    print(
                        f"[event_creator] Restored VC tracking for event "
                        f"{event_id} (vc={vc_id}, {len(vc.members)} member(s) present)."
                    )
                else:
                    # VC no longer exists — clear the stale ID
                    event["event_vc_id"] = None
                    data[event_id]       = event
                    needs_save           = True
                    print(
                        f"[event_creator] Event {event_id}: stale event_vc_id "
                        f"{vc_id} cleared (VC not found)."
                    )

            msg_id    = event.get("message")
            buttons   = get_enabled_button_titles(event)
            redirect  = event.get("redirect_url", "")
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

        if needs_save:
            await save_events(data)

        print(
            f"[event_creator] Registered {registered}/{len(data)} "
            f"persistent event view(s); restored {vc_rebuilt} active event VC(s)."
        )

        for guild in self.bot.guilds:
            try:
                await ensure_hierarchy_log_channel(guild)
            except Exception:
                pass

    # ----------------------------------------------------------------
    # Presence loop — fires every 60 s
    # ----------------------------------------------------------------

    async def _create_event_vc(
        self,
        guild:    discord.Guild,
        event:    Dict[str, Any],
        event_id: str,
    ) -> Optional[discord.VoiceChannel]:
        """
        Create the temporary event voice channel and seed in-memory tracking.

        Returns the new VoiceChannel, or None if creation failed (caller should
        leave the event untouched and retry on the next loop). Does NOT mutate
        the event record beyond what the caller persists — the caller is
        responsible for writing event["event_vc_id"] and saving.
        """
        # ── Determine VC category (same as the announcement channel) ─────────
        ann_channel = guild.get_channel(event.get("channel"))
        category    = ann_channel.category if isinstance(
            ann_channel, discord.TextChannel
        ) else None

        # ── Build permission overwrites for the event audience ───────────────
        bot_member = guild.get_member(self.bot.user.id) if self.bot.user else None
        overwrites = _build_vc_overwrites(
            guild,
            target=     event.get("target", "security_only"),
            bot_member= bot_member,
        )

        vc_name = f"🔴 {str(event.get('title', 'Event'))[:90]}"
        try:
            event_vc = await guild.create_voice_channel(
                name=       vc_name,
                category=   category,
                overwrites= overwrites,
                reason=     f"Event attendance tracking: {event_id}",
            )
        except discord.Forbidden:
            print(
                f"[event_creator] Missing 'Manage Roles' permission — cannot "
                f"set overwrites for event VC ({event_id}). "
                "Creating without access restrictions as fallback."
            )
            try:
                event_vc = await guild.create_voice_channel(
                    name=     vc_name,
                    category= category,
                    reason=   f"Event attendance tracking (no overwrites): {event_id}",
                )
            except Exception as e2:
                print(
                    f"[event_creator] Fallback VC creation also failed for "
                    f"{event_id}: {type(e2).__name__}: {e2}"
                )
                return None
        except Exception as e:
            print(
                f"[event_creator] Failed to create event VC for {event_id}: "
                f"{type(e).__name__}: {e}"
            )
            return None

        # Seed in-memory tracking so on_voice_state_update starts counting
        # cumulative time immediately (including early joiners before start).
        self._vc_event_map[event_vc.id] = event_id
        self._vc_join_times.setdefault(event_id, {})
        self._vc_cumulative.setdefault(event_id, {})
        for m in event_vc.members:
            self._vc_join_times[event_id].setdefault(
                m.id, int(datetime.now(timezone.utc).timestamp())
            )
        return event_vc

    @tasks.loop(seconds=60)
    async def presence_loop(self):
        """
        Two-phase event lifecycle, both driven from this loop:

        Phase 1 — VC_PRECREATE_SECONDS (5 min) before the scheduled time:
          • Creates the temporary voice channel in the same category as the
            announcement channel so it is ready before members arrive.

        Phase 2 — at the scheduled start time:
          • Verifies the VC still exists; if it was never created or has since
            been deleted, it is (re)created now.
          • Pulls every RSVP'd (non-Decline) member currently in any VC into
            the event VC.
          • DMs the event creator a single "Mark Event as Done" button.
          • Marks the event as started (presence_started=True).

        Cumulative VC-time tracking runs continuously via on_voice_state_update
        for any VC in _vc_event_map, so time accrues from the moment the VC is
        created in Phase 1.
        """
        data    = await load_events()
        now_ts  = int(datetime.now(timezone.utc).timestamp())
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
            if not isinstance(ts, int):
                continue
            # Not yet inside the pre-create window (5 min before start).
            if now_ts < ts - VC_PRECREATE_SECONDS:
                continue

            guild_id   = event.get("guild_id")
            creator_id = event.get("creator")
            if not isinstance(guild_id, int) or not isinstance(creator_id, int):
                continue

            guild   = self.bot.get_guild(guild_id)
            creator = guild.get_member(creator_id) if guild else None
            if not guild or not creator:
                continue

            # ── PHASE 1: ensure the event VC exists ──────────────────────────
            # Reuse a previously-created VC if it is still alive; otherwise
            # create it now. This both pre-creates the VC 5 min before start
            # and self-heals if the VC was deleted or never created (e.g. the
            # bot was offline during the pre-create window and is only now
            # catching up at/after the start time).
            vc_id    = event.get("event_vc_id")
            event_vc = guild.get_channel(vc_id) if isinstance(vc_id, int) else None
            if not isinstance(event_vc, discord.VoiceChannel):
                event_vc = await self._create_event_vc(guild, event, event_id)
                if event_vc is None:
                    continue   # creation failed — retry next loop
                event["event_vc_id"] = event_vc.id
                data[event_id]       = event
                changed              = True
                when = "pre-created" if now_ts < ts else "created at start"
                print(
                    f"[event_creator] Event '{event.get('title')}' VC "
                    f"{when} (VC={event_vc.id})."
                )

            # ── PHASE 2 gate: wait until the scheduled start time ─────────────
            # Before start, the VC simply sits ready; we do not pull members or
            # notify the creator until the event actually begins.
            if now_ts < ts:
                continue

            # ── PHASE 2: pull RSVP'd members (and the creator) already in a VC ─
            participants = compute_pull_targets(event)
            pulled       = 0
            pulled_set   = self._vc_pulled.setdefault(event_id, set())
            for pid in participants:
                member = guild.get_member(pid)
                if not member:
                    continue
                if member.voice and member.voice.channel \
                        and member.voice.channel.id != event_vc.id:
                    try:
                        await member.move_to(
                            event_vc,
                            reason="Event started — moved to event VC",
                        )
                        pulled_set.add(pid)
                        pulled += 1
                    except Exception:
                        pass  # member may have left VC between check and move

            # ── Mark event as started ────────────────────────────────────────
            event["presence_started"]     = True
            event["presence_started_utc"] = datetime.now(timezone.utc).isoformat()
            data[event_id]                = event
            changed                       = True

            vc_name = event_vc.name
            print(
                f"[event_creator] Event '{event.get('title')}' started. "
                f"VC={event_vc.id}, pulled {pulled}/{len(participants)} "
                f"RSVP'd member(s)."
            )

            # ── DM the creator with the "Mark as Done" button ────────────────
            try:
                dm    = await creator.create_dm()
                embed = discord.Embed(
                    title=       "⚔️ Your event has started!",
                    description= (
                        f"**Fleet:** {event.get('title', 'Event')}\n"
                        f"**Scheduled time:** <t:{ts}:F>\n\n"
                        f"A temporary voice channel **{vc_name}** has been created.\n"
                        f"RSVP'd members currently in a VC have been pulled in.\n\n"
                        f"**Attendance is being tracked automatically.**\n"
                        f"Members need **15 minutes** of cumulative VC time to qualify.\n\n"
                        f"When the fleet is over, press the button below."
                    ),
                    color=     discord.Color.blurple(),
                    timestamp= datetime.now(timezone.utc),
                )
                embed.set_footer(
                    text=f"Event ID: {event_id}"
                )
                await dm.send(
                    embed= embed,
                    view=  EventDoneView(event_id, self.bot),
                )
            except discord.Forbidden:
                print(
                    f"[event_creator] Cannot DM creator {creator_id} "
                    "(DMs closed). Event {event_id} VC created but creator not notified."
                )
            except Exception as e:
                print(
                    f"[event_creator] DM error for event {event_id}: "
                    f"{type(e).__name__}: {e}"
                )

        if changed:
            await save_events(data)

    @presence_loop.before_loop
    async def _before_presence_loop(self):
        await self.bot.wait_until_ready()

    # ----------------------------------------------------------------
    # Voice-channel attendance tracking
    # ----------------------------------------------------------------

    @commands.Cog.listener()
    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after:  discord.VoiceState,
    ):
        """
        Tracks cumulative VC time and pulls RSVP'd members into the event VC.

        Four cases handled per update:
          A) Member LEFT the event VC  → fold session time into cumulative.
          B) Member JOINED the event VC → record join timestamp.
          C) RSVP'd member joined any OTHER VC → move them to the event VC.
          D) All other updates          → ignored.
        """
        if not self._vc_event_map:
            return   # No active event VCs — fast exit

        now = int(datetime.now(timezone.utc).timestamp())

        left_event_vc   = before.channel and before.channel.id in self._vc_event_map
        joined_event_vc = after.channel  and after.channel.id  in self._vc_event_map

        # ── A: Member left the event VC ──────────────────────────────────────
        if left_event_vc:
            event_id = self._vc_event_map[before.channel.id]
            join_ts  = self._vc_join_times.get(event_id, {}).pop(member.id, None)
            if join_ts is not None:
                elapsed  = max(0, now - join_ts)
                cum      = self._vc_cumulative.setdefault(event_id, {})
                cum[member.id] = cum.get(member.id, 0) + elapsed

        # ── B: Member joined the event VC ────────────────────────────────────
        if joined_event_vc and not left_event_vc:
            event_id = self._vc_event_map[after.channel.id]
            self._vc_join_times.setdefault(event_id, {})[member.id] = now
            return   # Nothing else to do

        # ── C: RSVP'd member (or the creator) joined a NON-event VC ──────────
        # Pull them into the event VC for their guild ONLY if they have not
        # been pulled before. Once pulled (at event start or first join),
        # the member is free to leave voluntarily without being dragged back.
        if after.channel and not joined_event_vc:
            guild = member.guild
            # Find any event VC in this guild
            for vc_id, event_id in list(self._vc_event_map.items()):
                event_vc = guild.get_channel(vc_id)
                if not isinstance(event_vc, discord.VoiceChannel):
                    continue

                # Skip if this member has already been pulled for this event
                pulled_set = self._vc_pulled.setdefault(event_id, set())
                if member.id in pulled_set:
                    break

                data  = await load_events()
                event = data.get(event_id)
                if not isinstance(event, dict):
                    continue

                event        = normalize_event(event)
                participants = set(compute_pull_targets(event))

                if member.id in participants:
                    try:
                        await member.move_to(
                            event_vc,
                            reason="Event in progress — RSVP'd member joined a VC (first pull only)",
                        )
                        pulled_set.add(member.id)
                    except Exception:
                        pass
                    # The resulting on_voice_state_update for the move into
                    # event_vc will be handled by case B above.
                    break

    # ----------------------------------------------------------------
    # Periodic persistence of in-memory VC times  (every 2 minutes)
    # ----------------------------------------------------------------

    @tasks.loop(seconds=120)
    async def _vc_save_loop(self):
        """
        Banks in-memory cumulative VC times to disk so attendance is cumulative
        across leaves/rejoins and survives a bot restart mid-event.

        This loop is the source of truth for time and is self-healing: rather
        than trusting that every join/leave fired an on_voice_state_update, it
        reconciles against who is *actually* in the event VC right now. Each
        tick it banks every present member's elapsed time into their cumulative
        total and re-baselines their join timestamp, so:

          • A missed join event can't make a whole session vanish — anyone
            physically in the VC is picked up and credited from here on.
          • A missed leave event (or a restart while the member was away) can't
            keep counting — members no longer in the VC have their pending
            session folded in and their timer stopped.
          • Time already banked is never re-counted, so leaving and rejoining
            the same event keeps adding up instead of resetting.
        """
        if not self._vc_event_map:
            return

        data    = await load_events()
        changed = False
        now     = int(datetime.now(timezone.utc).timestamp())

        for vc_id, event_id in list(self._vc_event_map.items()):
            event = data.get(event_id)
            if not isinstance(event, dict):
                continue

            # Mutate the live in-memory stores directly (not copies) so banked
            # time and re-baselined timers persist for the next tick/finalize.
            cum   = self._vc_cumulative.setdefault(event_id, {})
            joins = self._vc_join_times.setdefault(event_id, {})

            # Resolve the actual VC so we can reconcile against real presence.
            guild = self.bot.get_guild(event.get("guild_id")) if isinstance(event.get("guild_id"), int) else None
            vc    = guild.get_channel(vc_id) if guild else None

            if isinstance(vc, discord.VoiceChannel):
                present_ids = {m.id for m in vc.members if not m.bot}

                # Bank + re-baseline everyone currently present (self-healing:
                # seeds anyone whose join event was missed).
                for uid in present_ids:
                    join_ts = joins.get(uid)
                    if join_ts is not None:
                        cum[uid] = cum.get(uid, 0) + max(0, now - join_ts)
                    joins[uid] = now

                # Fold + stop the timer for anyone we were tracking who is no
                # longer in the VC (self-healing: catches missed leave events).
                for uid in list(joins.keys()):
                    if uid not in present_ids:
                        join_ts = joins.pop(uid)
                        cum[uid] = cum.get(uid, 0) + max(0, now - join_ts)
            else:
                # VC not resolvable (e.g. guild not cached yet) — fall back to a
                # non-destructive estimate so we never lose progress, without
                # touching the live timers.
                for uid, join_ts in list(joins.items()):
                    cum[uid] = cum.get(uid, 0) + max(0, now - join_ts)
                    joins[uid] = now

            event["vc_cumulative_times"] = {str(k): v for k, v in cum.items()}
            data[event_id]               = event
            changed                      = True

        if changed:
            await save_events(data)

    @_vc_save_loop.before_loop
    async def _before_vc_save_loop(self):
        await self.bot.wait_until_ready()

    # ----------------------------------------------------------------
    # Event finalization  (called by EventDoneView)
    # ----------------------------------------------------------------

    async def _finalize_event(
        self,
        interaction: discord.Interaction,
        event_id:    str,
    ) -> None:
        """
        Called when the creator marks the event as done.

          1. Lock in all cumulative times (including ongoing sessions).
          2. Determine qualified members (≥ EVENT_VC_MIN_SECONDS).
          3. Award +5 AP and boost to each qualified participant (excluding
             the creator if they hold an excluded role).
          4. Log the final attendance list to #arc-hierarchy-log.
          5. Move everyone currently in the event VC to ARC Main.
          6. Delete the event VC.
          7. Close the event.
        """
        data  = await load_events()
        event = data.get(event_id)
        if not isinstance(event, dict):
            try:
                await interaction.followup.send("Event not found.", ephemeral=True)
            except Exception:
                pass
            return

        event     = normalize_event(event)
        guild_id  = event.get("guild_id")
        guild     = self.bot.get_guild(guild_id) if isinstance(guild_id, int) else None
        if not guild:
            try:
                await interaction.followup.send("Guild not found.", ephemeral=True)
            except Exception:
                pass
            return

        now    = int(datetime.now(timezone.utc).timestamp())
        vc_id  = event.get("event_vc_id")
        vc     = guild.get_channel(vc_id) if isinstance(vc_id, int) else None

        # ── 1. Lock in cumulative times ───────────────────────────────────────
        cum        = dict(self._vc_cumulative.get(event_id) or {})
        join_times = dict(self._vc_join_times.get(event_id, {}))

        # If in-memory data is absent (most commonly because the bot restarted
        # after the event VC was already deleted, so on_ready never restored
        # _vc_cumulative for this event), fall back to what the 2-minute
        # save-loop last persisted to disk.  Without this, qualified_ids would
        # always be empty after a restart, awarding 0 AP.
        if not cum and not join_times:
            persisted_raw = event.get("vc_cumulative_times") or {}
            cum = {
                int(k): int(v) for k, v in persisted_raw.items()
                if str(k).isdigit()
            }
            if cum:
                print(
                    f"[event_creator] _finalize_event: in-memory VC times for "
                    f"event {event_id} were empty; restored {len(cum)} "
                    f"entry(s) from the last disk snapshot."
                )
            else:
                print(
                    f"[event_creator] _finalize_event: WARNING — no VC time "
                    f"data found for event {event_id} (both in-memory and disk "
                    f"snapshots are empty). Qualified list will be empty."
                )

        for uid, join_ts in join_times.items():
            elapsed  = max(0, now - join_ts)
            cum[uid] = cum.get(uid, 0) + elapsed

        event["vc_cumulative_times"] = {str(k): v for k, v in cum.items()}

        # ── 2. Determine qualified members ────────────────────────────────────
        qualified_ids = [
            uid for uid, secs in cum.items()
            if secs >= EVENT_VC_MIN_SECONDS
        ]
        event["vc_qualified"] = qualified_ids

        # ── 3. Award AP to each qualified participant ─────────────────────────
        creator_id = event.get("creator")
        creator_m  = guild.get_member(creator_id) if isinstance(creator_id, int) else None
        event_title = str(event.get("title", "Event"))

        ap_count    = 0
        ap_failures: List[str] = []
        if creator_m and not has_any_role(creator_m, PRESENCE_BONUS_EXCLUDED_ROLES):
            for uid in qualified_ids:
                # The creator now qualifies (so the op counts for the directive),
                # but they must not pay themselves the per-attendee AP/boost.
                if uid == creator_id:
                    continue
                part_m = guild.get_member(uid)
                if not part_m:
                    continue
                try:
                    awarded = await award_creator_5ap(
                        guild, creator_m, part_m, event_title
                    )
                    if not awarded:
                        # award_creator_5ap returns False when the import of
                        # award_ap_with_bonuses fails or when the function itself
                        # raises — it never propagates the exception.  Log here
                        # so the failure is visible instead of silently ignored.
                        print(
                            f"[event_creator] award_creator_5ap returned False "
                            f"for uid={uid} in event {event_id}. Verify that "
                            f"cogs.ap_tracking.award_ap_with_bonuses exists and "
                            f"its call signature matches."
                        )
                        ap_failures.append(part_m.display_name)
                    else:
                        ap_count += 1

                    boosted = await register_or_extend_boost(
                        creator_id=     creator_m.id,
                        participant_id= part_m.id,
                        event_id=       event_id,
                    )
                    if not boosted:
                        print(
                            f"[event_creator] register_or_extend_boost failed "
                            f"for uid={uid} in event {event_id}."
                        )

                except Exception as e:
                    print(
                        f"[event_creator] AP award raised for uid={uid} "
                        f"in event {event_id}: {type(e).__name__}: {e}"
                    )
                    ap_failures.append(part_m.display_name if part_m else str(uid))

        # ── 4. Log to #arc-hierarchy-log ──────────────────────────────────────
        rsvp_ids = compute_participants(event)
        try:
            await log_confirmed_participants(
                guild,
                event_title=   event_title,
                event_id=      event_id,
                qualified_ids= qualified_ids,
                rsvp_ids=      rsvp_ids,
                cum_times=     {str(k): v for k, v in cum.items()},
            )
        except Exception as e:
            print(f"[event_creator] Log error for {event_id}: {e}")

        # ── 5. Move everyone in the event VC → ARC Main ───────────────────────
        moved = 0
        if isinstance(vc, discord.VoiceChannel):
            arc_main = discord.utils.get(guild.voice_channels, name=ARC_MAIN_VC)
            if arc_main:
                for member in list(vc.members):
                    try:
                        await member.move_to(
                            arc_main,
                            reason="Event ended — moved to ARC Main",
                        )
                        moved += 1
                    except Exception:
                        pass
            else:
                print(
                    f"[event_creator] ARC Main VC not found in guild {guild_id}. "
                    "Members not moved."
                )

        # ── 6. Delete the event VC ────────────────────────────────────────────
        if isinstance(vc, discord.VoiceChannel):
            try:
                await vc.delete(reason=f"Event ended: {event_id}")
            except Exception as e:
                print(
                    f"[event_creator] Could not delete event VC {vc_id}: "
                    f"{type(e).__name__}: {e}"
                )

        # ── 7. Close the event ────────────────────────────────────────────────
        event["active"]    = False
        event["closed"]    = True
        event["event_vc_id"] = None
        data[event_id]     = event
        await save_events(data)
        await refresh(self.bot, event_id)

        # ── Directive credit ──────────────────────────────────────────────────
        # Notify the Directive cog on every finalize. It credits the op when the
        # event carries a directive tag OR its title matches a directive op name
        # (crediting the creator); it returns "no_directive" cheaply otherwise.
        dcog = self.bot.cogs.get("DirectiveCog")
        if dcog:
            try:
                # Stamp the globally-unique event UUID so the directive dedup
                # keys on it — distinct same-named ops can never collide.
                event.setdefault("uuid", event_id)
                status = await dcog.credit_directive_completion(event)
                print(f"[event_creator] directive credit for {event_id}: {status}")
            except Exception as e:
                print(f"[event_creator] directive credit failed for {event_id}: {e}")

        # Clean up in-memory state
        self._vc_event_map.pop(vc_id, None)
        self._vc_join_times.pop(event_id, None)
        self._vc_cumulative.pop(event_id, None)
        self._vc_pulled.pop(event_id, None)

        print(
            f"[event_creator] Event '{event_title}' finalized. "
            f"Qualified: {len(qualified_ids)}, AP awarded: {ap_count}, "
            f"AP failures: {len(ap_failures)}, "
            f"Members moved to ARC Main: {moved}."
        )

        # ── DM summary back to creator ────────────────────────────────────────
        summary_lines = [
            f"✅ **Fleet '{event_title}' has been closed.**\n",
            f"**Qualified attendees (≥15 min):** {len(qualified_ids)}",
            f"**AP awards sent:** {ap_count}",
            f"**Members moved to ARC Main:** {moved}",
        ]
        if ap_failures:
            summary_lines.append(
                f"\n⚠️ **AP award failed for {len(ap_failures)} participant(s):** "
                + ", ".join(ap_failures)
                + "\nCheck the bot console for details."
            )
        if creator_m and has_any_role(creator_m, PRESENCE_BONUS_EXCLUDED_ROLES):
            summary_lines.append(
                "\n_(No AP bonuses applied — your role is excluded from receiving them.)_"
            )
        try:
            await interaction.followup.send(
                "\n".join(summary_lines), ephemeral=True
            )
        except Exception:
            pass

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
        description="Show fleet attendance — 15-min qualified members only.",
    )
    @app_commands.describe(
        member="Optional: filter to a single member's attendance history.",
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

        # Newest-first
        sorted_events = sorted(
            [(eid, e) for eid, e in data.items() if isinstance(e, dict)],
            key=lambda kv: kv[1].get("timestamp", 0),
            reverse=True,
        )

        if member:
            # ── per-member report ──────────────────────────────────────────
            if not has_any_role(member, {SUBSIDIZED_PING_ROLE}):
                await interaction.followup.send(
                    f"{member.mention} does not have the **{SUBSIDIZED_PING_ROLE}** role "
                    f"and is not included in the fleet log.",
                    ephemeral=True,
                )
                return

            uid   = member.id
            lines: List[str] = []

            for event_id, event in sorted_events:
                event = normalize_event(event)
                title = str(event.get("title", "Untitled"))
                ts    = event.get("timestamp", 0)

                qualified = current_qualified_ids(event)
                if uid not in qualified:
                    continue

                date_str = f"<t:{ts}:d>" if ts else "?"
                lines.append(f"• **{title}** ({date_str}) — ✅ Attended")

            if not lines:
                await interaction.followup.send(
                    f"{member.mention} has no qualified fleet attendance records.",
                    ephemeral=True,
                )
                return

            header = (
                f"📋 Fleet attendance for **{member.display_name}** "
                f"— {len(lines)} fleet(s) attended\n\n"
            )
            body = "\n".join(lines)

        else:
            # ── all-fleets report ──────────────────────────────────────────
            # Only list members who currently hold the ARC Subsidized role.
            sections: List[str] = []

            for event_id, event in sorted_events:
                event     = normalize_event(event)
                title     = str(event.get("title", "Untitled"))
                ts        = event.get("timestamp", 0)
                is_active = event.get("active", True) and not event.get("closed", False)
                status    = "🟢" if is_active else "🔴"
                date_str  = f"<t:{ts}:d>" if ts else "?"

                qualified_ids = current_qualified_ids(event)
                if not qualified_ids:
                    continue

                part_lines: List[str] = []

                for uid in qualified_ids:
                    m = guild.get_member(uid) if guild else None
                    if not m or not has_any_role(m, {SUBSIDIZED_PING_ROLE}):
                        continue
                    part_lines.append(f"  ✅ {m.display_name}")

                if part_lines:
                    sections.append(
                        f"{status} **{title}** — {date_str}\n"
                        + "\n".join(part_lines)
                    )

            if not sections:
                await interaction.followup.send(
                    f"No qualified attendance records found for "
                    f"**{SUBSIDIZED_PING_ROLE}** members.",
                    ephemeral=True,
                )
                return

            header = (
                f"📋 **Fleet Attendance Log** — {SUBSIDIZED_PING_ROLE} members  "
                f"(✅ = attended, ≥15 min in event VC)\n\n"
            )
            body = "\n\n".join(sections)

        full = header + body

        if len(full) <= 2000:
            await interaction.followup.send(full, ephemeral=True)
        else:
            buf = io.BytesIO(full.encode("utf-8"))
            await interaction.followup.send(
                (
                    f"📋 Fleet attendance for **{member.display_name}**"
                    if member
                    else "📋 Fleet Attendance Log"
                )
                + " — too long to display inline, see attached file.",
                file=discord.File(buf, filename="fleet_attendance_log.txt"),
                ephemeral=True,
            )


    # ----------------------------------------------------------------
    # /close_event
    # ----------------------------------------------------------------

    # Roles permitted to forcibly close any event (in addition to server admins)
    CLOSE_EVENT_ROLES: Set[str] = {
        "ARC Security Corporation Leader",
        "ARC Security Administration Council",
        "ARC General",
    }

    @app_commands.command(
        name="close_event",
        description="[Leadership] Finalize and close an active event, awarding AP to qualified attendees.",
    )
    @app_commands.describe(
        event_title="Exact or partial title of the event to close.",
    )
    async def close_event(
        self,
        interaction: discord.Interaction,
        event_title: str,
    ) -> None:
        await interaction.response.defer(ephemeral=True)

        # ── Authorization check ───────────────────────────────────────────────
        if not isinstance(interaction.user, discord.Member):
            await interaction.followup.send("Must be used in a server.", ephemeral=True)
            return

        is_admin       = interaction.user.guild_permissions.administrator
        has_close_role = any(
            r.name in self.CLOSE_EVENT_ROLES for r in interaction.user.roles
        )
        if not (is_admin or has_close_role):
            await interaction.followup.send(
                "❌ You are not authorised to close events.\n"
                "Required roles: **ARC Security Corporation Leader**, "
                "**ARC Security Administration Council**, or **ARC General**.",
                ephemeral=True,
            )
            return

        guild = interaction.guild
        if not guild:
            await interaction.followup.send("Must be used in a server.", ephemeral=True)
            return

        # ── Find the event ────────────────────────────────────────────────────
        data         = await load_events()
        search_lower = event_title.strip().lower()

        candidates = [
            (eid, e)
            for eid, e in data.items()
            if isinstance(e, dict)
            and search_lower in str(e.get("title", "")).lower()
            and (e.get("active", True) and not e.get("closed", False))
        ]

        if not candidates:
            await interaction.followup.send(
                f"❌ No **active** event matching **\"{event_title}\"** was found.\n"
                "Check the title spelling or use `/event_log` to browse events.",
                ephemeral=True,
            )
            return

        if len(candidates) > 1:
            lines = "\n".join(
                f"• `{eid[:8]}…` — **{e.get('title')}** "
                f"(<t:{e.get('timestamp', 0)}:d>)"
                for eid, e in candidates[:10]
            )
            await interaction.followup.send(
                f"⚠️ Multiple active events matched. Please be more specific:\n{lines}",
                ephemeral=True,
            )
            return

        event_id, event = candidates[0]
        event           = normalize_event(event)
        true_title      = str(event.get("title", "Event"))

        # ── Confirm intent before finalizing ─────────────────────────────────
        confirm_view = _CloseEventConfirmView(
            invoker=    interaction.user,
            cog=        self,
            event_id=   event_id,
            event_title=true_title,
        )

        creator_id = event.get("creator")
        creator_m  = guild.get_member(creator_id) if isinstance(creator_id, int) else None
        ts         = event.get("timestamp", 0)

        confirm_embed = discord.Embed(
            title=       f"⚠️ Close Event — Confirm",
            description= (
                f"You are about to **finalize and close** this event.\n\n"
                f"**Title:** {true_title}\n"
                f"**Creator:** {creator_m.mention if creator_m else f'<@{creator_id}>'}\n"
                f"**Scheduled:** <t:{ts}:F>\n\n"
                "This will:\n"
                "• Lock in all VC attendance times\n"
                "• Award AP to qualified members (≥15 min)\n"
                "• Move remaining members to **ARC Main**\n"
                "• Delete the event voice channel\n"
                "• Mark the event as **Closed**\n\n"
                "**This cannot be undone.**"
            ),
            color=discord.Color.orange(),
            timestamp=datetime.now(timezone.utc),
        )
        confirm_embed.set_footer(text=f"Event ID: {event_id}")

        msg = await interaction.followup.send(
            embed=confirm_embed, view=confirm_view, ephemeral=True
        )
        confirm_view.preview_msg = msg

    # ----------------------------------------------------------------
    # /retro_event_ap
    # ----------------------------------------------------------------

    @app_commands.command(
        name="retro_event_ap",
        description="[Admin] Retroactively award AP for an event that closed without finalizing.",
    )
    @app_commands.describe(
        event_title="Exact or partial event title to search for.",
        creator="The Discord member who created the event.",
    )
    async def retro_event_ap(
        self,
        interaction: discord.Interaction,
        event_title: str,
        creator:     discord.Member,
    ) -> None:
        await interaction.response.defer(ephemeral=True)

        # CEO / Director only
        if not (
            isinstance(interaction.user, discord.Member)
            and (
                interaction.user.guild_permissions.administrator
                or any(
                    r.name in {
                        "ARC Security Corporation Leader",
                        "ARC Security Administration Council",
                    }
                    for r in interaction.user.roles
                )
            )
        ):
            await interaction.followup.send("❌ Not authorised.", ephemeral=True)
            return

        guild = interaction.guild
        if not guild:
            await interaction.followup.send("Must be used in a server.", ephemeral=True)
            return

        # ── 1. Find the event record ──────────────────────────────────────────
        data = await load_events()
        search_title = event_title.strip().lower()

        candidates = [
            (eid, e) for eid, e in data.items()
            if isinstance(e, dict)
            and search_title in str(e.get("title", "")).lower()
            and int(e.get("creator", -1)) == creator.id
        ]

        if not candidates:
            await interaction.followup.send(
                f"❌ No event matching **\"{event_title}\"** found for "
                f"{creator.mention}.\n\n"
                "Check that the title matches and the creator is correct. "
                "Use `/event_log` to browse past events.",
                ephemeral=True,
            )
            return

        if len(candidates) > 1:
            lines = "\n".join(
                f"• `{eid[:8]}…` — **{e.get('title')}** "
                f"(<t:{e.get('timestamp', 0)}:d>)"
                for eid, e in candidates[:10]
            )
            await interaction.followup.send(
                f"⚠️ Multiple events matched. Please be more specific:\n{lines}",
                ephemeral=True,
            )
            return

        event_id, event = candidates[0]
        event           = normalize_event(event)
        true_title      = str(event.get("title", "Event"))

        # ── 2. Check if AP was already awarded ───────────────────────────────
        if event.get("retro_ap_awarded"):
            await interaction.followup.send(
                f"⚠️ Retroactive AP for **\"{true_title}\"** was already awarded. "
                "Run again would double-award. Aborting.",
                ephemeral=True,
            )
            return

        # ── 3. Read saved VC times ────────────────────────────────────────────
        cum_raw: Dict[str, Any] = event.get("vc_cumulative_times") or {}

        if not cum_raw:
            await interaction.followup.send(
                f"❌ No VC attendance data found for **\"{true_title}\"**.\n\n"
                "The `vc_cumulative_times` field is empty — the save loop may not "
                "have run before the VC was deleted. AP cannot be awarded automatically.\n"
                "Use `/give_ap` to award AP manually if needed.",
                ephemeral=True,
            )
            return

        # ── 4. Determine qualified members (≥15 min) ─────────────────────────
        qualified: List[Tuple[discord.Member, int]] = []   # (member, seconds)
        unresolved: List[int] = []

        for uid_str, secs in cum_raw.items():
            try:
                uid = int(uid_str)
            except ValueError:
                continue
            # The creator may now appear in the VC times (they get pulled in and
            # tracked), but they never receive the per-attendee AP for themselves.
            if uid == creator.id:
                continue
            m = guild.get_member(uid)
            if m is None:
                unresolved.append(uid)
                continue
            if int(secs) >= EVENT_VC_MIN_SECONDS:
                qualified.append((m, int(secs)))

        # ── 5. Check if creator is AP-excluded ───────────────────────────────
        creator_excluded = has_any_role(creator, PRESENCE_BONUS_EXCLUDED_ROLES)
        ap_per_participant = 5
        total_ap = len(qualified) * ap_per_participant if not creator_excluded else 0

        # ── 6. Build preview embed ────────────────────────────────────────────
        preview = discord.Embed(
            title=       f"📋 Retro AP Preview — \"{true_title}\"",
            description= (
                "Review the attendees below and confirm to award AP.\n"
                "**No AP has been awarded yet.**"
            ),
            color=       discord.Color.orange(),
            timestamp=   datetime.now(timezone.utc),
        )
        preview.add_field(name="Creator",   value=creator.mention,  inline=True)
        preview.add_field(name="Event",     value=true_title,        inline=True)
        preview.add_field(
            name="AP per participant",
            value=f"{ap_per_participant} AP" if not creator_excluded else "0 (excluded role)",
            inline=True,
        )

        if qualified:
            qual_lines = "\n".join(
                f"• {m.display_name} — "
                f"{s // 60} min {s % 60} s"
                for m, s in sorted(qualified, key=lambda x: -x[1])
            )
            preview.add_field(
                name=  f"✅ Qualified ({len(qualified)}) — ≥15 min",
                value= qual_lines[:1024],
                inline=False,
            )
        else:
            preview.add_field(
                name="✅ Qualified",
                value="Nobody met the 15-minute threshold.",
                inline=False,
            )

        # Show below-threshold members for transparency
        below = [
            (int(uid_str), int(secs))
            for uid_str, secs in cum_raw.items()
            if int(secs) < EVENT_VC_MIN_SECONDS
        ]
        if below:
            below_lines = "\n".join(
                f"• <@{uid}> — {s // 60} min {s % 60} s"
                for uid, s in sorted(below, key=lambda x: -x[1])[:10]
            )
            preview.add_field(
                name=  f"❌ Below threshold ({len(below)}) — <15 min",
                value= below_lines[:512],
                inline=False,
            )

        if unresolved:
            preview.add_field(
                name=  f"⚠️ {len(unresolved)} member(s) no longer in server",
                value= "Their VC time was recorded but they cannot receive AP.",
                inline=False,
            )

        preview.add_field(
            name=  "Total AP to be awarded",
            value= f"**{total_ap} AP** to {creator.mention}",
            inline=False,
        )
        preview.set_footer(text="Press Confirm to proceed, or Cancel to abort.")

        view = RetroApConfirmView(
            invoker=    interaction.user,
            cog=        self,
            event_id=   event_id,
            event_title=true_title,
            creator=    creator,
            qualified=  [m for m, _ in qualified],
        )

        msg = await interaction.followup.send(
            embed=preview, view=view, ephemeral=True
        )
        view.preview_msg = msg

    # ----------------------------------------------------------------
    # /rerun_event_ap
    # ----------------------------------------------------------------

    # Roles permitted to run /rerun_event_ap (same as close_event)
    RERUN_AP_ROLES: Set[str] = {
        "ARC Security Corporation Leader",
        "ARC Security Administration Council",
        "ARC General",
    }

    @app_commands.command(
        name="rerun_event_ap",
        description="[Admin] Re-run AP awarding for an event whose automatic payout failed.",
    )
    @app_commands.describe(
        event_id='Short Event ID shown in the embed footer, e.g. "EVT-A3F72B".',
    )
    async def rerun_event_ap(
        self,
        interaction: discord.Interaction,
        event_id: str,
    ) -> None:
        await interaction.response.defer(ephemeral=True)

        # ── Authorization ─────────────────────────────────────────────────────
        if not isinstance(interaction.user, discord.Member):
            await interaction.followup.send("Must be used in a server.", ephemeral=True)
            return

        is_admin       = interaction.user.guild_permissions.administrator
        has_rerun_role = any(
            r.name in self.RERUN_AP_ROLES for r in interaction.user.roles
        )
        if not (is_admin or has_rerun_role):
            await interaction.followup.send(
                "❌ You are not authorised to re-run AP awards.\n"
                "Required roles: **ARC Security Corporation Leader**, "
                "**ARC Security Administration Council**, or **ARC General**.",
                ephemeral=True,
            )
            return

        guild = interaction.guild
        if not guild:
            await interaction.followup.send("Must be used in a server.", ephemeral=True)
            return

        # ── Find the event by short_id ────────────────────────────────────────
        lookup = event_id.strip().upper()
        data   = await load_events()

        match_id:    Optional[str]          = None
        match_event: Optional[Dict[str, Any]] = None

        for eid, ev in data.items():
            if not isinstance(ev, dict):
                continue
            # Support both the stored short_id and the full UUID prefix so
            # admins can paste either format.
            stored = str(ev.get("short_id", "")).upper()
            if stored == lookup or eid.upper().startswith(lookup):
                match_id    = eid
                match_event = ev
                break

        if match_id is None or match_event is None:
            await interaction.followup.send(
                f"❌ No event found with ID **{event_id}**.\n"
                "The Event ID is shown in the embed footer, e.g. `EVT-A3F72B`. "
                "Use `/event_log` to browse past events.",
                ephemeral=True,
            )
            return

        match_event = normalize_event(match_event)
        true_title  = str(match_event.get("title", "Event"))
        short_id    = match_event.get("short_id", match_id[:10])

        # ── Guard: already awarded via this command ───────────────────────────
        if match_event.get("ap_rerun_awarded"):
            await interaction.followup.send(
                f"⚠️ AP for **\"{true_title}\"** (`{short_id}`) was already "
                "re-run via this command and cannot be awarded again to prevent "
                "double-awarding.",
                ephemeral=True,
            )
            return

        # ── Guard: also block if retro AP was awarded via /retro_event_ap ─────
        if match_event.get("retro_ap_awarded"):
            await interaction.followup.send(
                f"⚠️ Retroactive AP for **\"{true_title}\"** (`{short_id}`) was "
                "already awarded via `/retro_event_ap`. Cannot award again.",
                ephemeral=True,
            )
            return

        # ── Resolve qualified members from vc_qualified ───────────────────────
        # /rerun_event_ap specifically targets events that DID finalize (so
        # vc_qualified is already computed) but whose award_creator_5ap calls
        # silently failed.  Fall back to vc_cumulative_times if vc_qualified
        # is missing (e.g. very old event data).
        qualified_ids_raw: List[int] = current_qualified_ids(match_event)

        if not qualified_ids_raw:
            # Try computing from the persisted cumulative times
            cum_raw = match_event.get("vc_cumulative_times") or {}
            qualified_ids_raw = [
                int(k) for k, v in cum_raw.items()
                if str(k).isdigit() and int(v) >= EVENT_VC_MIN_SECONDS
            ]

        creator_id = match_event.get("creator")
        creator_m  = guild.get_member(creator_id) if isinstance(creator_id, int) else None
        ts         = match_event.get("timestamp", 0)

        # Resolve member objects and separate those who left the server.
        # The creator now qualifies for the op, but is never paid per-attendee
        # AP for their own attendance — drop them from the payout list.
        qualified_members: List[discord.Member] = []
        unresolved_ids:    List[int]            = []
        for uid in qualified_ids_raw:
            if uid == creator_id:
                continue
            m = guild.get_member(uid)
            if m:
                qualified_members.append(m)
            else:
                unresolved_ids.append(uid)

        creator_excluded = (
            has_any_role(creator_m, PRESENCE_BONUS_EXCLUDED_ROLES)
            if creator_m else False
        )
        ap_per_part = 5
        total_ap    = len(qualified_members) * ap_per_part if not creator_excluded else 0

        # ── Build preview embed ───────────────────────────────────────────────
        preview = discord.Embed(
            title=       f"🔁 Re-run AP Preview — \"{true_title}\"",
            description= (
                "This will re-send AP awards that failed during automatic "
                "finalization.\n**No AP has been awarded yet.**"
            ),
            color=       discord.Color.orange(),
            timestamp=   datetime.now(timezone.utc),
        )
        preview.add_field(name="Event ID",  value=f"`{short_id}`",      inline=True)
        preview.add_field(
            name="Creator",
            value=creator_m.mention if creator_m else f"<@{creator_id}>",
            inline=True,
        )
        preview.add_field(name="Scheduled", value=f"<t:{ts}:F>",        inline=True)
        preview.add_field(
            name="AP per participant",
            value=f"{ap_per_part} AP" if not creator_excluded else "0 (excluded role)",
            inline=True,
        )
        preview.add_field(
            name="Total AP to creator",
            value=f"**{total_ap} AP**",
            inline=True,
        )
        preview.add_field(name="\u200b", value="\u200b", inline=True)

        if qualified_members:
            preview.add_field(
                name=  f"✅ Qualified ({len(qualified_members)}) — ≥15 min in VC",
                value= "\n".join(
                    f"• {m.display_name}" for m in qualified_members
                )[:1024],
                inline=False,
            )
        else:
            preview.add_field(
                name="✅ Qualified",
                value="Nobody met the 15-minute threshold — 0 AP will be awarded.",
                inline=False,
            )

        if unresolved_ids:
            preview.add_field(
                name=  f"⚠️ {len(unresolved_ids)} member(s) no longer in server",
                value= "Their VC time was recorded but they cannot be credited.",
                inline=False,
            )

        if creator_excluded:
            preview.add_field(
                name="ℹ️ Creator excluded",
                value="Creator holds a role excluded from AP bonuses — no AP awarded.",
                inline=False,
            )

        preview.set_footer(text="Press Confirm to award, or Cancel to abort.")

        view = RerunApConfirmView(
            invoker=    interaction.user,
            cog=        self,
            event_id=   match_id,
            short_id=   short_id,
            event_title=true_title,
            creator=    creator_m,
            qualified=  qualified_members,
        )

        msg = await interaction.followup.send(
            embed=preview, view=view, ephemeral=True
        )
        view.preview_msg = msg


class _CloseEventConfirmView(discord.ui.View):
    """
    Two-button confirmation shown ephemerally when a leadership member runs
    /close_event.  Only the invoking member can press the buttons.
    Timeout = 5 minutes.
    """

    def __init__(
        self,
        invoker:     discord.Member,
        cog:         "EventCreator",
        event_id:    str,
        event_title: str,
    ) -> None:
        super().__init__(timeout=300)
        self.invoker     = invoker
        self.cog         = cog
        self.event_id    = event_id
        self.event_title = event_title
        self.preview_msg: Optional[discord.Message] = None

    def _auth(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.invoker.id

    def _disable_all(self) -> None:
        for item in self.children:
            item.disabled = True  # type: ignore[attr-defined]
        self.stop()

    @discord.ui.button(label="✅ Confirm — Close Event", style=discord.ButtonStyle.danger)
    async def confirm(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not self._auth(interaction):
            await interaction.response.send_message(
                "❌ Only the member who ran this command can confirm.", ephemeral=True
            )
            return

        button.disabled = True
        button.label    = "⏳ Closing…"
        try:
            await interaction.response.edit_message(view=self)
        except discord.InteractionResponded:
            # Already responded (shouldn't happen here, but guard anyway)
            pass
        except Exception:
            # edit_message can fail on ephemeral messages after certain time;
            # defer as a fallback so the token stays valid for the followup.
            try:
                await interaction.response.defer()
            except Exception:
                pass

        self._disable_all()

        # Delegate to the shared finalization helper
        await self.cog._finalize_event(
            interaction=interaction,
            event_id=   self.event_id,
        )

    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not self._auth(interaction):
            await interaction.response.send_message(
                "❌ Only the member who ran this command can cancel.", ephemeral=True
            )
            return

        await interaction.response.defer()
        self._disable_all()

        cancel_embed = discord.Embed(
            title=       "🚫 Close Event Cancelled",
            description= "No changes were made.",
            color=       discord.Color.greyple(),
        )
        if self.preview_msg:
            try:
                await self.preview_msg.edit(embed=cancel_embed, view=self)
            except Exception:
                pass

    async def on_timeout(self) -> None:
        self._disable_all()
        if self.preview_msg:
            try:
                timeout_embed = discord.Embed(
                    title=       "⏰ Close Event Timed Out",
                    description= "No confirmation received within 5 minutes. The event was not closed.",
                    color=       discord.Color.orange(),
                )
                await self.preview_msg.edit(embed=timeout_embed, view=self)
            except Exception:
                pass


class RetroApConfirmView(discord.ui.View):
    """
    Two-button confirmation shown to the invoking admin before retro AP is awarded.
    Only the admin who ran /retro_event_ap can press the buttons.
    """

    def __init__(
        self,
        invoker:     discord.Member,
        cog:         "EventCreator",
        event_id:    str,
        event_title: str,
        creator:     discord.Member,
        qualified:   List[discord.Member],
    ) -> None:
        super().__init__(timeout=300)   # 5 minutes to confirm
        self.invoker     = invoker
        self.cog         = cog
        self.event_id    = event_id
        self.event_title = event_title
        self.creator     = creator
        self.qualified   = qualified
        self.preview_msg: Optional[discord.Message] = None

    def _auth(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.invoker.id

    def _disable_all(self) -> None:
        for item in self.children:
            item.disabled = True  # type: ignore[attr-defined]
        self.stop()

    @discord.ui.button(label="✅ Confirm — Award AP", style=discord.ButtonStyle.success)
    async def confirm(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not self._auth(interaction):
            await interaction.response.send_message(
                "❌ Only the admin who ran this command can confirm.", ephemeral=True
            )
            return

        await interaction.response.defer()
        self._disable_all()

        guild       = interaction.guild
        ap_count    = 0
        failed:     List[str] = []
        excluded    = has_any_role(self.creator, PRESENCE_BONUS_EXCLUDED_ROLES)

        if not excluded:
            for part_m in self.qualified:
                try:
                    await award_creator_5ap(
                        guild, self.creator, part_m, self.event_title
                    )
                    await register_or_extend_boost(
                        creator_id=     self.creator.id,
                        participant_id= part_m.id,
                        event_id=       self.event_id,
                    )
                    ap_count += 1
                except Exception as e:
                    print(
                        f"[event_creator] retro AP failed for "
                        f"{part_m.id}: {e}"
                    )
                    failed.append(part_m.display_name)

        # Mark event so it can't be re-awarded
        data = await load_events()
        if self.event_id in data and isinstance(data[self.event_id], dict):
            data[self.event_id]["retro_ap_awarded"] = True
            data[self.event_id]["active"]           = False
            data[self.event_id]["closed"]           = True
            await save_events(data)

        # Log to #arc-hierarchy-log
        log_ch = await ensure_hierarchy_log_channel(guild)
        if log_ch:
            log_embed = discord.Embed(
                title=     f"🔁 Retroactive AP Awarded — \"{self.event_title}\"",
                color=     discord.Color.green(),
                timestamp= datetime.now(timezone.utc),
            )
            log_embed.add_field(name="Creator",    value=self.creator.mention, inline=True)
            log_embed.add_field(name="Actioned by",value=interaction.user.mention, inline=True)
            log_embed.add_field(
                name="AP awarded",
                value=f"**{ap_count * 5} AP** ({ap_count} participants × 5 AP)",
                inline=False,
            )
            if self.qualified:
                log_embed.add_field(
                    name="Qualified participants",
                    value="\n".join(f"• {m.display_name}" for m in self.qualified)[:1024],
                    inline=False,
                )
            if excluded:
                log_embed.add_field(
                    name="Note",
                    value="Creator holds an excluded role — no AP awarded.",
                    inline=False,
                )
            if failed:
                log_embed.add_field(
                    name="⚠️ Award failed for",
                    value=", ".join(failed),
                    inline=False,
                )
            try:
                await log_ch.send(embed=log_embed)
            except Exception:
                pass

        # Update the preview message
        result_embed = discord.Embed(
            title=     f"✅ Retro AP Awarded — \"{self.event_title}\"",
            color=     discord.Color.green(),
            timestamp= datetime.now(timezone.utc),
        )
        result_embed.add_field(name="Creator",  value=self.creator.mention, inline=True)
        result_embed.add_field(name="AP sent",  value=f"**{ap_count * 5} AP** to creator", inline=True)
        result_embed.add_field(name="Participants", value=str(ap_count), inline=True)
        if failed:
            result_embed.add_field(
                name="⚠️ Failed", value=", ".join(failed), inline=False
            )

        if self.preview_msg:
            try:
                await self.preview_msg.edit(embed=result_embed, view=self)
            except Exception:
                pass

    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not self._auth(interaction):
            await interaction.response.send_message(
                "❌ Only the admin who ran this command can cancel.", ephemeral=True
            )
            return

        await interaction.response.defer()
        self._disable_all()

        cancel_embed = discord.Embed(
            title=       "🚫 Retro AP Cancelled",
            description= "No AP was awarded.",
            color=       discord.Color.greyple(),
        )
        if self.preview_msg:
            try:
                await self.preview_msg.edit(embed=cancel_embed, view=self)
            except Exception:
                pass

    async def on_timeout(self) -> None:
        self._disable_all()
        if self.preview_msg:
            try:
                timeout_embed = discord.Embed(
                    title=       "⏰ Retro AP Timed Out",
                    description= "No confirmation received within 5 minutes. No AP was awarded.",
                    color=       discord.Color.orange(),
                )
                await self.preview_msg.edit(embed=timeout_embed, view=self)
            except Exception:
                pass


async def setup(bot: commands.Bot):
    await bot.add_cog(EventCreator(bot))

# ============================================================
# RERUN AP CONFIRM VIEW
# ============================================================

class RerunApConfirmView(discord.ui.View):
    """
    Two-button confirmation shown before /rerun_event_ap awards AP.

    Targets events that went through the normal VC-based finalization flow
    but whose award_creator_5ap calls failed silently (e.g. because
    cogs.ap_tracking was unavailable or had a mismatched signature).

    Differences from RetroApConfirmView:
      - Uses the pre-computed vc_qualified list — no vc_cumulative_times
        parsing required (the event already knows who qualified).
      - Sets ap_rerun_awarded rather than retro_ap_awarded so the two
        guards remain independent and the log clearly shows which path ran.
      - Accepts an explicit short_id for cleaner embed text.
    """

    def __init__(
        self,
        invoker:     discord.Member,
        cog:         "EventCreator",
        event_id:    str,
        short_id:    str,
        event_title: str,
        creator:     Optional[discord.Member],
        qualified:   List[discord.Member],
    ) -> None:
        super().__init__(timeout=300)
        self.invoker     = invoker
        self.cog         = cog
        self.event_id    = event_id
        self.short_id    = short_id
        self.event_title = event_title
        self.creator     = creator
        self.qualified   = qualified
        self.preview_msg: Optional[discord.Message] = None

    def _auth(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.invoker.id

    def _disable_all(self) -> None:
        for item in self.children:
            item.disabled = True  # type: ignore[attr-defined]
        self.stop()

    @discord.ui.button(label="✅ Confirm — Award AP", style=discord.ButtonStyle.success)
    async def confirm(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not self._auth(interaction):
            await interaction.response.send_message(
                "❌ Only the admin who ran this command can confirm.", ephemeral=True
            )
            return

        await interaction.response.defer()
        self._disable_all()

        guild    = interaction.guild
        ap_count = 0
        failed:  List[str] = []
        excluded = (
            has_any_role(self.creator, PRESENCE_BONUS_EXCLUDED_ROLES)
            if self.creator else True
        )

        if self.creator and not excluded:
            for part_m in self.qualified:
                try:
                    awarded = await award_creator_5ap(
                        guild, self.creator, part_m, self.event_title
                    )
                    if not awarded:
                        print(
                            f"[event_creator] rerun_event_ap: award_creator_5ap "
                            f"returned False for uid={part_m.id} in "
                            f"event {self.event_id}."
                        )
                        failed.append(part_m.display_name)
                    else:
                        ap_count += 1

                    boosted = await register_or_extend_boost(
                        creator_id=     self.creator.id,
                        participant_id= part_m.id,
                        event_id=       self.event_id,
                    )
                    if not boosted:
                        print(
                            f"[event_creator] rerun_event_ap: "
                            f"register_or_extend_boost failed for "
                            f"uid={part_m.id} in event {self.event_id}."
                        )
                except Exception as e:
                    print(
                        f"[event_creator] rerun_event_ap: exception for "
                        f"uid={part_m.id} in event {self.event_id}: "
                        f"{type(e).__name__}: {e}"
                    )
                    failed.append(part_m.display_name)

        # ── Persist the awarded flag so this can't be run twice ───────────────
        data = await load_events()
        if self.event_id in data and isinstance(data[self.event_id], dict):
            data[self.event_id]["ap_rerun_awarded"]     = True
            data[self.event_id]["ap_rerun_awarded_by"]  = interaction.user.id
            data[self.event_id]["ap_rerun_awarded_utc"] = (
                datetime.now(timezone.utc).isoformat()
            )
            await save_events(data)

        # ── Log to #arc-hierarchy-log ─────────────────────────────────────────
        log_ch = await ensure_hierarchy_log_channel(guild)
        if log_ch:
            log_embed = discord.Embed(
                title=     f"🔁 Re-run AP Awarded — \"{self.event_title}\"",
                color=     discord.Color.green(),
                timestamp= datetime.now(timezone.utc),
            )
            log_embed.add_field(
                name="Event ID",    value=f"`{self.short_id}`",           inline=True
            )
            log_embed.add_field(
                name="Creator",
                value=self.creator.mention if self.creator else "_(unknown)_",
                inline=True,
            )
            log_embed.add_field(
                name="Actioned by", value=interaction.user.mention,       inline=True
            )
            log_embed.add_field(
                name="AP awarded",
                value=f"**{ap_count * 5} AP** ({ap_count} × 5 AP)",
                inline=False,
            )
            if self.qualified:
                log_embed.add_field(
                    name="Qualified participants",
                    value="\n".join(
                        f"• {m.display_name}" for m in self.qualified
                    )[:1024],
                    inline=False,
                )
            if excluded:
                log_embed.add_field(
                    name="Note",
                    value="Creator holds an excluded role — no AP awarded.",
                    inline=False,
                )
            if failed:
                log_embed.add_field(
                    name="⚠️ Award failed for",
                    value=", ".join(failed),
                    inline=False,
                )
            try:
                await log_ch.send(embed=log_embed)
            except Exception:
                pass

        # ── Update the confirmation message ───────────────────────────────────
        result_embed = discord.Embed(
            title=     f"✅ Re-run AP Awarded — \"{self.event_title}\"",
            color=     discord.Color.green(),
            timestamp= datetime.now(timezone.utc),
        )
        result_embed.add_field(
            name="Event ID",    value=f"`{self.short_id}`",               inline=True
        )
        result_embed.add_field(
            name="Creator",
            value=self.creator.mention if self.creator else "_(unknown)_",
            inline=True,
        )
        result_embed.add_field(
            name="AP sent",     value=f"**{ap_count * 5} AP**",          inline=True
        )
        result_embed.add_field(
            name="Participants credited", value=str(ap_count),           inline=True
        )
        if failed:
            result_embed.add_field(
                name="⚠️ Failed", value=", ".join(failed),               inline=False
            )
        if excluded:
            result_embed.add_field(
                name="ℹ️ Excluded",
                value="Creator's role is excluded — no AP awarded.",
                inline=False,
            )

        if self.preview_msg:
            try:
                await self.preview_msg.edit(embed=result_embed, view=self)
            except Exception:
                pass

    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not self._auth(interaction):
            await interaction.response.send_message(
                "❌ Only the admin who ran this command can cancel.", ephemeral=True
            )
            return

        await interaction.response.defer()
        self._disable_all()

        cancel_embed = discord.Embed(
            title=       "🚫 Re-run AP Cancelled",
            description= "No AP was awarded.",
            color=       discord.Color.greyple(),
        )
        if self.preview_msg:
            try:
                await self.preview_msg.edit(embed=cancel_embed, view=self)
            except Exception:
                pass

    async def on_timeout(self) -> None:
        self._disable_all()
        if self.preview_msg:
            try:
                timeout_embed = discord.Embed(
                    title=       "⏰ Re-run AP Timed Out",
                    description= "No confirmation received within 5 minutes. No AP was awarded.",
                    color=       discord.Color.orange(),
                )
                await self.preview_msg.edit(embed=timeout_embed, view=self)
            except Exception:
                pass