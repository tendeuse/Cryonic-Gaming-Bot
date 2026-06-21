# cogs/directive.py
#
# ARC Officer Activity Tracker
# ============================
# Replaces the old three-focus-track program. Tracks how many events each ARC
# officer HOSTS during a Monday→Sunday week and rewards / enforces activity.
#
# Tracked ranks (low → high):
#   ARC Petty Officer · ARC Lieutenant · ARC Commander · ARC General ·
#   ARC Security Administration Council · ARC Security Corporation Leader
#
# Behaviour:
#   - 200 AP bonus every time an officer hosts 3 events in a week (repeatable —
#     6 events = 400 AP). The weekly host count resets every Monday (UTC).
#   - Two panels in #arc-directives, each with a "Create Event" button:
#       • Petty Officer panel  → only the 3 unrestricted ops.
#       • Officer panel (Lt+)  → all 7 titles; the 4 "class" titles are gated to
#         ARC Lieutenant or above and forced to the ARC Security audience.
#   - ARC Lieutenant + ARC Commander are MANDATED to host ≥3 events per week.
#     At the Monday rollover the bot posts a prompt in #directives-logs (pinging
#     the ARC Tendeuse role) for every mandated officer who fell short, with
#     Justify / Demote buttons usable only by ARC General or above. Demotion is
#     manual — nothing happens automatically.
#   - An event only counts when it is FINALIZED and met the minimum attendance
#     (creator + at least 1 other, ≥15 min in VC). The Event cog calls
#     credit_directive_completion() on finalize.

import os
import asyncio
import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import discord
from discord.ext import commands, tasks
from discord import app_commands

from . import db

# =====================
# CHANNELS
# =====================
DIRECTIVES_CHANNEL = "arc-directives"
LOG_CHANNEL        = "directives-logs"

# =====================
# PROGRAM CONFIG
# =====================
WEEKLY_QUOTA  = 3              # events a mandated officer must host per week
AP_PER_EVENTS = 3             # every N hosted events …
AP_BONUS      = 200           # … grants this much AP (repeatable per week)
RESTRICT_ROLE = "ARC Security"  # RSVP gate for the restricted class events
MIN_ATTENDEES = 2             # an op counts only with creator + 1 other (≥15 min)
TENDEUSE_ROLE = "ARC Tendeuse"  # pinged on every demotion prompt

# =====================
# RANK LADDER (low → high)
# =====================
PETTY_OFFICER_ROLE = "ARC Petty Officer"
LIEUTENANT_ROLE    = "ARC Lieutenant"
COMMANDER_ROLE     = "ARC Commander"
GENERAL_ROLE       = "ARC General"
DIRECTOR_ROLE      = "ARC Security Administration Council"
CEO_ROLE           = "ARC Security Corporation Leader"

RANK_LADDER: List[str] = [
    PETTY_OFFICER_ROLE,
    LIEUTENANT_ROLE,
    COMMANDER_ROLE,
    GENERAL_ROLE,
    DIRECTOR_ROLE,
    CEO_ROLE,
]

# Ranks that must host ≥ WEEKLY_QUOTA events per week (demotion risk).
MANDATED_ROLES = {LIEUTENANT_ROLE, COMMANDER_ROLE}

# Map a ladder role name → arc_hierarchy rank key (for demotion).
RANK_KEY_BY_ROLE = {
    PETTY_OFFICER_ROLE: "petty_officer",
    LIEUTENANT_ROLE:    "lieutenant",
    COMMANDER_ROLE:     "commander",
    GENERAL_ROLE:       "general",
}

# =====================
# EVENT TITLES
# =====================
# Restricted "class" titles — ARC Lieutenant+ only, ARC-Security-only audience.
RESTRICTED_TITLES = [
    "Dscan and Threat Assessment",
    "Scanning Class",
    "Rolling Class",
    "OpSec 101 Class",
]
# Open ops — any tracked officer, no RSVP role restriction.
OPEN_TITLES = [
    "Mining/PVE Fleet",
    "PVP Hunt",
    "Faction Warfare Fleet",
]
ALL_TITLES = RESTRICTED_TITLES + OPEN_TITLES


def is_restricted_title(title: str) -> bool:
    return title in RESTRICTED_TITLES


# =====================
# PERSISTENCE
# =====================
PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)
DATA_FILE = PERSIST_ROOT / "directives.json"

_file_lock = asyncio.Lock()


def utcnow_iso() -> str:
    return datetime.datetime.utcnow().isoformat()


def _monday_of(d: datetime.date) -> datetime.date:
    return d - datetime.timedelta(days=d.weekday())


def current_monday() -> str:
    today = datetime.datetime.now(datetime.timezone.utc).date()
    return _monday_of(today).isoformat()


def _empty_data() -> Dict[str, Any]:
    return {
        "week_start":        current_monday(),
        "hosts":             {},
        "panels":            {},
        "pending_demotions": {},
    }


def _atomic_write(path: Path, data: Dict[str, Any]) -> None:
    # Stored in MySQL kv_store under the old filename stem ("directives").
    db.kv_save(path.stem, data)


async def load_data() -> Dict[str, Any]:
    async with _file_lock:
        try:
            data = await asyncio.to_thread(db.kv_load, "directives", None)
            if not isinstance(data, dict) or not data:
                return _empty_data()
            # Drop any stale fields from the old focus-track schema and ensure
            # the keys we rely on exist.
            data.setdefault("week_start", current_monday())
            data.setdefault("hosts", {})
            data.setdefault("panels", {})
            data.setdefault("pending_demotions", {})
            if not isinstance(data["hosts"], dict):
                data["hosts"] = {}
            if not isinstance(data["panels"], dict):
                data["panels"] = {}
            if not isinstance(data["pending_demotions"], dict):
                data["pending_demotions"] = {}
            return data
        except Exception:
            return _empty_data()


async def save_data(data: Dict[str, Any]) -> None:
    async with _file_lock:
        await asyncio.to_thread(_atomic_write, DATA_FILE, data)


def _host_record(data: Dict[str, Any], uid: int) -> Dict[str, Any]:
    rec = data["hosts"].get(str(uid))
    if not isinstance(rec, dict):
        rec = {"count": 0, "bonuses_awarded": 0, "events": []}
        data["hosts"][str(uid)] = rec
    rec.setdefault("count", 0)
    rec.setdefault("bonuses_awarded", 0)
    rec.setdefault("events", [])
    return rec


# =====================
# RANK HELPERS
# =====================

def effective_rank_role(member: discord.Member) -> Optional[str]:
    """Highest ladder role the member holds, or None if they hold none."""
    held = {r.name for r in member.roles}
    for role_name in reversed(RANK_LADDER):
        if role_name in held:
            return role_name
    return None


def _rank_index(role_name: Optional[str]) -> int:
    if role_name is None:
        return -1
    try:
        return RANK_LADDER.index(role_name)
    except ValueError:
        return -1


def is_tracked_officer(member: discord.Member) -> bool:
    return effective_rank_role(member) is not None


def is_lieutenant_or_above(member: discord.Member) -> bool:
    return _rank_index(effective_rank_role(member)) >= _rank_index(LIEUTENANT_ROLE)


def is_general_or_above(member: discord.Member) -> bool:
    return _rank_index(effective_rank_role(member)) >= _rank_index(GENERAL_ROLE)


def is_mandated(member: discord.Member) -> bool:
    return effective_rank_role(member) in MANDATED_ROLES


def has_role(member: discord.Member, name: str) -> bool:
    return discord.utils.get(member.roles, name=name) is not None


# =====================
# LOGGING
# =====================

async def _log(guild: discord.Guild, message: str) -> None:
    ch = discord.utils.get(guild.text_channels, name=LOG_CHANNEL)
    if not ch:
        try:
            ch = await guild.create_text_channel(LOG_CHANNEL)
        except Exception:
            return
    try:
        await ch.send(message[:2000])
    except Exception:
        pass


# =====================
# EMBED BUILDERS
# =====================

def _week_range_str(week_start: str) -> str:
    try:
        start = datetime.date.fromisoformat(week_start)
    except Exception:
        start = datetime.date.today()
    end = start + datetime.timedelta(days=6)
    return f"{start.isoformat()} → {end.isoformat()} (UTC)"


def _leaderboard_lines(
    data: Dict[str, Any],
    guild: discord.Guild,
    predicate,
) -> str:
    """Render this week's host counts for members matching `predicate`."""
    hosts = data.get("hosts", {})
    rows: List[Tuple[str, int]] = []
    for uid, rec in hosts.items():
        m = guild.get_member(int(uid))
        if not m or not predicate(m):
            continue
        rows.append((m.display_name, int(rec.get("count", 0))))
    if not rows:
        return "_(no events hosted yet this week)_"
    rows.sort(key=lambda r: r[1], reverse=True)
    out = []
    for name, c in rows:
        trophy = " 🏆" if c >= WEEKLY_QUOTA else ""
        out.append(f"• {name} — **{c}** event(s){trophy}")
    return "\n".join(out)


def build_petty_embed(data: Dict[str, Any], guild: discord.Guild) -> discord.Embed:
    embed = discord.Embed(
        title="🎯 ARC Directives — Petty Officer Ops",
        description=(
            "Host fleets to support the corp. Every **3 events** you host in a "
            f"week earns a **{AP_BONUS} AP** bonus (repeatable).\n\n"
            "Use **Create Event** below to schedule one of the available ops."
        ),
        color=discord.Color.teal(),
        timestamp=datetime.datetime.utcnow(),
    )
    embed.add_field(
        name="🚀 Available Ops (ARC Security only or both corps)",
        value="\n".join(f"• `{t}`" for t in OPEN_TITLES),
        inline=False,
    )
    embed.add_field(
        name="🏅 This Week",
        value=_leaderboard_lines(
            data, guild,
            lambda m: effective_rank_role(m) == PETTY_OFFICER_ROLE,
        ),
        inline=False,
    )
    embed.add_field(
        name="🗓 Week", value=_week_range_str(data.get("week_start", "")), inline=False
    )
    embed.set_footer(text="ARC Petty Officer • host events, earn AP")
    return embed


def build_officer_embed(data: Dict[str, Any], guild: discord.Guild) -> discord.Embed:
    embed = discord.Embed(
        title="🎖 ARC Directives — Officer Ops",
        description=(
            "Host fleets and run classes. Every **3 events** you host in a week "
            f"earns a **{AP_BONUS} AP** bonus (repeatable).\n\n"
            f"**ARC Lieutenant & ARC Commander must host at least {WEEKLY_QUOTA} "
            "events per week** (Monday→Sunday) unless justified to **ARC General "
            "or above** — otherwise you may be demoted.\n\n"
            "Use **Create Event** below to schedule an op or class."
        ),
        color=discord.Color.dark_gold(),
        timestamp=datetime.datetime.utcnow(),
    )
    embed.add_field(
        name="🎓 Classes (ARC Lieutenant+ · ARC Security audience)",
        value="\n".join(f"• `{t}`" for t in RESTRICTED_TITLES),
        inline=False,
    )
    embed.add_field(
        name="🚀 Fleets / Ops (ARC Security only or both corps)",
        value="\n".join(f"• `{t}`" for t in OPEN_TITLES),
        inline=False,
    )
    embed.add_field(
        name="🏅 This Week",
        value=_leaderboard_lines(data, guild, is_lieutenant_or_above),
        inline=False,
    )
    embed.add_field(
        name="🗓 Week", value=_week_range_str(data.get("week_start", "")), inline=False
    )
    embed.set_footer(text="ARC Lieutenant and above • mandated: 3 events / week")
    return embed


# =====================
# EVENT-CREATION FLOW (title select → modal → Event cog)
# =====================

class DirectiveEventModal(discord.ui.Modal):
    """Collects remaining event details after a title has been picked."""

    description = discord.ui.TextInput(
        label="Description",
        style=discord.TextStyle.paragraph,
        placeholder="What is the op about? Forming up, location, comms…",
        max_length=1000,
        required=True,
    )
    datetime_utc = discord.ui.TextInput(
        label="Date & Time (UTC)  —  YYYY-MM-DD HH:MM",
        placeholder="e.g. 2026-06-15 20:00",
        required=True,
    )
    rsvp_buttons = discord.ui.TextInput(
        label="RSVP buttons (blank = Accept,Decline)",
        placeholder="Accept, Tentative, Decline",
        required=False,
        max_length=120,
    )

    def __init__(self, title_value: str, target: str):
        super().__init__(title=f"Create Op — {title_value[:40]}")
        self.title_value = title_value
        # Restricted class events are always ARC-Security-only; open ops carry
        # the audience the host chose ("security_only" or "public" = both corps).
        self.target = "security_only" if is_restricted_title(title_value) else target

    async def on_submit(self, interaction: discord.Interaction):
        # Re-validate permission at submit time (roles may have changed).
        member = interaction.user
        if is_restricted_title(self.title_value) and (
            not isinstance(member, discord.Member)
            or not is_lieutenant_or_above(member)
        ):
            await interaction.response.send_message(
                f"❌ Only **{LIEUTENANT_ROLE}** or above can run **"
                f"{self.title_value}**.",
                ephemeral=True,
            )
            return

        # Validate date with pure Python before any async I/O.
        try:
            dt = datetime.datetime.strptime(
                self.datetime_utc.value.strip(), "%Y-%m-%d %H:%M"
            ).replace(tzinfo=datetime.timezone.utc)
        except ValueError:
            await interaction.response.send_message(
                "❌ Invalid date. Use `YYYY-MM-DD HH:MM` (UTC).", ephemeral=True
            )
            return

        # Parse RSVP button names (dedup, title-case); default to Accept/Decline.
        names: List[str] = []
        raw = self.rsvp_buttons.value.strip()
        if raw:
            seen = set()
            for part in raw.split(","):
                n = part.strip().title()
                if n and n.lower() not in seen:
                    seen.add(n.lower())
                    names.append(n)
        if not names:
            names = ["Accept", "Decline"]

        await interaction.response.defer(ephemeral=True)

        data       = await load_data()
        week_start = data.get("week_start", current_monday())

        partial = {
            "creator_id":   interaction.user.id,
            "target":       self.target,
            "event_name":   self.title_value,
            "description":  self.description.value.strip(),
            "timestamp":    int(dt.timestamp()),
            "redirect_url": "",
            "button_count": len(names),
            "link_only":    False,
        }
        extra = {
            "directive_officer_id":  interaction.user.id,
            "directive_cycle_start": week_start,
        }
        # Only the restricted class events gate RSVP to ARC Security.
        if is_restricted_title(self.title_value):
            extra["restrict_role"] = RESTRICT_ROLE

        # Reuse the Event cog's full posting pipeline.
        try:
            from cogs.event_creator import _do_post_event
        except Exception as e:
            await interaction.followup.send(
                f"❌ Event system unavailable: `{e}`", ephemeral=True
            )
            return

        await _do_post_event(interaction, partial, names, extra=extra)


class TitleSelect(discord.ui.Select):
    def __init__(self, titles: List[str]):
        options = [discord.SelectOption(label=t, value=t) for t in titles]
        super().__init__(
            placeholder="Choose the op title…",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        title = self.values[0]
        # Restricted classes are ARC-Security-only — skip straight to the modal.
        if is_restricted_title(title):
            await interaction.response.send_modal(
                DirectiveEventModal(title, "security_only")
            )
            return
        # Open ops let the host pick the audience (ARC Security only / both corps).
        await interaction.response.send_message(
            f"Choose the audience for **{title}**:",
            view=AudienceView(title),
            ephemeral=True,
        )


class TitleSelectView(discord.ui.View):
    def __init__(self, titles: List[str]):
        super().__init__(timeout=300)
        self.add_item(TitleSelect(titles))


class AudienceButton(discord.ui.Button):
    def __init__(self, title_value: str, target: str, label: str, style):
        super().__init__(label=label, style=style)
        self.title_value = title_value
        self.target = target

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(
            DirectiveEventModal(self.title_value, self.target)
        )


class AudienceView(discord.ui.View):
    """Audience picker shown for the open (non-restricted) ops."""

    def __init__(self, title_value: str):
        super().__init__(timeout=300)
        self.add_item(AudienceButton(
            title_value, "security_only",
            "🔒 ARC Security only", discord.ButtonStyle.secondary,
        ))
        self.add_item(AudienceButton(
            title_value, "public",
            "🌐 Both Corps (Security + Subsidized)", discord.ButtonStyle.primary,
        ))


# =====================
# PANEL VIEWS
# =====================

class PettyCreateButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            label="🚀 Create Event",
            style=discord.ButtonStyle.primary,
            custom_id="directive:create:petty",
        )

    async def callback(self, interaction: discord.Interaction):
        cog = interaction.client.cogs.get("DirectiveCog")
        if cog:
            await cog.handle_create(interaction, OPEN_TITLES, require_lt=False)


class OfficerCreateButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            label="🎖 Create Event",
            style=discord.ButtonStyle.primary,
            custom_id="directive:create:officer",
        )

    async def callback(self, interaction: discord.Interaction):
        cog = interaction.client.cogs.get("DirectiveCog")
        if cog:
            await cog.handle_create(interaction, ALL_TITLES, require_lt=True)


class PettyPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(PettyCreateButton())


class OfficerPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(OfficerCreateButton())


# =====================
# DEMOTION PROMPT VIEW
# =====================

class DemotionJustifyButton(discord.ui.Button):
    def __init__(self, uid: int):
        super().__init__(
            label="✅ Justify",
            style=discord.ButtonStyle.success,
            custom_id=f"directive:justify:{uid}",
        )
        self.uid = uid

    async def callback(self, interaction: discord.Interaction):
        cog = interaction.client.cogs.get("DirectiveCog")
        if cog:
            await cog.handle_demotion_decision(interaction, self.uid, demote=False)


class DemotionDemoteButton(discord.ui.Button):
    def __init__(self, uid: int):
        super().__init__(
            label="⬇️ Demote",
            style=discord.ButtonStyle.danger,
            custom_id=f"directive:demote:{uid}",
        )
        self.uid = uid

    async def callback(self, interaction: discord.Interaction):
        cog = interaction.client.cogs.get("DirectiveCog")
        if cog:
            await cog.handle_demotion_decision(interaction, self.uid, demote=True)


class DemotionPromptView(discord.ui.View):
    def __init__(self, uid: int):
        super().__init__(timeout=None)
        self.add_item(DemotionJustifyButton(uid))
        self.add_item(DemotionDemoteButton(uid))


# =====================
# COG
# =====================

class DirectiveCog(commands.Cog):
    """ARC Officer Activity Tracker — weekly host counts, AP bonuses, demotions."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._data_lock = asyncio.Lock()
        if not self.reset_check.is_running():
            self.reset_check.start()

    def cog_unload(self):
        self.reset_check.cancel()

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    @commands.Cog.listener()
    async def on_ready(self):
        # Persistent panel views.
        self.bot.add_view(PettyPanelView())
        self.bot.add_view(OfficerPanelView())

        async with self._data_lock:
            data = await load_data()
            await save_data(data)

        # Re-bind any open demotion prompts so their buttons keep working.
        for guild in self.bot.guilds:
            pend = data.get("pending_demotions", {}).get(str(guild.id), {})
            if isinstance(pend, dict):
                for uid, msg_id in pend.items():
                    try:
                        self.bot.add_view(
                            DemotionPromptView(int(uid)), message_id=int(msg_id)
                        )
                    except Exception:
                        pass

        for guild in self.bot.guilds:
            await self._ensure_panels(guild)

    async def _get_directives_channel(
        self, guild: discord.Guild
    ) -> Optional[discord.TextChannel]:
        ch = discord.utils.get(guild.text_channels, name=DIRECTIVES_CHANNEL)
        if ch:
            return ch
        try:
            return await guild.create_text_channel(DIRECTIVES_CHANNEL)
        except Exception:
            return None

    async def _ensure_panels(self, guild: discord.Guild) -> None:
        """Post or refresh the two panels in #arc-directives."""
        ch = await self._get_directives_channel(guild)
        if not ch:
            return

        async with self._data_lock:
            data = await load_data()
            panels = data.setdefault("panels", {})
            gp = panels.get(str(guild.id))
            if not isinstance(gp, dict):
                gp = {}
                panels[str(guild.id)] = gp
            changed = False

            specs = [
                ("petty",   build_petty_embed(data, guild),   PettyPanelView()),
                ("officer", build_officer_embed(data, guild), OfficerPanelView()),
            ]
            for key, embed, view in specs:
                msg_id = gp.get(key)
                if msg_id:
                    try:
                        existing = await ch.fetch_message(int(msg_id))
                        await existing.edit(embed=embed, view=view)
                        continue
                    except Exception:
                        pass  # message gone — repost
                try:
                    msg = await ch.send(embed=embed, view=view)
                    gp[key] = msg.id
                    changed = True
                except Exception:
                    pass

            if changed:
                await save_data(data)

    async def _refresh_panels(
        self, guild: discord.Guild, data: Optional[Dict[str, Any]] = None
    ) -> None:
        """Edit both existing panel messages with fresh embeds."""
        if data is None:
            data = await load_data()
        ch = discord.utils.get(guild.text_channels, name=DIRECTIVES_CHANNEL)
        if not ch:
            return
        gp = data.get("panels", {}).get(str(guild.id), {})
        if not isinstance(gp, dict):
            return
        specs = [
            ("petty",   build_petty_embed(data, guild),   PettyPanelView()),
            ("officer", build_officer_embed(data, guild), OfficerPanelView()),
        ]
        for key, embed, view in specs:
            msg_id = gp.get(key)
            if not msg_id:
                continue
            try:
                msg = await ch.fetch_message(int(msg_id))
                await msg.edit(embed=embed, view=view)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Create-event button handler
    # ------------------------------------------------------------------

    async def handle_create(
        self,
        interaction: discord.Interaction,
        titles: List[str],
        *,
        require_lt: bool,
    ) -> None:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message(
                "Must be used in a server.", ephemeral=True
            )
            return

        member = interaction.user
        if require_lt:
            if not is_lieutenant_or_above(member):
                await interaction.response.send_message(
                    f"❌ This panel is for **{LIEUTENANT_ROLE}** and above.",
                    ephemeral=True,
                )
                return
        else:
            if not is_tracked_officer(member):
                await interaction.response.send_message(
                    "❌ Only ARC officers can host directive events.",
                    ephemeral=True,
                )
                return

        await interaction.response.send_message(
            "Select the op title to create:",
            view=TitleSelectView(titles),
            ephemeral=True,
        )

    # ------------------------------------------------------------------
    # Called by the Event cog when a directive-tagged event is finalized
    # ------------------------------------------------------------------

    async def credit_directive_completion(
        self, event: Dict[str, Any], *, force: bool = False
    ) -> str:
        """
        Tick an officer's weekly host count for a finalized event and award the
        200-AP-per-3-events bonus when a new multiple of 3 is reached.

        Returns one of:
          "no_directive" | "not_qualified" | "wrong_cycle" | "credited"
        """
        officer_id = event.get("directive_officer_id")
        if not officer_id:
            return "no_directive"

        guild_id = event.get("guild_id")
        guild = self.bot.get_guild(int(guild_id)) if guild_id else None

        # Minimum attendance: creator + at least 1 other (≥15 min in VC).
        # Skipped when force=True (admin manually vouching for the op).
        if not force:
            qualified = [
                int(x) for x in (event.get("vc_qualified") or [])
                if isinstance(x, int)
            ]
            creator_id = int(event.get("creator") or officer_id)
            if creator_id not in qualified or len(qualified) < MIN_ATTENDEES:
                if guild:
                    m = guild.get_member(int(officer_id))
                    name = m.display_name if m else f"<@{officer_id}>"
                    await _log(
                        guild,
                        f"⚠️ **{name}**'s op **{event.get('title')}** did not meet "
                        f"the minimum (creator + 1 member, ≥15 min) — **not "
                        f"counted** ({len(qualified)} qualified).",
                    )
                return "not_qualified"

        event_key = event.get("short_id") or event.get("message")
        new_bonuses = 0
        count = 0
        async with self._data_lock:
            data = await load_data()

            # Ignore stragglers finalized after a Monday rollover.
            if event.get("directive_cycle_start") != data.get("week_start"):
                return "wrong_cycle"

            rec = _host_record(data, int(officer_id))

            # Don't double-count an already-credited event.
            already = any(
                e.get("event_id") == event_key
                for e in rec.get("events", [])
                if isinstance(e, dict)
            )
            if event_key is not None and already:
                return "credited"

            rec["count"] = int(rec.get("count", 0)) + 1
            rec.setdefault("events", []).append({
                "title":        event.get("title"),
                "event_id":     event_key,
                "finalized_at": utcnow_iso(),
            })
            count = rec["count"]

            # AP bonus: one 200-AP grant for every completed block of 3 events.
            earned_blocks = count // AP_PER_EVENTS
            new_bonuses = earned_blocks - int(rec.get("bonuses_awarded", 0))
            if new_bonuses > 0:
                rec["bonuses_awarded"] = earned_blocks

            await save_data(data)

        if guild:
            await self._refresh_panels(guild, data)
            m = guild.get_member(int(officer_id))
            name = m.display_name if m else f"<@{officer_id}>"
            await _log(
                guild,
                f"🏅 **{name}** hosted **{event.get('title')}** — "
                f"**{count}** event(s) this week.",
            )
            if new_bonuses > 0 and m:
                total = new_bonuses * AP_BONUS
                awarded = await self._award_bonus(guild, m, total, count)
                if awarded:
                    await _log(
                        guild,
                        f"💰 **{name}** earned **{total} AP** for hosting "
                        f"{count} events this week (3-event bonus).",
                    )

        return "credited"

    async def _award_bonus(
        self, guild: discord.Guild, member: discord.Member, amount: int, count: int
    ) -> bool:
        """Award the flat 200-AP-per-3-events bonus via the AP system."""
        try:
            from cogs.ap_tracking import award_ap_with_bonuses
        except Exception as e:
            print(f"[directive] bonus import failed: {e}")
            return False
        try:
            await award_ap_with_bonuses(
                guild=guild,
                earner=member,
                base_amount=float(amount),
                source="weekly directive activity",
                reason=f"Hosted {count} events this week (3-event AP bonus)",
                log=True,
                actor=None,
                distribution_embed=True,
                distribution_title="Directive Activity Bonus",
            )
            return True
        except Exception as e:
            print(f"[directive] bonus award failed: {e}")
            return False

    # ------------------------------------------------------------------
    # Weekly rollover loop (Monday, UTC)
    # ------------------------------------------------------------------

    @tasks.loop(minutes=5)
    async def reset_check(self):
        async with self._data_lock:
            data = await load_data()
            stored = data.get("week_start")
            this_monday = current_monday()
            if stored == this_monday:
                return  # still the same week — nothing to do
            # New week detected — snapshot last week's host counts.
            hosts_snapshot = dict(data.get("hosts", {}))

        # Post results + demotion prompts for every guild.
        for guild in self.bot.guilds:
            await self._post_week_results(guild, hosts_snapshot)
            await self._post_demotion_prompts(guild, hosts_snapshot)

        # Start the new week.
        async with self._data_lock:
            data = await load_data()
            data["week_start"] = current_monday()
            data["hosts"] = {}
            await save_data(data)

        for guild in self.bot.guilds:
            await self._refresh_panels(guild)

    @reset_check.before_loop
    async def _before_reset_check(self):
        await self.bot.wait_until_ready()

    async def _post_week_results(
        self, guild: discord.Guild, hosts: Dict[str, Any]
    ) -> None:
        ch = discord.utils.get(guild.text_channels, name=LOG_CHANNEL)
        if not ch:
            try:
                ch = await guild.create_text_channel(LOG_CHANNEL)
            except Exception:
                return

        embed = discord.Embed(
            title="📊 Weekly Directive Activity",
            color=discord.Color.dark_blue(),
            timestamp=datetime.datetime.utcnow(),
        )
        rows: List[Tuple[str, int, bool]] = []
        for uid, rec in hosts.items():
            m = guild.get_member(int(uid))
            if not m or not is_tracked_officer(m):
                continue
            c = int(rec.get("count", 0))
            rows.append((m.display_name, c, is_mandated(m)))

        if not rows:
            embed.description = "No officer events were hosted this week."
        else:
            rows.sort(key=lambda r: r[1], reverse=True)
            lines = []
            for name, c, mandated in rows:
                if mandated:
                    status = "✅" if c >= WEEKLY_QUOTA else "⚠️"
                else:
                    status = "🏅" if c >= AP_PER_EVENTS else "•"
                lines.append(f"{status} {name} — **{c}** event(s)")
            embed.description = "\n".join(lines)
        embed.set_footer(text="A new weekly cycle has begun.")
        try:
            await ch.send(embed=embed)
        except Exception:
            pass

    async def _post_demotion_prompts(
        self, guild: discord.Guild, hosts: Dict[str, Any]
    ) -> None:
        ch = discord.utils.get(guild.text_channels, name=LOG_CHANNEL)
        if not ch:
            return

        tendeuse = discord.utils.get(guild.roles, name=TENDEUSE_ROLE)
        ping = tendeuse.mention if tendeuse else f"@{TENDEUSE_ROLE}"

        for member in guild.members:
            if not is_mandated(member):
                continue
            rec = hosts.get(str(member.id))
            count = int(rec.get("count", 0)) if isinstance(rec, dict) else 0
            if count >= WEEKLY_QUOTA:
                continue

            rank = effective_rank_role(member) or "Officer"
            embed = discord.Embed(
                title="⚠️ Activity Shortfall — Action Required",
                description=(
                    f"{member.mention} (**{rank}**) hosted **{count}/"
                    f"{WEEKLY_QUOTA}** events last week.\n\n"
                    f"**{GENERAL_ROLE} or above:** press **Justify** if this is "
                    "excused, or **Demote** to drop them one rank."
                ),
                color=discord.Color.red(),
                timestamp=datetime.datetime.utcnow(),
            )
            embed.set_footer(text="Only ARC General or above may decide.")
            try:
                msg = await ch.send(
                    content=ping,
                    embed=embed,
                    view=DemotionPromptView(member.id),
                    allowed_mentions=discord.AllowedMentions(roles=True),
                )
            except Exception:
                continue

            async with self._data_lock:
                data = await load_data()
                pend = data.setdefault("pending_demotions", {})
                gpend = pend.setdefault(str(guild.id), {})
                gpend[str(member.id)] = msg.id
                await save_data(data)

    # ------------------------------------------------------------------
    # Demotion prompt button handler
    # ------------------------------------------------------------------

    async def handle_demotion_decision(
        self, interaction: discord.Interaction, uid: int, *, demote: bool
    ) -> None:
        actor = interaction.user
        guild = interaction.guild
        if not guild or not isinstance(actor, discord.Member):
            await interaction.response.send_message(
                "Must be used in a server.", ephemeral=True
            )
            return

        if not (is_general_or_above(actor) or has_role(actor, TENDEUSE_ROLE)):
            await interaction.response.send_message(
                f"❌ Only **{GENERAL_ROLE}** or above may decide this.",
                ephemeral=True,
            )
            return

        target = guild.get_member(int(uid))
        target_name = target.display_name if target else f"<@{uid}>"

        if not demote:
            # Justified — clear the prompt.
            await interaction.response.edit_message(
                content=None,
                embed=discord.Embed(
                    title="✅ Justified",
                    description=(
                        f"{target_name}'s shortfall was **justified** by "
                        f"{actor.mention}. No demotion."
                    ),
                    color=discord.Color.green(),
                    timestamp=datetime.datetime.utcnow(),
                ),
                view=None,
            )
            await self._clear_pending(guild.id, uid)
            await _log(
                guild,
                f"✅ {actor.display_name} justified {target_name}'s activity "
                "shortfall — no demotion.",
            )
            return

        # Demote one rank via the hierarchy cog.
        if not target:
            await interaction.response.edit_message(
                content=None,
                embed=discord.Embed(
                    title="⚠️ Member Not Found",
                    description=f"<@{uid}> is no longer in the server.",
                    color=discord.Color.greyple(),
                ),
                view=None,
            )
            await self._clear_pending(guild.id, uid)
            return

        role_name = effective_rank_role(target)
        rank_key = RANK_KEY_BY_ROLE.get(role_name)
        if not rank_key:
            await interaction.response.send_message(
                f"❌ {target_name} is no longer at a demotable rank.",
                ephemeral=True,
            )
            await self._clear_pending(guild.id, uid)
            return

        try:
            from cogs.arc_hierarchy import (
                apply_rank_change, update_flowchart, log_action, DEMOTE_TO,
            )
        except Exception as e:
            await interaction.response.send_message(
                f"❌ Hierarchy system unavailable: `{e}`", ephemeral=True
            )
            return

        new_rank = DEMOTE_TO.get(rank_key)
        if not new_rank:
            await interaction.response.send_message(
                f"❌ {target_name} cannot be demoted further.", ephemeral=True
            )
            await self._clear_pending(guild.id, uid)
            return

        await interaction.response.defer()
        prev_rank, applied_rank = await apply_rank_change(target, new_rank)

        await interaction.edit_original_response(
            content=None,
            embed=discord.Embed(
                title="⬇️ Demoted",
                description=(
                    f"{target.mention} was demoted **{prev_rank} → {applied_rank}** "
                    f"by {actor.mention} for an activity shortfall."
                ),
                color=discord.Color.dark_red(),
                timestamp=datetime.datetime.utcnow(),
            ),
            view=None,
        )
        await self._clear_pending(guild.id, uid)
        try:
            await log_action(
                guild,
                f"Demotion (directive shortfall): {target.mention} "
                f"**{prev_rank} → {applied_rank}** by {actor.mention}.",
                mention_ids=[actor.id, target.id],
            )
            await update_flowchart(guild)
        except Exception:
            pass

    async def _clear_pending(self, guild_id: int, uid: int) -> None:
        async with self._data_lock:
            data = await load_data()
            gpend = data.get("pending_demotions", {}).get(str(guild_id))
            if isinstance(gpend, dict):
                gpend.pop(str(uid), None)
                await save_data(data)

    # ------------------------------------------------------------------
    # Admin slash commands
    # ------------------------------------------------------------------

    @app_commands.command(
        name="directive_setup",
        description="Re-post or refresh the two Directive panels in #arc-directives.",
    )
    async def directive_setup(self, interaction: discord.Interaction):
        if not isinstance(interaction.user, discord.Member) or not (
            interaction.user.guild_permissions.administrator
            or is_lieutenant_or_above(interaction.user)
        ):
            await interaction.response.send_message(
                "❌ Only admins or ARC Lieutenant+ can use this.", ephemeral=True
            )
            return
        await interaction.response.defer(ephemeral=True)
        await self._ensure_panels(interaction.guild)
        await interaction.followup.send(
            f"✅ Directive panels refreshed in `#{DIRECTIVES_CHANNEL}`.",
            ephemeral=True,
        )

    @app_commands.command(
        name="directive_credit",
        description="Manually credit a hosted event toward an officer's weekly count.",
    )
    @app_commands.describe(
        event_id="Event ID from the embed footer, e.g. EVT-A3F72B (UUID prefix also works).",
    )
    async def directive_credit(
        self, interaction: discord.Interaction, event_id: str
    ):
        if not isinstance(interaction.user, discord.Member) or not (
            interaction.user.guild_permissions.administrator
        ):
            await interaction.response.send_message(
                "❌ Only administrators can manually credit an event.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)

        try:
            from cogs.event_creator import load_events, normalize_event
        except Exception as e:
            await interaction.followup.send(
                f"❌ Could not load events: {e}", ephemeral=True
            )
            return

        lookup = event_id.strip().upper()
        data   = await load_events()
        match_event: Optional[Dict[str, Any]] = None
        for eid, ev in data.items():
            if not isinstance(ev, dict):
                continue
            stored = str(ev.get("short_id", "")).upper()
            if stored == lookup or eid.upper().startswith(lookup):
                match_event = normalize_event(ev)
                break

        if match_event is None:
            await interaction.followup.send(
                f"❌ No event found with ID **{event_id}**.", ephemeral=True
            )
            return

        if not match_event.get("directive_officer_id"):
            await interaction.followup.send(
                f"❌ **{match_event.get('title', 'That event')}** is not a "
                "directive op (no host attached), so there's nothing to credit.",
                ephemeral=True,
            )
            return

        status     = await self.credit_directive_completion(match_event, force=True)
        officer_id = match_event.get("directive_officer_id")
        title      = match_event.get("title", "the op")

        replies = {
            "credited":
                f"✅ Credited **{title}** to <@{officer_id}> toward their weekly "
                "host count.",
            "wrong_cycle":
                f"⚠️ **{title}** belongs to a previous week (the cycle has since "
                "reset), so it can't be credited toward the current week.",
            "no_directive":
                f"❌ **{title}** has no directive host attached.",
        }
        await interaction.followup.send(
            replies.get(status, f"⚠️ Unexpected result: `{status}`"),
            ephemeral=True,
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(DirectiveCog(bot))
