# cogs/directive.py
#
# ARC Weekly Directives — Officer Focus Program
# =============================================
# Replaces the old free-form task board. Each week the panel in #arc-directives
# shows THREE standing "directives" (focus tracks):
#
#   ⚔️ Combat Focus (PVP & FW)
#   💰 PVE Focus
#   🎓 Training Focus
#
# - ARC Lieutenant and ARC Commander SIGN UP for exactly ONE focus per week.
# - Each focus has a goal of "complete 3 ops per week".
# - Officers create events directly from the panel. Those events are:
#       • restricted so only ARC Security can participate, and
#       • forced to use one of the focus's approved, standardized titles
#         (so progress can be tracked).
# - Progress ticks up by one when an approved-title event is FINALIZED/CLOSED
#   (the Event cog calls credit_directive_completion() on finalize).
# - The cycle resets 7 days after it started; results are posted to
#   #directives-logs, then signups/progress clear and a new cycle begins.
# - Officers may change focus mid-week — their existing progress carries over.

import os
import json
import asyncio
import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import discord
from discord.ext import commands, tasks
from discord import app_commands

# =====================
# CHANNELS
# =====================
DIRECTIVES_CHANNEL = "arc-directives"
LOG_CHANNEL        = "directives-logs"

# =====================
# PROGRAM CONFIG
# =====================
GOAL          = 3              # ops to complete per week
CYCLE_DAYS    = 7              # cycle length
RESTRICT_ROLE = "ARC Security"  # only this role may RSVP to directive events
MIN_ATTENDEES = 2              # an op counts only with creator + 1 other (≥15 min)
GOAL_BONUS_AP = 200            # AP awarded once when an officer hits the weekly goal

# Only these ranks may sign up for a focus / create directive events.
ELIGIBLE_ROLES = {"ARC Lieutenant", "ARC Commander"}

# Focus tracks. The `titles` are the ONLY approved event titles for that focus —
# they must match exactly, since weekly progress is tracked by these titles.
FOCUSES: Dict[str, Dict[str, Any]] = {
    "combat": {
        "label": "⚔️ Combat Focus (PVP & FW)",
        "blurb": "Pick this if your week is about fights and the warzone.",
        "color": discord.Color.red(),
        "requirements": [
            "Lead a small gang roam (≤10 pilots)",
            "Join a corp/alliance PVP fleet ping",
            "Run a plexing op (solo, duo, or small gang)",
            "Defend or contest a system under militia call",
            "Solo/duo a kill in contested or enemy space",
            "Flip a Medium or Large plex",
            "Scout or call intel that leads to a confirmed kill or warzone shift",
        ],
        "titles": ["Faction Warfare Roam", "PVP Hunt"],
    },
    "pve": {
        "label": "💰 PVE Focus",
        "blurb": "Pick this if your week is about funding.",
        "color": discord.Color.gold(),
        "requirements": [
            "Run a PVE op (ratting, missions, or FW sites)",
            "Mentor a newer member through a PVE fit/run",
            "Run an escalation or higher-value site",
            "Generate and report a notable ISK/LP contribution",
        ],
        "titles": ["Mining/PVE Fleet"],
    },
    "training": {
        "label": "🎓 Training Focus",
        "blurb": "Pick this if your week is about teaching.",
        "color": discord.Color.blue(),
        "requirements": [
            "Teach a class from the existing syllabus, delivered as written "
            "(no improvising on content)",
            "Run a syllabus-based practical session (fitting workshop, fleet drill, etc.)",
            "Provide 1-on-1 syllabus-based mentorship to a new/junior member",
        ],
        "titles": [
            "Dscan and Threat Assessment",
            "Scanning Class",
            "Rolling Class",
            "OpSec 101 Class",
        ],
    },
}

# =====================
# PERSISTENCE
# =====================
PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)
DATA_FILE = PERSIST_ROOT / "directives.json"

_file_lock = asyncio.Lock()


def _atomic_write(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=4), encoding="utf-8")
    tmp.replace(path)


def _empty_data() -> Dict[str, Any]:
    return {"cycle": {"start": utcnow_iso(), "officers": {}}, "panels": {}}


async def load_data() -> Dict[str, Any]:
    async with _file_lock:
        if not DATA_FILE.exists():
            return _empty_data()
        try:
            txt = DATA_FILE.read_text(encoding="utf-8").strip()
            if not txt:
                return _empty_data()
            data = json.loads(txt)
            if not isinstance(data, dict):
                return _empty_data()
            data.setdefault("cycle", {"start": utcnow_iso(), "officers": {}})
            data.setdefault("panels", {})
            data["cycle"].setdefault("officers", {})
            return data
        except Exception:
            return _empty_data()


async def save_data(data: Dict[str, Any]) -> None:
    async with _file_lock:
        _atomic_write(DATA_FILE, data)


def utcnow_iso() -> str:
    return datetime.datetime.utcnow().isoformat()


# =====================
# CYCLE HELPERS
# =====================

def _ensure_cycle(data: Dict[str, Any]) -> bool:
    """Make sure a cycle exists. Returns True if it had to be (re)created."""
    cycle = data.get("cycle")
    if not isinstance(cycle, dict) or "start" not in cycle:
        data["cycle"] = {"start": utcnow_iso(), "officers": {}}
        return True
    cycle.setdefault("officers", {})
    return False


def _cycle_start_dt(data: Dict[str, Any]) -> datetime.datetime:
    raw = data.get("cycle", {}).get("start") or utcnow_iso()
    try:
        dt = datetime.datetime.fromisoformat(raw)
    except Exception:
        dt = datetime.datetime.utcnow()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt


def _cycle_end_dt(data: Dict[str, Any]) -> datetime.datetime:
    return _cycle_start_dt(data) + datetime.timedelta(days=CYCLE_DAYS)


def _cycle_expired(data: Dict[str, Any]) -> bool:
    now = datetime.datetime.now(datetime.timezone.utc)
    return now >= _cycle_end_dt(data)


def has_eligible_role(member: discord.Member) -> bool:
    return any(r.name in ELIGIBLE_ROLES for r in member.roles)


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
# EMBED BUILDER
# =====================

def _progress_bar(completions: int) -> str:
    done = min(completions, GOAL)
    return "✅" * done + "⬜" * max(0, GOAL - done)


def build_focus_embed(
    focus_key: str,
    data: Dict[str, Any],
    guild: Optional[discord.Guild] = None,
) -> discord.Embed:
    focus = FOCUSES[focus_key]

    bullets = "\n".join(f"• {r}" for r in focus["requirements"])
    embed = discord.Embed(
        title=focus["label"],
        description=(
            f"_{focus['blurb']}_\n\n"
            f"**Complete {GOAL} of the following per week:**\n{bullets}\n\n"
            f"_An op only counts when the creator + at least 1 other member "
            f"attend (≥15 min). Reach {GOAL}/{GOAL} to earn a "
            f"**{GOAL_BONUS_AP} AP** bonus._"
        ),
        color=focus["color"],
        timestamp=datetime.datetime.utcnow(),
    )

    embed.add_field(
        name="✅ Approved Event Titles",
        value="\n".join(f"• `{t}`" for t in focus["titles"]),
        inline=False,
    )

    officers = data.get("cycle", {}).get("officers", {})
    signed: List[Tuple[str, Dict[str, Any]]] = [
        (uid, off) for uid, off in officers.items()
        if off.get("focus") == focus_key
    ]

    if signed:
        lines = []
        for uid, off in sorted(
            signed, key=lambda kv: int(kv[1].get("completions", 0)), reverse=True
        ):
            m = guild.get_member(int(uid)) if guild else None
            name = m.display_name if m else f"<@{uid}>"
            c = int(off.get("completions", 0))
            trophy = " 🏆" if c >= GOAL else ""
            lines.append(f"• {name} — **{c}/{GOAL}** {_progress_bar(c)}{trophy}")
        value = "\n".join(lines)
    else:
        value = "_(no officers signed up yet)_"

    embed.add_field(
        name=f"🎖 Signed-up Officers ({len(signed)})",
        value=value,
        inline=False,
    )

    end_ts = int(_cycle_end_dt(data).timestamp())
    embed.add_field(name="🔄 Cycle Ends", value=f"<t:{end_ts}:R>", inline=False)
    embed.set_footer(
        text="Sign up for ONE focus per week • ARC Lieutenant & ARC Commander"
    )
    return embed


# =====================
# EVENT-CREATION FLOW (title select → modal → Event cog)
# =====================

class DirectiveEventModal(discord.ui.Modal):
    """
    Collects the remaining event details after a title has been picked.
    Title is preset (from the select) and audience is forced to ARC-Security-only.
    """

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

    def __init__(self, focus_key: str, title_value: str):
        super().__init__(title=f"Create Op — {title_value[:40]}")
        self.focus_key   = focus_key
        self.title_value = title_value

    async def on_submit(self, interaction: discord.Interaction):
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

        data        = await load_data()
        cycle_start = data.get("cycle", {}).get("start")

        partial = {
            "creator_id":   interaction.user.id,
            "target":       "security_only",
            "event_name":   self.title_value,
            "description":  self.description.value.strip(),
            "timestamp":    int(dt.timestamp()),
            "redirect_url": "",
            "button_count": len(names),
            "link_only":    False,
        }
        extra = {
            "directive_officer_id":  interaction.user.id,
            "directive_focus":       self.focus_key,
            "directive_cycle_start": cycle_start,
            "restrict_role":         RESTRICT_ROLE,
        }

        # Reuse the Event cog's full posting pipeline (channel/permission checks,
        # persistence, persistent-view registration, confirmation followup).
        try:
            from cogs.event_creator import _do_post_event
        except Exception as e:
            await interaction.followup.send(
                f"❌ Event system unavailable: `{e}`", ephemeral=True
            )
            return

        await _do_post_event(interaction, partial, names, extra=extra)


class TitleSelect(discord.ui.Select):
    def __init__(self, focus_key: str):
        self.focus_key = focus_key
        options = [
            discord.SelectOption(label=t, value=t)
            for t in FOCUSES[focus_key]["titles"]
        ]
        super().__init__(
            placeholder="Choose the approved op title…",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        title = self.values[0]
        await interaction.response.send_modal(
            DirectiveEventModal(self.focus_key, title)
        )


class TitleSelectView(discord.ui.View):
    def __init__(self, focus_key: str):
        super().__init__(timeout=300)
        self.add_item(TitleSelect(focus_key))


# =====================
# PANEL BUTTONS / VIEW
# =====================

class SignupButton(discord.ui.Button):
    def __init__(self, focus_key: str):
        super().__init__(
            label="✅ Sign Up",
            style=discord.ButtonStyle.success,
            custom_id=f"directive:signup:{focus_key}",
        )
        self.focus_key = focus_key

    async def callback(self, interaction: discord.Interaction):
        cog = interaction.client.cogs.get("DirectiveCog")
        if cog:
            await cog.handle_signup(interaction, self.focus_key)


class CreateEventButton(discord.ui.Button):
    def __init__(self, focus_key: str):
        super().__init__(
            label="⚔️ Create Event",
            style=discord.ButtonStyle.primary,
            custom_id=f"directive:create:{focus_key}",
        )
        self.focus_key = focus_key

    async def callback(self, interaction: discord.Interaction):
        cog = interaction.client.cogs.get("DirectiveCog")
        if cog:
            await cog.handle_create_event(interaction, self.focus_key)


class FocusPanelView(discord.ui.View):
    """Permanent per-focus panel: Sign Up + Create Event."""

    def __init__(self, focus_key: str):
        super().__init__(timeout=None)
        self.add_item(SignupButton(focus_key))
        self.add_item(CreateEventButton(focus_key))


# =====================
# COG
# =====================

class DirectiveCog(commands.Cog):
    """ARC Weekly Directives — three focus tracks with weekly reset."""

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
        # Register the three persistent focus views (survive restarts).
        for fk in FOCUSES:
            self.bot.add_view(FocusPanelView(fk))

        async with self._data_lock:
            data = await load_data()
            if _ensure_cycle(data):
                await save_data(data)

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
        """Post or refresh the three focus panels in #arc-directives."""
        ch = await self._get_directives_channel(guild)
        if not ch:
            return

        async with self._data_lock:
            data = await load_data()
            _ensure_cycle(data)
            panels = data.setdefault("panels", {})
            gp = panels.get(str(guild.id))
            if not isinstance(gp, dict):   # migrate stale/old-format value
                gp = {}
                panels[str(guild.id)] = gp
            changed = False

            for fk in FOCUSES:
                embed = build_focus_embed(fk, data, guild)
                view  = FocusPanelView(fk)
                msg_id = gp.get(fk)

                if msg_id:
                    try:
                        existing = await ch.fetch_message(int(msg_id))
                        await existing.edit(embed=embed, view=view)
                        continue
                    except Exception:
                        pass  # message gone — fall through and repost

                try:
                    msg = await ch.send(embed=embed, view=view)
                    gp[fk] = msg.id
                    changed = True
                except Exception:
                    pass

            if changed:
                await save_data(data)

    async def _refresh_panels(
        self, guild: discord.Guild, data: Optional[Dict[str, Any]] = None
    ) -> None:
        """Edit all three existing focus panel messages with fresh embeds."""
        if data is None:
            data = await load_data()
        ch = discord.utils.get(guild.text_channels, name=DIRECTIVES_CHANNEL)
        if not ch:
            return
        gp = data.get("panels", {}).get(str(guild.id), {})
        if not isinstance(gp, dict):
            return
        for fk in FOCUSES:
            msg_id = gp.get(fk)
            if not msg_id:
                continue
            try:
                msg = await ch.fetch_message(int(msg_id))
                await msg.edit(
                    embed=build_focus_embed(fk, data, guild),
                    view=FocusPanelView(fk),
                )
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Button handlers
    # ------------------------------------------------------------------

    async def handle_signup(
        self, interaction: discord.Interaction, focus_key: str
    ) -> None:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message(
                "Must be used in a server.", ephemeral=True
            )
            return

        member = interaction.user
        if not has_eligible_role(member):
            await interaction.response.send_message(
                "❌ Only **ARC Lieutenant** and **ARC Commander** can sign up "
                "for a weekly focus.",
                ephemeral=True,
            )
            return

        focus = FOCUSES[focus_key]

        async with self._data_lock:
            data = await load_data()
            _ensure_cycle(data)
            officers = data["cycle"]["officers"]
            uid = str(member.id)
            existing = officers.get(uid)

            if existing and existing.get("focus") == focus_key:
                await interaction.response.send_message(
                    f"You are already signed up for **{focus['label']}** "
                    f"(**{int(existing.get('completions', 0))}/{GOAL}**).",
                    ephemeral=True,
                )
                return

            if existing:
                # Switch focus — progress carries over.
                old_focus = existing.get("focus")
                existing["focus"] = focus_key
                carried = int(existing.get("completions", 0))
                reply = (
                    f"🔁 Switched to **{focus['label']}**.\n"
                    f"Your progress carried over: **{carried}/{GOAL}**."
                )
                log_line = (
                    f"🔁 **{member.display_name}** switched focus "
                    f"({FOCUSES.get(old_focus, {}).get('label', old_focus)} → "
                    f"{focus['label']}) — carried **{carried}/{GOAL}**"
                )
            else:
                officers[uid] = {"focus": focus_key, "completions": 0, "events": []}
                reply = (
                    f"✅ Signed up for **{focus['label']}**!\n"
                    f"Goal: complete **{GOAL}** approved ops this week."
                )
                log_line = (
                    f"✅ **{member.display_name}** signed up for {focus['label']}"
                )

            await save_data(data)

        await interaction.response.send_message(reply, ephemeral=True)
        await self._refresh_panels(interaction.guild, data)
        await _log(interaction.guild, log_line)

    async def handle_create_event(
        self, interaction: discord.Interaction, focus_key: str
    ) -> None:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message(
                "Must be used in a server.", ephemeral=True
            )
            return

        focus = FOCUSES[focus_key]
        data = await load_data()
        off = data.get("cycle", {}).get("officers", {}).get(str(interaction.user.id))

        if not off or off.get("focus") != focus_key:
            await interaction.response.send_message(
                f"❌ You must **Sign Up** for {focus['label']} before creating "
                "its events.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            f"Select the approved title for your **{focus['label']}** op:",
            view=TitleSelectView(focus_key),
            ephemeral=True,
        )

    # ------------------------------------------------------------------
    # Called by the Event cog when a directive-tagged event is finalized
    # ------------------------------------------------------------------

    async def credit_directive_completion(self, event: Dict[str, Any]) -> None:
        officer_id = event.get("directive_officer_id")
        if not officer_id:
            return

        guild_id = event.get("guild_id")
        guild = self.bot.get_guild(int(guild_id)) if guild_id else None

        # ── Minimum attendance: creator + at least 1 other (≥15 min in VC) ────
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
                    f"⚠️ **{name}**'s op **{event.get('title')}** did not meet the "
                    f"minimum (creator + 1 member, ≥15 min) — **not counted** "
                    f"({len(qualified)} qualified).",
                )
            return

        bonus_now = False
        async with self._data_lock:
            data = await load_data()
            _ensure_cycle(data)

            # Ignore stragglers finalized after a reset started a new cycle.
            if event.get("directive_cycle_start") != data["cycle"]["start"]:
                return

            off = data["cycle"]["officers"].get(str(officer_id))
            if not off:
                return  # officer withdrew / was cleared

            off["completions"] = int(off.get("completions", 0)) + 1
            off.setdefault("events", []).append({
                "title":        event.get("title"),
                "event_id":     event.get("short_id") or event.get("message"),
                "finalized_at": utcnow_iso(),
            })
            completions = off["completions"]

            # Weekly-goal bonus — awarded exactly once per officer per cycle.
            if completions >= GOAL and not off.get("goal_bonus_awarded"):
                off["goal_bonus_awarded"] = True
                bonus_now = True

            await save_data(data)

        if guild:
            await self._refresh_panels(guild, data)
            m = guild.get_member(int(officer_id))
            name = m.display_name if m else f"<@{officer_id}>"
            done = " 🏆 **Goal met!**" if completions >= GOAL else ""
            await _log(
                guild,
                f"🏅 **{name}** finalized **{event.get('title')}** — "
                f"**{completions}/{GOAL}** this week{done}",
            )

            if bonus_now:
                awarded = await self._award_goal_bonus(guild, m) if m else False
                if awarded:
                    await _log(
                        guild,
                        f"💰 **{name}** earned the **{GOAL_BONUS_AP} AP** weekly "
                        f"directive goal bonus!",
                    )

    async def _award_goal_bonus(
        self, guild: discord.Guild, member: discord.Member
    ) -> bool:
        """Award the flat weekly-goal AP bonus via the AP system."""
        try:
            from cogs.ap_tracking import award_ap_with_bonuses
        except Exception as e:
            print(f"[directive] goal bonus import failed: {e}")
            return False
        try:
            await award_ap_with_bonuses(
                guild=guild,
                earner=member,
                base_amount=float(GOAL_BONUS_AP),
                source="weekly directive goal",
                reason=f"Completed the weekly directive goal ({GOAL}/{GOAL})",
                log=True,
                actor=None,
                distribution_embed=True,
                distribution_title="Weekly Directive Goal Bonus",
            )
            return True
        except Exception as e:
            print(f"[directive] goal bonus award failed: {e}")
            return False

    # ------------------------------------------------------------------
    # Weekly reset loop
    # ------------------------------------------------------------------

    @tasks.loop(minutes=5)
    async def reset_check(self):
        async with self._data_lock:
            data = await load_data()
            if _ensure_cycle(data):
                await save_data(data)
            if not _cycle_expired(data):
                return
            # Snapshot officers before clearing.
            officers_snapshot = dict(data["cycle"]["officers"])

        # Post results to every guild's log channel.
        for guild in self.bot.guilds:
            await self._post_cycle_results(guild, officers_snapshot)

        # Start a fresh cycle.
        async with self._data_lock:
            data = await load_data()
            data["cycle"] = {"start": utcnow_iso(), "officers": {}}
            await save_data(data)

        for guild in self.bot.guilds:
            await self._refresh_panels(guild)

    @reset_check.before_loop
    async def _before_reset_check(self):
        await self.bot.wait_until_ready()

    async def _post_cycle_results(
        self, guild: discord.Guild, officers: Dict[str, Any]
    ) -> None:
        ch = discord.utils.get(guild.text_channels, name=LOG_CHANNEL)
        if not ch:
            try:
                ch = await guild.create_text_channel(LOG_CHANNEL)
            except Exception:
                return

        embed = discord.Embed(
            title="📊 Weekly Directive Results",
            color=discord.Color.dark_blue(),
            timestamp=datetime.datetime.utcnow(),
        )

        if not officers:
            embed.description = "No officers signed up this cycle."
        else:
            met = sum(
                1 for off in officers.values()
                if int(off.get("completions", 0)) >= GOAL
            )
            embed.description = (
                f"**{len(officers)}** officer(s) participated — "
                f"**{met}** met the {GOAL}-op goal. 🎯"
            )
            for fk, focus in FOCUSES.items():
                members = [
                    (uid, off) for uid, off in officers.items()
                    if off.get("focus") == fk
                ]
                if not members:
                    continue
                lines = []
                for uid, off in sorted(
                    members, key=lambda kv: int(kv[1].get("completions", 0)),
                    reverse=True,
                ):
                    m = guild.get_member(int(uid))
                    name = m.display_name if m else f"<@{uid}>"
                    c = int(off.get("completions", 0))
                    status = "✅ Met goal" if c >= GOAL else "❌ Missed"
                    lines.append(f"• {name} — **{c}/{GOAL}** — {status}")
                embed.add_field(
                    name=focus["label"], value="\n".join(lines), inline=False
                )

        embed.set_footer(text="A new weekly cycle has begun.")
        try:
            await ch.send(embed=embed)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Admin slash command — repost / refresh the panels
    # ------------------------------------------------------------------

    @app_commands.command(
        name="directive_setup",
        description="Re-post or refresh the three weekly Directive focus panels.",
    )
    async def directive_setup(self, interaction: discord.Interaction):
        if not isinstance(interaction.user, discord.Member) or not (
            interaction.user.guild_permissions.administrator
            or has_eligible_role(interaction.user)
        ):
            await interaction.response.send_message(
                "❌ Only admins, Lieutenants, and Commanders can use this.",
                ephemeral=True,
            )
            return
        await interaction.response.defer(ephemeral=True)
        await self._ensure_panels(interaction.guild)
        await interaction.followup.send(
            f"✅ Directive focus panels refreshed in `#{DIRECTIVES_CHANNEL}`.",
            ephemeral=True,
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(DirectiveCog(bot))
