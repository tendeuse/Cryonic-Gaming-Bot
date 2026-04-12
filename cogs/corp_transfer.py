# cogs/corp_transfer.py
#
# Corporation Transfer Application System
# ========================================
#
# FEATURES
# --------
# 1. /post_transfer_panel  — leadership posts a persistent embed + "Apply" button
#    in #wormhole-transfer-request.
#
# 2. Apply button  — starts an ephemeral Yes/No questionnaire for the member.
#    Five questions asked one at a time; the ephemeral message is edited in place
#    between questions so nothing spills into the channel.
#
# 3. On completion  — the full set of answers is logged as an embed to
#    #transfer-application with three action buttons for leadership:
#      • 📞 Reached Out  — records who reached out and when (repeatable)
#      • ✅ Accepted      — final decision; disables all 3 buttons
#      • ❌ Rejected      — final decision; disables all 3 buttons
#
# PERSISTENCE
# -----------
# • The "Apply" panel button is persistent (timeout=None) — re-registered on
#   every on_ready so it survives bot restarts.
# • Each application log message has its own persistent action view, keyed by
#   message_id, re-registered from /data/transfer_applications.json on on_ready.
# • In-progress Yes/No sessions are in-memory only.  If the bot restarts mid-
#   session the member simply clicks Apply again.
#
# SERVER SETUP COMPATIBILITY
# --------------------------
# REQUIRED_CHANNELS is declared at module level so server_setup.py auto-creates
# the two channels if they don't already exist.

import asyncio
import os
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import discord
from discord.ext import commands
from discord import app_commands


# ============================================================
# CONFIG
# ============================================================

REQUEST_CHANNEL = "wormhole-transfer-request"   # where the panel lives
LOG_CHANNEL     = "transfer-application"         # where applications are logged

APPS_PATH       = "/data/transfer_applications.json"

# Stable custom_id for the persistent Apply button
PANEL_CUSTOM_ID = "corp_transfer_apply"

# Roles that may post / repost the application panel
PANEL_POSTER_ROLES: Set[str] = {
    "ARC Petty Officer",
    "ARC Lieutenant",
    "ARC Commander",
    "ARC General",
    "ARC Security Administration Council",
    "ARC Security Corporation Leader",
}

# Eligibility questions — order matters
QUESTIONS: List[str] = [
    "Have you attended the Scanning classes?",
    "Have you attended the WH Rolling classes?",
    "Have you attended 2 Faction Warfare Fleets?",
    "Have you attended a WH introduction class?",
    "Are you currently skilling into the Caracal Navy Issue Corporation fit ⓒ.SA.CNI?",
]

# Picked up by server_setup.py auto-scanner
REQUIRED_CHANNELS: List[str] = [REQUEST_CHANNEL, LOG_CHANNEL]


# ============================================================
# HELPERS
# ============================================================

def _has_any_role(member: discord.Member, role_names: Set[str]) -> bool:
    return any(r.name in role_names for r in member.roles)


def _yn(value: bool) -> str:
    return "✅ Yes" if value else "❌ No"


def _ts_display(iso_str: str) -> str:
    """Convert a stored ISO timestamp to a Discord full timestamp string."""
    try:
        unix = int(datetime.fromisoformat(iso_str).timestamp())
        return f"<t:{unix}:f>"
    except Exception:
        return iso_str[:16] if iso_str else "?"


# ============================================================
# STORAGE
# ============================================================

_apps_lock: Optional[asyncio.Lock] = None


def _get_apps_lock() -> asyncio.Lock:
    global _apps_lock
    if _apps_lock is None:
        _apps_lock = asyncio.Lock()
    return _apps_lock


def _atomic_write(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


async def _load_apps() -> Dict[str, Any]:
    async with _get_apps_lock():
        if not os.path.exists(APPS_PATH):
            return {}
        try:
            with open(APPS_PATH, "r", encoding="utf-8") as f:
                txt = f.read().strip()
            if not txt:
                return {}
            data = json.loads(txt)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}


async def _save_apps(data: Dict[str, Any]) -> None:
    async with _get_apps_lock():
        _atomic_write(APPS_PATH, data)


# ============================================================
# EMBED BUILDER  (single source of truth)
# ============================================================

def _build_log_embed(app: Dict[str, Any]) -> discord.Embed:
    """
    Build (or rebuild) the full application log embed from stored app data.
    Called both when first posting and whenever an action button is clicked.
    """
    all_yes = bool(app.get("all_yes", False))
    status  = app.get("status", "pending")

    # Color reflects current decision state
    if status == "accepted":
        color = discord.Color.green()
    elif status == "rejected":
        color = discord.Color.red()
    elif all_yes:
        color = discord.Color.green()
    else:
        color = discord.Color.orange()

    # Restore original submission timestamp
    ts_str = app.get("submitted_at", "")
    try:
        ts = datetime.fromisoformat(ts_str) if ts_str else datetime.now(timezone.utc)
    except Exception:
        ts = datetime.now(timezone.utc)

    embed = discord.Embed(
        title=     "📋 Corporation Transfer Application",
        color=     color,
        timestamp= ts,
    )

    avatar_url = app.get("applicant_avatar") or None
    embed.set_author(
        name=     app.get("applicant_name", "Unknown"),
        icon_url= avatar_url,
    )

    # ── Top row: Applicant | Eligibility | Status ─────────────────────────
    applicant_id = app.get("applicant_id")
    embed.add_field(name="Applicant",   value=f"<@{applicant_id}>",                                               inline=True)
    embed.add_field(name="Eligibility", value="✅ All criteria met" if all_yes else "⚠️ One or more criteria not met", inline=True)
    embed.add_field(
        name="Status",
        value={
            "pending":  "🕐 Pending Review",
            "accepted": "✅ Accepted",
            "rejected": "❌ Rejected",
        }.get(status, "🕐 Pending Review"),
        inline=True,
    )

    # ── Q&A fields ────────────────────────────────────────────────────────
    answers = app.get("answers", [])
    for i, (question, answer) in enumerate(zip(QUESTIONS, answers), start=1):
        embed.add_field(
            name=  f"Q{i}. {question}",
            value= _yn(answer),
            inline=False,
        )

    # ── Reached-out log (may have multiple entries from different officers) ─
    reached_out: List[Dict] = app.get("reached_out", [])
    if reached_out:
        lines = [
            f"<@{e['by_id']}> — {_ts_display(e.get('at', ''))}"
            for e in reached_out
            if isinstance(e, dict) and e.get("by_id")
        ]
        if lines:
            embed.add_field(
                name=  "📞 Reached Out",
                value= "\n".join(lines),
                inline=False,
            )

    # ── Final decision ────────────────────────────────────────────────────
    decision: Optional[Dict] = app.get("decision")
    if decision and isinstance(decision, dict):
        by_id  = decision.get("by_id")
        action = decision.get("action", "")
        label  = "✅ Accepted by" if action == "accepted" else "❌ Rejected by"
        embed.add_field(
            name=  label,
            value= f"<@{by_id}> — {_ts_display(decision.get('at', ''))}",
            inline=False,
        )

    embed.set_footer(text=f"User ID: {applicant_id}")
    return embed


# ============================================================
# ACTION BUTTON & VIEW
# ============================================================

class ActionButton(discord.ui.Button):
    """
    One of the three leadership action buttons on an application log message.
    The message_id is baked into the custom_id so the view can be re-registered
    persistently after a bot restart without any additional lookup.

    custom_id format:  corp_ro:{message_id}
                       corp_acc:{message_id}
                       corp_rej:{message_id}
    """

    def __init__(
        self,
        action:     str,
        message_id: int,
        label:      str,
        style:      discord.ButtonStyle,
    ):
        super().__init__(
            label=     label,
            style=     style,
            custom_id= f"corp_{action}:{message_id}",
        )
        self.action     = action
        self.message_id = message_id

    async def callback(self, interaction: discord.Interaction) -> None:
        cog: Optional["CorpTransfer"] = interaction.client.cogs.get("CorpTransfer")  # type: ignore
        if cog is None:
            await interaction.response.send_message(
                "❌ System error — transfer cog not loaded.", ephemeral=True
            )
            return
        await cog._handle_action(interaction, self.action, self.message_id)


class ApplicationActionView(discord.ui.View):
    """
    Persistent view attached to every application log message.
    Uniqueness is guaranteed by message_id embedded in each button's custom_id.

    decided=True  → all buttons are pre-disabled (used when re-registering
                    views for already-decided applications after a restart).
    """

    def __init__(self, message_id: int, decided: bool = False):
        super().__init__(timeout=None)   # persistent
        self.add_item(ActionButton("ro",  message_id, "📞 Reached Out", discord.ButtonStyle.primary))
        self.add_item(ActionButton("acc", message_id, "✅ Accepted",     discord.ButtonStyle.success))
        self.add_item(ActionButton("rej", message_id, "❌ Rejected",     discord.ButtonStyle.danger))

        if decided:
            for item in self.children:
                item.disabled = True


# ============================================================
# QUESTION VIEWS
# ============================================================

class QuestionView(discord.ui.View):
    """
    Ephemeral yes/no view for a single question.  Not persistent — if the bot
    restarts mid-session the member clicks Apply again.
    """

    def __init__(self, cog: "CorpTransfer", user_id: int, q_index: int):
        super().__init__(timeout=300)   # 5-minute inactivity window
        self.cog     = cog
        self.user_id = user_id
        self.q_index = q_index

    async def _handle(self, interaction: discord.Interaction, answer: bool) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "⚠️ This application belongs to someone else.", ephemeral=True
            )
            return
        self.stop()
        await self.cog._record_answer(interaction, self.q_index, answer)

    @discord.ui.button(label="Yes", style=discord.ButtonStyle.success, custom_id="corp_transfer_q_yes")
    async def yes_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle(interaction, True)

    @discord.ui.button(label="No", style=discord.ButtonStyle.danger, custom_id="corp_transfer_q_no")
    async def no_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle(interaction, False)

    async def on_timeout(self) -> None:
        self.cog._sessions.pop(self.user_id, None)


# ============================================================
# APPLY PANEL VIEW
# ============================================================

class ApplyButtonView(discord.ui.View):
    """Persistent view containing the single 'Apply' button."""

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Apply",
        style=discord.ButtonStyle.primary,
        custom_id=PANEL_CUSTOM_ID,
        emoji="📋",
    )
    async def apply_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog: Optional["CorpTransfer"] = interaction.client.cogs.get("CorpTransfer")  # type: ignore
        if cog is None:
            await interaction.response.send_message(
                "❌ System error — transfer cog not loaded. Please contact an officer.",
                ephemeral=True,
            )
            return
        await cog._start_application(interaction)


# ============================================================
# COG
# ============================================================

class CorpTransfer(commands.Cog):

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # In-memory Q&A sessions: {user_id: [answer, ...]}
        self._sessions: Dict[int, List[bool]] = {}

    # ----------------------------------------------------------------
    # on_ready
    # ----------------------------------------------------------------

    @commands.Cog.listener()
    async def on_ready(self) -> None:
        # Register the panel Apply button
        self.bot.add_view(ApplyButtonView())

        # Re-register every stored application's action view
        apps  = await _load_apps()
        count = 0
        for msg_id_str, app in apps.items():
            if not msg_id_str.isdigit():
                continue
            msg_id  = int(msg_id_str)
            decided = app.get("status") in ("accepted", "rejected")
            view    = ApplicationActionView(msg_id, decided=decided)
            try:
                self.bot.add_view(view, message_id=msg_id)
                count += 1
            except Exception as e:
                print(
                    f"[corp_transfer] Could not re-register action view "
                    f"for message {msg_id}: {e}"
                )

        print(
            f"[corp_transfer] Registered ApplyButtonView + "
            f"{count} application action view(s)."
        )

    # ----------------------------------------------------------------
    # Q&A flow
    # ----------------------------------------------------------------

    async def _start_application(self, interaction: discord.Interaction) -> None:
        user_id = interaction.user.id
        if user_id in self._sessions:
            await interaction.response.send_message(
                "⚠️ You already have an application in progress — "
                "please finish answering the current question.",
                ephemeral=True,
            )
            return
        self._sessions[user_id] = []
        await self._ask_question(interaction, q_index=0, first=True)

    async def _ask_question(
        self,
        interaction: discord.Interaction,
        q_index:     int,
        first:       bool = False,
    ) -> None:
        total = len(QUESTIONS)
        embed = discord.Embed(
            title=       f"Corporation Transfer Application  ({q_index + 1} / {total})",
            description= f"**{QUESTIONS[q_index]}**",
            color=       discord.Color.blurple(),
        )
        embed.set_footer(
            text="Answer Yes or No • Session expires after 5 minutes of inactivity"
        )
        view = QuestionView(self, interaction.user.id, q_index)
        if first:
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        else:
            await interaction.response.edit_message(embed=embed, view=view)

    async def _record_answer(
        self,
        interaction: discord.Interaction,
        q_index:     int,
        answer:      bool,
    ) -> None:
        user_id = interaction.user.id
        answers = self._sessions.get(user_id)
        if answers is None:
            await interaction.response.send_message(
                "⚠️ Your session expired. Please click **Apply** again to restart.",
                ephemeral=True,
            )
            return

        answers.append(answer)
        next_index = q_index + 1
        if next_index < len(QUESTIONS):
            await self._ask_question(interaction, next_index, first=False)
        else:
            self._sessions.pop(user_id, None)
            await self._finalize_application(interaction, answers)

    # ----------------------------------------------------------------
    # Finalize — post log embed with action buttons
    # ----------------------------------------------------------------

    async def _finalize_application(
        self,
        interaction: discord.Interaction,
        answers:     List[bool],
    ) -> None:
        member  = interaction.user
        guild   = interaction.guild
        all_yes = all(answers)

        app: Dict[str, Any] = {
            "applicant_id":     member.id,
            "applicant_name":   member.display_name,
            "applicant_avatar": str(member.display_avatar.url),
            "all_yes":          all_yes,
            "answers":          answers,
            "status":           "pending",
            "submitted_at":     datetime.now(timezone.utc).isoformat(),
            "reached_out":      [],
            "decision":         None,
        }

        log_ch = (
            discord.utils.get(guild.text_channels, name=LOG_CHANNEL)
            if guild else None
        )

        if log_ch:
            try:
                # Post with a zero-id placeholder view, then immediately edit
                # to replace with the real view that has the correct message_id
                # baked into each button's custom_id.
                log_msg = await log_ch.send(
                    embed= _build_log_embed(app),
                    view=  ApplicationActionView(0),
                )
                real_view = ApplicationActionView(log_msg.id)
                await log_msg.edit(view=real_view)

                # Register persistently so buttons survive restarts
                try:
                    self.bot.add_view(real_view, message_id=log_msg.id)
                except Exception:
                    pass

                # Persist to disk
                apps = await _load_apps()
                apps[str(log_msg.id)] = app
                await _save_apps(apps)

            except Exception as e:
                print(
                    f"[corp_transfer] Failed to post application log "
                    f"for {member} ({member.id}): {e}"
                )
        else:
            print(
                f"[corp_transfer] WARNING: #{LOG_CHANNEL} not found — "
                f"application from {member} could not be logged."
            )

        # Confirm to the applicant (edit their ephemeral message)
        confirm_embed = discord.Embed(
            title=       "✅ Application Submitted",
            description= (
                "Your answers have been recorded and submitted to leadership for review.\n\n"
                + (
                    "**All criteria are currently met.** Leadership will be in touch shortly."
                    if all_yes else
                    "**Some criteria are not yet met.** Keep working toward them and "
                    "click Apply again when you're ready."
                )
            ),
            color= discord.Color.green() if all_yes else discord.Color.orange(),
        )
        confirm_embed.set_footer(text="ARC Security Corporation")
        await interaction.response.edit_message(embed=confirm_embed, view=None)

    # ----------------------------------------------------------------
    # Action button handler
    # ----------------------------------------------------------------

    async def _handle_action(
        self,
        interaction: discord.Interaction,
        action:      str,
        message_id:  int,
    ) -> None:
        apps = await _load_apps()
        key  = str(message_id)
        app  = apps.get(key)

        if not app or not isinstance(app, dict):
            await interaction.response.send_message(
                "⚠️ Application record not found. It may predate this feature.",
                ephemeral=True,
            )
            return

        # Prevent overwriting a final decision
        if app.get("status") in ("accepted", "rejected") and action in ("acc", "rej"):
            await interaction.response.send_message(
                "⚠️ This application has already been decided and cannot be changed.",
                ephemeral=True,
            )
            return

        actor   = interaction.user
        now_iso = datetime.now(timezone.utc).isoformat()

        if action == "ro":
            # Repeatable — any number of officers can log that they reached out
            app.setdefault("reached_out", []).append({
                "by_id":   actor.id,
                "by_name": actor.display_name,
                "at":      now_iso,
            })

        elif action == "acc":
            app["status"]   = "accepted"
            app["decision"] = {
                "action":  "accepted",
                "by_id":   actor.id,
                "by_name": actor.display_name,
                "at":      now_iso,
            }

        elif action == "rej":
            app["status"]   = "rejected"
            app["decision"] = {
                "action":  "rejected",
                "by_id":   actor.id,
                "by_name": actor.display_name,
                "at":      now_iso,
            }

        # Persist updated state
        apps[key] = app
        await _save_apps(apps)

        # Rebuild embed + view and edit the log message in place
        decided  = app.get("status") in ("accepted", "rejected")
        new_view = ApplicationActionView(message_id, decided=decided)

        await interaction.response.edit_message(
            embed= _build_log_embed(app),
            view=  new_view,
        )

    # ----------------------------------------------------------------
    # /post_transfer_panel
    # ----------------------------------------------------------------

    @app_commands.command(
        name="post_transfer_panel",
        description="Post the Corporation Transfer application panel in #wormhole-transfer-request.",
    )
    async def post_transfer_panel(self, interaction: discord.Interaction) -> None:
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Must be used in a server.", ephemeral=True)
            return

        if not self._can_post_panel(interaction.user):
            await interaction.response.send_message(
                "❌ You are not authorized to post the transfer panel.", ephemeral=True
            )
            return

        guild = interaction.guild
        ch    = (
            discord.utils.get(guild.text_channels, name=REQUEST_CHANNEL)
            if guild else None
        )
        if not ch:
            await interaction.response.send_message(
                f"❌ Channel `#{REQUEST_CHANNEL}` not found in this server.", ephemeral=True
            )
            return

        panel_embed = discord.Embed(
            title=       "🚀 Apply for Corporation Transfer",
            description= (
                "Ready to move from **ARC Subsidized** to **ARC Security**?\n\n"
                "Click the button below to begin your eligibility check. "
                "You will be asked a short series of Yes / No questions privately.\n\n"
                "Your responses will be reviewed by leadership."
            ),
            color= discord.Color.blurple(),
        )
        panel_embed.set_footer(text="ARC Security Corporation")

        await interaction.response.defer(ephemeral=True)
        try:
            await ch.send(embed=panel_embed, view=ApplyButtonView())
            await interaction.followup.send(
                f"✅ Transfer application panel posted in {ch.mention}.", ephemeral=True
            )
        except discord.Forbidden:
            await interaction.followup.send(
                f"❌ I don't have permission to send messages in {ch.mention}.", ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(f"❌ Failed to post panel: `{e}`", ephemeral=True)

    def _can_post_panel(self, member: discord.Member) -> bool:
        return _has_any_role(member, PANEL_POSTER_ROLES)


# ============================================================
# SETUP
# ============================================================

async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(CorpTransfer(bot))
