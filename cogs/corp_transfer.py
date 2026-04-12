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
#    Five questions asked one at a time; the message is edited in place between
#    questions so nothing spills into the channel.
#
# 3. On completion  — the full set of answers is logged as an embed to
#    #transfer-application.  The applicant receives a confirmation message.
#
# PERSISTENCE
# -----------
# • The "Apply" button view is persistent (timeout=None) and re-registered on
#   every on_ready so it survives bot restarts.
# • In-progress Yes/No sessions are in-memory only.  If the bot restarts mid-
#   session the member simply clicks Apply again.
#
# SERVER SETUP COMPATIBILITY
# --------------------------
# REQUIRED_CHANNELS is declared at module level so server_setup.py auto-creates
# the two channels if they don't already exist.

from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

import discord
from discord.ext import commands
from discord import app_commands


# ============================================================
# CONFIG
# ============================================================

REQUEST_CHANNEL = "wormhole-transfer-request"   # where the panel lives
LOG_CHANNEL     = "transfer-application"         # where applications are logged

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


# ============================================================
# VIEWS
# ============================================================

class QuestionView(discord.ui.View):
    """
    Ephemeral yes/no view presented for a single question.

    • timeout=300  — session expires after 5 min of inactivity per question.
    • Not persistent — intentional.  If the bot restarts mid-session the member
      clicks Apply again (sessions are cheap to restart).
    • Routes back to the cog via the stored reference so no global state is needed.
    """

    def __init__(self, cog: "CorpTransfer", user_id: int, q_index: int):
        super().__init__(timeout=300)
        self.cog     = cog
        self.user_id = user_id
        self.q_index = q_index

    async def _handle(self, interaction: discord.Interaction, answer: bool) -> None:
        # Ownership guard — only the applicant may answer their own questions
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "⚠️ This application belongs to someone else.", ephemeral=True
            )
            return
        # Stop the view so further button presses are ignored while we process
        self.stop()
        await self.cog._record_answer(interaction, self.q_index, answer)

    @discord.ui.button(
        label="Yes",
        style=discord.ButtonStyle.success,
        custom_id="corp_transfer_q_yes",
    )
    async def yes_btn(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        await self._handle(interaction, True)

    @discord.ui.button(
        label="No",
        style=discord.ButtonStyle.danger,
        custom_id="corp_transfer_q_no",
    )
    async def no_btn(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        await self._handle(interaction, False)

    async def on_timeout(self) -> None:
        """Clean up the stale session so the member can apply again."""
        self.cog._sessions.pop(self.user_id, None)


class ApplyButtonView(discord.ui.View):
    """
    Persistent view containing the single 'Apply' button.
    Registered with the bot on every on_ready so it survives restarts.
    """

    def __init__(self):
        super().__init__(timeout=None)   # persistent — never expires

    @discord.ui.button(
        label="Apply",
        style=discord.ButtonStyle.primary,
        custom_id=PANEL_CUSTOM_ID,
        emoji="📋",
    )
    async def apply_btn(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
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
        # In-memory session store: {user_id: [answer, answer, ...]}
        # Answers are True (Yes) or False (No), appended as questions are answered.
        self._sessions: Dict[int, List[bool]] = {}

    # ----------------------------------------------------------------
    # on_ready — register persistent view
    # ----------------------------------------------------------------

    @commands.Cog.listener()
    async def on_ready(self) -> None:
        self.bot.add_view(ApplyButtonView())
        print("[corp_transfer] Persistent ApplyButtonView registered.")

    # ----------------------------------------------------------------
    # Permission helpers
    # ----------------------------------------------------------------

    def _can_post_panel(self, member: discord.Member) -> bool:
        return _has_any_role(member, PANEL_POSTER_ROLES)

    # ----------------------------------------------------------------
    # Application flow
    # ----------------------------------------------------------------

    async def _start_application(self, interaction: discord.Interaction) -> None:
        """Entry point when a member clicks Apply."""
        user_id = interaction.user.id

        # One active session per user
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
        """
        Show question q_index to the member.

        first=True  → send a new ephemeral message (interaction.response.send_message)
        first=False → edit the existing ephemeral message in place
        """
        total    = len(QUESTIONS)
        question = QUESTIONS[q_index]

        embed = discord.Embed(
            title=       f"Corporation Transfer Application  ({q_index + 1} / {total})",
            description= f"**{question}**",
            color=       discord.Color.blurple(),
        )
        embed.set_footer(text="Answer Yes or No • Session expires after 5 minutes of inactivity")

        view = QuestionView(self, interaction.user.id, q_index)

        if first:
            await interaction.response.send_message(
                embed=embed, view=view, ephemeral=True
            )
        else:
            await interaction.response.edit_message(embed=embed, view=view)

    async def _record_answer(
        self,
        interaction: discord.Interaction,
        q_index:     int,
        answer:      bool,
    ) -> None:
        """Store the answer and advance to the next question or finalize."""
        user_id = interaction.user.id
        answers = self._sessions.get(user_id)

        if answers is None:
            # Session was cleaned up (timeout or restart)
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
            # All questions answered — clean up session and finalize
            self._sessions.pop(user_id, None)
            await self._finalize_application(interaction, answers)

    async def _finalize_application(
        self,
        interaction: discord.Interaction,
        answers:     List[bool],
    ) -> None:
        """
        Post the completed application to #transfer-application and
        confirm receipt to the applicant.
        """
        member  = interaction.user
        guild   = interaction.guild
        all_yes = all(answers)
        color   = discord.Color.green() if all_yes else discord.Color.orange()

        # ── Build the log embed ───────────────────────────────────────────────
        log_embed = discord.Embed(
            title=     "📋 Corporation Transfer Application",
            color=     color,
            timestamp= datetime.now(timezone.utc),
        )
        log_embed.set_author(
            name=     member.display_name,
            icon_url= member.display_avatar.url,
        )
        log_embed.add_field(
            name="Applicant",
            value=member.mention,
            inline=True,
        )
        log_embed.add_field(
            name="Eligibility",
            value="✅ All criteria met" if all_yes else "⚠️ One or more criteria not met",
            inline=True,
        )
        # Spacer to keep the two above fields on the same row
        log_embed.add_field(name="\u200b", value="\u200b", inline=True)

        for i, (question, answer) in enumerate(zip(QUESTIONS, answers), start=1):
            log_embed.add_field(
                name=  f"Q{i}. {question}",
                value= _yn(answer),
                inline=False,
            )

        log_embed.set_footer(text=f"User ID: {member.id}")

        # ── Post to #transfer-application ─────────────────────────────────────
        log_ch = (
            discord.utils.get(guild.text_channels, name=LOG_CHANNEL)
            if guild else None
        )
        if log_ch:
            try:
                await log_ch.send(embed=log_embed)
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

        # ── Confirm to the applicant (edit the ephemeral message in place) ────
        confirm_embed = discord.Embed(
            title= "✅ Application Submitted",
            description= (
                "Your answers have been recorded and submitted to leadership for review.\n\n"
                + (
                    "**All criteria are currently met.** Leadership will be in touch shortly."
                    if all_yes else
                    "**Some criteria are not yet met.** Keep working toward them and "
                    "click Apply again when you're ready."
                )
            ),
            color= color,
        )
        confirm_embed.set_footer(text="ARC Security Corporation")

        # Edit the last question message to show the confirmation
        await interaction.response.edit_message(embed=confirm_embed, view=None)

    # ----------------------------------------------------------------
    # /post_transfer_panel
    # ----------------------------------------------------------------

    @app_commands.command(
        name="post_transfer_panel",
        description="Post the Corporation Transfer application panel in #wormhole-transfer-request.",
    )
    async def post_transfer_panel(self, interaction: discord.Interaction) -> None:
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message(
                "Must be used in a server.", ephemeral=True
            )
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
                f"❌ Channel `#{REQUEST_CHANNEL}` not found in this server.",
                ephemeral=True,
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
                f"❌ I don't have permission to send messages in {ch.mention}.",
                ephemeral=True,
            )
        except Exception as e:
            await interaction.followup.send(
                f"❌ Failed to post panel: `{e}`", ephemeral=True
            )


# ============================================================
# SETUP
# ============================================================

async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(CorpTransfer(bot))
