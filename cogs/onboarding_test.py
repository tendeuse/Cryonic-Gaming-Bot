# cogs/onboarding_test.py
# discord.py 2.x – DM-based, restart-safe New Member Onboarding Test
#
# Members with the "Onboarding" role must pass this test before gaining full access.
# • 20 questions in the bank, 5 randomly selected per attempt
# • PASS = 100% (perfect score) — unlimited retries
# • On PASS: removes the "Onboarding" role
# • Results are logged to arc-hierarchy-log

import logging
import random
import datetime
from dataclasses import dataclass
from typing import Dict, List, Optional

import discord
from discord.ext import commands

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("onboarding_test")

# =====================
# CONFIG
# =====================
TEST_CHANNEL_NAME   = "onboarding"          # Channel where the Start button is posted
LOG_CH              = "arc-hierarchy-log"   # Channel where results are logged

QUESTIONS_PER_TEST  = 5        # Questions drawn per attempt
PASS_PERCENT        = 100      # Perfect score required

ROLE_TO_REMOVE_ON_PASS = "Onboarding"      # Removed when member passes

START_BUTTON_CUSTOM_ID = "onboarding_test:start_dm"   # Globally unique custom_id

START_MESSAGE_TEXT = (
    "**New Member Onboarding Test**\n"
    "Before you begin, make sure you have watched the full onboarding video.\n\n"
    "Click **Start Test (DM)** to receive a private 5-question test in your DMs.\n"
    f"Passing requires **{PASS_PERCENT}% (perfect score)**.\n"
    f"On PASS: the **{ROLE_TO_REMOVE_ON_PASS}** role will be removed, granting you full access.\n"
    "**No retry limit** — you may attempt as many times as needed."
)

# =====================
# HELPERS
# =====================
def _clamp_1_100(s: Optional[str], fallback: str = "Option") -> str:
    s = (s or "").strip()
    if not s:
        s = (fallback or "Option").strip()
    if len(s) > 100:
        s = s[:100]
    return s if s else "Option"


def _safe_value(s: Optional[str], fallback: str = "0") -> str:
    s = (s or "").strip()
    if not s:
        s = (fallback or "0").strip()
    if len(s) > 100:
        s = s[:100]
    return s if s else "0"


def message_has_start_button(msg: discord.Message) -> bool:
    try:
        for row in msg.components or []:
            for child in getattr(row, "children", []):
                if getattr(child, "custom_id", None) == START_BUTTON_CUSTOM_ID:
                    return True
    except Exception:
        return False
    return False


async def _safe_ephemeral_reply(interaction: discord.Interaction, content: str) -> None:
    """Safely reply ephemerally regardless of whether the interaction was already acked."""
    try:
        if interaction.response.is_done():
            await interaction.followup.send(content, ephemeral=True)
        else:
            await interaction.response.send_message(content, ephemeral=True)
    except Exception:
        pass


async def _safe_defer(interaction: discord.Interaction, *, ephemeral: bool = True) -> None:
    """Acknowledge interaction immediately to avoid 10062 Unknown Interaction."""
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=ephemeral)
    except Exception:
        pass


# =====================
# QUESTION DATACLASS
# =====================
@dataclass(frozen=True)
class Question:
    prompt: str
    options: List[str]
    correct_index: int


# =====================
# QUESTION BANK (20 questions — 5 selected at random per test)
# =====================
QUESTION_BANK: List[Question] = [

    # ── Section 1: Space Types & CONCORD ──────────────────────────────────────
    Question(
        "Space Types: What is the correct description of CONCORD's role in highsec?",
        [
            "CONCORD punishes illegal aggression after the fact but does not prevent attacks.",
            "CONCORD prevents all attacks on players in highsec.",
            "CONCORD only activates if you file a report.",
            "CONCORD protects players but only at stargates.",
        ],
        0,
    ),
    Question(
        "Space Types: Why do suicide gankers attack ships in highsec knowing they will be destroyed by CONCORD?",
        [
            "Because the target's cargo or ship value makes it profitable to loot the wreck.",
            "Because CONCORD will not respond in systems below 0.7 security.",
            "Because they receive a reward from the game for destroying ships.",
            "Because highsec aggression does not apply a criminal timer.",
        ],
        0,
    ),
    Question(
        "Space Types: What is true about gate guns in lowsec?",
        [
            "Gate guns only shoot players who initiate aggression near gates or stations.",
            "Gate guns protect all players in lowsec equally.",
            "Gate guns shoot everyone who enters the system.",
            "Gate guns cannot be tanked by any ship.",
        ],
        0,
    ),
    Question(
        "Space Types: Which mechanic is exclusive to nullsec and cannot be used in highsec or standard lowsec?",
        [
            "Warp disruption bubbles.",
            "Criminal timers.",
            "Gate guns.",
            "Suspect flags.",
        ],
        0,
    ),
    Question(
        "Space Types: What makes wormhole space uniquely dangerous compared to all other space types?",
        [
            "There is no Local chat, so enemies cannot be detected without active scanning.",
            "CONCORD response times are slower than in highsec.",
            "Gate guns are present but deal significantly more damage.",
            "Warp bubbles apply to all ships including interceptors.",
        ],
        0,
    ),

    # ── Section 2: Safety Settings & Crimewatch ───────────────────────────────
    Question(
        "Crimewatch: What does a Suspect Timer (yellow flag) mean for your ship?",
        [
            "Any player can freely attack you, but CONCORD will not intervene unless you attack first.",
            "CONCORD will destroy your ship immediately in highsec.",
            "You cannot dock or use stargates.",
            "You are immune to all damage from other players.",
        ],
        0,
    ),
    Question(
        "Crimewatch: Which action in highsec will earn you a Criminal Timer (red flag)?",
        [
            "Attacking a player illegally in highsec.",
            "Stealing from a can in nullsec.",
            "Activating a probe launcher near a station.",
            "Jumping through a gate without scouting.",
        ],
        0,
    ),
    Question(
        "Safety Settings: What does setting your safety to Green prevent?",
        [
            "Both suspect and criminal actions.",
            "Only criminal actions — suspect actions are still allowed.",
            "Nothing; green safety is decorative.",
            "Receiving damage from other players.",
        ],
        0,
    ),
    Question(
        "Safety Settings: Which safety setting allows suspect actions but still blocks criminal actions?",
        [
            "Yellow.",
            "Green.",
            "Red.",
            "Orange.",
        ],
        0,
    ),

    # ── Section 3–4: Communication & Pathfinder ───────────────────────────────
    Question(
        "Communication: What is expected of you when active during corp operations?",
        [
            "Be in voice comms and listen for instructions, even if you are not speaking.",
            "Only log into Discord if you are the fleet commander.",
            "Mute yourself in voice comms to avoid distracting others.",
            "Communication is optional and only used for emergencies.",
        ],
        0,
    ),
    Question(
        "Pathfinder: When planning a route through a wormhole chain, what information should you check in Pathfinder?",
        [
            "Wormhole mass, size, stability, danger status, and route to destination.",
            "Only the system name and number of jumps.",
            "The killboard for the past 24 hours only.",
            "The current mineral prices in Jita.",
        ],
        0,
    ),

    # ── Section 5: Gatecheck ──────────────────────────────────────────────────
    Question(
        "Gatecheck: What does seeing multiple recent ship kills on a route indicate?",
        [
            "An active gate camp — you should assume the threat is still present.",
            "The area is safe because the campers have already moved on.",
            "A fleet operation is in progress and you should join.",
            "The kills are from NPCs and pose no player threat.",
        ],
        0,
    ),

    # ── Section 6: Gate Travel & Cloak ────────────────────────────────────────
    Question(
        "Gate Travel: What should you do with the temporary gate cloak immediately after jumping through a stargate?",
        [
            "Use it to safely check Local, overview, and D-scan before deciding your next move.",
            "Break it immediately to align and warp to your destination.",
            "Use it to send a message in Local chat.",
            "Activate your propulsion module to speed away from the gate.",
        ],
        0,
    ),
    Question(
        "Gate Travel: Which travel rule is mandatory in nullsec?",
        [
            "Never warp gate-to-gate directly.",
            "Always use autopilot to avoid manual errors.",
            "Slowboat between gates to avoid bubble drag.",
            "Only travel in nullsec during downtime.",
        ],
        0,
    ),

    # ── Section 7: Nullsec Bubbles ────────────────────────────────────────────
    Question(
        "Nullsec Bubbles: If you are caught inside a warp disruption bubble close to the gate you just came from, what is the correct action?",
        [
            "Burn back to the gate and jump through.",
            "Immediately warp to a safe spot.",
            "Sit still and wait for help.",
            "Align to the destination gate and activate autopilot.",
        ],
        0,
    ),

    # ── Section 8: Directional Scan ───────────────────────────────────────────
    Question(
        "D-Scan: What does detecting combat probes on D-scan mean?",
        [
            "Someone is actively scanning your exact location — it is an immediate threat.",
            "A friendly fleet is searching for anomalies nearby.",
            "An NPC patrol is in the system.",
            "Your cloak is about to be automatically decloaked.",
        ],
        0,
    ),

    # ── Section 9: Bookmarks ──────────────────────────────────────────────────
    Question(
        "Bookmarks: How is a safe spot correctly created?",
        [
            "By dropping a bookmark mid-warp between two celestial objects.",
            "By right-clicking a planet and selecting 'Mark as Safe'.",
            "By anchoring a mobile depot 150 km from a gate.",
            "By bookmarking your current position while docked at a station.",
        ],
        0,
    ),
    Question(
        "Bookmarks: What is the purpose of an Instadock bookmark?",
        [
            "It places you inside the docking radius of a station for instant docking without exposure.",
            "It warps you 150 km above a station for scouting.",
            "It marks a wormhole entrance for future reference.",
            "It creates a safe spot in the middle of a system.",
        ],
        0,
    ),

    # ── Section 10: Autopilot ─────────────────────────────────────────────────
    Question(
        "Autopilot: Why is autopilot considered dangerous for valuable ships or cargo?",
        [
            "It warps short of gates and forces a slow approach, making you easy to scan, lock, and kill.",
            "It automatically sets your safety to Red.",
            "It broadcasts your route to all players in the system.",
            "It disables your shield and armour repairers while active.",
        ],
        0,
    ),

    # ── Section 11–18: High-Risk Systems, Fittings, Scams, Alts ──────────────
    Question(
        "High-Risk Systems: Which of the following is a well-known highsec system you should NEVER haul through due to persistent gank camps?",
        [
            "Uedama.",
            "Jita.",
            "Dodixie.",
            "Amarr.",
        ],
        0,
    ),
    Question(
        "Survival: If your ship becomes tackled (warp scrambled) during an engagement, what is the correct response?",
        [
            "Overheat your modules and follow fleet instructions immediately — freezing is the leading cause of death.",
            "Safely dock at the nearest station.",
            "Self-destruct to deny the kill.",
            "Immediately log off to prevent the ship from being destroyed.",
        ],
        0,
    ),

]


# =====================
# Answer Select (per question, row 0)
# =====================
class AnswerSelect(discord.ui.Select):
    def __init__(self, q_index: int, q: Question):
        letters = ["A", "B", "C", "D"]
        options: List[discord.SelectOption] = []
        for i in range(4):
            label = _clamp_1_100(f"{letters[i]})", fallback=f"{letters[i]})")
            desc  = _clamp_1_100(q.options[i], fallback="")
            options.append(
                discord.SelectOption(
                    label=label,
                    value=_safe_value(str(i), fallback=str(i)),
                    description=desc if desc else None,
                )
            )

        super().__init__(
            placeholder="Select your answer…",
            min_values=1,
            max_values=1,
            options=options,
            custom_id=f"onboarding_quiz:select:{q_index}",
            row=0,
        )
        self.q_index = q_index

    async def callback(self, interaction: discord.Interaction):
        view: "PagedQuizView" = self.view  # type: ignore
        view.answers[self.q_index] = int(self.values[0])
        await _safe_ephemeral_reply(interaction, f"✅ Answer recorded for Q{self.q_index + 1}.")


# =====================
# Paged Quiz View (sent via DM)
# =====================
class PagedQuizView(discord.ui.View):
    def __init__(self, user_id: int, guild_id: int, questions: List[Question], cog: "OnboardingTestCog"):
        super().__init__(timeout=900)   # 15-minute window to complete
        self.user_id   = user_id
        self.guild_id  = guild_id
        self.questions = questions
        self.cog       = cog

        self.page    = 0
        self.answers: Dict[int, int] = {}

        self.btn_prev   = discord.ui.Button(label="◀ Prev",  style=discord.ButtonStyle.secondary, row=1, custom_id="onboarding_quiz:prev")
        self.btn_next   = discord.ui.Button(label="Next ▶",  style=discord.ButtonStyle.secondary, row=1, custom_id="onboarding_quiz:next")
        self.btn_submit = discord.ui.Button(label="✅ Submit", style=discord.ButtonStyle.success,   row=2, custom_id="onboarding_quiz:submit")

        self.btn_prev.callback   = self._on_prev    # type: ignore
        self.btn_next.callback   = self._on_next    # type: ignore
        self.btn_submit.callback = self._on_submit  # type: ignore

        self._render()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await _safe_ephemeral_reply(interaction, "❌ This test belongs to someone else.")
            return False
        return True

    def _render(self):
        self.clear_items()
        q = self.questions[self.page]
        self.add_item(AnswerSelect(self.page, q))

        self.btn_prev.disabled   = (self.page == 0)
        self.btn_next.disabled   = (self.page >= len(self.questions) - 1)
        # Submit is only available on the last question
        self.btn_submit.disabled = (self.page != len(self.questions) - 1)

        self.add_item(self.btn_prev)
        self.add_item(self.btn_next)
        self.add_item(self.btn_submit)

    def content(self) -> str:
        q      = self.questions[self.page]
        chosen = self.answers.get(self.page)
        letters = ["A", "B", "C", "D"]
        chosen_txt = f"\n\n**Your current answer:** {letters[chosen]}" if chosen is not None else ""
        return (
            "**New Member Onboarding Test**\n"
            f"Question **{self.page + 1} / {len(self.questions)}**  |  "
            f"Passing requires **{PASS_PERCENT}% (all correct)**.\n\n"
            f"**Q{self.page + 1}.** {q.prompt}"
            f"{chosen_txt}"
        )

    async def _safe_edit(self, interaction: discord.Interaction):
        """Edit the quiz message safely in both guild and DM contexts."""
        try:
            if interaction.response.is_done():
                await interaction.followup.edit_message(
                    message_id=interaction.message.id,
                    content=self.content(),
                    view=self,
                )
            else:
                await interaction.response.edit_message(content=self.content(), view=self)
        except Exception as e:
            log.exception("_safe_edit failed: %r", e)
            try:
                if not interaction.response.is_done():
                    await interaction.response.defer(ephemeral=True)
            except Exception:
                pass

    async def _on_prev(self, interaction: discord.Interaction):
        await _safe_defer(interaction, ephemeral=True)
        if self.page > 0:
            self.page -= 1
        self._render()
        await self._safe_edit(interaction)

    async def _on_next(self, interaction: discord.Interaction):
        await _safe_defer(interaction, ephemeral=True)
        if self.page < len(self.questions) - 1:
            self.page += 1
        self._render()
        await self._safe_edit(interaction)

    async def _on_submit(self, interaction: discord.Interaction):
        await _safe_defer(interaction, ephemeral=True)

        correct = sum(
            1 for i, q in enumerate(self.questions)
            if self.answers.get(i, -1) == q.correct_index
        )
        total   = len(self.questions)
        percent = int((correct / total) * 100)
        passed  = (percent == PASS_PERCENT)

        # Disable all components on the quiz message
        for item in self.children:
            item.disabled = True
        try:
            if interaction.response.is_done():
                await interaction.followup.edit_message(message_id=interaction.message.id, view=self)
            else:
                await interaction.response.edit_message(view=self)
        except Exception:
            try:
                await interaction.followup.edit_message(message_id=interaction.message.id, view=self)
            except Exception:
                pass

        role_msg = "No role changes."
        if passed:
            role_msg = await self.cog.remove_onboarding(self.guild_id, self.user_id)

        await self.cog.log_result(self.guild_id, self.user_id, passed, correct, total, percent)

        if passed:
            result_text = (
                f"🎉 **PASS!** You scored **{correct}/{total} ({percent}%)**.\n"
                f"{role_msg}\n\n"
                "Welcome to ARC — fly safe, and don't autopilot your valuables."
            )
        else:
            result_text = (
                f"❌ **FAIL.** You scored **{correct}/{total} ({percent}%)**.\n"
                f"A perfect score (**{PASS_PERCENT}%**) is required.\n"
                "Review the onboarding video and try again — no retry limit."
            )

        await _safe_ephemeral_reply(interaction, result_text)


# =====================
# Start Button View (Persistent — survives bot restarts)
# =====================
class StartTestView(discord.ui.View):
    def __init__(self, cog: "OnboardingTestCog"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Start Test (DM)",
        style=discord.ButtonStyle.primary,
        custom_id=START_BUTTON_CUSTOM_ID,
    )
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await _safe_ephemeral_reply(interaction, "❌ This must be used inside the server.")
            return

        await _safe_defer(interaction, ephemeral=True)

        if len(QUESTION_BANK) < QUESTIONS_PER_TEST:
            await _safe_ephemeral_reply(
                interaction,
                f"⚠️ Not enough questions configured (need {QUESTIONS_PER_TEST}, found {len(QUESTION_BANK)}).",
            )
            return

        questions  = random.sample(QUESTION_BANK, QUESTIONS_PER_TEST)
        quiz_view  = PagedQuizView(interaction.user.id, interaction.guild.id, questions, self.cog)

        try:
            dm = await interaction.user.create_dm()
            await dm.send(quiz_view.content(), view=quiz_view)
        except discord.Forbidden:
            await _safe_ephemeral_reply(
                interaction,
                "❌ I couldn't DM you. Please enable DMs from server members in your privacy settings and try again.",
            )
            return
        except Exception as e:
            await _safe_ephemeral_reply(interaction, f"❌ Failed to start test: {type(e).__name__}: {e}")
            return

        await _safe_ephemeral_reply(interaction, "📨 Test sent to your DMs. Good luck!")


# =====================
# Cog
# =====================
class OnboardingTestCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot        = bot
        self.start_view = StartTestView(self)

    async def cog_load(self):
        # Register the persistent view so the button continues to work after restarts
        self.bot.add_view(self.start_view)

    @commands.Cog.listener()
    async def on_ready(self):
        """Post the Start button in the onboarding-test channel if not already present."""
        for guild in self.bot.guilds:
            channel = discord.utils.get(guild.text_channels, name=TEST_CHANNEL_NAME)
            if not channel:
                continue

            found = False
            try:
                async for msg in channel.history(limit=50):
                    if msg.author == guild.me and message_has_start_button(msg):
                        found = True
                        break
            except (discord.Forbidden, discord.HTTPException):
                found = False

            if not found:
                try:
                    await channel.send(START_MESSAGE_TEXT, view=self.start_view)
                except (discord.Forbidden, discord.HTTPException):
                    pass

    # ─── Role Management ────────────────────────────────────────────────────
    async def remove_onboarding(self, guild_id: int, user_id: int) -> str:
        guild = self.bot.get_guild(guild_id)
        if not guild:
            return "PASS recorded, but the server could not be found."

        member = guild.get_member(user_id)
        if not member:
            return "PASS recorded, but your member record could not be found."

        role = discord.utils.get(guild.roles, name=ROLE_TO_REMOVE_ON_PASS)
        if not role:
            return f"PASS recorded, but the role **{ROLE_TO_REMOVE_ON_PASS}** does not exist on this server."

        if role not in member.roles:
            return f"PASS recorded. You do not currently hold **{ROLE_TO_REMOVE_ON_PASS}** — no change needed."

        try:
            await member.remove_roles(role, reason="Passed Onboarding Test (100%)")
            return f"✅ The **{ROLE_TO_REMOVE_ON_PASS}** role has been removed. Welcome aboard!"
        except discord.Forbidden:
            return (
                f"PASS recorded, but the bot lacks permission to remove **{ROLE_TO_REMOVE_ON_PASS}**. "
                "Please contact an admin."
            )
        except discord.HTTPException:
            return (
                f"PASS recorded, but a Discord API error prevented removing **{ROLE_TO_REMOVE_ON_PASS}**. "
                "Please contact an admin."
            )

    # ─── Logging ────────────────────────────────────────────────────────────
    async def log_result(
        self,
        guild_id: int,
        user_id: int,
        passed: bool,
        correct: int,
        total: int,
        percent: int,
    ):
        guild = self.bot.get_guild(guild_id)
        if not guild:
            return

        channel = discord.utils.get(guild.text_channels, name=LOG_CH)
        if not channel:
            try:
                channel = await guild.create_text_channel(LOG_CH)
            except (discord.Forbidden, discord.HTTPException):
                return

        member = guild.get_member(user_id)
        who    = member.mention if member else f"<@{user_id}>"
        status = "✅ PASS" if passed else "❌ FAIL"
        ts     = int(datetime.datetime.now(datetime.timezone.utc).timestamp())

        try:
            await channel.send(
                f"**Onboarding Test {status}** — {who} | "
                f"Score: **{correct}/{total} ({percent}%)** | <t:{ts}:f>"
            )
        except (discord.Forbidden, discord.HTTPException):
            return


async def setup(bot: commands.Bot):
    await bot.add_cog(OnboardingTestCog(bot))
