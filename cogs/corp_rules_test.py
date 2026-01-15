# cogs/corp_rules_test.py
# discord.py 2.x - DM-based, restart-safe Corp Rules Test (signature-style)
# PASS = 100%, Unlimited retries, 5 random questions per attempt
# On PASS: removes "Newbro" role (does NOT grant any role)

import logging
import random
import datetime
from dataclasses import dataclass
from typing import Dict, List, Optional

import discord
from discord.ext import commands

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("corp_rules_test")

# =====================
# CONFIG
# =====================
TEST_CHANNEL_NAME = "corp-rules-test"
LOG_CH = "arc-hierarchy-log"

QUESTIONS_PER_TEST = 5      # EXACTLY 5 questions per run
PASS_PERCENT = 100          # perfect score required

# Role handling on pass
ROLE_TO_REMOVE_ON_PASS = "Newbro"

START_BUTTON_CUSTOM_ID = "corp_rules_test:start_dm"

START_MESSAGE_TEXT = (
    "**Corp Rules Test**\n"
    "Click **Start Test (DM)** to receive a private test in your DMs.\n"
    f"Passing requires **{PASS_PERCENT}% (perfect score)**.\n"
    f"On PASS: removes the **{ROLE_TO_REMOVE_ON_PASS}** role.\n"
    "No retry limit."
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
    """
    Safely reply ephemerally whether the interaction has already been acknowledged or not.
    Prevents 'Unknown interaction' and double-respond issues.
    """
    try:
        if interaction.response.is_done():
            await interaction.followup.send(content, ephemeral=True)
        else:
            await interaction.response.send_message(content, ephemeral=True)
    except Exception:
        pass


async def _safe_defer(interaction: discord.Interaction, *, ephemeral: bool = True) -> None:
    """
    Acknowledge interaction immediately to avoid 10062 Unknown interaction.
    """
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=ephemeral)
    except Exception:
        pass


# =====================
# QUESTION BANK (from your rules)
# =====================
@dataclass(frozen=True)
class Question:
    prompt: str
    options: List[str]
    correct_index: int


QUESTION_BANK: List[Question] = [
    # =========================
    # Ship Identification
    # =========================
    Question(
        "Ship Identification: What symbol must be included in your ship’s name?",
        ["ⓒ", "★", "ARC", "#"],
        0,
    ),
    Question(
        "Ship Identification: Failure to include the ⓒ symbol results in what consequence?",
        [
            "Loss of buyback access.",
            "Being considered a free target for other corporation members.",
            "Immediate kick from the corporation.",
            "No access to wormholes.",
        ],
        1,
    ),

    # =========================
    # Mandatory Ship Progression & Fits
    # =========================
    Question(
        "Mandatory Ships: What are members required to do regarding the mandatory corporation ships?",
        [
            "Only train the skills; owning the ships is optional.",
            "Skill into, own, and maintain the mandatory corporation ships listed for WH operations.",
            "Borrow ships from directors as needed.",
            "Only maintain ships if you run buyback contracts.",
        ],
        1,
    ),
    Question(
        "Fits: Where can official fits be found?",
        [
            "Corporation Shared Fittings section.",
            "Pinned messages in local chat.",
            "Only via private DM from leadership.",
            "On the alliance forum.",
        ],
        0,
    ),
    Question(
        "Required Ships: Which ship is listed as a required Frigate?",
        ["ⓒ.BI.HERON", "ⓒ.FO.RAVEN.PATROL", "ⓒ.TD.OSPREY.BASIC", "ⓒ.FO.DRAKE.D-A1.0"],
        0,
    ),
    Question(
        "Required Ships: Which ship is listed as a required Frigate (gas)?",
        ["ⓒ.BI.VENTURE.GAS", "ⓒ.TD.OSPREY.BASIC", "ⓒ.FO.DRAKE.D-A1.0", "ⓒ.FO.RAVEN.PATROL"],
        0,
    ),
    Question(
        "Required Ships: Which ship is listed as a required Cruiser?",
        ["ⓒ.TD.OSPREY.BASIC", "ⓒ.BI.HERON", "ⓒ.FO.RAVEN.PATROL", "ⓒ.BI.VENTURE.GAS"],
        0,
    ),
    Question(
        "Required Ships: Which ship is listed as a required Battlecruiser?",
        ["ⓒ.FO.DRAKE.D-A1.0", "ⓒ.FO.RAVEN.PATROL", "ⓒ.TD.OSPREY.BASIC", "ⓒ.BI.HERON"],
        0,
    ),
    Question(
        "Required Ships: Which ship is listed as a required Battleship / Roller?",
        ["ⓒ.FO.RAVEN.PATROL", "ⓒ.FO.DRAKE.D-A1.0", "ⓒ.TD.OSPREY.BASIC", "ⓒ.BI.VENTURE.GAS"],
        0,
    ),

    # =========================
    # Local Chat Conduct
    # =========================
    Question(
        "Local Chat Conduct: When should you engage in local chat?",
        [
            "Only to display good sportsmanship.",
            "Whenever you want to provoke enemies.",
            "To argue about doctrine fits.",
            "To negotiate buyback rates.",
        ],
        0,
    ),
    Question(
        "Local Chat Conduct: Which behavior is expected?",
        [
            "Avoid toxic behavior, arguments, or provocation.",
            "Use local chat to bait fights and trash talk.",
            "Spam local to draw attention away from allies.",
            "Only leadership may speak in local, ever.",
        ],
        0,
    ),

    # =========================
    # Conduct & Behavior
    # =========================
    Question(
        "Conduct & Behavior: What is the core conduct rule?",
        [
            "Don’t be an ass; treat members and allies with respect.",
            "Only directors must follow conduct rules.",
            "Respect is optional if you are in a fleet.",
            "It only applies in wormholes, not elsewhere.",
        ],
        0,
    ),

    # =========================
    # Ideology and Personal Beliefs
    # =========================
    Question(
        "Ideology and Personal Beliefs: What must be left outside the game?",
        [
            "Personal ideologies and real-world politics.",
            "Ship naming standards.",
            "Buyback contracts.",
            "Wormhole mapping.",
        ],
        0,
    ),

    # =========================
    # Buyback Program
    # =========================
    Question(
        "Buyback: All buyback contracts should be made to who?",
        ["ARC Tendeuse A", "Any director", "The corporation CEO only", "Spanish Corp"],
        0,
    ),
    Question(
        "Buyback: What is the buyback rate?",
        ["80% of Jita Buy price.", "100% of Jita Sell price.", "60% of Jita Buy price.", "90% of Jita Sell price."],
        0,
    ),
    Question(
        "Buyback: Which item is NOT accepted?",
        ["Reprocessed materials", "Blue Loot", "Gas", "Relic Site Loot"],
        0,
    ),
    Question(
        "Buyback: What must you include in contract notes?",
        ["Your Discord name", "Your real name", "A screenshot of your wallet", "Your account password"],
        0,
    ),
    Question(
        "Buyback: Contracts must be made in which station?",
        ["AT1", "Jita 4-4", "Any station in system", "Any Upwell structure"],
        0,
    ),
    Question(
        "Buyback Pricing: Ore pricing is calculated based on what?",
        [
            "The compressed version of the ore (even if submitted uncompressed).",
            "The uncompressed version only.",
            "The region median sell price only.",
            "The average of compressed and uncompressed.",
        ],
        0,
    ),
    Question(
        "Buyback: Which is an accepted buyback category?",
        ["Planetary Interaction Products", "Reprocessed materials", "Ships and fittings", "PLEX"],
        0,
    ),
    Question(
        "Buyback: General rule of thumb for what is valid for buyback is:",
        [
            "If it comes from a wormhole, it’s probably valid for buyback.",
            "Only ore is accepted.",
            "Only blue loot is accepted.",
            "Only items from nullsec are accepted.",
        ],
        0,
    ),

    # =========================
    # Bookmarking & Site Management Protocol
    # =========================
    Question(
        "Bookmarking: Where must all sites and wormholes be saved?",
        [
            "ARC Security Shared Folder.",
            "Personal bookmarks only.",
            "Only in the Corp CEO folder.",
            "In local chat for visibility.",
        ],
        0,
    ),
    Question(
        "Bookmarking: Where should you refer for bookmark naming/creation guidance?",
        [
            "See #scanning-rules when creating bookmarks.",
            "Ask in local chat every time.",
            "Use any naming scheme you prefer.",
            "Only directors may create bookmarks.",
        ],
        0,
    ),
    Question(
        "Bookmarking: Which folder is used for Combat Site Warp-In Points?",
        ["Sites Folder", "Wormhole Folder", "Safes Folder", "Gas Folder"],
        0,
    ),
    Question(
        "Bookmarking: Which folder is used for Safe Spots?",
        ["Safes Folder", "OPS Folder", "Rocks Folder", "Data/Relic Folder"],
        0,
    ),
    Question(
        "Bookmarking: Which folder is used for MTU locations and Salvage/Loot spots?",
        ["Salvage Folder", "Gas Folder", "Wormhole Folder", "+POS Folder"],
        0,
    ),

    # =========================
    # AFK Operations Policy (WH)
    # =========================
    Question(
        "AFK Operations (WH): What is the policy regarding AFK activity in wormhole space?",
        [
            "AFK activity is allowed if you are cloaked.",
            "All AFK activity in wormhole space is prohibited (including mining, gas huffing, scanning, or any unattended operation).",
            "AFK activity is allowed only for scanning.",
            "AFK activity is allowed if you are on comms.",
        ],
        1,
    ),
    Question(
        "AFK Operations (WH): What may happen if you are found AFK in wormhole space?",
        [
            "You may be engaged and destroyed by authorized ARC Security leadership and returned to Jita at your own expense.",
            "You will be awarded bonus AP for being present.",
            "You will only receive a verbal reminder with no enforcement.",
            "You will be moved to a different Discord channel.",
        ],
        0,
    ),
    Question(
        "AFK Operations (WH): Who is authorized to enforce the AFK Operations policy?",
        ["Officers and above", "Only Newbros", "Only the buyback officer", "Allies only"],
        0,
    ),
    Question(
        "AFK Operations (WH): How may compliance be verified?",
        [
            "Through random ship name changes to ensure active D-scan monitoring.",
            "By checking your wallet history.",
            "By reviewing your contracts.",
            "By requiring screenshots every 10 minutes.",
        ],
        0,
    ),

    # =========================
    # Logistics Policy – LS/NS Hauling Prohibition
    # =========================
    Question(
        "Logistics (LS/NS): What is the policy on hauling in Lowsec (LS) and Nullsec (NS)?",
        [
            "Allowed anytime as long as you fly tanky ships.",
            "Prohibited unless approved in advance by an Officer or above (leadership team).",
            "Allowed only on weekends.",
            "Allowed if you announce it in Discord.",
        ],
        1,
    ),
    Question(
        "Logistics (LS/NS): Unauthorized LS/NS hauling may result in:",
        [
            "Wormhole location access being revoked and formal disciplinary action.",
            "Immediate promotion to Officer.",
            "Free replacement ships from corp stock.",
            "No consequences if you survive.",
        ],
        0,
    ),
    Question(
        "Logistics (LS/NS): Who is responsible for enforcing the LS/NS hauling policy?",
        ["Officers and above", "Only Newbros", "Only logistics alts", "Anyone in local chat"],
        0,
    ),

    # =========================
    # Wormhole Voice Comms Requirement
    # =========================
    Question(
        "Voice Comms (WH): What is required when conducting any activity in wormhole space?",
        [
            "Be present in the designated Voice Comms (VC). Speaking is not required.",
            "Only type in text chat every 5 minutes.",
            "Stream your gameplay at all times.",
            "Be on VC only when mining.",
        ],
        0,
    ),
    Question(
        "Voice Comms (WH): Why is VC presence mandatory?",
        [
            "To enable immediate threat notification and response.",
            "To increase buyback rates.",
            "To allow access to corp fittings.",
            "To qualify for ship reimbursement.",
        ],
        0,
    ),
    Question(
        "Voice Comms (WH): Failure to comply may result in:",
        [
            "Disciplinary action and/or revocation of wormhole access, enforced by Officers and above.",
            "Automatic ISK fines only.",
            "A permanent ban from buyback.",
            "No impact if you are scanning.",
        ],
        0,
    ),

    # =========================
    # Alt Account Policy
    # =========================
    Question(
        "Alt Policy: What is required at the beginning of all ARC alt character names?",
        [
            "The ARC tag (e.g., ARC Solothon).",
            "The ⓒ symbol only.",
            "A random number.",
            "The director’s initials.",
        ],
        0,
    ),
    Question(
        "Alt Policy: Alt names must correspond to what for identification?",
        [
            "Your Discord username and/or main character name.",
            "Your real-life name.",
            "Your corporation wallet balance.",
            "Your ship hull type.",
        ],
        0,
    ),
    Question(
        "Alt Policy: What is the exception regarding ARC tagging?",
        [
            "Your main character may be the only non-ARC-tagged character.",
            "All characters must be ARC-tagged with no exceptions.",
            "Only industry alts can skip the ARC tag.",
            "Only directors can have non-ARC-tagged characters.",
        ],
        0,
    ),
]


# =====================
# Paged Quiz View (DM)
# =====================
class AnswerSelect(discord.ui.Select):
    def __init__(self, q_index: int, q: Question):
        letters = ["A", "B", "C", "D"]
        options: List[discord.SelectOption] = []
        for i in range(4):
            label = _clamp_1_100(f"{letters[i]}) Select", fallback=f"{letters[i]}) Select")
            desc = _clamp_1_100(q.options[i], fallback="")
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
            custom_id=f"corp_rules_quiz:select:{q_index}",
            row=0,
        )
        self.q_index = q_index

    async def callback(self, interaction: discord.Interaction):
        view: "PagedQuizView" = self.view  # type: ignore
        view.answers[self.q_index] = int(self.values[0])
        await _safe_ephemeral_reply(interaction, f"Recorded answer for Q{self.q_index + 1}.")


class PagedQuizView(discord.ui.View):
    def __init__(self, user_id: int, guild_id: int, questions: List[Question], cog: "CorpRulesTestCog"):
        super().__init__(timeout=900)
        self.user_id = user_id
        self.guild_id = guild_id
        self.questions = questions
        self.cog = cog

        self.page = 0
        self.answers: Dict[int, int] = {}

        self.btn_prev = discord.ui.Button(label="Prev", style=discord.ButtonStyle.secondary, row=1, custom_id="corp_rules_quiz:prev")
        self.btn_next = discord.ui.Button(label="Next", style=discord.ButtonStyle.secondary, row=1, custom_id="corp_rules_quiz:next")
        self.btn_submit = discord.ui.Button(label="Submit", style=discord.ButtonStyle.success, row=2, custom_id="corp_rules_quiz:submit")

        self.btn_prev.callback = self._on_prev  # type: ignore
        self.btn_next.callback = self._on_next  # type: ignore
        self.btn_submit.callback = self._on_submit  # type: ignore

        self._render()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await _safe_ephemeral_reply(interaction, "This test is not for you.")
            return False
        return True

    def _render(self):
        self.clear_items()
        q = self.questions[self.page]
        self.add_item(AnswerSelect(self.page, q))

        self.btn_prev.disabled = (self.page == 0)
        self.btn_next.disabled = (self.page >= len(self.questions) - 1)
        self.btn_submit.disabled = (self.page != len(self.questions) - 1)

        self.add_item(self.btn_prev)
        self.add_item(self.btn_next)
        self.add_item(self.btn_submit)

    def content(self) -> str:
        q = self.questions[self.page]
        chosen = self.answers.get(self.page)
        letters = ["A", "B", "C", "D"]
        chosen_txt = f"\n\n**Current Answer:** {letters[chosen]}" if chosen is not None else ""
        return (
            "**Corp Rules Test (Private)**\n"
            f"Question **{self.page + 1}/{len(self.questions)}**\n"
            f"Passing requires **{PASS_PERCENT}% (perfect score)**.\n\n"
            f"**Q{self.page + 1}.** {q.prompt}"
            f"{chosen_txt}"
        )

    async def _safe_edit(self, interaction: discord.Interaction):
        try:
            if interaction.response.is_done():
                await interaction.followup.edit_message(
                    message_id=interaction.message.id,
                    content=self.content(),
                    view=self
                )
            else:
                await interaction.response.edit_message(content=self.content(), view=self)
        except Exception as e:
            log.exception("Safe edit failed: %r", e)
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

        correct = 0
        for i, q in enumerate(self.questions):
            if self.answers.get(i, -1) == q.correct_index:
                correct += 1

        total = len(self.questions)
        percent = int((correct / total) * 100)
        passed = (percent == 100)

        for item in self.children:
            item.disabled = True

        # Disable components on quiz message
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
            role_msg = await self.cog.remove_newbro(self.guild_id, self.user_id)

        await self.cog.log_result(self.guild_id, self.user_id, passed, correct, total, percent)

        await _safe_ephemeral_reply(
            interaction,
            f"**Result:** {correct}/{total} (**{percent}%**) — {'PASS' if passed else 'FAIL'}\n{role_msg}"
        )


# =====================
# Start Button View (Persistent)
# =====================
class StartTestView(discord.ui.View):
    def __init__(self, cog: "CorpRulesTestCog"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Start Test (DM)", style=discord.ButtonStyle.primary, custom_id=START_BUTTON_CUSTOM_ID)
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await _safe_ephemeral_reply(interaction, "This must be used in a server.")
            return

        # ACK immediately to prevent 10062 Unknown interaction
        await _safe_defer(interaction, ephemeral=True)

        # exactly 5 random questions (if bank has >= 5)
        if len(QUESTION_BANK) < QUESTIONS_PER_TEST:
            await _safe_ephemeral_reply(
                interaction,
                f"Not enough questions configured. Need **{QUESTIONS_PER_TEST}**, found **{len(QUESTION_BANK)}**.",
            )
            return

        questions = random.sample(QUESTION_BANK, QUESTIONS_PER_TEST)
        quiz_view = PagedQuizView(interaction.user.id, interaction.guild.id, questions, self.cog)

        try:
            dm = await interaction.user.create_dm()
            await dm.send(quiz_view.content(), view=quiz_view)
        except discord.Forbidden:
            await _safe_ephemeral_reply(interaction, "I couldn't DM you. Enable DMs and try again.")
            return
        except Exception as e:
            await _safe_ephemeral_reply(interaction, f"Failed to start test: {type(e).__name__}: {e}")
            return

        await _safe_ephemeral_reply(interaction, "Test sent. Check your DMs.")


# =====================
# Cog
# =====================
class CorpRulesTestCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.start_view = StartTestView(self)

    async def cog_load(self):
        self.bot.add_view(self.start_view)

    @commands.Cog.listener()
    async def on_ready(self):
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

    async def remove_newbro(self, guild_id: int, user_id: int) -> str:
        guild = self.bot.get_guild(guild_id)
        if not guild:
            return "PASS recorded, but server not found."

        member = guild.get_member(user_id)
        if not member:
            return "PASS recorded, but member not found."

        role = discord.utils.get(guild.roles, name=ROLE_TO_REMOVE_ON_PASS)
        if not role:
            return f"PASS recorded, but role **{ROLE_TO_REMOVE_ON_PASS}** was not found."

        if role not in member.roles:
            return f"PASS recorded. You do not currently have **{ROLE_TO_REMOVE_ON_PASS}**."

        try:
            await member.remove_roles(role, reason="Passed Corp Rules Test (100%)")
            return f"✅ Removed **{ROLE_TO_REMOVE_ON_PASS}**."
        except discord.Forbidden:
            return f"PASS recorded, but I lack permission to remove **{ROLE_TO_REMOVE_ON_PASS}**."
        except discord.HTTPException:
            return f"PASS recorded, but an API error prevented removing **{ROLE_TO_REMOVE_ON_PASS}**."

    async def log_result(self, guild_id: int, user_id: int, passed: bool, correct: int, total: int, percent: int):
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
        who = member.mention if member else f"<@{user_id}>"
        status = "PASS" if passed else "FAIL"
        ts = int(datetime.datetime.now(datetime.timezone.utc).timestamp())

        try:
            await channel.send(
                f"**Corp Rules Test {status}** — {who} | Score: **{correct}/{total} ({percent}%)** | <t:{ts}:f>"
            )
        except (discord.Forbidden, discord.HTTPException):
            return


async def setup(bot: commands.Bot):
    await bot.add_cog(CorpRulesTestCog(bot))
