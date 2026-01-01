# cogs/signature_tagging_test.py
#
# Fixes in this version (copy/paste safe):
# 1) Removes ALL ephemeral responses in DMs (ephemeral is guild-only and causes "This interaction failed" in DMs).
# 2) Adds safe interaction helpers (defer/reply/edit) that work in both guild + DM contexts.
# 3) Keeps your existing behavior: 5 random questions, 2 tries/day (UTC), 100% required, grants Exploration Certified.
# 4) Ensures attempts file parent folder exists.

import discord
from discord.ext import commands
import random
from dataclasses import dataclass
from typing import List, Dict
import datetime
import json
from pathlib import Path

# =====================
# CONFIG
# =====================
TEST_CHANNEL_NAME = "exploration-test"
LOG_CH = "arc-hierarchy-log"

MAX_TRIES_PER_DAY = 2
QUESTIONS_PER_TEST = 5
PASS_PERCENT = 100  # perfect score required

CERT_ROLE_NAME = "Exploration Certified"

ATTEMPTS_FILE = Path("signature_tagging_attempts.json")
ATTEMPTS_FILE.parent.mkdir(parents=True, exist_ok=True)

START_BUTTON_CUSTOM_ID = "sig_tag_test:start_dm"

START_MESSAGE_TEXT = (
    "**Signature Tagging Standards Test**\n"
    "Click **Start Test (DM)** to receive a private test in your DMs.\n"
    f"Limit: **{MAX_TRIES_PER_DAY} tries per day (UTC)**.\n"
    f"Passing requires **{PASS_PERCENT}%** and grants **{CERT_ROLE_NAME}**."
)

# =====================
# HELPERS
# =====================
def utc_day_key() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")


def load_attempts() -> Dict[str, Dict[str, int]]:
    if not ATTEMPTS_FILE.exists():
        return {}
    try:
        return json.loads(ATTEMPTS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_attempts(data: Dict[str, Dict[str, int]]) -> None:
    ATTEMPTS_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")


def message_has_start_button(msg: discord.Message) -> bool:
    try:
        for row in msg.components or []:
            for child in getattr(row, "children", []):
                if getattr(child, "custom_id", None) == START_BUTTON_CUSTOM_ID:
                    return True
    except Exception:
        return False
    return False


def can_ephemeral(interaction: discord.Interaction) -> bool:
    # Ephemeral only works in guild interactions.
    return interaction.guild is not None


async def safe_defer(interaction: discord.Interaction) -> None:
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=can_ephemeral(interaction))
    except Exception:
        pass


async def safe_reply(interaction: discord.Interaction, content: str) -> None:
    try:
        if interaction.response.is_done():
            await interaction.followup.send(content, ephemeral=can_ephemeral(interaction))
        else:
            await interaction.response.send_message(content, ephemeral=can_ephemeral(interaction))
    except Exception:
        pass


async def safe_edit_message(interaction: discord.Interaction, *, content: str, view: discord.ui.View) -> None:
    """
    Edit the message that triggered this interaction (works in DM and guild).
    """
    try:
        if interaction.response.is_done():
            # Edit the original interaction message via followup (webhook edit)
            await interaction.followup.edit_message(message_id=interaction.message.id, content=content, view=view)  # type: ignore
        else:
            await interaction.response.edit_message(content=content, view=view)
    except Exception:
        # Last resort: just defer so Discord doesn't show "interaction failed"
        await safe_defer(interaction)


# =====================
# QUESTION BANK
# =====================
@dataclass(frozen=True)
class Question:
    prompt: str
    options: List[str]
    correct_index: int


QUESTION_BANK: List[Question] = [
    Question(
        "For Data/Relic/Gas sites, what is the correct signature naming format?",
        [
            "[Site Name] [Signature ID] [Status Marker]",
            "[Signature ID] [Status Marker] [Site Name]",
            "[Status Marker] [Signature ID] [Site Name]",
            "[Signature ID Letters] [Destination Class] [Site Name]",
        ],
        1,
    ),
    Question(
        "In the tagging standard, what does the '*' status marker indicate?",
        ["Rats are present", "Partially completed", "The site has been cleared", "The site is static"],
        2,
    ),
    Question(
        "Where should extra details like rats present or partial completion be recorded?",
        ["In the signature name", "In the notes/comments field", "In local chat", "In the wormhole name"],
        1,
    ),
    Question(
        "Which example correctly tags a cleared Data/Relic/Gas site?",
        [
            "PWO-081 Sizeable Perimeter Reservoir *",
            "PWO-081 * Sizeable Perimeter Reservoir",
            "PWO-081 - Cleared - Sizeable Perimeter Reservoir",
            "Sizeable Perimeter Reservoir PWO-081 *",
        ],
        1,
    ),
    Question(
        "For wormholes, what is the standard naming format?",
        [
            "[Signature ID] [Destination Class]",
            "[Destination Class] [Signature ID Letters]",
            "[Destination Class] [Full Signature ID]",
            "[Signature ID Letters] [Destination Class] S",
        ],
        1,
    ),
    Question(
        "A wormhole leads to a Class 2 system with signature letters IHQ. What is correct?",
        ["C2 IHQ", "IHQ C2", "C2-IHQ", "C2 IHQ *"],
        0,
    ),
    Question(
        "How do you tag a Home Entry wormhole?",
        ["Entry [Signature ID]", "'Entry [Signature ID]", "HomeEntry [Signature ID]", "[Signature ID] 'Entry"],
        1,
    ),
    Question(
        "Which is a correct Home Entry example for signature EAN?",
        ["'Entry EAN", "Entry 'EAN", "C2 'Entry EAN", "EAN 'Entry"],
        0,
    ),
    Question(
        "How do you tag a static wormhole?",
        ["Add '*'", "Add 'STATIC'", "Add 'S' at the end", "Add an apostrophe"],
        2,
    ),
    Question(
        "Which example correctly tags a static C3 wormhole with letters AAC?",
        ["C3 AAC S", "C3 AAC *", "AAC C3 S", "'Entry AAC S"],
        0,
    ),
]

# =====================
# Paged Quiz View
# =====================
class AnswerSelect(discord.ui.Select):
    def __init__(self, q_index: int, q: Question):
        super().__init__(
            placeholder="Select your answer…",
            min_values=1,
            max_values=1,
            options=[
                discord.SelectOption(label=f"A) {q.options[0]}", value="0"),
                discord.SelectOption(label=f"B) {q.options[1]}", value="1"),
                discord.SelectOption(label=f"C) {q.options[2]}", value="2"),
                discord.SelectOption(label=f"D) {q.options[3]}", value="3"),
            ],
            custom_id=f"sig_tag_quiz:select:{q_index}",
            row=0,
        )
        self.q_index = q_index

    async def callback(self, interaction: discord.Interaction):
        view: "PagedQuizView" = self.view  # type: ignore
        view.answers[self.q_index] = int(self.values[0])

        # In DMs, ephemeral is invalid. Use safe_reply (ephemeral in guild, normal in DM).
        await safe_reply(interaction, f"Recorded answer for Q{self.q_index + 1}.")


class PagedQuizView(discord.ui.View):
    def __init__(self, user_id: int, guild_id: int, questions: List[Question], cog: "SignatureTaggingTestCog"):
        super().__init__(timeout=900)
        self.user_id = user_id
        self.guild_id = guild_id
        self.questions = questions
        self.cog = cog

        self.page = 0
        self.answers: Dict[int, int] = {}

        # Buttons
        self.btn_prev = discord.ui.Button(label="Prev", style=discord.ButtonStyle.secondary, row=1, custom_id="sig_tag_quiz:prev")
        self.btn_next = discord.ui.Button(label="Next", style=discord.ButtonStyle.secondary, row=1, custom_id="sig_tag_quiz:next")
        self.btn_submit = discord.ui.Button(label="Submit", style=discord.ButtonStyle.success, row=2, custom_id="sig_tag_quiz:submit")

        self.btn_prev.callback = self._on_prev  # type: ignore
        self.btn_next.callback = self._on_next  # type: ignore
        self.btn_submit.callback = self._on_submit  # type: ignore

        self._render()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await safe_reply(interaction, "This test is not for you.")
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
        chosen = self.answers.get(self.page, None)
        chosen_txt = f"\n\n**Current Answer:** {['A','B','C','D'][chosen]}" if chosen is not None else ""
        return (
            "**Signature Tagging Standards Test (Private)**\n"
            f"Question **{self.page + 1}/{len(self.questions)}**\n"
            f"Passing requires **{PASS_PERCENT}%**.\n\n"
            f"**Q{self.page + 1}.** {q.prompt}"
            f"{chosen_txt}"
        )

    async def _on_prev(self, interaction: discord.Interaction):
        if self.page > 0:
            self.page -= 1
        self._render()
        await safe_edit_message(interaction, content=self.content(), view=self)

    async def _on_next(self, interaction: discord.Interaction):
        if self.page < len(self.questions) - 1:
            self.page += 1
        self._render()
        await safe_edit_message(interaction, content=self.content(), view=self)

    async def _on_submit(self, interaction: discord.Interaction):
        try:
            correct = 0
            for i, q in enumerate(self.questions):
                if self.answers.get(i, -1) == q.correct_index:
                    correct += 1

            total = len(self.questions)
            percent = int((correct / total) * 100)
            passed = (percent == PASS_PERCENT)

            # Disable UI
            for item in self.children:
                item.disabled = True
            await safe_edit_message(interaction, content=self.content(), view=self)

            role_msg = "No role changes."
            if passed:
                role_msg = await self.cog.grant_cert(self.guild_id, self.user_id)

            await self.cog.log_result(self.guild_id, self.user_id, passed, correct, total, percent)

            # Follow-up result (ephemeral in guild, normal in DM)
            await safe_reply(
                interaction,
                f"**Result:** {correct}/{total} (**{percent}%**) — {'PASS' if passed else 'FAIL'}\n{role_msg}"
            )

        except Exception:
            await safe_reply(interaction, "An error occurred submitting the test. Please retry.")
            raise


# =====================
# Start Button View
# =====================
class StartTestView(discord.ui.View):
    def __init__(self, cog: "SignatureTaggingTestCog"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Start Test (DM)", style=discord.ButtonStyle.primary, custom_id=START_BUTTON_CUSTOM_ID)
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await safe_reply(interaction, "This must be used in a server.")
            return

        day = utc_day_key()
        attempts = load_attempts()
        used = int(attempts.get(day, {}).get(str(interaction.user.id), 0))

        if used >= MAX_TRIES_PER_DAY:
            await safe_reply(interaction, f"You have used all **{MAX_TRIES_PER_DAY}** attempts for **{day} (UTC)**.")
            return

        questions = random.sample(QUESTION_BANK, QUESTIONS_PER_TEST)
        quiz_view = PagedQuizView(interaction.user.id, interaction.guild.id, questions, self.cog)

        try:
            dm = await interaction.user.create_dm()
            await dm.send(quiz_view.content(), view=quiz_view)
        except discord.Forbidden:
            await safe_reply(interaction, "I couldn't DM you. Enable DMs and try again.")
            return

        # consume attempt only after successful DM
        attempts.setdefault(day, {})
        attempts[day][str(interaction.user.id)] = used + 1
        save_attempts(attempts)

        await safe_reply(interaction, f"Test sent. Attempts used today: **{used + 1}/{MAX_TRIES_PER_DAY}** (UTC).")


# =====================
# Cog
# =====================
class SignatureTaggingTestCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.start_view = StartTestView(self)

    async def cog_load(self):
        # persistent view so the button continues to work after restart
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

    async def grant_cert(self, guild_id: int, user_id: int) -> str:
        guild = self.bot.get_guild(guild_id)
        if not guild:
            return "PASS recorded, but server not found."

        member = guild.get_member(user_id)
        if not member:
            return "PASS recorded, but member not found."

        role = discord.utils.get(guild.roles, name=CERT_ROLE_NAME)
        if not role:
            try:
                role = await guild.create_role(name=CERT_ROLE_NAME, reason="Exploration certification role")
            except (discord.Forbidden, discord.HTTPException):
                return f"PASS recorded, but couldn't create **{CERT_ROLE_NAME}** (missing permissions)."

        if role in member.roles:
            return f"Already has **{CERT_ROLE_NAME}**."

        try:
            await member.add_roles(role, reason="Passed Signature Tagging Standards Test")
            return f"✅ Granted **{CERT_ROLE_NAME}**."
        except discord.Forbidden:
            return f"PASS recorded, but I lack permission to grant **{CERT_ROLE_NAME}**."
        except discord.HTTPException:
            return f"PASS recorded, but an API error prevented granting **{CERT_ROLE_NAME}**."

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
                f"**Signature Tagging Test {status}** — {who} | Score: **{correct}/{total} ({percent}%)** | <t:{ts}:f>"
            )
        except (discord.Forbidden, discord.HTTPException):
            return


async def setup(bot: commands.Bot):
    await bot.add_cog(SignatureTaggingTestCog(bot))
