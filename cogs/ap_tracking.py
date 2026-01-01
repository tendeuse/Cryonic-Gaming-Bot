# cogs/ap_tracking.py
# Persistent AP tracking (Railway volume-aware)

from __future__ import annotations

# =====================
# IMPORTS
# =====================
import os
import json
import asyncio
import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import discord
from discord.ext import commands
from discord import app_commands


# =====================
# CONFIG
# =====================

# Railway persistent volume mount point
PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))

# Store AP data + exports on the persistent volume
DATA_FILE = PERSIST_ROOT / "ap_data.json"
EXPORT_DIR = PERSIST_ROOT / "ap_exports"

# Ensure directories exist
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

# Hierarchy data (owned by arc_hierarchy.py)
# NOTE: This remains in the project filesystem by default.
# If you also want it persisted, move it under PERSIST_ROOT as well.
HIERARCHY_FILE = Path("arc_hierarchy.json")
HIERARCHY_LOG_CH = "arc-hierarchy-log"

VOICE_INTERVAL = 180        # 3 minutes
VOICE_AP = 1
CHAT_INTERVAL = 1800       # 30 minutes
CHAT_AP = 15

MIN_ACCOUNT_AGE_DAYS = 14  # Alt-account mitigation

LYCAN_ROLE = "Lycan King"

AP_CHECK_CHANNEL = "ap-check"
AP_CHECK_EMBED_TITLE = "AP Balance"
AP_CHECK_EMBED_TEXT = "Click check ap to see your point balance"
AP_CHECK_BUTTON_LABEL = "Check Balance"

META_KEY = "_meta"
AP_CHECK_MESSAGE_ID_KEY = "ap_check_message_id"
LAST_WIPE_KEY = "last_wipe_utc"

# ARC roles (used for CEO bonus eligibility and permissions)
CEO_ROLE = "ARC Security Corporation Leader"
SECURITY_ROLE = "ARC Security"

# Join bonus
JOIN_BONUS_AP = 100
JOIN_BONUS_KEY = "join_bonus_awarded"

# AP distribution log channel
AP_DISTRIBUTION_LOG_CH = "member-join-logs-points-distribute"

# Claim keys
CLAIM_IGN_KEY = "ign"
CLAIM_GAME_KEY = "game"

# Game rates
GAME_EVE = "EVE online"
GAME_WOW = "World of Warcraft"
EVE_ISK_PER_AP = 100_000
WOW_GOLD_PER_AP = 10

# ARC ranks (read from hierarchy file)
RANK_SECURITY = "security"
RANK_OFFICER = "officer"
RANK_COMMANDER = "commander"
RANK_GENERAL = "general"
RANK_DIRECTOR = "director"

RANK_ORDER = [RANK_SECURITY, RANK_OFFICER, RANK_COMMANDER, RANK_GENERAL, RANK_DIRECTOR]
RANK_INDEX = {r: i for i, r in enumerate(RANK_ORDER)}


# =====================
# Utility / Persistence
# =====================

# Single lock for file I/O (safe across tasks within one process)
file_lock = asyncio.Lock()


def utcnow_iso() -> str:
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()


async def load_data() -> Dict[str, Any]:
    """
    Load AP data from DATA_FILE. If JSON is corrupt, back it up and return {}.
    """
    async with file_lock:
        if not DATA_FILE.exists():
            return {}

        try:
            return json.loads(DATA_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            # Corrupt JSON: keep a backup and reset
            backup = DATA_FILE.with_suffix(".bak")
            try:
                DATA_FILE.replace(backup)
            except Exception:
                pass
            return {}
        except Exception:
            return {}


async def save_data(data: Dict[str, Any]) -> None:
    """
    Atomic write to reduce risk of file corruption (write temp then replace).
    """
    async with file_lock:
        tmp = DATA_FILE.with_suffix(".tmp")
        payload = json.dumps(data, indent=4, ensure_ascii=False)
        tmp.write_text(payload, encoding="utf-8")
        tmp.replace(DATA_FILE)


def get_member_key(member: discord.abc.User) -> str:
    # Use Discord user ID as stable key
    return str(member.id)


def get_balance(data: Dict[str, Any], member_id: int) -> int:
    rec = data.get(str(member_id), {})
    ap = rec.get("ap", 0)
    try:
        return int(ap)
    except Exception:
        return 0


def set_balance(data: Dict[str, Any], member_id: int, new_balance: int) -> None:
    rec = data.setdefault(str(member_id), {})
    rec["ap"] = int(new_balance)


# =====================
# UI: AP Check Button
# =====================

class APCheckView(discord.ui.View):
    def __init__(self, cog: "APTracking"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label=AP_CHECK_BUTTON_LABEL,
        style=discord.ButtonStyle.primary,
        custom_id="ap_tracking:check_balance",
    )
    async def check_balance(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = await load_data()
        bal = get_balance(data, interaction.user.id)

        embed = discord.Embed(
            title=AP_CHECK_EMBED_TITLE,
            description=f"**{interaction.user.display_name}** has **{bal} AP**.",
            color=discord.Color.blurple(),
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)


# =====================
# Cog
# =====================

class APTracking(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def cog_load(self) -> None:
        # Ensure persistent view is registered so the button works after restarts.
        self.bot.add_view(APCheckView(self))

    @app_commands.command(name="ap", description="Show your current AP balance")
    async def ap(self, interaction: discord.Interaction):
        data = await load_data()
        bal = get_balance(data, interaction.user.id)

        embed = discord.Embed(
            title="AP Balance",
            description=f"**{interaction.user.display_name}** has **{bal} AP**.",
            color=discord.Color.blurple(),
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="ap_set", description="(Admin) Set a member's AP balance")
    @app_commands.default_permissions(administrator=True)
    async def ap_set(self, interaction: discord.Interaction, member: discord.Member, amount: int):
        if amount < 0:
            await interaction.response.send_message("Amount must be >= 0.", ephemeral=True)
            return

        data = await load_data()
        set_balance(data, member.id, amount)
        await save_data(data)

        await interaction.response.send_message(
            f"Set **{member.display_name}** to **{amount} AP**.",
            ephemeral=True
        )

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        """
        Award join bonus once per member (persisted).
        Also logs distribution to AP_DISTRIBUTION_LOG_CH if present.
        """
        # Alt-account mitigation (optional; skip awarding if too new)
        try:
            created = member.created_at  # aware datetime
            age_days = (discord.utils.utcnow() - created).days
            if age_days < MIN_ACCOUNT_AGE_DAYS:
                return
        except Exception:
            pass

        data = await load_data()
        rec = data.setdefault(str(member.id), {})

        if rec.get(JOIN_BONUS_KEY):
            return

        # Award
        current = get_balance(data, member.id)
        set_balance(data, member.id, current + JOIN_BONUS_AP)
        rec[JOIN_BONUS_KEY] = True

        # Meta
        meta = data.setdefault(META_KEY, {})
        meta.setdefault(LAST_WIPE_KEY, meta.get(LAST_WIPE_KEY) or utcnow_iso())

        await save_data(data)

        # Log distribution (best-effort)
        try:
            ch = discord.utils.get(member.guild.text_channels, name=AP_DISTRIBUTION_LOG_CH)
            if ch:
                await ch.send(
                    f"Join bonus awarded: **{member.mention}** +{JOIN_BONUS_AP} AP "
                    f"(new balance: {current + JOIN_BONUS_AP})."
                )
        except Exception:
            pass

    @app_commands.command(name="ap_post_check", description="(Admin) Post the AP check button panel in #ap-check")
    @app_commands.default_permissions(administrator=True)
    async def ap_post_check(self, interaction: discord.Interaction):
        """
        Posts (or re-posts) the persistent AP check panel in the AP_CHECK_CHANNEL.
        Stores the message ID in the persistent DATA_FILE meta so it can be tracked.
        """
        channel = discord.utils.get(interaction.guild.text_channels, name=AP_CHECK_CHANNEL)
        if not channel:
            await interaction.response.send_message(
                f"Channel #{AP_CHECK_CHANNEL} not found.", ephemeral=True
            )
            return

        embed = discord.Embed(
            title=AP_CHECK_EMBED_TITLE,
            description=AP_CHECK_EMBED_TEXT,
            color=discord.Color.blurple(),
        )

        msg = await channel.send(embed=embed, view=APCheckView(self))

        data = await load_data()
        meta = data.setdefault(META_KEY, {})
        meta[AP_CHECK_MESSAGE_ID_KEY] = msg.id
        await save_data(data)

        await interaction.response.send_message(
            f"Posted AP check panel in #{AP_CHECK_CHANNEL}.", ephemeral=True
        )


# =====================
# Extension entrypoint
# =====================

async def setup(bot: commands.Bot):
    await bot.add_cog(APTracking(bot))
