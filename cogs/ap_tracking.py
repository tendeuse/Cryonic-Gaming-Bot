# cogs/ap_tracking.py

import discord
import json
import asyncio
import datetime
import io
import csv
from discord.ext import commands, tasks
from discord import app_commands
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import os

# =====================
# PERSISTENCE (Railway Volume)
# =====================
PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)

DATA_FILE = PERSIST_ROOT / "ap_data.json"
EXPORT_DIR = PERSIST_ROOT / "ap_exports"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

# Hierarchy data (owned by arc_hierarchy.py)
HIERARCHY_FILE = PERSIST_ROOT / "arc_hierarchy.json"
HIERARCHY_LOG_CH = "arc-hierarchy-log"

# =====================
# CONFIG
# =====================
VOICE_INTERVAL = 180        # 3 minutes
VOICE_AP = 1
CHAT_INTERVAL = 1800       # 30 minutes
CHAT_AP = 15

MIN_ACCOUNT_AGE_DAYS = 14

LYCAN_ROLE = "Lycan King"
CEO_ROLE = "ARC Security Corporation Leader"
SECURITY_ROLE = "ARC Security"

AP_CHECK_CHANNEL = "ap-check"
AP_CHECK_EMBED_TITLE = "AP Balance"
AP_CHECK_EMBED_TEXT = "Click the button below to see your AP balance"
AP_CHECK_BUTTON_LABEL = "Check Balance"

META_KEY = "_meta"
AP_CHECK_MESSAGE_ID_KEY = "ap_check_message_id"
LAST_WIPE_KEY = "last_wipe_utc"

JOIN_BONUS_AP = 100
JOIN_BONUS_KEY = "join_bonus_awarded"

AP_DISTRIBUTION_LOG_CH = "member-join-logs-points-distribute"

CLAIM_IGN_KEY = "ign"
CLAIM_GAME_KEY = "game"

GAME_EVE = "EVE Online"
GAME_WOW = "World of Warcraft"
EVE_ISK_PER_AP = 100_000
WOW_GOLD_PER_AP = 10

RANK_SECURITY = "security"
RANK_OFFICER = "officer"
RANK_COMMANDER = "commander"
RANK_GENERAL = "general"
RANK_DIRECTOR = "director"

RANK_ORDER = [RANK_SECURITY, RANK_OFFICER, RANK_COMMANDER, RANK_GENERAL, RANK_DIRECTOR]
RANK_INDEX = {r: i for i, r in enumerate(RANK_ORDER)}

# =====================
# Persistence helpers
# =====================
file_lock = asyncio.Lock()

def utcnow() -> str:
    return datetime.datetime.utcnow().isoformat()

async def load() -> Dict[str, Any]:
    async with file_lock:
        if not DATA_FILE.exists():
            return {}
        try:
            return json.loads(DATA_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}

async def save(data: Dict[str, Any]) -> None:
    async with file_lock:
        DATA_FILE.write_text(json.dumps(data, indent=4), encoding="utf-8")

def safe_int_ap(v) -> int:
    try:
        return int(float(v))
    except Exception:
        return 0

def safe_float_ap(v) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0

def has_role_name(member: discord.Member, role_name: str) -> bool:
    return any(r.name == role_name for r in member.roles)

def is_alt_account(member: discord.Member) -> bool:
    age = (datetime.datetime.utcnow() - member.created_at.replace(tzinfo=None)).days
    return age < MIN_ACCOUNT_AGE_DAYS

def load_hierarchy() -> Dict[str, Any]:
    if not HIERARCHY_FILE.exists():
        return {"members": {}, "units": {}}
    try:
        return json.loads(HIERARCHY_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"members": {}, "units": {}}

# =====================
# Persistent AP Check View
# =====================
class APCheckView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label=AP_CHECK_BUTTON_LABEL,
        style=discord.ButtonStyle.primary,
        custom_id="apcheck:balance"
    )
    async def check_balance(self, interaction: discord.Interaction, _):
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Unable to resolve member.", ephemeral=True)
            return

        data = await load()
        ap = safe_int_ap(data.get(str(interaction.user.id), {}).get("ap", 0))
        await interaction.response.send_message(f"You have **{ap} AP**.", ephemeral=True)

# =====================
# Cog
# =====================
class APTracking(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def cog_load(self):
        self.bot.add_view(APCheckView())
        if not self.voice_loop.is_running():
            self.voice_loop.start()
        if not self.chat_loop.is_running():
            self.chat_loop.start()

    async def ensure_ap_check_message(self, guild: discord.Guild):
        channel = discord.utils.get(guild.text_channels, name=AP_CHECK_CHANNEL)
        if not channel:
            try:
                channel = await guild.create_text_channel(AP_CHECK_CHANNEL)
            except discord.Forbidden:
                return

        data = await load()
        meta = data.setdefault(META_KEY, {})
        gmeta = meta.setdefault(str(guild.id), {})
        gmeta.setdefault(LAST_WIPE_KEY, utcnow())

        embed = discord.Embed(
            title=AP_CHECK_EMBED_TITLE,
            description=AP_CHECK_EMBED_TEXT
        )

        msg_id = gmeta.get(AP_CHECK_MESSAGE_ID_KEY)
        if msg_id:
            try:
                msg = await channel.fetch_message(int(msg_id))
                await msg.edit(embed=embed, view=APCheckView())
                await save(data)
                return
            except Exception:
                pass

        msg = await channel.send(embed=embed, view=APCheckView())
        gmeta[AP_CHECK_MESSAGE_ID_KEY] = msg.id
        await save(data)

    @commands.Cog.listener()
    async def on_ready(self):
        for g in self.bot.guilds:
            await self.ensure_ap_check_message(g)

    @tasks.loop(seconds=VOICE_INTERVAL)
    async def voice_loop(self):
        for guild in self.bot.guilds:
            for vc in guild.voice_channels:
                if vc == guild.afk_channel:
                    continue
                members = [
                    m for m in vc.members
                    if isinstance(m, discord.Member)
                    and not m.bot
                    and not is_alt_account(m)
                    and m.voice
                    and not m.voice.self_mute
                    and not m.voice.self_deaf
                ]
                if len(members) < 2:
                    continue
                data = await load()
                for m in members:
                    rec = data.setdefault(str(m.id), {"ap": 0})
                    rec["ap"] = safe_float_ap(rec.get("ap", 0)) + VOICE_AP
                await save(data)

    @tasks.loop(seconds=CHAT_INTERVAL)
    async def chat_loop(self):
        now = datetime.datetime.utcnow()
        data = await load()
        for guild in self.bot.guilds:
            for m in guild.members:
                if not isinstance(m, discord.Member) or m.bot or is_alt_account(m):
                    continue
                rec = data.get(str(m.id))
                if not rec:
                    continue
                try:
                    last = datetime.datetime.fromisoformat(rec.get("last_chat", ""))
                except Exception:
                    continue
                if (now - last).total_seconds() <= CHAT_INTERVAL:
                    rec["ap"] = safe_float_ap(rec.get("ap", 0)) + CHAT_AP
        await save(data)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or not isinstance(message.author, discord.Member):
            return
        data = await load()
        rec = data.setdefault(str(message.author.id), {"ap": 0})
        rec["last_chat"] = utcnow()
        await save(data)

# =====================
# Extension entry point
# =====================
async def setup(bot: commands.Bot):
    await bot.add_cog(APTracking(bot))
