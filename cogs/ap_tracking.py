# cogs/ap_tracking.py
import discord
import json
import asyncio
import datetime
from discord.ext import commands, tasks
from discord import app_commands
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

# =====================
# CONFIG
# =====================
DATA_FILE = Path("data/ap_data.json")
DATA_FILE.parent.mkdir(exist_ok=True)

# Hierarchy data (owned by arc_hierarchy.py)
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

# ARC roles (used for CEO bonus eligibility)
CEO_ROLE = "ARC Security Corporation Leader"
SECURITY_ROLE = "ARC Security"

# Join bonus
JOIN_BONUS_AP = 100
JOIN_BONUS_KEY = "join_bonus_awarded"

# NEW: AP distribution log channel
AP_DISTRIBUTION_LOG_CH = "member-join-logs-points-distribute"

# ARC ranks (read from hierarchy file)
RANK_SECURITY = "security"
RANK_OFFICER = "officer"
RANK_COMMANDER = "commander"
RANK_GENERAL = "general"
RANK_DIRECTOR = "director"

RANK_ORDER = [RANK_SECURITY, RANK_OFFICER, RANK_COMMANDER, RANK_GENERAL, RANK_DIRECTOR]
RANK_INDEX = {r: i for i, r in enumerate(RANK_ORDER)}

# -------------------------
# Utility / Persistence
# -------------------------
file_lock = asyncio.Lock()

def utcnow():
    return datetime.datetime.utcnow().isoformat()

async def load():
    async with file_lock:
        if not DATA_FILE.exists():
            return {}
        try:
            return json.loads(DATA_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            backup = DATA_FILE.with_suffix(".bak")
            try:
                DATA_FILE.replace(backup)
            except Exception:
                pass
            return {}

async def save(data):
    async with file_lock:
        DATA_FILE.write_text(json.dumps(data, indent=4))

def has_role_name(member: discord.Member, role_name: str) -> bool:
    return any(r.name == role_name for r in member.roles)

def is_alt_account(member: discord.Member) -> bool:
    age = (datetime.datetime.utcnow() - member.created_at.replace(tzinfo=None)).days
    return age < MIN_ACCOUNT_AGE_DAYS

def safe_int_ap(value) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0

def safe_float_ap(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0

def load_hierarchy() -> Dict[str, Any]:
    if not HIERARCHY_FILE.exists():
        return {"members": {}, "units": {}}
    try:
        return json.loads(HIERARCHY_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"members": {}, "units": {}}

async def ensure_hierarchy_log_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    ch = discord.utils.get(guild.text_channels, name=HIERARCHY_LOG_CH)
    if ch:
        return ch
    try:
        return await guild.create_text_channel(HIERARCHY_LOG_CH)
    except discord.Forbidden:
        return None

async def log_hierarchy_ap(
    guild: discord.Guild,
    message: str,
    mention_ids: List[int]
) -> None:
    ch = await ensure_hierarchy_log_channel(guild)
    if not ch:
        return

    uniq: List[int] = []
    for i in mention_ids:
        if isinstance(i, int) and i not in uniq:
            uniq.append(i)

    mentions = []
    for uid in uniq:
        m = guild.get_member(uid)
        if m:
            mentions.append(m.mention)

    prefix = (" ".join(mentions) + "\n") if mentions else ""
    await ch.send(prefix + message)

# -------------------------
# NEW: AP Distribution Log (embedded)
# -------------------------
async def ensure_ap_distribution_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    ch = discord.utils.get(guild.text_channels, name=AP_DISTRIBUTION_LOG_CH)
    if ch:
        return ch
    try:
        return await guild.create_text_channel(AP_DISTRIBUTION_LOG_CH)
    except discord.Forbidden:
        return None

async def log_ap_distribution_embed(
    guild: discord.Guild,
    *,
    title: str,
    recipient: discord.Member,
    amount: float,
    source: str,
    reason: Optional[str] = None,
    actor: Optional[discord.Member] = None,
) -> None:
    """
    Posts a single embed to #member-join-logs-points-distribute
    for join bonus + manual /give_ap + video submission (or any other logged award).
    """
    ch = await ensure_ap_distribution_channel(guild)
    if not ch:
        return

    embed = discord.Embed(
        title=title,
        timestamp=datetime.datetime.utcnow()
    )
    embed.add_field(name="Recipient", value=f"{recipient.mention} (`{recipient.id}`)", inline=False)
    embed.add_field(name="Amount", value=f"**+{amount:.2f} AP**", inline=True)
    embed.add_field(name="Source", value=source, inline=True)

    if actor:
        embed.add_field(name="Issued By", value=f"{actor.mention} (`{actor.id}`)", inline=False)

    if reason:
        embed.add_field(name="Reason", value=reason[:1024], inline=False)

    try:
        await ch.send(embed=embed)
    except (discord.Forbidden, discord.HTTPException):
        pass

# -------------------------
# Bonus Logic
# -------------------------
def member_hierarchy_record(h: Dict[str, Any], member_id: int) -> Dict[str, Any]:
    return h.get("members", {}).get(str(member_id), {}) if isinstance(h, dict) else {}

def member_rank_and_unit(h: Dict[str, Any], member_id: int) -> Tuple[str, Optional[int]]:
    rec = member_hierarchy_record(h, member_id)
    rank = rec.get("rank", RANK_SECURITY)
    unit_director_id = rec.get("director_id")
    if not isinstance(unit_director_id, int):
        unit_director_id = None
    if rank not in RANK_INDEX:
        rank = RANK_SECURITY
    return rank, unit_director_id

def members_in_unit_by_rank(guild: discord.Guild, h: Dict[str, Any], unit_director_id: int) -> Dict[str, List[int]]:
    out: Dict[str, List[int]] = {r: [] for r in RANK_ORDER}
    members_map = h.get("members", {}) if isinstance(h, dict) else {}
    for uid_str, rec in members_map.items():
        if not isinstance(rec, dict):
            continue
        if rec.get("director_id") != unit_director_id:
            continue
        try:
            uid = int(uid_str)
        except Exception:
            continue
        if not guild.get_member(uid):
            continue
        r = rec.get("rank", RANK_SECURITY)
        if r not in out:
            r = RANK_SECURITY
        out[r].append(uid)
    return out

def higher_tiers_than(rank: str) -> List[str]:
    i = RANK_INDEX.get(rank, 0)
    return [r for r in RANK_ORDER if RANK_INDEX[r] > i]

def ceo_ids(guild: discord.Guild) -> List[int]:
    ids: List[int] = []
    for m in guild.members:
        if isinstance(m, discord.Member) and has_role_name(m, CEO_ROLE):
            ids.append(m.id)
    return ids

async def add_ap_raw(data: Dict[str, Any], member_id: int, amount: float) -> None:
    rec = data.setdefault(str(member_id), {"ap": 0, "last_chat": None})
    rec["ap"] = safe_float_ap(rec.get("ap", 0)) + float(amount)

async def award_ap_with_bonuses(
    guild: discord.Guild,
    earner: discord.Member,
    base_amount: float,
    source: str,
    reason: Optional[str] = None,
    *,
    log: bool = True,
    actor: Optional[discord.Member] = None,
    distribution_embed: bool = False,
    distribution_title: Optional[str] = None,
) -> None:
    """
    Adds base AP to earner, then distributes:
      - Unit bonuses: for each higher tier in the same unit, 10% of base split equally among that tier
      - CEO bonus: each CEO gets full 10% (NOT divided) when earner has ARC Security role (static role)
    Bonuses do not cascade (they do not trigger further bonuses).

    NEW:
      - If distribution_embed=True, posts an embed to #member-join-logs-points-distribute
        showing the base points given (not the downstream bonuses).
    """
    if base_amount <= 0:
        return

    h = load_hierarchy()
    earner_rank, unit_director_id = member_rank_and_unit(h, earner.id)

    data = await load()

    # 1) Base AP
    await add_ap_raw(data, earner.id, base_amount)

    distributions: List[str] = []
    mention_ids: List[int] = []

    # 2) Unit bonuses (only if unit is known)
    if unit_director_id is not None:
        mention_ids.append(unit_director_id)

        by_rank = members_in_unit_by_rank(guild, h, unit_director_id)
        for tier in higher_tiers_than(earner_rank):
            targets = by_rank.get(tier, [])
            if not targets:
                continue
            pool = base_amount * 0.10
            each = pool / float(len(targets))
            for uid in targets:
                await add_ap_raw(data, uid, each)
            distributions.append(f"- {tier}: +{pool:.2f} total across {len(targets)} member(s) (+{each:.2f} each)")
            if tier == RANK_DIRECTOR:
                mention_ids.extend(targets)

    # 3) CEO bonus (NOT divided)
    ceo_bonus = 0.0
    ceo_targets: List[int] = []
    if has_role_name(earner, SECURITY_ROLE):
        ceo_targets = ceo_ids(guild)
        if ceo_targets:
            ceo_bonus = base_amount * 0.10
            for uid in ceo_targets:
                await add_ap_raw(data, uid, ceo_bonus)
            mention_ids.extend(ceo_targets)

    await save(data)

    # NEW: distribution embed (base award)
    if distribution_embed:
        await log_ap_distribution_embed(
            guild,
            title=distribution_title or "AP Awarded",
            recipient=earner,
            amount=float(base_amount),
            source=source,
            reason=reason,
            actor=actor,
        )

    # 4) Logging (hierarchy log channel)
    if log:
        lines = [
            f"AP base award: {earner.mention} **+{base_amount:.2f} AP** via **{source}**."
        ]
        if reason:
            lines.append(f"Reason: {reason}")

        if unit_director_id is None:
            lines.append("Unit bonus: skipped (member has no unit / director_id in arc_hierarchy.json).")
        elif distributions:
            lines.append("Unit bonuses (each higher tier gets 10% of base; split within tier):")
            lines.extend(distributions)
        else:
            lines.append("Unit bonuses: none (no higher-tier members in unit).")

        if ceo_targets and ceo_bonus > 0:
            lines.append(f"CEO bonus: each CEO received **+{ceo_bonus:.2f} AP** (10% of base; not divided).")
        else:
            lines.append("CEO bonus: none (no CEO found).")

        await log_hierarchy_ap(guild, "\n".join(lines), mention_ids)

# -------------------------
# Persistent View
# -------------------------
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
            await interaction.response.send_message("Could not resolve member.", ephemeral=True)
            return

        data = await load()
        ap = safe_int_ap(data.get(str(interaction.user.id), {}).get("ap", 0))
        await interaction.response.send_message(f"You have **{ap} AP**.", ephemeral=True)

# -------------------------
# Cog
# -------------------------
class APTracking(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
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

        overwrites = {
            guild.default_role: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=False
            )
        }

        if guild.me:
            overwrites[guild.me] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                manage_messages=True
            )

        try:
            await channel.edit(overwrites=overwrites)
        except discord.Forbidden:
            pass

        data = await load()
        meta = data.setdefault(META_KEY, {})
        gmeta = meta.setdefault(str(guild.id), {})

        embed = discord.Embed(
            title=AP_CHECK_EMBED_TITLE,
            description=AP_CHECK_EMBED_TEXT
        )

        view = APCheckView()
        msg_id = gmeta.get(AP_CHECK_MESSAGE_ID_KEY)

        if msg_id:
            try:
                msg = await channel.fetch_message(int(msg_id))
                await msg.edit(embed=embed, view=view)
                return
            except Exception:
                pass

        msg = await channel.send(embed=embed, view=view)
        gmeta[AP_CHECK_MESSAGE_ID_KEY] = msg.id
        await save(data)

    @commands.Cog.listener()
    async def on_ready(self):
        self.bot.add_view(APCheckView())
        for g in self.bot.guilds:
            await self.ensure_ap_check_message(g)
            await ensure_hierarchy_log_channel(g)
            # NEW: ensure distribution channel exists
            await ensure_ap_distribution_channel(g)

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        """
        Awards a one-time 100 AP bonus when a member joins the server.
        - Skips alt accounts under MIN_ACCOUNT_AGE_DAYS.
        - Only awards once per user (even if they leave/rejoin).
        - Does NOT trigger hierarchy/CEO bonuses (this is a join incentive, not an earn event).
        Also posts an embed in #member-join-logs-points-distribute.
        """
        if not isinstance(member, discord.Member) or member.bot:
            return
        if is_alt_account(member):
            return

        data = await load()
        rec = data.setdefault(str(member.id), {"ap": 0, "last_chat": None})

        if rec.get(JOIN_BONUS_KEY) is True:
            return

        await add_ap_raw(data, member.id, float(JOIN_BONUS_AP))
        rec[JOIN_BONUS_KEY] = True
        await save(data)

        # NEW: distribution embed
        await log_ap_distribution_embed(
            member.guild,
            title="Join Bonus Awarded",
            recipient=member,
            amount=float(JOIN_BONUS_AP),
            source="join bonus",
            reason="Automatic join bonus",
            actor=None
        )

        # Optional: log to hierarchy log channel (public record)
        try:
            await log_hierarchy_ap(
                member.guild,
                f"Join bonus: {member.mention} received **+{JOIN_BONUS_AP} AP** for joining the server.",
                mention_ids=[member.id],
            )
        except Exception:
            pass

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or not isinstance(message.author, discord.Member):
            return
        if is_alt_account(message.author):
            return

        data = await load()
        rec = data.setdefault(str(message.author.id), {"ap": 0, "last_chat": None})
        rec["last_chat"] = utcnow()
        await save(data)

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
                    and m.voice
                    and not m.voice.self_mute
                    and not m.voice.self_deaf
                    and not is_alt_account(m)
                ]
                if len(members) < 2:
                    continue

                for m in members:
                    await award_ap_with_bonuses(
                        guild=guild,
                        earner=m,
                        base_amount=float(VOICE_AP),
                        source="voice",
                        reason=None,
                        log=False
                    )

    @tasks.loop(seconds=CHAT_INTERVAL)
    async def chat_loop(self):
        now = datetime.datetime.utcnow()
        data = await load()

        for guild in self.bot.guilds:
            for m in guild.members:
                if not isinstance(m, discord.Member) or m.bot:
                    continue
                if is_alt_account(m):
                    continue

                rec = data.get(str(m.id))
                if not isinstance(rec, dict):
                    continue
                ts = rec.get("last_chat")
                if not ts:
                    continue
                try:
                    last = datetime.datetime.fromisoformat(ts)
                except ValueError:
                    continue

                if (now - last).total_seconds() <= CHAT_INTERVAL:
                    await award_ap_with_bonuses(
                        guild=guild,
                        earner=m,
                        base_amount=float(CHAT_AP),
                        source="chat",
                        reason=None,
                        log=False
                    )

    # -------------------------
    # Slash Commands
    # -------------------------
    @app_commands.command(name="give_ap", description="Give AP to a member (Lycan King only).")
    async def give_ap(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
        amount: app_commands.Range[int, 1, 1_000_000],
        reason: str | None = None
    ):
        if not has_role_name(interaction.user, LYCAN_ROLE):
            await interaction.response.send_message("❌ Not authorized.", ephemeral=True)
            return

        await award_ap_with_bonuses(
            guild=interaction.guild,
            earner=member,
            base_amount=float(amount),
            source="manual give_ap",
            reason=reason,
            log=True,
            actor=interaction.user,
            distribution_embed=True,
            distribution_title="Manual AP Awarded"
        )

        await interaction.response.send_message(
            f"Added **{amount} AP** to {member.mention} (bonuses redistributed).",
            ephemeral=True
        )

    @app_commands.command(name="remove_ap", description="Remove AP from a member (Lycan King only).")
    async def remove_ap(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
        amount: app_commands.Range[int, 1, 1_000_000],
        reason: str | None = None
    ):
        if not has_role_name(interaction.user, LYCAN_ROLE):
            await interaction.response.send_message("❌ Not authorized.", ephemeral=True)
            return

        data = await load()
        rec = data.setdefault(str(member.id), {"ap": 0, "last_chat": None})
        rec["ap"] = max(0, safe_float_ap(rec.get("ap", 0)) - float(amount))
        await save(data)

        await interaction.response.send_message(
            f"Removed **{amount} AP** from {member.mention}.",
            ephemeral=True
        )

    @app_commands.command(name="point", description="Check AP for yourself or a member (public).")
    async def point(self, interaction: discord.Interaction, member: discord.Member | None = None):
        target = member or interaction.user
        data = await load()
        ap = safe_int_ap(data.get(str(target.id), {}).get("ap", 0))
        await interaction.response.send_message(f"{target.mention} has **{ap} AP**.")

async def setup(bot: commands.Bot):
    await bot.add_cog(APTracking(bot))
