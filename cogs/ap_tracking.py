# cogs/ap_tracking.py
#
# Railway-volume persistent AP system + hierarchy-aware bonus redistribution
# - Persists to /data by default (Railway volume). Override with env var PERSIST_ROOT.
# - Keeps ALL commands/reports from your posted script:
#     /apclaim, /give_ap, /remove_ap, /transfer_ap, /ap_info, /export_ap, /point
# - Maintains:
#     AP check channel with persistent button
#     Join bonus + distribution embed log channel
#     Hierarchy log channel
#     Voice + chat AP loops with bonuses
#
# NOTE:
#   This cog expects arc_hierarchy.py to write hierarchy data to the SAME volume:
#   /data/arc_hierarchy.json (or PERSIST_ROOT/arc_hierarchy.json).
#
import os
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
GAME_EVE = "EVE Online"
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

# -------------------------
# Utility / Persistence
# -------------------------
file_lock = asyncio.Lock()

def utcnow() -> str:
    return datetime.datetime.utcnow().isoformat()

def _default_ap_data() -> Dict[str, Any]:
    return {}

def _atomic_write_json(p: Path, data: Dict[str, Any]) -> None:
    """
    Atomic JSON write:
      - write to .tmp in same directory
      - replace target
    Reduces risk of corruption on crash/redeploy.
    """
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    payload = json.dumps(data, indent=4)
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(p)

async def load() -> Dict[str, Any]:
    async with file_lock:
        if not DATA_FILE.exists():
            return _default_ap_data()
        try:
            txt = DATA_FILE.read_text(encoding="utf-8").strip()
            if not txt:
                return _default_ap_data()
            data = json.loads(txt)
            if not isinstance(data, dict):
                return _default_ap_data()
            return data
        except json.JSONDecodeError:
            # Keep a backup for inspection, then reset
            try:
                bak = DATA_FILE.with_suffix(DATA_FILE.suffix + ".bak")
                DATA_FILE.replace(bak)
            except Exception:
                pass
            return _default_ap_data()
        except Exception:
            return _default_ap_data()

async def save(data: Dict[str, Any]) -> None:
    async with file_lock:
        _atomic_write_json(DATA_FILE, data)

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
        txt = HIERARCHY_FILE.read_text(encoding="utf-8").strip()
        if not txt:
            return {"members": {}, "units": {}}
        data = json.loads(txt)
        if not isinstance(data, dict):
            return {"members": {}, "units": {}}
        data.setdefault("members", {})
        data.setdefault("units", {})
        if not isinstance(data["members"], dict):
            data["members"] = {}
        if not isinstance(data["units"], dict):
            data["units"] = {}
        return data
    except Exception:
        return {"members": {}, "units": {}}

def is_authorized_admin(member: discord.Member) -> bool:
    return has_role_name(member, CEO_ROLE) or has_role_name(member, LYCAN_ROLE)

def fmt_int(n: int) -> str:
    return f"{n:,}"

def payout_for(game: str, ap: int) -> Tuple[str, str]:
    if game == GAME_EVE:
        return (fmt_int(ap * EVE_ISK_PER_AP), "ISK")
    if game == GAME_WOW:
        return (fmt_int(ap * WOW_GOLD_PER_AP), "Gold")
    return ("", "")

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
# AP Distribution Log (embedded)
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

    # Distribution embed (base award)
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

    # Hierarchy log channel
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
# Reporting / Export Helpers
# -------------------------
def iter_member_records(data: Dict[str, Any]) -> List[Tuple[int, Dict[str, Any]]]:
    """
    Returns list of (member_id, record) for all non-meta records.
    """
    out: List[Tuple[int, Dict[str, Any]]] = []
    for k, v in data.items():
        if k == META_KEY:
            continue
        if not isinstance(v, dict):
            continue
        try:
            uid = int(k)
        except Exception:
            continue
        out.append((uid, v))
    return out

def build_ap_rows_by_game(
    guild: discord.Guild,
    data: Dict[str, Any],
) -> Dict[str, List[List[str]]]:
    """
    Produces CSV rows grouped by game label.
    Columns:
      Discord Name, IGN, Game, AP, Payout Amount, Payout Currency
    """
    groups: Dict[str, List[List[str]]] = {
        GAME_WOW: [],
        GAME_EVE: [],
        "Unclaimed / Unknown": []
    }

    for uid, rec in iter_member_records(data):
        member = guild.get_member(uid)
        if not member:
            continue

        ap_int = safe_int_ap(rec.get("ap", 0))
        ign = (rec.get(CLAIM_IGN_KEY) or "").strip()
        game = (rec.get(CLAIM_GAME_KEY) or "").strip()

        if game not in (GAME_WOW, GAME_EVE):
            game_label = "Unclaimed / Unknown"
        else:
            game_label = game

        payout_amount, payout_currency = payout_for(game, ap_int)

        groups[game_label].append([
            member.display_name,
            ign,
            game if game else "",
            str(ap_int),
            payout_amount,
            payout_currency
        ])

    # Sort each group: highest AP first, then name
    for gname in list(groups.keys()):
        groups[gname].sort(key=lambda r: (-safe_int_ap(r[3]), r[0].lower()))
    return groups

def render_grouped_csv(groups: Dict[str, List[List[str]]]) -> bytes:
    """
    CSV divided per game:
      - World of Warcraft players
      - blank line
      - EVE Online players
      - blank line
      - Unclaimed / Unknown
    """
    output = io.StringIO()
    writer = csv.writer(output)

    header = ["Discord Name", "IGN (via /apclaim)", "Game", "AP", "Converted Amount", "Currency"]

    def write_section(title: str, rows: List[List[str]]):
        writer.writerow([title])
        writer.writerow(header)
        if rows:
            writer.writerows(rows)
        else:
            writer.writerow(["(none)"])
        writer.writerow([])  # blank line separator

    write_section(GAME_WOW, groups.get(GAME_WOW, []))
    write_section(GAME_EVE, groups.get(GAME_EVE, []))
    write_section("Unclaimed / Unknown", groups.get("Unclaimed / Unknown", []))

    return output.getvalue().encode("utf-8")

async def wipe_ap_in_data(data: Dict[str, Any]) -> None:
    """
    Wipes AP balances for all users while preserving:
      - join bonus flag (JOIN_BONUS_KEY)
      - ign/game claim fields
    Also clears last_chat timestamps to avoid immediate chat awards.
    """
    for _, rec in iter_member_records(data):
        rec["ap"] = 0
        rec["last_chat"] = None

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
# AP Claim Flow (Buttons -> Modal)
# -------------------------
class APClaimIGNModal(discord.ui.Modal):
    def __init__(self, *, game_value: str):
        super().__init__(title="Claim AP - Enter IGN")
        self.game_value = game_value

        self.ign_input = discord.ui.TextInput(
            label="In-Game Name (IGN)",
            placeholder="Type the character name you want AP claimed on",
            min_length=1,
            max_length=64,
            required=True
        )
        self.add_item(self.ign_input)

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Could not resolve guild/member.", ephemeral=True)
            return

        ign = str(self.ign_input.value).strip()
        if not ign:
            await interaction.response.send_message("IGN cannot be empty.", ephemeral=True)
            return

        data = await load()
        rec = data.setdefault(str(interaction.user.id), {"ap": 0, "last_chat": None})
        rec[CLAIM_GAME_KEY] = self.game_value
        rec[CLAIM_IGN_KEY] = ign
        await save(data)

        embed = discord.Embed(
            title="AP Claim Saved",
            description=f"Saved your claim:\n**Game:** {self.game_value}\n**IGN:** {ign}",
            timestamp=datetime.datetime.utcnow()
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

class APClaimGameView(discord.ui.View):
    def __init__(self, owner_id: int):
        super().__init__(timeout=120)
        self.owner_id = owner_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # Only the user who ran /apclaim can use these buttons
        if interaction.user and interaction.user.id == self.owner_id:
            return True
        await interaction.response.send_message("This claim menu isn't for you.", ephemeral=True)
        return False

    @discord.ui.button(label=GAME_EVE, style=discord.ButtonStyle.primary, custom_id="apclaim:game:eve")
    async def pick_eve(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await interaction.response.send_modal(APClaimIGNModal(game_value=GAME_EVE))

    @discord.ui.button(label=GAME_WOW, style=discord.ButtonStyle.primary, custom_id="apclaim:game:wow")
    async def pick_wow(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await interaction.response.send_modal(APClaimIGNModal(game_value=GAME_WOW))

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

        me = guild.me or guild.get_member(self.bot.user.id) if self.bot.user else None
        if me:
            overwrites[me] = discord.PermissionOverwrite(
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
        gmeta.setdefault(LAST_WIPE_KEY, gmeta.get(LAST_WIPE_KEY) or utcnow())

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
                await save(data)
                return
            except Exception:
                pass

        msg = await channel.send(embed=embed, view=view)
        gmeta[AP_CHECK_MESSAGE_ID_KEY] = msg.id
        await save(data)

    @commands.Cog.listener()
    async def on_ready(self):
        # Persistent AP check view
        self.bot.add_view(APCheckView())

        for g in self.bot.guilds:
            await self.ensure_ap_check_message(g)
            await ensure_hierarchy_log_channel(g)
            await ensure_ap_distribution_channel(g)

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
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

        await log_ap_distribution_embed(
            member.guild,
            title="Join Bonus Awarded",
            recipient=member,
            amount=float(JOIN_BONUS_AP),
            source="join bonus",
            reason="Automatic join bonus",
            actor=None
        )

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
    @app_commands.command(name="apclaim", description="Claim your IGN and game for AP exports.")
    async def apclaim(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Could not resolve guild/member.", ephemeral=True)
            return

        embed = discord.Embed(
            title="Claim Your AP Payout Game",
            description="Select which game you want to claim AP on. You will be prompted for the IGN next.",
            timestamp=datetime.datetime.utcnow()
        )
        view = APClaimGameView(owner_id=interaction.user.id)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @app_commands.command(name="give_ap", description="Give AP to a member (Lycan King only).")
    async def give_ap(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
        amount: app_commands.Range[int, 1, 1_000_000],
        reason: str | None = None
    ):
        if not isinstance(interaction.user, discord.Member) or not has_role_name(interaction.user, LYCAN_ROLE):
            await interaction.response.send_message("Not authorized.", ephemeral=True)
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
        if not isinstance(interaction.user, discord.Member) or not has_role_name(interaction.user, LYCAN_ROLE):
            await interaction.response.send_message("Not authorized.", ephemeral=True)
            return

        data = await load()
        rec = data.setdefault(str(member.id), {"ap": 0, "last_chat": None})
        rec["ap"] = max(0, safe_float_ap(rec.get("ap", 0)) - float(amount))
        await save(data)

        await interaction.response.send_message(
            f"Removed **{amount} AP** from {member.mention}.",
            ephemeral=True
        )

    @app_commands.command(name="transfer_ap", description="Transfer your own AP to another member (no bonuses).")
    async def transfer_ap(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
        amount: app_commands.Range[int, 1, 1_000_000],
        reason: str | None = None
    ):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Could not resolve guild/member.", ephemeral=True)
            return

        sender: discord.Member = interaction.user
        recipient: discord.Member = member

        if recipient.bot:
            await interaction.response.send_message("You cannot transfer AP to a bot.", ephemeral=True)
            return
        if recipient.id == sender.id:
            await interaction.response.send_message("You cannot transfer AP to yourself.", ephemeral=True)
            return
        if is_alt_account(sender):
            await interaction.response.send_message("Transfers are not allowed from alt accounts.", ephemeral=True)
            return

        data = await load()
        srec = data.setdefault(str(sender.id), {"ap": 0, "last_chat": None})
        rrec = data.setdefault(str(recipient.id), {"ap": 0, "last_chat": None})

        sender_ap = safe_float_ap(srec.get("ap", 0))
        amt = float(amount)

        if sender_ap < amt:
            await interaction.response.send_message(
                f"Insufficient AP. You have **{int(sender_ap)} AP**, tried to transfer **{amount} AP**.",
                ephemeral=True
            )
            return

        srec["ap"] = max(0.0, sender_ap - amt)
        rrec["ap"] = safe_float_ap(rrec.get("ap", 0)) + amt
        await save(data)

        await log_ap_distribution_embed(
            interaction.guild,
            title="AP Transfer Received",
            recipient=recipient,
            amount=amt,
            source="member transfer",
            reason=(f"From {sender.display_name}" + (f" — {reason}" if reason else "")),
            actor=sender
        )

        try:
            msg_lines = [
                f"AP transfer: {sender.mention} ➜ {recipient.mention} **{amt:.2f} AP**."
            ]
            if reason:
                msg_lines.append(f"Reason: {reason}")
            msg_lines.append(f"Sender new balance: **{safe_int_ap(srec.get('ap', 0))} AP**.")
            msg_lines.append(f"Recipient new balance: **{safe_int_ap(rrec.get('ap', 0))} AP**.")
            await log_hierarchy_ap(interaction.guild, "\n".join(msg_lines), [sender.id, recipient.id])
        except Exception:
            pass

        await interaction.response.send_message(
            f"{sender.mention} transferred **{amount} AP** to {recipient.mention}."
            + (f" Reason: {reason}" if reason else "")
        )

    @app_commands.command(
        name="ap_info",
        description="Admin report: current AP per member since last wipe (CEO / Lycan King only)."
    )
    async def ap_info(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Could not resolve guild/member.", ephemeral=True)
            return
        if not is_authorized_admin(interaction.user):
            await interaction.response.send_message("Not authorized.", ephemeral=True)
            return

        data = await load()
        meta = data.get(META_KEY, {})
        gmeta = meta.get(str(interaction.guild.id), {}) if isinstance(meta, dict) else {}
        last_wipe = gmeta.get(LAST_WIPE_KEY) or "unknown"

        groups = build_ap_rows_by_game(interaction.guild, data)
        csv_bytes = render_grouped_csv(groups)

        wow_total = sum(safe_int_ap(r[3]) for r in groups.get(GAME_WOW, []))
        eve_total = sum(safe_int_ap(r[3]) for r in groups.get(GAME_EVE, []))
        unk_total = sum(safe_int_ap(r[3]) for r in groups.get("Unclaimed / Unknown", []))
        members_count = sum(len(v) for v in groups.values())

        file = discord.File(io.BytesIO(csv_bytes), filename="ap_report.csv")

        embed = discord.Embed(
            title="AP Report (Since Last Wipe)",
            description=(
                f"Last wipe (UTC): **{last_wipe}**\n"
                f"Members included: **{members_count}**\n\n"
                f"{GAME_WOW}: **{wow_total} AP**\n"
                f"{GAME_EVE}: **{eve_total} AP**\n"
                f"Unclaimed / Unknown: **{unk_total} AP**"
            ),
            timestamp=datetime.datetime.utcnow()
        )

        await interaction.response.send_message(embed=embed, file=file, ephemeral=True)

    @app_commands.command(
        name="export_ap",
        description="Backup AP, export report CSV, then wipe AP (CEO / Lycan King only)."
    )
    async def export_ap(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Could not resolve guild/member.", ephemeral=True)
            return
        if not is_authorized_admin(interaction.user):
            await interaction.response.send_message("Not authorized.", ephemeral=True)
            return

        data = await load()

        groups = build_ap_rows_by_game(interaction.guild, data)
        csv_bytes = render_grouped_csv(groups)

        ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        backup_json_path = EXPORT_DIR / f"ap_backup_{interaction.guild.id}_{ts}.json"
        backup_csv_path = EXPORT_DIR / f"ap_export_{interaction.guild.id}_{ts}.csv"

        try:
            backup_json_path.write_text(json.dumps(data, indent=4), encoding="utf-8")
        except Exception:
            pass

        try:
            backup_csv_path.write_bytes(csv_bytes)
        except Exception:
            pass

        await wipe_ap_in_data(data)

        meta = data.setdefault(META_KEY, {})
        gmeta = meta.setdefault(str(interaction.guild.id), {})
        gmeta[LAST_WIPE_KEY] = utcnow()

        await save(data)

        try:
            await log_hierarchy_ap(
                interaction.guild,
                f"AP export + wipe executed by {interaction.user.mention}. Backup saved; AP balances reset to 0. Last wipe set to UTC {gmeta[LAST_WIPE_KEY]}.",
                mention_ids=[interaction.user.id],
            )
        except Exception:
            pass

        file = discord.File(io.BytesIO(csv_bytes), filename=f"ap_export_{interaction.guild.id}_{ts}.csv")
        await interaction.response.send_message(
            content="Export complete. CSV attached. AP balances have been wiped.",
            file=file,
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
