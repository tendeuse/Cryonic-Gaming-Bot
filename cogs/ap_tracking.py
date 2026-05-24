# cogs/ap_tracking.py
#
# Railway-volume persistent AP system + leadership bonus redistribution
# - Persists to /data by default (Railway volume). Override with env var PERSIST_ROOT.
# - Keeps ALL commands/reports from your posted script:
#   /apclaim, /give_ap, /remove_ap, /transfer_ap, /ap_info, /export_ap, /point
# - /ap_audit       — full audit trail per player since last AP wipe
# - /ap_list_backups   — list available backup files for this server
# - /ap_restore_backup — restore AP from a backup and re-send the CSV report
# - Maintains:
#     AP check channel with persistent button
#     Join bonus + distribution embed log channel
#     Hierarchy log channel
#     Voice + chat AP loops with bonuses
#
# BONUS POLICY:
# - CEO(s): each gets +10% of base (NOT divided)
# - Directors: total pool = 10% of base, split evenly among Directors (excluding CEOs)
# - Leadership bonus triggers only when earner has SECURITY_ROLE
# - Unit tier bonuses are disabled in this version
#
# PERM POLICY:
# - This cog WILL create required channels if missing
# - It WILL NOT change @everyone permissions on an existing channel (ap-check)
# - It WILL ensure the bot has the permissions it needs on ap-check
#
# EVENT PRESENCE BOOSTS:
# - Reads /data/ap_boosts.json (via BOOSTS_FILE)
# - When a participant earns AP, any active boost entries grant an additional % of base_amount
#   to a beneficiary (event creator) for the next 24 hours.
# - Boosts DO NOT stack: duplicate beneficiaries are deduped; only one award per beneficiary applies.
#
# AUDIT LOG:
# - Every AP change is appended to each user's "audit" list inside ap_data.json.
# - Audit lists are cleared when AP is wiped, so they always reflect the current cycle.
# - /ap_audit [member] shows the log. Admins can view anyone; members see only themselves.
#   If the log exceeds 25 entries it is also attached as a CSV file.

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

DATA_FILE   = PERSIST_ROOT / "ap_data.json"
EXPORT_DIR  = PERSIST_ROOT / "ap_exports"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

# Hierarchy data (owned by arc_hierarchy.py) - still present for compatibility
HIERARCHY_FILE    = PERSIST_ROOT / "arc_hierarchy.json"
HIERARCHY_LOG_CH  = "arc-hierarchy-log"

# Event Presence Boost File (used by event_creator.py)
BOOSTS_FILE = PERSIST_ROOT / "ap_boosts.json"

# =====================
# CONFIG
# =====================
VOICE_INTERVAL  = 180     # 3 minutes
VOICE_AP        = 1
CHAT_INTERVAL   = 1800    # 30 minutes
CHAT_AP         = 15
MIN_ACCOUNT_AGE_DAYS = 14  # Alt-account mitigation

# Roles that bypass the account-age block and trigger retroactive AP catch-up.
# These are matched by role NAME exactly as they appear in Discord.
BYPASS_ROLE_NAMES = {"1", "2", "3", "4", "5", "6"}

LYCAN_ROLE           = "Lycan King"
AP_CHECK_CHANNEL     = "ap-check"
AP_CHECK_EMBED_TITLE = "AP Balance"
AP_CHECK_EMBED_TEXT  = "Click check ap to see your point balance"
AP_CHECK_BUTTON_LABEL = "Check Balance"

META_KEY              = "_meta"
AP_CHECK_MESSAGE_ID_KEY = "ap_check_message_id"
LAST_WIPE_KEY         = "last_wipe_utc"

# ARC roles (bonus eligibility + admin permissions)
CEO_ROLE        = "ARC Security Corporation Leader"
DIRECTORS_ROLE  = "ARC Security Administration Council"
SECURITY_ROLE   = "ARC Security"
SUBSIDIZED_ROLE = "ARC Subsidized"

# Join bonus
JOIN_BONUS_AP  = 100
JOIN_BONUS_KEY = "join_bonus_awarded"

# AP distribution log channel
AP_DISTRIBUTION_LOG_CH = "member-join-logs-points-distribute"

# Claim keys
CLAIM_IGN_KEY  = "ign"
CLAIM_GAME_KEY = "game"

# Game rates
GAME_EVE = "EVE Online"
GAME_WOW = "World of Warcraft"
EVE_ISK_PER_AP = 100_000
WOW_GOLD_PER_AP = 10

# ARC ranks (kept for compatibility; not used for bonuses in this version)
RANK_SECURITY  = "security"
RANK_OFFICER   = "officer"
RANK_COMMANDER = "commander"
RANK_GENERAL   = "general"
RANK_DIRECTOR  = "director"
RANK_ORDER = [RANK_SECURITY, RANK_OFFICER, RANK_COMMANDER, RANK_GENERAL, RANK_DIRECTOR]
RANK_INDEX = {r: i for i, r in enumerate(RANK_ORDER)}

# Audit log key (per-user record)
AUDIT_KEY = "audit"

# Maximum entries shown inline in the embed before a file is also attached
AUDIT_EMBED_MAX = 25

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

def has_bypass_role(member: discord.Member) -> bool:
    """Return True if the member holds any of the account-age bypass roles."""
    return any(r.name in BYPASS_ROLE_NAMES for r in member.roles)

def earns_leadership_bonus(member: discord.Member) -> bool:
    """Return True if this earner's AP should trigger CEO / Director bonuses."""
    return has_role_name(member, SECURITY_ROLE) or has_role_name(member, SUBSIDIZED_ROLE)

def is_alt_account(member: discord.Member) -> bool:
    """
    Return True if the member should be treated as a too-young alt account.
    Members who hold a bypass role (names "1"–"6") are always allowed through,
    even if their Discord account is less than MIN_ACCOUNT_AGE_DAYS old.
    """
    if has_bypass_role(member):
        return False
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
    # Not used for bonuses in this version; kept so the file dependency doesn't break anything.
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
    embed.add_field(name="Recipient",  value=f"{recipient.mention} ({recipient.id})", inline=False)
    embed.add_field(name="Amount",     value=f"**+{amount:.2f} AP**",                inline=True)
    embed.add_field(name="Source",     value=source,                                  inline=True)
    if actor:
        embed.add_field(name="Issued By", value=f"{actor.mention} ({actor.id})", inline=False)
    if reason:
        embed.add_field(name="Reason", value=reason[:1024], inline=False)
    try:
        await ch.send(embed=embed)
    except (discord.Forbidden, discord.HTTPException):
        pass

# -------------------------
# Event Presence Boost Logic
# -------------------------
def _load_boosts_file() -> Dict[str, Any]:
    """
    File schema:
    {
      "participants": {
        "<participant_id>": [
          {"beneficiary": <creator_id>, "percent": 0.10, "expires": <unix>, "event_id": "..."}
        ]
      }
    }
    """
    if not BOOSTS_FILE.exists():
        return {"participants": {}}
    try:
        txt = BOOSTS_FILE.read_text(encoding="utf-8").strip()
        if not txt:
            return {"participants": {}}
        data = json.loads(txt)
        if not isinstance(data, dict):
            return {"participants": {}}
        data.setdefault("participants", {})
        if not isinstance(data["participants"], dict):
            data["participants"] = {}
        return data
    except Exception:
        return {"participants": {}}

def _save_boosts_file(data: Dict[str, Any]) -> None:
    try:
        BOOSTS_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = BOOSTS_FILE.with_suffix(BOOSTS_FILE.suffix + ".tmp")
        tmp.write_text(json.dumps(data, indent=4), encoding="utf-8")
        tmp.replace(BOOSTS_FILE)
    except Exception:
        pass

def _apply_participant_boosts(
    boosts_data: Dict[str, Any],
    *,
    participant_id: int,
    base_amount: float,
) -> Tuple[List[Tuple[int, float, str]], bool]:
    """
    Returns:
      awards:  [(beneficiary_id, bonus_amount, event_id), ...]
      changed: whether boosts_data should be saved (expired/invalid pruned, deduped)

    Defensive no-stacking:
      - If multiple active entries exist for same beneficiary, only one award is applied.
    """
    changed = False
    now  = int(datetime.datetime.utcnow().timestamp())
    participants = boosts_data.get("participants", {})
    if not isinstance(participants, dict):
        return ([], False)
    key     = str(participant_id)
    entries = participants.get(key, [])
    if not isinstance(entries, list) or not entries:
        return ([], False)

    best_by_beneficiary: Dict[int, Dict[str, Any]] = {}

    for entry in entries:
        if not isinstance(entry, dict):
            changed = True
            continue
        expires    = int(entry.get("expires", 0) or 0)
        if expires <= now:
            changed = True
            continue
        beneficiary = entry.get("beneficiary")
        percent     = float(entry.get("percent", 0) or 0)
        event_id    = str(entry.get("event_id", "") or "")
        if not isinstance(beneficiary, int) or percent <= 0:
            changed = True
            continue
        prev = best_by_beneficiary.get(beneficiary)
        if prev is None or int(prev.get("expires", 0) or 0) < expires:
            best_by_beneficiary[beneficiary] = {
                "beneficiary": beneficiary,
                "percent": percent,
                "expires": expires,
                "event_id": event_id,
            }
        else:
            changed = True

    kept = list(best_by_beneficiary.values())
    if len(kept) != len(entries):
        changed = True

    participants[key]          = kept
    boosts_data["participants"] = participants

    awards: List[Tuple[int, float, str]] = []
    if base_amount > 0:
        for entry in kept:
            beneficiary = int(entry["beneficiary"])
            percent     = float(entry.get("percent", 0) or 0)
            event_id    = str(entry.get("event_id", "") or "")
            bonus = float(base_amount) * percent
            if bonus > 0:
                awards.append((beneficiary, bonus, event_id))

    return (awards, changed)

# -------------------------
# Audit Log Helpers
# -------------------------
def append_audit(
    data: Dict[str, Any],
    member_id: int,
    delta: float,
    source: str,
    *,
    reason: Optional[str] = None,
    actor_id: Optional[int] = None,
) -> None:
    """
    Append one AP transaction entry to the user's audit list.
    Called *before* save() so everything lands in the same write.

    Entry schema:
        {
            "ts":       "<ISO-8601 UTC>",
            "delta":    <float, positive = gain, negative = loss>,
            "source":   "<string>",
            "reason":   "<string or absent>",
            "actor_id": <int or absent>
        }
    """
    rec   = data.setdefault(str(member_id), {"ap": 0, "last_chat": None})
    audit = rec.setdefault(AUDIT_KEY, [])
    entry: Dict[str, Any] = {
        "ts":     utcnow(),
        "delta":  round(delta, 4),
        "source": source,
    }
    if reason:
        entry["reason"] = reason
    if actor_id is not None:
        entry["actor_id"] = actor_id
    audit.append(entry)


def build_audit_csv(entries: List[Dict[str, Any]], member_display: str) -> bytes:
    """Render a user's audit entries to a UTF-8 CSV."""
    out    = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["#", "Timestamp (UTC)", "Delta AP", "Source", "Reason", "Actor ID"])
    for i, e in enumerate(entries, 1):
        writer.writerow([
            i,
            e.get("ts", ""),
            e.get("delta", ""),
            e.get("source", ""),
            e.get("reason", ""),
            e.get("actor_id", ""),
        ])
    return out.getvalue().encode("utf-8")


# -------------------------
# Bonus Logic
# -------------------------
def ceo_ids(guild: discord.Guild) -> List[int]:
    ids: List[int] = []
    for m in guild.members:
        if isinstance(m, discord.Member) and has_role_name(m, CEO_ROLE):
            ids.append(m.id)
    return ids

def director_ids(guild: discord.Guild) -> List[int]:
    ids: List[int] = []
    for m in guild.members:
        if isinstance(m, discord.Member) and has_role_name(m, DIRECTORS_ROLE):
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

    _ = load_hierarchy()   # kept for compatibility / future use
    data = await load()

    actor_id = actor.id if actor else None

    # 1) Base AP + audit
    await add_ap_raw(data, earner.id, base_amount)
    append_audit(data, earner.id, base_amount, source, reason=reason, actor_id=actor_id)

    mention_ids: List[int] = [earner.id]

    # 2) Leadership bonuses
    ceo_bonus_each    = 0.0
    ceo_targets:      List[int] = []
    directors_pool    = 0.0
    directors_each    = 0.0
    directors_targets: List[int] = []

    if earns_leadership_bonus(earner):
        ceo_targets   = ceo_ids(guild)
        all_directors = director_ids(guild)
        ceo_set       = set(ceo_targets)
        directors_targets = [uid for uid in all_directors if uid not in ceo_set]

        # CEO: 10% EACH, not divided
        if ceo_targets:
            ceo_bonus_each = base_amount * 0.10
            ceo_source     = f"leadership bonus (CEO) from {earner.display_name} via {source}"
            for uid in ceo_targets:
                await add_ap_raw(data, uid, ceo_bonus_each)
                append_audit(data, uid, ceo_bonus_each, ceo_source, reason=reason, actor_id=earner.id)
            mention_ids.extend(ceo_targets)

        # Directors: TOTAL pool 10% split between them
        if directors_targets:
            directors_pool  = base_amount * 0.10
            directors_each  = directors_pool / float(len(directors_targets))
            dir_source      = f"leadership bonus (Director) from {earner.display_name} via {source}"
            for uid in directors_targets:
                await add_ap_raw(data, uid, directors_each)
                append_audit(data, uid, directors_each, dir_source, reason=reason, actor_id=earner.id)
            mention_ids.extend(directors_targets)

    # 3) Event presence boosts (participant -> creator % for 24h)
    boosts = _load_boosts_file()
    boost_awards, boosts_changed = _apply_participant_boosts(
        boosts,
        participant_id=earner.id,
        base_amount=float(base_amount),
    )
    if boost_awards:
        for beneficiary_id, bonus_amount, event_id in boost_awards:
            await add_ap_raw(data, beneficiary_id, float(bonus_amount))
            append_audit(
                data, beneficiary_id, float(bonus_amount),
                f"event boost (event {event_id}) from participant {earner.display_name}",
                actor_id=earner.id,
            )
            mention_ids.append(beneficiary_id)
        if boosts_changed:
            _save_boosts_file(boosts)

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
        lines.append("Unit bonuses: disabled (leadership-only bonus mode).")
        if not earns_leadership_bonus(earner):
            lines.append(f"Leadership bonus: none (earner missing {SECURITY_ROLE} or {SUBSIDIZED_ROLE}).")
        else:
            if ceo_targets and ceo_bonus_each > 0:
                lines.append(
                    f"CEO bonus: each CEO received **+{ceo_bonus_each:.2f} AP** (10% of base; not divided)."
                )
            else:
                lines.append("CEO bonus: none (no CEO found).")
            if directors_targets and directors_pool > 0:
                lines.append(
                    f"Directors bonus: pool **+{directors_pool:.2f} AP** (10% of base) split across "
                    f"{len(directors_targets)} Director(s) (**+{directors_each:.2f} each**)."
                )
            else:
                lines.append("Directors bonus: none (no eligible Directors found).")

        if boost_awards:
            for beneficiary_id, bonus_amount, event_id in boost_awards:
                bmem = guild.get_member(beneficiary_id)
                who  = bmem.mention if bmem else f"{beneficiary_id}"
                lines.append(
                    f"Event boost: {who} received **+{bonus_amount:.2f} AP** "
                    f"(from participant presence confirmation; event {event_id}; non-stacking; time-extended only)."
                )

        await log_hierarchy_ap(guild, "\n".join(lines), mention_ids)

# -------------------------
# Reporting / Export Helpers
# -------------------------
def iter_member_records(data: Dict[str, Any]) -> List[Tuple[int, Dict[str, Any]]]:
    """Returns list of (member_id, record) for all non-meta records."""
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
    Columns: Discord Name, IGN, Game, AP, Payout Amount, Payout Currency
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
        ap_int   = safe_int_ap(rec.get("ap", 0))
        ign      = (rec.get(CLAIM_IGN_KEY)  or "").strip()
        game     = (rec.get(CLAIM_GAME_KEY) or "").strip()
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
        writer.writerow([])

    write_section(GAME_WOW,             groups.get(GAME_WOW,             []))
    write_section(GAME_EVE,             groups.get(GAME_EVE,             []))
    write_section("Unclaimed / Unknown", groups.get("Unclaimed / Unknown", []))
    return output.getvalue().encode("utf-8")

async def wipe_ap_in_data(data: Dict[str, Any]) -> None:
    """
    Wipes AP balances for all users while preserving:
      - join bonus flag (JOIN_BONUS_KEY)
      - ign/game claim fields
    Also clears last_chat timestamps to avoid immediate chat awards,
    and clears the per-user audit log (logs are "since last reset").
    """
    for _, rec in iter_member_records(data):
        rec["ap"]        = 0
        rec["last_chat"] = None
        rec[AUDIT_KEY]   = []   # clear audit so next cycle starts fresh

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
        # Defer immediately to prevent 10062 "Unknown interaction" errors when
        # the async load() call takes longer than Discord's 3-second ack window.
        try:
            await interaction.response.defer(ephemeral=True)
        except discord.NotFound:
            print(
                f"[APCheckView] Interaction expired (10062) for user "
                f"{interaction.user} — likely a reconnect race. Ignoring."
            )
            return

        if not isinstance(interaction.user, discord.Member):
            await interaction.followup.send("Could not resolve member.", ephemeral=True)
            return
        data = await load()
        ap = safe_int_ap(data.get(str(interaction.user.id), {}).get("ap", 0))
        await interaction.followup.send(f"You have **{ap} AP**.", ephemeral=True)

# -------------------------
# AP Claim Flow (Buttons -> Modal)
# -------------------------
class APClaimIGNModal(discord.ui.Modal):
    def __init__(self, *, game_value: str):
        super().__init__(title="Claim AP - Enter IGN")
        self.game_value = game_value
        self.ign_input  = discord.ui.TextInput(
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

        await interaction.response.defer(ephemeral=True)

        data = await load()
        rec  = data.setdefault(str(interaction.user.id), {"ap": 0, "last_chat": None})
        rec[CLAIM_GAME_KEY] = self.game_value
        rec[CLAIM_IGN_KEY]  = ign
        await save(data)
        embed = discord.Embed(
            title=       "AP Claim Saved",
            description= f"Saved your claim:\n**Game:** {self.game_value}\n**IGN:** {ign}",
            timestamp=   datetime.datetime.utcnow()
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

class APClaimGameView(discord.ui.View):
    def __init__(self, owner_id: int):
        super().__init__(timeout=120)
        self.owner_id = owner_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
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
# Batch-award helper (no I/O — caller owns load/save)
# -------------------------
def _apply_ap_to_data(
    data: Dict[str, Any],
    earner: discord.Member,
    base_amount: float,
    source: str,
    reason: Optional[str],
    ceo_id_list: List[int],
    director_id_list: List[int],
    boosts: Dict[str, Any],
) -> Tuple[List[int], List[Tuple[int, float, str]], bool]:
    """
    Apply one AP award (base + leadership bonuses + event boosts) directly to
    *data* without any file I/O.  The caller is responsible for a single
    load() before the batch and a single save() afterwards.

    Returns:
        mention_ids   – list of member IDs involved in the transaction
        boost_awards  – list of (beneficiary_id, bonus_amount, event_id)
        boosts_changed – whether *boosts* was mutated (caller should save it)
    """
    if base_amount <= 0:
        return ([], [], False)

    rec = data.setdefault(str(earner.id), {"ap": 0, "last_chat": None})
    rec["ap"] = safe_float_ap(rec.get("ap", 0)) + float(base_amount)
    append_audit(data, earner.id, base_amount, source, reason=reason)

    mention_ids: List[int] = [earner.id]

    if earns_leadership_bonus(earner):
        ceo_set            = set(ceo_id_list)
        eligible_directors = [uid for uid in director_id_list if uid not in ceo_set]

        # CEO: +10% each (not divided)
        if ceo_id_list:
            ceo_bonus = base_amount * 0.10
            ceo_src   = f"leadership bonus (CEO) from {earner.display_name} via {source}"
            for uid in ceo_id_list:
                r = data.setdefault(str(uid), {"ap": 0, "last_chat": None})
                r["ap"] = safe_float_ap(r.get("ap", 0)) + ceo_bonus
                append_audit(data, uid, ceo_bonus, ceo_src, reason=reason, actor_id=earner.id)
            mention_ids.extend(ceo_id_list)

        # Directors: 10% pool split evenly
        if eligible_directors:
            dir_each = (base_amount * 0.10) / float(len(eligible_directors))
            dir_src  = f"leadership bonus (Director) from {earner.display_name} via {source}"
            for uid in eligible_directors:
                r = data.setdefault(str(uid), {"ap": 0, "last_chat": None})
                r["ap"] = safe_float_ap(r.get("ap", 0)) + dir_each
                append_audit(data, uid, dir_each, dir_src, reason=reason, actor_id=earner.id)
            mention_ids.extend(eligible_directors)

    # Event presence boosts
    boost_awards, boosts_changed = _apply_participant_boosts(
        boosts,
        participant_id=earner.id,
        base_amount=float(base_amount),
    )
    for beneficiary_id, bonus_amount, event_id in boost_awards:
        r = data.setdefault(str(beneficiary_id), {"ap": 0, "last_chat": None})
        r["ap"] = safe_float_ap(r.get("ap", 0)) + float(bonus_amount)
        append_audit(
            data, beneficiary_id, float(bonus_amount),
            f"event boost (event {event_id}) from participant {earner.display_name}",
            actor_id=earner.id,
        )
        mention_ids.append(beneficiary_id)

    return (mention_ids, boost_awards, boosts_changed)


# -------------------------
# Cog
# -------------------------
class APTracking(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # Cache of {guild_id: (ceo_id_list, director_id_list)}.
        # Built on ready and refreshed whenever a member's roles change.
        # Avoids iterating all guild members on every voice/chat tick.
        self._leadership_cache: Dict[int, Tuple[List[int], List[int]]] = {}
        if not self.voice_loop.is_running():
            self.voice_loop.start()
        if not self.chat_loop.is_running():
            self.chat_loop.start()

    # -------------------------
    # Leadership ID cache helpers
    # -------------------------
    def _refresh_leadership_cache(self, guild: discord.Guild) -> None:
        """Recompute CEO / Director ID lists for *guild* and store in cache."""
        ceos = [
            m.id for m in guild.members
            if isinstance(m, discord.Member) and has_role_name(m, CEO_ROLE)
        ]
        dirs = [
            m.id for m in guild.members
            if isinstance(m, discord.Member) and has_role_name(m, DIRECTORS_ROLE)
        ]
        self._leadership_cache[guild.id] = (ceos, dirs)

    def _get_leadership_ids(self, guild: discord.Guild) -> Tuple[List[int], List[int]]:
        """Return (ceo_ids, director_ids) for *guild*, populating cache on first call."""
        if guild.id not in self._leadership_cache:
            self._refresh_leadership_cache(guild)
        return self._leadership_cache[guild.id]

    async def _patch_channel_overwrites_preserve_everyone(
        self,
        channel: discord.TextChannel,
        guild: discord.Guild,
        *,
        bot_overwrite: discord.PermissionOverwrite,
    ) -> None:
        """
        Updates overwrites WITHOUT touching @everyone's overwrite entry.
        - Preserves current @everyone overwrite exactly as-is (including if absent).
        - Ensures the bot has required overwrite.
        """
        try:
            current  = dict(channel.overwrites)
            everyone = guild.default_role
            everyone_entry = current.get(everyone, None)
            me = guild.me or (guild.get_member(self.bot.user.id) if self.bot.user else None)
            if me:
                current[me] = bot_overwrite
            if everyone_entry is not None:
                current[everyone] = everyone_entry
            else:
                current.pop(everyone, None)
            await channel.edit(overwrites=current)
        except discord.Forbidden:
            pass
        except Exception:
            pass

    async def ensure_ap_check_message(self, guild: discord.Guild):
        channel      = discord.utils.get(guild.text_channels, name=AP_CHECK_CHANNEL)
        bot_overwrite = discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            manage_messages=True,
            read_message_history=True,
        )

        if not channel:
            try:
                overwrites = {
                    guild.default_role: discord.PermissionOverwrite(
                        view_channel=True,
                        send_messages=False
                    )
                }
                me = guild.me or (guild.get_member(self.bot.user.id) if self.bot.user else None)
                if me:
                    overwrites[me] = bot_overwrite
                channel = await guild.create_text_channel(AP_CHECK_CHANNEL, overwrites=overwrites)
            except discord.Forbidden:
                return
            except Exception:
                return
        else:
            await self._patch_channel_overwrites_preserve_everyone(
                channel, guild, bot_overwrite=bot_overwrite,
            )

        data  = await load()
        meta  = data.setdefault(META_KEY, {})
        gmeta = meta.setdefault(str(guild.id), {})
        gmeta.setdefault(LAST_WIPE_KEY, gmeta.get(LAST_WIPE_KEY) or utcnow())

        embed = discord.Embed(title=AP_CHECK_EMBED_TITLE, description=AP_CHECK_EMBED_TEXT)
        view  = APCheckView()

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

    # -------------------------
    # Retroactive AP (bypass-role grant)
    # -------------------------
    async def _count_active_chat_intervals(self, member: discord.Member) -> int:
        """
        Scan message history across every readable text channel to find how many
        distinct CHAT_INTERVAL (30-min) windows the member sent at least one
        message in, from their join date up to now (capped at MIN_ACCOUNT_AGE_DAYS).

        Returns the number of active windows (each worth CHAT_AP on award).
        """
        if not member.joined_at:
            return 0

        joined     = member.joined_at.replace(tzinfo=None)
        now        = datetime.datetime.utcnow()
        scan_start = max(joined, now - datetime.timedelta(days=MIN_ACCOUNT_AGE_DAYS))

        active_buckets: set = set()

        for channel in member.guild.text_channels:
            try:
                async for msg in channel.history(after=scan_start, before=now, limit=None):
                    if msg.author.id == member.id:
                        ts     = msg.created_at.replace(tzinfo=None)
                        epoch  = (ts - datetime.datetime(1970, 1, 1)).total_seconds()
                        bucket = int(epoch // CHAT_INTERVAL)
                        active_buckets.add(bucket)
            except (discord.Forbidden, discord.HTTPException):
                continue
            await asyncio.sleep(0)   # yield between channels

        return len(active_buckets)

    async def _award_retroactive_ap(self, member: discord.Member) -> None:
        """
        Called the first time a bypass role (name "1"–"6") is assigned to a member
        who was previously blocked by the account-age check.

        Awards:
          • Join bonus (100 AP) — if it was never granted.
          • Chat AP catch-up   — one CHAT_AP award for every CHAT_INTERVAL window
            that has elapsed since the member joined the server, capped at
            MIN_ACCOUNT_AGE_DAYS worth of intervals (the maximum time they could
            have been blocked).

        Voice AP is intentionally omitted — there is no record of past voice activity.
        Leadership bonuses are NOT cascaded on this retroactive award to avoid large
        unexpected payouts to CEO / Director accounts.

        All awards are written to the per-user audit log and to both log channels.
        """
        if not isinstance(member, discord.Member) or not member.guild:
            return

        # Only award retroactive AP if the member was actually blocked.
        # If their Discord account is already MIN_ACCOUNT_AGE_DAYS or older,
        # they were never affected by the alt-account check and earned AP normally.
        account_age_days = (
            datetime.datetime.utcnow() - member.created_at.replace(tzinfo=None)
        ).days
        if account_age_days >= MIN_ACCOUNT_AGE_DAYS:
            return

        data = await load()
        rec  = data.setdefault(str(member.id), {"ap": 0, "last_chat": None})

        retroactive_total = 0.0
        reasons: List[str] = []

        # ── 1. Join bonus only ────────────────────────────────────────────────
        if not rec.get(JOIN_BONUS_KEY):
            await add_ap_raw(data, member.id, float(JOIN_BONUS_AP))
            append_audit(
                data, member.id, float(JOIN_BONUS_AP),
                "join bonus (retroactive — bypass role granted)",
            )
            rec[JOIN_BONUS_KEY] = True
            retroactive_total  += JOIN_BONUS_AP
            reasons.append(f"join bonus: +{JOIN_BONUS_AP} AP")

        # ── 2. Chat AP — based on actual messages sent while blocked ──────────
        intervals = await self._count_active_chat_intervals(member)
        if intervals > 0:
            chat_retro = float(intervals * CHAT_AP)
            await add_ap_raw(data, member.id, chat_retro)
            append_audit(
                data, member.id, chat_retro,
                "chat AP (retroactive — bypass role granted)",
                reason=f"{intervals} active 30-min window(s) found in message history",
            )
            retroactive_total += chat_retro
            reasons.append(
                f"chat catch-up ({intervals} active window(s) × {CHAT_AP} AP): +{int(chat_retro)} AP"
            )

        if retroactive_total <= 0:
            # Nothing to award (e.g. join bonus already given, joined seconds ago)
            await save(data)
            return

        await save(data)

        # ── 3. Distribution embed ─────────────────────────────────────────────
        try:
            await log_ap_distribution_embed(
                member.guild,
                title="Retroactive AP Awarded — Bypass Role Granted",
                recipient=member,
                amount=retroactive_total,
                source="bypass_role_grant",
                reason="; ".join(reasons),
            )
        except Exception:
            pass

        # ── 4. Hierarchy log ──────────────────────────────────────────────────
        try:
            bullet_lines = "\n".join(f"  • {r}" for r in reasons)
            await log_hierarchy_ap(
                member.guild,
                (
                    f"Bypass role granted: {member.mention} received retroactive "
                    f"**+{int(retroactive_total)} AP** (account-age block lifted).\n"
                    f"{bullet_lines}"
                ),
                mention_ids=[member.id],
            )
        except Exception:
            pass

    # -------------------------
    # Role / event listeners
    # -------------------------
    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """Invalidate the leadership cache whenever a member's roles change.
        Also triggers retroactive AP catch-up the first time a bypass role is assigned."""
        if before.roles != after.roles and after.guild:
            self._refresh_leadership_cache(after.guild)

            # Detect first bypass-role assignment:
            #   - the member did NOT have a bypass role before
            #   - at least one bypass role is newly added
            before_had_bypass = has_bypass_role(before)
            after_has_bypass  = has_bypass_role(after)
            if not before_had_bypass and after_has_bypass:
                await self._award_retroactive_ap(after)

    @commands.Cog.listener()
    async def on_ready(self):
        self.bot.add_view(APCheckView())
        for g in self.bot.guilds:
            self._refresh_leadership_cache(g)
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
        rec  = data.setdefault(str(member.id), {"ap": 0, "last_chat": None})
        if rec.get(JOIN_BONUS_KEY) is True:
            return
        await add_ap_raw(data, member.id, float(JOIN_BONUS_AP))
        append_audit(data, member.id, float(JOIN_BONUS_AP), "join bonus")
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
        rec  = data.setdefault(str(message.author.id), {"ap": 0, "last_chat": None})
        rec["last_chat"] = utcnow()
        await save(data)

    @tasks.loop(seconds=VOICE_INTERVAL)
    async def voice_loop(self):
        # Single load/save per tick; yield to the event loop between members
        # so the Discord heartbeat is never starved.
        boosts: Dict[str, Any] = _load_boosts_file()
        boosts_changed = False
        data = await load()

        for guild in self.bot.guilds:
            ceos, dirs = self._get_leadership_ids(guild)
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
                    _apply_ap_to_data(
                        data, m, float(VOICE_AP), "voice", None,
                        ceos, dirs, boosts,
                    )
                    await asyncio.sleep(0)   # yield so heartbeat isn't blocked

        await save(data)
        if boosts_changed:
            _save_boosts_file(boosts)

    @tasks.loop(seconds=CHAT_INTERVAL)
    async def chat_loop(self):
        now    = datetime.datetime.utcnow()
        boosts: Dict[str, Any] = _load_boosts_file()
        boosts_changed = False
        data   = await load()

        for guild in self.bot.guilds:
            ceos, dirs = self._get_leadership_ids(guild)
            for i, m in enumerate(guild.members):
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
                    _, _, changed = _apply_ap_to_data(
                        data, m, float(CHAT_AP), "chat", None,
                        ceos, dirs, boosts,
                    )
                    if changed:
                        boosts_changed = True
                # Yield every 50 members to stay cooperative
                if i % 50 == 0:
                    await asyncio.sleep(0)

        await save(data)
        if boosts_changed:
            _save_boosts_file(boosts)

    # -------------------------
    # Slash Commands
    # -------------------------

    @app_commands.command(name="apclaim", description="Claim your IGN and game for AP exports.")
    async def apclaim(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Could not resolve guild/member.", ephemeral=True)
            return
        embed = discord.Embed(
            title=       "Claim Your AP Payout Game",
            description= "Select which game you want to claim AP on. You will be prompted for the IGN next.",
            timestamp=   datetime.datetime.utcnow()
        )
        view = APClaimGameView(owner_id=interaction.user.id)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @app_commands.command(name="give_ap", description="Give AP to a member (CEO / Lycan King only).")
    async def give_ap(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
        amount: app_commands.Range[int, 1, 1_000_000],
        reason: str | None = None
    ):
        if not isinstance(interaction.user, discord.Member) or not is_authorized_admin(interaction.user):
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

    @app_commands.command(name="remove_ap", description="Remove AP from a member (CEO / Lycan King only).")
    async def remove_ap(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
        amount: app_commands.Range[int, 1, 1_000_000],
        reason: str | None = None
    ):
        if not isinstance(interaction.user, discord.Member) or not is_authorized_admin(interaction.user):
            await interaction.response.send_message("Not authorized.", ephemeral=True)
            return
        data = await load()
        rec  = data.setdefault(str(member.id), {"ap": 0, "last_chat": None})
        rec["ap"] = max(0, safe_float_ap(rec.get("ap", 0)) - float(amount))
        append_audit(
            data, member.id, -float(amount), "manual remove_ap",
            reason=reason, actor_id=interaction.user.id
        )
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
        sender:    discord.Member = interaction.user
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

        data  = await load()
        srec  = data.setdefault(str(sender.id),    {"ap": 0, "last_chat": None})
        rrec  = data.setdefault(str(recipient.id), {"ap": 0, "last_chat": None})
        sender_ap = safe_float_ap(srec.get("ap", 0))
        amt       = float(amount)

        if sender_ap < amt:
            await interaction.response.send_message(
                f"Insufficient AP. You have **{int(sender_ap)} AP**, tried to transfer **{amount} AP**.",
                ephemeral=True
            )
            return

        srec["ap"] = max(0.0, sender_ap - amt)
        rrec["ap"] = safe_float_ap(rrec.get("ap", 0)) + amt

        append_audit(
            data, sender.id, -amt, "transfer_ap (sent)",
            reason=reason, actor_id=sender.id
        )
        append_audit(
            data, recipient.id, amt, f"transfer_ap (received from {sender.display_name})",
            reason=reason, actor_id=sender.id
        )

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
            msg_lines = [f"AP transfer: {sender.mention} ➜ {recipient.mention} **{amt:.2f} AP**."]
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

        data   = await load()
        meta   = data.get(META_KEY, {})
        gmeta  = meta.get(str(interaction.guild.id), {}) if isinstance(meta, dict) else {}
        last_wipe = gmeta.get(LAST_WIPE_KEY) or "unknown"

        groups    = build_ap_rows_by_game(interaction.guild, data)
        csv_bytes = render_grouped_csv(groups)

        wow_total = sum(safe_int_ap(r[3]) for r in groups.get(GAME_WOW, []))
        eve_total = sum(safe_int_ap(r[3]) for r in groups.get(GAME_EVE, []))
        unk_total = sum(safe_int_ap(r[3]) for r in groups.get("Unclaimed / Unknown", []))
        members_count = sum(len(v) for v in groups.values())

        file  = discord.File(io.BytesIO(csv_bytes), filename="ap_report.csv")
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

        data      = await load()
        groups    = build_ap_rows_by_game(interaction.guild, data)
        csv_bytes = render_grouped_csv(groups)
        ts        = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        backup_json_path = EXPORT_DIR / f"ap_backup_{interaction.guild.id}_{ts}.json"
        backup_csv_path  = EXPORT_DIR / f"ap_export_{interaction.guild.id}_{ts}.csv"

        try:
            backup_json_path.write_text(json.dumps(data, indent=4), encoding="utf-8")
        except Exception:
            pass
        try:
            backup_csv_path.write_bytes(csv_bytes)
        except Exception:
            pass

        await wipe_ap_in_data(data)
        meta  = data.setdefault(META_KEY, {})
        gmeta = meta.setdefault(str(interaction.guild.id), {})
        gmeta[LAST_WIPE_KEY] = utcnow()
        await save(data)

        try:
            await log_hierarchy_ap(
                interaction.guild,
                f"AP export + wipe executed by {interaction.user.mention}. "
                f"Backup saved; AP balances reset to 0. Last wipe set to UTC {gmeta[LAST_WIPE_KEY]}.",
                mention_ids=[interaction.user.id],
            )
        except Exception:
            pass

        file = discord.File(
            io.BytesIO(csv_bytes),
            filename=f"ap_export_{interaction.guild.id}_{ts}.csv"
        )
        await interaction.response.send_message(
            content="Export complete. CSV attached. AP balances have been wiped.",
            file=file,
            ephemeral=True
        )

    @app_commands.command(name="point", description="Check AP for yourself or a member (public).")
    async def point(self, interaction: discord.Interaction, member: discord.Member | None = None):
        target = member or interaction.user
        data   = await load()
        ap     = safe_int_ap(data.get(str(target.id), {}).get("ap", 0))
        await interaction.response.send_message(f"{target.mention} has **{ap} AP**.")

    @app_commands.command(
        name="ap_audit",
        description="Show the AP transaction log for a player since the last reset."
    )
    @app_commands.describe(
        member="The member whose audit log you want to view (leave blank for yourself)."
    )
    async def ap_audit(
        self,
        interaction: discord.Interaction,
        member: discord.Member | None = None
    ):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Could not resolve guild/member.", ephemeral=True)
            return

        caller   = interaction.user
        is_admin = is_authorized_admin(caller)

        if member is None:
            target = caller
        elif member.id != caller.id and not is_admin:
            await interaction.response.send_message(
                "You can only view your own audit log.", ephemeral=True
            )
            return
        else:
            target = member

        data    = await load()
        rec     = data.get(str(target.id), {})
        entries = rec.get(AUDIT_KEY, []) if isinstance(rec, dict) else []

        if not entries:
            await interaction.response.send_message(
                f"No audit entries found for {target.mention} since the last AP reset.",
                ephemeral=True
            )
            return

        total_count   = len(entries)
        total_gained  = sum(e.get("delta", 0) for e in entries if e.get("delta", 0) > 0)
        total_removed = sum(e.get("delta", 0) for e in entries if e.get("delta", 0) < 0)
        net           = total_gained + total_removed

        display = entries[-AUDIT_EMBED_MAX:] if total_count > AUDIT_EMBED_MAX else entries

        embed = discord.Embed(
            title=       f"AP Audit Log — {target.display_name}",
            description=(
                f"Showing **{len(display)}** of **{total_count}** entries since last reset.\n"
                f"Total earned: **+{total_gained:.2f} AP** | "
                f"Total removed: **{total_removed:.2f} AP** | "
                f"Net: **{net:.2f} AP**"
            ),
            timestamp=datetime.datetime.utcnow(),
            colour=discord.Colour.blurple()
        )
        embed.set_footer(text=f"User ID: {target.id}")

        lines: List[str] = []
        for e in display:
            ts_raw = e.get("ts", "")
            try:
                dt = datetime.datetime.fromisoformat(ts_raw)
                ts_fmt = dt.strftime("%Y-%m-%d %H:%M UTC")
            except Exception:
                ts_fmt = ts_raw

            delta    = e.get("delta", 0)
            sign     = "+" if delta >= 0 else ""
            source   = e.get("source", "unknown")
            reason   = e.get("reason", "")
            actor_id = e.get("actor_id")

            line = f"`{ts_fmt}` **{sign}{delta:.2f} AP** via *{source}*"
            if reason:
                line += f" — {reason}"
            if actor_id:
                actor_m = interaction.guild.get_member(int(actor_id))
                line += f" (by {actor_m.display_name if actor_m else actor_id})"
            lines.append(line)

        chunk, chunks = "", []
        for line in lines:
            candidate = (chunk + "\n" + line).lstrip("\n")
            if len(candidate) > 1024:
                chunks.append(chunk)
                chunk = line
            else:
                chunk = candidate
        if chunk:
            chunks.append(chunk)

        for i, c in enumerate(chunks, 1):
            embed.add_field(
                name=  f"Entries (part {i})" if len(chunks) > 1 else "Entries",
                value= c,
                inline=False
            )

        files: List[discord.File] = []
        if total_count > AUDIT_EMBED_MAX:
            csv_bytes = build_audit_csv(entries, target.display_name)
            ts_label  = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            files.append(discord.File(
                io.BytesIO(csv_bytes),
                filename=f"ap_audit_{target.id}_{ts_label}.csv"
            ))
            embed.description += (
                f"\n\n📎 Full log attached as CSV ({total_count} entries)."
            )

        await interaction.response.send_message(embed=embed, files=files, ephemeral=True)

    # -------------------------
    # Backup restore helpers
    # -------------------------

    def _list_guild_backups(self, guild_id: int) -> List[Path]:
        """
        Return all backup JSON files for this guild, newest first.
        Filename format: ap_backup_{guild_id}_{timestamp}.json
        """
        prefix = f"ap_backup_{guild_id}_"
        files = sorted(
            [p for p in EXPORT_DIR.glob(f"{prefix}*.json")],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        return files

    # -------------------------
    # /ap_list_backups
    # -------------------------

    @app_commands.command(
        name="ap_list_backups",
        description="List available AP backup files for this server (CEO / Lycan King only)."
    )
    async def ap_list_backups(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Must be used in a server.", ephemeral=True)
            return
        if not is_authorized_admin(interaction.user):
            await interaction.response.send_message("Not authorized.", ephemeral=True)
            return

        backups = self._list_guild_backups(interaction.guild.id)

        if not backups:
            await interaction.response.send_message(
                "No backup files found for this server.\n"
                "Backups are created automatically each time `/export_ap` is run.",
                ephemeral=True,
            )
            return

        lines: List[str] = []
        for i, path in enumerate(backups[:15]):  # cap display at 15
            mtime = datetime.datetime.utcfromtimestamp(path.stat().st_mtime)
            label = "  ← most recent" if i == 0 else ""
            lines.append(f"`{path.name}` — `{mtime.strftime('%Y-%m-%d %H:%M UTC')}`{label}")

        embed = discord.Embed(
            title="AP Backup Files",
            description="\n".join(lines),
            timestamp=datetime.datetime.utcnow(),
        )
        embed.set_footer(
            text="Use /ap_restore_backup to restore. Omit backup_name to use the most recent."
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    # -------------------------
    # /ap_restore_backup
    # -------------------------

    @app_commands.command(
        name="ap_restore_backup",
        description="Restore AP balances from a backup and re-send the CSV report (CEO / Lycan King only)."
    )
    @app_commands.describe(
        backup_name="Exact backup filename from /ap_list_backups. Leave blank to use the most recent."
    )
    async def ap_restore_backup(
        self,
        interaction: discord.Interaction,
        backup_name: Optional[str] = None,
    ):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Must be used in a server.", ephemeral=True)
            return
        if not is_authorized_admin(interaction.user):
            await interaction.response.send_message("Not authorized.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        # ── 1. Locate backup file ─────────────────────────────────────────────
        if backup_name:
            backup_path = EXPORT_DIR / backup_name.strip()
            if not backup_path.exists():
                await interaction.followup.send(
                    f"❌ Backup file `{backup_name}` not found.\n"
                    "Use `/ap_list_backups` to see available files.",
                    ephemeral=True,
                )
                return
        else:
            backups = self._list_guild_backups(interaction.guild.id)
            if not backups:
                await interaction.followup.send(
                    "❌ No backup files found for this server.\n"
                    "Backups are created automatically each time `/export_ap` is run.",
                    ephemeral=True,
                )
                return
            backup_path = backups[0]

        # ── 2. Load backup ────────────────────────────────────────────────────
        try:
            backup_raw = backup_path.read_text(encoding="utf-8").strip()
            backup_data: Dict[str, Any] = json.loads(backup_raw)
            if not isinstance(backup_data, dict):
                raise ValueError("Backup root is not a dict.")
        except Exception as e:
            await interaction.followup.send(
                f"❌ Failed to read backup file: `{e}`", ephemeral=True
            )
            return

        mtime = datetime.datetime.utcfromtimestamp(backup_path.stat().st_mtime)
        backup_label = f"`{backup_path.name}` (saved `{mtime.strftime('%Y-%m-%d %H:%M UTC')}`)"

        # ── 3. Restore AP values into current data ────────────────────────────
        # Only overwrites the `ap` field — preserves audit logs, claim fields,
        # and join bonus flags on the current live record.
        current_data = await load()
        restored = 0
        skipped  = 0

        for uid_str, backup_rec in backup_data.items():
            if uid_str == META_KEY:
                continue
            if not isinstance(backup_rec, dict):
                continue

            backup_ap = safe_float_ap(backup_rec.get("ap", 0))
            if backup_ap <= 0:
                skipped += 1
                continue

            # Merge: restore ap, preserve everything else in current record
            current_rec = current_data.setdefault(
                uid_str, {"ap": 0, "last_chat": None}
            )
            current_rec["ap"] = backup_ap

            # Restore ign/game claim fields if the current record is missing them
            for claim_key in (CLAIM_IGN_KEY, CLAIM_GAME_KEY):
                if claim_key in backup_rec and claim_key not in current_rec:
                    current_rec[claim_key] = backup_rec[claim_key]

            # Audit trail entry so there is a paper trail of the restore
            append_audit(
                current_data,
                int(uid_str),
                backup_ap,
                source="ap_restore_backup",
                reason=f"Restored from {backup_path.name}",
                actor_id=interaction.user.id,
            )
            restored += 1

        await save(current_data)

        # ── 4. Log to hierarchy channel ───────────────────────────────────────
        try:
            await log_hierarchy_ap(
                interaction.guild,
                f"AP restore executed by {interaction.user.mention}.\n"
                f"Backup: {backup_label}\n"
                f"Members restored: **{restored}** | Skipped (0 AP in backup): **{skipped}**",
                mention_ids=[interaction.user.id],
            )
        except Exception:
            pass

        # ── 5. Re-generate and send the CSV report from backup data ───────────
        try:
            groups    = build_ap_rows_by_game(interaction.guild, backup_data)
            csv_bytes = render_grouped_csv(groups)
            ts        = mtime.strftime("%Y%m%d_%H%M%S")
            filename  = f"ap_restored_report_{interaction.guild.id}_{ts}.csv"
            file      = discord.File(io.BytesIO(csv_bytes), filename=filename)

            wow_total     = sum(safe_int_ap(r[3]) for r in groups.get(GAME_WOW, []))
            eve_total     = sum(safe_int_ap(r[3]) for r in groups.get(GAME_EVE, []))
            unk_total     = sum(safe_int_ap(r[3]) for r in groups.get("Unclaimed / Unknown", []))
            total_members = sum(len(v) for v in groups.values())

            embed = discord.Embed(
                title="✅ AP Restore Complete — Backup Report",
                description=(
                    f"**Backup file:** {backup_label}\n"
                    f"**Members restored:** `{restored}`\n"
                    f"**Skipped (0 AP in backup):** `{skipped}`\n\n"
                    f"**{GAME_WOW}:** `{wow_total} AP`\n"
                    f"**{GAME_EVE}:** `{eve_total} AP`\n"
                    f"**Unclaimed / Unknown:** `{unk_total} AP`\n"
                    f"**Total members in report:** `{total_members}`\n\n"
                    "AP balances have been restored. CSV report attached."
                ),
                timestamp=datetime.datetime.utcnow(),
            )
            embed.set_footer(text=f"Restored by {interaction.user} — audit entries written per member.")
            await interaction.followup.send(embed=embed, file=file, ephemeral=True)

        except Exception as e:
            # Restore already saved — report partial success clearly
            await interaction.followup.send(
                f"✅ AP balances restored from {backup_label} "
                f"({restored} members).\n"
                f"⚠️ CSV generation failed: `{e}`",
                ephemeral=True,
            )


async def setup(bot: commands.Bot):
    await bot.add_cog(APTracking(bot))