# cogs/arc_hierarchy.py
#
# ARC Hierarchy (Unified + Automatic Corp Detection + Compact Flowchart)
# =====================================================================
# Corps (FIXED):
#   - ARC Subsidized
#   - ARC Security
#
# Corp membership is AUTO-DETECTED from roles:
#   If member has "ARC Subsidized" => corp = ARC Subsidized
#   Else if member has "ARC Security" => corp = ARC Security
#   Else => Unassigned
#
# Leadership is shared across both corps (CEO + Directors + ranks).
#
# RANKS (UPDATED):
#   - ARC General
#   - ARC Commander
#   - ARC Lieutenant        (was "ARC Officer" role)
#   - ARC Petty Officer     (NEW: below Lieutenant, above line members)
#   - Line members are "security" rank (no rank-role; corp role covers membership)
#
# IMPORTANT (UPDATED BEHAVIOR):
#   - On promotion: bot ADDS the new rank role but DOES NOT remove lower rank roles.
#   - On demotion: bot removes ONLY roles ABOVE the target rank (so no higher perms linger),
#                  but keeps lower rank roles unless manually removed.
#   - Legacy role "ARC Officer" is renamed to "ARC Lieutenant" if possible; otherwise removed if found.
#
# Commands:
#   /arc flowchart_refresh
#   /arc roster_corp (subsidized|security|unassigned)
#   /arc promote
#   /arc demote
#   /arc directive_create
#   /arc directive_list
#   /arc directive_done
#
# Auto:
#   - on_ready: bootstrap roles, sync corp keys, refresh flowchart
#   - on_member_update: auto-sync corp key if corp roles change, refresh flowchart

import os
import json
import io
import asyncio
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

import discord
from discord.ext import commands
from discord import app_commands

# =====================
# PERSISTENCE (Railway Volume)
# =====================
PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)
DATA_FILE = PERSIST_ROOT / "arc_hierarchy.json"
file_lock = asyncio.Lock()

# =====================
# ROLES (AUTHORITY)
# =====================
CEO_ROLE = "ARC Security Corporation Leader"            # PROTECTED: NEVER REMOVED BY BOT
DIRECTOR_ROLE = "ARC Security Administration Council"   # PROTECTED: NEVER REMOVED BY BOT

GENERAL_ROLE = "ARC General"
COMMANDER_ROLE = "ARC Commander"

# UPDATED ROLE NAMES
LIEUTENANT_ROLE = "ARC Lieutenant"          # renamed from "ARC Officer"
PETTY_OFFICER_ROLE = "ARC Petty Officer"    # NEW

# Legacy role name (we will try to rename it, and also strip it if found)
LEGACY_OFFICER_ROLE = "ARC Officer"

# =====================
# ROLES (CORPORATIONS)
# =====================
CORP_ROLE_SECURITY = "ARC Security"
CORP_ROLE_SUBSIDIZED = "ARC Subsidized"

# =====================
# CHANNELS
# =====================
LOG_CH = "arc-hierarchy-log"
FLOWCHART_CH = "corp-flowchart"
DIRECTIVES_CH = "arc-directives"

# =====================
# RANKS (UPDATED)
# =====================
RANK_SECURITY = "security"
RANK_PETTY_OFFICER = "petty_officer"
RANK_LIEUTENANT = "lieutenant"
RANK_COMMANDER = "commander"
RANK_GENERAL = "general"
RANK_DIRECTOR = "director"
RANK_CEO = "ceo"

# Security rank has NO rank-role (corp role handles membership)
ROLE_BY_RANK = {
    RANK_SECURITY: None,
    RANK_PETTY_OFFICER: PETTY_OFFICER_ROLE,
    RANK_LIEUTENANT: LIEUTENANT_ROLE,
    RANK_COMMANDER: COMMANDER_ROLE,
    RANK_GENERAL: GENERAL_ROLE,
    RANK_DIRECTOR: DIRECTOR_ROLE,
    RANK_CEO: CEO_ROLE,
}

PROMOTE_TO = {
    RANK_SECURITY: RANK_PETTY_OFFICER,
    RANK_PETTY_OFFICER: RANK_LIEUTENANT,
    RANK_LIEUTENANT: RANK_COMMANDER,
    RANK_COMMANDER: RANK_GENERAL,
    RANK_GENERAL: None,
}

DEMOTE_TO = {
    RANK_GENERAL: RANK_COMMANDER,
    RANK_COMMANDER: RANK_LIEUTENANT,
    RANK_LIEUTENANT: RANK_PETTY_OFFICER,
    RANK_PETTY_OFFICER: RANK_SECURITY,
}

# =====================
# CORPORATION KEYS (FIXED)
# =====================
CORP_UNASSIGNED_KEY = "unassigned"
CORP_SECURITY_KEY = "arc-security"
CORP_SUBSIDIZED_KEY = "arc-subsidized"

# =====================
# PERSISTENCE
# =====================
def _default_data() -> Dict[str, Any]:
    return {
        "members": {},
        "corporations": {
            CORP_UNASSIGNED_KEY: {"name": "Unassigned", "role_id": None},
            CORP_SECURITY_KEY: {"name": "ARC Security", "role_id": None},
            CORP_SUBSIDIZED_KEY: {"name": "ARC Subsidized", "role_id": None},
        },
        "directives": {},
        "flowchart": {},
    }

def _atomic_write_json(p: Path, data: Dict[str, Any]) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    payload = json.dumps(data, indent=2, ensure_ascii=False)
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(p)

def _coerce_dict(v: Any) -> Dict[str, Any]:
    return v if isinstance(v, dict) else {}

def _migrate_legacy_units_to_corps(data: Dict[str, Any]) -> Dict[str, Any]:
    # Drop any old "units" structure; normalize member records
    data.setdefault("members", {})
    data.setdefault("corporations", _default_data()["corporations"])
    data.setdefault("directives", {})
    data.setdefault("flowchart", {})

    if "units" in data:
        data.pop("units", None)

    members = _coerce_dict(data.get("members"))
    for uid_str, rec in list(members.items()):
        rec = _coerce_dict(rec)
        if "corp_key" not in rec:
            members[uid_str] = {
                "rank": rec.get("rank", RANK_SECURITY) if isinstance(rec.get("rank"), str) else RANK_SECURITY,
                "corp_key": CORP_UNASSIGNED_KEY,
            }
        else:
            if not isinstance(rec.get("rank"), str):
                rec["rank"] = RANK_SECURITY
            if not isinstance(rec.get("corp_key"), str):
                rec["corp_key"] = CORP_UNASSIGNED_KEY

        # --- Rank migration ---
        # Old script stored "officer". New rank key is "lieutenant".
        if isinstance(members[uid_str], dict):
            r = members[uid_str].get("rank")
            if r == "officer":
                members[uid_str]["rank"] = RANK_LIEUTENANT

    data["members"] = members
    return data

def load_data() -> Dict[str, Any]:
    try:
        if not DATA_FILE.exists():
            return _default_data()

        txt = DATA_FILE.read_text(encoding="utf-8").strip()
        if not txt:
            return _default_data()

        data = json.loads(txt)
        if not isinstance(data, dict):
            return _default_data()

        data = _migrate_legacy_units_to_corps(data)

        corps = data.setdefault("corporations", {})
        corps.setdefault(CORP_UNASSIGNED_KEY, {"name": "Unassigned", "role_id": None})
        corps.setdefault(CORP_SECURITY_KEY, {"name": "ARC Security", "role_id": None})
        corps.setdefault(CORP_SUBSIDIZED_KEY, {"name": "ARC Subsidized", "role_id": None})

        if not isinstance(data.get("members"), dict):
            data["members"] = {}
        if not isinstance(data.get("directives"), dict):
            data["directives"] = {}
        if not isinstance(data.get("flowchart"), dict):
            data["flowchart"] = {}

        return data

    except json.JSONDecodeError:
        try:
            bak = DATA_FILE.with_suffix(DATA_FILE.suffix + ".bak")
            DATA_FILE.replace(bak)
        except Exception:
            pass
        return _default_data()
    except Exception:
        return _default_data()

def save_data(data: Dict[str, Any]) -> None:
    _atomic_write_json(DATA_FILE, data)

# =====================
# HELPERS
# =====================
def get_role(guild: discord.Guild, name: str) -> Optional[discord.Role]:
    return discord.utils.get(guild.roles, name=name)

def has_role(member: discord.Member, role_name: str) -> bool:
    return discord.utils.get(member.roles, name=role_name) is not None

def is_director(member: discord.Member) -> bool:
    return has_role(member, DIRECTOR_ROLE)

def is_ceo(member: discord.Member) -> bool:
    return has_role(member, CEO_ROLE)

def can_manage(member: discord.Member) -> bool:
    return is_ceo(member) or is_director(member)

def ensure_member_record(data: Dict[str, Any], user_id: int) -> Dict[str, Any]:
    rec = data.setdefault("members", {}).setdefault(str(user_id), {
        "rank": RANK_SECURITY,
        "corp_key": CORP_UNASSIGNED_KEY,
    })
    if not isinstance(rec, dict):
        rec = {"rank": RANK_SECURITY, "corp_key": CORP_UNASSIGNED_KEY}
        data["members"][str(user_id)] = rec

    rec.setdefault("rank", RANK_SECURITY)
    rec.setdefault("corp_key", CORP_UNASSIGNED_KEY)

    if not isinstance(rec["rank"], str):
        rec["rank"] = RANK_SECURITY
    if not isinstance(rec["corp_key"], str):
        rec["corp_key"] = CORP_UNASSIGNED_KEY

    # Migrate legacy "officer" to lieutenant at runtime too (extra safety)
    if rec["rank"] == "officer":
        rec["rank"] = RANK_LIEUTENANT

    # Guard against unknown values
    if rec["rank"] not in (RANK_SECURITY, RANK_PETTY_OFFICER, RANK_LIEUTENANT, RANK_COMMANDER, RANK_GENERAL):
        rec["rank"] = RANK_SECURITY

    return rec

async def ensure_log_channel(guild: discord.Guild) -> discord.TextChannel:
    ch = discord.utils.get(guild.text_channels, name=LOG_CH)
    if ch:
        return ch
    return await guild.create_text_channel(LOG_CH)

async def safe_log(guild: discord.Guild, msg: str) -> None:
    try:
        ch = await ensure_log_channel(guild)
        await ch.send(msg[:1900])
    except Exception:
        pass

async def ensure_flowchart_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    ch = discord.utils.get(guild.text_channels, name=FLOWCHART_CH)
    if ch:
        return ch
    try:
        return await guild.create_text_channel(FLOWCHART_CH)
    except (discord.Forbidden, discord.HTTPException) as e:
        await safe_log(guild, f"Flowchart: could not create #{FLOWCHART_CH}. Missing perms? `{type(e).__name__}`")
        return None

async def ensure_directives_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    ch = discord.utils.get(guild.text_channels, name=DIRECTIVES_CH)
    if ch:
        return ch
    try:
        return await guild.create_text_channel(DIRECTIVES_CH)
    except (discord.Forbidden, discord.HTTPException) as e:
        await safe_log(guild, f"Directives: could not create #{DIRECTIVES_CH}. Missing perms? `{type(e).__name__}`")
        return None

async def log_action(guild: discord.Guild, content: str, mention_ids: List[int]) -> None:
    ch = await ensure_log_channel(guild)
    uniq: List[int] = []
    for i in mention_ids:
        if isinstance(i, int) and i not in uniq:
            uniq.append(i)

    mentions: List[str] = []
    for uid in uniq:
        m = guild.get_member(uid)
        if m:
            mentions.append(m.mention)

    prefix = (" ".join(mentions) + "\n") if mentions else ""
    await ch.send(prefix + content)

def _sort_members_casefold(members: List[discord.Member]) -> List[discord.Member]:
    return sorted(members, key=lambda x: (x.display_name or "").casefold())

def _chunk_text(text: str, max_len: int = 1900) -> List[str]:
    lines = text.split("\n")
    chunks: List[str] = []
    buf: List[str] = []
    cur = 0
    for line in lines:
        add = len(line) + 1
        if buf and (cur + add) > max_len:
            chunks.append("\n".join(buf))
            buf = [line]
            cur = len(line) + 1
        else:
            buf.append(line)
            cur += add
    if buf:
        chunks.append("\n".join(buf))
    return chunks

def detect_corp_key_from_member(member: discord.Member) -> str:
    # Priority: Subsidized > Security > Unassigned
    if has_role(member, CORP_ROLE_SUBSIDIZED):
        return CORP_SUBSIDIZED_KEY
    if has_role(member, CORP_ROLE_SECURITY):
        return CORP_SECURITY_KEY
    return CORP_UNASSIGNED_KEY

def corp_display_name_by_key(corp_key: str) -> str:
    return {
        CORP_SECURITY_KEY: "ARC Security",
        CORP_SUBSIDIZED_KEY: "ARC Subsidized",
        CORP_UNASSIGNED_KEY: "Unassigned",
    }.get(corp_key, "Unassigned")

def corp_tag(corp_key: str) -> str:
    return {
        CORP_SECURITY_KEY: "[SEC]",
        CORP_SUBSIDIZED_KEY: "[SUB]",
        CORP_UNASSIGNED_KEY: "[—]",
    }.get(corp_key, "[—]")

# =====================
# BOOTSTRAP ROLES
# =====================
async def bootstrap_fixed_corps(guild: discord.Guild) -> None:
    # Ensure corp roles exist (best effort)
    security_role = get_role(guild, CORP_ROLE_SECURITY)
    subsidized_role = get_role(guild, CORP_ROLE_SUBSIDIZED)

    if security_role is None:
        try:
            security_role = await guild.create_role(name=CORP_ROLE_SECURITY, reason="ARC corp bootstrap")
        except (discord.Forbidden, discord.HTTPException):
            security_role = None

    if subsidized_role is None:
        try:
            subsidized_role = await guild.create_role(name=CORP_ROLE_SUBSIDIZED, reason="ARC corp bootstrap")
        except (discord.Forbidden, discord.HTTPException):
            subsidized_role = None

    async with file_lock:
        data = load_data()
        corps = data.setdefault("corporations", {})
        corps.setdefault(CORP_UNASSIGNED_KEY, {"name": "Unassigned", "role_id": None})
        corps.setdefault(CORP_SECURITY_KEY, {"name": "ARC Security", "role_id": None})
        corps.setdefault(CORP_SUBSIDIZED_KEY, {"name": "ARC Subsidized", "role_id": None})

        corps[CORP_SECURITY_KEY]["name"] = "ARC Security"
        corps[CORP_SUBSIDIZED_KEY]["name"] = "ARC Subsidized"

        if security_role is not None:
            corps[CORP_SECURITY_KEY]["role_id"] = security_role.id
        if subsidized_role is not None:
            corps[CORP_SUBSIDIZED_KEY]["role_id"] = subsidized_role.id

        save_data(data)

async def bootstrap_rank_roles(guild: discord.Guild) -> None:
    """
    Ensures rank roles exist:
      - ARC Petty Officer (new)
      - ARC Lieutenant (renamed from ARC Officer if present)
      - ARC Commander
      - ARC General

    Best-effort:
      - If "ARC Officer" exists and "ARC Lieutenant" does NOT, rename the role.
      - Otherwise, create missing roles.
    """
    # 1) Rename legacy role if appropriate
    legacy = get_role(guild, LEGACY_OFFICER_ROLE)
    lieutenant = get_role(guild, LIEUTENANT_ROLE)
    if legacy is not None and lieutenant is None:
        try:
            await legacy.edit(name=LIEUTENANT_ROLE, reason="ARC rank rename: ARC Officer -> ARC Lieutenant")
        except (discord.Forbidden, discord.HTTPException):
            # if rename fails, we'll try creating lieutenant instead
            pass

    # refresh after possible rename
    lieutenant = get_role(guild, LIEUTENANT_ROLE)

    # 2) Create missing rank roles
    for role_name in (PETTY_OFFICER_ROLE, LIEUTENANT_ROLE, COMMANDER_ROLE, GENERAL_ROLE):
        if get_role(guild, role_name) is None:
            try:
                await guild.create_role(name=role_name, reason="ARC rank bootstrap")
            except (discord.Forbidden, discord.HTTPException):
                pass

async def sync_member_corp_from_roles(member: discord.Member, *, reason: str) -> Tuple[str, str]:
    async with file_lock:
        data = load_data()
        rec = ensure_member_record(data, member.id)
        old_key = rec.get("corp_key", CORP_UNASSIGNED_KEY)
        new_key = detect_corp_key_from_member(member)
        if old_key != new_key:
            rec["corp_key"] = new_key
            save_data(data)
        return old_key, new_key

# =====================
# RANK ROLE RETENTION LOGIC (NEW)
# =====================
def _rank_order() -> List[str]:
    # Lowest -> Highest (security has no role)
    return [RANK_SECURITY, RANK_PETTY_OFFICER, RANK_LIEUTENANT, RANK_COMMANDER, RANK_GENERAL]

def _roles_above_rank(guild: discord.Guild, rank: str) -> List[discord.Role]:
    """
    Returns discord.Role objects for ranks strictly ABOVE the given rank.
    Keeps lower/equal rank roles intact.
    Also includes legacy "ARC Officer" for removal if found.
    """
    order = _rank_order()
    try:
        idx = order.index(rank)
    except ValueError:
        idx = 0  # treat unknown as security

    above = set(order[idx + 1:])

    roles: List[discord.Role] = []

    # Always remove legacy Officer role if it exists (post-migration cleanup)
    legacy = get_role(guild, LEGACY_OFFICER_ROLE)
    if legacy is not None:
        roles.append(legacy)

    rank_to_role_name = {
        RANK_PETTY_OFFICER: PETTY_OFFICER_ROLE,
        RANK_LIEUTENANT: LIEUTENANT_ROLE,
        RANK_COMMANDER: COMMANDER_ROLE,
        RANK_GENERAL: GENERAL_ROLE,
    }

    for r in above:
        role_name = rank_to_role_name.get(r)
        if role_name:
            role_obj = get_role(guild, role_name)
            if role_obj is not None:
                roles.append(role_obj)

    return roles

async def apply_rank_change(member: discord.Member, new_rank: str) -> Tuple[str, str]:
    """
    UPDATED BEHAVIOR:
      - Promotion: ADDS the new rank role, DOES NOT remove lower rank roles.
      - Demotion: removes ONLY roles ABOVE the new rank (and legacy 'ARC Officer').
      - Keeps lower-rank roles forever unless manually removed.
    """
    guild = member.guild
    async with file_lock:
        data = load_data()
        rec = ensure_member_record(data, member.id)

        old_rank = rec.get("rank", RANK_SECURITY)
        rec["rank"] = new_rank

        # Remove only roles ABOVE the target new_rank (and legacy Officer)
        to_remove: List[discord.Role] = []
        for role in _roles_above_rank(guild, new_rank):
            if role in member.roles:
                to_remove.append(role)

        if to_remove:
            try:
                await member.remove_roles(*to_remove, reason="ARC rank change: removing ranks above target rank")
            except (discord.Forbidden, discord.HTTPException):
                pass

        # Add rank role for new_rank (if any) WITHOUT removing lower roles
        role_name = ROLE_BY_RANK.get(new_rank)
        if role_name:
            role_obj = get_role(guild, role_name)
            if role_obj and role_obj not in member.roles:
                try:
                    await member.add_roles(role_obj, reason=f"ARC rank change: add role for {new_rank}")
                except (discord.Forbidden, discord.HTTPException):
                    pass

        save_data(data)
        return old_rank, new_rank

# =====================
# FLOWCHART (Compact)
# =====================
def _format_rank_block(title: str, members: List[discord.Member]) -> List[str]:
    lines: List[str] = [f"{title}:"]
    ms = _sort_members_casefold(members)
    if not ms:
        lines.append("- None")
        return lines
    for m in ms:
        ck = detect_corp_key_from_member(m)
        lines.append(f"- {m.display_name} {corp_tag(ck)}")
    return lines

def build_flowchart_text(guild: discord.Guild, data: Dict[str, Any]) -> str:
    members_map = data.get("members", {})

    ceos: List[discord.Member] = []
    directors: List[discord.Member] = []
    generals: List[discord.Member] = []
    commanders: List[discord.Member] = []
    lieutenants: List[discord.Member] = []
    petty_officers: List[discord.Member] = []

    corp_counts = {
        CORP_SUBSIDIZED_KEY: {"total": 0, "security": 0, "ranked": 0},
        CORP_SECURITY_KEY: {"total": 0, "security": 0, "ranked": 0},
        CORP_UNASSIGNED_KEY: {"total": 0, "security": 0, "ranked": 0},
    }

    for m in guild.members:
        corp_key = detect_corp_key_from_member(m)
        corp_counts[corp_key]["total"] += 1

        if is_ceo(m):
            ceos.append(m)
            corp_counts[corp_key]["ranked"] += 1
            continue
        if is_director(m):
            directors.append(m)
            corp_counts[corp_key]["ranked"] += 1
            continue

        rec = members_map.get(str(m.id))
        rank = rec.get("rank", RANK_SECURITY) if isinstance(rec, dict) else RANK_SECURITY
        if not isinstance(rank, str):
            rank = RANK_SECURITY
        if rank == "officer":
            rank = RANK_LIEUTENANT

        if rank == RANK_GENERAL:
            generals.append(m)
            corp_counts[corp_key]["ranked"] += 1
        elif rank == RANK_COMMANDER:
            commanders.append(m)
            corp_counts[corp_key]["ranked"] += 1
        elif rank == RANK_LIEUTENANT:
            lieutenants.append(m)
            corp_counts[corp_key]["ranked"] += 1
        elif rank == RANK_PETTY_OFFICER:
            petty_officers.append(m)
            corp_counts[corp_key]["ranked"] += 1
        else:
            corp_counts[corp_key]["security"] += 1

    total_members = sum(v["total"] for v in corp_counts.values())

    lines: List[str] = []
    lines.append("ARC Corporate Flowchart (Compact)")
    lines.append("")
    lines.append(f"Total Members: {total_members}")
    lines.append("")
    lines.append("LEADERSHIP (All Corps)")
    lines.append("---------------------")

    lines.extend(_format_rank_block("CEO(s)", ceos))
    lines.append("")
    lines.extend(_format_rank_block("Directors Council", directors))
    lines.append("")
    lines.extend(_format_rank_block("Generals", generals))
    lines.append("")
    lines.extend(_format_rank_block("Commanders", commanders))
    lines.append("")
    lines.extend(_format_rank_block("Lieutenants", lieutenants))
    lines.append("")
    lines.extend(_format_rank_block("Petty Officers", petty_officers))
    lines.append("")
    lines.append("CORPORATIONS (Summary)")
    lines.append("---------------------")

    for ck in (CORP_SUBSIDIZED_KEY, CORP_SECURITY_KEY, CORP_UNASSIGNED_KEY):
        c = corp_counts[ck]
        lines.append(
            f"- {corp_display_name_by_key(ck)}: "
            f"Total {c['total']} | Ranked {c['ranked']} | Security {c['security']}"
        )

    lines.append("")
    lines.append("Tip: Use /arc roster_corp (subsidized|security|unassigned) for full member lists.")
    return "\n".join(lines).strip()

async def update_flowchart(guild: discord.Guild) -> None:
    ch = await ensure_flowchart_channel(guild)
    if not ch:
        return

    me = guild.me
    if me:
        perms = ch.permissions_for(me)
        if not perms.view_channel or not perms.send_messages:
            await safe_log(
                guild,
                f"Flowchart: cannot post in #{FLOWCHART_CH}. "
                f"Need View Channel + Send Messages. Current: view={perms.view_channel}, send={perms.send_messages}"
            )
            return

    async with file_lock:
        data = load_data()
        flow = data.setdefault("flowchart", {})
        stored_message_id = flow.get("message_id")

    text = build_flowchart_text(guild, data)
    parts = _chunk_text(f"```text\n{text}\n```", max_len=1900)

    msg: Optional[discord.Message] = None
    if isinstance(stored_message_id, int):
        try:
            msg = await ch.fetch_message(stored_message_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            msg = None

    try:
        if msg:
            await msg.edit(content=parts[0])
        else:
            msg = await ch.send(parts[0])
            async with file_lock:
                data = load_data()
                flow = data.setdefault("flowchart", {})
                flow["channel_id"] = ch.id
                flow["message_id"] = msg.id
                save_data(data)

        for p in parts[1:]:
            try:
                await ch.send(p)
            except (discord.Forbidden, discord.HTTPException):
                break

    except (discord.Forbidden, discord.HTTPException) as e:
        await safe_log(guild, f"Flowchart: failed to send/edit message in #{FLOWCHART_CH}. `{type(e).__name__}`")

# =====================
# DIRECTIVES
# =====================
def _now_unix() -> int:
    return int(discord.utils.utcnow().timestamp())

# =====================
# COG
# =====================
class ARCHierarchyCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    arc = app_commands.Group(name="arc", description="ARC hierarchy (unified) commands")

    # -----------------
    # FLOWCHART
    # -----------------
    @arc.command(name="flowchart_refresh", description="Force refresh the compact flowchart in #corp-flowchart")
    async def flowchart_refresh(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        actor = interaction.user
        if not isinstance(actor, discord.Member) or not can_manage(actor):
            await interaction.followup.send("Only the CEO and Directors may use this command.", ephemeral=True)
            return

        await bootstrap_fixed_corps(interaction.guild)
        await bootstrap_rank_roles(interaction.guild)
        await self.sync_all_members(interaction.guild, actor_id=actor.id, log=False)
        await update_flowchart(interaction.guild)
        await interaction.followup.send("✅ Flowchart refreshed.", ephemeral=True)

    # -----------------
    # ROSTER (CORP)
    # -----------------
    @arc.command(name="roster_corp", description="Show roster for a Corporation (grouped by rank)")
    @app_commands.describe(corporation="Choose: subsidized | security | unassigned")
    async def roster_corp(self, interaction: discord.Interaction, corporation: str):
        await interaction.response.defer(ephemeral=False, thinking=True)

        corp_in = (corporation or "").strip().lower()
        if corp_in in ("subsidized", "arc subsidized", "subs", "sub"):
            corp_key = CORP_SUBSIDIZED_KEY
        elif corp_in in ("security", "arc security", "sec"):
            corp_key = CORP_SECURITY_KEY
        elif corp_in in ("unassigned", "none", "unitless"):
            corp_key = CORP_UNASSIGNED_KEY
        else:
            await interaction.followup.send("Invalid corporation. Use: `subsidized`, `security`, or `unassigned`.", ephemeral=True)
            return

        await self.sync_all_members(interaction.guild, actor_id=interaction.user.id, log=False)

        async with file_lock:
            data = load_data()

        corp_name = corp_display_name_by_key(corp_key)

        members: List[Tuple[discord.Member, Dict[str, Any]]] = []
        for m in interaction.guild.members:
            rec = data.get("members", {}).get(str(m.id))
            if isinstance(rec, dict) and rec.get("corp_key") == corp_key:
                members.append((m, rec))

        groups: Dict[str, List[discord.Member]] = {
            "ceo": [],
            RANK_DIRECTOR: [],
            RANK_GENERAL: [],
            RANK_COMMANDER: [],
            RANK_LIEUTENANT: [],
            RANK_PETTY_OFFICER: [],
            RANK_SECURITY: [],
        }

        for m, rec in members:
            if is_ceo(m):
                groups["ceo"].append(m)
                continue
            if is_director(m):
                groups[RANK_DIRECTOR].append(m)
                continue

            r = rec.get("rank", RANK_SECURITY)
            if r == "officer":
                r = RANK_LIEUTENANT
            if r not in groups:
                r = RANK_SECURITY
            groups[r].append(m)

        def sort_names(ms: List[discord.Member]) -> List[discord.Member]:
            return sorted(ms, key=lambda x: (x.display_name or "").casefold())

        def fmt_group(key: str, title: str) -> List[str]:
            ms = sort_names(groups.get(key, []))
            if not ms:
                return [f"**{title} (0)**", "- None"]
            lines = [f"**{title} ({len(ms)})**"]
            for mm in ms:
                lines.append(f"- {mm.display_name}")
            return lines

        lines: List[str] = []
        lines.append(f"**Corporation:** {corp_name}")
        lines.append("")
        lines.extend(fmt_group("ceo", "CEO(s)"))
        lines.append("")
        lines.extend(fmt_group(RANK_DIRECTOR, "Directors"))
        lines.append("")
        lines.extend(fmt_group(RANK_GENERAL, "Generals"))
        lines.append("")
        lines.extend(fmt_group(RANK_COMMANDER, "Commanders"))
        lines.append("")
        lines.extend(fmt_group(RANK_LIEUTENANT, "Lieutenants"))
        lines.append("")
        lines.extend(fmt_group(RANK_PETTY_OFFICER, "Petty Officers"))
        lines.append("")
        lines.extend(fmt_group(RANK_SECURITY, "Security"))

        text = "\n".join(lines)

        if len(text) <= 1900:
            await interaction.followup.send(text, ephemeral=False)
            return

        fp = io.BytesIO(text.encode("utf-8"))
        file = discord.File(fp, filename=f"corp_roster_{corp_name}.txt")
        await interaction.followup.send(
            content="Roster is too large for a single message; attached as a file.",
            file=file,
            ephemeral=False,
        )

    # -----------------
    # PROMOTE / DEMOTE
    # -----------------
    @arc.command(name="promote", description="Promote a member (Directors/CEO)")
    @app_commands.describe(member="Member to promote")
    async def promote(self, interaction: discord.Interaction, member: discord.Member):
        await interaction.response.defer(ephemeral=True, thinking=True)

        actor = interaction.user
        if not isinstance(actor, discord.Member) or not can_manage(actor):
            await interaction.followup.send("Only the CEO and Directors may use this command.", ephemeral=True)
            return

        await bootstrap_rank_roles(interaction.guild)
        await sync_member_corp_from_roles(member, reason="promote: pre-sync")

        async with file_lock:
            data = load_data()
            rec = ensure_member_record(data, member.id)
            old_rank = rec.get("rank", RANK_SECURITY)
            nxt = PROMOTE_TO.get(old_rank)
            if not nxt:
                await interaction.followup.send(
                    f"{member.display_name} cannot be promoted further from **{old_rank}**.",
                    ephemeral=True
                )
                return

        prev_rank, new_rank = await apply_rank_change(member, nxt)

        async with file_lock:
            data = load_data()
            corp_key = detect_corp_key_from_member(member)
            rec = ensure_member_record(data, member.id)
            rec["corp_key"] = corp_key
            save_data(data)

        await log_action(
            interaction.guild,
            f"Promotion: {member.mention} **{prev_rank} → {new_rank}** by {actor.mention} (Corp: **{corp_display_name_by_key(corp_key)}**).",
            mention_ids=[actor.id, member.id],
        )
        await interaction.followup.send(
            f"✅ {member.mention} promoted: **{prev_rank} → {new_rank}**.",
            ephemeral=True,
        )
        await update_flowchart(interaction.guild)

    @arc.command(name="demote", description="Demote a member (Directors/CEO)")
    @app_commands.describe(member="Member to demote")
    async def demote(self, interaction: discord.Interaction, member: discord.Member):
        await interaction.response.defer(ephemeral=True, thinking=True)

        actor = interaction.user
        if not isinstance(actor, discord.Member) or not can_manage(actor):
            await interaction.followup.send("Only the CEO and Directors may use this command.", ephemeral=True)
            return

        await bootstrap_rank_roles(interaction.guild)
        await sync_member_corp_from_roles(member, reason="demote: pre-sync")

        async with file_lock:
            data = load_data()
            rec = ensure_member_record(data, member.id)
            old_rank = rec.get("rank", RANK_SECURITY)
            nxt = DEMOTE_TO.get(old_rank)
            if not nxt:
                await interaction.followup.send(
                    f"{member.display_name} cannot be demoted from **{old_rank}**.",
                    ephemeral=True
                )
                return

        prev_rank, new_rank = await apply_rank_change(member, nxt)

        async with file_lock:
            data = load_data()
            corp_key = detect_corp_key_from_member(member)
            rec = ensure_member_record(data, member.id)
            rec["corp_key"] = corp_key
            save_data(data)

        await log_action(
            interaction.guild,
            f"Demotion: {member.mention} **{prev_rank} → {new_rank}** by {actor.mention} (Corp: **{corp_display_name_by_key(corp_key)}**).",
            mention_ids=[actor.id, member.id],
        )
        await interaction.followup.send(
            f"✅ {member.mention} demoted: **{prev_rank} → {new_rank}**.",
            ephemeral=True,
        )
        await update_flowchart(interaction.guild)

    # -----------------
    # DIRECTIVES
    # -----------------
    @arc.command(name="directive_create", description="Create a directive (CEO/Directors)")
    @app_commands.describe(title="Short title", body="Details / expected outcome")
    async def directive_create(self, interaction: discord.Interaction, title: str, body: str):
        await interaction.response.defer(ephemeral=True, thinking=True)

        actor = interaction.user
        if not isinstance(actor, discord.Member) or not can_manage(actor):
            await interaction.followup.send("Only the CEO and Directors may use this command.", ephemeral=True)
            return

        title = (title or "").strip()[:80]
        body = (body or "").strip()[:1500]
        if not title:
            await interaction.followup.send("Title cannot be empty.", ephemeral=True)
            return
        if not body:
            await interaction.followup.send("Body cannot be empty.", ephemeral=True)
            return

        directive_id = str(int(discord.utils.utcnow().timestamp() * 1000))

        async with file_lock:
            data = load_data()
            data.setdefault("directives", {})[directive_id] = {
                "title": title,
                "body": body,
                "status": "open",
                "created_by": actor.id,
                "created_at": int(discord.utils.utcnow().timestamp()),
            }
            save_data(data)

        ch = await ensure_directives_channel(interaction.guild)
        if ch:
            try:
                await ch.send(
                    f"📌 **DIRECTIVE #{directive_id}** — **{title}**\n"
                    f"By: {actor.mention}\n"
                    f"{body}"
                )
            except (discord.Forbidden, discord.HTTPException):
                pass

        await log_action(
            interaction.guild,
            f"Directive created: **#{directive_id} {title}** by {actor.mention}.",
            mention_ids=[actor.id],
        )
        await interaction.followup.send(f"✅ Directive created: **#{directive_id} {title}**", ephemeral=True)

    @arc.command(name="directive_list", description="List open directives")
    async def directive_list(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)

        async with file_lock:
            data = load_data()
            directives = data.get("directives", {})

        open_items: List[Tuple[str, Dict[str, Any]]] = []
        for did, d in directives.items():
            if isinstance(d, dict) and d.get("status") == "open":
                open_items.append((did, d))
        open_items.sort(key=lambda x: int(x[1].get("created_at", 0)))

        if not open_items:
            await interaction.followup.send("No open directives.", ephemeral=True)
            return

        lines = ["**Open Directives:**"]
        for did, d in open_items[:25]:
            lines.append(f"- **#{did}** — {str(d.get('title',''))[:80]}")
        await interaction.followup.send("\n".join(lines), ephemeral=True)

    @arc.command(name="directive_done", description="Mark a directive as done (CEO/Directors)")
    @app_commands.describe(directive_id="Directive ID (numbers)")
    async def directive_done(self, interaction: discord.Interaction, directive_id: str):
        await interaction.response.defer(ephemeral=True, thinking=True)

        actor = interaction.user
        if not isinstance(actor, discord.Member) or not can_manage(actor):
            await interaction.followup.send("Only the CEO and Directors may use this command.", ephemeral=True)
            return

        directive_id = (directive_id or "").strip()
        async with file_lock:
            data = load_data()
            d = data.get("directives", {}).get(directive_id)
            if not isinstance(d, dict):
                await interaction.followup.send("Directive not found.", ephemeral=True)
                return
            d["status"] = "done"
            save_data(data)

        ch = await ensure_directives_channel(interaction.guild)
        if ch:
            try:
                await ch.send(f"✅ **DIRECTIVE #{directive_id}** marked **DONE** by {actor.mention}.")
            except (discord.Forbidden, discord.HTTPException):
                pass

        await log_action(
            interaction.guild,
            f"Directive done: **#{directive_id}** marked done by {actor.mention}.",
            mention_ids=[actor.id],
        )
        await interaction.followup.send(f"✅ Directive #{directive_id} marked done.", ephemeral=True)

    # -----------------
    # SYNC HELPERS
    # -----------------
    async def sync_all_members(self, guild: discord.Guild, *, actor_id: Optional[int], log: bool) -> int:
        changed = 0
        async with file_lock:
            data = load_data()
            for m in guild.members:
                rec = ensure_member_record(data, m.id)
                old_key = rec.get("corp_key", CORP_UNASSIGNED_KEY)
                new_key = detect_corp_key_from_member(m)
                if old_key != new_key:
                    rec["corp_key"] = new_key
                    changed += 1
            save_data(data)

        if log and changed:
            await log_action(guild, f"Corp sync: updated **{changed}** member record(s) from role detection.", mention_ids=[actor_id] if actor_id else [])
        return changed

    # -----------------
    # AUTO-SYNC ON CORP ROLE CHANGES
    # -----------------
    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        before_sub = has_role(before, CORP_ROLE_SUBSIDIZED)
        after_sub = has_role(after, CORP_ROLE_SUBSIDIZED)
        before_sec = has_role(before, CORP_ROLE_SECURITY)
        after_sec = has_role(after, CORP_ROLE_SECURITY)

        if (before_sub == after_sub) and (before_sec == after_sec):
            return

        old_key, new_key = await sync_member_corp_from_roles(after, reason="role change")
        if old_key != new_key:
            await safe_log(
                after.guild,
                f"Auto corp sync: {after.display_name} **{corp_display_name_by_key(old_key)} → {corp_display_name_by_key(new_key)}** (role change)."
            )
            await update_flowchart(after.guild)

    # -----------------
    # READY
    # -----------------
    @commands.Cog.listener()
    async def on_ready(self):
        await asyncio.sleep(2)
        for g in self.bot.guilds:
            try:
                await ensure_log_channel(g)
                await ensure_directives_channel(g)
                await bootstrap_fixed_corps(g)
                await bootstrap_rank_roles(g)
                await self.sync_all_members(guild=g, actor_id=None, log=False)
                await update_flowchart(g)
            except Exception:
                pass

async def setup(bot: commands.Bot):
    await bot.add_cog(ARCHierarchyCog(bot))