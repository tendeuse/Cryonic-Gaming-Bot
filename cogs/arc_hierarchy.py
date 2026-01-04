# cogs/arc_hierarchy.py

import os
import discord
from discord.ext import commands
from discord import app_commands
import json
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import io
import asyncio

# =====================
# PERSISTENCE (Railway Volume)
# =====================
# Mount your Railway Volume at /data.
# Optionally override with env var PERSIST_ROOT (e.g., "/data").
PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)

# Persist hierarchy state on the volume
DATA_FILE = PERSIST_ROOT / "arc_hierarchy.json"

# Single-process lock to serialize JSON read/write
file_lock = asyncio.Lock()

# =====================
# ROLES
# =====================
CEO_ROLE = "ARC Security Corporation Leader"            # PROTECTED: NEVER REMOVED BY BOT
DIRECTOR_ROLE = "ARC Security Administration Council"   # PROTECTED: NEVER REMOVED BY BOT
GENERAL_ROLE = "ARC General"
COMMANDER_ROLE = "ARC Commander"
OFFICER_ROLE = "ARC Officer"
SECURITY_ROLE = "ARC Security"  # PROTECTED: NEVER REMOVED BY BOT

UNITLESS_ROLE = "Unitless"

# Rank roles the bot must NEVER remove automatically
PROTECTED_RANK_ROLES = {SECURITY_ROLE, DIRECTOR_ROLE, CEO_ROLE}

# =====================
# CHANNELS
# =====================
LOG_CH = "arc-hierarchy-log"
FLOWCHART_CH = "corp-flowchart"

# =====================
# RANKS
# =====================
RANK_SECURITY = "security"
RANK_OFFICER = "officer"
RANK_COMMANDER = "commander"
RANK_GENERAL = "general"
RANK_DIRECTOR = "director"
RANK_CEO = "ceo"

ROLE_BY_RANK = {
    RANK_SECURITY: SECURITY_ROLE,
    RANK_OFFICER: OFFICER_ROLE,
    RANK_COMMANDER: COMMANDER_ROLE,
    RANK_GENERAL: GENERAL_ROLE,
    RANK_DIRECTOR: DIRECTOR_ROLE,
    RANK_CEO: CEO_ROLE,
}

PROMOTE_TO = {
    RANK_SECURITY: RANK_OFFICER,
    RANK_OFFICER: RANK_COMMANDER,
    RANK_COMMANDER: RANK_GENERAL,
    RANK_GENERAL: None,  # unit cap
}

DEMOTE_TO = {
    RANK_GENERAL: RANK_COMMANDER,
    RANK_COMMANDER: RANK_OFFICER,
    RANK_OFFICER: RANK_SECURITY,
}

# =====================
# PERSISTENCE
# =====================
def _default_data() -> Dict[str, Any]:
    return {"members": {}, "units": {}, "flowchart": {}}

def _atomic_write_json(p: Path, data: Dict[str, Any]) -> None:
    """
    Atomic JSON write:
      - write to .tmp in same directory
      - replace target
    Reduces risk of corruption on crash/redeploy.
    """
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    payload = json.dumps(data, indent=2)
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(p)

def load_data() -> Dict[str, Any]:
    """
    Robust load:
      - if missing: default structure
      - if blank/corrupt: keep a .bak and default
    """
    try:
        if not DATA_FILE.exists():
            return _default_data()

        txt = DATA_FILE.read_text(encoding="utf-8").strip()
        if not txt:
            return _default_data()

        data = json.loads(txt)
        if not isinstance(data, dict):
            return _default_data()

        data.setdefault("members", {})
        data.setdefault("units", {})
        data.setdefault("flowchart", {})

        if not isinstance(data["members"], dict):
            data["members"] = {}
        if not isinstance(data["units"], dict):
            data["units"] = {}
        if not isinstance(data["flowchart"], dict):
            data["flowchart"] = {}

        return data

    except json.JSONDecodeError:
        # Keep a backup for inspection, then reset
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
    return data.setdefault("members", {}).setdefault(str(user_id), {
        "rank": RANK_SECURITY,
        "director_id": None,
        "supervisor_id": None,
    })

async def ensure_log_channel(guild: discord.Guild) -> discord.TextChannel:
    ch = discord.utils.get(guild.text_channels, name=LOG_CH)
    if ch:
        return ch
    return await guild.create_text_channel(LOG_CH)

async def ensure_flowchart_channel(guild: discord.Guild) -> discord.TextChannel:
    ch = discord.utils.get(guild.text_channels, name=FLOWCHART_CH)
    if ch:
        return ch
    return await guild.create_text_channel(FLOWCHART_CH)

def unit_role_ids(data: Dict[str, Any]) -> List[int]:
    ids: List[int] = []
    for u in data.get("units", {}).values():
        rid = u.get("unit_role_id")
        if isinstance(rid, int):
            ids.append(rid)
    return ids

def rank_roles_to_strip(guild: discord.Guild) -> List[discord.Role]:
    """
    Rank roles that may be stripped during transfers/rank enforcement.
    IMPORTANT: excludes protected roles (ARC Security + Director + CEO).
    """
    candidates = [OFFICER_ROLE, COMMANDER_ROLE, GENERAL_ROLE, CEO_ROLE, DIRECTOR_ROLE]
    out: List[discord.Role] = []
    for name in candidates:
        if name in PROTECTED_RANK_ROLES:
            continue
        r = get_role(guild, name)
        if r:
            out.append(r)
    return out

async def strip_member_for_unit_change(
    member: discord.Member,
    data: Dict[str, Any]
) -> Tuple[List[discord.Role], List[discord.Role]]:
    """
    On unit transfer:
      - Remove ALL removable rank roles (never remove ARC Security, Director, or CEO)
      - Remove ALL unit roles
    """
    guild = member.guild

    removed_rank: List[discord.Role] = []
    for role in rank_roles_to_strip(guild):
        if role in member.roles:
            removed_rank.append(role)
    if removed_rank:
        await member.remove_roles(*removed_rank, reason="ARC unit transfer: stripping prior ranks")

    removed_unit: List[discord.Role] = []
    for rid in unit_role_ids(data):
        r = guild.get_role(rid)
        if r and r in member.roles:
            removed_unit.append(r)
    if removed_unit:
        await member.remove_roles(*removed_unit, reason="ARC unit transfer: stripping prior unit role(s)")

    return removed_rank, removed_unit

async def remove_unitless_if_present(member: discord.Member) -> None:
    role = get_role(member.guild, UNITLESS_ROLE)
    if role and role in member.roles:
        try:
            await member.remove_roles(role, reason="ARC unit assignment: removing Unitless")
        except (discord.Forbidden, discord.HTTPException):
            pass

def get_member_unit_director_id(data: Dict[str, Any], member_id: int) -> Optional[int]:
    rec = data.get("members", {}).get(str(member_id))
    if not rec:
        return None
    did = rec.get("director_id")
    return did if isinstance(did, int) else None

async def assign_member_to_unit(
    member: discord.Member,
    director: discord.Member,
    data: Dict[str, Any],
    *,
    strip_first: bool = True,
) -> Tuple[Optional[int], Optional[str]]:
    """
    Shared logic for:
      - /arc join
      - director creating a new unit (treated as joining that unit)

    Behavior:
      - Optionally strips prior ranks + prior unit roles (keeps protected roles)
      - Adds the unit role
      - Removes Unitless
      - Sets director_id
      - Sets rank to security ONLY if member is not a Director/CEO (protect leadership ranks)
    """
    guild = member.guild

    unit = data.get("units", {}).get(str(director.id))
    if not unit:
        return None, None

    old_director_id = get_member_unit_director_id(data, member.id)

    if strip_first:
        await strip_member_for_unit_change(member, data)

    # Add new unit role
    role = guild.get_role(unit["unit_role_id"])
    if role:
        try:
            await member.add_roles(role, reason="ARC unit assignment: assigning unit role")
        except (discord.Forbidden, discord.HTTPException):
            pass

    # Remove Unitless as soon as someone is in a unit
    await remove_unitless_if_present(member)

    # Storage update
    rec = ensure_member_record(data, member.id)
    rec["director_id"] = director.id

    # Do NOT downgrade protected leadership in storage
    if not (is_director(member) or is_ceo(member)):
        rec["rank"] = RANK_SECURITY

    return old_director_id, unit.get("unit_name")

async def apply_rank_change(member: discord.Member, new_rank: str) -> Tuple[str, str]:
    """
    Enforces only one rank role at once, never removing protected roles
    (ARC Security + Director + CEO).
    """
    guild = member.guild

    # Serialize state updates
    async with file_lock:
        data = load_data()
        rec = ensure_member_record(data, member.id)

        old_rank = rec.get("rank", RANK_SECURITY)
        rec["rank"] = new_rank

        # Remove prior rank roles except protected roles
        to_remove: List[discord.Role] = []
        for role in rank_roles_to_strip(guild):
            if role in member.roles:
                to_remove.append(role)
        if to_remove:
            await member.remove_roles(*to_remove, reason="ARC rank change: removing prior rank roles")

        # Add the new rank role (if applicable)
        if new_rank in (RANK_OFFICER, RANK_COMMANDER, RANK_GENERAL, RANK_DIRECTOR, RANK_CEO):
            role_name = ROLE_BY_RANK.get(new_rank)
            role_obj = get_role(guild, role_name) if role_name else None
            if role_obj and role_obj not in member.roles:
                try:
                    await member.add_roles(role_obj, reason=f"ARC rank change: set to {new_rank}")
                except (discord.Forbidden, discord.HTTPException):
                    pass

        save_data(data)
        return old_rank, new_rank

async def log_action(guild: discord.Guild, content: str, mention_director_ids: List[int]) -> None:
    ch = await ensure_log_channel(guild)

    uniq: List[int] = []
    for i in mention_director_ids:
        if isinstance(i, int) and i not in uniq:
            uniq.append(i)

    mentions = []
    for did in uniq:
        m = guild.get_member(did)
        if m:
            mentions.append(m.mention)

    prefix = (" ".join(mentions) + "\n") if mentions else ""
    await ch.send(prefix + content)

def _rank_label(rank: str) -> str:
    return {
        RANK_CEO: "CEO",
        RANK_DIRECTOR: "Director",
        RANK_GENERAL: "General",
        RANK_COMMANDER: "Commander",
        RANK_OFFICER: "Officer",
        RANK_SECURITY: "Security",
    }.get(rank, rank)

def _sort_members_casefold(members: List[discord.Member]) -> List[discord.Member]:
    return sorted(members, key=lambda x: (x.display_name or "").casefold())

def _chunk_lines(lines: List[str], max_len: int = 1900) -> List[str]:
    """
    Splits a list of lines into multiple message-safe chunks.
    Uses max_len < 2000 to leave room for formatting.
    """
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

async def update_flowchart(guild: discord.Guild) -> None:
    """
    Maintains a single "corp flowchart" message in #corp-flowchart.
    Updated whenever rosters/units/ranks change.
    """
    # Read persisted message/channel ids without doing Discord API calls under lock.
    async with file_lock:
        data = load_data()
        flow = data.setdefault("flowchart", {})
        stored_channel_id = flow.get("channel_id")
        stored_message_id = flow.get("message_id")
        # do not save here unless we change something
        # (saving on every call is unnecessary I/O)

    # Ensure channel exists
    ch: Optional[discord.TextChannel] = None
    try:
        ch = discord.utils.get(guild.text_channels, name=FLOWCHART_CH)
        if not ch:
            ch = await ensure_flowchart_channel(guild)
    except (discord.Forbidden, discord.HTTPException):
        return

    # If the stored channel id is different, overwrite it to the current one.
    if not isinstance(stored_channel_id, int) or stored_channel_id != ch.id:
        async with file_lock:
            data = load_data()
            flow = data.setdefault("flowchart", {})
            flow["channel_id"] = ch.id
            save_data(data)

    # Build the flowchart content (text-based tree)
    async with file_lock:
        data = load_data()

    # Identify CEO(s)
    ceos: List[discord.Member] = []
    ceo_role = get_role(guild, CEO_ROLE)
    if ceo_role:
        ceos = _sort_members_casefold([m for m in guild.members if ceo_role in m.roles])

    ceo_line = "CEO: (unassigned)"
    if ceos:
        if len(ceos) == 1:
            ceo_line = f"CEO: {ceos[0].display_name}"
        else:
            ceo_line = "CEO(s): " + ", ".join([m.display_name for m in ceos])

    # Directors are "unit owners" from data["units"] (most consistent with your current structure)
    unit_owner_ids: List[int] = []
    for k in data.get("units", {}).keys():
        try:
            unit_owner_ids.append(int(k))
        except Exception:
            pass

    # Also include any Directors who have the Director role but no unit entry (so they still show)
    directors_role = get_role(guild, DIRECTOR_ROLE)
    directors_with_role: List[discord.Member] = []
    if directors_role:
        directors_with_role = [m for m in guild.members if directors_role in m.roles]
    for m in directors_with_role:
        if m.id not in unit_owner_ids:
            unit_owner_ids.append(m.id)

    # Sort directors by display name when possible
    director_members: List[discord.Member] = []
    for did in unit_owner_ids:
        dm = guild.get_member(did)
        if dm:
            director_members.append(dm)
    director_members = _sort_members_casefold(director_members)

    # Prepare lines (use code block for a clean tree)
    lines: List[str] = []
    lines.append("ARC Corporate Flowchart")
    lines.append("")
    lines.append(ceo_line)
    lines.append("")

    if not director_members:
        lines.append("No Directors found.")
    else:
        for d in director_members:
            unit = data.get("units", {}).get(str(d.id))
            unit_name = unit.get("unit_name") if isinstance(unit, dict) else None
            unit_label = f"{d.display_name}" + (f"  [{unit_name}]" if unit_name else "")

            lines.append(f"├─ Director: {unit_label}")

            # Gather roster for this director_id
            members_for_director: List[Tuple[discord.Member, Dict[str, Any]]] = []
            for m in guild.members:
                rec = data.get("members", {}).get(str(m.id))
                if isinstance(rec, dict) and rec.get("director_id") == d.id:
                    members_for_director.append((m, rec))

            groups: Dict[str, List[discord.Member]] = {
                RANK_DIRECTOR: [],
                RANK_GENERAL: [],
                RANK_COMMANDER: [],
                RANK_OFFICER: [],
                RANK_SECURITY: [],
            }
            for m, rec in members_for_director:
                r = rec.get("rank", RANK_SECURITY)
                if r not in groups:
                    r = RANK_SECURITY
                groups[r].append(m)

            # Ensure director appears in their own roster group if assigned
            # (many setups store director_id for director; if not, this won't force it)
            # Sort and print
            any_listed = False
            for rank in (RANK_DIRECTOR, RANK_GENERAL, RANK_COMMANDER, RANK_OFFICER, RANK_SECURITY):
                ms = _sort_members_casefold(groups.get(rank, []))
                if not ms:
                    continue
                any_listed = True
                lines.append(f"│  ├─ {_rank_label(rank)} ({len(ms)})")
                for m in ms:
                    lines.append(f"│  │  • {m.display_name}")
            if not any_listed:
                lines.append("│  └─ (No assigned members)")

            lines.append("")

    # Wrap into a single embed; if too large, fall back to file attachment but still keep a single pinned-ish message.
    # We will keep the message simple: an embed with chunks in fields if needed.
    content_chunks = _chunk_lines(lines, max_len=1800)  # keep room for code block markers

    embed = discord.Embed(
        title="Corp Flowchart",
        description="Auto-updated hierarchy view (CEO → Directors → Unit roster).",
        color=discord.Color.blurple(),
    )

    if len(content_chunks) == 1:
        embed.add_field(name="Flow", value=f"```text\n{content_chunks[0]}\n```", inline=False)
    else:
        # Multiple chunks; place them in multiple fields (Discord allows up to 25 fields)
        for idx, chunk in enumerate(content_chunks[:24], start=1):
            embed.add_field(name=f"Flow (part {idx})", value=f"```text\n{chunk}\n```", inline=False)
        if len(content_chunks) > 24:
            embed.add_field(
                name="Flow (truncated)",
                value="```text\nOutput too large to display fully in embed fields.\nConsider splitting units or reducing roster size.\n```",
                inline=False
            )

    # Upsert the message (edit if exists, otherwise create)
    msg: Optional[discord.Message] = None
    if isinstance(stored_message_id, int):
        try:
            msg = await ch.fetch_message(stored_message_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            msg = None

    if msg:
        try:
            await msg.edit(embed=embed, content=None)
            return
        except (discord.Forbidden, discord.HTTPException):
            # If we cannot edit, try to send a new one
            msg = None

    # Create a new message
    try:
        new_msg = await ch.send(embed=embed)
    except (discord.Forbidden, discord.HTTPException):
        return

    # Persist the new message id
    async with file_lock:
        data = load_data()
        flow = data.setdefault("flowchart", {})
        flow["channel_id"] = ch.id
        flow["message_id"] = new_msg.id
        save_data(data)

# =====================
# MODAL
# =====================
class CreateUnitModal(discord.ui.Modal, title="Create ARC Unit"):
    unit_name = discord.ui.TextInput(label="Unit Name", max_length=64)

    def __init__(self, cog: "ARCHierarchyCog"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        director = interaction.user

        # Validate quickly BEFORE deferring
        if not is_director(director):
            await interaction.response.send_message("Only Directors may create units.", ephemeral=True)
            return

        data = load_data()
        if str(director.id) in data["units"]:
            await interaction.response.send_message("You already own a unit.", ephemeral=True)
            return

        # Defer immediately to avoid Unknown interaction (10062)
        await interaction.response.defer(ephemeral=False, thinking=True)

        try:
            name = self.unit_name.value.strip()
            role = await guild.create_role(name=name)

            category = await guild.create_category(
                f"Unit - {name}",
                overwrites={
                    guild.default_role: discord.PermissionOverwrite(view_channel=False),
                    role: discord.PermissionOverwrite(view_channel=True, send_messages=True),
                },
            )

            text_ch = await guild.create_text_channel("unit-chat", category=category)
            voice_ch = await guild.create_voice_channel("unit-voice", category=category)

            # Persist unit first so assign_member_to_unit can find it
            data["units"][str(director.id)] = {
                "unit_name": name,
                "unit_role_id": role.id,
                "category_id": category.id,
            }

            # Treat creation as joining: strip removable roles + remove Unitless + add unit role
            old_director_id, _unit_name = await assign_member_to_unit(
                director,
                director,
                data,
                strip_first=True,
            )

            # Ensure director record is correct (role itself is protected)
            rec = ensure_member_record(data, director.id)
            rec["rank"] = RANK_DIRECTOR
            rec["director_id"] = director.id

            save_data(data)

            mention_ids: List[int] = []
            if isinstance(old_director_id, int):
                mention_ids.append(old_director_id)
            mention_ids.append(director.id)

            await log_action(
                guild,
                f"Unit created: **{name}** by {director.mention}.",
                mention_director_ids=mention_ids,
            )

            await interaction.followup.send(
                f"Unit **{name}** created.\nChannels: {text_ch.mention}, {voice_ch.mention}",
                ephemeral=False,
            )

            # Update flowchart after a roster/unit change
            await update_flowchart(guild)

        except (discord.Forbidden, discord.HTTPException) as e:
            await interaction.followup.send(
                f"Unit creation failed due to a permissions/API error.\n`{type(e).__name__}`",
                ephemeral=True,
            )

# =====================
# COG
# =====================
class ARCHierarchyCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    arc = app_commands.Group(name="arc", description="ARC hierarchy commands")

    # -----------------
    # UNIT MGMT
    # -----------------
    @arc.command(name="create_unit")
    async def create_unit(self, interaction: discord.Interaction):
        await interaction.response.send_modal(CreateUnitModal(self))

    @arc.command(name="join")
    @app_commands.describe(director="Director you want to join")
    async def join(self, interaction: discord.Interaction, director: discord.Member):
        # Join can do multiple role operations; defer to avoid Unknown interaction.
        await interaction.response.defer(ephemeral=False, thinking=True)

        guild = interaction.guild
        member = interaction.user
        data = load_data()

        if not is_director(director):
            await interaction.followup.send("Target must be a Director.", ephemeral=True)
            return

        unit = data["units"].get(str(director.id))
        if not unit:
            await interaction.followup.send("That Director has no unit.", ephemeral=True)
            return

        old_director_id, unit_name = await assign_member_to_unit(
            member,
            director,
            data,
            strip_first=True,
        )
        save_data(data)

        mention_ids: List[int] = []
        if isinstance(old_director_id, int):
            mention_ids.append(old_director_id)
        mention_ids.append(director.id)
        if can_manage(member):
            mention_ids.append(member.id)

        await log_action(
            guild,
            f"Unit transfer: {member.mention} joined **{unit_name}** (Director: {director.mention}).",
            mention_director_ids=mention_ids,
        )

        await interaction.followup.send(
            f"{member.mention} joined **{unit_name}**.",
            ephemeral=False,
        )

        # Update flowchart after roster change
        await update_flowchart(guild)

    # NEW: CEO-only unit ownership transfer (roles, reports, AP attribution via director_id)
    @arc.command(name="transfer_unit")
    @app_commands.describe(
        from_director="Director currently owning the unit",
        to_director="Director who will receive the unit"
    )
    async def transfer_unit(self, interaction: discord.Interaction, from_director: discord.Member, to_director: discord.Member):
        # I/O + multiple operations; defer to avoid Unknown interaction.
        await interaction.response.defer(ephemeral=True, thinking=True)

        actor = interaction.user
        guild = interaction.guild

        if not isinstance(actor, discord.Member) or not is_ceo(actor):
            await interaction.followup.send("Only the CEO may use this command.", ephemeral=True)
            return

        if not is_director(from_director):
            await interaction.followup.send("`from_director` must be a Director.", ephemeral=True)
            return

        if not is_director(to_director):
            await interaction.followup.send("`to_director` must be a Director.", ephemeral=True)
            return

        if from_director.id == to_director.id:
            await interaction.followup.send("Source and destination Directors are the same.", ephemeral=True)
            return

        moved_count = 0
        unit: Dict[str, Any] = {}

        # Serialize JSON update (prevents races with join/promote/demote)
        async with file_lock:
            data = load_data()

            # Validate: source unit exists
            unit = data.get("units", {}).get(str(from_director.id))
            if not unit:
                await interaction.followup.send("That source Director has no unit to transfer.", ephemeral=True)
                return

            # Validate: destination director does NOT already own a unit
            if str(to_director.id) in data.get("units", {}):
                await interaction.followup.send("The destination Director already owns a unit.", ephemeral=True)
                return

            # Move the unit entry (ownership transfer)
            data["units"][str(to_director.id)] = unit
            data["units"].pop(str(from_director.id), None)

            # Update all members assigned to the old unit → new director_id
            members = data.get("members", {})
            for _uid, rec in members.items():
                if isinstance(rec, dict) and rec.get("director_id") == from_director.id:
                    rec["director_id"] = to_director.id
                    moved_count += 1

            # Ensure destination director record exists and is correct
            to_rec = ensure_member_record(data, to_director.id)
            to_rec["rank"] = RANK_DIRECTOR
            to_rec["director_id"] = to_director.id

            save_data(data)

        # Ensure destination director has the unit role so they can access the unit category/channels
        try:
            role_id = unit.get("unit_role_id")
            role_obj = guild.get_role(role_id) if isinstance(role_id, int) else None
            if role_obj and role_obj not in to_director.roles:
                await to_director.add_roles(role_obj, reason="ARC unit transfer: new Director receiving unit role")
            await remove_unitless_if_present(to_director)
        except (discord.Forbidden, discord.HTTPException):
            pass

        await log_action(
            guild,
            (
                f"Unit transferred: **{unit.get('unit_name', 'Unnamed Unit')}**\n"
                f"From Director: {from_director.mention}\n"
                f"To Director: {to_director.mention}\n"
                f"Moved member records: **{moved_count}**"
            ),
            mention_director_ids=[from_director.id, to_director.id, actor.id],
        )

        await interaction.followup.send(
            f"✅ Transferred unit **{unit.get('unit_name', 'Unnamed Unit')}** from {from_director.mention} to {to_director.mention}.\n"
            f"Updated **{moved_count}** member record(s).",
            ephemeral=True,
        )

        # Update flowchart after unit ownership + roster mapping changes
        await update_flowchart(guild)

    # -----------------
    # PROMOTE / DEMOTE
    # -----------------
    @arc.command(name="promote")
    @app_commands.describe(member="Member to promote")
    async def promote(self, interaction: discord.Interaction, member: discord.Member):
        # Role changes + logging; defer for safety.
        await interaction.response.defer(ephemeral=True, thinking=True)

        actor = interaction.user
        guild = interaction.guild

        if not can_manage(actor):
            await interaction.followup.send("Only the CEO and Directors may use this command.", ephemeral=True)
            return

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

        data = load_data()
        director_id = get_member_unit_director_id(data, member.id)

        mention_ids: List[int] = []
        if isinstance(director_id, int):
            mention_ids.append(director_id)
        if can_manage(actor):
            mention_ids.append(actor.id)

        await log_action(
            guild,
            f"Promotion: {member.mention} **{prev_rank} → {new_rank}** by {actor.mention}.",
            mention_director_ids=mention_ids,
        )

        # Promote result: ephemeral keeps channels clean; change to False if you want public.
        await interaction.followup.send(
            f"{member.mention} promoted: **{prev_rank} → {new_rank}**.",
            ephemeral=True,
        )

        # Update flowchart after rank changes
        await update_flowchart(guild)

    @arc.command(name="demote")
    @app_commands.describe(member="Member to demote")
    async def demote(self, interaction: discord.Interaction, member: discord.Member):
        # Role changes + logging; defer for safety.
        await interaction.response.defer(ephemeral=True, thinking=True)

        actor = interaction.user
        guild = interaction.guild

        if not can_manage(actor):
            await interaction.followup.send("Only the CEO and Directors may use this command.", ephemeral=True)
            return

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

        data = load_data()
        director_id = get_member_unit_director_id(data, member.id)

        mention_ids: List[int] = []
        if isinstance(director_id, int):
            mention_ids.append(director_id)
        if can_manage(actor):
            mention_ids.append(actor.id)

        await log_action(
            guild,
            f"Demotion: {member.mention} **{prev_rank} → {new_rank}** by {actor.mention}.",
            mention_director_ids=mention_ids,
        )

        # Demote result: ephemeral keeps channels clean; change to False if you want public.
        await interaction.followup.send(
            f"{member.mention} demoted: **{prev_rank} → {new_rank}**.",
            ephemeral=True,
        )

        # Update flowchart after rank changes
        await update_flowchart(guild)

    # -----------------
    # ROSTER (GROUPED)
    # -----------------
    @arc.command(name="roster")
    @app_commands.describe(director="Director whose unit roster you want to view")
    async def roster(self, interaction: discord.Interaction, director: discord.Member):
        # FIX: ACK immediately so the interaction doesn't expire (Unknown interaction 10062)
        await interaction.response.defer(ephemeral=False, thinking=True)

        data = load_data()
        unit = data["units"].get(str(director.id))
        if not unit:
            await interaction.followup.send("No unit found for that Director.", ephemeral=True)
            return

        members: List[Tuple[discord.Member, Dict[str, Any]]] = []
        for m in interaction.guild.members:
            rec = data.get("members", {}).get(str(m.id))
            if rec and rec.get("director_id") == director.id:
                members.append((m, rec))

        groups: Dict[str, List[Tuple[discord.Member, Dict[str, Any]]]] = {
            RANK_DIRECTOR: [],
            RANK_GENERAL: [],
            RANK_COMMANDER: [],
            RANK_OFFICER: [],
            RANK_SECURITY: [],
        }

        for m, rec in members:
            r = rec.get("rank", RANK_SECURITY)
            if r not in groups:
                r = RANK_SECURITY
            groups[r].append((m, rec))

        def fmt_group(rank: str, title: str) -> List[str]:
            items = groups.get(rank, [])
            if not items:
                return [f"**{title} (0)**", "- None"]
            items.sort(key=lambda x: x[0].display_name.lower())
            lines = [f"**{title} ({len(items)})**"]
            for m, _rec in items:
                lines.append(f"- {m.display_name}")
            return lines

        lines: List[str] = []
        lines.append(f"**Unit:** {unit['unit_name']}")
        lines.append(f"**Director:** {director.mention}")
        lines.append("")

        lines.extend(fmt_group(RANK_DIRECTOR, "Directors"))
        lines.append("")
        lines.extend(fmt_group(RANK_GENERAL, "Generals"))
        lines.append("")
        lines.extend(fmt_group(RANK_COMMANDER, "Commanders"))
        lines.append("")
        lines.extend(fmt_group(RANK_OFFICER, "Officers"))
        lines.append("")
        lines.extend(fmt_group(RANK_SECURITY, "Security"))

        text = "\n".join(lines)

        # FIX: avoid hitting Discord 2000 character limit
        if len(text) <= 1900:
            await interaction.followup.send(text, ephemeral=False)
            return

        fp = io.BytesIO(text.encode("utf-8"))
        file = discord.File(fp, filename=f"roster_{unit['unit_name']}.txt")
        await interaction.followup.send(
            content="Roster is too large for a single message; attached as a file.",
            file=file,
            ephemeral=False,
        )

    @commands.Cog.listener()
    async def on_ready(self):
        for g in self.bot.guilds:
            await ensure_log_channel(g)
            # Ensure flowchart channel exists and is current on startup
            await update_flowchart(g)

async def setup(bot: commands.Bot):
    await bot.add_cog(ARCHierarchyCog(bot))
