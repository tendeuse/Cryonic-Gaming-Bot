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
PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)

DATA_FILE = PERSIST_ROOT / "arc_hierarchy.json"

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
    RANK_GENERAL: None,
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
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    payload = json.dumps(data, indent=2)
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(p)

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

async def safe_log(guild: discord.Guild, msg: str) -> None:
    try:
        ch = await ensure_log_channel(guild)
        await ch.send(msg[:1900])
    except Exception:
        # If logging itself fails, we cannot do more safely.
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

def unit_role_ids(data: Dict[str, Any]) -> List[int]:
    ids: List[int] = []
    for u in data.get("units", {}).values():
        rid = u.get("unit_role_id")
        if isinstance(rid, int):
            ids.append(rid)
    return ids

def rank_roles_to_strip(guild: discord.Guild) -> List[discord.Role]:
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
    guild = member.guild

    unit = data.get("units", {}).get(str(director.id))
    if not unit:
        return None, None

    old_director_id = get_member_unit_director_id(data, member.id)

    if strip_first:
        await strip_member_for_unit_change(member, data)

    role = guild.get_role(unit["unit_role_id"])
    if role:
        try:
            await member.add_roles(role, reason="ARC unit assignment: assigning unit role")
        except (discord.Forbidden, discord.HTTPException):
            pass

    await remove_unitless_if_present(member)

    rec = ensure_member_record(data, member.id)
    rec["director_id"] = director.id

    if not (is_director(member) or is_ceo(member)):
        rec["rank"] = RANK_SECURITY

    return old_director_id, unit.get("unit_name")

async def apply_rank_change(member: discord.Member, new_rank: str) -> Tuple[str, str]:
    guild = member.guild

    async with file_lock:
        data = load_data()
        rec = ensure_member_record(data, member.id)

        old_rank = rec.get("rank", RANK_SECURITY)
        rec["rank"] = new_rank

        to_remove: List[discord.Role] = []
        for role in rank_roles_to_strip(guild):
            if role in member.roles:
                to_remove.append(role)
        if to_remove:
            await member.remove_roles(*to_remove, reason="ARC rank change: removing prior rank roles")

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

def build_flowchart_text(guild: discord.Guild, data: Dict[str, Any]) -> str:
    # CEO(s) by role
    ceo_role = get_role(guild, CEO_ROLE)
    ceos: List[discord.Member] = []
    if ceo_role:
        ceos = _sort_members_casefold([m for m in guild.members if ceo_role in m.roles])

    header = ["ARC Corporate Flowchart", ""]
    if not ceos:
        header.append("CEO: (unassigned)")
    elif len(ceos) == 1:
        header.append(f"CEO: {ceos[0].display_name}")
    else:
        header.append("CEO(s): " + ", ".join([m.display_name for m in ceos]))
    header.append("")

    # Directors: prefer unit owners (data["units"] keys), and include Directors by role even if no unit.
    unit_owner_ids: List[int] = []
    for k in data.get("units", {}).keys():
        try:
            unit_owner_ids.append(int(k))
        except Exception:
            pass

    directors_role = get_role(guild, DIRECTOR_ROLE)
    if directors_role:
        for m in guild.members:
            if directors_role in m.roles and m.id not in unit_owner_ids:
                unit_owner_ids.append(m.id)

    director_members: List[discord.Member] = []
    for did in unit_owner_ids:
        dm = guild.get_member(did)
        if dm:
            director_members.append(dm)
    director_members = _sort_members_casefold(director_members)

    lines: List[str] = header[:]

    if not director_members:
        lines.append("No Directors found.")
        return "\n".join(lines)

    for idx, d in enumerate(director_members):
        unit = data.get("units", {}).get(str(d.id))
        unit_name = unit.get("unit_name") if isinstance(unit, dict) else None
        unit_label = f"{d.display_name}" + (f"  [{unit_name}]" if unit_name else "")
        lines.append(f"├─ Director: {unit_label}")

        # Build roster grouped by stored rank
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

        # spacing between directors
        lines.append("")

    return "\n".join(lines).strip()

async def update_flowchart(guild: discord.Guild) -> None:
    """
    Posts/updates a single flowchart message in #corp-flowchart.
    Uses plain text (code block) to avoid needing Embed Links permission.
    """
    # Ensure channel exists
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

    # Load state and stored message pointer
    async with file_lock:
        data = load_data()
        flow = data.setdefault("flowchart", {})
        stored_message_id = flow.get("message_id")

    text = build_flowchart_text(guild, data)

    # If too long, send as multiple messages but still keep the first message tracked and updateable.
    # First message holds part 1, subsequent parts are sent fresh each time (best-effort).
    parts = _chunk_text(f"```text\n{text}\n```", max_len=1900)

    # Try fetch existing tracked message
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

        # Post additional parts (best effort, do not persist their ids)
        for p in parts[1:]:
            try:
                await ch.send(p)
            except (discord.Forbidden, discord.HTTPException):
                break

    except (discord.Forbidden, discord.HTTPException) as e:
        await safe_log(guild, f"Flowchart: failed to send/edit message in #{FLOWCHART_CH}. `{type(e).__name__}`")

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

        if not is_director(director):
            await interaction.response.send_message("Only Directors may create units.", ephemeral=True)
            return

        data = load_data()
        if str(director.id) in data["units"]:
            await interaction.response.send_message("You already own a unit.", ephemeral=True)
            return

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

            data["units"][str(director.id)] = {
                "unit_name": name,
                "unit_role_id": role.id,
                "category_id": category.id,
            }

            old_director_id, _unit_name = await assign_member_to_unit(
                director,
                director,
                data,
                strip_first=True,
            )

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

            await update_flowchart(guild)

        except (discord.Forbidden, discord.HTTPException) as e:
            await interaction.followup.send(
                f"Unit creation failed due to a permissions/API error.\n`{type(e).__name__}`",
                ephemeral=True,
            )
            await safe_log(guild, f"Create unit failed: `{type(e).__name__}`")

# =====================
# COG
# =====================
class ARCHierarchyCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    arc = app_commands.Group(name="arc", description="ARC hierarchy commands")

    # -----------------
    # FLOWCHART
    # -----------------
    @arc.command(name="flowchart_refresh", description="Force refresh the corp flowchart in #corp-flowchart")
    async def flowchart_refresh(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        actor = interaction.user
        if not isinstance(actor, discord.Member) or not can_manage(actor):
            await interaction.followup.send("Only the CEO and Directors may use this command.", ephemeral=True)
            return

        await update_flowchart(interaction.guild)
        await interaction.followup.send("✅ Flowchart refreshed.", ephemeral=True)

    # -----------------
    # UNIT MGMT
    # -----------------
    @arc.command(name="create_unit")
    async def create_unit(self, interaction: discord.Interaction):
        await interaction.response.send_modal(CreateUnitModal(self))

    @arc.command(name="join")
    @app_commands.describe(director="Director you want to join")
    async def join(self, interaction: discord.Interaction, director: discord.Member):
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

        await update_flowchart(guild)

    @arc.command(name="transfer_unit")
    @app_commands.describe(
        from_director="Director currently owning the unit",
        to_director="Director who will receive the unit"
    )
    async def transfer_unit(self, interaction: discord.Interaction, from_director: discord.Member, to_director: discord.Member):
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

        async with file_lock:
            data = load_data()

            unit = data.get("units", {}).get(str(from_director.id))
            if not unit:
                await interaction.followup.send("That source Director has no unit to transfer.", ephemeral=True)
                return

            if str(to_director.id) in data.get("units", {}):
                await interaction.followup.send("The destination Director already owns a unit.", ephemeral=True)
                return

            data["units"][str(to_director.id)] = unit
            data["units"].pop(str(from_director.id), None)

            members = data.get("members", {})
            for _uid, rec in members.items():
                if isinstance(rec, dict) and rec.get("director_id") == from_director.id:
                    rec["director_id"] = to_director.id
                    moved_count += 1

            to_rec = ensure_member_record(data, to_director.id)
            to_rec["rank"] = RANK_DIRECTOR
            to_rec["director_id"] = to_director.id

            save_data(data)

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

        await update_flowchart(guild)

    # -----------------
    # PROMOTE / DEMOTE
    # -----------------
    @arc.command(name="promote")
    @app_commands.describe(member="Member to promote")
    async def promote(self, interaction: discord.Interaction, member: discord.Member):
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

        await interaction.followup.send(
            f"{member.mention} promoted: **{prev_rank} → {new_rank}**.",
            ephemeral=True,
        )

        await update_flowchart(guild)

    @arc.command(name="demote")
    @app_commands.describe(member="Member to demote")
    async def demote(self, interaction: discord.Interaction, member: discord.Member):
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

        await interaction.followup.send(
            f"{member.mention} demoted: **{prev_rank} → {new_rank}**.",
            ephemeral=True,
        )

        await update_flowchart(guild)

    # -----------------
    # ROSTER (GROUPED)
    # -----------------
    @arc.command(name="roster")
    @app_commands.describe(director="Director whose unit roster you want to view")
    async def roster(self, interaction: discord.Interaction, director: discord.Member):
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
        # Give Discord a brief moment so channels/guild caches are stable
        await asyncio.sleep(2)
        for g in self.bot.guilds:
            await ensure_log_channel(g)
            await update_flowchart(g)

async def setup(bot: commands.Bot):
    await bot.add_cog(ARCHierarchyCog(bot))
