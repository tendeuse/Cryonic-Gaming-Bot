# cogs/arc_hierarchy.py

import discord
from discord.ext import commands
from discord import app_commands
import json
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import io

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
# CHANNELS / STORAGE
# =====================
LOG_CH = "arc-hierarchy-log"
DATA_FILE = Path("arc_hierarchy.json")

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
def load_data() -> Dict[str, Any]:
    if not DATA_FILE.exists():
        return {"members": {}, "units": {}}
    return json.loads(DATA_FILE.read_text(encoding="utf-8"))

def save_data(data: Dict[str, Any]) -> None:
    DATA_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")

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
        data = load_data()

        # Validate quickly BEFORE deferring
        if not is_director(director):
            await interaction.response.send_message("Only Directors may create units.", ephemeral=True)
            return

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

async def setup(bot: commands.Bot):
    await bot.add_cog(ARCHierarchyCog(bot))
