# cogs/unitless_enforcer.py
#
# Standalone cog:
# - Ensures members who have "ARC Security" role AND are NOT in any Unit get the "Unitless" role.
# - Removes "Unitless" once they join a Unit.
#
# Notes:
# - Unit membership is inferred from the persisted arc_hierarchy.json (same file your hierarchy cog uses),
#   using the stored member record field: members[user_id]["director_id"].
# - This cog does NOT modify unit roles or ranks; it only manages the "Unitless" role.
# - Designed to be safe on Railway: robust JSON load, serialized I/O, periodic sync + event-driven sync.

import os
import json
import asyncio
import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Set, List

import discord
from discord.ext import commands, tasks

# =====================
# CONFIG
# =====================
SECURITY_ROLE_NAME = "ARC Security"
UNITLESS_ROLE_NAME = "Unitless"

# Same persistence root your arc_hierarchy cog uses
PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)
HIERARCHY_FILE = PERSIST_ROOT / "arc_hierarchy.json"

# Optional log channel (created if missing). Set to None to disable.
LOG_CHANNEL_NAME = "arc-hierarchy-log"

# How often to run a full reconciliation (minutes)
RECONCILE_INTERVAL_MINUTES = 10

# Rate limiting / safety
MAX_CHANGES_PER_CYCLE = 250
PER_MEMBER_DELAY_SECONDS = 0.25

# Serialize file reads (single-process)
file_lock = asyncio.Lock()


# =====================
# JSON LOAD (robust)
# =====================
def _default_data() -> Dict[str, Any]:
    return {"members": {}, "units": {}}

def load_hierarchy_data() -> Dict[str, Any]:
    try:
        if not HIERARCHY_FILE.exists():
            return _default_data()

        txt = HIERARCHY_FILE.read_text(encoding="utf-8").strip()
        if not txt:
            return _default_data()

        data = json.loads(txt)
        if not isinstance(data, dict):
            return _default_data()

        data.setdefault("members", {})
        data.setdefault("units", {})
        if not isinstance(data["members"], dict):
            data["members"] = {}
        if not isinstance(data["units"], dict):
            data["units"] = {}

        return data
    except json.JSONDecodeError:
        # Preserve a backup if corrupt
        try:
            bak = HIERARCHY_FILE.with_suffix(HIERARCHY_FILE.suffix + ".bak")
            HIERARCHY_FILE.replace(bak)
        except Exception:
            pass
        return _default_data()
    except Exception:
        return _default_data()


# =====================
# ROLE HELPERS
# =====================
def get_role(guild: discord.Guild, name: str) -> Optional[discord.Role]:
    return discord.utils.get(guild.roles, name=name)

def has_role(member: discord.Member, role: Optional[discord.Role]) -> bool:
    return role is not None and role in member.roles

async def ensure_log_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    if not LOG_CHANNEL_NAME:
        return None
    ch = discord.utils.get(guild.text_channels, name=LOG_CHANNEL_NAME)
    if ch:
        return ch
    try:
        return await guild.create_text_channel(LOG_CHANNEL_NAME)
    except (discord.Forbidden, discord.HTTPException):
        return None

async def log(guild: discord.Guild, msg: str) -> None:
    ch = await ensure_log_channel(guild)
    if not ch:
        return
    try:
        await ch.send(msg[:1900])
    except Exception:
        pass


# =====================
# UNIT MEMBERSHIP CHECK
# =====================
def is_member_in_unit(data: Dict[str, Any], user_id: int) -> bool:
    """
    True if arc_hierarchy.json says the member is assigned to a unit.
    Uses stored members[user_id]["director_id"] being an int.
    """
    rec = data.get("members", {}).get(str(user_id))
    if not isinstance(rec, dict):
        return False
    did = rec.get("director_id")
    return isinstance(did, int)


# =====================
# COG
# =====================
class UnitlessEnforcer(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._ready_once = False

    async def cog_load(self):
        # Start background reconcile when cog is loaded
        if not self.reconcile_task.is_running():
            self.reconcile_task.start()

    async def cog_unload(self):
        if self.reconcile_task.is_running():
            self.reconcile_task.cancel()

    @commands.Cog.listener()
    async def on_ready(self):
        # Run a one-time reconcile shortly after ready
        if self._ready_once:
            return
        self._ready_once = True
        await asyncio.sleep(2)
        for g in self.bot.guilds:
            await self.reconcile_guild(g, reason="startup")

    @tasks.loop(minutes=RECONCILE_INTERVAL_MINUTES)
    async def reconcile_task(self):
        for g in self.bot.guilds:
            await self.reconcile_guild(g, reason="scheduled")

    @reconcile_task.before_loop
    async def before_reconcile_task(self):
        await self.bot.wait_until_ready()

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """
        Event-driven enforcement:
        - If ARC Security role is added/removed, or Unitless changed, reconcile that single member.
        """
        if before.guild is None:
            return

        # Only act if roles changed
        if set(before.roles) == set(after.roles):
            return

        # Quick targeted reconcile for this member
        await self.reconcile_member(after, reason="member_update")

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        # New join: reconcile
        await self.reconcile_member(member, reason="member_join")

    async def reconcile_guild(self, guild: discord.Guild, *, reason: str) -> None:
        """
        Full pass: ensure correct Unitless assignment for all members with ARC Security.
        """
        security_role = get_role(guild, SECURITY_ROLE_NAME)
        unitless_role = get_role(guild, UNITLESS_ROLE_NAME)

        if security_role is None:
            await log(guild, f"[UnitlessEnforcer] Missing role: '{SECURITY_ROLE_NAME}'. ({reason})")
            return
        if unitless_role is None:
            await log(guild, f"[UnitlessEnforcer] Missing role: '{UNITLESS_ROLE_NAME}'. ({reason})")
            return

        # Load hierarchy data once per guild reconcile
        async with file_lock:
            data = load_hierarchy_data()

        changes = 0
        for m in guild.members:
            if not isinstance(m, discord.Member) or m.bot:
                continue
            if not has_role(m, security_role):
                continue

            in_unit = is_member_in_unit(data, m.id)
            should_have_unitless = (not in_unit)

            has_unitless = has_role(m, unitless_role)

            # Add Unitless
            if should_have_unitless and not has_unitless:
                try:
                    await m.add_roles(unitless_role, reason="UnitlessEnforcer: ARC Security not in unit")
                    changes += 1
                    await asyncio.sleep(PER_MEMBER_DELAY_SECONDS)
                except (discord.Forbidden, discord.HTTPException):
                    pass

            # Remove Unitless once in a unit
            if (not should_have_unitless) and has_unitless:
                try:
                    await m.remove_roles(unitless_role, reason="UnitlessEnforcer: member is in a unit")
                    changes += 1
                    await asyncio.sleep(PER_MEMBER_DELAY_SECONDS)
                except (discord.Forbidden, discord.HTTPException):
                    pass

            if changes >= MAX_CHANGES_PER_CYCLE:
                await log(guild, f"[UnitlessEnforcer] Reconcile capped at {MAX_CHANGES_PER_CYCLE} changes. ({reason})")
                break

        if changes > 0:
            await log(guild, f"[UnitlessEnforcer] Reconciled {changes} role change(s). ({reason})")

    async def reconcile_member(self, member: discord.Member, *, reason: str) -> None:
        """
        Targeted enforcement for a single member.
        """
        guild = member.guild
        security_role = get_role(guild, SECURITY_ROLE_NAME)
        unitless_role = get_role(guild, UNITLESS_ROLE_NAME)
        if security_role is None or unitless_role is None:
            return

        if member.bot:
            return

        if not has_role(member, security_role):
            # If they don't have ARC Security, we do nothing (and do not remove Unitless)
            return

        async with file_lock:
            data = load_hierarchy_data()

        in_unit = is_member_in_unit(data, member.id)
        should_have_unitless = (not in_unit)
        has_unitless = has_role(member, unitless_role)

        try:
            if should_have_unitless and not has_unitless:
                await member.add_roles(unitless_role, reason="UnitlessEnforcer: ARC Security not in unit")
                await log(guild, f"[UnitlessEnforcer] Added Unitless to {member.mention}. ({reason})")
            elif (not should_have_unitless) and has_unitless:
                await member.remove_roles(unitless_role, reason="UnitlessEnforcer: member is in a unit")
                await log(guild, f"[UnitlessEnforcer] Removed Unitless from {member.mention}. ({reason})")
        except (discord.Forbidden, discord.HTTPException):
            pass


async def setup(bot: commands.Bot):
    await bot.add_cog(UnitlessEnforcer(bot))
