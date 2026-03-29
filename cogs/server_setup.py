# cogs/server_setup.py
#
# Dynamic Server Setup
# ====================
# Scans every loaded cog's module at runtime to discover which Discord roles
# and channels they actually need, then creates anything that's missing.
#
# There are NO hardcoded lists here.  The source of truth is the other cogs.
#
# ── How the scanner works ─────────────────────────────────────────────────
# For each loaded cog (excluding this one) the scanner inspects the real
# Python module object using `sys.modules`.  It walks every module-level name
# and applies naming-convention heuristics:
#
#   Role variables   — name ends with  _ROLE / _ROLES / _ROLE_NAME / _ROLES_NAME
#   Channel variables— name ends with  _CHANNEL / _CHANNELS / _CHANNEL_NAME
#                      _CHANNELS_NAME  or  _CH
#
# Collection rules:
#   • Plain str    → collect the value directly
#   • list/tuple/set/frozenset of str → collect each element
#   • dict         → collect VALUES only  (never keys — avoids "main"/"hs" etc.)
#   • Other dicts  → scan one level deeper for sub-keys matching the patterns
#                    (catches FEEDS, SHOPS structures)
#
# Opt-in override (optional but always wins):
#   Any cog module may declare at module level:
#       REQUIRED_ROLES:    list[str]
#       REQUIRED_CHANNELS: list[str]
#   When present the heuristic is skipped for that module entirely.
#
# Value filters make sure stray strings (URLs, custom-IDs, format strings,
# lowercase RSVP types) are never mistaken for role / channel names.
#
# ── Slash command ─────────────────────────────────────────────────────────
#   /server_setup — re-run the scan and create anything missing (CEO only)
#
# ── Persistent hook ───────────────────────────────────────────────────────
#   on_member_update: when ARC Security is newly granted, auto-assign Newbro
#   and Unitless (unchanged from the previous version).

import re
import sys
from typing import Any

import discord
from discord.ext import commands
from discord import app_commands

# ── Local constants for the on_member_update hook only ────────────────────
# These are intentionally NOT picked up by the scanner (server_setup is
# always excluded from its own scan).
_SECURITY_ROLE = "ARC Security"
_NEWBRO_ROLE   = "Newbro"
_UNITLESS_ROLE = "Unitless"

# ── Variable-name patterns ────────────────────────────────────────────────
#
# Both patterns use word-boundary anchors so sub-strings are NOT matched.
# Examples of what they deliberately reject:
#   ROLE_ASSIGN_TYPES     ("ROLE" at the start, not the end)
#   AP_CHECK_EMBED_TITLE  ("CH" is inside "CHECK", not a standalone word)
#   AP_CHECK_MESSAGE_ID_KEY
#
_ROLE_VAR = re.compile(
    r'(?:^|_)ROLES?(?:_NAMES?)?$',
    re.IGNORECASE,
)
_CHANNEL_VAR = re.compile(
    r'(?:^|_)CHANNELS?(?:_NAMES?)?$|_CH$',
    re.IGNORECASE,
)


# ── Value validators ──────────────────────────────────────────────────────

def _valid_role(v: Any) -> bool:
    """True only for strings that look like real Discord role names."""
    if not isinstance(v, str) or not v or len(v) > 100:
        return False
    if '\n' in v or '{' in v or ':' in v:
        return False
    if v.startswith('http'):
        return False
    # Every real role in this server starts with an uppercase letter.
    # This filters out RSVP types ("accept", "damage") and dict keys
    # ("main", "hs") that occasionally survive variable-name matching.
    return v[0].isupper()


def _valid_channel(v: Any) -> bool:
    """True only for strings that look like real Discord channel names."""
    if not isinstance(v, str) or not v or len(v) > 100:
        return False
    # Channel names must not contain spaces, slashes, or colons
    if ' ' in v or '/' in v or ':' in v:
        return False
    if v.startswith('http') or '{' in v:
        return False
    return any(c.isalpha() for c in v)


# ── Module scanner ────────────────────────────────────────────────────────

def _scan_module(module: Any) -> tuple[set[str], set[str]]:
    """
    Return (roles, channels) discovered in `module`.

    Uses actual Python runtime values — no source text parsing.
    Dict VALUES are collected; dict KEYS are never collected.
    """
    # Opt-in explicit declarations override heuristics entirely
    if hasattr(module, 'REQUIRED_ROLES') or hasattr(module, 'REQUIRED_CHANNELS'):
        return (
            {r for r in getattr(module, 'REQUIRED_ROLES',    []) if _valid_role(r)},
            {c for c in getattr(module, 'REQUIRED_CHANNELS', []) if _valid_channel(c)},
        )

    roles:    set[str] = set()
    channels: set[str] = set()

    for attr_name in dir(module):
        if attr_name.startswith('_'):
            continue
        try:
            val = getattr(module, attr_name)
        except Exception:
            continue

        uname = attr_name.upper()
        is_role_var    = bool(_ROLE_VAR.search(uname))    and 'CHANNEL' not in uname
        is_channel_var = bool(_CHANNEL_VAR.search(uname))

        # ── Plain string ──────────────────────────────────────────────────
        if isinstance(val, str):
            if is_role_var    and _valid_role(val):    roles.add(val)
            if is_channel_var and _valid_channel(val): channels.add(val)

        # ── Sequence / set (all elements are values, no keys) ─────────────
        elif isinstance(val, (list, tuple, set, frozenset)):
            for item in val:
                if not isinstance(item, str):
                    continue
                if is_role_var    and _valid_role(item):    roles.add(item)
                if is_channel_var and _valid_channel(item): channels.add(item)

        # ── Dict ──────────────────────────────────────────────────────────
        elif isinstance(val, dict):
            if is_role_var or is_channel_var:
                # Variable is explicitly named as a role/channel container:
                # collect only VALUES (never keys).
                for v in val.values():
                    if not isinstance(v, str):
                        continue
                    if is_role_var    and _valid_role(v):    roles.add(v)
                    if is_channel_var and _valid_channel(v): channels.add(v)
            else:
                # Variable name is not obviously a role/channel container,
                # but its values may be sub-dicts with channel/role sub-keys.
                # Handles patterns like:
                #   FEEDS = {"main": {"channel": "kill-mail", ...}}
                #   SHOPS = {"main": {"shop_channel": "ap-eve-shop", ...}}
                for sub_val in val.values():
                    if not isinstance(sub_val, dict):
                        continue
                    for sub_key, sub_sub_val in sub_val.items():
                        if not isinstance(sub_key, str) or not isinstance(sub_sub_val, str):
                            continue
                        sk = sub_key.upper()
                        if _CHANNEL_VAR.search(sk) and _valid_channel(sub_sub_val):
                            channels.add(sub_sub_val)
                        if _ROLE_VAR.search(sk) and 'CHANNEL' not in sk and _valid_role(sub_sub_val):
                            roles.add(sub_sub_val)

    return roles, channels


# ── Cog ───────────────────────────────────────────────────────────────────

class ServerSetup(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    # ── Collect requirements from all loaded cogs ─────────────────────────

    def _collect_from_cogs(self) -> tuple[set[str], set[str]]:
        """
        Walk every loaded cog, resolve its module from sys.modules, scan it.
        This cog's own module is always skipped.
        """
        own_module = type(self).__module__
        all_roles:    set[str] = set()
        all_channels: set[str] = set()

        for cog in self.bot.cogs.values():
            mod_name = type(cog).__module__
            if mod_name == own_module:
                continue
            module = sys.modules.get(mod_name)
            if module is None:
                continue
            r, c = _scan_module(module)
            all_roles    |= r
            all_channels |= c

        return all_roles, all_channels

    # ── Create what's missing ─────────────────────────────────────────────

    async def _ensure(
        self, guild: discord.Guild
    ) -> tuple[list[str], list[str], list[str], list[str]]:
        """
        Returns (created_roles, existing_roles, created_channels, existing_channels).
        """
        needed_roles, needed_channels = self._collect_from_cogs()

        created_roles: list[str]    = []
        existing_roles: list[str]   = []
        created_channels: list[str] = []
        existing_channels: list[str]= []

        for name in sorted(needed_roles):
            if discord.utils.get(guild.roles, name=name):
                existing_roles.append(name)
            else:
                try:
                    await guild.create_role(
                        name=name,
                        reason="ServerSetup: required by a loaded cog",
                    )
                    created_roles.append(name)
                    print(f"[server_setup] {guild.name}: created role '{name}'")
                except (discord.Forbidden, discord.HTTPException) as exc:
                    print(f"[server_setup] {guild.name}: could not create role '{name}': {exc}")

        for name in sorted(needed_channels):
            if discord.utils.get(guild.text_channels, name=name):
                existing_channels.append(name)
            else:
                try:
                    await guild.create_text_channel(
                        name,
                        reason="ServerSetup: required by a loaded cog",
                    )
                    created_channels.append(name)
                    print(f"[server_setup] {guild.name}: created channel '#{name}'")
                except (discord.Forbidden, discord.HTTPException) as exc:
                    print(f"[server_setup] {guild.name}: could not create channel '#{name}': {exc}")

        return created_roles, existing_roles, created_channels, existing_channels

    # ── Listeners ─────────────────────────────────────────────────────────

    @commands.Cog.listener()
    async def on_ready(self):
        print("[server_setup] Scanning cogs for required roles and channels…")
        for guild in self.bot.guilds:
            cr, _, cc, _ = await self._ensure(guild)
            if cr:
                print(f"[server_setup] {guild.name}: created {len(cr)} role(s): {cr}")
            if cc:
                print(f"[server_setup] {guild.name}: created {len(cc)} channel(s): {cc}")
        print("[server_setup] Scan complete.")

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """Auto-assign Newbro + Unitless when ARC Security is newly granted."""
        before_ids = {r.id for r in before.roles}
        after_ids  = {r.id for r in after.roles}
        if before_ids == after_ids:
            return

        guild         = after.guild
        security_role = discord.utils.get(guild.roles, name=_SECURITY_ROLE)
        if not security_role or security_role.id not in after_ids:
            return
        if security_role.id in before_ids:   # already had the role before
            return

        to_add = []
        for role_name in (_NEWBRO_ROLE, _UNITLESS_ROLE):
            role = discord.utils.get(guild.roles, name=role_name)
            if role and role not in after.roles:
                to_add.append(role)

        if to_add:
            try:
                await after.add_roles(
                    *to_add,
                    reason="ARC Security granted: auto-assign Newbro + Unitless",
                )
            except (discord.Forbidden, discord.HTTPException):
                pass

    # ── Slash command ──────────────────────────────────────────────────────

    @app_commands.command(
        name="server_setup",
        description="Scan all loaded cogs and create any missing roles or channels.",
    )
    async def server_setup_cmd(self, interaction: discord.Interaction):
        if not interaction.guild:
            await interaction.response.send_message(
                "Must be used in a server.", ephemeral=True
            )
            return

        is_owner = interaction.user.id == interaction.guild.owner_id
        is_ceo   = (
            isinstance(interaction.user, discord.Member)
            and any(r.name == "ARC Security Corporation Leader" for r in interaction.user.roles)
        )
        if not (is_owner or is_ceo):
            await interaction.response.send_message(
                "❌ Only the server owner or `ARC Security Corporation Leader` can run this.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)

        cr, er, cc, ec = await self._ensure(interaction.guild)

        color = discord.Color.green() if (cr or cc) else discord.Color.blurple()
        embed = discord.Embed(title="🔧 Server Setup — Scan Report", color=color)

        embed.add_field(
            name=f"✅ Roles created ({len(cr)})",
            value="\n".join(f"• {r}" for r in cr) or "*(none)*",
            inline=False,
        )
        embed.add_field(
            name=f"✅ Channels created ({len(cc)})",
            value="\n".join(f"• #{c}" for c in cc) or "*(none)*",
            inline=False,
        )
        embed.add_field(
            name=f"— Roles already present ({len(er)})",
            value="\n".join(f"• {r}" for r in er) or "*(none)*",
            inline=False,
        )
        embed.add_field(
            name=f"— Channels already present ({len(ec)})",
            value="\n".join(f"• #{c}" for c in ec) or "*(none)*",
            inline=False,
        )

        await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(ServerSetup(bot))
