# cogs/new_member_roles.py
#
# Order:
# 1) If "EVE online" role is added for a NEW player -> add "ARC Subsidized" and ensure "ARC Security" is NOT present.
# 2) On join -> immediately add "New Member".
# 3) When "New Member" is removed -> add "Onboarding" + "Scheduling".
# + /fix_roles (LEADERSHIP) -> remove Onboarding/Scheduling from anyone who still has New Member.
#
# CHANGE (YOUR REQUEST):
# - Track/log whenever the "ARC Security" role is GIVEN to a member (any source),
#   and post it in #roles-log with best-effort actor detection (audit logs).
#
# Keeps the existing slash commands + restrictions; /fix_roles is set to leadership-only.

import discord
from discord.ext import commands
from discord import app_commands
import asyncio
import json
import time
import os
import aiohttp
from datetime import datetime, timedelta, timezone

DATA_DIR = "/data"
DATA_FILE = os.path.join(DATA_DIR, "member_roles.json")

NEW_MEMBER_ROLE = "New Member"          # display / log strings only
NEW_MEMBER_ROLE_ID = 1419837428146901013  # authoritative lookup – immune to renames
SECURITY_ROLE = "ARC Security"
SUBSIDIZED_ROLE = "ARC Subsidized"
SCHEDULING_ROLE = "Scheduling"
ONBOARDING_ROLE = "Onboarding"

EVE_ROLE = "EVE online"

GENESIS_ROLE = "ARC Genesis"
DIRECTOR_ROLE = "ARC Security Administration Council"
CEO_ROLE = "ARC Security Corporation Leader"

LOG_CHANNEL_NAME = "audit-log"

NEW_MEMBER_DM = (
    "You've been identified by this bot as a new member of discord server Cryonic Gaming. Welcome! "
    " Since we don't know you yet, your chat access is temporarily suspended. You aren't in trouble, "
    "but you are required to have a brief talk with a moderator to regain access and to receive information "
    "needed for community safety and your convenience. 🙂\n"
    "Click the link below to create a ticket. A human moderator will respond within 24 hours.\n"
    "https://discord.com/channels/559041517663289344/1459204314882117662"
)

NEW_PLAYER_WINDOW_DAYS = 14
ROLE_OP_THROTTLE_SECONDS = 0.35


# ----------------- Persistence -----------------

def ensure_data():
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(DATA_FILE):
        with open(DATA_FILE, "w") as f:
            json.dump({"pending": {}, "rewarded": []}, f, indent=2)


def load_data():
    ensure_data()
    with open(DATA_FILE, "r") as f:
        data = json.load(f)

    if "pending" not in data or not isinstance(data["pending"], dict):
        data["pending"] = {}
    if "rewarded" not in data or not isinstance(data["rewarded"], list):
        data["rewarded"] = []
    return data


def save_data(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)


# ----------------- Robust Discord HTTP retry -----------------

async def _retry_discord_http(action_coro_factory,
                             *,
                             attempts: int = 5,
                             base_delay: float = 2.0,
                             max_delay: float = 45.0):
    last_exc = None
    for i in range(attempts):
        try:
            return await action_coro_factory()
        except (aiohttp.ClientConnectionError,
                aiohttp.ClientConnectorError,
                aiohttp.ServerTimeoutError,
                aiohttp.ClientOSError,
                asyncio.TimeoutError) as e:
            last_exc = e
        except discord.HTTPException as e:
            last_exc = e
            if 400 <= getattr(e, "status", 0) < 500 and getattr(e, "status", 0) != 429:
                raise
        except (discord.Forbidden, discord.NotFound):
            raise

        delay = min(max_delay, base_delay * (2 ** i))
        delay = delay * (0.85 + 0.3 * ((asyncio.get_running_loop().time() % 1.0)))
        await asyncio.sleep(delay)

    raise last_exc


# ----------------- Cog -----------------

class NewMemberRoles(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        ensure_data()
        self._data_lock = asyncio.Lock()

        # prevents spamming multiple security logs in rapid succession for same user
        self._security_log_guard: dict[int, float] = {}

        # prevents sending duplicate DMs if role event fires more than once quickly
        self._new_member_dm_guard: dict[int, float] = {}

        self._boot_task = bot.loop.create_task(self.bootstrap_existing_members())

    def cog_unload(self):
        if hasattr(self, "_boot_task") and self._boot_task and not self._boot_task.done():
            self._boot_task.cancel()

    # ---------- Utilities ----------

    def get_role(self, guild: discord.Guild, name: str):
        return discord.utils.get(guild.roles, name=name)

    def get_new_member_role(self, guild: discord.Guild):
        """Always look up New Member by ID so renames don't break anything."""
        return discord.utils.get(guild.roles, id=NEW_MEMBER_ROLE_ID)

    def get_log_channel(self, guild: discord.Guild):
        return discord.utils.get(guild.text_channels, name=LOG_CHANNEL_NAME)

    async def log(self, guild: discord.Guild, message: str):
        channel = self.get_log_channel(guild)
        if channel:
            try:
                await _retry_discord_http(lambda: channel.send(message), attempts=3, base_delay=1.0, max_delay=10.0)
            except Exception:
                pass

    async def _safe_add_roles(self, member: discord.Member, *roles: discord.Role, reason: str):
        roles = [r for r in roles if r is not None]
        if not roles:
            return
        await _retry_discord_http(lambda: member.add_roles(*roles, reason=reason))
        await asyncio.sleep(ROLE_OP_THROTTLE_SECONDS)

    async def _safe_remove_roles(self, member: discord.Member, *roles: discord.Role, reason: str):
        roles = [r for r in roles if r is not None]
        if not roles:
            return
        await _retry_discord_http(lambda: member.remove_roles(*roles, reason=reason))
        await asyncio.sleep(ROLE_OP_THROTTLE_SECONDS)

    # ---------- Audit helper for Security role logs ----------

    async def _find_role_update_actor(self, guild: discord.Guild, target_id: int) -> str:
        """
        Best-effort actor resolution:
        - tries audit logs
        - returns mention or a short fallback string
        """
        actor = "Unknown"
        try:
            async for entry in guild.audit_logs(limit=8, action=discord.AuditLogAction.member_role_update):
                if entry.target and getattr(entry.target, "id", None) == target_id:
                    # Optional: could check entry.created_at recency here; keep it simple/robust.
                    if entry.user:
                        actor = entry.user.mention
                    break
        except discord.Forbidden:
            actor = "Audit log unavailable"
        except Exception:
            actor = "Audit log error"
        return actor

    async def _log_security_role_granted(self, before: discord.Member, after: discord.Member):
        """
        Logs when ARC Security is newly added.
        Includes actor if we can infer via audit logs.
        Includes a small dedupe guard to avoid duplicate logs in a few seconds window.
        """
        try:
            if before.guild.id != after.guild.id:
                return
            if before.id != after.id:
                return

            before_roles = {r.name for r in before.roles}
            after_roles = {r.name for r in after.roles}

            if SECURITY_ROLE not in after_roles or SECURITY_ROLE in before_roles:
                return  # not newly granted

            now = time.time()
            last = self._security_log_guard.get(after.id, 0.0)
            if (now - last) < 5.0:
                return
            self._security_log_guard[after.id] = now

            actor = await self._find_role_update_actor(after.guild, after.id)

            await self.log(
                after.guild,
                f"🛡️ **{SECURITY_ROLE} granted**: {after.mention} received **{SECURITY_ROLE}**"
                + (f" (by {actor})." if actor else ".")
            )
        except Exception:
            # never break other role flows
            return

    # ---------- New Member DM ----------

    async def _send_new_member_dm(self, member: discord.Member):
        """
        Send the welcome/suspension DM when 'New Member' role is assigned,
        then log the outcome to #roles-log.
        Includes a 5-second dedup guard to avoid double sends on rapid events.
        """
        now = time.time()
        last = self._new_member_dm_guard.get(member.id, 0.0)
        if (now - last) < 5.0:
            return
        self._new_member_dm_guard[member.id] = now

        dm_status: str
        try:
            await _retry_discord_http(
                lambda: member.send(NEW_MEMBER_DM),
                attempts=3,
                base_delay=1.0,
                max_delay=10.0,
            )
            dm_status = "✅ DM sent successfully."
        except discord.Forbidden:
            dm_status = "⚠️ DM could not be sent (user has DMs disabled or has blocked the bot)."
        except Exception as e:
            dm_status = f"⚠️ DM failed ({type(e).__name__})."

        await self.log(
            member.guild,
            f"📨 **New Member DM** → {member.mention}: {dm_status}",
        )

    # ---------- New Player Detection ----------

    async def is_new_player(self, member: discord.Member) -> bool:
        guild = member.guild
        nm_role = self.get_new_member_role(guild)

        if nm_role and nm_role in member.roles:
            return True

        async with self._data_lock:
            data = load_data()
            if str(member.id) in data.get("pending", {}):
                return True

        if member.joined_at:
            cutoff = datetime.now(timezone.utc) - timedelta(days=NEW_PLAYER_WINDOW_DAYS)
            if member.joined_at >= cutoff:
                return True

        return False

    # ---------- Rule 1: EVE role added (new player) -> Subsidized ON, Security OFF ----------

    async def handle_eve_role_added(self, member: discord.Member, reason: str = "EVE role trigger"):
        guild = member.guild
        eve = self.get_role(guild, EVE_ROLE)
        subsidized = self.get_role(guild, SUBSIDIZED_ROLE)
        security = self.get_role(guild, SECURITY_ROLE)

        if not eve or eve not in member.roles:
            return
        if not subsidized:
            await self.log(guild, f"⚠️ Missing role **{SUBSIDIZED_ROLE}**; cannot apply EVE trigger for {member.mention}.")
            return

        if not await self.is_new_player(member):
            return

        try:
            changed_bits = []

            if subsidized not in member.roles:
                await self._safe_add_roles(member, subsidized, reason=f"{reason} (ensure subsidized)")
                changed_bits.append(f"added **{SUBSIDIZED_ROLE}**")

            if security and security in member.roles:
                await self._safe_remove_roles(member, security, reason=f"{reason} (ensure no security)")
                changed_bits.append(f"removed **{SECURITY_ROLE}**")

            if changed_bits:
                await self.log(
                    guild,
                    f"🟦 **EVE trigger (new player)**: {member.mention} received **{EVE_ROLE}** → "
                    + ", ".join(changed_bits) + "."
                )

        except discord.Forbidden:
            await self.log(guild, f"⚠️ EVE trigger failed for {member.mention}: missing permissions / role hierarchy.")
        except discord.NotFound:
            pass
        except Exception as e:
            await self.log(guild, f"⚠️ EVE trigger error for {member.mention}: {type(e).__name__}")

    # ---------- Bootstrap Existing Members ----------

    async def bootstrap_existing_members(self):
        await self.bot.wait_until_ready()

        async with self._data_lock:
            data = load_data()

        changed = False

        for guild in self.bot.guilds:
            nm_role = self.get_new_member_role(guild)
            if nm_role:
                for member in guild.members:
                    if nm_role in member.roles:
                        uid = str(member.id)
                        if uid not in data["rewarded"]:
                            data["rewarded"].append(uid)
                            changed = True

        if changed:
            async with self._data_lock:
                save_data(data)

    # ---------- Member Join Handling (Rule 2) ----------

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        uid = str(member.id)

        async with self._data_lock:
            data = load_data()
            if uid in data.get("rewarded", []):
                await self.log(
                    member.guild,
                    f"↩️ {member.mention} rejoined and was already onboarded previously — skipping the **{NEW_MEMBER_ROLE}** flow."
                )
                return

        role = self.get_new_member_role(member.guild)
        if role and role not in member.roles:
            try:
                await self._safe_add_roles(member, role, reason="Auto New Member role on join")
                await self.log(member.guild, f"🟢 **New Member added** to {member.mention} (on join).")
            except discord.Forbidden:
                await self.log(member.guild, f"⚠️ Could not add **{NEW_MEMBER_ROLE}** to {member.mention} (permissions/hierarchy).")
            except Exception:
                await self.log(member.guild, f"⚠️ Could not add **{NEW_MEMBER_ROLE}** to {member.mention} (transient error).")

    # ---------- Role Update Handling ----------

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        before_roles = {r.name for r in before.roles}
        after_roles = {r.name for r in after.roles}
        before_role_ids = {r.id for r in before.roles}
        after_role_ids = {r.id for r in after.roles}

        # NEW: log when ARC Security is granted
        if (SECURITY_ROLE in after_roles) and (SECURITY_ROLE not in before_roles):
            await self._log_security_role_granted(before, after)

        if (EVE_ROLE in after_roles) and (EVE_ROLE not in before_roles):
            await self.handle_eve_role_added(after, reason="EVE role added")

        # NEW: send DM when New Member role is granted (auto timer or manual)
        if (NEW_MEMBER_ROLE_ID in after_role_ids) and (NEW_MEMBER_ROLE_ID not in before_role_ids):
            await self._send_new_member_dm(after)

        if NEW_MEMBER_ROLE_ID in before_role_ids and NEW_MEMBER_ROLE_ID not in after_role_ids:
            actor = "Unknown"
            try:
                async for entry in after.guild.audit_logs(limit=5, action=discord.AuditLogAction.member_role_update):
                    if entry.target and entry.target.id == after.id:
                        actor = entry.user.mention
                        break
            except discord.Forbidden:
                actor = "Audit log unavailable"
            except Exception:
                actor = "Audit log error"

            await self.log(after.guild, f"🔴 **New Member removed** from {after.mention} by {actor}")

            uid = str(after.id)

            async with self._data_lock:
                data = load_data()
                already_rewarded = uid in data.get("rewarded", [])

            if already_rewarded:
                return

            sched = self.get_role(after.guild, SCHEDULING_ROLE)
            onboard = self.get_role(after.guild, ONBOARDING_ROLE)

            roles_to_add = []
            if sched and sched not in after.roles:
                roles_to_add.append(sched)
            if onboard and onboard not in after.roles:
                roles_to_add.append(onboard)

            if roles_to_add:
                try:
                    await self._safe_add_roles(after, *roles_to_add, reason="New Member removed -> grant Scheduling + Onboarding")
                    await self.log(
                        after.guild,
                        f"🟣 **Post-New Member**: {after.mention} granted "
                        + " + ".join(f"**{r.name}**" for r in roles_to_add)
                        + "."
                    )
                except discord.Forbidden:
                    await self.log(after.guild, f"⚠️ Could not add Scheduling/Onboarding to {after.mention} (permissions/hierarchy).")
                    return
                except Exception:
                    await self.log(after.guild, f"⚠️ Could not add Scheduling/Onboarding to {after.mention} (transient error).")
                    return

            async with self._data_lock:
                data = load_data()
                data.setdefault("rewarded", [])
                if uid not in data["rewarded"]:
                    data["rewarded"].append(uid)
                    save_data(data)

    # ---------- Permission Check (robust) ----------

    def leadership_only():
        allowed = {GENESIS_ROLE, DIRECTOR_ROLE, CEO_ROLE}
        async def predicate(interaction: discord.Interaction):
            member = interaction.user
            if not isinstance(member, discord.Member):
                return False
            return any(r.name in allowed for r in member.roles)
        return app_commands.check(predicate)

    def genesis_only():
        async def predicate(interaction: discord.Interaction):
            member = interaction.user
            if not isinstance(member, discord.Member):
                return False
            return any(r.name == GENESIS_ROLE for r in member.roles)
        return app_commands.check(predicate)

    # Optional: nicer error when a check fails
    @commands.Cog.listener()
    async def on_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.CheckFailure):
            try:
                if interaction.response.is_done():
                    await interaction.followup.send("❌ You don't have permission to use this command.", ephemeral=True)
                else:
                    await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
            except Exception:
                pass

    # ---------- Slash Commands ----------

    @app_commands.command(
        name="transfer_to_security",
        description="Transfer a member from ARC Subsidized to ARC Security"
    )
    @leadership_only()
    async def transfer_to_security(self, interaction: discord.Interaction, member: discord.Member):
        guild = interaction.guild
        if not guild:
            await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
            return

        security = self.get_role(guild, SECURITY_ROLE)
        subsidized = self.get_role(guild, SUBSIDIZED_ROLE)

        if not security or not subsidized:
            await interaction.response.send_message(
                f"Missing roles: ensure **{SECURITY_ROLE}** and **{SUBSIDIZED_ROLE}** exist.",
                ephemeral=True
            )
            return

        if subsidized not in member.roles:
            await interaction.response.send_message(
                f"{member.mention} does not have **{SUBSIDIZED_ROLE}**.",
                ephemeral=True
            )
            return

        try:
            await self._safe_add_roles(member, security, reason=f"Transfer to security by {interaction.user}")
            await self._safe_remove_roles(member, subsidized, reason=f"Transfer to security by {interaction.user}")

            await self.log(
                guild,
                f"🟦 **Transfer**: {interaction.user.mention} transferred {member.mention} "
                f"from **{SUBSIDIZED_ROLE}** → **{SECURITY_ROLE}**."
            )

            if interaction.response.is_done():
                await interaction.followup.send(
                    f"✅ Transferred {member.mention} to **{SECURITY_ROLE}** (removed **{SUBSIDIZED_ROLE}**).",
                    ephemeral=True
                )
            else:
                await interaction.response.send_message(
                    f"✅ Transferred {member.mention} to **{SECURITY_ROLE}** (removed **{SUBSIDIZED_ROLE}**).",
                    ephemeral=True
                )
        except discord.Forbidden:
            await interaction.response.send_message(
                "❌ I don't have permission (check role hierarchy / Manage Roles).",
                ephemeral=True
            )
        except Exception as e:
            await interaction.response.send_message(
                f"❌ Discord API/network error: {type(e).__name__}",
                ephemeral=True
            )

    @app_commands.command(
        name="remove_scheduling",
        description="Remove Scheduling role from a member"
    )
    @genesis_only()
    async def remove_scheduling(self, interaction: discord.Interaction, member: discord.Member):
        role = self.get_role(interaction.guild, SCHEDULING_ROLE)
        if role and role in member.roles:
            try:
                await self._safe_remove_roles(member, role, reason=f"Removed by {interaction.user}")
            except Exception:
                pass
            await interaction.response.send_message(
                f"Scheduling role removed from {member.mention}.",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                "Member does not have Scheduling role.",
                ephemeral=True
            )

    @app_commands.command(
        name="remove_onboarding",
        description="Remove Onboarding role from a member"
    )
    @genesis_only()
    async def remove_onboarding(self, interaction: discord.Interaction, member: discord.Member):
        role = self.get_role(interaction.guild, ONBOARDING_ROLE)
        if role and role in member.roles:
            try:
                await self._safe_remove_roles(member, role, reason=f"Removed by {interaction.user}")
            except Exception:
                pass
            await interaction.response.send_message(
                f"Onboarding role removed from {member.mention}.",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                "Member does not have Onboarding role.",
                ephemeral=True
            )

    @app_commands.command(
        name="rollback_subsidy_security",
        description="Rollback: remove ARC Subsidized from anyone who has ARC Security."
    )
    @genesis_only()
    async def rollback_subsidy_security(self, interaction: discord.Interaction):
        guild = interaction.guild
        if not guild:
            await interaction.response.send_message("Use this in a server.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        security = self.get_role(guild, SECURITY_ROLE)
        subsidized = self.get_role(guild, SUBSIDIZED_ROLE)

        if not security or not subsidized:
            await interaction.followup.send(
                f"Missing roles. Need **{SECURITY_ROLE}** and **{SUBSIDIZED_ROLE}** to exist.",
                ephemeral=True
            )
            return

        removed = 0
        failed = 0

        try:
            async for member in guild.fetch_members(limit=None):
                if security not in member.roles:
                    continue
                if subsidized not in member.roles:
                    continue

                try:
                    await self._safe_remove_roles(
                        member, subsidized,
                        reason="Rollback: Subsidized was mistakenly granted to Security members"
                    )
                    removed += 1
                except (discord.Forbidden, discord.NotFound):
                    failed += 1
                except Exception:
                    failed += 1

        except discord.Forbidden:
            await interaction.followup.send(
                "❌ I can't fetch members. Enable **Server Members Intent** and make sure I have permission.",
                ephemeral=True
            )
            return

        await self.log(
            guild,
            f"🧹 **Rollback complete**: removed **{SUBSIDIZED_ROLE}** from {removed} members who had **{SECURITY_ROLE}** "
            f"(failed {failed})."
        )

        await interaction.followup.send(
            f"✅ Rollback done.\nRemoved: {removed}\nFailed: {failed}\n"
            f"Details posted in **#{LOG_CHANNEL_NAME}** (if it exists).",
            ephemeral=True
        )

    @app_commands.command(
        name="fix_roles",
        description="Fix: remove Onboarding/Scheduling from anyone who still has New Member."
    )
    @leadership_only()
    async def fix_roles(self, interaction: discord.Interaction):
        guild = interaction.guild
        if not guild:
            await interaction.response.send_message("Use this in a server.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        nm = self.get_new_member_role(guild)
        sched = self.get_role(guild, SCHEDULING_ROLE)
        onboard = self.get_role(guild, ONBOARDING_ROLE)

        if not nm:
            await interaction.followup.send(
                f"Missing role: **{NEW_MEMBER_ROLE}** does not exist.",
                ephemeral=True
            )
            return

        if not sched and not onboard:
            await interaction.followup.send(
                "Nothing to do: Scheduling/Onboarding roles are missing.",
                ephemeral=True
            )
            return

        updated = 0
        removed_roles_total = 0
        failed = 0

        try:
            async for member in guild.fetch_members(limit=None):
                if nm not in member.roles:
                    continue

                to_remove = []
                if sched and sched in member.roles:
                    to_remove.append(sched)
                if onboard and onboard in member.roles:
                    to_remove.append(onboard)

                if not to_remove:
                    continue

                try:
                    await self._safe_remove_roles(
                        member, *to_remove,
                        reason="Fix roles: New Member should not have Onboarding/Scheduling"
                    )
                    updated += 1
                    removed_roles_total += len(to_remove)
                except (discord.Forbidden, discord.NotFound):
                    failed += 1
                except Exception:
                    failed += 1

        except discord.Forbidden:
            await interaction.followup.send(
                "❌ I can't fetch members. Enable **Server Members Intent** and make sure I have permission.",
                ephemeral=True
            )
            return

        await self.log(
            guild,
            f"🛠️ **/fix_roles complete**: updated {updated} members (removed {removed_roles_total} roles total). Failed: {failed}."
        )

        await interaction.followup.send(
            f"✅ Fix complete.\n"
            f"Members updated: {updated}\n"
            f"Roles removed (total): {removed_roles_total}\n"
            f"Failed: {failed}\n"
            f"Details posted in **#{LOG_CHANNEL_NAME}** (if it exists).",
            ephemeral=True
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(NewMemberRoles(bot))