# cogs/audit_log.py
#
# Comprehensive server audit log — posts rich embeds to #audit-log
# for every observable change in the guild.
#
# EVENTS COVERED
# ──────────────
# Roles:        role create / delete / update (name, colour, perms, position)
# Members:      role add/remove, nickname, timeout, join, leave, ban, unban
# Messages:     edit (shows before → after), delete (shows original content),
#               bulk delete (summary + attachment)
# Channels:     create / delete / update (name, topic, perms, slowmode, NSFW, etc.)
# Voice:        join / leave / move / server mute / server deafen
# Server:       guild setting changes (name, icon, verification level, etc.)
#
# DESIGN
# ──────
# • Pure observer — never modifies roles, channels, or members.
# • All embeds include a timestamp; actor is resolved from audit logs
#   where Discord provides it (role changes, bans, kicks, channel edits).
# • Message cache: discord.py only fires on_message_edit / on_message_delete
#   for messages in the internal cache. Ensure the bot's Intents include
#   message_content and members for full coverage.
# • The cog does NOT create #audit-log — it expects the channel to exist.
#   If the channel is missing, events are silently skipped.

import discord
from discord.ext import commands
from datetime import datetime, timezone
from typing import Optional, List

AUDIT_LOG_CHANNEL = "audit-log"

# Maximum characters of message content to show in embeds
_MSG_PREVIEW_LEN = 1024


class AuditLog(commands.Cog):
    """Comprehensive guild audit logger."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _ch(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        return discord.utils.get(guild.text_channels, name=AUDIT_LOG_CHANNEL)

    async def _send(
        self, guild: discord.Guild, embed: discord.Embed, **kwargs
    ) -> None:
        ch = self._ch(guild)
        if not ch:
            return
        try:
            await ch.send(embed=embed, **kwargs)
        except (discord.Forbidden, discord.HTTPException):
            pass

    @staticmethod
    def _ts() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _trunc(text: Optional[str], limit: int = _MSG_PREVIEW_LEN) -> str:
        if not text:
            return "*empty*"
        if len(text) <= limit:
            return text
        return text[: limit - 1] + "…"

    async def _audit_actor(
        self,
        guild: discord.Guild,
        action: discord.AuditLogAction,
        target_id: int,
    ) -> str:
        """Best-effort actor lookup from Discord audit logs."""
        try:
            async for entry in guild.audit_logs(limit=5, action=action):
                if entry.target and getattr(entry.target, "id", None) == target_id:
                    if entry.user:
                        return entry.user.mention
                    break
        except (discord.Forbidden, discord.HTTPException):
            pass
        return "Unknown"

    # =====================================================================
    # ROLE EVENTS
    # =====================================================================

    @commands.Cog.listener()
    async def on_guild_role_create(self, role: discord.Role) -> None:
        actor = await self._audit_actor(
            role.guild, discord.AuditLogAction.role_create, role.id
        )
        embed = discord.Embed(
            title="➕ Role Created",
            colour=role.colour if role.colour.value else discord.Colour.green(),
            timestamp=self._ts(),
        )
        embed.add_field(name="Role", value=f"{role.mention} (`{role.name}`)", inline=True)
        embed.add_field(name="ID", value=str(role.id), inline=True)
        embed.add_field(name="Created by", value=actor, inline=True)
        embed.add_field(
            name="Permissions",
            value=f"`{role.permissions.value}`",
            inline=False,
        )
        await self._send(role.guild, embed)

    @commands.Cog.listener()
    async def on_guild_role_delete(self, role: discord.Role) -> None:
        actor = await self._audit_actor(
            role.guild, discord.AuditLogAction.role_delete, role.id
        )
        embed = discord.Embed(
            title="🗑️ Role Deleted",
            colour=discord.Colour.red(),
            timestamp=self._ts(),
        )
        embed.add_field(name="Role", value=f"`{role.name}`", inline=True)
        embed.add_field(name="ID", value=str(role.id), inline=True)
        embed.add_field(name="Deleted by", value=actor, inline=True)
        await self._send(role.guild, embed)

    @commands.Cog.listener()
    async def on_guild_role_update(
        self, before: discord.Role, after: discord.Role
    ) -> None:
        changes: List[str] = []

        if before.name != after.name:
            changes.append(f"**Name:** `{before.name}` → `{after.name}`")
        if before.colour != after.colour:
            changes.append(
                f"**Colour:** `{before.colour}` → `{after.colour}`"
            )
        if before.hoist != after.hoist:
            changes.append(
                f"**Hoisted:** `{before.hoist}` → `{after.hoist}`"
            )
        if before.mentionable != after.mentionable:
            changes.append(
                f"**Mentionable:** `{before.mentionable}` → `{after.mentionable}`"
            )
        if before.permissions != after.permissions:
            added = after.permissions.value & ~before.permissions.value
            removed = before.permissions.value & ~after.permissions.value
            parts: List[str] = []
            if added:
                added_perms = [
                    perm for perm, val in discord.Permissions(added)
                    if val
                ]
                parts.append("✅ " + ", ".join(f"`{p}`" for p in added_perms))
            if removed:
                removed_perms = [
                    perm for perm, val in discord.Permissions(removed)
                    if val
                ]
                parts.append("❌ " + ", ".join(f"`{p}`" for p in removed_perms))
            changes.append("**Permissions:**\n" + "\n".join(parts))
        if before.position != after.position:
            changes.append(
                f"**Position:** `{before.position}` → `{after.position}`"
            )

        if not changes:
            return  # icon-only or irrelevant change

        actor = await self._audit_actor(
            after.guild, discord.AuditLogAction.role_update, after.id
        )

        embed = discord.Embed(
            title="✏️ Role Updated",
            colour=after.colour if after.colour.value else discord.Colour.orange(),
            timestamp=self._ts(),
        )
        embed.add_field(
            name="Role", value=f"{after.mention} (`{after.name}`)", inline=True
        )
        embed.add_field(name="Updated by", value=actor, inline=True)
        embed.add_field(
            name="Changes",
            value=self._trunc("\n".join(changes)),
            inline=False,
        )
        await self._send(after.guild, embed)

    # =====================================================================
    # MEMBER EVENTS
    # =====================================================================

    @commands.Cog.listener()
    async def on_member_update(
        self, before: discord.Member, after: discord.Member
    ) -> None:
        guild = after.guild

        # ── Role changes ─────────────────────────────────────────────────
        before_roles = set(before.roles)
        after_roles = set(after.roles)
        added = after_roles - before_roles
        removed = before_roles - after_roles

        if added or removed:
            actor = await self._audit_actor(
                guild, discord.AuditLogAction.member_role_update, after.id
            )
            embed = discord.Embed(
                title="🛡️ Member Roles Changed",
                colour=discord.Colour.blue(),
                timestamp=self._ts(),
            )
            embed.add_field(name="Member", value=f"{after.mention} (`{after}`)", inline=True)
            embed.add_field(name="By", value=actor, inline=True)
            if added:
                embed.add_field(
                    name="➕ Added",
                    value=", ".join(r.mention for r in sorted(added, key=lambda r: r.position, reverse=True)),
                    inline=False,
                )
            if removed:
                embed.add_field(
                    name="➖ Removed",
                    value=", ".join(f"`{r.name}`" for r in sorted(removed, key=lambda r: r.position, reverse=True)),
                    inline=False,
                )
            await self._send(guild, embed)

        # ── Nickname change ──────────────────────────────────────────────
        if before.nick != after.nick:
            actor = await self._audit_actor(
                guild, discord.AuditLogAction.member_update, after.id
            )
            embed = discord.Embed(
                title="📝 Nickname Changed",
                colour=discord.Colour.greyple(),
                timestamp=self._ts(),
            )
            embed.add_field(name="Member", value=f"{after.mention} (`{after}`)", inline=True)
            embed.add_field(name="By", value=actor, inline=True)
            embed.add_field(name="Before", value=f"`{before.nick or '(none)'}`", inline=True)
            embed.add_field(name="After", value=f"`{after.nick or '(none)'}`", inline=True)
            await self._send(guild, embed)

        # ── Timeout change ───────────────────────────────────────────────
        before_timeout = before.timed_out_until
        after_timeout = after.timed_out_until
        if before_timeout != after_timeout:
            if after_timeout and after_timeout > datetime.now(timezone.utc):
                actor = await self._audit_actor(
                    guild, discord.AuditLogAction.member_update, after.id
                )
                embed = discord.Embed(
                    title="⏱️ Member Timed Out",
                    colour=discord.Colour.dark_orange(),
                    timestamp=self._ts(),
                )
                embed.add_field(name="Member", value=after.mention, inline=True)
                embed.add_field(name="By", value=actor, inline=True)
                embed.add_field(
                    name="Until",
                    value=f"<t:{int(after_timeout.timestamp())}:F>",
                    inline=False,
                )
                await self._send(guild, embed)
            elif before_timeout and (not after_timeout or after_timeout <= datetime.now(timezone.utc)):
                embed = discord.Embed(
                    title="✅ Timeout Removed",
                    colour=discord.Colour.green(),
                    timestamp=self._ts(),
                )
                embed.add_field(name="Member", value=after.mention, inline=True)
                await self._send(guild, embed)

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member) -> None:
        embed = discord.Embed(
            title="📥 Member Joined",
            colour=discord.Colour.green(),
            timestamp=self._ts(),
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="Member", value=f"{member.mention} (`{member}`)", inline=True)
        embed.add_field(name="ID", value=str(member.id), inline=True)
        created = int(member.created_at.timestamp())
        embed.add_field(name="Account Created", value=f"<t:{created}:R>", inline=True)
        await self._send(member.guild, embed)

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member) -> None:
        # Try to determine if this was a kick
        actor = "Left voluntarily"
        try:
            async for entry in member.guild.audit_logs(
                limit=5, action=discord.AuditLogAction.kick
            ):
                if entry.target and entry.target.id == member.id:
                    delta = (datetime.now(timezone.utc) - entry.created_at).total_seconds()
                    if delta < 10:
                        actor = f"Kicked by {entry.user.mention}" if entry.user else "Kicked"
                        if entry.reason:
                            actor += f"\nReason: {entry.reason}"
                    break
        except (discord.Forbidden, discord.HTTPException):
            pass

        roles = [r.name for r in member.roles if r != member.guild.default_role]

        embed = discord.Embed(
            title="📤 Member Left",
            colour=discord.Colour.red(),
            timestamp=self._ts(),
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="Member", value=f"`{member}` ({member.id})", inline=True)
        embed.add_field(name="Status", value=actor, inline=True)
        if roles:
            embed.add_field(
                name="Roles Held",
                value=self._trunc(", ".join(roles), 1024),
                inline=False,
            )
        await self._send(member.guild, embed)

    @commands.Cog.listener()
    async def on_member_ban(
        self, guild: discord.Guild, user: discord.User
    ) -> None:
        actor = "Unknown"
        reason = ""
        try:
            async for entry in guild.audit_logs(
                limit=5, action=discord.AuditLogAction.ban
            ):
                if entry.target and entry.target.id == user.id:
                    actor = entry.user.mention if entry.user else "Unknown"
                    reason = entry.reason or ""
                    break
        except (discord.Forbidden, discord.HTTPException):
            pass

        embed = discord.Embed(
            title="🔨 Member Banned",
            colour=discord.Colour.dark_red(),
            timestamp=self._ts(),
        )
        embed.add_field(name="User", value=f"`{user}` ({user.id})", inline=True)
        embed.add_field(name="Banned by", value=actor, inline=True)
        if reason:
            embed.add_field(name="Reason", value=reason[:1024], inline=False)
        await self._send(guild, embed)

    @commands.Cog.listener()
    async def on_member_unban(
        self, guild: discord.Guild, user: discord.User
    ) -> None:
        actor = await self._audit_actor(
            guild, discord.AuditLogAction.unban, user.id
        )
        embed = discord.Embed(
            title="🔓 Member Unbanned",
            colour=discord.Colour.green(),
            timestamp=self._ts(),
        )
        embed.add_field(name="User", value=f"`{user}` ({user.id})", inline=True)
        embed.add_field(name="Unbanned by", value=actor, inline=True)
        await self._send(guild, embed)

    # =====================================================================
    # MESSAGE EVENTS
    # =====================================================================

    @commands.Cog.listener()
    async def on_message_edit(
        self, before: discord.Message, after: discord.Message
    ) -> None:
        if not after.guild:
            return
        if after.author.bot:
            return
        if before.content == after.content:
            return  # embed-only update (link preview, etc.)

        embed = discord.Embed(
            title="✏️ Message Edited",
            colour=discord.Colour.gold(),
            timestamp=self._ts(),
        )
        embed.add_field(
            name="Author",
            value=f"{after.author.mention} (`{after.author}`)",
            inline=True,
        )
        embed.add_field(
            name="Channel",
            value=after.channel.mention,
            inline=True,
        )
        embed.add_field(
            name="Before",
            value=self._trunc(before.content),
            inline=False,
        )
        embed.add_field(
            name="After",
            value=self._trunc(after.content),
            inline=False,
        )
        embed.add_field(
            name="Jump",
            value=f"[Go to message]({after.jump_url})",
            inline=False,
        )
        await self._send(after.guild, embed)

    @commands.Cog.listener()
    async def on_message_delete(self, message: discord.Message) -> None:
        if not message.guild:
            return
        if message.author.bot:
            return

        # Try to find who deleted it
        actor = message.author.mention  # self-delete by default
        try:
            async for entry in message.guild.audit_logs(
                limit=5, action=discord.AuditLogAction.message_delete
            ):
                if (
                    entry.target
                    and entry.target.id == message.author.id
                    and getattr(entry.extra, "channel", None)
                    and entry.extra.channel.id == message.channel.id
                ):
                    delta = (datetime.now(timezone.utc) - entry.created_at).total_seconds()
                    if delta < 10:
                        actor = entry.user.mention if entry.user else actor
                    break
        except (discord.Forbidden, discord.HTTPException):
            pass

        embed = discord.Embed(
            title="🗑️ Message Deleted",
            colour=discord.Colour.red(),
            timestamp=self._ts(),
        )
        embed.add_field(
            name="Author",
            value=f"{message.author.mention} (`{message.author}`)",
            inline=True,
        )
        embed.add_field(name="Channel", value=message.channel.mention, inline=True)
        embed.add_field(name="Deleted by", value=actor, inline=True)
        embed.add_field(
            name="Content",
            value=self._trunc(message.content),
            inline=False,
        )

        # Log attachments
        if message.attachments:
            att_lines = [f"`{a.filename}` ({a.size} bytes)" for a in message.attachments[:5]]
            embed.add_field(
                name="Attachments",
                value="\n".join(att_lines),
                inline=False,
            )
        await self._send(message.guild, embed)

    @commands.Cog.listener()
    async def on_bulk_message_delete(
        self, messages: List[discord.Message]
    ) -> None:
        if not messages:
            return
        guild = messages[0].guild
        if not guild:
            return

        channel = messages[0].channel

        embed = discord.Embed(
            title="🗑️ Bulk Message Delete",
            colour=discord.Colour.dark_red(),
            timestamp=self._ts(),
        )
        embed.add_field(name="Channel", value=channel.mention, inline=True)
        embed.add_field(name="Count", value=str(len(messages)), inline=True)

        # Build a text file with all deleted messages
        lines: List[str] = []
        for msg in sorted(messages, key=lambda m: m.created_at):
            ts = msg.created_at.strftime("%Y-%m-%d %H:%M:%S UTC")
            content = msg.content or "(no text)"
            lines.append(f"[{ts}] {msg.author} ({msg.author.id}): {content}")

        import io
        file_bytes = "\n".join(lines).encode("utf-8")
        file = discord.File(
            fp=io.BytesIO(file_bytes),
            filename=f"bulk_delete_{channel.id}_{int(datetime.now(timezone.utc).timestamp())}.txt",
        )
        embed.add_field(
            name="Details",
            value="Full content attached as text file.",
            inline=False,
        )

        ch = self._ch(guild)
        if ch:
            try:
                await ch.send(embed=embed, file=file)
            except (discord.Forbidden, discord.HTTPException):
                pass

    # =====================================================================
    # CHANNEL EVENTS
    # =====================================================================

    @commands.Cog.listener()
    async def on_guild_channel_create(
        self, channel: discord.abc.GuildChannel
    ) -> None:
        actor = await self._audit_actor(
            channel.guild, discord.AuditLogAction.channel_create, channel.id
        )
        ch_type = str(channel.type).replace("ChannelType.", "").replace("_", " ").title()
        embed = discord.Embed(
            title="📁 Channel Created",
            colour=discord.Colour.green(),
            timestamp=self._ts(),
        )
        embed.add_field(name="Channel", value=f"{channel.mention} (`{channel.name}`)", inline=True)
        embed.add_field(name="Type", value=ch_type, inline=True)
        embed.add_field(name="Created by", value=actor, inline=True)
        if hasattr(channel, "category") and channel.category:
            embed.add_field(name="Category", value=channel.category.name, inline=True)
        await self._send(channel.guild, embed)

    @commands.Cog.listener()
    async def on_guild_channel_delete(
        self, channel: discord.abc.GuildChannel
    ) -> None:
        actor = await self._audit_actor(
            channel.guild, discord.AuditLogAction.channel_delete, channel.id
        )
        embed = discord.Embed(
            title="📁 Channel Deleted",
            colour=discord.Colour.red(),
            timestamp=self._ts(),
        )
        embed.add_field(name="Channel", value=f"`#{channel.name}`", inline=True)
        embed.add_field(name="Type", value=str(channel.type).replace("_", " ").title(), inline=True)
        embed.add_field(name="Deleted by", value=actor, inline=True)
        await self._send(channel.guild, embed)

    @commands.Cog.listener()
    async def on_guild_channel_update(
        self,
        before: discord.abc.GuildChannel,
        after: discord.abc.GuildChannel,
    ) -> None:
        changes: List[str] = []

        if before.name != after.name:
            changes.append(f"**Name:** `{before.name}` → `{after.name}`")

        if hasattr(before, "topic") and hasattr(after, "topic"):
            if before.topic != after.topic:
                changes.append(
                    f"**Topic:** `{before.topic or '(none)'}` → `{after.topic or '(none)'}`"
                )

        if hasattr(before, "slowmode_delay") and hasattr(after, "slowmode_delay"):
            if before.slowmode_delay != after.slowmode_delay:
                changes.append(
                    f"**Slowmode:** `{before.slowmode_delay}s` → `{after.slowmode_delay}s`"
                )

        if hasattr(before, "nsfw") and hasattr(after, "nsfw"):
            if before.nsfw != after.nsfw:
                changes.append(f"**NSFW:** `{before.nsfw}` → `{after.nsfw}`")

        if hasattr(before, "bitrate") and hasattr(after, "bitrate"):
            if before.bitrate != after.bitrate:
                changes.append(
                    f"**Bitrate:** `{before.bitrate // 1000}kbps` → `{after.bitrate // 1000}kbps`"
                )

        if hasattr(before, "user_limit") and hasattr(after, "user_limit"):
            if before.user_limit != after.user_limit:
                changes.append(
                    f"**User Limit:** `{before.user_limit or '∞'}` → `{after.user_limit or '∞'}`"
                )

        if before.category != after.category:
            bcat = before.category.name if before.category else "(none)"
            acat = after.category.name if after.category else "(none)"
            changes.append(f"**Category:** `{bcat}` → `{acat}`")

        # ── Permission overwrite changes ─────────────────────────────────
        before_overwrites = dict(before.overwrites)
        after_overwrites = dict(after.overwrites)
        all_targets = set(before_overwrites.keys()) | set(after_overwrites.keys())

        for target in all_targets:
            b_ow = before_overwrites.get(target)
            a_ow = after_overwrites.get(target)
            target_name = getattr(target, "name", str(target))

            if b_ow is None and a_ow is not None:
                changes.append(f"**Permission override added** for `{target_name}`")
            elif b_ow is not None and a_ow is None:
                changes.append(f"**Permission override removed** for `{target_name}`")
            elif b_ow is not None and a_ow is not None:
                b_pair = b_ow.pair()
                a_pair = a_ow.pair()
                if b_pair != a_pair:
                    # Compute specific permission changes
                    perm_changes: List[str] = []
                    for perm, _ in discord.Permissions.all():
                        b_val = getattr(b_ow, perm, None)
                        a_val = getattr(a_ow, perm, None)
                        # PermissionOverwrite attrs return True/False/None
                        if b_val != a_val:
                            state_map = {True: "✅", False: "❌", None: "⬜"}
                            perm_changes.append(
                                f"`{perm}`: {state_map.get(b_val, '?')} → {state_map.get(a_val, '?')}"
                            )
                    if perm_changes:
                        changes.append(
                            f"**Perms changed for `{target_name}`:**\n"
                            + "\n".join(perm_changes[:15])
                            + (f"\n… +{len(perm_changes) - 15} more" if len(perm_changes) > 15 else "")
                        )

        if not changes:
            return

        actor = await self._audit_actor(
            after.guild, discord.AuditLogAction.channel_update, after.id
        )

        embed = discord.Embed(
            title="📝 Channel Updated",
            colour=discord.Colour.orange(),
            timestamp=self._ts(),
        )
        embed.add_field(name="Channel", value=f"{after.mention} (`{after.name}`)", inline=True)
        embed.add_field(name="Updated by", value=actor, inline=True)
        embed.add_field(
            name="Changes",
            value=self._trunc("\n".join(changes)),
            inline=False,
        )
        await self._send(after.guild, embed)

    # =====================================================================
    # VOICE STATE EVENTS
    # =====================================================================

    @commands.Cog.listener()
    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ) -> None:
        guild = member.guild
        changes: List[str] = []
        colour = discord.Colour.light_grey()

        # Join / Leave / Move
        if before.channel is None and after.channel is not None:
            changes.append(f"**Joined** {after.channel.mention}")
            colour = discord.Colour.green()
        elif before.channel is not None and after.channel is None:
            changes.append(f"**Left** {before.channel.mention}")
            colour = discord.Colour.red()
        elif (
            before.channel is not None
            and after.channel is not None
            and before.channel.id != after.channel.id
        ):
            changes.append(
                f"**Moved** {before.channel.mention} → {after.channel.mention}"
            )
            colour = discord.Colour.blue()

        # Server mute / deafen (mod action, not self)
        if before.mute != after.mute:
            changes.append(
                f"**Server Mute:** `{before.mute}` → `{after.mute}`"
            )
        if before.deaf != after.deaf:
            changes.append(
                f"**Server Deafen:** `{before.deaf}` → `{after.deaf}`"
            )

        if not changes:
            return  # self-mute / self-deafen — skip to avoid noise

        embed = discord.Embed(
            title="🎙️ Voice State Update",
            colour=colour,
            timestamp=self._ts(),
        )
        embed.add_field(name="Member", value=f"{member.mention} (`{member}`)", inline=True)
        embed.add_field(name="Changes", value="\n".join(changes), inline=False)
        await self._send(guild, embed)

    # =====================================================================
    # SERVER SETTINGS
    # =====================================================================

    @commands.Cog.listener()
    async def on_guild_update(
        self, before: discord.Guild, after: discord.Guild
    ) -> None:
        changes: List[str] = []

        if before.name != after.name:
            changes.append(f"**Name:** `{before.name}` → `{after.name}`")
        if before.icon != after.icon:
            changes.append("**Icon** changed")
        if before.banner != after.banner:
            changes.append("**Banner** changed")
        if before.description != after.description:
            changes.append(
                f"**Description:** `{before.description or '(none)'}` → `{after.description or '(none)'}`"
            )
        if before.verification_level != after.verification_level:
            changes.append(
                f"**Verification Level:** `{before.verification_level}` → `{after.verification_level}`"
            )
        if before.default_notifications != after.default_notifications:
            changes.append(
                f"**Default Notifications:** `{before.default_notifications}` → `{after.default_notifications}`"
            )
        if before.explicit_content_filter != after.explicit_content_filter:
            changes.append(
                f"**Explicit Content Filter:** `{before.explicit_content_filter}` → `{after.explicit_content_filter}`"
            )
        if before.afk_channel != after.afk_channel:
            b_afk = before.afk_channel.mention if before.afk_channel else "(none)"
            a_afk = after.afk_channel.mention if after.afk_channel else "(none)"
            changes.append(f"**AFK Channel:** {b_afk} → {a_afk}")
        if before.afk_timeout != after.afk_timeout:
            changes.append(
                f"**AFK Timeout:** `{before.afk_timeout}s` → `{after.afk_timeout}s`"
            )
        if before.system_channel != after.system_channel:
            b_sys = before.system_channel.mention if before.system_channel else "(none)"
            a_sys = after.system_channel.mention if after.system_channel else "(none)"
            changes.append(f"**System Channel:** {b_sys} → {a_sys}")
        if before.mfa_level != after.mfa_level:
            changes.append(
                f"**2FA Requirement:** `{before.mfa_level}` → `{after.mfa_level}`"
            )
        if before.vanity_url_code != after.vanity_url_code:
            changes.append(
                f"**Vanity URL:** `{before.vanity_url_code or '(none)'}` → `{after.vanity_url_code or '(none)'}`"
            )
        if before.premium_tier != after.premium_tier:
            changes.append(
                f"**Boost Tier:** `{before.premium_tier}` → `{after.premium_tier}`"
            )

        if not changes:
            return

        embed = discord.Embed(
            title="⚙️ Server Settings Updated",
            colour=discord.Colour.purple(),
            timestamp=self._ts(),
        )
        embed.add_field(
            name="Changes",
            value=self._trunc("\n".join(changes)),
            inline=False,
        )
        await self._send(after, embed)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(AuditLog(bot))
