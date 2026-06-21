# cogs/anti_scam.py
#
# Anti-scam / anti-spam auto-moderation.
#
# PURPOSE
# ───────
# Stops the recurring "MrBeast / Betchoco crypto-casino giveaway" image spam
# (and similar floods) where a compromised or throwaway account posts the same
# screenshots to many open channels at once. The attached images are renamed on
# every run, so this cog deliberately does NOT trust filenames — it matches on
# things the spammer cannot trivially change.
#
# DETECTION (any single signal triggers action) — all filename-independent
# ────────────────────────────────────────────────────────────────────────
# 1. Keyword / URL scoring   – the scam's own text (betchoco, promo code BONUS,
#                              crypto-casino giveaway/withdraw wording, …).
# 2. Known-image hashing     – the message's image BYTES are hashed (SHA-256,
#                              plus a perceptual average-hash when Pillow is
#                              installed) and compared against a learned set.
#                              Renaming the file does not change the bytes, so a
#                              re-upload of the same screenshot is still caught.
#                              Hashes persist in MySQL (kv_store) and survive
#                              restarts. Teach new ones with the right-click
#                              "Flag scam: purge + kick" message menu.
# 3. Cross-channel duplicate – the SAME author posting the SAME payload
#                              (normalised text + the set of attachment byte
#                              sizes — no download needed) to 2+ different
#                              channels inside a short window. This is the
#                              "same message on multiple open channels" pattern.
#
# ACTION
# ──────
# • Delete the offending message.
# • Delete the same author's other recently-buffered messages (the copies that
#   landed in the other channels) — best-effort cross-channel cleanup.
# • Kick the author (configurable; never staff/bots — see IMMUNE checks).
# • Log everything to #audit-log (silently skipped if the channel is absent).
#
# SAFETY
# ──────
# Staff are immune: the guild owner, anyone with administrator / kick_members /
# manage_messages, anyone holding an IMMUNE_ROLES role, bots, and the bot
# itself are never actioned. Keyword scoring needs a threshold (a single weak
# word will not kick anyone). Toggle the whole cog with /antiscam enabled.

from __future__ import annotations

import re
import time
import hashlib
import asyncio
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import discord
from discord.ext import commands
from discord import app_commands

from . import db

# =====================================================================
# CONFIG
# =====================================================================

LOG_CHANNEL_NAME = "audit-log"

# kv_store document key for persisted state (enabled flag + learned hashes).
KV_KEY = "anti_scam"

# Roles whose holders are NEVER actioned (leadership / staff).
IMMUNE_ROLES: Set[str] = {
    "ARC Security Corporation Leader",
    "ARC Security Administration Council",
    "ARC General",
    "ARC Commander",
    "ARC Lieutenant",
    "Moderator",
    "Administrator",
}

# Cross-channel duplicate detection.
DUP_WINDOW_SECONDS = 90        # look-back window for "same message, many channels"
DUP_MIN_CHANNELS = 2           # posting identical payload to >= this many channels
RECENT_BUFFER_SECONDS = 120    # how long we keep messages for cross-channel cleanup

# Image fetching guards (avoid downloading huge files on a busy server).
MAX_IMAGE_BYTES = 12 * 1024 * 1024   # skip attachments larger than this
MAX_IMAGES_PER_MSG = 6               # only hash the first N image attachments

# Keyword scoring: act when total score >= KEYWORD_THRESHOLD.
KEYWORD_THRESHOLD = 2

# Strong indicators — each worth 2 points (one alone trips the threshold).
STRONG_PATTERNS = [
    r"betchoco",
    r"bet[\s\-_.]*choco",
    r"\bvyro\b.*\b(casino|crypto|bonus)\b",
]

# Weak indicators — each worth 1 point (need two to trip the threshold).
WEAK_PATTERNS = [
    r"\bpromo[\s\-_]*code\b",
    r"\bbonus\b",
    r"\bgiveaway\b",
    r"\bfree\s*\$?\s*\d",
    r"\bregister(?:s|ed)?\b.*\bwithdraw",
    r"\bcrypto(?:currency)?\s*casino\b",
    r"\busdt\b",
    r"\bwithdraw(?:al)?\b.*\b(crypto|usdt|wallet|tether)\b",
    r"\bclaim your (?:reward|bonus)\b",
    r"\bspecial promo\s*code\b",
    r"\$2[.,]?500\b",
    r"\$2[.,]?700\b",
]

_STRONG_RE = [re.compile(p, re.IGNORECASE) for p in STRONG_PATTERNS]
_WEAK_RE = [re.compile(p, re.IGNORECASE) for p in WEAK_PATTERNS]

# Optional perceptual hashing (only if Pillow happens to be installed). Falls
# back to exact SHA-256 byte matching otherwise — no hard dependency added.
try:  # pragma: no cover - optional dependency
    from PIL import Image  # type: ignore
    import io as _io
    _HAS_PIL = True
except Exception:  # pragma: no cover
    _HAS_PIL = False

# Hamming distance (out of 64 bits) under which two average-hashes are "the
# same image" — tolerates re-compression / minor cropping.
AHASH_MAX_DISTANCE = 8


def _normalise_text(text: str) -> str:
    """Lower-case and collapse whitespace so trivial edits hash the same."""
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _ahash_bytes(data: bytes) -> Optional[str]:
    """8x8 average hash as a 16-char hex string, or None if Pillow is absent."""
    if not _HAS_PIL:
        return None
    try:
        img = Image.open(_io.BytesIO(data)).convert("L").resize((8, 8))
        pixels = list(img.getdata())
        avg = sum(pixels) / len(pixels)
        bits = 0
        for px in pixels:
            bits = (bits << 1) | (1 if px >= avg else 0)
        return f"{bits:016x}"
    except Exception:
        return None


def _hamming_hex(a: str, b: str) -> int:
    try:
        return bin(int(a, 16) ^ int(b, 16)).count("1")
    except Exception:
        return 64


class AntiScam(commands.Cog):
    """Auto-detect and remove scam/spam image floods, then kick the poster."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.enabled: bool = True

        # Learned scam-image fingerprints.
        self.known_sha256: Set[str] = set()
        self.known_ahash: List[str] = []

        # Per-(guild, author) recent messages, for cross-channel detection and
        # cleanup: list of (message, fingerprint, monotonic_ts).
        self._recent: Dict[Tuple[int, int], List[Tuple[discord.Message, str, float]]] = (
            defaultdict(list)
        )

        # Authors currently being actioned — avoids racing on a burst.
        self._busy: Set[Tuple[int, int]] = set()

        # Right-click message command to teach the filter a new scam image.
        self._ctx_menu = app_commands.ContextMenu(
            name="Flag scam: purge + kick",
            callback=self._ctx_flag_scam,
        )

    # ── lifecycle ─────────────────────────────────────────────────────────

    async def cog_load(self) -> None:
        self.bot.tree.add_command(self._ctx_menu)
        try:
            state = await db.akv_load(KV_KEY, {}) or {}
            self.enabled = bool(state.get("enabled", True))
            self.known_sha256 = set(state.get("sha256", []))
            self.known_ahash = list(state.get("ahash", []))
            print(
                f"[AntiScam] loaded: enabled={self.enabled}, "
                f"{len(self.known_sha256)} sha256 / {len(self.known_ahash)} ahash known."
            )
        except Exception as e:
            print(f"[AntiScam] could not load state: {type(e).__name__}: {e}")

    async def cog_unload(self) -> None:
        self.bot.tree.remove_command(self._ctx_menu.name, type=self._ctx_menu.type)

    async def _save_state(self) -> None:
        try:
            await db.akv_save(
                KV_KEY,
                {
                    "enabled": self.enabled,
                    "sha256": sorted(self.known_sha256),
                    "ahash": self.known_ahash,
                },
            )
        except Exception as e:
            print(f"[AntiScam] could not save state: {type(e).__name__}: {e}")

    # ── helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _is_immune(member: discord.Member) -> bool:
        if member.bot:
            return True
        if member.guild.owner_id == member.id:
            return True
        perms = member.guild_permissions
        if perms.administrator or perms.kick_members or perms.manage_messages:
            return True
        return any(r.name in IMMUNE_ROLES for r in member.roles)

    def _log_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        return discord.utils.get(guild.text_channels, name=LOG_CHANNEL_NAME)

    async def _log(self, guild: discord.Guild, embed: discord.Embed) -> None:
        ch = self._log_channel(guild)
        if not ch:
            return
        try:
            await ch.send(embed=embed)
        except (discord.Forbidden, discord.HTTPException):
            pass

    @staticmethod
    def _image_attachments(message: discord.Message) -> List[discord.Attachment]:
        out: List[discord.Attachment] = []
        for a in message.attachments:
            ct = (a.content_type or "").lower()
            is_img = ct.startswith("image/") or bool(
                re.search(r"\.(png|jpe?g|gif|webp|bmp)$", (a.filename or "").lower())
            )
            if is_img and a.size <= MAX_IMAGE_BYTES:
                out.append(a)
            if len(out) >= MAX_IMAGES_PER_MSG:
                break
        return out

    @staticmethod
    def _fingerprint(message: discord.Message) -> str:
        """Cheap content fingerprint (no download): normalised text + the
        sorted set of attachment byte-sizes. Identical re-uploads of the same
        files to different channels collide here."""
        sizes = ",".join(str(s) for s in sorted(a.size for a in message.attachments))
        basis = f"{_normalise_text(message.content)}|{sizes}"
        return hashlib.sha1(basis.encode("utf-8", "ignore")).hexdigest()

    def _keyword_score(self, text: str) -> int:
        if not text:
            return 0
        score = 0
        for rx in _STRONG_RE:
            if rx.search(text):
                score += 2
        for rx in _WEAK_RE:
            if rx.search(text):
                score += 1
        return score

    async def _hash_message_images(
        self, message: discord.Message
    ) -> Tuple[Set[str], List[str]]:
        """Download image attachments and return (sha256 set, ahash list)."""
        shas: Set[str] = set()
        ahs: List[str] = []
        for att in self._image_attachments(message):
            try:
                data = await att.read()
            except (discord.HTTPException, discord.NotFound):
                continue
            shas.add(hashlib.sha256(data).hexdigest())
            ah = _ahash_bytes(data)
            if ah:
                ahs.append(ah)
        return shas, ahs

    def _matches_known_image(self, shas: Set[str], ahs: List[str]) -> bool:
        if shas & self.known_sha256:
            return True
        for ah in ahs:
            for known in self.known_ahash:
                if _hamming_hex(ah, known) <= AHASH_MAX_DISTANCE:
                    return True
        return False

    def _prune_recent(self, key: Tuple[int, int]) -> None:
        now = time.monotonic()
        kept = [
            t for t in self._recent.get(key, [])
            if now - t[2] <= RECENT_BUFFER_SECONDS
        ]
        if kept:
            self._recent[key] = kept
        else:
            self._recent.pop(key, None)

    # ── main listener ─────────────────────────────────────────────────────

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if not self.enabled:
            return
        if message.guild is None or message.author.bot:
            return
        if not isinstance(message.author, discord.Member):
            return
        if self._is_immune(message.author):
            return

        key = (message.guild.id, message.author.id)
        fp = self._fingerprint(message)

        # Record for cross-channel detection / later cleanup.
        self._recent[key].append((message, fp, time.monotonic()))
        self._prune_recent(key)

        # ── Signal 1: keyword / URL scoring ─────────────────────────────
        reason: Optional[str] = None
        score = self._keyword_score(message.content)
        if score >= KEYWORD_THRESHOLD:
            reason = f"scam keywords matched (score {score})"

        # ── Signal 3: cross-channel duplicate flood ─────────────────────
        if reason is None:
            same_fp_channels = {
                m.channel.id
                for (m, f, _ts) in self._recent[key]
                if f == fp
            }
            has_payload = bool(message.attachments) or len(message.content) >= 8
            if has_payload and len(same_fp_channels) >= DUP_MIN_CHANNELS:
                reason = (
                    f"same message posted to {len(same_fp_channels)} channels "
                    f"in <{DUP_WINDOW_SECONDS}s"
                )

        # ── Signal 2: known scam-image hash ─────────────────────────────
        # Only download when there are image attachments AND we haven't already
        # decided — keeps network/CPU cost off the normal-message path.
        if reason is None and self._image_attachments(message) and (
            self.known_sha256 or self.known_ahash
        ):
            shas, ahs = await self._hash_message_images(message)
            if self._matches_known_image(shas, ahs):
                reason = "matched a known scam image"

        if reason is None:
            return

        await self._action(message.guild, message.author, reason, trigger=message)

    # ── enforcement ───────────────────────────────────────────────────────

    async def _action(
        self,
        guild: discord.Guild,
        member: discord.Member,
        reason: str,
        trigger: discord.Message,
    ) -> None:
        key = (guild.id, member.id)
        if key in self._busy:
            return
        self._busy.add(key)
        try:
            # Collect every buffered message from this author (the cross-channel
            # copies) plus the trigger, newest first, de-duplicated.
            self._prune_recent(key)
            to_delete: List[discord.Message] = [
                m for (m, _f, _ts) in self._recent.get(key, [])
            ]
            if trigger not in to_delete:
                to_delete.append(trigger)

            seen: Set[int] = set()
            deleted = 0
            for msg in to_delete:
                if msg.id in seen:
                    continue
                seen.add(msg.id)
                try:
                    await msg.delete()
                    deleted += 1
                except (discord.Forbidden, discord.NotFound, discord.HTTPException):
                    pass

            self._recent.pop(key, None)

            # Kick the author.
            kick_status = "kicked"
            try:
                await member.kick(reason=f"Anti-scam: {reason}")
            except discord.Forbidden:
                kick_status = "⚠️ kick failed (missing permission / role hierarchy)"
            except discord.HTTPException as e:
                kick_status = f"⚠️ kick failed ({type(e).__name__})"

            print(
                f"[AntiScam] {member} ({member.id}) in {guild.name}: {reason}; "
                f"deleted {deleted} message(s); {kick_status}."
            )

            embed = discord.Embed(
                title="🚫 Scam spam removed",
                colour=discord.Colour.dark_red(),
                timestamp=datetime.now(timezone.utc),
            )
            embed.add_field(
                name="Member", value=f"`{member}` ({member.id})", inline=True
            )
            embed.add_field(name="Action", value=kick_status, inline=True)
            embed.add_field(
                name="Messages deleted", value=str(deleted), inline=True
            )
            embed.add_field(name="Reason", value=reason, inline=False)
            await self._log(guild, embed)
        finally:
            self._busy.discard(key)

    # =====================================================================
    # Right-click: teach the filter + purge + kick
    # =====================================================================

    async def _ctx_flag_scam(
        self, interaction: discord.Interaction, message: discord.Message
    ) -> None:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message(
                "Use this in a server.", ephemeral=True
            )
            return
        if not (
            interaction.user.guild_permissions.kick_members
            or interaction.user.guild_permissions.manage_messages
            or interaction.user.guild_permissions.administrator
        ):
            await interaction.response.send_message(
                "You need Kick Members / Manage Messages to use this.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)

        # Learn the image fingerprints so future re-uploads (renamed) are caught.
        learned = 0
        if self._image_attachments(message):
            shas, ahs = await self._hash_message_images(message)
            new_sha = shas - self.known_sha256
            self.known_sha256 |= new_sha
            self.known_ahash.extend(ahs)
            learned = len(new_sha) + len(ahs)
            if learned:
                await self._save_state()

        target = message.author
        note = ""
        if isinstance(target, discord.Member) and not self._is_immune(target):
            await self._action(
                interaction.guild,
                target,
                reason="manually flagged as scam by a moderator",
                trigger=message,
            )
        else:
            note = " (author is immune/left — only deleted this message)"
            try:
                await message.delete()
            except (discord.Forbidden, discord.NotFound, discord.HTTPException):
                pass

        await interaction.followup.send(
            f"Flagged. Learned {learned} image fingerprint(s){note}.",
            ephemeral=True,
        )

    # =====================================================================
    # Admin slash commands
    # =====================================================================

    antiscam = app_commands.Group(
        name="antiscam",
        description="Manage the anti-scam auto-moderator.",
        default_permissions=discord.Permissions(manage_guild=True),
    )

    @antiscam.command(name="status", description="Show anti-scam status.")
    async def status(self, interaction: discord.Interaction) -> None:
        embed = discord.Embed(
            title="🛡️ Anti-scam status",
            colour=discord.Colour.blue(),
        )
        embed.add_field(
            name="Enabled", value="✅ yes" if self.enabled else "❌ no", inline=True
        )
        embed.add_field(
            name="Known scam images",
            value=f"{len(self.known_sha256)} exact / {len(self.known_ahash)} perceptual",
            inline=True,
        )
        embed.add_field(
            name="Perceptual hashing",
            value="on (Pillow)" if _HAS_PIL else "off (install Pillow for fuzzy match)",
            inline=True,
        )
        embed.add_field(
            name="Cross-channel rule",
            value=f"same post in ≥{DUP_MIN_CHANNELS} channels within {DUP_WINDOW_SECONDS}s",
            inline=False,
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @antiscam.command(name="enabled", description="Turn the anti-scam filter on or off.")
    @app_commands.describe(on="True to enable, False to disable.")
    async def set_enabled(self, interaction: discord.Interaction, on: bool) -> None:
        self.enabled = on
        await self._save_state()
        await interaction.response.send_message(
            f"Anti-scam filter is now **{'ENABLED' if on else 'DISABLED'}**.",
            ephemeral=True,
        )

    @antiscam.command(
        name="clear_images",
        description="Forget all learned scam-image fingerprints.",
    )
    async def clear_images(self, interaction: discord.Interaction) -> None:
        n = len(self.known_sha256) + len(self.known_ahash)
        self.known_sha256.clear()
        self.known_ahash.clear()
        await self._save_state()
        await interaction.response.send_message(
            f"Cleared {n} learned image fingerprint(s).", ephemeral=True
        )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(AntiScam(bot))
