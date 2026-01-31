import os
import json
import datetime
import asyncio
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import aiohttp
import discord
from discord.ext import commands, tasks
from discord import app_commands
from discord.ui import View, Button, Modal, TextInput

# =====================
# CONFIG
# =====================

PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)

DATA_FILE = PERSIST_ROOT / "ign_registry.json"

REQUEST_PANEL_CHANNEL_NAME = "request-to-access-locations"
ACCESS_CHANNEL_NAME = "location_access"

ARC_SECURITY_ROLE = "ARC Security"

ALLOWED_ROLES = {
    "ARC Security Corporation Leader",
    "ARC Security Administration Council",
    "ARC General",
    "ARC Commander",
    "ARC Officer",
}

MAX_IGNS_PER_USER = 10

PANEL_EMBED_TEXT = "Please register all the alts that you want to be provided with the Wormhole access"
PANEL_BUTTON_LABEL = "Register"

# ---- EVE / ESI ----
EVE_CLIENT_ID = os.getenv("EVE_CLIENT_ID", "").strip()
EVE_CLIENT_SECRET = os.getenv("EVE_CLIENT_SECRET", "").strip()
EVE_REFRESH_TOKEN_ENV = os.getenv("EVE_REFRESH_TOKEN", "").strip()
EVE_CORP_ID = os.getenv("EVE_CORP_ID", "").strip()
CORP_CHECK_MINUTES = int(os.getenv("CORP_CHECK_MINUTES", "70"))

SSO_TOKEN_URL = "https://login.eveonline.com/v2/oauth/token"
ESI_BASE = "https://esi.evetech.net/latest"

# =====================
# STORAGE
# =====================

def utcnow_iso() -> str:
    return datetime.datetime.utcnow().isoformat()

def load_state() -> Dict[str, Any]:
    if not DATA_FILE.exists():
        return {
            "users": {},
            "requests": {},
            "leave_warnings": {},
            "panels": {},
            # ESI helpers:
            "esi": {
                "refresh_token": None,      # stored refresh token (rotates sometimes)
                "access_token": None,
                "access_expires_utc": None, # ISO
            },
            "char_index": {
                # "lower ign": {"character_id": 123, "name": "Exact Name", "updated_utc": "..."}
            },
            "corp_watch": {
                # "character_id": {"in_corp": True/False/None, "last_change_utc": "...", "last_alerted_left_utc": "..."}
            },
        }
    try:
        s = json.loads(DATA_FILE.read_text(encoding="utf-8"))
        s.setdefault("users", {})
        s.setdefault("requests", {})
        s.setdefault("leave_warnings", {})
        s.setdefault("panels", {})
        s.setdefault("esi", {"refresh_token": None, "access_token": None, "access_expires_utc": None})
        s.setdefault("char_index", {})
        s.setdefault("corp_watch", {})
        return s
    except Exception:
        return {
            "users": {},
            "requests": {},
            "leave_warnings": {},
            "panels": {},
            "esi": {"refresh_token": None, "access_token": None, "access_expires_utc": None},
            "char_index": {},
            "corp_watch": {},
        }

def save_state(state: Dict[str, Any]) -> None:
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = DATA_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=4), encoding="utf-8")
    tmp.replace(DATA_FILE)

def normalize_ign(s: str) -> str:
    return " ".join((s or "").strip().split())

def split_igns(raw: str) -> List[str]:
    if not raw:
        return []
    parts: List[str] = []
    for chunk in raw.replace("\n", ",").split(","):
        ign = normalize_ign(chunk)
        if ign:
            parts.append(ign)

    seen = set()
    out: List[str] = []
    for ign in parts:
        key = ign.lower()
        if key not in seen:
            seen.add(key)
            out.append(ign)
    return out

# =====================
# PERMISSION CHECKS
# =====================

def has_any_role(member: discord.Member, role_names: set) -> bool:
    return any(r.name in role_names for r in getattr(member, "roles", []))

def had_role_name(member: discord.Member, role_name: str) -> bool:
    return any(r.name == role_name for r in getattr(member, "roles", []))

def require_roles():
    async def predicate(interaction: discord.Interaction) -> bool:
        if not isinstance(interaction.user, discord.Member):
            return False
        return has_any_role(interaction.user, ALLOWED_ROLES)
    return app_commands.check(predicate)

# =====================
# INTERACTION SAFETY HELPERS
# =====================

async def safe_defer(interaction: discord.Interaction, ephemeral: bool = True) -> None:
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=ephemeral)
    except Exception:
        pass

async def safe_reply(
    interaction: discord.Interaction,
    content: Optional[str] = None,
    *,
    embed: Optional[discord.Embed] = None,
    ephemeral: bool = True,
) -> None:
    try:
        if interaction.response.is_done():
            await interaction.followup.send(content=content, embed=embed, ephemeral=ephemeral)
        else:
            await interaction.response.send_message(content=content, embed=embed, ephemeral=ephemeral)
    except Exception:
        pass

# =====================
# ESI / SSO CLIENT
# =====================

class EveSSOESI:
    def __init__(self, state: Dict[str, Any]):
        self.state = state
        self._lock = asyncio.Lock()

    def _parse_iso(self, s: Optional[str]) -> Optional[datetime.datetime]:
        if not s:
            return None
        try:
            return datetime.datetime.fromisoformat(s)
        except Exception:
            return None

    def _is_access_valid(self) -> bool:
        esi = self.state.get("esi", {})
        tok = esi.get("access_token")
        exp = self._parse_iso(esi.get("access_expires_utc"))
        if not tok or not exp:
            return False
        return datetime.datetime.utcnow() < (exp - datetime.timedelta(minutes=2))

    async def _refresh_access(self, session: aiohttp.ClientSession) -> Optional[str]:
        async with self._lock:
            if self._is_access_valid():
                return self.state["esi"]["access_token"]

            refresh_token = (self.state.get("esi", {}).get("refresh_token") or "").strip()
            if not refresh_token:
                refresh_token = EVE_REFRESH_TOKEN_ENV

            if not (EVE_CLIENT_ID and EVE_CLIENT_SECRET and refresh_token):
                return None

            auth = aiohttp.BasicAuth(EVE_CLIENT_ID, EVE_CLIENT_SECRET)
            data = {"grant_type": "refresh_token", "refresh_token": refresh_token}

            try:
                async with session.post(SSO_TOKEN_URL, data=data, auth=auth, timeout=30) as r:
                    if r.status != 200:
                        return None
                    payload = await r.json()

                access_token = payload.get("access_token")
                expires_in = int(payload.get("expires_in", 0))
                new_refresh = payload.get("refresh_token")

                if not access_token or expires_in <= 0:
                    return None

                exp = datetime.datetime.utcnow() + datetime.timedelta(seconds=expires_in)

                esi = self.state.setdefault("esi", {})
                esi["access_token"] = access_token
                esi["access_expires_utc"] = exp.isoformat()
                if new_refresh:
                    esi["refresh_token"] = new_refresh

                save_state(self.state)
                return access_token
            except Exception:
                return None

    async def get_access_token(self, session: aiohttp.ClientSession) -> Optional[str]:
        if self._is_access_valid():
            return self.state["esi"]["access_token"]
        return await self._refresh_access(session)

    async def resolve_names_to_character_ids(
        self,
        session: aiohttp.ClientSession,
        names: List[str],
    ) -> Dict[str, int]:
        clean = []
        seen = set()
        for n in names:
            n2 = normalize_ign(n)
            if not n2:
                continue
            k = n2.lower()
            if k not in seen:
                seen.add(k)
                clean.append(n2)

        if not clean:
            return {}

        url = f"{ESI_BASE}/universe/ids/?datasource=tranquility"
        try:
            async with session.post(url, json=clean, timeout=30) as r:
                if r.status != 200:
                    return {}
                data = await r.json()
        except Exception:
            return {}

        out: Dict[str, int] = {}
        chars = data.get("characters") or []
        for c in chars:
            name = c.get("name")
            cid = c.get("id")
            if isinstance(name, str) and isinstance(cid, int):
                out[name.lower()] = cid
        return out

    async def get_corp_membertracking_ids(
        self,
        session: aiohttp.ClientSession,
        corp_id: int,
    ) -> Optional[List[int]]:
        token = await self.get_access_token(session)
        if not token:
            return None

        url = f"{ESI_BASE}/corporations/{corp_id}/membertracking/?datasource=tranquility"
        headers = {"Authorization": f"Bearer {token}"}

        try:
            async with session.get(url, headers=headers, timeout=45) as r:
                if r.status != 200:
                    return None
                data = await r.json()
        except Exception:
            return None

        ids: List[int] = []
        for row in data or []:
            cid = row.get("character_id")
            if isinstance(cid, int):
                ids.append(cid)
        return ids

# =====================
# UI: MODAL
# =====================

class RegisterIGNModal(Modal):
    def __init__(self, cog: "IGNRegistrationCog"):
        super().__init__(title="Register IGN")
        self.cog = cog
        self.igns = TextInput(
            label="Enter your EVE IGN(s)",
            placeholder="Example: ARC Tendeuse A (or multiple: IGN1, IGN2)",
            required=True,
            max_length=400,
            style=discord.TextStyle.paragraph,
        )
        self.add_item(self.igns)

    async def on_submit(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        raw = str(self.igns.value)
        igns = split_igns(raw)

        if not igns:
            await safe_reply(interaction, "No valid IGN was provided.", ephemeral=True)
            return

        await self.cog.handle_register_submission(interaction, igns)

# =====================
# UI: PERSISTENT VIEWS
# =====================

class RegisterPanelView(View):
    def __init__(self, cog: "IGNRegistrationCog"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label=PANEL_BUTTON_LABEL,
        style=discord.ButtonStyle.primary,
        custom_id="ign_panel:register",
    )
    async def open_register(self, interaction: discord.Interaction, button: Button):
        try:
            if interaction.response.is_done():
                await safe_reply(interaction, "Please click the button again.", ephemeral=True)
                return
            await interaction.response.send_modal(RegisterIGNModal(self.cog))
        except discord.NotFound:
            return
        except Exception:
            await safe_reply(interaction, "Failed to open the registration modal.", ephemeral=True)

class AccessRequestView(View):
    def __init__(self, cog: "IGNRegistrationCog"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Added to Access List",
        style=discord.ButtonStyle.success,
        custom_id="ign_access:added",
    )
    async def mark_added(self, interaction: discord.Interaction, button: Button):
        await self.cog.handle_access_button(interaction, action="added")

    @discord.ui.button(
        label="Revert to Pending",
        style=discord.ButtonStyle.secondary,
        custom_id="ign_access:revert",
    )
    async def revert_pending(self, interaction: discord.Interaction, button: Button):
        await self.cog.handle_access_button(interaction, action="revert")

class LeaveWarningView(View):
    def __init__(self, cog: "IGNRegistrationCog"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Marked Removed (Purge Data)",
        style=discord.ButtonStyle.danger,
        custom_id="ign_leave:purge",
    )
    async def mark_removed_purge(self, interaction: discord.Interaction, button: Button):
        await self.cog.handle_leave_purge_button(interaction)

# =====================
# COG
# =====================

class IGNRegistrationCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state = load_state()

        self.bot.add_view(RegisterPanelView(self))
        self.bot.add_view(AccessRequestView(self))
        self.bot.add_view(LeaveWarningView(self))

        self._panels_ensured_once: bool = False
        self._corp_loop_started: bool = False

        self.esi = EveSSOESI(self.state)

    # -------------------------
    # Channel ensure
    # -------------------------

    async def ensure_request_panel_channel(self, guild: discord.Guild) -> discord.TextChannel:
        ch = discord.utils.get(guild.text_channels, name=REQUEST_PANEL_CHANNEL_NAME)
        if ch:
            return ch

        me = guild.me
        if me is None and self.bot.user is not None:
            me = guild.get_member(self.bot.user.id)

        overwrites = {
            guild.default_role: discord.PermissionOverwrite(
                read_messages=True,
                send_messages=False,
                add_reactions=False,
            )
        }
        if me is not None:
            overwrites[me] = discord.PermissionOverwrite(
                read_messages=True,
                send_messages=True,
                embed_links=True,
                manage_messages=True,
            )

        return await guild.create_text_channel(
            REQUEST_PANEL_CHANNEL_NAME,
            overwrites=overwrites,
            reason="Channel for location access request panel",
        )

    async def ensure_access_channel(self, guild: discord.Guild) -> discord.TextChannel:
        ch = discord.utils.get(guild.text_channels, name=ACCESS_CHANNEL_NAME)
        if ch:
            return ch

        me = guild.me
        if me is None and self.bot.user is not None:
            me = guild.get_member(self.bot.user.id)

        overwrites = {
            guild.default_role: discord.PermissionOverwrite(
                send_messages=False,
                add_reactions=False,
                read_messages=True,
            )
        }
        if me is not None:
            overwrites[me] = discord.PermissionOverwrite(
                send_messages=True,
                embed_links=True,
                read_messages=True,
                manage_messages=True,
            )

        return await guild.create_text_channel(
            ACCESS_CHANNEL_NAME,
            overwrites=overwrites,
            reason="Channel for location access list requests",
        )

    # -------------------------
    # Panel management
    # -------------------------

    def get_panel_record(self, guild_id: int) -> Dict[str, Any]:
        panels = self.state.setdefault("panels", {})
        return panels.setdefault(str(guild_id), {"channel_id": None, "message_id": None, "updated_utc": utcnow_iso()})

    async def ensure_register_panel_message(self, guild: discord.Guild) -> None:
        ch = await self.ensure_request_panel_channel(guild)

        rec = self.get_panel_record(guild.id)
        rec["channel_id"] = ch.id

        embed = discord.Embed(
            description=PANEL_EMBED_TEXT,
            timestamp=datetime.datetime.utcnow(),
        )
        embed.set_footer(text="Wormhole Access Registration")

        msg_id = rec.get("message_id")
        if msg_id:
            try:
                msg = await ch.fetch_message(int(msg_id))
                await msg.edit(embed=embed, view=RegisterPanelView(self))
                rec["updated_utc"] = utcnow_iso()
                save_state(self.state)
                return
            except Exception:
                pass

        try:
            msg = await ch.send(embed=embed, view=RegisterPanelView(self))
            rec["message_id"] = msg.id
            rec["updated_utc"] = utcnow_iso()
            save_state(self.state)
        except Exception:
            pass

    # -------------------------
    # Embeds
    # -------------------------

    def build_request_embed(
        self,
        discord_user_id: int,
        discord_user_tag: str,
        igns: List[str],
        status: str,
        message_id: Optional[int] = None,
    ) -> discord.Embed:
        desc = (
            f"**Discord:** <@{discord_user_id}> (`{discord_user_tag}`)\n"
            f"**IGN(s):** {', '.join(igns)}\n"
            f"**Status:** `{status}`\n"
        )
        if message_id:
            desc += f"\n**Request Message ID:** `{message_id}`"

        emb = discord.Embed(
            title="Location Access Request",
            description=desc,
            timestamp=datetime.datetime.utcnow(),
        )
        emb.set_footer(text="IGN Registration Workflow")
        return emb

    def build_leave_warning_embed(
        self,
        member_tag: str,
        member_id: int,
        igns: List[str],
        status: str,
        warning_id: Optional[int] = None,
        purged_by: Optional[str] = None,
        reason: str = "DISCORD_LEFT",
        extra: Optional[str] = None,
    ) -> discord.Embed:
        reason_line = "Discord member left server" if reason == "DISCORD_LEFT" else "Character left corporation"
        desc = (
            f"**Reason:** `{reason_line}`\n"
            f"**Member:** `{member_tag}` (ID: `{member_id}`)\n"
            f"**Character(s) to remove:** {', '.join(igns)}\n"
            f"**Status:** `{status}`\n"
        )
        if extra:
            desc += f"\n**Details:** {extra}\n"
        if warning_id:
            desc += f"\n**Warning Message ID:** `{warning_id}`"
        if purged_by:
            desc += f"\n**Processed By:** {purged_by}"
        desc += "\n\nAction required: remove these character names from the in-game access list."

        emb = discord.Embed(
            title="Offboarding Alert — Remove From Access List",
            description=desc,
            timestamp=datetime.datetime.utcnow(),
        )
        emb.set_footer(text="IGN Registration / Offboarding")
        return emb

    def build_corp_leave_notice_embed(
        self,
        member_tag: str,
        member_id: int,
        igns: List[str],
        character_id: int,
    ) -> discord.Embed:
        desc = (
            f"**Notice:** `Character left corporation`\n"
            f"**Member:** `{member_tag}` (ID: `{member_id}`)\n"
            f"**Character ID:** `{character_id}`\n"
            f"**Registered IGN(s):** {', '.join(igns) if igns else '(none)'}\n"
            f"\nNo action required. This is an informational alert."
        )
        emb = discord.Embed(
            title="Corp Membership Notice",
            description=desc,
            timestamp=datetime.datetime.utcnow(),
        )
        emb.set_footer(text="Corp Membership Monitor")
        return emb

    # -------------------------
    # State helpers
    # -------------------------

    def get_user_record(self, user_id: int) -> Dict[str, Any]:
        users = self.state.setdefault("users", {})
        return users.setdefault(
            str(user_id),
            {
                "igns": [],
                "character_ids": [],
                "created_utc": utcnow_iso(),
                "updated_utc": utcnow_iso(),
                "last_request_message_id": None,
                "last_request_channel_id": None,
            },
        )

    def add_user_igns(self, user_id: int, new_igns: List[str]) -> List[str]:
        rec = self.get_user_record(user_id)
        existing = rec.get("igns", [])
        merged = existing[:]

        existing_lower = {x.lower() for x in existing}
        for ign in new_igns:
            if ign.lower() not in existing_lower:
                merged.append(ign)
                existing_lower.add(ign.lower())

        merged = merged[:MAX_IGNS_PER_USER]
        rec["igns"] = merged
        rec["updated_utc"] = utcnow_iso()
        save_state(self.state)
        return merged

    def set_user_character_ids(self, user_id: int, char_ids: List[int]) -> None:
        rec = self.get_user_record(user_id)
        out = []
        seen = set()
        for cid in char_ids:
            if isinstance(cid, int) and cid not in seen:
                seen.add(cid)
                out.append(cid)
        rec["character_ids"] = out
        rec["updated_utc"] = utcnow_iso()
        save_state(self.state)

    def delete_user_record(self, user_id: int) -> Optional[List[str]]:
        users = self.state.setdefault("users", {})
        rec = users.pop(str(user_id), None)
        if rec:
            save_state(self.state)
            return rec.get("igns", [])
        save_state(self.state)
        return None

    def remove_user_ign(self, user_id: int, ign: Optional[str] = None) -> List[str]:
        users = self.state.setdefault("users", {})
        key = str(user_id)
        if key not in users:
            return []

        if ign is None:
            removed = users[key].get("igns", [])
            users.pop(key, None)
            save_state(self.state)
            return removed

        ign_n = normalize_ign(ign)
        current = users[key].get("igns", [])
        remaining = [x for x in current if x.lower() != ign_n.lower()]
        users[key]["igns"] = remaining
        users[key]["updated_utc"] = utcnow_iso()

        if not remaining:
            users.pop(key, None)

        save_state(self.state)
        return remaining

    def upsert_request(self, message_id: int, payload: Dict[str, Any]) -> None:
        reqs = self.state.setdefault("requests", {})
        reqs[str(message_id)] = payload
        save_state(self.state)

    def get_request(self, message_id: int) -> Optional[Dict[str, Any]]:
        return self.state.get("requests", {}).get(str(message_id))

    def delete_request(self, message_id: int) -> None:
        reqs = self.state.setdefault("requests", {})
        reqs.pop(str(message_id), None)
        save_state(self.state)

    def update_request_status(self, message_id: int, status: str, actor_id: int) -> None:
        req = self.get_request(message_id)
        if not req:
            return
        req["status"] = status
        req["updated_utc"] = utcnow_iso()
        req["updated_by"] = actor_id
        self.upsert_request(message_id, req)

    def upsert_leave_warning(self, message_id: int, payload: Dict[str, Any]) -> None:
        lw = self.state.setdefault("leave_warnings", {})
        lw[str(message_id)] = payload
        save_state(self.state)

    def get_leave_warning(self, message_id: int) -> Optional[Dict[str, Any]]:
        return self.state.get("leave_warnings", {}).get(str(message_id))

    def update_leave_warning(self, message_id: int, status: str, processed_by: str) -> None:
        lw = self.get_leave_warning(message_id)
        if not lw:
            return
        lw["status"] = status
        lw["processed_by"] = processed_by
        lw["updated_utc"] = utcnow_iso()
        self.upsert_leave_warning(message_id, lw)

    async def delete_previous_user_request_message(self, guild: discord.Guild, user_id: int) -> None:
        rec = self.get_user_record(user_id)
        old_ch_id = rec.get("last_request_channel_id")
        old_msg_id = rec.get("last_request_message_id")

        if not old_ch_id or not old_msg_id:
            return

        try:
            ch = guild.get_channel(int(old_ch_id))
            if ch is None:
                ch = await guild.fetch_channel(int(old_ch_id))

            if isinstance(ch, discord.TextChannel):
                try:
                    msg = await ch.fetch_message(int(old_msg_id))
                    await msg.delete()
                except Exception:
                    pass
        except Exception:
            pass

        self.delete_request(int(old_msg_id))

        rec["last_request_channel_id"] = None
        rec["last_request_message_id"] = None
        rec["updated_utc"] = utcnow_iso()
        save_state(self.state)

    # -------------------------
    # ESI mapping helpers
    # -------------------------

    async def ensure_character_ids_for_user(self, user_id: int, igns: List[str]) -> List[int]:
        char_index = self.state.setdefault("char_index", {})
        missing = []
        found_ids: List[int] = []

        for ign in igns:
            k = ign.lower()
            rec = char_index.get(k)
            if rec and isinstance(rec.get("character_id"), int):
                found_ids.append(int(rec["character_id"]))
            else:
                missing.append(ign)

        if missing:
            async with aiohttp.ClientSession() as session:
                mapping = await self.esi.resolve_names_to_character_ids(session, missing)
            for name_lower, cid in mapping.items():
                char_index[name_lower] = {"character_id": cid, "name": name_lower, "updated_utc": utcnow_iso()}
                found_ids.append(cid)
            save_state(self.state)

        out = []
        seen = set()
        for cid in found_ids:
            if cid not in seen:
                seen.add(cid)
                out.append(cid)
        return out

    def find_discord_user_for_character(self, character_id: int) -> Optional[int]:
        users = self.state.get("users", {})
        for uid_str, rec in users.items():
            cids = rec.get("character_ids", []) or []
            if character_id in cids:
                try:
                    return int(uid_str)
                except Exception:
                    return None
        return None

    # -------------------------
    # Core handlers
    # -------------------------

    async def handle_register_submission(self, interaction: discord.Interaction, igns: List[str]):
        if not interaction.guild:
            await safe_reply(interaction, "This must be used in a server.", ephemeral=True)
            return

        merged = self.add_user_igns(interaction.user.id, igns)

        char_ids = await self.ensure_character_ids_for_user(interaction.user.id, merged)
        self.set_user_character_ids(interaction.user.id, char_ids)

        await self.delete_previous_user_request_message(interaction.guild, interaction.user.id)

        ch = await self.ensure_access_channel(interaction.guild)

        temp_embed = self.build_request_embed(
            discord_user_id=interaction.user.id,
            discord_user_tag=str(interaction.user),
            igns=merged,
            status="PENDING",
            message_id=None,
        )

        msg = await ch.send(embed=temp_embed, view=AccessRequestView(self))

        final_embed = self.build_request_embed(
            discord_user_id=interaction.user.id,
            discord_user_tag=str(interaction.user),
            igns=merged,
            status="PENDING",
            message_id=msg.id,
        )
        await msg.edit(embed=final_embed)

        self.upsert_request(
            msg.id,
            {
                "guild_id": interaction.guild.id,
                "channel_id": ch.id,
                "message_id": msg.id,
                "discord_user_id": interaction.user.id,
                "discord_user_tag": str(interaction.user),
                "igns": merged,
                "status": "PENDING",
                "created_utc": utcnow_iso(),
                "updated_utc": utcnow_iso(),
                "updated_by": None,
            },
        )

        urec = self.get_user_record(interaction.user.id)
        urec["last_request_channel_id"] = ch.id
        urec["last_request_message_id"] = msg.id
        urec["updated_utc"] = utcnow_iso()
        save_state(self.state)

        await safe_reply(
            interaction,
            "IGN registration submitted/updated. Staff will process it in #location_access.",
            ephemeral=True,
        )

    async def handle_access_button(self, interaction: discord.Interaction, action: str):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await safe_reply(interaction, "This action must be used in a server.", ephemeral=True)
            return
        if not has_any_role(interaction.user, ALLOWED_ROLES):
            await safe_reply(interaction, "You do not have permission to use these buttons.", ephemeral=True)
            return

        await safe_defer(interaction, ephemeral=True)

        message = interaction.message
        if not message:
            await safe_reply(interaction, "Unable to read the request message.", ephemeral=True)
            return

        req = self.get_request(message.id)
        if not req:
            await safe_reply(interaction, "This request is not recognized (missing from storage).", ephemeral=True)
            return

        new_status = "ADDED" if action == "added" else "PENDING"
        self.update_request_status(message.id, new_status, interaction.user.id)

        emb = self.build_request_embed(
            discord_user_id=int(req["discord_user_id"]),
            discord_user_tag=str(req.get("discord_user_tag", "unknown")),
            igns=req.get("igns", []),
            status=new_status,
            message_id=message.id,
        )

        try:
            await message.edit(embed=emb, view=AccessRequestView(self))
        except Exception:
            pass

        await safe_reply(interaction, f"Request updated to `{new_status}`.", ephemeral=True)

    async def handle_leave_purge_button(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await safe_reply(interaction, "This action must be used in a server.", ephemeral=True)
            return
        if not has_any_role(interaction.user, ALLOWED_ROLES):
            await safe_reply(interaction, "You do not have permission to use this button.", ephemeral=True)
            return

        await safe_defer(interaction, ephemeral=True)

        msg = interaction.message
        if not msg:
            await safe_reply(interaction, "Unable to read the warning message.", ephemeral=True)
            return

        lw = self.get_leave_warning(msg.id)
        if not lw:
            await safe_reply(interaction, "This warning is not recognized (missing from storage).", ephemeral=True)
            return

        departed_user_id = int(lw["discord_user_id"])
        igns = lw.get("igns", [])

        self.delete_user_record(departed_user_id)

        processed_by = f"{interaction.user} (ID: {interaction.user.id})"
        self.update_leave_warning(msg.id, status="REMOVED", processed_by=processed_by)

        new_embed = self.build_leave_warning_embed(
            member_tag=str(lw.get("discord_user_tag", "unknown")),
            member_id=departed_user_id,
            igns=igns,
            status="REMOVED",
            warning_id=msg.id,
            purged_by=processed_by,
            reason=str(lw.get("reason", "DISCORD_LEFT")),
            extra=lw.get("extra"),
        )
        try:
            await msg.edit(embed=new_embed, view=LeaveWarningView(self))
        except Exception:
            pass

        await safe_reply(
            interaction,
            "Marked as removed and purged the member's IGN data from the bot.",
            ephemeral=True,
        )

    # -------------------------
    # Events
    # -------------------------

    @commands.Cog.listener()
    async def on_ready(self):
        if not self._panels_ensured_once:
            self._panels_ensured_once = True
            for guild in list(getattr(self.bot, "guilds", [])):
                try:
                    await self.ensure_register_panel_message(guild)
                except Exception:
                    pass

        if not self._corp_loop_started:
            self._corp_loop_started = True
            if EVE_CLIENT_ID and EVE_CLIENT_SECRET and (EVE_REFRESH_TOKEN_ENV or self.state.get("esi", {}).get("refresh_token")) and EVE_CORP_ID.isdigit():
                self.corp_membership_watch_loop.start()

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        if not had_role_name(member, ARC_SECURITY_ROLE):
            return

        rec = self.state.get("users", {}).get(str(member.id))
        if not rec:
            return

        igns = rec.get("igns", [])
        if not igns:
            return

        try:
            ch = await self.ensure_access_channel(member.guild)

            temp = self.build_leave_warning_embed(
                member_tag=str(member),
                member_id=member.id,
                igns=igns,
                status="PENDING_REMOVAL",
                warning_id=None,
                purged_by=None,
                reason="DISCORD_LEFT",
                extra=None,
            )
            msg = await ch.send(embed=temp, view=LeaveWarningView(self))

            final = self.build_leave_warning_embed(
                member_tag=str(member),
                member_id=member.id,
                igns=igns,
                status="PENDING_REMOVAL",
                warning_id=msg.id,
                purged_by=None,
                reason="DISCORD_LEFT",
                extra=None,
            )
            await msg.edit(embed=final, view=LeaveWarningView(self))

            self.upsert_leave_warning(
                msg.id,
                {
                    "guild_id": member.guild.id,
                    "channel_id": ch.id,
                    "message_id": msg.id,
                    "discord_user_id": member.id,
                    "discord_user_tag": str(member),
                    "igns": igns,
                    "status": "PENDING_REMOVAL",
                    "reason": "DISCORD_LEFT",
                    "extra": None,
                    "created_utc": utcnow_iso(),
                    "updated_utc": utcnow_iso(),
                    "processed_by": None,
                },
            )

        except Exception:
            pass

    # -------------------------
    # Corp watch loop
    # -------------------------

    @tasks.loop(minutes=CORP_CHECK_MINUTES)
    async def corp_membership_watch_loop(self):
        if not EVE_CORP_ID.isdigit():
            return
        corp_id = int(EVE_CORP_ID)

        users = self.state.get("users", {}) or {}
        tracked: List[int] = []
        for _, rec in users.items():
            for cid in rec.get("character_ids", []) or []:
                if isinstance(cid, int):
                    tracked.append(cid)
        tracked_set = set(tracked)
        if not tracked_set:
            return

        async with aiohttp.ClientSession() as session:
            member_ids = await self.esi.get_corp_membertracking_ids(session, corp_id)

        if member_ids is None:
            return

        in_corp_now = set(member_ids)
        corp_watch = self.state.setdefault("corp_watch", {})

        for cid in tracked_set:
            key = str(cid)
            rec = corp_watch.setdefault(key, {"in_corp": None, "last_change_utc": None, "last_alerted_left_utc": None})

            prev = rec.get("in_corp")
            now = (cid in in_corp_now)

            if prev is None:
                rec["in_corp"] = now
                rec["last_change_utc"] = utcnow_iso()
                continue

            if prev is True and now is False:
                rec["in_corp"] = False
                rec["last_change_utc"] = utcnow_iso()
                rec["last_alerted_left_utc"] = utcnow_iso()

                await self._post_corp_leave_notice(cid)

            elif prev is False and now is True:
                rec["in_corp"] = True
                rec["last_change_utc"] = utcnow_iso()

        save_state(self.state)

    @corp_membership_watch_loop.before_loop
    async def before_corp_membership_watch_loop(self):
        await self.bot.wait_until_ready()

    async def _post_corp_leave_notice(self, character_id: int) -> None:
        """
        Informational notice only (no action, no buttons, no leave_warnings storage).
        """
        discord_user_id = self.find_discord_user_for_character(character_id)

        guilds = list(getattr(self.bot, "guilds", []))
        for g in guilds:
            try:
                ch = await self.ensure_access_channel(g)

                member_tag = "unknown"
                member_id = 0
                igns: List[str] = []

                if discord_user_id is not None:
                    member_id = int(discord_user_id)
                    m = g.get_member(member_id)
                    if m:
                        member_tag = str(m)
                    rec = self.state.get("users", {}).get(str(member_id), {})
                    igns = rec.get("igns", []) or []

                embed = self.build_corp_leave_notice_embed(
                    member_tag=member_tag,
                    member_id=member_id,
                    igns=igns,
                    character_id=character_id,
                )
                await ch.send(embed=embed)
            except Exception:
                continue

    # -------------------------
    # Slash commands (staff only)
    # -------------------------

    @app_commands.command(name="unregister_ign", description="Remove a member's IGN(s) from their Discord link.")
    @require_roles()
    async def unregister_ign(self, interaction: discord.Interaction, member: discord.Member, ign: Optional[str] = None):
        await safe_defer(interaction, ephemeral=True)

        users = self.state.get("users", {})
        if str(member.id) not in users:
            await safe_reply(interaction, "That member has no registered IGN(s).", ephemeral=True)
            return

        if ign is None:
            removed = users[str(member.id)].get("igns", [])
            self.remove_user_ign(member.id, ign=None)
            await safe_reply(
                interaction,
                f"Removed all IGNs for {member.mention}: {', '.join(removed) if removed else '(none)'}",
                ephemeral=True,
            )
            return

        before = users[str(member.id)].get("igns", [])
        remaining = self.remove_user_ign(member.id, ign=ign)
        if len(remaining) == len(before):
            await safe_reply(interaction, "No matching IGN found to remove.", ephemeral=True)
        else:
            await safe_reply(
                interaction,
                f"Removed `{normalize_ign(ign)}` for {member.mention}. Remaining: {', '.join(remaining) if remaining else '(none)'}",
                ephemeral=True,
            )

    @app_commands.command(name="list_ign", description="List the registered IGN(s) for a member.")
    @require_roles()
    async def list_ign(self, interaction: discord.Interaction, member: discord.Member):
        await safe_defer(interaction, ephemeral=True)

        rec = self.state.get("users", {}).get(str(member.id))
        if not rec or not rec.get("igns"):
            await safe_reply(interaction, "No registered IGN(s) for that member.", ephemeral=True)
            return

        igns = rec.get("igns", [])
        cids = rec.get("character_ids", [])
        emb = discord.Embed(
            title="Registered IGN(s)",
            description=(
                f"**Member:** {member.mention} (`{member}`)\n"
                f"**IGN(s):** {', '.join(igns)}\n"
                f"**Character ID(s):** {', '.join(str(x) for x in (cids or [])) or '(unresolved)'}"
            ),
            timestamp=datetime.datetime.utcnow(),
        )
        emb.set_footer(text="IGN Registration")
        await safe_reply(interaction, embed=emb, ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(IGNRegistrationCog(bot))