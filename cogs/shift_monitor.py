# shift_monitor.py
# discord.py 2.x Cog (Railway-safe, restart-safe, rate-limit-safe)
# - Auto-creates missing channels + escalation role
# - Persists ONLY IDs + primitives (no discord.Message objects in JSON)
# - Uses unique custom_ids per shift/escalation so Views survive restarts
# - Avoids spam message edits (edits only on state changes)
# - Invite tracking: init cache on startup + persistent invite_registry
# - "Owner-only" admin functions: Server Owner OR role "ARC Security Corporation Leader"

import asyncio
import json
import os
from datetime import datetime, timezone, timedelta, time as dtime
from pathlib import Path
from typing import Any, Dict, Optional

import discord
from discord.ext import commands, tasks
from discord import app_commands

# ================= CONFIG =================
GUILD_ID = 1444318058419322983

# Role that is allowed to use owner-only functions (in addition to server owner)
OWNER_ROLE_NAME = "ARC Security Corporation Leader"

# Server objects to ensure exist
SHIFT_CATEGORY_NAME = "Recruiter Scheduling"
ESCALATION_ROLE_NAME = "Recruiter Escalation"  # created if missing
LOG_CHANNEL_NAME = "shift-log"

CHANNEL_NAMES = {
    1: "recruiter-scheduling-1",
    2: "recruiter-scheduling-2",
    3: "recruiter-scheduling-3",
    4: "recruiter-scheduling-4",
    "escalation": "recruiter-claims",
}

# Checkpoints interpreted in UTC (tell me if you want AST/ADT instead)
CHECKPOINTS_DEFAULT = {
    1: "00:00",
    2: "06:00",
    3: "12:00",
    4: "18:00",
}

ESCALATION_TIMEOUT = timedelta(seconds=15)  # testing; set to minutes for real usage

# Railway persistence
DATA_DIR = Path("/data")
STATE_FILE = DATA_DIR / "shift_state.json"


# ================= UTIL =================
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_hhmm(s: str) -> dtime:
    return datetime.strptime(s, "%H:%M").time()


def atomic_json_write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


def safe_int(x, default=None):
    try:
        return int(x)
    except Exception:
        return default


def get_current_checkpoint(checkpoints: Dict[int, str]) -> int:
    ordered = sorted((k, parse_hhmm(v)) for k, v in checkpoints.items())
    now = now_utc().time()

    for i in range(len(ordered)):
        cp, start = ordered[i]
        next_start = ordered[(i + 1) % len(ordered)][1]
        if start <= now < next_start or (i == len(ordered) - 1 and (now >= start or now < ordered[0][1])):
            return cp
    return ordered[0][0]


def time_until_next_checkpoint(checkpoints: Dict[int, str]) -> timedelta:
    now = now_utc()
    times = sorted(parse_hhmm(v) for v in checkpoints.values())
    for t in times:
        if now.time() < t:
            return datetime.combine(now.date(), t, tzinfo=timezone.utc) - now
    return datetime.combine(now.date() + timedelta(days=1), times[0], tzinfo=timezone.utc) - now


def fmt_td(td: timedelta) -> str:
    total = max(0, int(td.total_seconds()))
    h, r = divmod(total, 3600)
    m, s = divmod(r, 60)
    return f"{h}h {m}m {s}s"


def has_owner_privs(interaction: discord.Interaction) -> bool:
    """
    Owner-only access: server owner OR member has OWNER_ROLE_NAME.
    """
    if not interaction.guild:
        return False

    if interaction.user.id == interaction.guild.owner_id:
        return True

    if isinstance(interaction.user, discord.Member):
        return any(r.name == OWNER_ROLE_NAME for r in interaction.user.roles)

    return False


# ================= VIEWS =================
class ShiftView(discord.ui.View):
    """
    Persistent view. custom_ids unique per shift:
      shift:{timer_id}:start
      shift:{timer_id}:stop
    """
    def __init__(self, cog: "ShiftMonitor", timer_id: str, shift_num: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.timer_id = timer_id
        self.shift_num = shift_num

        self.start_btn.custom_id = f"shift:{timer_id}:start"
        self.stop_btn.custom_id = f"shift:{timer_id}:stop"

        self.sync_enabled_states()

    def sync_enabled_states(self):
        state = self.cog.shift_state.get(self.timer_id, {})
        active = bool(state.get("active"))
        locked = bool(state.get("locked"))
        running = bool(state.get("running"))

        self.start_btn.disabled = not (active and (not locked) and (not running))
        self.stop_btn.disabled = not running

    @discord.ui.button(label="Start Shift", style=discord.ButtonStyle.success, custom_id="shift:placeholder:start")
    async def start_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_shift_start(interaction, self.timer_id)

    @discord.ui.button(label="End Shift", style=discord.ButtonStyle.danger, custom_id="shift:placeholder:stop")
    async def stop_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_shift_stop(interaction, self.timer_id)


class EscalationView(discord.ui.View):
    """
    Persistent view. custom_ids unique per shift:
      esc:{shift_num}:claim
      esc:{shift_num}:stop
    """
    def __init__(self, cog: "ShiftMonitor", shift_num: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.shift_num = shift_num

        self.claim_btn.custom_id = f"esc:{shift_num}:claim"
        self.stop_btn.custom_id = f"esc:{shift_num}:stop"

        self.sync_enabled_states()

    def sync_enabled_states(self):
        esc = self.cog.escalation_state.get(str(self.shift_num), {})
        owner_id = esc.get("owner_id")
        self.claim_btn.disabled = owner_id is not None
        self.stop_btn.disabled = owner_id is None

    @discord.ui.button(label="Claim Shift", style=discord.ButtonStyle.success, custom_id="esc:placeholder:claim")
    async def claim_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_escalation_claim(interaction, self.shift_num)

    @discord.ui.button(label="Stop Shift", style=discord.ButtonStyle.danger, custom_id="esc:placeholder:stop")
    async def stop_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_escalation_stop(interaction, self.shift_num)


# ================= COG =================
class ShiftMonitor(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

        self.shift_state: Dict[str, Dict[str, Any]] = {}
        self.escalation_state: Dict[str, Dict[str, Any]] = {}
        self.invite_cache: Dict[str, int] = {}
        self.invite_registry: Dict[str, Dict[str, Any]] = {}

        self.checkpoints: Dict[int, str] = dict(CHECKPOINTS_DEFAULT)

        self.escalation_role_id: Optional[int] = None
        self._log_channel_id: Optional[int] = None

        self._lock = asyncio.Lock()

        self.load_state()

    # ----------------- Persistence -----------------
    def load_state(self):
        if not STATE_FILE.exists():
            return
        try:
            with STATE_FILE.open("r", encoding="utf-8") as f:
                data = json.load(f)

            self.shift_state = data.get("shift_state", {}) or {}
            self.escalation_state = data.get("escalation_state", {}) or {}
            self.invite_cache = data.get("invite_cache", {}) or {}
            self.invite_registry = data.get("invite_registry", {}) or {}

            cp = data.get("checkpoints")
            if isinstance(cp, dict):
                self.checkpoints = {safe_int(k, k): v for k, v in cp.items() if safe_int(k, None) is not None}

            self.escalation_role_id = data.get("escalation_role_id")
            self._log_channel_id = data.get("log_channel_id")

        except Exception:
            self.shift_state = {}
            self.escalation_state = {}
            self.invite_cache = {}
            self.invite_registry = {}
            self.checkpoints = dict(CHECKPOINTS_DEFAULT)
            self.escalation_role_id = None
            self._log_channel_id = None

    def save_state(self):
        payload = {
            "shift_state": self.shift_state,
            "escalation_state": self.escalation_state,
            "invite_cache": self.invite_cache,
            "invite_registry": self.invite_registry,
            "checkpoints": self.checkpoints,
            "escalation_role_id": self.escalation_role_id,
            "log_channel_id": self._log_channel_id,
        }
        atomic_json_write(STATE_FILE, payload)

    # ----------------- Logging -----------------
    async def log(self, message: str):
        print(message)
        guild = self.bot.get_guild(GUILD_ID)
        if not guild:
            return

        channel = None
        if self._log_channel_id:
            channel = guild.get_channel(self._log_channel_id)

        if channel is None:
            channel = discord.utils.get(guild.text_channels, name=LOG_CHANNEL_NAME)
            if channel:
                self._log_channel_id = channel.id
                self.save_state()

        if channel:
            try:
                await channel.send(message)
            except discord.HTTPException:
                pass

    # ----------------- Server Setup -----------------
    async def ensure_role(self, guild: discord.Guild) -> discord.Role:
        role = None
        if self.escalation_role_id:
            role = guild.get_role(self.escalation_role_id)
        if role is None:
            role = discord.utils.get(guild.roles, name=ESCALATION_ROLE_NAME)
        if role is None:
            role = await guild.create_role(name=ESCALATION_ROLE_NAME, reason="ShiftMonitor auto-setup")
        self.escalation_role_id = role.id
        self.save_state()
        return role

    async def ensure_category(self, guild: discord.Guild) -> discord.CategoryChannel:
        cat = discord.utils.get(guild.categories, name=SHIFT_CATEGORY_NAME)
        if cat is None:
            cat = await guild.create_category(name=SHIFT_CATEGORY_NAME, reason="ShiftMonitor auto-setup")
        return cat

    async def ensure_text_channel(self, guild: discord.Guild, name: str, category: Optional[discord.CategoryChannel]) -> discord.TextChannel:
        ch = discord.utils.get(guild.text_channels, name=name)
        if ch is None:
            ch = await guild.create_text_channel(name=name, category=category, reason="ShiftMonitor auto-setup")
        return ch

    async def ensure_server_objects(self):
        guild = self.bot.get_guild(GUILD_ID)
        if not guild:
            return

        cat = await self.ensure_category(guild)
        await self.ensure_role(guild)

        log_ch = discord.utils.get(guild.text_channels, name=LOG_CHANNEL_NAME)
        if log_ch is None:
            log_ch = await guild.create_text_channel(name=LOG_CHANNEL_NAME, category=cat, reason="ShiftMonitor auto-setup")
        self._log_channel_id = log_ch.id

        for i in range(1, 5):
            await self.ensure_text_channel(guild, CHANNEL_NAMES[i], cat)
        await self.ensure_text_channel(guild, CHANNEL_NAMES["escalation"], cat)

        self.save_state()

    # ----------------- Message Hydration -----------------
    async def fetch_message(self, guild: discord.Guild, channel_id: int, message_id: int) -> Optional[discord.Message]:
        ch = guild.get_channel(channel_id)
        if not isinstance(ch, discord.TextChannel):
            return None
        try:
            return await ch.fetch_message(message_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return None

    async def ensure_shift_message(self, guild: discord.Guild, shift_num: int) -> None:
        timer_id = f"id_{shift_num}"
        channel = discord.utils.get(guild.text_channels, name=CHANNEL_NAMES[shift_num])
        if channel is None:
            return

        state = self.shift_state.get(timer_id)
        if not state:
            state = {
                "shift_num": shift_num,
                "channel_id": channel.id,
                "message_id": None,
                "active": False,
                "running": False,
                "locked": False,
                "escalated": False,
                "owner_id": None,
                "started_by": None,
                "activated_ts": None,
                "start_ts": None,
                "last_render_key": None,
            }
            self.shift_state[timer_id] = state

        state["channel_id"] = channel.id

        msg = None
        if state.get("message_id"):
            msg = await self.fetch_message(guild, state["channel_id"], state["message_id"])

        if msg is None:
            view = ShiftView(self, timer_id, shift_num)
            msg = await channel.send(f"⏱️ **Shift {shift_num}**", view=view)
            state["message_id"] = msg.id
            state["last_render_key"] = None

        self.bot.add_view(ShiftView(self, timer_id, shift_num))
        await self.render_shift_if_needed(guild, timer_id)

    async def ensure_escalation_message(self, guild: discord.Guild, shift_num: int) -> None:
        esc = self.escalation_state.get(str(shift_num))
        if not esc:
            return

        ch = discord.utils.get(guild.text_channels, name=CHANNEL_NAMES["escalation"])
        if ch is None:
            return
        esc["channel_id"] = ch.id

        msg = None
        if esc.get("message_id"):
            msg = await self.fetch_message(guild, esc["channel_id"], esc["message_id"])

        if msg is None:
            role = await self.ensure_role(guild)
            remaining = time_until_next_checkpoint(self.checkpoints)
            content = f"{role.mention} **Shift {shift_num} has unclaimed time ({fmt_td(remaining)} remaining). Claim it?**"
            view = EscalationView(self, shift_num)
            msg = await ch.send(content, view=view)
            esc["message_id"] = msg.id
            esc["start_ts"] = esc.get("start_ts") or int(now_utc().timestamp())
            esc["last_render_key"] = None

        self.bot.add_view(EscalationView(self, shift_num))
        await self.render_escalation_if_needed(guild, shift_num)

    # ----------------- Rendering -----------------
    async def render_shift_if_needed(self, guild: discord.Guild, timer_id: str) -> None:
        st = self.shift_state.get(timer_id)
        if not st:
            return
        render_key = f"a={int(bool(st.get('active')))}|r={int(bool(st.get('running')))}|l={int(bool(st.get('locked')))}|o={st.get('owner_id')}"
        if st.get("last_render_key") == render_key:
            return

        msg = await self.fetch_message(guild, st["channel_id"], st["message_id"])
        if msg is None:
            return

        view = ShiftView(self, timer_id, st["shift_num"])
        try:
            await msg.edit(view=view)
            st["last_render_key"] = render_key
            self.save_state()
        except discord.HTTPException:
            pass

    async def render_escalation_if_needed(self, guild: discord.Guild, shift_num: int) -> None:
        esc = self.escalation_state.get(str(shift_num))
        if not esc:
            return
        render_key = f"owner={esc.get('owner_id')}"
        if esc.get("last_render_key") == render_key:
            return

        msg = await self.fetch_message(guild, esc["channel_id"], esc["message_id"])
        if msg is None:
            return

        view = EscalationView(self, shift_num)
        try:
            await msg.edit(view=view)
            esc["last_render_key"] = render_key
            self.save_state()
        except discord.HTTPException:
            pass

    # ----------------- Core shift actions -----------------
    async def escalate_shift(self, shift_num: int, reason: str):
        guild = self.bot.get_guild(GUILD_ID)
        if not guild:
            return

        esc_key = str(shift_num)
        if esc_key in self.escalation_state:
            return

        timer_id = f"id_{shift_num}"
        st = self.shift_state.get(timer_id)
        if not st:
            return

        st["locked"] = True
        await self.render_shift_if_needed(guild, timer_id)

        role = await self.ensure_role(guild)
        ch = discord.utils.get(guild.text_channels, name=CHANNEL_NAMES["escalation"])
        if not ch:
            return

        remaining = time_until_next_checkpoint(self.checkpoints)
        content = f"{role.mention} **Shift {shift_num} has unclaimed time ({fmt_td(remaining)} remaining). Claim it?**\nReason: {reason}"
        msg = await ch.send(content, view=EscalationView(self, shift_num))

        self.escalation_state[esc_key] = {
            "shift_num": shift_num,
            "channel_id": ch.id,
            "message_id": msg.id,
            "owner_id": None,
            "start_ts": int(now_utc().timestamp()),
            "last_render_key": None,
        }

        self.bot.add_view(EscalationView(self, shift_num))
        self.save_state()

        await self.log(f"⚠️ Escalation started for Shift {shift_num} ({reason}) (<t:{int(datetime.now().timestamp())}:f>)")

    async def end_escalation(self, shift_num: int, reason: str):
        guild = self.bot.get_guild(GUILD_ID)
        if not guild:
            return

        esc_key = str(shift_num)
        esc = self.escalation_state.get(esc_key)
        if not esc:
            return

        timer_id = f"id_{shift_num}"
        st = self.shift_state.get(timer_id)
        if st:
            st["locked"] = False
            await self.render_shift_if_needed(guild, timer_id)

        self.escalation_state.pop(esc_key, None)
        self.save_state()

        await self.log(f"🟥 Escalation ended for Shift {shift_num} ({reason}) (<t:{int(datetime.now().timestamp())}:f>)")

    async def force_stop(self, timer_id: str, reason: str, manual: bool = False):
        guild = self.bot.get_guild(GUILD_ID)
        if not guild:
            return

        st = self.shift_state.get(timer_id)
        if not st or not st.get("running"):
            return

        st["running"] = False
        st["started_by"] = None
        st["start_ts"] = None

        # Escalate if manually stopped while still active and >60 minutes remain.
        if manual and st.get("active") and (time_until_next_checkpoint(self.checkpoints) > timedelta(minutes=60)) and not st.get("escalated"):
            st["escalated"] = True
            self.save_state()
            await self.render_shift_if_needed(guild, timer_id)
            await self.log(f"⚠️ Shift {st['shift_num']} ended early — escalating (<t:{int(datetime.now().timestamp())}:f>)")
            await self.escalate_shift(st["shift_num"], "Ended early")
            return

        self.save_state()
        await self.render_shift_if_needed(guild, timer_id)
        await self.log(f"🔴 Shift {st['shift_num']} ended ({reason}) (<t:{int(datetime.now().timestamp())}:f>)")

    # ----------------- Interaction handlers -----------------
    async def handle_shift_start(self, interaction: discord.Interaction, timer_id: str):
        if not interaction.guild or interaction.guild.id != GUILD_ID:
            return

        async with self._lock:
            st = self.shift_state.get(timer_id)
            if not st:
                await interaction.response.send_message("❌ Shift state missing.", ephemeral=True)
                return

            if not st.get("active"):
                await interaction.response.send_message("❌ Shift is not active.", ephemeral=True)
                return
            if st.get("locked"):
                await interaction.response.send_message("❌ Shift is locked due to escalation.", ephemeral=True)
                return
            if st.get("running"):
                await interaction.response.send_message("❌ Shift already running.", ephemeral=True)
                return

            guild_owner_id = interaction.guild.owner_id
            owner_id = st.get("owner_id")
            if owner_id and interaction.user.id not in (owner_id, guild_owner_id):
                await interaction.response.send_message("❌ You are not assigned to this shift.", ephemeral=True)
                return

            st["running"] = True
            st["started_by"] = interaction.user.id
            st["start_ts"] = int(now_utc().timestamp())
            self.save_state()

        await self.render_shift_if_needed(interaction.guild, timer_id)
        await interaction.response.send_message(f"🟢 Shift {st['shift_num']} started.", ephemeral=True)
        await self.log(f"🟢 Shift {st['shift_num']} started by {interaction.user.display_name} (<t:{int(datetime.now().timestamp())}:f>)")

    async def handle_shift_stop(self, interaction: discord.Interaction, timer_id: str):
        if not interaction.guild or interaction.guild.id != GUILD_ID:
            return

        async with self._lock:
            st = self.shift_state.get(timer_id)
            if not st:
                await interaction.response.send_message("❌ Shift state missing.", ephemeral=True)
                return

            starter = st.get("started_by")
            if interaction.user.id not in (starter, interaction.guild.owner_id):
                await interaction.response.send_message("❌ You did not start this shift.", ephemeral=True)
                return

        await self.force_stop(timer_id, "Manual stop", manual=True)
        await interaction.response.send_message("🔴 Shift ended.", ephemeral=True)

    async def handle_escalation_claim(self, interaction: discord.Interaction, shift_num: int):
        if not interaction.guild or interaction.guild.id != GUILD_ID:
            return

        async with self._lock:
            esc = self.escalation_state.get(str(shift_num))
            if not esc:
                await interaction.response.send_message("❌ No active escalation for that shift.", ephemeral=True)
                return
            if esc.get("owner_id") is not None:
                await interaction.response.send_message("Shift already claimed.", ephemeral=True)
                return

            esc["owner_id"] = interaction.user.id
            self.save_state()

        await self.render_escalation_if_needed(interaction.guild, shift_num)
        await interaction.response.send_message("✅ You claimed this escalation shift.", ephemeral=True)
        await self.log(f"✅ Escalation shift {shift_num} claimed by {interaction.user.display_name} (<t:{int(datetime.now().timestamp())}:f>)")

    async def handle_escalation_stop(self, interaction: discord.Interaction, shift_num: int):
        if not interaction.guild or interaction.guild.id != GUILD_ID:
            return

        async with self._lock:
            esc = self.escalation_state.get(str(shift_num))
            if not esc:
                await interaction.response.send_message("❌ No active escalation for that shift.", ephemeral=True)
                return
            if interaction.user.id != esc.get("owner_id"):
                await interaction.response.send_message("Only the claimer can stop this shift.", ephemeral=True)
                return

        await self.end_escalation(shift_num, "Manual stop by claimer")
        await interaction.response.send_message("🟥 Escalation shift ended.", ephemeral=True)

    # ----------------- Checkpoint loop -----------------
    @tasks.loop(seconds=10)
    async def checkpoint_loop(self):
        guild = self.bot.get_guild(GUILD_ID)
        if not guild:
            return

        current_cp = get_current_checkpoint(self.checkpoints)
        now_ts = int(now_utc().timestamp())
        to_escalate: list[int] = []
        to_render: set[str] = set()

        async with self._lock:
            for timer_id, st in self.shift_state.items():
                sn = st.get("shift_num")
                if sn not in (1, 2, 3, 4):
                    continue

                locked = bool(st.get("locked"))
                should_be_active = (sn == current_cp) and (not locked)

                if should_be_active and not st.get("active"):
                    st["active"] = True
                    st["activated_ts"] = now_ts
                    st["escalated"] = False
                    to_render.add(timer_id)

                elif (not should_be_active) and st.get("active"):
                    if st.get("running"):
                        st["running"] = False
                        st["started_by"] = None
                        st["start_ts"] = None
                    st["active"] = False
                    st["activated_ts"] = None
                    st["escalated"] = False
                    to_render.add(timer_id)

                if (
                    st.get("active")
                    and not st.get("running")
                    and not st.get("escalated")
                    and st.get("activated_ts") is not None
                    and (now_ts - int(st["activated_ts"])) >= int(ESCALATION_TIMEOUT.total_seconds())
                ):
                    st["escalated"] = True
                    to_render.add(timer_id)
                    to_escalate.append(sn)

            self.save_state()

        for timer_id in to_render:
            await self.render_shift_if_needed(guild, timer_id)

        for sn in to_escalate:
            if str(sn) not in self.escalation_state:
                await self.escalate_shift(sn, f"No one started within {fmt_td(ESCALATION_TIMEOUT)}")

    @checkpoint_loop.before_loop
    async def before_checkpoint_loop(self):
        await self.bot.wait_until_ready()

    # ----------------- Invite tracking -----------------
    async def init_invites(self, guild: discord.Guild):
        try:
            invites = await guild.invites()
        except discord.Forbidden:
            return
        except discord.HTTPException:
            return

        cache = {i.code: (i.uses or 0) for i in invites}
        try:
            vanity = await guild.vanity_invite()
            if vanity and vanity.uses is not None:
                cache["vanity"] = vanity.uses
        except Exception:
            pass

        self.invite_cache = cache
        self.save_state()

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        if member.guild.id != GUILD_ID:
            return
        guild = member.guild

        try:
            new_invites = await guild.invites()
        except discord.Forbidden:
            return
        except discord.HTTPException:
            return

        used_code = None
        for inv in new_invites:
            old_uses = self.invite_cache.get(inv.code, 0)
            new_uses = inv.uses or 0
            if new_uses > old_uses:
                used_code = inv.code
                break

        if used_code is None:
            try:
                vanity = await guild.vanity_invite()
                if vanity and vanity.uses is not None and vanity.uses > self.invite_cache.get("vanity", 0):
                    used_code = "vanity"
            except Exception:
                pass

        self.invite_cache = {inv.code: (inv.uses or 0) for inv in new_invites}
        try:
            vanity = await guild.vanity_invite()
            if vanity and vanity.uses is not None:
                self.invite_cache["vanity"] = vanity.uses
        except Exception:
            pass
        self.save_state()

        if used_code is None:
            await self.log(f"⚠️ Could not detect which invite {member.mention} used. (<t:{int(datetime.now().timestamp())}:f>)")
            return

        assigned = self.invite_registry.get(used_code)
        if not assigned:
            await self.log(f"🚨 ALERT: {member.mention} joined using `{used_code}`, which is not an assigned invite. (<t:{int(datetime.now().timestamp())}:f>)")
            return

        shift_id = assigned.get("shift_id")
        owner_id = assigned.get("owner_id")
        await self.log(f"✅ <@{owner_id}>'s assigned invite `{used_code}` used by {member.mention} (Shift {shift_id}) (<t:{int(datetime.now().timestamp())}:f>).")

    # ----------------- Slash Commands -----------------
    @app_commands.command(name="setup_shifts", description="Create missing channels/role and initialize shift messages.")
    async def setup_shifts_cmd(self, interaction: discord.Interaction):
        if not interaction.guild or interaction.guild.id != GUILD_ID:
            return
        if not has_owner_privs(interaction):
            await interaction.response.send_message(
                f"Only the **server owner** or **{OWNER_ROLE_NAME}** can use this.",
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)

        await self.ensure_server_objects()

        guild = interaction.guild
        for i in range(1, 5):
            await self.ensure_shift_message(guild, i)

        for key in list(self.escalation_state.keys()):
            sn = safe_int(key, None)
            if sn in (1, 2, 3, 4):
                await self.ensure_escalation_message(guild, sn)

        await self.init_invites(guild)

        await interaction.followup.send("✅ Setup complete.", ephemeral=True)

    @app_commands.command(name="setcheckpoint", description="Set a checkpoint time (HH:MM in UTC).")
    async def setcheckpoint_cmd(self, interaction: discord.Interaction, shift_number: int, time_str: str):
        if not interaction.guild or interaction.guild.id != GUILD_ID:
            return
        if not has_owner_privs(interaction):
            await interaction.response.send_message(
                f"Only the **server owner** or **{OWNER_ROLE_NAME}** can use this.",
                ephemeral=True
            )
            return
        if shift_number not in (1, 2, 3, 4):
            await interaction.response.send_message("Shift number must be 1-4.", ephemeral=True)
            return
        try:
            parse_hhmm(time_str)
        except Exception:
            await interaction.response.send_message("Invalid time. Use HH:MM (e.g., 06:00).", ephemeral=True)
            return

        async with self._lock:
            self.checkpoints[shift_number] = time_str
            self.save_state()

        await interaction.response.send_message("✅ Checkpoint updated.", ephemeral=True)

    @app_commands.command(name="setowner", description="Assign a user to a shift.")
    async def setowner_cmd(self, interaction: discord.Interaction, shift_number: int, user: discord.Member):
        if not interaction.guild or interaction.guild.id != GUILD_ID:
            return
        if not has_owner_privs(interaction):
            await interaction.response.send_message(
                f"Only the **server owner** or **{OWNER_ROLE_NAME}** can use this.",
                ephemeral=True
            )
            return
        if shift_number not in (1, 2, 3, 4):
            await interaction.response.send_message("Shift number must be 1-4.", ephemeral=True)
            return

        timer_id = f"id_{shift_number}"
        async with self._lock:
            if timer_id not in self.shift_state:
                await interaction.response.send_message("Shift not initialized yet. Run /setup_shifts first.", ephemeral=True)
                return
            self.shift_state[timer_id]["owner_id"] = user.id
            self.shift_state[timer_id]["last_render_key"] = None
            self.save_state()

        await self.render_shift_if_needed(interaction.guild, timer_id)
        await interaction.response.send_message(f"✅ Owner for Shift {shift_number} set to {user.mention}.", ephemeral=True)

    @app_commands.command(name="linkassign", description="Assign an invite URL/code to a shift for auditing.")
    async def linkassign_cmd(self, interaction: discord.Interaction, shift_id: int, invite_url: str):
        if not interaction.guild or interaction.guild.id != GUILD_ID:
            return
        if not has_owner_privs(interaction):
            await interaction.response.send_message(
                f"Only the **server owner** or **{OWNER_ROLE_NAME}** can use this.",
                ephemeral=True
            )
            return
        if shift_id not in (1, 2, 3, 4):
            await interaction.response.send_message("Shift id must be 1-4.", ephemeral=True)
            return

        code = invite_url.split("/")[-1].strip().replace(" ", "")

        try:
            invites = await interaction.guild.invites()
        except discord.Forbidden:
            await interaction.response.send_message("Bot needs **Manage Server** permission to read invites.", ephemeral=True)
            return
        except discord.HTTPException:
            await interaction.response.send_message("Could not fetch invites right now. Try again.", ephemeral=True)
            return

        valid_codes = {i.code for i in invites}
        if code != "vanity" and code not in valid_codes:
            await interaction.response.send_message("Invalid or unknown invite code (or use `vanity`).", ephemeral=True)
            return

        async with self._lock:
            self.invite_registry[code] = {"shift_id": shift_id, "owner_id": interaction.user.id}
            self.save_state()

        await interaction.response.send_message(f"✅ Invite `{code}` assigned to Shift {shift_id}.", ephemeral=True)

    @app_commands.command(name="checkpoints", description="Show checkpoints (UTC).")
    async def checkpoints_cmd(self, interaction: discord.Interaction):
        if not interaction.guild or interaction.guild.id != GUILD_ID:
            return
        lines = [f"{k}: {v} UTC" for k, v in sorted(self.checkpoints.items())]
        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    # ----------------- Lifecycle -----------------
    async def cog_load(self):
        guild_obj = discord.Object(id=GUILD_ID)
        self.bot.tree.add_command(self.setup_shifts_cmd, guild=guild_obj)
        self.bot.tree.add_command(self.setcheckpoint_cmd, guild=guild_obj)
        self.bot.tree.add_command(self.setowner_cmd, guild=guild_obj)
        self.bot.tree.add_command(self.linkassign_cmd, guild=guild_obj)
        self.bot.tree.add_command(self.checkpoints_cmd, guild=guild_obj)
        try:
            await self.bot.tree.sync(guild=guild_obj)
        except Exception:
            pass

        await self.bot.wait_until_ready()
        guild = self.bot.get_guild(GUILD_ID)
        if not guild:
            return

        await self.ensure_server_objects()

        for i in range(1, 5):
            await self.ensure_shift_message(guild, i)

        for key in list(self.escalation_state.keys()):
            sn = safe_int(key, None)
            if sn in (1, 2, 3, 4):
                await self.ensure_escalation_message(guild, sn)

        await self.init_invites(guild)

        if not self.checkpoint_loop.is_running():
            self.checkpoint_loop.start()

        await self.log("✅ ShiftMonitor loaded and running.")

    async def cog_unload(self):
        if self.checkpoint_loop.is_running():
            self.checkpoint_loop.cancel()
        self.save_state()


# ================= SETUP =================
async def setup(bot: commands.Bot):
    await bot.add_cog(ShiftMonitor(bot))
