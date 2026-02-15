# cogs/shift_monitor.py
# GLOBAL (multi-guild) version
# - No hardcoded GUILD_ID
# - Per-guild state persisted to /data/shift_state.json
# - Persistent views with custom_id containing guild_id to avoid collisions
# - No deadlock on startup; post-ready init runs in background
# - Restart-safe: stores only primitives (ids, bools, ints, strings)
# - FIX: Avoid message EDIT burst on startup (prevents 429 rate limits)
# - FIX: Global edit throttle for any unavoidable edits
#
# ✅ NEW FEATURE:
# - Auto "clock out" on shift rollover (checkpoint change) if someone forgot to end the shift.
#   This records duration + logs the auto clock-out event so you don't lose the session.

import asyncio
import json
import os
from datetime import datetime, timezone, timedelta, time as dtime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import discord
from discord.ext import commands, tasks
from discord import app_commands

# ================= CONFIG (GLOBAL DEFAULTS) =================
OWNER_ROLE_NAME = "ARC Security Corporation Leader"

SHIFT_CATEGORY_NAME = "Recruiter Scheduling"
ESCALATION_ROLE_NAME = "Recruiter Escalation"
LOG_CHANNEL_NAME = "shift-log"

CHANNEL_NAMES = {
    1: "recruiter-scheduling-1",
    2: "recruiter-scheduling-2",
    3: "recruiter-scheduling-3",
    4: "recruiter-scheduling-4",
    "escalation": "recruiter-claims",
}

CHECKPOINTS_DEFAULT = {
    1: "00:00",
    2: "06:00",
    3: "12:00",
    4: "18:00",
}

# ✅ CHANGED: 15 seconds -> 20 minutes
ESCALATION_TIMEOUT = timedelta(minutes=20)

DATA_DIR = Path(os.getenv("PERSIST_ROOT", "/data"))
STATE_FILE = DATA_DIR / "shift_state.json"

# Global spacing between actual message edits (safety net)
GLOBAL_EDIT_MIN_INTERVAL_SECONDS = 1.25


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
    if not interaction.guild:
        return False
    if interaction.user.id == interaction.guild.owner_id:
        return True
    if isinstance(interaction.user, discord.Member):
        return any(r.name == OWNER_ROLE_NAME for r in interaction.user.roles)
    return False


# ----------------- Persistent Views -----------------
class ShiftView(discord.ui.View):
    def __init__(self, cog: "ShiftMonitor", guild_id: int, timer_id: str, shift_num: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild_id = guild_id
        self.timer_id = timer_id
        self.shift_num = shift_num

        # Persistent unique ids (include guild_id to avoid collisions across servers)
        self.start_btn.custom_id = f"shift:{guild_id}:{timer_id}:start"
        self.stop_btn.custom_id = f"shift:{guild_id}:{timer_id}:stop"
        self.sync_enabled_states()

    def sync_enabled_states(self):
        st = self.cog.get_shift_state(self.guild_id).get(self.timer_id, {})
        active = bool(st.get("active"))
        locked = bool(st.get("locked"))
        running = bool(st.get("running"))
        self.start_btn.disabled = not (active and (not locked) and (not running))
        self.stop_btn.disabled = not running

    @discord.ui.button(label="Start Shift", style=discord.ButtonStyle.success, custom_id="shift:placeholder:start")
    async def start_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_shift_start(interaction, self.guild_id, self.timer_id)

    @discord.ui.button(label="End Shift", style=discord.ButtonStyle.danger, custom_id="shift:placeholder:stop")
    async def stop_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_shift_stop(interaction, self.guild_id, self.timer_id)


class EscalationView(discord.ui.View):
    def __init__(self, cog: "ShiftMonitor", guild_id: int, shift_num: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild_id = guild_id
        self.shift_num = shift_num

        self.claim_btn.custom_id = f"esc:{guild_id}:{shift_num}:claim"
        self.stop_btn.custom_id = f"esc:{guild_id}:{shift_num}:stop"
        self.sync_enabled_states()

    def sync_enabled_states(self):
        esc = self.cog.get_escalation_state(self.guild_id).get(str(self.shift_num), {})
        owner_id = esc.get("owner_id")
        self.claim_btn.disabled = owner_id is not None
        self.stop_btn.disabled = owner_id is None

    @discord.ui.button(label="Claim Shift", style=discord.ButtonStyle.success, custom_id="esc:placeholder:claim")
    async def claim_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_escalation_claim(interaction, self.guild_id, self.shift_num)

    @discord.ui.button(label="Stop Shift", style=discord.ButtonStyle.danger, custom_id="esc:placeholder:stop")
    async def stop_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_escalation_stop(interaction, self.guild_id, self.shift_num)


# ----------------- Cog -----------------
class ShiftMonitor(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

        # GLOBAL persisted state keyed by guild_id (as string)
        self.state: Dict[str, Any] = {"guilds": {}}

        self._lock = asyncio.Lock()
        self._init_task: Optional[asyncio.Task] = None

        # global edit limiter
        self._edit_lock = asyncio.Lock()
        self._last_edit_at = 0.0

        self.load_state()

    # ----------------- Per-guild state helpers -----------------
    def _ensure_guild_state(self, guild_id: int) -> Dict[str, Any]:
        gkey = str(guild_id)
        guilds = self.state.setdefault("guilds", {})
        gs = guilds.get(gkey)
        if not isinstance(gs, dict):
            gs = {}
            guilds[gkey] = gs

        gs.setdefault("shift_state", {})
        gs.setdefault("escalation_state", {})
        gs.setdefault("invite_cache", {})
        gs.setdefault("invite_registry", {})
        gs.setdefault("checkpoints", dict(CHECKPOINTS_DEFAULT))
        gs.setdefault("escalation_role_id", None)
        gs.setdefault("log_channel_id", None)
        return gs

    def get_shift_state(self, guild_id: int) -> Dict[str, Dict[str, Any]]:
        gs = self._ensure_guild_state(guild_id)
        ss = gs.get("shift_state")
        if not isinstance(ss, dict):
            ss = {}
            gs["shift_state"] = ss
        return ss  # type: ignore

    def get_escalation_state(self, guild_id: int) -> Dict[str, Dict[str, Any]]:
        gs = self._ensure_guild_state(guild_id)
        es = gs.get("escalation_state")
        if not isinstance(es, dict):
            es = {}
            gs["escalation_state"] = es
        return es  # type: ignore

    def get_checkpoints(self, guild_id: int) -> Dict[int, str]:
        gs = self._ensure_guild_state(guild_id)
        cp = gs.get("checkpoints")
        if not isinstance(cp, dict):
            cp = dict(CHECKPOINTS_DEFAULT)
            gs["checkpoints"] = cp

        out: Dict[int, str] = {}
        for k, v in cp.items():
            ik = safe_int(k, None)
            if ik is None:
                continue
            out[ik] = str(v)

        if not out:
            out = dict(CHECKPOINTS_DEFAULT)

        gs["checkpoints"] = {str(k): v for k, v in out.items()}
        return out

    # ----------------- Persistence -----------------
    def load_state(self):
        if not STATE_FILE.exists():
            return
        try:
            data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict) and isinstance(data.get("guilds"), dict):
                self.state = data
        except Exception:
            self.state = {"guilds": {}}

    def save_state(self):
        atomic_json_write(STATE_FILE, self.state)

    # ----------------- Logging -----------------
    async def log(self, guild: discord.Guild, message: str):
        print(message, flush=True)

        gs = self._ensure_guild_state(guild.id)
        log_channel_id = gs.get("log_channel_id")

        ch = None
        if log_channel_id:
            ch = guild.get_channel(int(log_channel_id))

        if ch is None:
            ch = discord.utils.get(guild.text_channels, name=LOG_CHANNEL_NAME)
            if ch:
                gs["log_channel_id"] = ch.id
                self.save_state()

        if ch:
            try:
                await ch.send(message)
            except discord.HTTPException:
                pass

    # ----------------- Server Setup -----------------
    async def ensure_role(self, guild: discord.Guild) -> discord.Role:
        gs = self._ensure_guild_state(guild.id)
        role_id = gs.get("escalation_role_id")
        role = guild.get_role(int(role_id)) if role_id else None
        if role is None:
            role = discord.utils.get(guild.roles, name=ESCALATION_ROLE_NAME)
        if role is None:
            role = await guild.create_role(name=ESCALATION_ROLE_NAME, reason="ShiftMonitor auto-setup")
        gs["escalation_role_id"] = role.id
        self.save_state()
        return role

    async def ensure_category(self, guild: discord.Guild) -> discord.CategoryChannel:
        cat = discord.utils.get(guild.categories, name=SHIFT_CATEGORY_NAME)
        if cat is None:
            cat = await guild.create_category(name=SHIFT_CATEGORY_NAME, reason="ShiftMonitor auto-setup")
        return cat

    async def ensure_text_channel(
        self,
        guild: discord.Guild,
        name: str,
        category: Optional[discord.CategoryChannel],
    ) -> discord.TextChannel:
        ch = discord.utils.get(guild.text_channels, name=name)
        if ch is None:
            ch = await guild.create_text_channel(name=name, category=category, reason="ShiftMonitor auto-setup")
        return ch

    async def ensure_server_objects(self, guild: discord.Guild):
        cat = await self.ensure_category(guild)
        await self.ensure_role(guild)

        log_ch = discord.utils.get(guild.text_channels, name=LOG_CHANNEL_NAME)
        if log_ch is None:
            log_ch = await guild.create_text_channel(
                name=LOG_CHANNEL_NAME,
                category=cat,
                reason="ShiftMonitor auto-setup",
            )

        gs = self._ensure_guild_state(guild.id)
        gs["log_channel_id"] = log_ch.id

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

    async def _throttled_edit_view(self, msg: discord.Message, view: discord.ui.View) -> bool:
        try:
            async with self._edit_lock:
                now = asyncio.get_running_loop().time()
                wait_for = (self._last_edit_at + GLOBAL_EDIT_MIN_INTERVAL_SECONDS) - now
                if wait_for > 0:
                    await asyncio.sleep(wait_for)
                await msg.edit(view=view)
                self._last_edit_at = asyncio.get_running_loop().time()
            return True
        except Exception:
            return False

    async def ensure_shift_message(self, guild: discord.Guild, shift_num: int) -> None:
        timer_id = f"id_{shift_num}"
        channel = discord.utils.get(guild.text_channels, name=CHANNEL_NAMES[shift_num])
        if channel is None:
            return

        shift_state = self.get_shift_state(guild.id)

        st = shift_state.get(timer_id)
        if not st:
            st = {
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
                # ✅ NEW: last session audit (optional)
                "last_end_ts": None,
                "last_duration_s": None,
                "last_end_reason": None,
                "last_ended_by": None,
            }
            shift_state[timer_id] = st

        st["channel_id"] = channel.id

        msg = None
        if st.get("message_id"):
            msg = await self.fetch_message(guild, int(st["channel_id"]), int(st["message_id"]))

        created = False
        if msg is None:
            view = ShiftView(self, guild.id, timer_id, shift_num)
            msg = await channel.send(f"⏱️ **Shift {shift_num}**", view=view)
            st["message_id"] = msg.id
            st["last_render_key"] = None
            created = True

        # register persistent view (safe to call multiple times)
        self.bot.add_view(ShiftView(self, guild.id, timer_id, shift_num))
        self.save_state()

        # IMPORTANT FIX:
        # - If we did NOT create the message, do NOT edit it on startup.
        if created:
            await self.render_shift_if_needed(guild, timer_id)

    async def render_shift_if_needed(self, guild: discord.Guild, timer_id: str) -> None:
        shift_state = self.get_shift_state(guild.id)
        st = shift_state.get(timer_id)
        if not st:
            return

        render_key = f"a={int(bool(st.get('active')))}|r={int(bool(st.get('running')))}|l={int(bool(st.get('locked')))}|o={st.get('owner_id')}"
        if st.get("last_render_key") == render_key:
            return

        msg = await self.fetch_message(guild, int(st["channel_id"]), int(st["message_id"]))
        if msg is None:
            return

        view = ShiftView(self, guild.id, timer_id, int(st["shift_num"]))

        if st.get("last_render_key") is None and msg.components:
            st["last_render_key"] = render_key
            self.save_state()
            return

        ok = await self._throttled_edit_view(msg, view)
        if ok:
            st["last_render_key"] = render_key
            self.save_state()

    # ----------------- ✅ Shift session ending (manual + rollover) -----------------
    def _end_shift_session_unsafe(
        self,
        st: Dict[str, Any],
        *,
        reason: str,
        ended_by: Optional[int],
        end_ts: int,
    ) -> int:
        """
        Must be called under self._lock.
        Returns duration seconds (0 if unknown).
        """
        start_ts = st.get("start_ts")
        dur_s = 0
        if start_ts:
            try:
                dur_s = max(0, int(end_ts) - int(start_ts))
            except Exception:
                dur_s = 0

        st["running"] = False
        st["started_by"] = None
        st["start_ts"] = None
        st["escalated"] = False  # optional reset on end
        st["last_render_key"] = None

        # audit trail
        st["last_end_ts"] = int(end_ts)
        st["last_duration_s"] = int(dur_s)
        st["last_end_reason"] = str(reason)
        st["last_ended_by"] = int(ended_by) if ended_by is not None else None

        return int(dur_s)

    # ----------------- Escalation -----------------
    async def escalate_shift(self, guild: discord.Guild, shift_num: int, reason: str):
        escalation_state = self.get_escalation_state(guild.id)
        esc_key = str(shift_num)
        if esc_key in escalation_state:
            return

        timer_id = f"id_{shift_num}"
        shift_state = self.get_shift_state(guild.id)
        st = shift_state.get(timer_id)
        if not st:
            return

        st["locked"] = True
        st["last_render_key"] = None
        await self.render_shift_if_needed(guild, timer_id)

        role = await self.ensure_role(guild)
        ch = discord.utils.get(guild.text_channels, name=CHANNEL_NAMES["escalation"])
        if not ch:
            return

        checkpoints = self.get_checkpoints(guild.id)
        remaining = time_until_next_checkpoint(checkpoints)

        content = (
            f"{role.mention} **Shift {shift_num} has unclaimed time ({fmt_td(remaining)} remaining). Claim it?**\n"
            f"Reason: {reason}"
        )
        msg = await ch.send(content, view=EscalationView(self, guild.id, shift_num))

        escalation_state[esc_key] = {
            "shift_num": shift_num,
            "channel_id": ch.id,
            "message_id": msg.id,
            "owner_id": None,
            "start_ts": int(now_utc().timestamp()),
            "last_render_key": None,
        }

        self.bot.add_view(EscalationView(self, guild.id, shift_num))
        self.save_state()
        await self.log(guild, f"⚠️ Escalation started for Shift {shift_num} ({reason}) (<t:{int(datetime.now().timestamp())}:f>)")

    async def end_escalation(self, guild: discord.Guild, shift_num: int, reason: str):
        escalation_state = self.get_escalation_state(guild.id)
        esc_key = str(shift_num)
        if esc_key not in escalation_state:
            return

        timer_id = f"id_{shift_num}"
        shift_state = self.get_shift_state(guild.id)
        st = shift_state.get(timer_id)
        if st:
            st["locked"] = False
            st["last_render_key"] = None
            await self.render_shift_if_needed(guild, timer_id)

        escalation_state.pop(esc_key, None)
        self.save_state()
        await self.log(guild, f"🟥 Escalation ended for Shift {shift_num} ({reason}) (<t:{int(datetime.now().timestamp())}:f>)")

    # ----------------- Interaction handlers -----------------
    async def handle_shift_start(self, interaction: discord.Interaction, guild_id: int, timer_id: str):
        if not interaction.guild or interaction.guild.id != guild_id:
            return

        async with self._lock:
            shift_state = self.get_shift_state(guild_id)
            st = shift_state.get(timer_id)
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

            owner_id = st.get("owner_id")
            if owner_id and interaction.user.id not in (int(owner_id), interaction.guild.owner_id):
                await interaction.response.send_message("❌ You are not assigned to this shift.", ephemeral=True)
                return

            st["running"] = True
            st["started_by"] = interaction.user.id
            st["start_ts"] = int(now_utc().timestamp())
            st["last_render_key"] = None
            self.save_state()

        await self.render_shift_if_needed(interaction.guild, timer_id)
        await interaction.response.send_message(f"🟢 Shift {st['shift_num']} started.", ephemeral=True)
        await self.log(interaction.guild, f"🟢 Shift {st['shift_num']} started by {interaction.user.display_name} (<t:{int(datetime.now().timestamp())}:f>)")

    async def handle_shift_stop(self, interaction: discord.Interaction, guild_id: int, timer_id: str):
        if not interaction.guild or interaction.guild.id != guild_id:
            return

        end_ts = int(now_utc().timestamp())
        duration_s = 0
        shift_num = None

        async with self._lock:
            shift_state = self.get_shift_state(guild_id)
            st = shift_state.get(timer_id)
            if not st:
                await interaction.response.send_message("❌ Shift state missing.", ephemeral=True)
                return

            shift_num = st.get("shift_num")
            started_by = st.get("started_by")
            if interaction.user.id not in (int(started_by) if started_by else -1, interaction.guild.owner_id):
                await interaction.response.send_message("❌ You did not start this shift.", ephemeral=True)
                return

            if not st.get("running"):
                await interaction.response.send_message("❌ Shift is not running.", ephemeral=True)
                return

            duration_s = self._end_shift_session_unsafe(
                st,
                reason="Manual clock out",
                ended_by=interaction.user.id,
                end_ts=end_ts,
            )
            self.save_state()

        await self.render_shift_if_needed(interaction.guild, timer_id)

        pretty = fmt_td(timedelta(seconds=duration_s))
        await interaction.response.send_message(f"🔴 Shift ended. **Duration:** {pretty}", ephemeral=True)

        # log after interaction response (safe)
        try:
            await self.log(
                interaction.guild,
                f"🔴 Shift {shift_num} ended by {interaction.user.display_name} | Duration: {pretty} (<t:{end_ts}:f>)",
            )
        except Exception:
            pass

    async def handle_escalation_claim(self, interaction: discord.Interaction, guild_id: int, shift_num: int):
        if not interaction.guild or interaction.guild.id != guild_id:
            return

        async with self._lock:
            escalation_state = self.get_escalation_state(guild_id)
            esc = escalation_state.get(str(shift_num))
            if not esc:
                await interaction.response.send_message("❌ No active escalation for that shift.", ephemeral=True)
                return
            if esc.get("owner_id") is not None:
                await interaction.response.send_message("Shift already claimed.", ephemeral=True)
                return

            esc["owner_id"] = interaction.user.id
            esc["last_render_key"] = None
            self.save_state()

        await interaction.response.send_message("✅ You claimed this escalation shift.", ephemeral=True)
        await self.log(interaction.guild, f"✅ Escalation shift {shift_num} claimed by {interaction.user.display_name} (<t:{int(datetime.now().timestamp())}:f>)")

    async def handle_escalation_stop(self, interaction: discord.Interaction, guild_id: int, shift_num: int):
        if not interaction.guild or interaction.guild.id != guild_id:
            return

        async with self._lock:
            escalation_state = self.get_escalation_state(guild_id)
            esc = escalation_state.get(str(shift_num))
            if not esc:
                await interaction.response.send_message("❌ No active escalation for that shift.", ephemeral=True)
                return
            if interaction.user.id != esc.get("owner_id"):
                await interaction.response.send_message("Only the claimer can stop this shift.", ephemeral=True)
                return

        await self.end_escalation(interaction.guild, shift_num, "Manual stop by claimer")
        await interaction.response.send_message("🟥 Escalation shift ended.", ephemeral=True)

    # ----------------- Checkpoint loop (GLOBAL) -----------------
    @tasks.loop(seconds=10)
    async def checkpoint_loop(self):
        for guild in list(self.bot.guilds):
            checkpoints = self.get_checkpoints(guild.id)
            current_cp = get_current_checkpoint(checkpoints)
            now_ts = int(now_utc().timestamp())

            shift_state = self.get_shift_state(guild.id)
            escalation_state = self.get_escalation_state(guild.id)

            to_escalate: list[int] = []
            to_render: set[str] = set()
            to_log: list[str] = []

            async with self._lock:
                for timer_id, st in shift_state.items():
                    sn = st.get("shift_num")
                    if sn not in (1, 2, 3, 4):
                        continue

                    locked = bool(st.get("locked"))
                    should_be_active = (sn == current_cp) and (not locked)

                    if should_be_active and not st.get("active"):
                        st["active"] = True
                        st["activated_ts"] = now_ts
                        st["escalated"] = False
                        st["last_render_key"] = None
                        to_render.add(timer_id)

                    elif (not should_be_active) and st.get("active"):
                        # ✅ NEW: Auto clock-out on rollover if user forgot to end
                        if st.get("running"):
                            duration_s = self._end_shift_session_unsafe(
                                st,
                                reason="Auto clock out on shift rollover",
                                ended_by=None,
                                end_ts=now_ts,
                            )
                            pretty = fmt_td(timedelta(seconds=duration_s))
                            to_log.append(
                                f"⏱️✅ **Auto clock-out**: Shift {sn} rolled over while running | Duration: {pretty} (<t:{now_ts}:f>)"
                            )

                        st["active"] = False
                        st["activated_ts"] = None
                        st["escalated"] = False
                        # (running/start info already cleared above if it was running)
                        st["running"] = False
                        st["started_by"] = None
                        st["start_ts"] = None
                        st["last_render_key"] = None
                        to_render.add(timer_id)

                    if (
                        st.get("active")
                        and not st.get("running")
                        and not st.get("escalated")
                        and st.get("activated_ts") is not None
                        and (now_ts - int(st["activated_ts"])) >= int(ESCALATION_TIMEOUT.total_seconds())
                    ):
                        st["escalated"] = True
                        st["last_render_key"] = None
                        to_render.add(timer_id)
                        to_escalate.append(int(sn))

                self.save_state()

            for timer_id in to_render:
                await self.render_shift_if_needed(guild, timer_id)

            # log rollover auto clock-outs (outside lock)
            for msg in to_log:
                await self.log(guild, msg)

            for sn in to_escalate:
                if str(sn) not in escalation_state:
                    await self.escalate_shift(guild, sn, f"No one started within {fmt_td(ESCALATION_TIMEOUT)}")

    @checkpoint_loop.before_loop
    async def before_checkpoint_loop(self):
        await self.bot.wait_until_ready()

    # ----------------- Slash Commands (GLOBAL) -----------------
    @app_commands.command(name="setup_shifts", description="Create missing channels/role and initialize shift messages.")
    async def setup_shifts_cmd(self, interaction: discord.Interaction):
        if not interaction.guild:
            return
        if not has_owner_privs(interaction):
            await interaction.response.send_message(
                f"Only the **server owner** or **{OWNER_ROLE_NAME}** can use this.",
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)

        await self.ensure_server_objects(interaction.guild)
        for i in range(1, 5):
            await self.ensure_shift_message(interaction.guild, i)

        if not self.checkpoint_loop.is_running():
            self.checkpoint_loop.start()

        await interaction.followup.send("✅ Setup complete.", ephemeral=True)

    @app_commands.command(name="setowner", description="Assign a user to a shift.")
    async def setowner_cmd(self, interaction: discord.Interaction, shift_number: int, user: discord.Member):
        if not interaction.guild:
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
            shift_state = self.get_shift_state(interaction.guild.id)
            if timer_id not in shift_state:
                await interaction.response.send_message("Shift not initialized yet. Run /setup_shifts first.", ephemeral=True)
                return
            shift_state[timer_id]["owner_id"] = user.id
            shift_state[timer_id]["last_render_key"] = None
            self.save_state()

        await self.render_shift_if_needed(interaction.guild, timer_id)
        await interaction.response.send_message(f"✅ Owner for Shift {shift_number} set to {user.mention}.", ephemeral=True)

    # ----------------- Lifecycle -----------------
    async def cog_load(self):
        if self._init_task is None or self._init_task.done():
            self._init_task = self.bot.loop.create_task(self._post_ready_init())
        print("[shift_monitor] cog_load: scheduled post-ready init", flush=True)

    async def _post_ready_init(self):
        await self.bot.wait_until_ready()
        try:
            guild_ids = list((self.state.get("guilds") or {}).keys())
            for gkey in guild_ids:
                gid = safe_int(gkey, None)
                if gid is None:
                    continue
                guild = self.bot.get_guild(gid)
                if not guild:
                    continue

                await self.ensure_server_objects(guild)
                for i in range(1, 5):
                    await self.ensure_shift_message(guild, i)

                esc = self.get_escalation_state(guild.id)
                for sn_str in list(esc.keys()):
                    sn = safe_int(sn_str, None)
                    if sn is None:
                        continue
                    self.bot.add_view(EscalationView(self, guild.id, sn))

            if not self.checkpoint_loop.is_running():
                self.checkpoint_loop.start()

            print("[shift_monitor] post-ready init complete", flush=True)
        except Exception as e:
            print(f"[shift_monitor] post-ready init failed: {type(e).__name__}: {e}", flush=True)
            import traceback as _tb
            _tb.print_exception(type(e), e, e.__traceback__)

    async def cog_unload(self):
        if self.checkpoint_loop.is_running():
            self.checkpoint_loop.cancel()
        if self._init_task and not self._init_task.done():
            self._init_task.cancel()
        self.save_state()

    # ----------------- Interaction Router (GLOBAL persistent buttons) -----------------
    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        try:
            data = interaction.data or {}
            custom_id = data.get("custom_id")
            if not custom_id or not isinstance(custom_id, str):
                return

            # shift:<guild_id>:<timer_id>:start|stop
            if custom_id.startswith("shift:"):
                parts = custom_id.split(":")
                if len(parts) != 4:
                    return
                _, gid_str, timer_id, action = parts
                gid = safe_int(gid_str, None)
                if gid is None:
                    return
                if action == "start":
                    await self.handle_shift_start(interaction, gid, timer_id)
                elif action == "stop":
                    await self.handle_shift_stop(interaction, gid, timer_id)
                return

            # esc:<guild_id>:<shift_num>:claim|stop
            if custom_id.startswith("esc:"):
                parts = custom_id.split(":")
                if len(parts) != 4:
                    return
                _, gid_str, sn_str, action = parts
                gid = safe_int(gid_str, None)
                sn = safe_int(sn_str, None)
                if gid is None or sn is None:
                    return
                if action == "claim":
                    await self.handle_escalation_claim(interaction, gid, sn)
                elif action == "stop":
                    await self.handle_escalation_stop(interaction, gid, sn)
                return
        except Exception:
            return


async def setup(bot: commands.Bot):
    # idempotent setup (safe if your loader double-loads)
    if bot.get_cog("ShiftMonitor") is not None:
        return
    await bot.add_cog(ShiftMonitor(bot))
