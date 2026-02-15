# cogs/video_submission.py
#
# Video Submission + Approval (Railway /data persistent, restart-safe)
#
# FIXES INCLUDED:
# - Legacy button compatibility (old approvals after restarts/format changes)
# - Admin tools: list/repost/force-decide
# - Repair tool: /video_set_submitter key @member  (fixes <@0>)
# - NEW: /video_recalc key  (re-fetches duration/title and recalculates AP)
# - NEW: /video_recalc_all_pending  (batch fix backlog AP=0)
# - SAFETY: approval refuses if submitter missing OR video still has 0 duration/ap
#
# Requires:
# - YOUTUBE_API_KEY
# - GOOGLE_SERVICE_ACCOUNT_JSON (raw JSON or base64)
#
# discord.py 2.x

import os
import json
import re
import hashlib
import datetime
import isodate
import asyncio
import base64
import inspect
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Optional, Tuple

import discord
from discord.ext import commands, tasks
from discord import app_commands
from discord.ui import View

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account

print("VIDEO_SUBMISSION LOADED VERSION: 2026-02-15-FULLFIX+RECALC")

# =====================
# PERSISTENCE (Railway Volume)
# =====================
PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)

VIDEO_FILE = PERSIST_ROOT / "video_submissions.json"
AP_FILE = PERSIST_ROOT / "ap_data.json"
AUDIT_FILE = PERSIST_ROOT / "video_audit_log.json"
REPORT_STATE_FILE = PERSIST_ROOT / "video_report_state.json"

# =====================
# ENV / CONFIG
# =====================
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")
if not YOUTUBE_API_KEY:
    raise RuntimeError("YOUTUBE_API_KEY is not set in environment variables.")

SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

APPROVAL_CHANNEL = "video-submissions"

CEO_ROLE = "ARC Security Corporation Leader"
DIRECTOR_ROLE = "ARC Security Administration Council"
SECURITY_ROLE = "ARC Security"

# Report command / automation restrictions
LYCAN_ROLE = "Lycan King"
REPORT_DM_USER_ID = 559041382573015060

AP_DISTRIBUTION_LOG_CH = "member-join-logs-points-distribute"

LOCAL_TZ = ZoneInfo("America/Moncton")

# Single-process lock to serialize JSON read/write
file_lock = asyncio.Lock()


async def maybe_await(result):
    if inspect.isawaitable(result):
        return await result
    return result


async def safe_remove_cog(bot: commands.Bot, name: str) -> None:
    """discord.py variants differ: remove_cog may be sync or async."""
    try:
        if bot.get_cog(name) is None:
            return
        await maybe_await(bot.remove_cog(name))
    except Exception:
        pass


# =====================
# ATOMIC JSON IO
# =====================

def _load_file(p: Path):
    try:
        if not p.exists():
            return {}
        raw = p.read_text(encoding="utf-8").strip()
        if not raw:
            return {}
        return json.loads(raw)
    except Exception:
        return {}


def _atomic_write_json(p: Path, d) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(d, indent=4), encoding="utf-8")
    tmp.replace(p)


async def load(p: Path):
    async with file_lock:
        return _load_file(p)


async def save(p: Path, d):
    async with file_lock:
        _atomic_write_json(p, d)


def now_iso():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def iso_to_dt_utc(iso_str: str) -> datetime.datetime | None:
    try:
        dt = datetime.datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)
    except Exception:
        return None


def iso_to_discord_ts(iso_str: str) -> str:
    try:
        dt = datetime.datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return f"<t:{int(dt.timestamp())}:f>"
    except Exception:
        return iso_str


def unix_to_discord_ts(unix_s: int) -> str:
    try:
        return f"<t:{int(unix_s)}:f>"
    except Exception:
        return str(unix_s)


# =====================
# PERMISSIONS / ROLES
# =====================

def is_manager(member: discord.Member) -> bool:
    return any(r.name in (CEO_ROLE, DIRECTOR_ROLE) for r in member.roles)


def can_run_video_report(member: discord.Member) -> bool:
    return any(r.name in (CEO_ROLE, LYCAN_ROLE) for r in member.roles)


def corp_ceos(guild: discord.Guild):
    return [m for m in guild.members if any(r.name == CEO_ROLE for r in m.roles)]


# =====================
# URL PARSERS
# =====================

def yt_id(url: str):
    # Supports watch?v=, youtu.be/, /shorts/, /embed/, /live/
    m = re.search(r"(?:v=|youtu\.be/|/shorts/|/embed/|/live/)([A-Za-z0-9_-]{11})", url)
    return m.group(1) if m else None


def drive_id(url: str):
    patterns = [
        r"/file/d/([A-Za-z0-9_-]+)",
        r"/d/([A-Za-z0-9_-]+)",
        r"[?&]id=([A-Za-z0-9_-]+)",
        r"drive\.google\.com/open\?id=([A-Za-z0-9_-]+)",
        r"drive\.google\.com/uc\?id=([A-Za-z0-9_-]+)",
    ]
    for pat in patterns:
        m = re.search(pat, url)
        if m:
            return m.group(1)
    return None


def fingerprint(platform: str, duration: float, title: str = ""):
    base = f"{platform}:{round(duration)}:{title.lower().strip()}"
    return hashlib.sha256(base.encode()).hexdigest()


def calc_ap(seconds: float) -> int:
    # 1000 AP per hour
    try:
        return int((float(seconds) / 3600.0) * 1000)
    except Exception:
        return 0


# =====================
# DISCORD SAFETY HELPERS
# =====================

async def safe_defer(interaction: discord.Interaction, *, ephemeral: bool):
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=ephemeral, thinking=True)
    except (discord.NotFound, discord.HTTPException):
        pass


async def safe_send(interaction: discord.Interaction, content: str, *, ephemeral: bool):
    try:
        if not interaction.response.is_done():
            await interaction.response.send_message(content, ephemeral=ephemeral)
        else:
            await interaction.followup.send(content, ephemeral=ephemeral)
    except (discord.NotFound, discord.HTTPException):
        pass


async def ensure_text_channel(guild: discord.Guild, name: str) -> discord.TextChannel | None:
    ch = discord.utils.get(guild.text_channels, name=name)
    if ch:
        return ch
    try:
        return await guild.create_text_channel(name)
    except discord.Forbidden:
        return None


# =====================
# REPORT HELPERS
# =====================

def fmt_hms(total_seconds: float) -> str:
    s = int(round(total_seconds))
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h}h {m:02d}m {sec:02d}s"


def date_str_to_local_range(date_from: str, date_to: str) -> tuple[datetime.datetime, datetime.datetime] | None:
    try:
        df = datetime.date.fromisoformat(date_from)
        dt = datetime.date.fromisoformat(date_to)
    except Exception:
        return None
    if dt < df:
        return None

    start_local = datetime.datetime(df.year, df.month, df.day, 0, 0, 0, tzinfo=LOCAL_TZ)
    end_local = datetime.datetime(dt.year, dt.month, dt.day, 23, 59, 59, tzinfo=LOCAL_TZ)
    return (start_local.astimezone(datetime.timezone.utc), end_local.astimezone(datetime.timezone.utc))


async def build_video_length_report_embed(
    *,
    guild: discord.Guild | None,
    start_utc: datetime.datetime,
    end_utc: datetime.datetime,
    title_prefix: str
) -> discord.Embed:
    videos = await load(VIDEO_FILE)
    if not isinstance(videos, dict):
        videos = {}

    totals: dict[int, float] = {}
    counts: dict[int, int] = {}

    for _key, v in videos.items():
        if not isinstance(v, dict):
            continue

        submitted_at = v.get("submitted_at")
        dt_utc = iso_to_dt_utc(submitted_at) if isinstance(submitted_at, str) else None
        if not dt_utc:
            continue

        if dt_utc < start_utc or dt_utc > end_utc:
            continue

        sid = int(v.get("submitter", 0) or 0)
        dur = float(v.get("duration", 0) or 0)
        if sid <= 0 or dur <= 0:
            continue

        totals[sid] = totals.get(sid, 0.0) + dur
        counts[sid] = counts.get(sid, 0) + 1

    sorted_rows = sorted(totals.items(), key=lambda kv: kv[1], reverse=True)

    lines: list[str] = []
    for sid, total_sec in sorted_rows:
        member_name = f"<@{sid}>"
        if guild:
            m = guild.get_member(sid)
            if m:
                member_name = f"{m.display_name} ({m.mention})"
        lines.append(f"• **{member_name}** — **{fmt_hms(total_sec)}** ({counts.get(sid, 0)} videos)")

    start_local = start_utc.astimezone(LOCAL_TZ)
    end_local = end_utc.astimezone(LOCAL_TZ)
    range_label = f"{start_local.date().isoformat()} to {end_local.date().isoformat()} ({LOCAL_TZ.key})"

    e = discord.Embed(
        title=f"{title_prefix} — Video Length Report",
        description=f"**Range:** {range_label}\n\n" + ("\n".join(lines) if lines else "_No submissions found in this date range._"),
        timestamp=datetime.datetime.utcnow()
    )
    e.set_footer(text="Totals are based on submitted_at timestamps recorded at submission time.")
    return e


async def post_points_distribution_confirmation(
    guild: discord.Guild,
    *,
    submitter: discord.Member | None,
    submitter_id: int,
    title: str,
    url: str,
    seconds: float,
    awarded_ap: int,
    decided_by: discord.Member,
    ceo_bonus_each: int,
    ts_iso: str
) -> None:
    ch = await ensure_text_channel(guild, AP_DISTRIBUTION_LOG_CH)
    if not ch:
        return

    recipient_mention = submitter.mention if submitter else f"<@{submitter_id}>"

    e = discord.Embed(
        title="Point Distribution Confirmation",
        description="Video submission approved and AP distributed.",
        timestamp=datetime.datetime.utcnow()
    )
    e.add_field(name="Recipient", value=f"{recipient_mention} (`{submitter_id}`)", inline=False)
    e.add_field(name="Awarded AP", value=f"**+{awarded_ap} AP**", inline=True)
    e.add_field(name="Rate", value="1000 AP / hour", inline=True)
    e.add_field(name="Duration", value=f"{round(seconds / 3600, 2)} hours", inline=True)
    e.add_field(name="Video Title", value=title[:256], inline=False)
    e.add_field(name="URL", value=url, inline=False)

    if ceo_bonus_each > 0:
        e.add_field(name="CEO Bonus", value=f"**+{ceo_bonus_each} AP** to each CEO", inline=False)

    e.add_field(name="Approved By", value=f"{decided_by.mention} (`{decided_by.id}`)", inline=False)
    e.add_field(name="Approved At", value=iso_to_discord_ts(ts_iso), inline=False)

    try:
        await ch.send(embed=e)
    except (discord.Forbidden, discord.HTTPException):
        pass


# =====================
# SERVICE ACCOUNT PARSER (robust)
# =====================

def _parse_service_account_json(raw: str) -> dict:
    """
    Accepts:
      - Raw JSON object text
      - JSON wrapped in quotes
      - Double-encoded JSON
      - A file path to a JSON file
      - Base64-encoded JSON (whitespace/newlines allowed)
    """
    def _try_json(text: str):
        try:
            return json.loads(text)
        except Exception:
            return None

    if not raw or not raw.strip():
        raise RuntimeError(
            "GOOGLE_SERVICE_ACCOUNT_JSON is not set. Provide raw JSON text, a JSON file path, or base64-encoded JSON."
        )

    s = raw.strip()

    # 0) Base64 (common on Railway for long values)
    b64_candidate = re.sub(r"\s+", "", s)
    if re.fullmatch(r"[A-Za-z0-9+/=]+", b64_candidate or "") and len(b64_candidate) > 64:
        try:
            decoded = base64.b64decode(b64_candidate, validate=False).decode("utf-8", errors="strict")
            j = _try_json(decoded)
            if isinstance(j, dict):
                return j
            if isinstance(j, str):
                j2 = _try_json(j)
                if isinstance(j2, dict):
                    return j2
        except Exception:
            pass

    # 1) Direct JSON (or JSON string that contains JSON)
    j = _try_json(s)
    if isinstance(j, dict):
        return j
    if isinstance(j, str):
        j2 = _try_json(j)
        if isinstance(j2, dict):
            return j2

    # 2) Strip surrounding quotes
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s2 = s[1:-1].strip()
        j = _try_json(s2)
        if isinstance(j, dict):
            return j
        if isinstance(j, str):
            j2 = _try_json(j)
            if isinstance(j2, dict):
                return j2
        s = s2

    # 3) File path
    try:
        p = Path(s)
        if p.is_file():
            file_text = p.read_text(encoding="utf-8").strip()
            j = _try_json(file_text)
            if isinstance(j, dict):
                return j
            if isinstance(j, str):
                j2 = _try_json(j)
                if isinstance(j2, dict):
                    return j2
            s = file_text
    except Exception:
        pass

    snippet = s[:80].replace("\n", "\\n")
    raise RuntimeError(
        "GOOGLE_SERVICE_ACCOUNT_JSON could not be parsed. Provide raw JSON text, a JSON file path, or base64-encoded JSON. "
        f"(value starts with: {snippet!r})"
    )


# =====================
# MODAL: REPORT DATES
# =====================

class VideoLengthReportModal(discord.ui.Modal, title="Video Length Report"):
    date_from = discord.ui.TextInput(
        label="From date (YYYY-MM-DD)",
        placeholder="2026-02-02",
        required=True,
        max_length=10
    )
    date_to = discord.ui.TextInput(
        label="To date (YYYY-MM-DD)",
        placeholder="2026-02-15",
        required=True,
        max_length=10
    )

    def __init__(self, cog: "VideoSubmissionCog"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await safe_send(interaction, "❌ This must be used in a server.", ephemeral=True)
            return

        if not can_run_video_report(interaction.user):
            await safe_send(interaction, f"❌ Only **{CEO_ROLE}** and **{LYCAN_ROLE}** can run this report.", ephemeral=True)
            return

        rng = date_str_to_local_range(str(self.date_from.value).strip(), str(self.date_to.value).strip())
        if not rng:
            await safe_send(interaction, "❌ Invalid dates. Use YYYY-MM-DD, and ensure To ≥ From.", ephemeral=True)
            return

        start_utc, end_utc = rng
        embed = await build_video_length_report_embed(
            guild=interaction.guild,
            start_utc=start_utc,
            end_utc=end_utc,
            title_prefix="Manual"
        )

        await safe_send(interaction, "✅ Report generated:", ephemeral=True)
        try:
            await interaction.followup.send(embed=embed, ephemeral=True)
        except (discord.NotFound, discord.HTTPException):
            pass


# =====================
# APPROVAL VIEW (PERSISTENT + UNIQUE CUSTOM_IDS)
# =====================

def decision_embed(base: discord.Embed, *, approved: bool, decided_by: discord.Member, ts_iso: str) -> discord.Embed:
    e = discord.Embed(title=base.title, description=base.description)
    if base.footer and base.footer.text:
        e.set_footer(text=base.footer.text)

    for f in base.fields:
        e.add_field(name=f.name, value=f.value, inline=f.inline)

    status = "✅ Approved" if approved else "❌ Rejected"
    e.add_field(name="Status", value=status, inline=True)
    e.add_field(name="Decided By", value=decided_by.mention, inline=True)
    e.add_field(name="Decided At", value=iso_to_discord_ts(ts_iso), inline=False)
    return e


def disable_view(view: View) -> View:
    for item in view.children:
        try:
            item.disabled = True
        except Exception:
            pass
    return view


class ApprovalView(View):
    """Persistent approval view with UNIQUE custom_ids per video."""
    def __init__(self, cog: "VideoSubmissionCog", video_key: str):
        super().__init__(timeout=None)
        self.cog = cog
        self.video_key = str(video_key)

        approve_id = f"video:approve:{self.video_key}"
        reject_id = f"video:reject:{self.video_key}"

        btn_approve = discord.ui.Button(label="✅ Approve", style=discord.ButtonStyle.green, custom_id=approve_id)
        btn_reject = discord.ui.Button(label="❌ Reject", style=discord.ButtonStyle.red, custom_id=reject_id)

        async def _approve_cb(interaction: discord.Interaction):
            await self.cog.process_decision(interaction, self.video_key, approve=True)

        async def _reject_cb(interaction: discord.Interaction):
            await self.cog.process_decision(interaction, self.video_key, approve=False)

        btn_approve.callback = _approve_cb  # type: ignore
        btn_reject.callback = _reject_cb    # type: ignore

        self.add_item(btn_approve)
        self.add_item(btn_reject)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return False
        if not is_manager(interaction.user):
            await safe_send(interaction, "❌ Only the CEO and Directors can approve/reject videos.", ephemeral=True)
            return False
        return True


# =====================
# COG
# =====================

class VideoSubmissionCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

        # YouTube client
        self.youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY, cache_discovery=False)

        # Drive client from env JSON (raw or base64 supported)
        creds_info = _parse_service_account_json(SERVICE_ACCOUNT_JSON)
        creds = service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        self.drive = build("drive", "v3", credentials=creds, cache_discovery=False)

        self.video_report_scheduler.start()

    async def cog_load(self):
        await self._restore_pending_views()

    def cog_unload(self):
        try:
            self.video_report_scheduler.cancel()
        except Exception:
            pass

    async def _restore_pending_views(self):
        videos = await load(VIDEO_FILE)
        if not isinstance(videos, dict):
            return
        for key, v in videos.items():
            if not isinstance(v, dict):
                continue
            if v.get("approved") is None:
                try:
                    self.bot.add_view(ApprovalView(self, str(key)))
                except Exception:
                    pass

    # -----------------
    # LEGACY BUTTON FIX
    # -----------------

    def _extract_key_from_embed(self, embed: discord.Embed) -> Optional[str]:
        candidates = []
        for f in getattr(embed, "fields", []) or []:
            if isinstance(f.value, str) and "http" in f.value:
                candidates.append(f.value)
        if isinstance(embed.description, str) and "http" in embed.description:
            candidates.append(embed.description)

        for text in candidates:
            yid = yt_id(text)
            if yid:
                return yid
            did = drive_id(text)
            if did:
                return did
        return None

    def _parse_custom_id_for_key(self, custom_id: str, interaction: discord.Interaction) -> Tuple[Optional[str], Optional[bool]]:
        cid = str(custom_id or "")

        m = re.fullmatch(r"video:(approve|reject):(.+)", cid)
        if m:
            return m.group(2), (m.group(1) == "approve")

        m = re.fullmatch(r"(approve|reject)[:_](.+)", cid)
        if m:
            return m.group(2), (m.group(1) == "approve")

        m = re.fullmatch(r"video_(approve|reject)_(.+)", cid)
        if m:
            return m.group(2), (m.group(1) == "approve")

        m = re.fullmatch(r"(approve_video|reject_video)[:_](.+)", cid)
        if m:
            return m.group(2), (m.group(1).startswith("approve"))

        if cid in ("approve_video", "reject_video", "video_approve", "video_reject", "approve", "reject"):
            approve = cid in ("approve_video", "video_approve", "approve")
            try:
                if interaction.message and interaction.message.embeds:
                    key = self._extract_key_from_embed(interaction.message.embeds[0])
                    return key, approve
            except Exception:
                return None, None

        return None, None

    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        try:
            if interaction.type != discord.InteractionType.component:
                return
            if not interaction.data or not isinstance(interaction.data, dict):
                return

            custom_id = interaction.data.get("custom_id")
            if not custom_id:
                return

            key, approve = self._parse_custom_id_for_key(str(custom_id), interaction)
            if not key or approve is None:
                return

            await self.process_decision(interaction, str(key), approve=bool(approve))
        except Exception:
            return

    # --------
    # Platform API lookups
    # --------

    def youtube_data_blocking(self, vid: str):
        r = self.youtube.videos().list(part="contentDetails,snippet", id=vid).execute()
        if not r.get("items"):
            raise ValueError("YouTube video not found or not accessible.")
        item = r["items"][0]
        seconds = isodate.parse_duration(item["contentDetails"]["duration"]).total_seconds()
        title = item["snippet"]["title"]
        return float(seconds), str(title)

    def drive_duration_blocking(self, fid: str):
        def fetch(file_id: str):
            return self.drive.files().get(
                fileId=file_id,
                fields="id,name,mimeType,size,videoMediaMetadata,shortcutDetails"
            ).execute()

        f = fetch(fid)

        if f.get("mimeType") == "application/vnd.google-apps.shortcut":
            sd = f.get("shortcutDetails") or {}
            target_id = sd.get("targetId")
            if not target_id:
                raise ValueError("Drive link is a shortcut but has no targetId.")
            f = fetch(target_id)

        title = str(f.get("name", "Untitled"))
        vmeta = f.get("videoMediaMetadata") or {}
        ms = vmeta.get("durationMillis")

        if ms is None:
            mime = f.get("mimeType")
            size = f.get("size")
            raise ValueError(
                f"No durationMillis returned. mimeType={mime}, size={size}. "
                "Likely still processing, not a Drive-video, or service account lacks access."
            )

        sec = int(ms) / 1000
        return float(sec), title

    async def _recalc_entry(self, key: str, v: dict) -> Tuple[bool, str]:
        """
        Re-fetch duration/title from YouTube/Drive and recompute AP.
        Returns (changed?, message)
        """
        url = str(v.get("url", "") or "")
        key = str(key).strip()

        yid = yt_id(url) or (key if re.fullmatch(r"[A-Za-z0-9_-]{11}", key) else None)
        did = drive_id(url) or (key if not yid else None)

        if not yid and not did:
            return False, "unsupported url/key"

        try:
            if yid:
                seconds, title = await asyncio.to_thread(self.youtube_data_blocking, yid)
                platform = "youtube"
                real_key = yid
            else:
                seconds, title = await asyncio.to_thread(self.drive_duration_blocking, did)
                platform = "drive"
                real_key = did
        except Exception as e:
            return False, f"lookup failed: {type(e).__name__}: {e}"

        ap_reward = calc_ap(seconds)
        fp = fingerprint(platform, seconds, title)

        changed = False
        if float(v.get("duration", 0) or 0) != float(seconds):
            v["duration"] = float(seconds)
            changed = True
        if str(v.get("title", "") or "") != str(title):
            v["title"] = str(title)
            changed = True
        if int(v.get("ap", 0) or 0) != int(ap_reward):
            v["ap"] = int(ap_reward)
            changed = True
        if str(v.get("fingerprint", "") or "") != str(fp):
            v["fingerprint"] = str(fp)
            changed = True

        # If the stored dict key is wrong vs derived key, we won't rename automatically here
        # (to avoid breaking existing approvals). We'll just update fields.
        return changed, f"duration={round(seconds/3600,2)}h ap={ap_reward}"

    # =====================
    # MANUAL REPORT COMMAND
    # =====================

    @app_commands.command(
        name="video_length_report",
        description="Report total submitted video length per member for a date range (YYYY-MM-DD)."
    )
    async def video_length_report(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await safe_send(interaction, "❌ This command must be used in a server.", ephemeral=True)
            return

        if not can_run_video_report(interaction.user):
            await safe_send(interaction, f"❌ Only **{CEO_ROLE}** and **{LYCAN_ROLE}** can run this report.", ephemeral=True)
            return

        try:
            await interaction.response.send_modal(VideoLengthReportModal(self))
        except (discord.HTTPException, discord.NotFound):
            await safe_send(interaction, "❌ Could not open the report modal. Please try again.", ephemeral=True)

    # =====================
    # AUTO REPORT SCHEDULER
    # =====================

    @tasks.loop(minutes=15)
    async def video_report_scheduler(self):
        try:
            state = await load(REPORT_STATE_FILE)
            if not isinstance(state, dict):
                state = {}

            now_local = datetime.datetime.now(tz=LOCAL_TZ)
            day = now_local.day

            if day not in (2, 16):
                return

            run_key = f"{now_local.date().isoformat()}-day{day}"
            if state.get(run_key) is True:
                return

            if day == 16:
                y = now_local.year
                m = now_local.month
                date_from = datetime.date(y, m, 2)
                date_to = datetime.date(y, m, 15)
                title_prefix = "Auto (2nd–15th)"
            else:
                y = now_local.year
                m = now_local.month
                first_of_month = datetime.date(y, m, 1)
                prev_month_last = first_of_month - datetime.timedelta(days=1)
                date_from = datetime.date(prev_month_last.year, prev_month_last.month, 16)
                date_to = datetime.date(y, m, 1)
                title_prefix = "Auto (16th–1st)"

            start_local = datetime.datetime(date_from.year, date_from.month, date_from.day, 0, 0, 0, tzinfo=LOCAL_TZ)
            end_local = datetime.datetime(date_to.year, date_to.month, date_to.day, 23, 59, 59, tzinfo=LOCAL_TZ)
            start_utc = start_local.astimezone(datetime.timezone.utc)
            end_utc = end_local.astimezone(datetime.timezone.utc)

            embed = await build_video_length_report_embed(
                guild=None,
                start_utc=start_utc,
                end_utc=end_utc,
                title_prefix=title_prefix
            )

            user = await self.bot.fetch_user(REPORT_DM_USER_ID)
            if not user:
                return

            await user.send(embed=embed)

            state[run_key] = True
            await save(REPORT_STATE_FILE, state)

        except discord.Forbidden:
            return
        except Exception:
            return

    @video_report_scheduler.before_loop
    async def before_video_report_scheduler(self):
        await self.bot.wait_until_ready()

    # =====================
    # MANAGER TOOLS
    # =====================

    @app_commands.command(name="video_list_pending", description="List pending (unapproved) video submissions")
    async def video_list_pending(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await safe_send(interaction, "❌ Must be used in a server.", ephemeral=True)
            return
        if not is_manager(interaction.user):
            await safe_send(interaction, "❌ Only CEO/Directors can use this.", ephemeral=True)
            return

        videos = await load(VIDEO_FILE)
        if not isinstance(videos, dict) or not videos:
            await safe_send(interaction, "No submissions found.", ephemeral=True)
            return

        pending = []
        for k, v in videos.items():
            if isinstance(v, dict) and v.get("approved") is None:
                pending.append((str(k), v))

        if not pending:
            await safe_send(interaction, "✅ No pending submissions.", ephemeral=True)
            return

        lines = []
        for k, v in pending[:40]:
            title = str(v.get("title", "Untitled"))[:60]
            sub = int(v.get("submitter", 0) or 0)
            ap = int(v.get("ap", 0) or 0)

            submitted_at = v.get("submitted_at")
            if isinstance(submitted_at, str) and submitted_at.endswith("Z"):
                ts_label = iso_to_discord_ts(submitted_at)
            else:
                # your list output shows unix; support that too
                try:
                    ts_label = unix_to_discord_ts(int(submitted_at))
                except Exception:
                    ts_label = str(submitted_at or "")

            lines.append(f"• **{k}** — {title} — <@{sub}> — **{ap} AP** — {ts_label}")

        extra = ""
        if len(pending) > 40:
            extra = f"\n…and **{len(pending)-40}** more."

        await safe_send(interaction, "Pending submissions:\n" + "\n".join(lines) + extra, ephemeral=True)

    @app_commands.command(name="video_repost_pending", description="Repost pending approvals with fresh working buttons (auto-recalc AP if needed)")
    async def video_repost_pending(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await safe_send(interaction, "❌ Must be used in a server.", ephemeral=True)
            return
        if not is_manager(interaction.user):
            await safe_send(interaction, "❌ Only CEO/Directors can use this.", ephemeral=True)
            return

        approval_ch = discord.utils.get(interaction.guild.text_channels, name=APPROVAL_CHANNEL)
        if not approval_ch:
            await safe_send(interaction, f"❌ Could not find `#{APPROVAL_CHANNEL}`.", ephemeral=True)
            return

        videos = await load(VIDEO_FILE)
        if not isinstance(videos, dict) or not videos:
            await safe_send(interaction, "No submissions found.", ephemeral=True)
            return

        pending_keys = [k for k, v in videos.items() if isinstance(v, dict) and v.get("approved") is None]
        if not pending_keys:
            await safe_send(interaction, "✅ No pending submissions to repost.", ephemeral=True)
            return

        recalced = 0
        posted = 0

        # attempt recalc for broken entries before posting
        for k in pending_keys[:50]:
            v = videos.get(k)
            if not isinstance(v, dict):
                continue

            if int(v.get("ap", 0) or 0) <= 0 or float(v.get("duration", 0) or 0) <= 0:
                changed, _msg = await self._recalc_entry(str(k), v)
                if changed:
                    recalced += 1

        if recalced:
            await save(VIDEO_FILE, videos)

        for k in pending_keys[:50]:
            v = videos.get(k)
            if not isinstance(v, dict):
                continue

            title = str(v.get("title", "Untitled"))
            url = str(v.get("url", ""))
            seconds = float(v.get("duration", 0) or 0)
            ap_reward = int(v.get("ap", 0) or 0)
            submitter_id = int(v.get("submitter", 0) or 0)

            embed = discord.Embed(title="🎥 Video Approval Required (Reposted)", description=title)
            embed.add_field(name="Submitter", value=f"<@{submitter_id}>", inline=False)
            embed.add_field(name="Duration (hours)", value=round(seconds / 3600, 2), inline=True)
            embed.add_field(name="AP Reward", value=ap_reward, inline=True)
            embed.add_field(name="URL", value=url, inline=False)
            embed.set_footer(text=f"Video Key: {k}")

            try:
                try:
                    self.bot.add_view(ApprovalView(self, str(k)))
                except Exception:
                    pass

                await approval_ch.send(embed=embed, view=ApprovalView(self, str(k)))
                posted += 1
            except (discord.Forbidden, discord.HTTPException):
                continue

        await safe_send(
            interaction,
            f"✅ Reposted **{posted}** pending approval cards in #{APPROVAL_CHANNEL}. "
            f"(Auto-recalculated **{recalced}** entries with 0 AP/duration.)",
            ephemeral=True
        )

    @app_commands.command(name="video_force_decide", description="Force approve/reject a submission by key (no button click needed)")
    @app_commands.describe(key="Video key (YouTube ID or Drive ID)", decision="approve or reject")
    async def video_force_decide(self, interaction: discord.Interaction, key: str, decision: str):
        await safe_defer(interaction, ephemeral=True)

        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await safe_send(interaction, "❌ Must be used in a server.", ephemeral=True)
            return
        if not is_manager(interaction.user):
            await safe_send(interaction, "❌ Only CEO/Directors can use this.", ephemeral=True)
            return

        d = decision.strip().lower()
        if d not in ("approve", "reject"):
            await safe_send(interaction, "❌ decision must be `approve` or `reject`.", ephemeral=True)
            return

        await self.process_decision(interaction, str(key).strip(), approve=(d == "approve"))

    @app_commands.command(name="video_set_submitter", description="Repair a submission by setting the correct submitter (fixes <@0>)")
    @app_commands.describe(key="Video key (YouTube ID or Drive ID)", member="Discord member who should receive AP")
    async def video_set_submitter(self, interaction: discord.Interaction, key: str, member: discord.Member):
        await safe_defer(interaction, ephemeral=True)

        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await safe_send(interaction, "❌ Must be used in a server.", ephemeral=True)
            return
        if not is_manager(interaction.user):
            await safe_send(interaction, "❌ Only CEO/Directors can use this.", ephemeral=True)
            return

        key = str(key).strip()
        videos = await load(VIDEO_FILE)
        if not isinstance(videos, dict) or key not in videos or not isinstance(videos[key], dict):
            await safe_send(interaction, f"❌ Video not found for key `{key}`.", ephemeral=True)
            return

        videos[key]["submitter"] = int(member.id)
        try:
            videos[key]["submitter_had_security"] = any(r.name == SECURITY_ROLE for r in member.roles)
        except Exception:
            pass

        await save(VIDEO_FILE, videos)
        await safe_send(interaction, f"✅ Set submitter for `{key}` to {member.mention}.", ephemeral=True)

    @app_commands.command(name="video_recalc", description="Recalculate duration/title/AP for a submission by key (fixes 0 AP legacy rows)")
    @app_commands.describe(key="Video key (YouTube ID or Drive ID)")
    async def video_recalc(self, interaction: discord.Interaction, key: str):
        await safe_defer(interaction, ephemeral=True)

        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await safe_send(interaction, "❌ Must be used in a server.", ephemeral=True)
            return
        if not is_manager(interaction.user):
            await safe_send(interaction, "❌ Only CEO/Directors can use this.", ephemeral=True)
            return

        key = str(key).strip()
        videos = await load(VIDEO_FILE)
        if not isinstance(videos, dict) or key not in videos or not isinstance(videos[key], dict):
            await safe_send(interaction, f"❌ Video not found for key `{key}`.", ephemeral=True)
            return

        changed, msg = await self._recalc_entry(key, videos[key])
        if changed:
            await save(VIDEO_FILE, videos)
            await safe_send(interaction, f"✅ Recalculated `{key}`: {msg}", ephemeral=True)
        else:
            await safe_send(interaction, f"⚠️ No change for `{key}` ({msg}).", ephemeral=True)

    @app_commands.command(name="video_recalc_all_pending", description="Batch recalc duration/title/AP for pending submissions where AP/duration are 0")
    async def video_recalc_all_pending(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await safe_send(interaction, "❌ Must be used in a server.", ephemeral=True)
            return
        if not is_manager(interaction.user):
            await safe_send(interaction, "❌ Only CEO/Directors can use this.", ephemeral=True)
            return

        videos = await load(VIDEO_FILE)
        if not isinstance(videos, dict) or not videos:
            await safe_send(interaction, "No submissions found.", ephemeral=True)
            return

        changed_count = 0
        tried = 0
        failed = 0

        # cap to avoid hammering APIs too hard in one command; raise if needed
        keys = [k for k, v in videos.items() if isinstance(v, dict) and v.get("approved") is None]
        for k in keys[:75]:
            v = videos.get(k)
            if not isinstance(v, dict):
                continue

            if int(v.get("ap", 0) or 0) > 0 and float(v.get("duration", 0) or 0) > 0:
                continue

            tried += 1
            changed, msg = await self._recalc_entry(str(k), v)
            if changed:
                changed_count += 1
            else:
                # if lookup failed, msg will say so
                if "lookup failed" in msg:
                    failed += 1

        if changed_count:
            await save(VIDEO_FILE, videos)

        await safe_send(
            interaction,
            f"✅ Recalc complete. Tried **{tried}** pending entries; updated **{changed_count}**; lookup failures **{failed}**.",
            ephemeral=True
        )

    # =====================
    # SUBMISSION / APPROVAL FLOW
    # =====================

    @app_commands.command(name="submit_video", description="Submit a YouTube or Google Drive video for AP approval")
    @app_commands.describe(url="YouTube or Google Drive video URL")
    async def submit_video(self, interaction: discord.Interaction, url: str):
        await safe_defer(interaction, ephemeral=True)

        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await safe_send(interaction, "❌ This command must be used in a server.", ephemeral=True)
            return

        videos = await load(VIDEO_FILE)
        audits = await load(AUDIT_FILE)

        yid = yt_id(url)
        did = drive_id(url)

        if not yid and not did:
            await safe_send(interaction, "❌ Unsupported video URL.", ephemeral=True)
            return

        try:
            if yid:
                seconds, title = await asyncio.to_thread(self.youtube_data_blocking, yid)
                platform = "youtube"
                key = yid
            else:
                seconds, title = await asyncio.to_thread(self.drive_duration_blocking, did)
                platform = "drive"
                key = did
        except HttpError as e:
            await safe_send(interaction, f"❌ Could not read video data. (HttpError: {e})", ephemeral=True)
            return
        except Exception as e:
            await safe_send(interaction, f"❌ Could not read video data. ({type(e).__name__}: {e})", ephemeral=True)
            return

        fp = fingerprint(platform, seconds, title)
        if isinstance(audits, dict) and fp in audits:
            await safe_send(interaction, "❌ This video (or a re-upload) was already submitted.", ephemeral=True)
            return

        ap_reward = calc_ap(seconds)
        submitter_had_security = any(r.name == SECURITY_ROLE for r in interaction.user.roles)

        if not isinstance(videos, dict):
            videos = {}

        videos[str(key)] = {
            "url": url,
            "submitter": interaction.user.id,
            "duration": float(seconds),
            "title": str(title),
            "ap": int(ap_reward),
            "fingerprint": fp,
            "approved": None,
            "submitted_at": now_iso(),
            "submitter_had_security": bool(submitter_had_security),
        }

        await save(VIDEO_FILE, videos)

        try:
            self.bot.add_view(ApprovalView(self, str(key)))
        except Exception:
            pass

        approval_ch = discord.utils.get(interaction.guild.text_channels, name=APPROVAL_CHANNEL)
        if approval_ch:
            embed = discord.Embed(title="🎥 Video Approval Required", description=title)
            embed.add_field(name="Submitter", value=interaction.user.mention, inline=False)
            embed.add_field(name="Duration (hours)", value=round(seconds / 3600, 2), inline=True)
            embed.add_field(name="AP Reward", value=ap_reward, inline=True)
            embed.add_field(name="URL", value=url, inline=False)
            embed.set_footer(text=f"Video Key: {str(key)}")

            try:
                await approval_ch.send(embed=embed, view=ApprovalView(self, str(key)))
            except (discord.Forbidden, discord.HTTPException):
                pass
        else:
            await safe_send(
                interaction,
                f"✅ Video submitted, but I could not find the approval channel `#{APPROVAL_CHANNEL}`. Please notify staff.",
                ephemeral=True
            )
            return

        await safe_send(interaction, "✅ Video submitted for approval.", ephemeral=True)

    async def process_decision(self, interaction: discord.Interaction, key: str, approve: bool):
        await safe_defer(interaction, ephemeral=False)

        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        if not is_manager(interaction.user):
            await safe_send(interaction, "❌ Only the CEO and Directors can approve/reject videos.", ephemeral=True)
            return

        videos = await load(VIDEO_FILE)
        ap_data = await load(AP_FILE)
        audits = await load(AUDIT_FILE)

        if not isinstance(videos, dict):
            videos = {}
        if not isinstance(ap_data, dict):
            ap_data = {}
        if not isinstance(audits, dict):
            audits = {}

        key = str(key).strip()
        if key not in videos:
            await safe_send(interaction, f"❌ Video not found for key `{key}`.", ephemeral=False)
            return

        video = videos[key]
        if not isinstance(video, dict):
            await safe_send(interaction, "❌ Video record is corrupted (not a dict).", ephemeral=False)
            return

        if video.get("approved") is not None:
            await safe_send(interaction, "⚠️ Already processed.", ephemeral=False)
            return

        submitter_id = int(video.get("submitter", 0) or 0)
        if submitter_id <= 0:
            await safe_send(
                interaction,
                "❌ This submission has no valid submitter recorded (submitter=0). "
                f"Fix it with `/video_set_submitter key:{key} member:@User` then approve again.",
                ephemeral=False
            )
            return

        # SAFETY: if legacy row never had metadata, force recalc first
        if approve and (float(video.get("duration", 0) or 0) <= 0 or int(video.get("ap", 0) or 0) <= 0):
            await safe_send(
                interaction,
                f"❌ This submission still has **0 duration/AP**. Run `/video_recalc key:{key}` first, then approve.",
                ephemeral=False
            )
            return

        submitter = interaction.guild.get_member(submitter_id)
        if submitter is None:
            try:
                submitter = await interaction.guild.fetch_member(submitter_id)
            except Exception:
                submitter = None

        awarded_ap = 0
        ceo_bonus_each = 0
        ts = now_iso()

        if approve:
            awarded_ap = int(video.get("ap", 0) or 0)
            uid = str(submitter_id)

            ap_data.setdefault(uid, {"ap": 0})
            ap_data[uid]["ap"] = int(ap_data[uid].get("ap", 0) or 0) + awarded_ap

            had_security = video.get("submitter_had_security")
            if had_security is None and submitter:
                had_security = any(r.name == SECURITY_ROLE for r in submitter.roles)
            had_security = bool(had_security)

            if had_security:
                ceo_bonus_each = int(awarded_ap * 0.10)
                for leader in corp_ceos(interaction.guild):
                    lid = str(leader.id)
                    ap_data.setdefault(lid, {"ap": 0})
                    ap_data[lid]["ap"] = int(ap_data[lid].get("ap", 0) or 0) + ceo_bonus_each

            await save(AP_FILE, ap_data)

            await post_points_distribution_confirmation(
                interaction.guild,
                submitter=submitter,
                submitter_id=submitter_id,
                title=str(video.get("title", "Untitled")),
                url=str(video.get("url", "")),
                seconds=float(video.get("duration", 0) or 0),
                awarded_ap=int(awarded_ap),
                decided_by=interaction.user,
                ceo_bonus_each=int(ceo_bonus_each),
                ts_iso=ts
            )

        video["approved"] = bool(approve)
        audits[str(video.get("fingerprint", ""))] = {
            "video_key": key,
            "approved": bool(approve),
            "ap": int(video.get("ap", 0) or 0) if approve else 0,
            "decided_by": interaction.user.id,
            "timestamp": ts
        }

        await save(VIDEO_FILE, videos)
        await save(AUDIT_FILE, audits)

        try:
            if interaction.message and interaction.message.embeds:
                base = interaction.message.embeds[0]
                updated = decision_embed(base, approved=bool(approve), decided_by=interaction.user, ts_iso=ts)
                disabled = disable_view(ApprovalView(self, key))
                await interaction.message.edit(embed=updated, view=disabled)
        except Exception:
            pass

        status = "✅ Approved" if approve else "❌ Rejected"
        who = interaction.user.mention
        sub = submitter.mention if submitter else f"<@{submitter_id}>"
        title = str(video.get("title", "Untitled"))

        extra = ""
        if approve:
            extra = f" — Awarded **+{int(video.get('ap', 0) or 0)} AP**"
            if ceo_bonus_each > 0:
                extra += f" (CEO bonus: **+{ceo_bonus_each} AP** each)"

        await safe_send(
            interaction,
            f"{status} by {who} — {sub} — **{title}**{extra}",
            ephemeral=False
        )


async def setup(bot: commands.Bot):
    await safe_remove_cog(bot, "VideoSubmission")      # legacy name
    await safe_remove_cog(bot, "VideoSubmissionCog")   # current name
    await bot.add_cog(VideoSubmissionCog(bot))
