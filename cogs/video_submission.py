import os
import json
import re
import hashlib
import datetime
import isodate
import asyncio
import base64
from pathlib import Path
from zoneinfo import ZoneInfo

import discord
from discord.ext import commands, tasks
from discord import app_commands
from discord.ui import View

from googleapiclient.discovery import build
from google.oauth2 import service_account

VIDEO_FILE = Path("data/video_submissions.json")
AP_FILE = Path("data/ap_data.json")
AUDIT_FILE = Path("data/video_audit_log.json")
REPORT_STATE_FILE = Path("data/video_report_state.json")

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")
if not YOUTUBE_API_KEY:
    raise RuntimeError("YOUTUBE_API_KEY is not set in environment variables.")

# Service account JSON from env (Railway Variables)
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

file_lock = asyncio.Lock()

# =====================
# UTILITIES
# =====================

def _load_file(p: Path):
    try:
        if not p.exists():
            return {}
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _save_file(p: Path, d):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(d, indent=4), encoding="utf-8")

async def load(p: Path):
    async with file_lock:
        return _load_file(p)

async def save(p: Path, d):
    async with file_lock:
        _save_file(p, d)

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

def is_manager(member: discord.Member) -> bool:
    return any(r.name in (CEO_ROLE, DIRECTOR_ROLE) for r in member.roles)

def can_run_video_report(member: discord.Member) -> bool:
    return any(r.name in (CEO_ROLE, LYCAN_ROLE) for r in member.roles)

def corp_ceos(guild: discord.Guild):
    return [m for m in guild.members if any(r.name == CEO_ROLE for r in m.roles)]

def yt_id(url: str):
    m = re.search(r"(?:v=|youtu\.be/)([A-Za-z0-9_-]{11})", url)
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

def _parse_service_account_json(raw: str) -> dict:
    def _try_json(text: str) -> dict | None:
        try:
            return json.loads(text)
        except Exception:
            return None

    normalized = raw.strip().replace("\\n", "\n")
    parse_error = RuntimeError(
        "GOOGLE_SERVICE_ACCOUNT_JSON could not be parsed. Provide raw JSON text, a JSON file path, or base64-encoded JSON."
    )

    parsed = _try_json(normalized)
    if parsed is not None:
        return parsed

    maybe_path = Path(normalized)
    if maybe_path.is_file():
        file_text = maybe_path.read_text(encoding="utf-8").replace("\\n", "\n")
        parsed = _try_json(file_text)
        if parsed is not None:
            return parsed

    try:
        decoded_bytes = base64.b64decode(normalized, validate=True)
        decoded = decoded_bytes.decode("utf-8", errors="strict").replace("\\n", "\n")
    except Exception as e:
        raise parse_error from e

    parsed = _try_json(decoded)
    if parsed is not None:
        return parsed

    raise parse_error

# =====================
# MODAL: REPORT DATES
# =====================

class VideoLengthReportModal(discord.ui.Modal, title="Video Length Report"):
    date_from = discord.ui.TextInput(
        label="From date (YYYY-MM-DD)",
        placeholder="2025-12-02",
        required=True,
        max_length=10
    )
    date_to = discord.ui.TextInput(
        label="To date (YYYY-MM-DD)",
        placeholder="2025-12-15",
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

        rng = date_str_to_local_range(str(self.date_from).strip(), str(self.date_to).strip())
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
# APPROVAL VIEW
# =====================

class ApprovalView(View):
    def __init__(self, cog: "VideoSubmissionCog", video_key: str):
        super().__init__(timeout=None)
        self.cog = cog
        self.video_key = str(video_key)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return False
        if not is_manager(interaction.user):
            await safe_send(interaction, "❌ Only the CEO and Directors can approve/reject videos.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="✅ Approve", style=discord.ButtonStyle.green, custom_id="video:approve")
    async def approve_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.process_decision(interaction, self.video_key, approve=True)

    @discord.ui.button(label="❌ Reject", style=discord.ButtonStyle.red, custom_id="video:reject")
    async def reject_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.process_decision(interaction, self.video_key, approve=False)

# =====================
# COG
# =====================

class VideoSubmissionCog(commands.Cog, name="VideoSubmissionCog"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

        self.youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY, cache_discovery=False)

        if not SERVICE_ACCOUNT_JSON:
            raise RuntimeError(
                "GOOGLE_SERVICE_ACCOUNT_JSON is not set. Provide raw JSON text, a JSON file path, or base64-encoded JSON."
            )

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
                fields="id,name,mimeType,size,videoMediaMetadata,mediaInfo,shortcutDetails"
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
        minfo = f.get("mediaInfo") or {}

        ms = vmeta.get("durationMillis")
        if ms is None:
            ms = minfo.get("durationMillis")

        if ms is None:
            mime = f.get("mimeType")
            size = f.get("size")
            raise ValueError(
                f"No durationMillis returned. mimeType={mime}, size={size}. "
                "Likely still processing, not a Drive-video, or service account lacks access."
            )

        sec = int(ms) / 1000
        return float(sec), title

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

    @app_commands.command(name="submit_video", description="Submit a YouTube or Google Drive video for AP approval")
    @app_commands.describe(url="YouTube or Google Drive video URL")
    async def submit_video(self, interaction: discord.Interaction, url: str):
        await safe_defer(interaction, ephemeral=True)

        if not interaction.guild:
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
        except Exception as e:
            await safe_send(interaction, f"❌ Could not read video data. ({type(e).__name__}: {e})", ephemeral=True)
            return

        fp = fingerprint(platform, seconds, title)
        if fp in audits:
            await safe_send(interaction, "❌ This video (or a re-upload) was already submitted.", ephemeral=True)
            return

        ap_reward = int((seconds / 3600) * 1000)

        videos[str(key)] = {
            "url": url,
            "submitter": interaction.user.id,
            "duration": seconds,
            "title": title,
            "ap": ap_reward,
            "fingerprint": fp,
            "approved": None,
            "submitted_at": now_iso()
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

            await approval_ch.send(embed=embed, view=ApprovalView(self, str(key)))

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

        key = str(key)
        if key not in videos:
            await safe_send(interaction, "❌ Video not found.", ephemeral=False)
            return

        video = videos[key]
        if video.get("approved") is not None:
            await safe_send(interaction, "⚠️ Already processed.", ephemeral=False)
            return

        submitter_id = int(video["submitter"])
        submitter = interaction.guild.get_member(submitter_id)

        awarded_ap = 0
        ceo_bonus_each = 0
        ts = now_iso()

        if approve and submitter:
            awarded_ap = int(video.get("ap", 0))
            uid = str(submitter.id)

            ap_data.setdefault(uid, {"ap": 0})
            ap_data[uid]["ap"] = int(ap_data[uid].get("ap", 0)) + awarded_ap

            if any(r.name == SECURITY_ROLE for r in submitter.roles):
                ceo_bonus_each = int(awarded_ap * 0.10)
                for leader in corp_ceos(interaction.guild):
                    lid = str(leader.id)
                    ap_data.setdefault(lid, {"ap": 0})
                    ap_data[lid]["ap"] = int(ap_data[lid].get("ap", 0)) + ceo_bonus_each

            await save(AP_FILE, ap_data)

            await post_points_distribution_confirmation(
                interaction.guild,
                submitter=submitter,
                submitter_id=submitter_id,
                title=str(video.get("title", "Untitled")),
                url=str(video.get("url", "")),
                seconds=float(video.get("duration", 0)),
                awarded_ap=int(awarded_ap),
                decided_by=interaction.user,
                ceo_bonus_each=int(ceo_bonus_each),
                ts_iso=ts
            )

        video["approved"] = bool(approve)
        audits[video["fingerprint"]] = {
            "video_key": key,
            "approved": bool(approve),
            "ap": int(video.get("ap", 0)) if approve else 0,
            "decided_by": interaction.user.id,
            "timestamp": ts
        }

        await save(VIDEO_FILE, videos)
        await save(AUDIT_FILE, audits)

        try:
            if interaction.message and interaction.message.embeds:
                base = interaction.message.embeds[0]
                updated = decision_embed(base, approved=bool(approve), decided_by=interaction.user, ts_iso=ts)
                await interaction.message.edit(embed=updated, view=disable_view(ApprovalView(self, key)))
        except Exception:
            pass

        status = "✅ Approved" if approve else "❌ Rejected"
        who = interaction.user.mention
        sub = submitter.mention if submitter else f"<@{submitter_id}>"
        title = video.get("title", "Untitled")

        extra = ""
        if approve:
            extra = f" — Awarded **+{int(video.get('ap', 0))} AP**"
            if ceo_bonus_each > 0:
                extra += f" (CEO bonus: **+{ceo_bonus_each} AP** each)"

        await safe_send(
            interaction,
            f"{status} by {who} — {sub} — **{title}**{extra}",
            ephemeral=False
        )

async def setup(bot: commands.Bot):
    # HARD STOP: remove any old copies before adding (works even if something loads twice)
    try:
        if bot.get_cog("VideoSubmission"):
            bot.remove_cog("VideoSubmission")
        if bot.get_cog("VideoSubmissionCog"):
            bot.remove_cog("VideoSubmissionCog")
    except Exception:
        pass

    await bot.add_cog(VideoSubmissionCog(bot))
