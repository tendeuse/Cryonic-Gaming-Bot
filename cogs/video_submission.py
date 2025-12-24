import discord
from discord.ext import commands
from discord import app_commands
from discord.ui import View
import json
import re
import hashlib
import datetime
import isodate
import asyncio
from pathlib import Path
from googleapiclient.discovery import build
from google.oauth2 import service_account

VIDEO_FILE = Path("data/video_submissions.json")
AP_FILE = Path("data/ap_data.json")
AUDIT_FILE = Path("data/video_audit_log.json")

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")
if not YOUTUBE_API_KEY:
    raise RuntimeError("YOUTUBE_API_KEY is not set in environment variables.")
GOOGLE_SERVICE_ACCOUNT_FILE = "service_account.json"
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

APPROVAL_CHANNEL = "video-submissions"

CEO_ROLE = "ARC Security Corporation Leader"
DIRECTOR_ROLE = "ARC Security Administration Council"
SECURITY_ROLE = "ARC Security"

AP_DISTRIBUTION_LOG_CH = "member-join-logs-points-distribute"

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

def corp_ceos(guild: discord.Guild):
    return [m for m in guild.members if any(r.name == CEO_ROLE for r in m.roles)]

def yt_id(url: str):
    m = re.search(r"(?:v=|youtu\.be/)([A-Za-z0-9_-]{11})", url)
    return m.group(1) if m else None

def drive_id(url: str):
    m = re.search(r"/d/([A-Za-z0-9_-]+)", url)
    if m:
        return m.group(1)
    m = re.search(r"[?&]id=([A-Za-z0-9_-]+)", url)
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
# APPROVAL VIEW (SAFE)
# =====================

class ApprovalView(View):
    def __init__(self, cog: "VideoSubmission", video_key: str):
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

    @discord.ui.button(
        label="✅ Approve",
        style=discord.ButtonStyle.green,
        custom_id="video:approve"
    )
    async def approve_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.process_decision(interaction, self.video_key, approve=True)

    @discord.ui.button(
        label="❌ Reject",
        style=discord.ButtonStyle.red,
        custom_id="video:reject"
    )
    async def reject_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.process_decision(interaction, self.video_key, approve=False)

# =====================
# COG
# =====================

class VideoSubmission(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

        # googleapiclient is blocking when calling .execute(), but you correctly run those in to_thread later.
        self.youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY, cache_discovery=False)
        creds = service_account.Credentials.from_service_account_file(
            GOOGLE_SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        self.drive = build("drive", "v3", credentials=creds, cache_discovery=False)

    async def cog_load(self):
        await self._restore_pending_views()

    async def _restore_pending_views(self):
        videos = await load(VIDEO_FILE)
        if not isinstance(videos, dict):
            return

        for key, v in videos.items():
            if not isinstance(v, dict):
                continue
            if v.get("approved") is None:
                try:
                    # Persistent view for each pending key
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
        f = self.drive.files().get(fileId=fid, fields="name,videoMediaMetadata").execute()
        vmeta = f.get("videoMediaMetadata") or {}
        ms = vmeta.get("durationMillis")
        if ms is None:
            raise ValueError("Drive file has no video duration metadata (not a video or not accessible).")
        sec = int(ms) / 1000
        return float(sec), str(f.get("name", "Untitled"))

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
            await safe_send(interaction, f"❌ Could not read video data. ({type(e).__name__})", ephemeral=True)
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
            "approved": None
        }

        await save(VIDEO_FILE, videos)

        # Register persistent view for this key
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
        # ACK immediately
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

        # mark decision + audit
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

        # Update original approval message
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
    await bot.add_cog(VideoSubmission(bot))
