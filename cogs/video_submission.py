# cogs/video_submission.py

import os
import json
import re
import hashlib
import datetime
import asyncio
import base64
from pathlib import Path
from typing import Optional, Tuple

import discord
from discord.ext import commands
from discord import app_commands

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

import isodate

# =====================
# PERSISTENCE (Railway)
# =====================
PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)

VIDEO_FILE = PERSIST_ROOT / "video_submissions.json"
AUDIT_FILE = PERSIST_ROOT / "video_audit.json"

# =====================
# ENV / CONFIG
# =====================
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "").strip()
YOUTUBE_ENABLED = bool(YOUTUBE_API_KEY)

SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

APPROVAL_CHANNEL = "video-submissions"

CEO_ROLE = "ARC Security Corporation Leader"
DIRECTOR_ROLE = "ARC Security Administration Council"


_file_lock = asyncio.Lock()


async def load(path: Path) -> dict:
    async with _file_lock:
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}


async def save(path: Path, data: dict) -> None:
    async with _file_lock:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, path)


def yt_id(url: str) -> Optional[str]:
    m = re.search(r"(?:v=|youtu\.be/)([A-Za-z0-9_-]{6,})", url.strip())
    return m.group(1) if m else None


def drive_id(url: str) -> Optional[str]:
    url = url.strip()
    m = re.search(r"/d/([A-Za-z0-9_-]{10,})", url)
    if m:
        return m.group(1)
    m = re.search(r"[?&]id=([A-Za-z0-9_-]{10,})", url)
    return m.group(1) if m else None


def fingerprint(platform: str, seconds: int, title: str) -> str:
    raw = f"{platform}|{seconds}|{title}".lower().strip()
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def decode_service_account_json() -> dict:
    raw = SERVICE_ACCOUNT_JSON
    if not raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not set.")

    # If it's a file path
    if raw.endswith(".json") and Path(raw).exists():
        return json.loads(Path(raw).read_text(encoding="utf-8"))

    # Try base64
    try:
        maybe = base64.b64decode(raw).decode("utf-8")
        if maybe.strip().startswith("{"):
            return json.loads(maybe)
    except Exception:
        pass

    # Try raw JSON
    if raw.strip().startswith("{"):
        return json.loads(raw)

    raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON could not be parsed (expected raw JSON, base64 JSON, or file path).")


class VideoSubmissionCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

        self.youtube_enabled = YOUTUBE_ENABLED
        self.youtube = None
        if self.youtube_enabled:
            self.youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY, cache_discovery=False)

        self.drive = None
        if SERVICE_ACCOUNT_JSON:
            try:
                info = decode_service_account_json()
                creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
                self.drive = gbuild("drive", "v3", credentials=creds, cache_discovery=False)
            except Exception as e:
                print(f"[video_submission] Drive disabled: {type(e).__name__}: {e}")

    def youtube_data_blocking(self, video_id: str) -> Tuple[int, str]:
        if not self.youtube_enabled or self.youtube is None:
            raise RuntimeError("YouTube is disabled (missing YOUTUBE_API_KEY).")

        resp = self.youtube.videos().list(part="contentDetails,snippet", id=video_id).execute()
        items = resp.get("items", [])
        if not items:
            raise RuntimeError("YouTube video not found (or private).")

        title = items[0]["snippet"]["title"]
        dur = items[0]["contentDetails"]["duration"]
        seconds = int(isodate.parse_duration(dur).total_seconds())
        return seconds, title

    def drive_duration_blocking(self, file_id: str) -> Tuple[int, str]:
        if self.drive is None:
            raise RuntimeError("Drive is disabled (missing/invalid GOOGLE_SERVICE_ACCOUNT_JSON).")

        meta = self.drive.files().get(fileId=file_id, fields="name,videoMediaMetadata").execute()
        title = meta.get("name", "Drive Video")
        vmeta = meta.get("videoMediaMetadata") or {}
        ms = int(vmeta.get("durationMillis") or 0)
        if ms <= 0:
            raise RuntimeError("Could not read Drive video duration (durationMillis missing).")
        return max(1, ms // 1000), title

    @app_commands.command(name="submit_video", description="Submit a YouTube or Google Drive video for approval.")
    async def submit_video(self, interaction: discord.Interaction, url: str):
        await interaction.response.defer(ephemeral=True)

        yid = yt_id(url)
        did = drive_id(url)

        if yid and not self.youtube_enabled:
            await interaction.followup.send(
                "❌ YouTube submissions are disabled because `YOUTUBE_API_KEY` is not set on Railway.",
                ephemeral=True,
            )
            return

        if not yid and not did:
            await interaction.followup.send("❌ Unsupported URL.", ephemeral=True)
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
            await interaction.followup.send(f"❌ API error: {e}", ephemeral=True)
            return
        except Exception as e:
            await interaction.followup.send(f"❌ Error: {type(e).__name__}: {e}", ephemeral=True)
            return

        fp = fingerprint(platform, seconds, title)
        audits = await load(AUDIT_FILE)
        if fp in audits:
            await interaction.followup.send("❌ Already submitted (or re-upload detected).", ephemeral=True)
            return

        pending = await load(VIDEO_FILE)
        pending[str(key)] = {
            "url": url,
            "platform": platform,
            "seconds": seconds,
            "title": title,
            "submitted_by": interaction.user.id,
            "submitted_at": int(datetime.datetime.now(datetime.timezone.utc).timestamp()),
            "fingerprint": fp,
        }
        await save(VIDEO_FILE, pending)

        guild = interaction.guild
        if not guild:
            await interaction.followup.send("❌ Must be used in a server.", ephemeral=True)
            return

        ch = discord.utils.get(guild.text_channels, name=APPROVAL_CHANNEL)
        if not ch:
            await interaction.followup.send(f"❌ Missing channel `#{APPROVAL_CHANNEL}`.", ephemeral=True)
            return

        embed = discord.Embed(
            title="Video Submission",
            description=f"**Title:** {title}\n**Platform:** {platform}\n**Duration:** {seconds}s\n\n{url}",
            timestamp=datetime.datetime.now(datetime.timezone.utc),
        )
        await ch.send(embed=embed)

        await interaction.followup.send(f"✅ Submitted to #{APPROVAL_CHANNEL}.", ephemeral=True)

    async def cog_load(self):
        if not self.youtube_enabled:
            print("[video_submission] YouTube disabled (missing YOUTUBE_API_KEY). Cog loaded; Drive may still work.")


async def setup(bot: commands.Bot):
    await bot.add_cog(VideoSubmissionCog(bot))
