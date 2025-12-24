# cogs/reports.py
import discord
import json
import io
import csv
import asyncio
import datetime
from pathlib import Path
from discord.ext import commands
from discord import app_commands
from discord.ui import View, Button, Modal, TextInput

DATA_FILE = Path("data/ap_data.json")
DATA_FILE.parent.mkdir(parents=True, exist_ok=True)

LYCAN_ROLE = "Lycan King"

META_KEY = "_meta"
REPORTS_KEY = "reports"

# Stored inside ap_data.json so it survives restarts
BACKUP_KEY = "ap_backup"          # latest backup snapshot
BACKUP_TS_KEY = "ap_backup_utc"   # timestamp for latest backup

# Claims storage (inside each user's dict)
CLAIMS_KEY = "claims"
CLAIM_HISTORY_MAX = 10

file_lock = asyncio.Lock()


# -------------------------
# Helpers (time)
# -------------------------

def utcnow_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


# -------------------------
# Interaction safety helpers
# -------------------------

async def safe_defer(interaction: discord.Interaction, *, ephemeral: bool):
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=ephemeral, thinking=True)
    except (discord.NotFound, discord.HTTPException):
        pass


async def safe_send(
    interaction: discord.Interaction,
    content: str = "",
    *,
    ephemeral: bool = True,
    embed: discord.Embed | None = None,
    view: View | None = None
):
    """
    Some discord.py versions error if you pass view=None.
    Only include 'view' when it is not None.
    """
    try:
        kwargs = {"content": content, "ephemeral": ephemeral}
        if embed is not None:
            kwargs["embed"] = embed
        if view is not None:
            kwargs["view"] = view

        if not interaction.response.is_done():
            await interaction.response.send_message(**kwargs)
        else:
            await interaction.followup.send(**kwargs)
    except (discord.NotFound, discord.HTTPException):
        pass


# -------------------------
# Persistence
# -------------------------

async def load_data() -> dict:
    async with file_lock:
        if not DATA_FILE.exists():
            return {}
        try:
            return json.loads(DATA_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            bak = DATA_FILE.with_suffix(".corrupt.bak")
            try:
                DATA_FILE.replace(bak)
            except Exception:
                pass
            return {}


async def save_data(data: dict) -> None:
    async with file_lock:
        DATA_FILE.write_text(json.dumps(data, indent=4), encoding="utf-8")


def safe_int(value) -> int:
    try:
        return int(float(value))
    except Exception:
        return 0


def is_lycan(member: discord.abc.User | discord.Member) -> bool:
    return isinstance(member, discord.Member) and any(r.name == LYCAN_ROLE for r in member.roles)


def require_lycan():
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return False
        if is_lycan(interaction.user):
            return True
        await safe_send(interaction, f"❌ You must have the **{LYCAN_ROLE}** role.", ephemeral=True)
        return False
    return app_commands.check(predicate)


# -------------------------
# Claim UI
# -------------------------

class ClaimIGNModal(Modal):
    def __init__(self, cog: "Reports", game: str):
        super().__init__(title=f"AP Claim — {game}")
        self.cog = cog
        self.game = game

        self.ign = TextInput(
            label="Enter your IGN for delivery",
            placeholder="Example: ARC Tendeuse A",
            required=True,
            max_length=80,
            style=discord.TextStyle.short,
        )
        self.add_item(self.ign)

    async def on_submit(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        ign_value = " ".join(str(self.ign.value or "").strip().split())
        if not ign_value:
            await safe_send(interaction, "❌ Invalid IGN.", ephemeral=True)
            return

        await self.cog.record_claim(interaction, game=self.game, ign=ign_value)


class APClaimView(View):
    def __init__(self, cog: "Reports"):
        super().__init__(timeout=120)
        self.cog = cog

    async def _open_modal(self, interaction: discord.Interaction, game: str):
        try:
            if interaction.response.is_done():
                await safe_send(interaction, "Please run `/apclaim` again.", ephemeral=True)
                return
            await interaction.response.send_modal(ClaimIGNModal(self.cog, game))
        except (discord.NotFound, discord.HTTPException):
            pass

    @discord.ui.button(label="World of Warcraft", style=discord.ButtonStyle.primary)
    async def wow(self, interaction: discord.Interaction, button: Button):
        await self._open_modal(interaction, "World of Warcraft")

    @discord.ui.button(label="Eve Online", style=discord.ButtonStyle.success)
    async def eve(self, interaction: discord.Interaction, button: Button):
        await self._open_modal(interaction, "Eve Online")

    @discord.ui.button(label="Oath", style=discord.ButtonStyle.secondary)
    async def oath(self, interaction: discord.Interaction, button: Button):
        await self._open_modal(interaction, "Oath")


# -------------------------
# Cog
# -------------------------

class Reports(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def record_claim(self, interaction: discord.Interaction, *, game: str, ign: str):
        data = await load_data()

        uid = str(interaction.user.id)
        rec = data.get(uid)
        if not isinstance(rec, dict):
            rec = {"ap": 0}
            data[uid] = rec

        history = rec.get(CLAIMS_KEY)
        if not isinstance(history, list):
            history = []

        history.append({
            "game": game,
            "ign": ign,
            "requested_utc": utcnow_iso(),
            "requested_by": interaction.user.id,
        })
        rec[CLAIMS_KEY] = history[-CLAIM_HISTORY_MAX:]

        await save_data(data)

        current_ap = safe_int(rec.get("ap", 0))
        await safe_send(
            interaction,
            f"✅ Claim request recorded.\n"
            f"**Game:** {game}\n"
            f"**IGN:** `{ign}`\n"
            f"**Current AP (record):** {current_ap}",
            ephemeral=True
        )

    async def resolve_member_strings(self, guild: discord.Guild, user_id: int) -> tuple[str, str]:
        m = guild.get_member(user_id)
        if m:
            return str(m.display_name), str(m)

        try:
            m2 = await guild.fetch_member(user_id)
            return str(m2.display_name), str(m2)
        except Exception:
            pass

        try:
            u = await self.bot.fetch_user(user_id)
            return str(u), str(u)
        except Exception:
            return f"Unknown ({user_id})", f"Unknown ({user_id})"

    def last_claim_fields(self, user_record: dict) -> tuple[str, str, str]:
        claims = user_record.get(CLAIMS_KEY)
        if isinstance(claims, list) and claims:
            last = claims[-1]
            if isinstance(last, dict):
                return (
                    str(last.get("game", "") or ""),
                    str(last.get("ign", "") or ""),
                    str(last.get("requested_utc", "") or ""),
                )
        return "", "", ""

    # =====================
    # SLASH COMMANDS
    # =====================

    @app_commands.command(
        name="apclaim",
        description="Request an AP claim (pick game + IGN)."
    )
    async def apclaim(self, interaction: discord.Interaction):
        emb = discord.Embed(
            title="AP Claim",
            description=(
                "Select the game you want to claim on.\n"
                "You will then be prompted for the IGN to deliver the in-game currency."
            ),
            timestamp=datetime.datetime.utcnow(),
        )
        emb.set_footer(text="This request is recorded for staff export.")
        await safe_send(interaction, embed=emb, view=APClaimView(self), ephemeral=True)

    @app_commands.command(
        name="export_ap",
        description="Backup AP, export report CSV, then wipe AP."
    )
    @require_lycan()
    async def export_ap(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        if not interaction.guild:
            await safe_send(interaction, "❌ This command must be used in a server.", ephemeral=True)
            return

        data = await load_data()

        # Build snapshot rows from current AP state (since last wipe)
        rows: list[dict] = []
        for uid, rec in data.items():
            if not isinstance(uid, str) or uid.startswith("_"):
                continue
            if not isinstance(rec, dict):
                continue

            ap_val = safe_int(rec.get("ap", 0))
            if ap_val <= 0:
                continue

            # CRITICAL: never convert Discord IDs via float()
            try:
                uid_int = int(uid)
            except Exception:
                continue

            display_name, username_label = await self.resolve_member_strings(interaction.guild, uid_int)
            claim_game, claim_ign, claim_utc = self.last_claim_fields(rec)

            rows.append({
                "uid": uid,
                "display_name": display_name,
                "username": username_label,
                "ap": ap_val,
                "claim_game": claim_game,
                "claim_ign": claim_ign,
                "claim_requested_utc": claim_utc,
            })

        rows.sort(key=lambda r: (int(r.get("ap", 0)), r.get("display_name", "")), reverse=True)

        # 1) Backup snapshot FIRST
        meta = data.setdefault(META_KEY, {})
        reports_meta = meta.setdefault(REPORTS_KEY, {})
        reports_meta[BACKUP_TS_KEY] = utcnow_iso()
        reports_meta[BACKUP_KEY] = {
            r["uid"]: {
                "ap": int(r["ap"]),
                "display_name": r["display_name"],
                "username": r["username"],
                "claim_game": r["claim_game"],
                "claim_ign": r["claim_ign"],
                "claim_requested_utc": r["claim_requested_utc"],
                CLAIMS_KEY: (data.get(r["uid"], {}).get(CLAIMS_KEY, []) if isinstance(data.get(r["uid"]), dict) else []),
            }
            for r in rows
        }
        await save_data(data)

        # 2) Build CSV report
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "Discord User ID",
            "Discord Display Name",
            "Discord Username",
            "AP (since last wipe)",
            "Last Claim Game",
            "Last Claim IGN",
            "Last Claim Requested (UTC)",
        ])

        for r in rows:
            writer.writerow([
                r["uid"],
                r["display_name"],
                r["username"],
                r["ap"],
                r["claim_game"],
                r["claim_ign"],
                r["claim_requested_utc"],
            ])

        csv_bytes = output.getvalue().encode("utf-8")

        # 3) Reset AP AFTER backup + report generation
        cleared = {META_KEY: meta}
        for uid, rec in data.items():
            if not isinstance(uid, str) or uid.startswith("_"):
                continue
            if not isinstance(rec, dict):
                continue
            claims = rec.get(CLAIMS_KEY, [])
            cleared[uid] = {
                "ap": 0,
                CLAIMS_KEY: claims if isinstance(claims, list) else [],
            }

        await save_data(cleared)

        backup_ts = reports_meta.get(BACKUP_TS_KEY, "unknown")
        await safe_send(
            interaction,
            f"✅ Export complete.\nBackup: `{backup_ts}` | Rows: `{len(rows)}` | AP reset to `0`.",
            ephemeral=True
        )

        try:
            await interaction.followup.send(
                file=discord.File(io.BytesIO(csv_bytes), filename="ap_report.csv"),
                ephemeral=True
            )
        except (discord.NotFound, discord.HTTPException):
            pass

    @app_commands.command(
        name="restore_ap",
        description="Restore AP from the last export backup."
    )
    @require_lycan()
    async def restore_ap(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        data = await load_data()
        meta = data.get(META_KEY, {})
        reports_meta = meta.get(REPORTS_KEY, {})

        backup = reports_meta.get(BACKUP_KEY)
        backup_ts = reports_meta.get(BACKUP_TS_KEY)

        if not isinstance(backup, dict) or not backup:
            await safe_send(interaction, "❌ No backup available (run `/export_ap` first).", ephemeral=True)
            return

        merged = dict(data)
        for uid, rec in backup.items():
            if not isinstance(uid, str):
                continue
            if not isinstance(rec, dict):
                merged[uid] = {"ap": safe_int(rec)}
                continue

            merged.setdefault(uid, {})
            if not isinstance(merged[uid], dict):
                merged[uid] = {}

            merged[uid]["ap"] = safe_int(rec.get("ap", 0))

            claims = rec.get(CLAIMS_KEY)
            if isinstance(claims, list):
                merged[uid][CLAIMS_KEY] = claims

        merged[META_KEY] = meta
        await save_data(merged)

        await safe_send(
            interaction,
            f"✅ AP restored{f' from `{backup_ts}`' if backup_ts else ''}.",
            ephemeral=True
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(Reports(bot))
