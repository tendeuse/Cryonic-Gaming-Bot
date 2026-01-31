import os
import json
import sqlite3
import aiohttp
import discord
from discord.ext import commands
from discord import app_commands
from discord.ui import View, Button
from datetime import datetime
from pathlib import Path

# =========================
# PATHS
# =========================

DATA = Path(os.getenv("PERSIST_ROOT", "/data"))
DATA.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA / "buyback.db"
IGN_FILE = DATA / "ign_registry.json"

# =========================
# COG
# =========================

class BuybackAuto(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

        self.bearer = os.getenv("ESI_BEARER_TOKEN")
        if not self.bearer:
            raise RuntimeError("ESI_BEARER_TOKEN missing")

        self.session = aiohttp.ClientSession()
        self.db = sqlite3.connect(DB_PATH)
        self.db.row_factory = sqlite3.Row
        self.init_db()

    # ================= CONFIG =================
    CORP_ID = 98743131
    BUYBACK_CHARACTER_ID = 2122848297
    AT1_STRUCTURE_ID = 1048840990158

    BUYBACK_RATE = 0.80
    PAYOUT_CHANNEL = "buyback-payout"
    APPROVER_ROLE = "ARC Security Corporation Leader"

    ESI = "https://esi.evetech.net/latest"
    JANICE = "https://janice.e-351.com/api/v2/appraisal"
    # ==========================================

    # ================= DB =================

    def init_db(self):
        self.db.execute("""
        CREATE TABLE IF NOT EXISTS contracts (
            contract_id INTEGER PRIMARY KEY,
            ign TEXT,
            discord_id INTEGER,
            status TEXT,
            total REAL,
            payout REAL,
            ts TEXT
        )
        """)
        self.db.execute("""
        CREATE TABLE IF NOT EXISTS type_cache (
            type_id INTEGER PRIMARY KEY,
            name TEXT
        )
        """)
        self.db.commit()

    # ================= HELPERS =================

    def esi_headers(self):
        return {
            "Authorization": f"Bearer {self.bearer}",
            "Accept": "application/json",
            "User-Agent": "ARC-Buyback-Bot"
        }

    async def type_name(self, type_id: int) -> str:
        row = self.db.execute(
            "SELECT name FROM type_cache WHERE type_id=?",
            (type_id,)
        ).fetchone()
        if row:
            return row["name"]

        async with self.session.get(f"{self.ESI}/universe/types/{type_id}/") as r:
            if r.status != 200:
                return f"typeID:{type_id}"
            data = await r.json()
            name = data["name"]

        self.db.execute(
            "INSERT OR IGNORE INTO type_cache VALUES (?,?)",
            (type_id, name)
        )
        self.db.commit()
        return name

    def resolve_discord_from_ign(self, ign: str):
        if not IGN_FILE.exists():
            return None
        try:
            data = json.loads(IGN_FILE.read_text())
        except Exception:
            return None

        for uid, rec in data.get("users", {}).items():
            if ign.lower() in (x.lower() for x in rec.get("igns", [])):
                return int(uid)
        return None

    # ================= SLASH COMMAND =================

    @app_commands.command(name="buyback", description="Scan buyback contracts once")
    @app_commands.checks.has_role(APPROVER_ROLE)
    async def buyback_scan(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        async with self.session.get(
            f"{self.ESI}/corporations/{self.CORP_ID}/contracts/",
            headers=self.esi_headers()
        ) as r:
            if r.status != 200:
                body = await r.text()
                await interaction.followup.send(
                    f"❌ ESI error {r.status}\n```{body}```",
                    ephemeral=True
                )
                return

            contracts = await r.json()

        new = 0

        for c in contracts:
            cid = c["contract_id"]

            if self.db.execute(
                "SELECT 1 FROM contracts WHERE contract_id=?",
                (cid,)
            ).fetchone():
                continue

            if c["type"] != "item_exchange":
                continue
            if c["assignee_id"] != self.BUYBACK_CHARACTER_ID:
                continue
            if c["start_location_id"] != self.AT1_STRUCTURE_ID:
                continue

            await self.process_contract(c)
            new += 1

        await interaction.followup.send(
            f"✅ Buyback scan complete — {new} new contract(s) processed.",
            ephemeral=True
        )

    # ================= PROCESS =================

    async def process_contract(self, c):
    cid = c["contract_id"]
    ign = str(c["issuer_id"])
    discord_id = self.resolve_discord_from_ign(ign)

    async with self.session.get(
        f"{self.ESI}/corporations/{self.CORP_ID}/contracts/{cid}/items/",
        headers=self.esi_headers()
    ) as r:
        items = await r.json()

    # Janice expects "quantity ItemName" per line, not "1x ItemName"
    lines = []
    for i in items:
        name = await self.type_name(i["type_id"])
        lines.append(f"{i['quantity']} {name}")

    # Prepare payload
    payload = {
        "market": "jita",
        "pricing": "buy",
        "items": "\n".join(lines)
    }

    async with self.session.post(self.JANICE, json=payload) as r:
        if r.status != 200:
            text = await r.text()
            print(f"❌ Janice API error {r.status}:\n{text}")
            return
        appraisal = await r.json()

    total = appraisal["effective_prices"]["total"]
    payout = total * self.BUYBACK_RATE

    # Save contract
    self.db.execute(
        "INSERT INTO contracts VALUES (?, ?, ?, 'PRICED', ?, ?, ?)",
        (cid, ign, discord_id, total, payout, datetime.utcnow().isoformat())
    )
    self.db.commit()

    # Send embed
    channel = discord.utils.get(
        self.bot.get_all_channels(), name=self.PAYOUT_CHANNEL
    )
    if not channel:
        return

    emb = discord.Embed(
        title="📦 Buyback Contract",
        color=discord.Color.blue(),
        timestamp=datetime.utcnow()
    )
    emb.add_field(name="IGN (pay this character)", value=ign, inline=False)
    emb.add_field(
        name="Discord",
        value=f"<@{discord_id}>" if discord_id else "Not found",
        inline=False
    )
    emb.add_field(name="Jita Buy Total", value=f"{total:,.0f} ISK")
    emb.add_field(name="80% Payout", value=f"{payout:,.0f} ISK")

    await channel.send(embed=emb, view=ApprovalView(self, cid))


# =========================
# APPROVAL VIEW
# =========================

class ApprovalView(View):
    def __init__(self, cog: BuybackAuto, cid: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.cid = cid

    async def interaction_check(self, interaction):
        return any(
            r.name == self.cog.APPROVER_ROLE
            for r in interaction.user.roles
        )

    @discord.ui.button(label="Approve", style=discord.ButtonStyle.success)
    async def approve(self, interaction, _):
        self.cog.db.execute(
            "UPDATE contracts SET status='APPROVED' WHERE contract_id=?",
            (self.cid,)
        )
        self.cog.db.commit()
        await interaction.response.send_message(
            "✅ Approved. Pay ISK, then click **Paid**.",
            ephemeral=True
        )

    @discord.ui.button(label="Paid", style=discord.ButtonStyle.primary)
    async def paid(self, interaction, _):
        self.cog.db.execute(
            "UPDATE contracts SET status='PAID' WHERE contract_id=?",
            (self.cid,)
        )
        self.cog.db.commit()
        await interaction.response.send_message("💰 Marked as PAID.", ephemeral=True)

    @discord.ui.button(label="Reject", style=discord.ButtonStyle.danger)
    async def reject(self, interaction, _):
        self.cog.db.execute(
            "UPDATE contracts SET status='REJECTED' WHERE contract_id=?",
            (self.cid,)
        )
        self.cog.db.commit()
        await interaction.response.send_message("❌ Rejected.", ephemeral=True)

# =========================
# SETUP
# =========================

async def setup(bot: commands.Bot):
    await bot.add_cog(BuybackAuto(bot))
