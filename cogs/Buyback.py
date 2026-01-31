import os
import json
import sqlite3
import aiohttp
import discord
from discord.ext import commands, tasks
from discord.ui import View, Button
from datetime import datetime
from pathlib import Path
import re

# =========================
# PATHS / STORAGE
# =========================

PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)

DB_PATH = PERSIST_ROOT / "buyback.db"
IGN_FILE = PERSIST_ROOT / "ign_registry.json"

# =========================
# COG
# =========================

class BuybackAuto(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

        self.esi_token = os.getenv("ESI_BEARER_TOKEN")
        if not self.esi_token:
            raise RuntimeError("ESI_BEARER_TOKEN missing")

        self.session = aiohttp.ClientSession()
        self.db = sqlite3.connect(DB_PATH)
        self.db.row_factory = sqlite3.Row
        self.create_tables()

        self.poll_contracts.start()
        print("[BUYBACK] Started")

    # ================= CONFIG =================
    CORP_ID = 98743131
    BUYBACK_CHARACTER_ID = 2122848297
    AT1_STRUCTURE_ID = 1048840990158

    BUYBACK_RATE = 0.80
    PAYOUT_CHANNEL = "buyback-payout"
    APPROVER_ROLE = "ARC Security Corporation Leader"

    ESI = "https://esi.evetech.net/latest"
    JANICE_API = "https://janice.e-351.com/api/v2/appraisal"
    CHECK_INTERVAL = 120
    # ==========================================

    # ================= DB =================

    def create_tables(self):
        cur = self.db.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS contracts (
            contract_id INTEGER PRIMARY KEY,
            ign TEXT,
            discord_id INTEGER,
            status TEXT,
            total_jita REAL,
            payout REAL,
            timestamp TEXT
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS type_cache (
            type_id INTEGER PRIMARY KEY,
            name TEXT
        )
        """)
        self.db.commit()

    def contract_seen(self, cid: int) -> bool:
        return self.db.execute(
            "SELECT 1 FROM contracts WHERE contract_id=?",
            (cid,)
        ).fetchone() is not None

    # ================= HELPERS =================

    def esi_headers(self):
        return {
            "Authorization": f"Bearer {self.esi_token}",
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

    def load_ign_registry(self):
        if not IGN_FILE.exists():
            return {}
        try:
            return json.loads(IGN_FILE.read_text())
        except Exception:
            return {}

    def resolve_discord_from_ign(self, ign: str):
        data = self.load_ign_registry()
        for uid, rec in data.get("users", {}).items():
            if ign.lower() in (x.lower() for x in rec.get("igns", [])):
                return int(uid)
        return None

    # ================= POLLER =================

    @tasks.loop(seconds=CHECK_INTERVAL)
    async def poll_contracts(self):
        url = f"{self.ESI}/corporations/{self.CORP_ID}/contracts/"
        async with self.session.get(url, headers=self.esi_headers()) as r:
            if r.status != 200:
                print("[BUYBACK] ESI error", r.status)
                return
            contracts = await r.json()

        for c in contracts:
            cid = c["contract_id"]

            if self.contract_seen(cid):
                continue
            if c["type"] != "item_exchange":
                continue
            if c["assignee_id"] != self.BUYBACK_CHARACTER_ID:
                continue
            if c["start_location_id"] != self.AT1_STRUCTURE_ID:
                continue

            await self.process_contract(c)

    # ================= PROCESS =================

    async def process_contract(self, contract):
        cid = contract["contract_id"]

        # ---- Resolve IGN from contract issuer ----
        ign = str(contract.get("issuer_id"))

        # ---- Resolve Discord from IGN registry ----
        discord_id = self.resolve_discord_from_ign(ign)

        self.db.execute(
            "INSERT INTO contracts VALUES (?, ?, ?, 'PRICED', 0, 0, ?)",
            (cid, ign, discord_id, datetime.utcnow().isoformat())
        )
        self.db.commit()

        # ---- ITEMS ----
        async with self.session.get(
            f"{self.ESI}/corporations/{self.CORP_ID}/contracts/{cid}/items/",
            headers=self.esi_headers()
        ) as r:
            items = await r.json()

        janice_lines = []
        for i in items:
            name = await self.type_name(i["type_id"])
            janice_lines.append(f"{i['quantity']}x {name}")

        # ---- JANICE ----
        async with self.session.post(
            self.JANICE_API,
            json={"market": "jita", "pricing": "buy", "items": "\n".join(janice_lines)}
        ) as r:
            appraisal = await r.json()

        total = appraisal["effective_prices"]["total"]
        payout = total * self.BUYBACK_RATE

        self.db.execute(
            "UPDATE contracts SET total_jita=?, payout=? WHERE contract_id=?",
            (total, payout, cid)
        )
        self.db.commit()

        channel = discord.utils.get(
            self.bot.get_all_channels(), name=self.PAYOUT_CHANNEL
        )
        if not channel:
            return

        emb = discord.Embed(
            title="📦 Buyback Contract (Priced)",
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )

        item_lines = []
        for row in appraisal["items"]:
            item_lines.append(
                f"• **{row['name']}** × {row['quantity']} — `{row['total']:,} ISK`"
            )

        emb.add_field(name="Items", value="\n".join(item_lines)[:1024], inline=False)
        emb.add_field(name="Jita Buy Total", value=f"{total:,.0f} ISK")
        emb.add_field(name="80% Buyback", value=f"{payout:,.0f} ISK")
        emb.add_field(name="IGN (Pay this character)", value=ign, inline=False)

        mention = f"<@{discord_id}>" if discord_id else "Discord not found"
        emb.add_field(name="Discord", value=mention, inline=False)

        await channel.send(embed=emb, view=ApprovalView(self, cid))

    # ================= STATE =================

    async def set_status(self, cid: int, status: str):
        self.db.execute(
            "UPDATE contracts SET status=? WHERE contract_id=?",
            (status, cid)
        )
        self.db.commit()

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
        await self.cog.set_status(self.cid, "APPROVED")
        await interaction.response.send_message("✅ Approved. Pay ISK, then click **Paid**.", ephemeral=True)

    @discord.ui.button(label="Paid", style=discord.ButtonStyle.primary)
    async def paid(self, interaction, _):
        await self.cog.set_status(self.cid, "PAID")
        await interaction.response.send_message("💰 Marked as PAID.", ephemeral=True)

    @discord.ui.button(label="Reject", style=discord.ButtonStyle.danger)
    async def reject(self, interaction, _):
        await self.cog.set_status(self.cid, "REJECTED")
        await interaction.response.send_message("❌ Rejected.", ephemeral=True)

# =========================
# SETUP
# =========================

async def setup(bot: commands.Bot):
    await bot.add_cog(BuybackAuto(bot))
