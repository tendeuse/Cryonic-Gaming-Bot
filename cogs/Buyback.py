import discord
from discord.ext import commands, tasks
from discord.ui import View, Button
import aiohttp
import sqlite3
import re
from datetime import datetime

class BuybackAuto(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.session = aiohttp.ClientSession()
        self.db = sqlite3.connect("buyback.db")
        self.db.row_factory = sqlite3.Row
        self.create_tables()
        self.processed = set()
        self.poll_contracts.start()

    # ================= CONFIG =================
    CORP_ID = 98743131
    BUYBACK_CHARACTER_ID = 2122848297

    AT1_STRUCTURE_ID = 1048840990158  # <-- PUT REAL STRUCTURE ID HERE

    BUYBACK_RATE = 0.80
    PAYOUT_CHANNEL = "buyback-payout"
    APPROVER_ROLE = "ARC Security Corporation Leader"

    ESI = "https://esi.evetech.net/latest"
    JANICE = "https://janice.e-351.com/a/"
    CHECK_INTERVAL = 120
    # ==========================================

    def create_tables(self):
        self.db.execute("""
        CREATE TABLE IF NOT EXISTS buyback_contracts (
            contract_id INTEGER PRIMARY KEY,
            issuer_id INTEGER,
            discord_name TEXT,
            discord_id INTEGER,
            janice_total REAL,
            payout REAL,
            status TEXT,
            approved_by TEXT,
            timestamp TEXT
        )
        """)
        self.db.commit()

    def cog_unload(self):
        self.poll_contracts.cancel()
        self.db.close()

    @tasks.loop(seconds=CHECK_INTERVAL)
    async def poll_contracts(self):
        await self.bot.wait_until_ready()

        async with self.session.get(
            f"{self.ESI}/corporations/{self.CORP_ID}/contracts/"
        ) as resp:
            if resp.status != 200:
                return
            contracts = await resp.json()

        for c in contracts:
            cid = c["contract_id"]

            if cid in self.processed:
                continue
            if c["type"] != "item_exchange":
                continue
            if c["assignee_id"] != self.BUYBACK_CHARACTER_ID:
                continue
            if c["start_location_id"] != self.AT1_STRUCTURE_ID:
                continue

            self.processed.add(cid)
            await self.handle_contract(c)

    async def handle_contract(self, contract):
        cid = contract["contract_id"]

        # --- Extract Discord from contract note ---
        note = contract.get("title", "")
        match = re.search(r"discord\s*:\s*([^\n]+)", note, re.I)
        discord_name = match.group(1).strip() if match else None

        discord_user = None
        if discord_name:
            for member in self.bot.get_all_members():
                if (
                    member.name == discord_name
                    or f"{member.name}#{member.discriminator}" == discord_name
                ):
                    discord_user = member
                    break

        # --- Pull items ---
        async with self.session.get(
            f"{self.ESI}/corporations/{self.CORP_ID}/contracts/{cid}/items/"
        ) as resp:
            items = await resp.json()

        janice_lines = []
        abyssal = False

        for i in items:
            janice_lines.append(f"{i['quantity']}x typeID:{i['type_id']}")
            if i.get("is_singleton"):
                abyssal = True

        channel = discord.utils.get(
            self.bot.get_all_channels(),
            name=self.PAYOUT_CHANNEL
        )

        ping = discord_user.mention if discord_user else "⚠ Discord not found"

        embed = discord.Embed(
            title="📦 Buyback Contract Pending Approval",
            color=discord.Color.orange(),
            timestamp=datetime.utcnow()
        )

        embed.add_field(name="Contract ID", value=str(cid))
        embed.add_field(name="Location", value="AT1")
        embed.add_field(name="Rate", value="80% Jita Buy")
        embed.add_field(name="Submitted By", value=ping, inline=False)

        if abyssal:
            embed.add_field(
                name="Abyssal Items",
                value="Detected (allowed)",
                inline=False
            )

        embed.add_field(
            name="Janice Appraisal",
            value=f"[Open Janice]({self.JANICE})",
            inline=False
        )

        view = BuybackApprovalView(self, cid, discord_user)

        await channel.send(embed=embed, view=view)
        await channel.send(f"```{chr(10).join(janice_lines)}```")

    async def record(self, cid, discord_user, janice_total, status, approver):
        payout = janice_total * self.BUYBACK_RATE if status == "APPROVED" else 0

        self.db.execute("""
        INSERT OR REPLACE INTO buyback_contracts
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            cid,
            None,
            discord_user.name if discord_user else None,
            discord_user.id if discord_user else None,
            janice_total,
            payout,
            status,
            approver,
            datetime.utcnow().isoformat()
        ))
        self.db.commit()

class BuybackApprovalView(View):
    def __init__(self, cog, cid, discord_user):
        super().__init__(timeout=None)
        self.cog = cog
        self.cid = cid
        self.discord_user = discord_user

    async def interaction_check(self, interaction):
        return discord.utils.get(
            interaction.user.roles,
            name=self.cog.APPROVER_ROLE
        ) is not None

    @discord.ui.button(label="Approve", style=discord.ButtonStyle.success)
    async def approve(self, interaction, button):
        await interaction.response.send_message(
            "Reply with Janice **Jita Buy** total (numbers only):",
            ephemeral=True
        )

        msg = await self.cog.bot.wait_for(
            "message",
            check=lambda m: m.author == interaction.user,
            timeout=60
        )

        total = float(msg.content.replace(",", ""))
        await self.cog.record(
            self.cid, self.discord_user, total,
            "APPROVED", interaction.user.display_name
        )

        await interaction.followup.send("✅ Buyback approved.", ephemeral=True)

    @discord.ui.button(label="Reject", style=discord.ButtonStyle.danger)
    async def reject(self, interaction, button):
        await self.cog.record(
            self.cid, self.discord_user, 0,
            "REJECTED", interaction.user.display_name
        )
        await interaction.response.send_message(
            "❌ Buyback rejected.", ephemeral=True
        )

async def setup(bot):
    await bot.add_cog(BuybackAuto(bot))
