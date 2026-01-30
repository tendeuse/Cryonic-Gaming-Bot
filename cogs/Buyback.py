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
        print("[BUYBACK] Cog loaded, starting poll loop")
        self.poll_contracts.start()

    # ================= CONFIG =================
    CORP_ID = 98743131
    BUYBACK_CHARACTER_ID = 2122848297

    AT1_STRUCTURE_ID = 1048840990158  # <- Your structure ID

    BUYBACK_RATE = 0.80
    PAYOUT_CHANNEL = "buyback-payout"
    APPROVER_ROLE = "ARC Security Corporation Leader"

    ESI = "https://esi.evetech.net/latest"
    JANICE = "https://janice.e-351.com/a/"
    CHECK_INTERVAL = 120

    # ✅ Add your Bearer token here
    ESI_BEARER_TOKEN = "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IkpXVC1TaWduYXR1cmUtS2V5IiwidHlwIjoiSldUIn0.eyJzY3AiOlsiZXNpLWNhbGVuZGFyLnJlc3BvbmRfY2FsZW5kYXJfZXZlbnRzLnYxIiwiZXNpLWNhbGVuZGFyLnJlYWRfY2FsZW5kYXJfZXZlbnRzLnYxIiwiZXNpLWxvY2F0aW9uLnJlYWRfbG9jYXRpb24udjEiLCJlc2ktbG9jYXRpb24ucmVhZF9zaGlwX3R5cGUudjEiLCJlc2ktbWFpbC5vcmdhbml6ZV9tYWlsLnYxIiwiZXNpLW1haWwucmVhZF9tYWlsLnYxIiwiZXNpLW1haWwuc2VuZF9tYWlsLnYxIiwiZXNpLXNraWxscy5yZWFkX3NraWxscy52MSIsImVzaS1za2lsbHMucmVhZF9za2lsbHF1ZXVlLnYxIiwiZXNpLXdhbGxldC5yZWFkX2NoYXJhY3Rlcl93YWxsZXQudjEiLCJlc2ktc2VhcmNoLnNlYXJjaF9zdHJ1Y3R1cmVzLnYxIiwiZXNpLWNsb25lcy5yZWFkX2Nsb25lcy52MSIsImVzaS1jaGFyYWN0ZXJzLnJlYWRfY29udGFjdHMudjEiLCJlc2ktdW5pdmVyc2UucmVhZF9zdHJ1Y3R1cmVzLnYxIiwiZXNpLWtpbGxtYWlscy5yZWFkX2tpbGxtYWlscy52MSIsImVzaS1jb3Jwb3JhdGlvbnMucmVhZF9jb3Jwb3JhdGlvbl9tZW1iZXJzaGlwLnYxIiwiZXNpLWFzc2V0cy5yZWFkX2Fzc2V0cy52MSIsImVzaS1wbGFuZXRzLm1hbmFnZV9wbGFuZXRzLnYxIiwiZXNpLWZsZWV0cy5yZWFkX2ZsZWV0LnYxIiwiZXNpLWZsZWV0cy53cml0ZV9mbGVldC52MSIsImVzaS11aS5vcGVuX3dpbmRvdy52MSIsImVzaS11aS53cml0ZV93YXlwb2ludC52MSIsImVzaS1jaGFyYWN0ZXJzLndyaXRlX2NvbnRhY3RzLnYxIiwiZXNpLWZpdHRpbmdzLnJlYWRfZml0dGluZ3MudjEiLCJlc2ktZml0dGluZ3Mud3JpdGVfZml0dGluZ3MudjEiLCJlc2ktbWFya2V0cy5zdHJ1Y3R1cmVfbWFya2V0cy52MSIsImVzaS1jb3Jwb3JhdGlvbnMucmVhZF9zdHJ1Y3R1cmVzLnYxIiwiZXNpLWNoYXJhY3RlcnMucmVhZF9sb3lhbHR5LnYxIiwiZXNpLWNoYXJhY3RlcnMucmVhZF9tZWRhbHMudjEiLCJlc2ktY2hhcmFjdGVycy5yZWFkX3N0YW5kaW5ncy52MSIsImVzaS1jaGFyYWN0ZXJzLnJlYWRfYWdlbnRzX3Jlc2VhcmNoLnYxIiwiZXNpLWluZHVzdHJ5LnJlYWRfY2hhcmFjdGVyX2pvYnMudjEiLCJlc2ktbWFya2V0cy5yZWFkX2NoYXJhY3Rlcl9vcmRlcnMudjEiLCJlc2ktY2hhcmFjdGVycy5yZWFkX2JsdWVwcmludHMudjEiLCJlc2ktY2hhcmFjdGVycy5yZWFkX2NvcnBvcmF0aW9uX3JvbGVzLnYxIiwiZXNpLWxvY2F0aW9uLnJlYWRfb25saW5lLnYxIiwiZXNpLWNvbnRyYWN0cy5yZWFkX2NoYXJhY3Rlcl9jb250cmFjdHMudjEiLCJlc2ktY2xvbmVzLnJlYWRfaW1wbGFudHMudjEiLCJlc2ktY2hhcmFjdGVycy5yZWFkX2ZhdGlndWUudjEiLCJlc2kta2lsbG1haWxzLnJlYWRfY29ycG9yYXRpb25fa2lsbG1haWxzLnYxIiwiZXNpLWNvcnBvcmF0aW9ucy50cmFja19tZW1iZXJzLnYxIiwiZXNpLXdhbGxldC5yZWFkX2NvcnBvcmF0aW9uX3dhbGxldHMudjEiLCJlc2ktY2hhcmFjdGVycy5yZWFkX25vdGlmaWNhdGlvbnMudjEiLCJlc2ktY29ycG9yYXRpb25zLnJlYWRfZGl2aXNpb25zLnYxIiwiZXNpLWNvcnBvcmF0aW9ucy5yZWFkX2NvbnRhY3RzLnYxIiwiZXNpLWFzc2V0cy5yZWFkX2NvcnBvcmF0aW9uX2Fzc2V0cy52MSIsImVzaS1jb3Jwb3JhdGlvbnMucmVhZF90aXRsZXMudjEiLCJlc2ktY29ycG9yYXRpb25zLnJlYWRfYmx1ZXByaW50cy52MSIsImVzaS1jb250cmFjdHMucmVhZF9jb3Jwb3JhdGlvbl9jb250cmFjdHMudjEiLCJlc2ktY29ycG9yYXRpb25zLnJlYWRfc3RhbmRpbmdzLnYxIiwiZXNpLWNvcnBvcmF0aW9ucy5yZWFkX3N0YXJiYXNlcy52MSIsImVzaS1pbmR1c3RyeS5yZWFkX2NvcnBvcmF0aW9uX2pvYnMudjEiLCJlc2ktbWFya2V0cy5yZWFkX2NvcnBvcmF0aW9uX29yZGVycy52MSIsImVzaS1jb3Jwb3JhdGlvbnMucmVhZF9jb250YWluZXJfbG9ncy52MSIsImVzaS1pbmR1c3RyeS5yZWFkX2NoYXJhY3Rlcl9taW5pbmcudjEiLCJlc2ktaW5kdXN0cnkucmVhZF9jb3Jwb3JhdGlvbl9taW5pbmcudjEiLCJlc2ktcGxhbmV0cy5yZWFkX2N1c3RvbXNfb2ZmaWNlcy52MSIsImVzaS1jb3Jwb3JhdGlvbnMucmVhZF9mYWNpbGl0aWVzLnYxIiwiZXNpLWNvcnBvcmF0aW9ucy5yZWFkX21lZGFscy52MSIsImVzaS1jaGFyYWN0ZXJzLnJlYWRfdGl0bGVzLnYxIiwiZXNpLWFsbGlhbmNlcy5yZWFkX2NvbnRhY3RzLnYxIiwiZXNpLWNoYXJhY3RlcnMucmVhZF9md19zdGF0cy52MSIsImVzaS1jb3Jwb3JhdGlvbnMucmVhZF9md19zdGF0cy52MSIsImVzaS1jb3Jwb3JhdGlvbnMucmVhZF9wcm9qZWN0cy52MSIsImVzaS1jb3Jwb3JhdGlvbnMucmVhZF9mcmVlbGFuY2Vfam9icy52MSIsImVzaS1jaGFyYWN0ZXJzLnJlYWRfZnJlZWxhbmNlX2pvYnMudjEiXSwianRpIjoiOGZmZWU3YzMtZTNkOS00YWZmLTllYzctZDE5ZWJhZmY4OTEzIiwia2lkIjoiSldULVNpZ25hdHVyZS1LZXkiLCJzdWIiOiJDSEFSQUNURVI6RVZFOjIxMjI4NDgyOTciLCJhenAiOiJkZXZlbG9wZXJzX2V2ZW9ubGluZV9jb20iLCJ0ZW5hbnQiOiJ0cmFucXVpbGl0eSIsInRpZXIiOiJsaXZlIiwicmVnaW9uIjoid29ybGQiLCJhdWQiOlsiZGV2ZWxvcGVyc19ldmVvbmxpbmVfY29tIiwiRVZFIE9ubGluZSJdLCJuYW1lIjoiQVJDIFRlbmRldXNlIEEiLCJvd25lciI6ImI2RlVTRHhBYlBtOEk3ZisvVHl4SXphcENoRT0iLCJleHAiOjE3Njk3NDA0MzMsImlhdCI6MTc2OTczOTIzMywiaXNzIjoiaHR0cHM6Ly9sb2dpbi5ldmVvbmxpbmUuY29tIn0.dcYl8vFXtin0rzDDzo5MS3pLrhpwVIGlIWrDRzqCYvBIwY7w8qMDxZzME1tFL2Wc9mgaoUK0FXHJmfT7G50CK_-w_Mw25QD83lX5K4Zcm6h0_rYHQqAtbsLywEmDfeOTFGpGxQuVReG2ZeYYW8XsjAYbZBd958Qrf8xD5ozdjvd9mE2HSz-2fombwuY4Kz3m2lNSQMxCq14Dajz1Fw1_zQvCKNjBFWS-1sp5DI9fSrJnbNugP1Ndfylop72Wm7bloob1ob0kCBNkZUGgNDc0Aaa32doMTCFqrI4OLki_wVWchBxSZJTAQBUJ-8L9tD8neIklKDFljFi-CCTHrpszCQ"
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
        print("[BUYBACK] Polling contracts...")

        headers = {"Authorization": f"Bearer {self.ESI_BEARER_TOKEN}"}

        async with self.session.get(
            f"{self.ESI}/corporations/{self.CORP_ID}/contracts/",
            headers=headers
        ) as resp:
            print(f"[BUYBACK] ESI status: {resp.status}")
            if resp.status != 200:
                return
            contracts = await resp.json()

        print(f"[BUYBACK] Contracts returned: {len(contracts)}")

        for c in contracts:
            cid = c["contract_id"]

            print(
                f"[DEBUG] Contract {cid} | "
                f"type={c['type']} | "
                f"assignee={c['assignee_id']} | "
                f"location={c['start_location_id']}"
            )

            if cid in self.processed:
                print(f"[SKIP] {cid} already processed")
                continue

            if c["type"] != "item_exchange":
                print(f"[SKIP] {cid} not item_exchange")
                continue

            if c["assignee_id"] != self.BUYBACK_CHARACTER_ID:
                print(
                    f"[SKIP] {cid} wrong assignee "
                    f"(got {c['assignee_id']})"
                )
                continue

            if c["start_location_id"] != self.AT1_STRUCTURE_ID:
                print(
                    f"[SKIP] {cid} wrong structure "
                    f"(got {c['start_location_id']})"
                )
                continue

            print(f"[MATCH] Processing contract {cid}")
            self.processed.add(cid)
            await self.handle_contract(c)

    async def handle_contract(self, contract):
        cid = contract["contract_id"]
        print(f"[HANDLE] Contract {cid}")

        # --- Extract Discord from contract note ---
        note = contract.get("title", "")
        print(f"[NOTE] {note}")

        match = re.search(r"discord\s*:\s*([^\n]+)", note, re.I)
        discord_name = match.group(1).strip() if match else None
        print(f"[DISCORD] Extracted: {discord_name}")

        discord_user = None
        if discord_name:
            for member in self.bot.get_all_members():
                if (
                    member.name == discord_name
                    or f"{member.name}#{member.discriminator}" == discord_name
                ):
                    discord_user = member
                    break

        print(f"[DISCORD] Resolved user: {discord_user}")

        headers = {"Authorization": f"Bearer {self.ESI_BEARER_TOKEN}"}

        # --- Pull items ---
        async with self.session.get(
            f"{self.ESI}/corporations/{self.CORP_ID}/contracts/{cid}/items/",
            headers=headers
        ) as resp:
            print(f"[ITEMS] Status: {resp.status}")
            items = await resp.json()

        print(f"[ITEMS] Count: {len(items)}")

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

        print(f"[CHANNEL] Found channel: {channel}")

        if not channel:
            print("[ERROR] buyback-payout channel not found")
            return

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

        print(f"[DONE] Contract {cid} posted")

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
        allowed = discord.utils.get(
            interaction.user.roles,
            name=self.cog.APPROVER_ROLE
        ) is not None
        print(
            f"[INTERACTION] {interaction.user} allowed={allowed}"
        )
        return allowed

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
