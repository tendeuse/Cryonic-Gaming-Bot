import os
import json
import time
import base64
import sqlite3
import aiohttp
import discord
from discord.ext import commands
from discord import app_commands
from discord.ui import View
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List

# =========================
# PATHS
# =========================

DATA = Path(os.getenv("PERSIST_ROOT", "/data"))
DATA.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA / "buyback.db"
IGN_FILE = DATA / "ign_registry.json"

# =========================
# OAUTH HELPER
# =========================

class EveOAuth:
    def __init__(self):
        self.client_id = os.getenv("EVE_CLIENT_ID")
        self.client_secret = os.getenv("EVE_CLIENT_SECRET")
        self.refresh_token = os.getenv("EVE_REFRESH_TOKEN")

        if not self.client_id or not self.client_secret or not self.refresh_token:
            raise RuntimeError("Missing EVE_CLIENT_ID / EVE_CLIENT_SECRET / EVE_REFRESH_TOKEN in environment variables.")

        self._access_token: Optional[str] = None
        self._expires_at: float = 0.0  # epoch seconds

    async def get_access_token(self, session: aiohttp.ClientSession) -> str:
        # Reuse token if still valid (60s buffer)
        if self._access_token and time.time() < (self._expires_at - 60):
            return self._access_token

        basic = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode("utf-8")).decode("ascii")
        headers = {
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
        }

        async with session.post("https://login.eveonline.com/v2/oauth/token", headers=headers, data=data) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"OAuth refresh failed ({resp.status}): {text}")

            payload = await resp.json()

        self._access_token = payload["access_token"]
        self._expires_at = time.time() + int(payload.get("expires_in", 1200))
        return self._access_token

# =========================
# COG
# =========================

class BuybackAuto(commands.Cog):
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

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.oauth = EveOAuth()
        self.session = aiohttp.ClientSession()

        self.db = sqlite3.connect(DB_PATH)
        self.db.row_factory = sqlite3.Row
        self.init_db()

    def cog_unload(self):
        # discord.py will call this on unload
        try:
            if self.session and not self.session.closed:
                # can't await here; best-effort close
                self.bot.loop.create_task(self.session.close())
        except Exception:
            pass
        try:
            self.db.close()
        except Exception:
            pass

    # ================= DB =================

    def init_db(self):
        self.db.execute("""
        CREATE TABLE IF NOT EXISTS contracts (
            contract_id INTEGER PRIMARY KEY,
            issuer_char_id INTEGER,
            issuer_name TEXT,
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

    async def esi_headers(self) -> Dict[str, str]:
        token = await self.oauth.get_access_token(self.session)
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "ARC-Buyback-Bot",
        }

    async def get_character_name(self, character_id: int) -> str:
        # This endpoint does not require auth, but we can still call it without headers
        async with self.session.get(f"{self.ESI}/characters/{character_id}/") as r:
            if r.status != 200:
                return str(character_id)
            data = await r.json()
            return data.get("name", str(character_id))

    async def type_name(self, type_id: int) -> str:
        row = self.db.execute(
            "SELECT name FROM type_cache WHERE type_id=?",
            (type_id,)
        ).fetchone()
        if row:
            return row["name"]

        # universe/types does not require auth
        async with self.session.get(f"{self.ESI}/universe/types/{type_id}/") as r:
            if r.status != 200:
                return f"typeID:{type_id}"
            data = await r.json()
            name = data.get("name", f"typeID:{type_id}")

        self.db.execute(
            "INSERT OR IGNORE INTO type_cache(type_id, name) VALUES (?, ?)",
            (type_id, name)
        )
        self.db.commit()
        return name

    def resolve_discord_from_ign(self, ign: str) -> Optional[int]:
        """
        Match EVE character name (IGN) to your IGN registry:
        state["users"][discord_user_id]["igns"] = [list of character names]
        """
        if not IGN_FILE.exists():
            return None
        try:
            data = json.loads(IGN_FILE.read_text(encoding="utf-8"))
        except Exception:
            return None

        ign_l = (ign or "").strip().lower()
        if not ign_l:
            return None

        users = data.get("users", {})
        for uid, rec in users.items():
            igns = rec.get("igns", [])
            for x in igns:
                if (x or "").strip().lower() == ign_l:
                    try:
                        return int(uid)
                    except Exception:
                        return None
        return None

    def get_channel(self) -> Optional[discord.TextChannel]:
        for guild in self.bot.guilds:
            ch = discord.utils.get(guild.text_channels, name=self.PAYOUT_CHANNEL)
            if ch:
                return ch
        return None

    # ================= SLASH COMMAND =================

    @app_commands.command(name="buyback", description="Scan buyback contracts once (manual)")
    @app_commands.checks.has_role(APPROVER_ROLE)
    async def buyback_scan(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        headers = await self.esi_headers()

        async with self.session.get(
            f"{self.ESI}/corporations/{self.CORP_ID}/contracts/",
            headers=headers
        ) as r:
            body = await r.text()
            if r.status != 200:
                await interaction.followup.send(
                    f"❌ ESI error {r.status}\n```{body[:1800]}```",
                    ephemeral=True
                )
                return
            contracts = json.loads(body)

        new = 0
        skipped = 0

        for c in contracts:
            try:
                cid = int(c["contract_id"])
            except Exception:
                continue

            # already processed?
            if self.db.execute("SELECT 1 FROM contracts WHERE contract_id=?", (cid,)).fetchone():
                skipped += 1
                continue

            # filters
            if c.get("type") != "item_exchange":
                continue
            if int(c.get("assignee_id", 0)) != self.BUYBACK_CHARACTER_ID:
                continue
            if int(c.get("start_location_id", 0)) != self.AT1_STRUCTURE_ID:
                continue

            ok = await self.process_contract(c)
            if ok:
                new += 1

        await interaction.followup.send(
            f"✅ Buyback scan complete — {new} new contract(s) processed. ({skipped} already saved)",
            ephemeral=True
        )

    # ================= PROCESS =================

    async def process_contract(self, c: Dict[str, Any]) -> bool:
        cid = int(c["contract_id"])
        issuer_char_id = int(c.get("issuer_id", 0))

        issuer_name = await self.get_character_name(issuer_char_id)
        discord_id = self.resolve_discord_from_ign(issuer_name)

        headers = await self.esi_headers()

        # Pull items
        async with self.session.get(
            f"{self.ESI}/corporations/{self.CORP_ID}/contracts/{cid}/items/",
            headers=headers
        ) as r:
            text = await r.text()
            if r.status != 200:
                print(f"[BUYBACK] Failed items for {cid}: {r.status} {text[:500]}")
                return False
            items = json.loads(text)

        # Build Janice lines: "quantity ItemName" per line
        janice_lines: List[str] = []
        display_lines: List[str] = []
        total_qty = 0

        for it in items:
            qty = int(it.get("quantity", 0))
            type_id = int(it.get("type_id", 0))
            name = await self.type_name(type_id)

            total_qty += qty
            janice_lines.append(f"{qty} {name}")
            # display (limit spam later)
            display_lines.append(f"{qty:,} × {name}")

        payload = {
            "market": "jita",
            "pricing": "buy",
            "items": "\n".join(janice_lines),
        }

        # Janice appraisal
        async with self.session.post(self.JANICE, json=payload) as r:
            janice_text = await r.text()
            if r.status != 200:
                print(f"[BUYBACK] Janice error for {cid}: {r.status}\n{janice_text[:1000]}")
                # still record as FAILED so it won't loop forever
                self.db.execute(
                    "INSERT OR REPLACE INTO contracts VALUES (?, ?, ?, ?, 'JANICE_FAILED', 0, 0, ?)",
                    (cid, issuer_char_id, issuer_name, discord_id, datetime.utcnow().isoformat())
                )
                self.db.commit()
                ch = self.get_channel()
                if ch:
                    emb = discord.Embed(
                        title="❌ Buyback Contract — Pricing Failed",
                        description=f"Janice returned HTTP {r.status}. Contract saved as `JANICE_FAILED`.",
                        color=discord.Color.red(),
                        timestamp=datetime.utcnow(),
                    )
                    emb.add_field(name="Contract ID", value=str(cid), inline=True)
                    emb.add_field(name="Issuer (IGN)", value=issuer_name, inline=True)
                    emb.add_field(name="Discord", value=f"<@{discord_id}>" if discord_id else "Not found", inline=False)
                    await ch.send(embed=emb)
                return False

            appraisal = json.loads(janice_text)

        total = float(appraisal["effective_prices"]["total"])
        payout = total * self.BUYBACK_RATE

        # Save contract (PRICED)
        self.db.execute(
            "INSERT OR REPLACE INTO contracts VALUES (?, ?, ?, ?, 'PRICED', ?, ?, ?)",
            (cid, issuer_char_id, issuer_name, discord_id, total, payout, datetime.utcnow().isoformat())
        )
        self.db.commit()

        # Send embed
        channel = self.get_channel()
        if not channel:
            print("[BUYBACK] payout channel not found")
            return False

        ping = f"<@{discord_id}>" if discord_id else "Not found"

        emb = discord.Embed(
            title="📦 Buyback Contract — Pending",
            color=discord.Color.orange(),
            timestamp=datetime.utcnow(),
            description="Items are priced at **Jita 4-4 Buy** via Janice. Payout is **80%**."
        )
        emb.add_field(name="Contract ID", value=str(cid), inline=True)
        emb.add_field(name="Issuer (pay this IGN)", value=issuer_name, inline=True)
        emb.add_field(name="Discord", value=ping, inline=False)
        emb.add_field(name="Jita Buy Total", value=f"{total:,.0f} ISK", inline=True)
        emb.add_field(name="80% Payout", value=f"{payout:,.0f} ISK", inline=True)

        # show a preview of items (avoid huge embeds)
        preview = "\n".join(display_lines[:15])
        if len(display_lines) > 15:
            preview += f"\n… and {len(display_lines) - 15} more line(s)"
        emb.add_field(name="Items (preview)", value=f"```{preview}```", inline=False)

        await channel.send(embed=emb, view=ApprovalView(self, cid))
        return True

# =========================
# APPROVAL VIEW
# =========================

class ApprovalView(View):
    def __init__(self, cog: BuybackAuto, cid: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.cid = cid

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not isinstance(interaction.user, discord.Member):
            return False
        return any(r.name == self.cog.APPROVER_ROLE for r in interaction.user.roles)

    @discord.ui.button(label="Approve", style=discord.ButtonStyle.success)
    async def approve(self, interaction: discord.Interaction, _):
        self.cog.db.execute(
            "UPDATE contracts SET status='APPROVED' WHERE contract_id=?",
            (self.cid,)
        )
        self.cog.db.commit()
        await interaction.response.send_message(
            "✅ Approved. Pay ISK in-game, then click **Paid**.",
            ephemeral=True
        )

    @discord.ui.button(label="Paid", style=discord.ButtonStyle.primary)
    async def paid(self, interaction: discord.Interaction, _):
        self.cog.db.execute(
            "UPDATE contracts SET status='PAID' WHERE contract_id=?",
            (self.cid,)
        )
        self.cog.db.commit()
        await interaction.response.send_message("💰 Marked as PAID.", ephemeral=True)

    @discord.ui.button(label="Reject", style=discord.ButtonStyle.danger)
    async def reject(self, interaction: discord.Interaction, _):
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