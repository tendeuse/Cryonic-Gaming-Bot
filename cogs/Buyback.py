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
from typing import Optional, Dict, Any, List, Tuple

# =========================
# PATHS
# =========================

DATA = Path(os.getenv("PERSIST_ROOT", "/data"))
DATA.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA / "buyback.db"
IGN_FILE = DATA / "ign_registry.json"

# =========================
# OAUTH HELPER (refresh token -> access token)
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
        # reuse token if still valid (60s buffer)
        if self._access_token and time.time() < (self._expires_at - 60):
            return self._access_token

        basic = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode("utf-8")
        ).decode("ascii")

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
    JANICE_MARKET = "jita"
    JANICE_PRICING = "buy"
    # ==========================================

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.oauth = EveOAuth()
        self.session = aiohttp.ClientSession()

        self.db = sqlite3.connect(DB_PATH)
        self.db.row_factory = sqlite3.Row

        self.init_db()

    def cog_unload(self):
        try:
            if self.session and not self.session.closed:
                self.bot.loop.create_task(self.session.close())
        except Exception:
            pass
        try:
            self.db.close()
        except Exception:
            pass

    # ================= DB =================

    def _table_columns(self, table: str) -> List[str]:
        rows = self.db.execute(f"PRAGMA table_info({table})").fetchall()
        return [r["name"] for r in rows]

    def _migrate_contracts_if_needed(self) -> None:
        """
        You previously had a 7-column contracts table:
          (contract_id, ign, discord_id, status, total, payout, ts)

        This cog uses an 8-column table:
          (contract_id, issuer_char_id, issuer_name, discord_id, status, total, payout, ts)

        This migrates old -> new without losing data.
        """
        existing = self.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='contracts'"
        ).fetchone()
        if not existing:
            return

        cols = self._table_columns("contracts")
        if cols == ["contract_id", "issuer_char_id", "issuer_name", "discord_id", "status", "total", "payout", "ts"]:
            return  # already correct

        print(f"[BUYBACK] Migrating contracts table. Old columns: {cols}")

        self.db.execute("ALTER TABLE contracts RENAME TO contracts_old")

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

        old_cols = self._table_columns("contracts_old")

        if old_cols == ["contract_id", "ign", "discord_id", "status", "total", "payout", "ts"]:
            self.db.execute("""
            INSERT INTO contracts (contract_id, issuer_char_id, issuer_name, discord_id, status, total, payout, ts)
            SELECT contract_id, NULL, ign, discord_id, status, total, payout, ts
            FROM contracts_old
            """)
        else:
            common = set(old_cols)
            issuer_name_expr = "issuer_name" if "issuer_name" in common else ("ign" if "ign" in common else "NULL")
            issuer_char_expr = "issuer_char_id" if "issuer_char_id" in common else "NULL"
            discord_expr = "discord_id" if "discord_id" in common else "NULL"
            status_expr = "status" if "status" in common else "'UNKNOWN'"
            total_expr = "total" if "total" in common else "0"
            payout_expr = "payout" if "payout" in common else "0"
            ts_expr = "ts" if "ts" in common else "NULL"

            self.db.execute(f"""
            INSERT INTO contracts (contract_id, issuer_char_id, issuer_name, discord_id, status, total, payout, ts)
            SELECT contract_id, {issuer_char_expr}, {issuer_name_expr}, {discord_expr}, {status_expr}, {total_expr}, {payout_expr}, {ts_expr}
            FROM contracts_old
            """)

        self.db.execute("DROP TABLE contracts_old")
        self.db.commit()
        print("[BUYBACK] Migration complete.")

    def init_db(self):
        self._migrate_contracts_if_needed()

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

    def upsert_contract(
        self,
        *,
        contract_id: int,
        issuer_char_id: Optional[int],
        issuer_name: Optional[str],
        discord_id: Optional[int],
        status: str,
        total: float,
        payout: float,
        ts: Optional[str] = None
    ) -> None:
        self.db.execute(
            """
            INSERT OR REPLACE INTO contracts
              (contract_id, issuer_char_id, issuer_name, discord_id, status, total, payout, ts)
            VALUES
              (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                contract_id,
                issuer_char_id,
                issuer_name,
                discord_id,
                status,
                float(total),
                float(payout),
                ts or datetime.utcnow().isoformat(),
            ),
        )
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
            for x in rec.get("igns", []):
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

            if self.db.execute("SELECT 1 FROM contracts WHERE contract_id=?", (cid,)).fetchone():
                skipped += 1
                continue

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

        async with self.session.get(
            f"{self.ESI}/corporations/{self.CORP_ID}/contracts/{cid}/items/",
            headers=headers
        ) as r:
            text = await r.text()
            if r.status != 200:
                print(f"[BUYBACK] Failed items for {cid}: {r.status} {text[:500]}")
                self.upsert_contract(
                    contract_id=cid,
                    issuer_char_id=issuer_char_id,
                    issuer_name=issuer_name,
                    discord_id=discord_id,
                    status="ITEMS_FAILED",
                    total=0,
                    payout=0,
                )
                return False
            items = json.loads(text)

        # Build Janice lines: "quantity ItemName" per line
        janice_lines: List[str] = []
        display_lines: List[str] = []

        for it in items:
            qty = int(it.get("quantity", 0))
            type_id = int(it.get("type_id", 0))
            if qty <= 0 or type_id <= 0:
                continue

            name = await self.type_name(type_id)

            # Janice accepts: "quantity Item Name"
            janice_lines.append(f"{qty} {name}")
            display_lines.append(f"{qty:,} × {name}")

        # -------------------------
        # Janice payload hardening
        # -------------------------
        safe_lines: List[str] = []
        for line in janice_lines:
            line = (line or "").replace("\r", " ").replace("\n", " ").strip()
            if line:
                safe_lines.append(line)

        if not safe_lines:
            print(f"[BUYBACK] No valid items in contract {cid}")
            self.upsert_contract(
                contract_id=cid,
                issuer_char_id=issuer_char_id,
                issuer_name=issuer_name,
                discord_id=discord_id,
                status="EMPTY_ITEMS",
                total=0,
                payout=0,
            )
            return False

        # -------------------------------
        # JANICE APPRAISAL (FORM first, retry JSON on 400)
        # -------------------------------
        form = {
            "market": self.JANICE_MARKET,
            "pricing": self.JANICE_PRICING,
            "items": "\n".join(safe_lines),
        }

        base_headers = {
            "Accept": "application/json",
            "User-Agent": "ARC-Buyback-Bot",
        }

        # Try FORM (x-www-form-urlencoded) first
        status = 0
        janice_text = ""
        form_headers = dict(base_headers)
        form_headers["Content-Type"] = "application/x-www-form-urlencoded"

        async with self.session.post(self.JANICE, data=form, headers=form_headers) as r1:
            janice_text = await r1.text()
            status = r1.status

        # If 400, retry JSON (some configs expect JSON)
        if status == 400:
            async with self.session.post(self.JANICE, json=form, headers=base_headers) as r2:
                janice_text = await r2.text()
                status = r2.status

        if status != 200:
            print(f"[BUYBACK] Janice error for {cid}: {status}\n{(janice_text or '')[:1500]}")
            self.upsert_contract(
                contract_id=cid,
                issuer_char_id=issuer_char_id,
                issuer_name=issuer_name,
                discord_id=discord_id,
                status="JANICE_FAILED",
                total=0,
                payout=0,
            )

            ch = self.get_channel()
            if ch:
                emb = discord.Embed(
                    title="❌ Buyback Contract — Pricing Failed",
                    description=f"Janice returned HTTP **{status}**.\nSaved as `JANICE_FAILED` so it won't re-loop.",
                    color=discord.Color.red(),
                    timestamp=datetime.utcnow(),
                )
                emb.add_field(name="Contract ID", value=str(cid), inline=True)
                emb.add_field(name="Issuer (IGN)", value=issuer_name, inline=True)
                emb.add_field(name="Discord", value=f"<@{discord_id}>" if discord_id else "Not found", inline=False)

                snippet = (janice_text or "").strip() or "(empty response body)"
                if len(snippet) > 900:
                    snippet = snippet[:900] + "…"
                emb.add_field(name="Janice response (snippet)", value=f"```{snippet}```", inline=False)

                await ch.send(embed=emb)

            return False

        # Parse JSON (Janice sometimes returns 200 but body isn't valid JSON behind proxies)
        try:
            appraisal = json.loads(janice_text)
        except Exception:
            print(f"[BUYBACK] Janice returned non-JSON for {cid}: {(janice_text or '')[:500]}")
            self.upsert_contract(
                contract_id=cid,
                issuer_char_id=issuer_char_id,
                issuer_name=issuer_name,
                discord_id=discord_id,
                status="JANICE_BAD_JSON",
                total=0,
                payout=0,
            )
            return False

        # Extract total
        try:
            total = float(appraisal["effective_prices"]["total"])
        except Exception:
            print(f"[BUYBACK] Unexpected Janice JSON keys for {cid}: {list(appraisal.keys())[:30]}")
            self.upsert_contract(
                contract_id=cid,
                issuer_char_id=issuer_char_id,
                issuer_name=issuer_name,
                discord_id=discord_id,
                status="JANICE_SHAPE_UNKNOWN",
                total=0,
                payout=0,
            )
            return False

        payout = total * self.BUYBACK_RATE

        self.upsert_contract(
            contract_id=cid,
            issuer_char_id=issuer_char_id,
            issuer_name=issuer_name,
            discord_id=discord_id,
            status="PRICED",
            total=total,
            payout=payout,
        )

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
            description="Priced at **Jita 4-4 Buy** via Janice. Payout is **80%**.",
        )
        emb.add_field(name="Contract ID", value=str(cid), inline=True)
        emb.add_field(name="IGN (pay this character)", value=issuer_name, inline=True)
        emb.add_field(name="Discord", value=ping, inline=False)
        emb.add_field(name="Jita Buy Total", value=f"{total:,.0f} ISK", inline=True)
        emb.add_field(name="80% Payout", value=f"{payout:,.0f} ISK", inline=True)

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