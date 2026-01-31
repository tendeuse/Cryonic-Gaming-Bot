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

    # ---- JANICE v2 (text/plain) ----
    JANICE = "https://janice.e-351.com/api/rest/v2/appraisal"
    JANICE_MARKET_ID = 2               # 2 = Jita (per Janice API)
    JANICE_PRICING = "buy"             # buy/sell/split/purchase
    JANICE_PRICING_VARIANT = "immediate"  # immediate/top5percent
    JANICE_API_KEY = os.getenv("JANICE_API_KEY")  # REQUIRED (X-ApiKey)
    # -------------------------------

    # retryable statuses
    RETRYABLE_STATUSES = {
        "JANICE_FAILED",
        "JANICE_BAD_JSON",
        "JANICE_SHAPE_UNKNOWN",
        "ITEMS_FAILED",
        "EMPTY_ITEMS",
    }
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
        Target columns:
          (contract_id, issuer_char_id, issuer_name, discord_id, status, total, payout, ts, janice_http, janice_snippet)
        """
        existing = self.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='contracts'"
        ).fetchone()
        if not existing:
            return

        cols = self._table_columns("contracts")
        target = [
            "contract_id",
            "issuer_char_id",
            "issuer_name",
            "discord_id",
            "status",
            "total",
            "payout",
            "ts",
            "janice_http",
            "janice_snippet",
        ]
        if cols == target:
            return

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
            ts TEXT,
            janice_http INTEGER,
            janice_snippet TEXT
        )
        """)

        old_cols = self._table_columns("contracts_old")
        common = set(old_cols)

        issuer_name_expr = "issuer_name" if "issuer_name" in common else ("ign" if "ign" in common else "NULL")
        issuer_char_expr = "issuer_char_id" if "issuer_char_id" in common else "NULL"
        discord_expr = "discord_id" if "discord_id" in common else "NULL"
        status_expr = "status" if "status" in common else "'UNKNOWN'"
        total_expr = "total" if "total" in common else "0"
        payout_expr = "payout" if "payout" in common else "0"
        ts_expr = "ts" if "ts" in common else "NULL"
        janice_http_expr = "janice_http" if "janice_http" in common else "NULL"
        janice_snippet_expr = "janice_snippet" if "janice_snippet" in common else "NULL"

        self.db.execute(f"""
        INSERT INTO contracts (
            contract_id, issuer_char_id, issuer_name, discord_id,
            status, total, payout, ts, janice_http, janice_snippet
        )
        SELECT
            contract_id, {issuer_char_expr}, {issuer_name_expr}, {discord_expr},
            {status_expr}, {total_expr}, {payout_expr}, {ts_expr}, {janice_http_expr}, {janice_snippet_expr}
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
            ts TEXT,
            janice_http INTEGER,
            janice_snippet TEXT
        )
        """)

        self.db.execute("""
        CREATE TABLE IF NOT EXISTS type_cache (
            type_id INTEGER PRIMARY KEY,
            name TEXT
        )
        """)

        # cache mapping original ore type_id -> compressed type_id + name (best-effort)
        self.db.execute("""
        CREATE TABLE IF NOT EXISTS compress_cache (
            original_type_id INTEGER PRIMARY KEY,
            compressed_type_id INTEGER,
            compressed_name TEXT
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
        ts: Optional[str] = None,
        janice_http: Optional[int] = None,
        janice_snippet: Optional[str] = None
    ) -> None:
        self.db.execute(
            """
            INSERT OR REPLACE INTO contracts
              (contract_id, issuer_char_id, issuer_name, discord_id, status, total, payout, ts, janice_http, janice_snippet)
            VALUES
              (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                janice_http,
                janice_snippet,
            ),
        )
        self.db.commit()

    def get_contract_row(self, contract_id: int) -> Optional[sqlite3.Row]:
        return self.db.execute(
            "SELECT * FROM contracts WHERE contract_id=?",
            (contract_id,)
        ).fetchone()

    def should_retry_existing(self, row: sqlite3.Row) -> bool:
        status = (row["status"] or "").strip().upper()
        return status in self.RETRYABLE_STATUSES

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

    async def _resolve_type_id_by_name(self, name: str) -> Optional[int]:
        """
        ESI /universe/ids/ resolves names to IDs; we use it to find 'Compressed {name}'
        """
        if not name:
            return None
        url = f"{self.ESI}/universe/ids/"
        payload = [name]
        async with self.session.post(url, json=payload) as r:
            if r.status != 200:
                return None
            data = await r.json()

        inv = data.get("inventory_types") or []
        for rec in inv:
            if (rec.get("name") or "").strip() == name:
                try:
                    return int(rec.get("id"))
                except Exception:
                    return None
        return None

    async def maybe_convert_to_compressed(self, type_id: int, name: str) -> tuple[int, str]:
        """
        If 'Compressed {name}' exists as a type, convert to it.
        Otherwise return original. Cached.
        """
        if not type_id or not name:
            return type_id, name
        if name.startswith("Compressed "):
            return type_id, name

        row = self.db.execute(
            "SELECT compressed_type_id, compressed_name FROM compress_cache WHERE original_type_id=?",
            (type_id,)
        ).fetchone()
        if row:
            ctid = row["compressed_type_id"]
            cname = row["compressed_name"]
            if ctid and cname:
                return int(ctid), str(cname)
            return type_id, name

        candidate = f"Compressed {name}"
        ctid = await self._resolve_type_id_by_name(candidate)

        if ctid:
            self.db.execute(
                "INSERT OR REPLACE INTO compress_cache(original_type_id, compressed_type_id, compressed_name) VALUES (?, ?, ?)",
                (type_id, int(ctid), candidate)
            )
            self.db.execute(
                "INSERT OR IGNORE INTO type_cache(type_id, name) VALUES (?, ?)",
                (int(ctid), candidate)
            )
            self.db.commit()
            return int(ctid), candidate

        self.db.execute(
            "INSERT OR REPLACE INTO compress_cache(original_type_id, compressed_type_id, compressed_name) VALUES (?, NULL, NULL)",
            (type_id,)
        )
        self.db.commit()
        return type_id, name

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

    async def fetch_corp_contracts(self) -> List[Dict[str, Any]]:
        headers = await self.esi_headers()
        async with self.session.get(
            f"{self.ESI}/corporations/{self.CORP_ID}/contracts/",
            headers=headers
        ) as r:
            body = await r.text()
            if r.status != 200:
                raise RuntimeError(f"ESI error {r.status}: {body[:1200]}")
            return json.loads(body)

    async def find_contract_in_esi(self, contract_id: int) -> Optional[Dict[str, Any]]:
        try:
            contracts = await self.fetch_corp_contracts()
        except Exception as e:
            print(f"[BUYBACK] find_contract_in_esi failed: {e}")
            return None

        for c in contracts:
            try:
                if int(c.get("contract_id", 0)) == int(contract_id):
                    return c
            except Exception:
                continue
        return None

    # ================= SLASH COMMANDS =================

    @app_commands.command(name="buyback", description="Scan buyback contracts once (manual)")
    @app_commands.checks.has_role(APPROVER_ROLE)
    async def buyback_scan(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        try:
            contracts = await self.fetch_corp_contracts()
        except Exception as e:
            await interaction.followup.send(f"❌ {e}", ephemeral=True)
            return

        new = 0
        retried = 0
        skipped = 0

        for c in contracts:
            try:
                cid = int(c["contract_id"])
            except Exception:
                continue

            if c.get("type") != "item_exchange":
                continue
            if int(c.get("assignee_id", 0)) != self.BUYBACK_CHARACTER_ID:
                continue
            if int(c.get("start_location_id", 0)) != self.AT1_STRUCTURE_ID:
                continue

            row = self.get_contract_row(cid)
            if row:
                if self.should_retry_existing(row):
                    ok = await self.process_contract(c, force=True)
                    retried += 1
                    if ok:
                        pass
                else:
                    skipped += 1
                continue

            ok = await self.process_contract(c)
            if ok:
                new += 1

        await interaction.followup.send(
            f"✅ Buyback scan complete — {new} new, {retried} retried failed, {skipped} skipped (already handled).",
            ephemeral=True
        )

    @app_commands.command(name="buyback_retry", description="Retry a specific contract ID (Janice/ESI failures)")
    @app_commands.checks.has_role(APPROVER_ROLE)
    @app_commands.describe(contract_id="The EVE contract ID to retry")
    async def buyback_retry(self, interaction: discord.Interaction, contract_id: int):
        await interaction.response.defer(ephemeral=True)

        c = await self.find_contract_in_esi(contract_id)
        if not c:
            await interaction.followup.send(
                "❌ I couldn't find that contract in the corporation contracts list (it may be too old or not visible via this endpoint).",
                ephemeral=True
            )
            return

        ok = await self.process_contract(c, force=True)
        if ok:
            await interaction.followup.send(f"✅ Retried contract {contract_id} successfully.", ephemeral=True)
        else:
            await interaction.followup.send(f"⚠️ Retried contract {contract_id}, but it still failed. Check the payout channel for the error embed.", ephemeral=True)

    @app_commands.command(name="buyback_retry_failed", description="Retry the most recent failed contracts saved in the DB")
    @app_commands.checks.has_role(APPROVER_ROLE)
    @app_commands.describe(limit="How many failed contracts to retry (default 10)")
    async def buyback_retry_failed(self, interaction: discord.Interaction, limit: int = 10):
        await interaction.response.defer(ephemeral=True)

        if limit < 1:
            limit = 1
        if limit > 50:
            limit = 50

        rows = self.db.execute(
            """
            SELECT contract_id, status, ts
            FROM contracts
            WHERE UPPER(status) IN ({})
            ORDER BY ts DESC
            LIMIT ?
            """.format(",".join("?" for _ in self.RETRYABLE_STATUSES)),
            (*[s.upper() for s in self.RETRYABLE_STATUSES], limit)
        ).fetchall()

        if not rows:
            await interaction.followup.send("✅ No failed contracts found to retry.", ephemeral=True)
            return

        attempted = 0
        succeeded = 0
        not_found = 0

        for r in rows:
            cid = int(r["contract_id"])
            c = await self.find_contract_in_esi(cid)
            if not c:
                not_found += 1
                continue

            attempted += 1
            ok = await self.process_contract(c, force=True)
            if ok:
                succeeded += 1

        await interaction.followup.send(
            f"✅ Retry complete — attempted {attempted}, succeeded {succeeded}, not found in ESI list {not_found}.",
            ephemeral=True
        )

    # ================= PROCESS =================

    async def process_contract(self, c: Dict[str, Any], force: bool = False) -> bool:
        cid = int(c["contract_id"])

        existing = self.get_contract_row(cid)
        if existing and not force:
            return False

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
                    janice_http=None,
                    janice_snippet=f"ESI items HTTP {r.status}: {text[:900]}",
                )
                return False
            items = json.loads(text)

        # Build Janice lines as: "ItemName qty" (simple text)
        janice_lines: List[str] = []
        display_lines: List[str] = []

        for it in items:
            qty = int(it.get("quantity", 0))
            type_id = int(it.get("type_id", 0))
            if qty <= 0 or type_id <= 0:
                continue

            name = await self.type_name(type_id)

            # Convert ores to compressed variant if available
            type_id, name = await self.maybe_convert_to_compressed(type_id, name)

            janice_lines.append(f"{name} {qty}")
            display_lines.append(f"{qty:,} × {name}")

        if not janice_lines:
            self.upsert_contract(
                contract_id=cid,
                issuer_char_id=issuer_char_id,
                issuer_name=issuer_name,
                discord_id=discord_id,
                status="EMPTY_ITEMS",
                total=0,
                payout=0,
                janice_http=None,
                janice_snippet="No valid items after filtering (qty/type_id).",
            )
            return False

        # -------------------------------
        # JANICE v2 APPRAISAL (text/plain)
        # -------------------------------
        raw_text = "\n".join([ln.strip() for ln in janice_lines if (ln or "").strip()])

        params = {
            "market": str(self.JANICE_MARKET_ID),
            "pricing": self.JANICE_PRICING,
            "pricingVariant": self.JANICE_PRICING_VARIANT,
            "persist": "false",
            "compactize": "true",
        }

        j_headers = {
            "Accept": "application/json",
            "Content-Type": "text/plain",
            "User-Agent": "ARC-Buyback-Bot",
        }
        if self.JANICE_API_KEY:
            j_headers["X-ApiKey"] = self.JANICE_API_KEY

        async with self.session.post(
            self.JANICE,
            params=params,
            data=raw_text.encode("utf-8"),
            headers=j_headers
        ) as jr:
            janice_text = await jr.text()
            status = jr.status

        if status != 200:
            snippet = (janice_text or "").strip() or "(empty response body)"
            if len(snippet) > 900:
                snippet = snippet[:900] + "…"

            print(f"[BUYBACK] Janice error for {cid}: {status}\n{snippet}")
            self.upsert_contract(
                contract_id=cid,
                issuer_char_id=issuer_char_id,
                issuer_name=issuer_name,
                discord_id=discord_id,
                status="JANICE_FAILED",
                total=0,
                payout=0,
                janice_http=status,
                janice_snippet=snippet,
            )

            ch = self.get_channel()
            if ch:
                emb = discord.Embed(
                    title="❌ Buyback Contract — Pricing Failed",
                    description=(
                        f"Janice returned HTTP **{status}**.\n"
                        f"Saved as `JANICE_FAILED`.\n"
                        f"Use `/buyback_retry {cid}` after Janice is healthy again."
                    ),
                    color=discord.Color.red(),
                    timestamp=datetime.utcnow(),
                )
                emb.add_field(name="Contract ID", value=str(cid), inline=True)
                emb.add_field(name="Issuer (IGN)", value=issuer_name, inline=True)
                emb.add_field(name="Discord", value=f"<@{discord_id}>" if discord_id else "Not found", inline=False)
                emb.add_field(name="Janice response (snippet)", value=f"```{snippet}```", inline=False)
                await ch.send(embed=emb)

            return False

        # Parse JSON
        try:
            appraisal = json.loads(janice_text)
        except Exception:
            snippet = (janice_text or "").strip() or "(empty body)"
            if len(snippet) > 900:
                snippet = snippet[:900] + "…"
            self.upsert_contract(
                contract_id=cid,
                issuer_char_id=issuer_char_id,
                issuer_name=issuer_name,
                discord_id=discord_id,
                status="JANICE_BAD_JSON",
                total=0,
                payout=0,
                janice_http=200,
                janice_snippet=snippet,
            )
            return False

        # Janice v2 totals (pricing=buy => totalBuyPrice)
        try:
            effective = appraisal["effectivePrices"]
            total = float(effective["totalBuyPrice"])
        except Exception:
            self.upsert_contract(
                contract_id=cid,
                issuer_char_id=issuer_char_id,
                issuer_name=issuer_name,
                discord_id=discord_id,
                status="JANICE_SHAPE_UNKNOWN",
                total=0,
                payout=0,
                janice_http=200,
                janice_snippet=f"Unexpected JSON shape; keys={list(appraisal.keys())[:30]}",
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
            janice_http=200,
            janice_snippet=None,
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