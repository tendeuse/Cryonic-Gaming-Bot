import os
import json
import time
import base64
import sqlite3
import aiohttp
import asyncio
import discord
from discord.ext import commands
from discord import app_commands
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

# =========================
# PATHS (Railway persistent volume)
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
            raise RuntimeError(
                "Missing EVE_CLIENT_ID / EVE_CLIENT_SECRET / EVE_REFRESH_TOKEN in environment variables."
            )

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
        data = {"grant_type": "refresh_token", "refresh_token": self.refresh_token}

        async with session.post(
            "https://login.eveonline.com/v2/oauth/token", headers=headers, data=data
        ) as resp:
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

    # --- Contract pagination cutoff ---
    CONTRACT_LOOKBACK_DAYS = 15  # stop paging once contracts are older than this many days

    # --- ESI MARKET PRICING (Janice-like) ---
    JITA_4_4_LOCATION_ID = 60003760
    THE_FORGE_REGION_ID = 10000002

    MARKET_CACHE_TTL = 600  # seconds
    MARKET_CONCURRENCY = 6

    OUTLIER_LOW_FACTOR = 0.70
    OUTLIER_HIGH_FACTOR = 1.50
    TOP_BOOK_SAMPLE = 30
    # ----------------------------------------

    RETRYABLE_STATUSES = {
        "ESI_PRICE_FAILED",
        "ITEMS_FAILED",
        "EMPTY_ITEMS",
        "ESI_PRICE_NO_ORDERS",
    }
    # ==========================================

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.oauth = EveOAuth()
        self.session = aiohttp.ClientSession()

        self.db = sqlite3.connect(DB_PATH)
        self.db.row_factory = sqlite3.Row

        self._market_sem = asyncio.Semaphore(self.MARKET_CONCURRENCY)
        self._market_cache: Dict[int, Dict[str, Any]] = {}  # type_id -> {ts, orders:[{price, vol, minv}]}

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

    async def maybe_convert_to_compressed(self, type_id: int, name: str) -> Tuple[int, str]:
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

    @staticmethod
    def _parse_esi_dt(s: Optional[str]) -> Optional[datetime]:
        """
        ESI timestamps are ISO8601, typically ending with 'Z'.
        Returns timezone-aware UTC datetime when possible.
        """
        if not s:
            return None
        try:
            ss = s.strip()
            if ss.endswith("Z"):
                ss = ss[:-1] + "+00:00"
            dt = datetime.fromisoformat(ss)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    # ================= ESI CONTRACTS (PAGINATED) =================
    async def fetch_corp_contracts(self) -> List[Dict[str, Any]]:
        """
        Corp contracts endpoint is PAGINATED.

        Added behavior:
        - Stop paging once we reach contracts older than CONTRACT_LOOKBACK_DAYS (based on `date_issued`).
        - We still include any contracts on the current page that are within the lookback window.
        """
        headers = await self.esi_headers()

        cutoff = datetime.now(timezone.utc) - timedelta(days=int(self.CONTRACT_LOOKBACK_DAYS))

        all_contracts: List[Dict[str, Any]] = []
        page = 1

        while True:
            async with self.session.get(
                f"{self.ESI}/corporations/{self.CORP_ID}/contracts/",
                headers=headers,
                params={"page": page},
            ) as r:
                body = await r.text()
                if r.status != 200:
                    raise RuntimeError(f"ESI error {r.status} (page {page}): {body[:1200]}")

                try:
                    data = json.loads(body)
                except Exception:
                    raise RuntimeError(f"ESI returned non-JSON (page {page}): {body[:400]}")

                if not isinstance(data, list) or len(data) == 0:
                    break

                # Keep only contracts within the lookback window, but use "older encountered" to stop paging.
                older_encountered = False
                for c in data:
                    dt = self._parse_esi_dt(c.get("date_issued"))
                    if dt is None:
                        # If we can't parse date, keep it (safer) and don't use it to stop paging.
                        all_contracts.append(c)
                        continue

                    if dt < cutoff:
                        older_encountered = True
                        # Do not include old contracts (beyond lookback)
                        continue

                    all_contracts.append(c)

                # If this page contained any contracts older than cutoff, and ESI is sorted newest->oldest,
                # then subsequent pages will be even older: stop paging now.
                if older_encountered:
                    break

                try:
                    x_pages = int(r.headers.get("X-Pages", "1"))
                except Exception:
                    x_pages = 1

                if page >= x_pages:
                    break

                page += 1

        return all_contracts

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

    # ================= MARKET PRICING (ESI) =================
    @staticmethod
    def _median(nums: List[float]) -> Optional[float]:
        if not nums:
            return None
        s = sorted(nums)
        n = len(s)
        mid = n // 2
        if n % 2 == 1:
            return float(s[mid])
        return (float(s[mid - 1]) + float(s[mid])) / 2.0

    async def _esi_get_json_with_backoff(self, url: str, params: Dict[str, Any]) -> Tuple[int, Any, Dict[str, str]]:
        backoff = 1.0
        for _ in range(6):
            async with self.session.get(
                url,
                params=params,
                headers={"Accept": "application/json", "User-Agent": "ARC-Buyback-Bot"},
            ) as r:
                status = r.status
                hdrs = {k: v for k, v in r.headers.items()}
                if status in (420, 429):
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2.0, 16.0)
                    continue
                if status != 200:
                    text = await r.text()
                    return status, text, hdrs
                try:
                    return status, await r.json(), hdrs
                except Exception:
                    text = await r.text()
                    return 200, text, hdrs
        return 429, "Rate limit/backoff exhausted", {}

    async def _fetch_jita_buy_orders(self, type_id: int) -> List[Dict[str, Any]]:
        now = time.time()
        cached = self._market_cache.get(type_id)
        if cached and (now - float(cached.get("ts", 0))) < self.MARKET_CACHE_TTL:
            return cached.get("orders", [])

        async with self._market_sem:
            now = time.time()
            cached = self._market_cache.get(type_id)
            if cached and (now - float(cached.get("ts", 0))) < self.MARKET_CACHE_TTL:
                return cached.get("orders", [])

            url = f"{self.ESI}/markets/{self.THE_FORGE_REGION_ID}/orders/"
            page = 1
            all_orders: List[Dict[str, Any]] = []

            while True:
                params = {"order_type": "buy", "type_id": int(type_id), "page": page}
                status, data, hdrs = await self._esi_get_json_with_backoff(url, params)

                if status != 200:
                    raise RuntimeError(f"ESI market orders HTTP {status}: {str(data)[:600]}")

                if not isinstance(data, list) or len(data) == 0:
                    break

                for o in data:
                    try:
                        if int(o.get("location_id", 0)) != int(self.JITA_4_4_LOCATION_ID):
                            continue
                        v_rem = int(o.get("volume_remain", 0))
                        if v_rem <= 0:
                            continue
                        all_orders.append(o)
                    except Exception:
                        continue

                try:
                    xpages = int(hdrs.get("X-Pages", "1"))
                except Exception:
                    xpages = 1
                if page >= xpages:
                    break
                page += 1

            norm: List[Dict[str, Any]] = []
            for o in all_orders:
                try:
                    norm.append({
                        "price": float(o.get("price", 0.0)),
                        "vol": int(o.get("volume_remain", 0)),
                        "minv": int(o.get("min_volume", 1)),
                    })
                except Exception:
                    continue

            norm.sort(key=lambda x: x["price"], reverse=True)
            self._market_cache[type_id] = {"ts": time.time(), "orders": norm}
            return norm

    def _filter_orders_janice_like(self, orders: List[Dict[str, Any]], qty_needed: int) -> List[Dict[str, Any]]:
        if not orders:
            return []

        qty_needed = max(1, int(qty_needed))
        tiny_threshold = max(1, min(1000, int(qty_needed * 0.01)))

        candidates = []
        for o in orders:
            if int(o.get("vol", 0)) < tiny_threshold:
                continue
            if float(o.get("price", 0.0)) <= 0:
                continue
            candidates.append(o)

        if not candidates:
            candidates = orders[:]

        sample = candidates[: self.TOP_BOOK_SAMPLE]
        med = self._median([float(o["price"]) for o in sample if float(o.get("price", 0)) > 0])
        if not med or med <= 0:
            return candidates

        low_cut = med * float(self.OUTLIER_LOW_FACTOR)
        high_cut = med * float(self.OUTLIER_HIGH_FACTOR)
        filtered = [o for o in candidates if (low_cut <= float(o["price"]) <= high_cut)]
        return filtered if filtered else candidates

    async def price_jita_buy_immediate(self, type_id: int, qty: int) -> Tuple[float, float, str]:
        qty = max(0, int(qty))
        if qty <= 0:
            return 0.0, 0.0, "qty<=0"

        orders = await self._fetch_jita_buy_orders(type_id)
        if not orders:
            return 0.0, 0.0, "no Jita 4-4 buy orders"

        orders = self._filter_orders_janice_like(orders, qty)

        remaining = qty
        total = 0.0
        filled = 0

        best_price = float(orders[0]["price"]) if orders else 0.0

        for o in orders:
            if remaining <= 0:
                break
            price = float(o["price"])
            vol = int(o["vol"])
            minv = int(o.get("minv", 1))

            if price <= 0 or vol <= 0:
                continue
            if remaining < minv:
                continue

            take = min(remaining, vol)
            total += take * price
            remaining -= take
            filled += take

        if remaining > 0 and best_price > 0:
            total += remaining * best_price
            filled += remaining
            remaining = 0

        unit_eff = (total / qty) if qty > 0 else 0.0
        note = f"filled={filled}/{qty} (remainder@best if needed)"
        return unit_eff, total, note

    # ================= SLASH COMMANDS =================
    @app_commands.command(name="buyback", description="Scan OUTSTANDING buyback contracts once (manual)")
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

        found_outstanding: List[Tuple[int, str]] = []

        # Diagnostics (helpful if filters fail again)
        diag = {
            "total_returned": len(contracts),
            "status_outstanding": 0,
            "type_item_exchange": 0,
            "assignee_match": 0,
            "start_location_match": 0,
        }

        sample_activeish: List[str] = []

        for c in contracts:
            status = (c.get("status") or "").strip().lower()
            ctype = (c.get("type") or "").strip().lower()

            if len(sample_activeish) < 12 and status in ("outstanding", "in_progress"):
                try:
                    sample_activeish.append(
                        f"{int(c.get('contract_id', 0))} | status={status} | type={ctype} | "
                        f"assignee={int(c.get('assignee_id', 0))} | start_loc={int(c.get('start_location_id', 0))}"
                    )
                except Exception:
                    pass

            if status != "outstanding":
                continue
            diag["status_outstanding"] += 1

            try:
                cid = int(c["contract_id"])
            except Exception:
                continue

            if ctype != "item_exchange":
                continue
            diag["type_item_exchange"] += 1

            if int(c.get("assignee_id", 0)) != int(self.BUYBACK_CHARACTER_ID):
                continue
            diag["assignee_match"] += 1

            if int(c.get("start_location_id", 0)) != int(self.AT1_STRUCTURE_ID):
                continue
            diag["start_location_match"] += 1

            row = self.get_contract_row(cid)
            if row:
                found_outstanding.append((cid, (row["status"] or "UNKNOWN")))
            else:
                found_outstanding.append((cid, "NOT_IN_DB"))

            # process new or retryable
            if row:
                if self.should_retry_existing(row):
                    _ = await self.process_contract(c, force=True)
                    retried += 1
                else:
                    skipped += 1
                continue

            ok = await self.process_contract(c)
            if ok:
                new += 1

        if not found_outstanding:
            msg = (
                "✅ No **OUTSTANDING** buyback contracts found (matching type/assignee/location).\n\n"
                "**Diagnostics:**\n"
                f"- Total contracts returned (≤{self.CONTRACT_LOOKBACK_DAYS}d lookback): `{diag['total_returned']}`\n"
                f"- Status outstanding: `{diag['status_outstanding']}`\n"
                f"- Outstanding + item_exchange: `{diag['type_item_exchange']}`\n"
                f"- + assignee match ({self.BUYBACK_CHARACTER_ID}): `{diag['assignee_match']}`\n"
                f"- + start_location match ({self.AT1_STRUCTURE_ID}): `{diag['start_location_match']}`\n\n"
                "**Sample (active-ish contracts ESI returned):**\n"
            )
            if sample_activeish:
                msg += "```" + "\n".join(sample_activeish) + "```"
            else:
                msg += "_No outstanding/in_progress sample contracts were returned by ESI._"

            await interaction.followup.send(msg, ephemeral=True)
            return

        found_outstanding.sort(key=lambda x: x[0], reverse=True)

        lines = [f"{cid}  —  {st}" for cid, st in found_outstanding[:25]]
        more = f"\n… and {len(found_outstanding) - 25} more." if len(found_outstanding) > 25 else ""

        await interaction.followup.send(
            "📌 **Outstanding Buyback Contracts (matching filters)**\n"
            f"```{chr(10).join(lines)}{more}```\n"
            f"✅ Scan complete — {new} new, {retried} retried failed, {skipped} skipped (already handled).",
            ephemeral=True
        )

    @app_commands.command(name="buyback_retry", description="Retry a specific contract ID (pricing/ESI failures)")
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
            await interaction.followup.send(
                f"⚠️ Retried contract {contract_id}, but it still failed. Check the payout channel for the error embed.",
                ephemeral=True
            )

    @app_commands.command(name="buyback_retry_failed", description="Retry the most recent failed contracts saved in the DB")
    @app_commands.checks.has_role(APPROVER_ROLE)
    @app_commands.describe(limit="How many failed contracts to retry (default 10)")
    async def buyback_retry_failed(self, interaction: discord.Interaction, limit: int = 10):
        await interaction.response.defer(ephemeral=True)

        limit = max(1, min(50, int(limit)))

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
                    janice_http=r.status,
                    janice_snippet=f"ESI items HTTP {r.status}: {text[:900]}",
                )
                return False

            try:
                items = json.loads(text)
            except Exception:
                items = []

        display_lines: List[str] = []
        price_debug_lines: List[str] = []
        total = 0.0

        for it in items:
            qty = int(it.get("quantity", 0))
            type_id = int(it.get("type_id", 0))
            if qty <= 0 or type_id <= 0:
                continue

            name = await self.type_name(type_id)
            type_id, name = await self.maybe_convert_to_compressed(type_id, name)

            try:
                unit_eff, line_total, note = await self.price_jita_buy_immediate(type_id, qty)
            except Exception as e:
                msg = f"Pricing failed for {name} (type_id={type_id}) qty={qty}: {e}"
                print(f"[BUYBACK] {msg}")

                self.upsert_contract(
                    contract_id=cid,
                    issuer_char_id=issuer_char_id,
                    issuer_name=issuer_name,
                    discord_id=discord_id,
                    status="ESI_PRICE_FAILED",
                    total=0,
                    payout=0,
                    janice_http=None,
                    janice_snippet=msg[:900],
                )

                ch = self.get_channel()
                if ch:
                    emb = discord.Embed(
                        title="❌ Buyback Contract — Pricing Failed",
                        description=(
                            "ESI market pricing failed while calculating Jita 4-4 buy.\n"
                            "Saved as `ESI_PRICE_FAILED`.\n"
                            f"Use `/buyback_retry {cid}` after ESI recovers."
                        ),
                        color=discord.Color.red(),
                        timestamp=datetime.utcnow(),
                    )
                    emb.add_field(name="Contract ID", value=str(cid), inline=True)
                    emb.add_field(name="Issuer (IGN)", value=issuer_name, inline=True)
                    emb.add_field(name="Discord", value=f"<@{discord_id}>" if discord_id else "Not found", inline=False)
                    emb.add_field(name="Error", value=f"```{msg[:900]}```", inline=False)
                    await ch.send(embed=emb)
                return False

            if line_total <= 0:
                price_debug_lines.append(f"{name}: 0 ISK ({note})")
            else:
                price_debug_lines.append(f"{name}: {unit_eff:,.2f}/u ({note})")

            total += float(line_total)
            display_lines.append(f"{qty:,} × {name}")

        if not display_lines:
            self.upsert_contract(
                contract_id=cid,
                issuer_char_id=issuer_char_id,
                issuer_name=issuer_name,
                discord_id=discord_id,
                status="EMPTY_ITEMS",
                total=0,
                payout=0,
                janice_http=None,
                janice_snippet="No valid items returned by ESI items endpoint.",
            )
            return False

        payout = total * float(self.BUYBACK_RATE)

        # Record success
        self.upsert_contract(
            contract_id=cid,
            issuer_char_id=issuer_char_id,
            issuer_name=issuer_name,
            discord_id=discord_id,
            status="PRICED",
            total=total,
            payout=payout,
            janice_http=None,
            janice_snippet=None,
        )

        # Send payout embed
        ch = self.get_channel()
        if ch:
            emb = discord.Embed(
                title="✅ Buyback Contract — Priced",
                description="Contract priced using ESI Jita 4-4 buy orders.",
                color=discord.Color.green(),
                timestamp=datetime.utcnow(),
            )
            emb.add_field(name="Contract ID", value=str(cid), inline=True)
            emb.add_field(name="Issuer (IGN)", value=issuer_name, inline=True)
            emb.add_field(name="Discord", value=f"<@{discord_id}>" if discord_id else "Not found", inline=False)
            emb.add_field(name="Items", value="```" + "\n".join(display_lines)[:1000] + "```", inline=False)
            emb.add_field(name="Total (Jita Buy)", value=f"{total:,.2f} ISK", inline=True)
            emb.add_field(name=f"Payout ({int(self.BUYBACK_RATE*100)}%)", value=f"{payout:,.2f} ISK", inline=True)
            emb.add_field(name="Price Notes", value="```" + "\n".join(price_debug_lines)[:1000] + "```", inline=False)
            await ch.send(embed=emb)

        return True


async def setup(bot: commands.Bot):
    await bot.add_cog(BuybackAuto(bot))