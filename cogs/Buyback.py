# cogs/buyback_contracts.py
import os
import asyncio
import time
import json
import sqlite3
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any

import aiohttp
import discord
from discord.ext import commands, tasks
from discord import app_commands
from pathlib import Path

# =========================
# CONFIG (ENV)
# =========================
PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)

DB_PATH = PERSIST_ROOT / "buyback_contracts.db"
IGN_REGISTRY_PATH = PERSIST_ROOT / "ign_registry.json"

EVE_CLIENT_ID = os.getenv("EVE_CLIENT_ID", "")
EVE_CLIENT_SECRET = os.getenv("EVE_CLIENT_SECRET", "")
EVE_REFRESH_TOKEN = os.getenv("EVE_REFRESH_TOKEN", "")
EVE_CHARACTER_ID = int(os.getenv("EVE_CHARACTER_ID", "0"))

# Filters (buyback rules)
BUYBACK_ASSIGNEE_ID = int(os.getenv("BUYBACK_ASSIGNEE_ID", "2122848297"))
BUYBACK_START_LOCATION_ID = int(os.getenv("BUYBACK_START_LOCATION_ID", "1048840990158"))
BUYBACK_TYPE = os.getenv("BUYBACK_TYPE", "item_exchange")
BUYBACK_STATUS = os.getenv("BUYBACK_STATUS", "outstanding")

# Pricing
JITA_REGION_ID = int(os.getenv("JITA_REGION_ID", "10000002"))
JITA_LOCATION_ID = int(os.getenv("JITA_LOCATION_ID", "60003760"))
PAYOUT_MULTIPLIER = float(os.getenv("PAYOUT_MULTIPLIER", "0.8"))

# Discord output
BUYBACK_CHANNEL_NAME = os.getenv("BUYBACK_CHANNEL_NAME", "buyback-payout")
SCAN_INTERVAL_SECONDS = int(os.getenv("BUYBACK_SCAN_INTERVAL", "300"))
BUYBACK_AUTO_SCAN = os.getenv("BUYBACK_AUTO_SCAN", "0").strip().lower() in ("1", "true", "yes", "on")

# ESI
ESI_BASE = "https://esi.evetech.net/latest"
ESI_UA = os.getenv("ESI_USER_AGENT", "ARC Buyback Bot (discord)")

# Caches
TYPE_CACHE_TTL = 24 * 3600
PRICE_CACHE_TTL = 15 * 60
CHAR_NAME_CACHE_TTL = 24 * 3600

# =========================
# ORE -> COMPRESSED
# =========================
ORE_TO_COMPRESSED_NAME: Dict[str, str] = {
    "Veldspar": "Compressed Veldspar",
    "Concentrated Veldspar": "Compressed Concentrated Veldspar",
    "Dense Veldspar": "Compressed Dense Veldspar",
    "Scordite": "Compressed Scordite",
    "Condensed Scordite": "Compressed Condensed Scordite",
    "Massive Scordite": "Compressed Massive Scordite",
    "Pyroxeres": "Compressed Pyroxeres",
    "Solid Pyroxeres": "Compressed Solid Pyroxeres",
    "Viscous Pyroxeres": "Compressed Viscous Pyroxeres",
    "Plagioclase": "Compressed Plagioclase",
    "Azure Plagioclase": "Compressed Azure Plagioclase",
    "Rich Plagioclase": "Compressed Rich Plagioclase",
    "Omber": "Compressed Omber",
    "Silvery Omber": "Compressed Silvery Omber",
    "Golden Omber": "Compressed Golden Omber",
    "Kernite": "Compressed Kernite",
    "Luminous Kernite": "Compressed Luminous Kernite",
    "Fiery Kernite": "Compressed Fiery Kernite",
    "Jaspet": "Compressed Jaspet",
    "Pure Jaspet": "Compressed Pure Jaspet",
    "Pristine Jaspet": "Compressed Pristine Jaspet",
    "Hemorphite": "Compressed Hemorphite",
    "Vivid Hemorphite": "Compressed Vivid Hemorphite",
    "Radiant Hemorphite": "Compressed Radiant Hemorphite",
    "Hedbergite": "Compressed Hedbergite",
    "Vitric Hedbergite": "Compressed Vitric Hedbergite",
    "Glazed Hedbergite": "Compressed Glazed Hedbergite",
    "Gneiss": "Compressed Gneiss",
    "Iridescent Gneiss": "Compressed Iridescent Gneiss",
    "Prismatic Gneiss": "Compressed Prismatic Gneiss",
    "Dark Ochre": "Compressed Dark Ochre",
    "Onyx Ochre": "Compressed Onyx Ochre",
    "Obsidian Ochre": "Compressed Obsidian Ochre",
    "Spodumain": "Compressed Spodumain",
    "Bright Spodumain": "Compressed Bright Spodumain",
    "Gleaming Spodumain": "Compressed Gleaming Spodumain",
    "Crokite": "Compressed Crokite",
    "Sharp Crokite": "Compressed Sharp Crokite",
    "Crystalline Crokite": "Compressed Crystalline Crokite",
    "Bistot": "Compressed Bistot",
    "Triclinic Bistot": "Compressed Triclinic Bistot",
    "Monoclinic Bistot": "Compressed Monoclinic Bistot",
    "Arkonor": "Compressed Arkonor",
    "Crimson Arkonor": "Compressed Crimson Arkonor",
    "Prime Arkonor": "Compressed Prime Arkonor",
    "Mercoxit": "Compressed Mercoxit",
    "Magma Mercoxit": "Compressed Magma Mercoxit",
    "Vitreous Mercoxit": "Compressed Vitreous Mercoxit",
}

# =========================
# DB
# =========================
def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_contracts (
            contract_id INTEGER PRIMARY KEY,
            processed_at INTEGER NOT NULL,
            total_payout REAL NOT NULL,
            payload_json TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS type_cache (
            type_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            cached_at INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS price_cache (
            type_id INTEGER PRIMARY KEY,
            jita_buy REAL NOT NULL,
            cached_at INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS char_name_cache (
            character_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            cached_at INTEGER NOT NULL
        )
        """
    )
    return conn

# =========================
# OAUTH
# =========================
class EveOAuth:
    def __init__(self):
        if not (EVE_CLIENT_ID and EVE_CLIENT_SECRET and EVE_REFRESH_TOKEN and EVE_CHARACTER_ID):
            raise RuntimeError("Missing EVE_CLIENT_ID / EVE_CLIENT_SECRET / EVE_REFRESH_TOKEN / EVE_CHARACTER_ID env vars.")

    async def get_access_token(self, session: aiohttp.ClientSession) -> str:
        url = "https://login.eveonline.com/v2/oauth/token"
        auth = aiohttp.BasicAuth(EVE_CLIENT_ID, EVE_CLIENT_SECRET)
        data = {"grant_type": "refresh_token", "refresh_token": EVE_REFRESH_TOKEN}
        headers = {"User-Agent": ESI_UA}

        async with session.post(url, data=data, auth=auth, headers=headers) as resp:
            txt = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"OAuth failed: {resp.status} {txt}")
            j = await resp.json()
            return j["access_token"]

# =========================
# ESI CLIENT
# =========================
class EsiClient:
    def __init__(self, oauth: EveOAuth):
        self.oauth = oauth

    async def _get(self, session: aiohttp.ClientSession, url: str, token: str, params: Optional[dict] = None):
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": ESI_UA,
        }
        async with session.get(url, headers=headers, params=params) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"ESI GET failed {resp.status} {url} :: {text}")
            return await resp.json(), resp.headers

    async def get_all_character_contracts(self, session: aiohttp.ClientSession, token: str) -> List[dict]:
        all_rows: List[dict] = []
        page = 1
        while True:
            url = f"{ESI_BASE}/characters/{EVE_CHARACTER_ID}/contracts/"
            data, headers = await self._get(session, url, token, params={"page": page})
            if not isinstance(data, list):
                raise RuntimeError("Unexpected contracts payload (not a list).")
            if not data:
                break
            all_rows.extend(data)

            x_pages = headers.get("X-Pages")
            if x_pages is not None:
                try:
                    if page >= int(x_pages):
                        break
                except ValueError:
                    pass
            page += 1
            await asyncio.sleep(0.05)
        return all_rows

    async def get_character_contract_items(self, session: aiohttp.ClientSession, token: str, contract_id: int) -> List[dict]:
        url = f"{ESI_BASE}/characters/{EVE_CHARACTER_ID}/contracts/{contract_id}/items/"
        data, _headers = await self._get(session, url, token)
        if not isinstance(data, list):
            raise RuntimeError("Unexpected contract items payload (not a list).")
        return data

    async def get_type_name(self, session: aiohttp.ClientSession, token: str, conn: sqlite3.Connection, type_id: int) -> str:
        now = int(time.time())
        cur = conn.execute("SELECT name, cached_at FROM type_cache WHERE type_id=?", (type_id,))
        row = cur.fetchone()
        if row:
            name, cached_at = row
            if now - int(cached_at) <= TYPE_CACHE_TTL:
                return str(name)

        url = f"{ESI_BASE}/universe/types/{type_id}/"
        data, _ = await self._get(session, url, token)
        name = data.get("name") or f"type_id:{type_id}"

        conn.execute("INSERT OR REPLACE INTO type_cache(type_id, name, cached_at) VALUES(?,?,?)", (type_id, name, now))
        conn.commit()
        return name

    async def get_character_name(self, session: aiohttp.ClientSession, token: str, conn: sqlite3.Connection, character_id: int) -> str:
        now = int(time.time())
        cur = conn.execute("SELECT name, cached_at FROM char_name_cache WHERE character_id=?", (character_id,))
        row = cur.fetchone()
        if row:
            name, cached_at = row
            if now - int(cached_at) <= CHAR_NAME_CACHE_TTL:
                return str(name)

        url = f"{ESI_BASE}/characters/{character_id}/"
        data, _ = await self._get(session, url, token)
        name = data.get("name") or f"character_id:{character_id}"

        conn.execute("INSERT OR REPLACE INTO char_name_cache(character_id, name, cached_at) VALUES(?,?,?)", (character_id, name, now))
        conn.commit()
        return str(name)

    async def get_jita_buy_price(self, session: aiohttp.ClientSession, token: str, conn: sqlite3.Connection, type_id: int) -> float:
        now = int(time.time())
        cur = conn.execute("SELECT jita_buy, cached_at FROM price_cache WHERE type_id=?", (type_id,))
        row = cur.fetchone()
        if row:
            jita_buy, cached_at = row
            if now - int(cached_at) <= PRICE_CACHE_TTL:
                return float(jita_buy)

        url = f"{ESI_BASE}/markets/{JITA_REGION_ID}/orders/"
        best = 0.0
        page = 1
        while True:
            data, headers = await self._get(session, url, token, params={"order_type": "buy", "type_id": type_id, "page": page})
            if not data:
                break

            for o in data:
                if int(o.get("location_id", 0)) != JITA_LOCATION_ID:
                    continue
                price = float(o.get("price", 0.0))
                if price > best:
                    best = price

            x_pages = headers.get("X-Pages")
            if x_pages is not None:
                try:
                    if page >= int(x_pages):
                        break
                except ValueError:
                    pass

            page += 1
            await asyncio.sleep(0.05)

        conn.execute("INSERT OR REPLACE INTO price_cache(type_id, jita_buy, cached_at) VALUES(?,?,?)", (type_id, best, now))
        conn.commit()
        return float(best)

# =========================
# MAIN COG
# =========================
class BuybackContracts(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.oauth = EveOAuth()
        self.esi = EsiClient(self.oauth)
        self._lock = asyncio.Lock()

        if BUYBACK_AUTO_SCAN:
            self.scan_loop.start()

    def cog_unload(self):
        if self.scan_loop.is_running():
            self.scan_loop.cancel()

    async def _get_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        return discord.utils.get(guild.text_channels, name=BUYBACK_CHANNEL_NAME)

    def _contract_matches(self, row: dict) -> bool:
        try:
            return (
                row.get("status") == BUYBACK_STATUS
                and row.get("type") == BUYBACK_TYPE
                and int(row.get("assignee_id", 0)) == BUYBACK_ASSIGNEE_ID
                and int(row.get("start_location_id", 0)) == BUYBACK_START_LOCATION_ID
            )
        except Exception:
            return False

    def _compress_if_ore_name(self, name: str) -> str:
        return ORE_TO_COMPRESSED_NAME.get(name, name)

    # -------------------------
    # IGN registry lookup
    # -------------------------
    def _load_ign_registry_state(self) -> Dict[str, Any]:
        try:
            if not IGN_REGISTRY_PATH.exists():
                return {}
            return json.loads(IGN_REGISTRY_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _discord_user_id_for_character_id(self, character_id: int) -> Optional[int]:
        state = self._load_ign_registry_state()
        users = (state.get("users") or {})
        for uid_str, rec in users.items():
            try:
                uid = int(uid_str)
            except Exception:
                continue
            cids = rec.get("character_ids") or []
            if any(isinstance(x, int) and x == character_id for x in cids):
                return uid
        return None

    # -------------------------
    # Processed tracking
    # -------------------------
    async def _already_processed(self, contract_id: int) -> bool:
        conn = db_connect()
        try:
            cur = conn.execute("SELECT 1 FROM processed_contracts WHERE contract_id=?", (contract_id,))
            return cur.fetchone() is not None
        finally:
            conn.close()

    async def _mark_processed(self, contract_id: int, payload: dict, total: float):
        conn = db_connect()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO processed_contracts(contract_id, processed_at, total_payout, payload_json) VALUES(?,?,?,?)",
                (contract_id, int(time.time()), float(total), json.dumps(payload)),
            )
            conn.commit()
        finally:
            conn.close()

    # -------------------------
    # Appraisal
    # -------------------------
    async def _appraise_contract(self, session: aiohttp.ClientSession, token: str, contract_id: int) -> Tuple[dict, float]:
        conn = db_connect()
        try:
            raw_items = await self.esi.get_character_contract_items(session, token, contract_id)
            included = [i for i in raw_items if bool(i.get("is_included", True))]

            lines = []
            total = 0.0

            for it in included:
                type_id = int(it["type_id"])
                qty = int(it.get("quantity", 0))

                name = await self.esi.get_type_name(session, token, conn, type_id)
                priced_name = self._compress_if_ore_name(name)

                price_type_id = type_id
                if priced_name != name:
                    ids_url = f"{ESI_BASE}/universe/ids/"
                    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json", "User-Agent": ESI_UA}
                    async with session.post(ids_url, headers=headers, json=[priced_name]) as resp:
                        txt = await resp.text()
                        if resp.status != 200:
                            raise RuntimeError(f"ESI POST ids failed {resp.status}: {txt}")
                        j = await resp.json()
                        inv_types = j.get("inventory_types") or []
                        if inv_types:
                            price_type_id = int(inv_types[0]["id"])

                jita_buy = await self.esi.get_jita_buy_price(session, token, conn, price_type_id)
                unit_payout = jita_buy * PAYOUT_MULTIPLIER
                line_total = unit_payout * qty

                lines.append(
                    {
                        "name": priced_name,
                        "qty": qty,
                        "jita_buy": jita_buy,
                        "payout_unit": unit_payout,
                        "line_total": line_total,
                    }
                )
                total += line_total

            payload = {
                "contract_id": contract_id,
                "multiplier": PAYOUT_MULTIPLIER,
                "jita_location_id": JITA_LOCATION_ID,
                "lines": lines,
                "total": total,
            }
            return payload, total
        finally:
            conn.close()

    async def _post_appraisal(
        self,
        channel: discord.TextChannel,
        payload: dict,
        *,
        issuer_id: int,
        issuer_name: str,
        discord_user_id: Optional[int],
    ):
        cid = payload["contract_id"]
        total = payload["total"]
        mult = payload["multiplier"]

        mention = f"<@{discord_user_id}>" if discord_user_id else None
        issuer_line = f"**{issuer_name}** (`{issuer_id}`)" + (f" — {mention}" if mention else "")

        embed = discord.Embed(
            title=f"✅ Buyback Appraisal — Contract {cid}",
            description=(
                f"**Issuer:** {issuer_line}\n"
                f"Pricing = **{int(mult*100)}%** of **Jita 4-4 BUY** orders (location `{JITA_LOCATION_ID}`)."
            ),
            color=0x2ecc71,
        )

        lines = payload["lines"]
        chunks: List[str] = []
        buf = ""

        for ln in lines:
            row = (
                f"• **{ln['name']}** × {ln['qty']}\n"
                f"  Jita Buy: {ln['jita_buy']:.2f} ISK | Payout/unit: {ln['payout_unit']:.2f} ISK\n"
                f"  Line: **{ln['line_total']:.2f} ISK**\n"
            )
            if len(buf) + len(row) > 900:
                chunks.append(buf)
                buf = row
            else:
                buf += row

        if buf:
            chunks.append(buf)

        for idx, ch in enumerate(chunks[:10]):
            embed.add_field(name="Items" if idx == 0 else "Items (cont.)", value=ch, inline=False)

        embed.add_field(name="Total Payout", value=f"**{total:,.2f} ISK**", inline=False)
        await channel.send(embed=embed)

    # =========================
    # SCAN CORE (returns diagnostics)
    # =========================
    async def _resolve_target_channel(self) -> Tuple[Optional[discord.TextChannel], Optional[str]]:
        for g in self.bot.guilds:
            ch = await self._get_channel(g)
            if ch:
                return ch, None
        return None, f"Channel `#{BUYBACK_CHANNEL_NAME}` not found in any guild."

    async def _scan(self, *, mode: str, force_latest: bool = False) -> Dict[str, int]:
        """
        mode:
          - "all_new": post all unprocessed matches
          - "latest": post newest matching outstanding (optionally even if processed when force_latest=True)
        """
        stats = {"contracts_total": 0, "matches": 0, "skipped_processed": 0, "posted": 0}
        target_channel, err = await self._resolve_target_channel()
        if not target_channel:
            # represent as 0s; caller prints err
            return stats | {"channel_missing": 1}

        async with aiohttp.ClientSession() as session:
            token = await self.oauth.get_access_token(session)
            all_contracts = await self.esi.get_all_character_contracts(session, token)
            stats["contracts_total"] = len(all_contracts)

            matches = [c for c in all_contracts if self._contract_matches(c)]
            matches.sort(key=lambda x: int(x.get("contract_id", 0)), reverse=True)
            stats["matches"] = len(matches)

            if not matches:
                return stats

            if mode == "latest":
                c = matches[0]
                cid = int(c["contract_id"])
                if (not force_latest) and (await self._already_processed(cid)):
                    stats["skipped_processed"] = 1
                    return stats

                issuer_id = int(c.get("issuer_id", 0) or 0)
                conn = db_connect()
                try:
                    issuer_name = await self.esi.get_character_name(session, token, conn, issuer_id) if issuer_id else "Unknown"
                finally:
                    conn.close()

                discord_user_id = self._discord_user_id_for_character_id(issuer_id) if issuer_id else None
                payload, total = await self._appraise_contract(session, token, cid)
                await self._post_appraisal(target_channel, payload, issuer_id=issuer_id, issuer_name=issuer_name, discord_user_id=discord_user_id)
                await self._mark_processed(cid, payload, total)
                stats["posted"] = 1
                return stats

            # mode == "all_new"
            for c in matches:
                cid = int(c["contract_id"])
                if await self._already_processed(cid):
                    stats["skipped_processed"] += 1
                    continue

                issuer_id = int(c.get("issuer_id", 0) or 0)
                conn = db_connect()
                try:
                    issuer_name = await self.esi.get_character_name(session, token, conn, issuer_id) if issuer_id else "Unknown"
                finally:
                    conn.close()

                discord_user_id = self._discord_user_id_for_character_id(issuer_id) if issuer_id else None
                payload, total = await self._appraise_contract(session, token, cid)
                await self._post_appraisal(target_channel, payload, issuer_id=issuer_id, issuer_name=issuer_name, discord_user_id=discord_user_id)
                await self._mark_processed(cid, payload, total)
                stats["posted"] += 1

            return stats

    # =========================
    # LOOP (optional)
    # =========================
    @tasks.loop(seconds=SCAN_INTERVAL_SECONDS)
    async def scan_loop(self):
        if self._lock.locked():
            return
        async with self._lock:
            # autoscan should behave like "all_new"
            try:
                await self._scan(mode="all_new")
            except Exception:
                pass

    @scan_loop.before_loop
    async def before_scan_loop(self):
        await self.bot.wait_until_ready()

    # =========================
    # COMMANDS
    # =========================
    @app_commands.command(name="buyback_check", description="Scan and appraise ALL unprocessed outstanding buyback contracts.")
    async def buyback_check(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        async with self._lock:
            try:
                stats = await self._scan(mode="all_new")
            except Exception as e:
                await interaction.followup.send(f"❌ Scan failed: `{e}`", ephemeral=True)
                return

        if stats.get("channel_missing"):
            await interaction.followup.send(f"❌ Could not find channel `#{BUYBACK_CHANNEL_NAME}` in any guild.", ephemeral=True)
            return

        await interaction.followup.send(
            f"✅ Scan done.\n"
            f"- Contracts total: `{stats['contracts_total']}`\n"
            f"- Matches (filters): `{stats['matches']}`\n"
            f"- Skipped (already processed): `{stats['skipped_processed']}`\n"
            f"- Posted: `{stats['posted']}`",
            ephemeral=True,
        )

    @app_commands.command(name="buyback_latest", description="ALWAYS appraise the newest outstanding buyback contract (optionally force repost).")
    @app_commands.describe(force="If true, repost even if already processed.")
    async def buyback_latest(self, interaction: discord.Interaction, force: bool = False):
        await interaction.response.defer(ephemeral=True)
        async with self._lock:
            try:
                stats = await self._scan(mode="latest", force_latest=force)
            except Exception as e:
                await interaction.followup.send(f"❌ Latest appraisal failed: `{e}`", ephemeral=True)
                return

        if stats.get("channel_missing"):
            await interaction.followup.send(f"❌ Could not find channel `#{BUYBACK_CHANNEL_NAME}` in any guild.", ephemeral=True)
            return

        if stats["matches"] == 0:
            await interaction.followup.send(
                "⚠️ No matching outstanding contracts.\n"
                f"- Contracts total: `{stats['contracts_total']}`\n"
                f"- Filters: status=`{BUYBACK_STATUS}`, type=`{BUYBACK_TYPE}`, assignee_id=`{BUYBACK_ASSIGNEE_ID}`, start_location_id=`{BUYBACK_START_LOCATION_ID}`",
                ephemeral=True,
            )
            return

        if stats["posted"] == 0 and stats["skipped_processed"] > 0 and not force:
            await interaction.followup.send(
                "⚠️ Newest matching contract is already processed.\n"
                "Run `/buyback_latest force:true` to repost it anyway.",
                ephemeral=True,
            )
            return

        await interaction.followup.send(
            f"✅ Latest appraisal posted.\n"
            f"- Contracts total: `{stats['contracts_total']}`\n"
            f"- Matches (filters): `{stats['matches']}`",
            ephemeral=True,
        )

async def setup(bot: commands.Bot):
    await bot.add_cog(BuybackContracts(bot))
