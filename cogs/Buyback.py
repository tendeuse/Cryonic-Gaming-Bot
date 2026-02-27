# cogs/buyback_contracts.py
import os
import asyncio
import time
import json
import sqlite3
import datetime
import re
from typing import Dict, List, Optional, Tuple, Any

import aiohttp
import discord
from discord.ext import commands
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
EVE_CHARACTER_ID = int(os.getenv("EVE_CHARACTER_ID", "0"))  # character used to read contracts

# Filters (your buyback rules)
BUYBACK_ASSIGNEE_ID = int(os.getenv("BUYBACK_ASSIGNEE_ID", "2122848297"))
BUYBACK_START_LOCATION_ID = int(os.getenv("BUYBACK_START_LOCATION_ID", "1048840990158"))
BUYBACK_TYPE = os.getenv("BUYBACK_TYPE", "item_exchange")
BUYBACK_STATUS = os.getenv("BUYBACK_STATUS", "outstanding")

# Pricing
JITA_REGION_ID = int(os.getenv("JITA_REGION_ID", "10000002"))     # The Forge
JITA_LOCATION_ID = int(os.getenv("JITA_LOCATION_ID", "60003760")) # Jita 4-4
PAYOUT_MULTIPLIER = float(os.getenv("PAYOUT_MULTIPLIER", "0.8"))  # 80%

# Discord output
BUYBACK_CHANNEL_NAME = os.getenv("BUYBACK_CHANNEL_NAME", "buyback-payout")

# Optional: safety limit (0 = unlimited).
MAX_POST_PER_RUN = int(os.getenv("BUYBACK_MAX_POST_PER_RUN", "0"))

# Role allowed to mark paid
PAID_BUTTON_ROLE_NAME = "ARC Security Corporation Leader"

# ESI
ESI_BASE = "https://esi.evetech.net/latest"
ESI_UA = os.getenv("ESI_USER_AGENT", "ARC Buyback Bot (discord)")

# Local caches
TYPE_CACHE_TTL = 24 * 3600
PRICE_CACHE_TTL = 15 * 60
CHAR_NAME_CACHE_TTL = 24 * 3600

# HTTP timeouts (client-side)
HTTP_TIMEOUT_TOTAL = int(os.getenv("BUYBACK_HTTP_TIMEOUT_TOTAL", "60"))
HTTP_TIMEOUT_CONNECT = int(os.getenv("BUYBACK_HTTP_TIMEOUT_CONNECT", "15"))
HTTP_TIMEOUT_SOCK_READ = int(os.getenv("BUYBACK_HTTP_TIMEOUT_SOCK_READ", "60"))

# Retry config for transient ESI errors
ESI_MAX_ATTEMPTS = int(os.getenv("BUYBACK_ESI_MAX_ATTEMPTS", "5"))
ESI_BACKOFF_CAP_SECONDS = int(os.getenv("BUYBACK_ESI_BACKOFF_CAP", "10"))

# =========================
# ORE -> COMPRESSED (name-based)
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
# DB (caching + paid status)
# =========================
def db_connect():
    conn = sqlite3.connect(DB_PATH)
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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS buyback_paid (
            contract_id INTEGER PRIMARY KEY,
            paid_at INTEGER NOT NULL,
            paid_by_discord_id INTEGER NOT NULL,
            paid_by_tag TEXT NOT NULL
        )
        """
    )
    return conn

def _utc_iso(ts: int) -> str:
    return datetime.datetime.utcfromtimestamp(ts).replace(tzinfo=datetime.timezone.utc).isoformat().replace("+00:00", "Z")

def get_paid_status(contract_id: int) -> Optional[dict]:
    conn = db_connect()
    try:
        cur = conn.execute(
            "SELECT paid_at, paid_by_discord_id, paid_by_tag FROM buyback_paid WHERE contract_id=?",
            (int(contract_id),),
        )
        row = cur.fetchone()
        if not row:
            return None
        paid_at, paid_by_id, paid_by_tag = row
        return {
            "paid_at": int(paid_at),
            "paid_by_discord_id": int(paid_by_id),
            "paid_by_tag": str(paid_by_tag),
        }
    finally:
        conn.close()

def mark_paid(contract_id: int, paid_by_id: int, paid_by_tag: str) -> None:
    conn = db_connect()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO buyback_paid(contract_id, paid_at, paid_by_discord_id, paid_by_tag) VALUES(?,?,?,?)",
            (int(contract_id), int(time.time()), int(paid_by_id), str(paid_by_tag)),
        )
        conn.commit()
    finally:
        conn.close()

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
    async def _get(
        self,
        session: aiohttp.ClientSession,
        url: str,
        token: str,
        params: Optional[dict] = None,
        *,
        max_attempts: int = ESI_MAX_ATTEMPTS,
    ):
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json", "User-Agent": ESI_UA}
        transient_statuses = {502, 503, 504, 520, 521, 522}

        last_err: Optional[str] = None

        for attempt in range(1, max_attempts + 1):
            try:
                async with session.get(url, headers=headers, params=params) as resp:
                    text = await resp.text()

                    if resp.status == 200:
                        return await resp.json(), resp.headers

                    if resp.status in transient_statuses:
                        last_err = f"{resp.status} {text}"
                    else:
                        raise RuntimeError(f"ESI GET failed {resp.status} {url} :: {text}")

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_err = repr(e)

            if attempt < max_attempts:
                delay = min(2 ** (attempt - 1), ESI_BACKOFF_CAP_SECONDS)  # 1,2,4,8,cap...
                await asyncio.sleep(delay)

        raise RuntimeError(f"ESI GET failed after {max_attempts} attempts {url} :: {last_err}")

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
        data, _ = await self._get(session, url, token)
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
        return str(name)

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
            data, headers = await self._get(
                session,
                url,
                token,
                params={"order_type": "buy", "type_id": type_id, "page": page},
            )
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
# UI: Paid button (persistent via DynamicItem)
# =========================
class PaidButton(discord.ui.DynamicItem[discord.ui.Button], template=r"buyback:paid:(?P<cid>\d+)"):
    def __init__(self, contract_id: int):
        super().__init__(
            discord.ui.Button(
                label="Paid ✅",
                style=discord.ButtonStyle.success,
                custom_id=f"buyback:paid:{int(contract_id)}",
            )
        )
        self.contract_id = int(contract_id)

    @classmethod
    async def from_custom_id(cls, interaction: discord.Interaction, item: discord.ui.Button, match: re.Match[str]):
        cid = int(match.group("cid"))
        return cls(cid)

    async def callback(self, interaction: discord.Interaction):
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("❌ This must be used in a server.", ephemeral=True)
            return

        if not any(r.name == PAID_BUTTON_ROLE_NAME for r in interaction.user.roles):
            await interaction.response.send_message(
                f"❌ Only **{PAID_BUTTON_ROLE_NAME}** can mark contracts as paid.",
                ephemeral=True,
            )
            return

        existing = get_paid_status(self.contract_id)
        if existing:
            await interaction.response.send_message(
                f"⚠️ Contract `{self.contract_id}` is already marked PAID (by `{existing['paid_by_tag']}` at `{_utc_iso(existing['paid_at'])}`).",
                ephemeral=True,
            )
            return

        mark_paid(self.contract_id, interaction.user.id, str(interaction.user))

        msg = interaction.message
        if not msg or not msg.embeds:
            await interaction.response.send_message("✅ Marked as PAID. (Could not edit message embed.)", ephemeral=True)
            return

        emb = msg.embeds[0]

        paid = get_paid_status(self.contract_id)
        paid_line = (
            f"**PAID** ✅\n"
            f"By: <@{paid['paid_by_discord_id']}> (`{paid['paid_by_tag']}`)\n"
            f"At: `{_utc_iso(paid['paid_at'])}`"
        )

        updated = False
        new_emb = discord.Embed(
            title=emb.title,
            description=emb.description,
            color=emb.color,
            timestamp=emb.timestamp,
        )

        # preserve footer / author / thumbnail / image if present
        if emb.footer and emb.footer.text:
            new_emb.set_footer(
                text=emb.footer.text,
                icon_url=getattr(emb.footer, "icon_url", None) or discord.Embed.Empty,
            )
        if emb.author and emb.author.name:
            new_emb.set_author(
                name=emb.author.name,
                url=getattr(emb.author, "url", None) or discord.Embed.Empty,
                icon_url=getattr(emb.author, "icon_url", None) or discord.Embed.Empty,
            )
        if emb.thumbnail and emb.thumbnail.url:
            new_emb.set_thumbnail(url=emb.thumbnail.url)
        if emb.image and emb.image.url:
            new_emb.set_image(url=emb.image.url)

        for f in emb.fields:
            if (f.name or "").strip().lower() == "status":
                new_emb.add_field(name="Status", value=paid_line, inline=False)
                updated = True
            else:
                new_emb.add_field(name=f.name, value=f.value, inline=f.inline)

        if not updated:
            new_emb.add_field(name="Status", value=paid_line, inline=False)

        view = BuybackPaidView(self.contract_id, disabled=True)
        await interaction.response.edit_message(embed=new_emb, view=view)

class BuybackPaidView(discord.ui.View):
    def __init__(self, contract_id: int, *, disabled: bool = False):
        super().__init__(timeout=None)
        btn = PaidButton(int(contract_id))
        if disabled:
            btn.item.disabled = True
        self.add_item(btn)

# =========================
# COG
# =========================
class BuybackContracts(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.oauth = EveOAuth()
        self.esi = EsiClient()
        self._lock = asyncio.Lock()

        # Register dynamic item for persistence after restarts
        bot.add_dynamic_items(PaidButton)

    def _make_session_timeout(self) -> aiohttp.ClientTimeout:
        return aiohttp.ClientTimeout(
            total=HTTP_TIMEOUT_TOTAL,
            connect=HTTP_TIMEOUT_CONNECT,
            sock_read=HTTP_TIMEOUT_SOCK_READ,
        )

    async def _get_target_channel(self) -> Optional[discord.TextChannel]:
        for g in self.bot.guilds:
            ch = discord.utils.get(g.text_channels, name=BUYBACK_CHANNEL_NAME)
            if ch:
                return ch
        return None

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
    # IGN Registry lookup (issuer_id -> discord user)
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
        users = state.get("users") or {}
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
    # Appraisal
    # -------------------------
    async def _appraise_contract(self, session: aiohttp.ClientSession, token: str, contract_id: int) -> Tuple[dict, float]:
        conn = db_connect()
        try:
            raw_items = await self.esi.get_character_contract_items(session, token, contract_id)
            included = [i for i in raw_items if bool(i.get("is_included", True))]

            # per-appraisal in-memory caches to reduce repeated lookups
            name_mem: Dict[int, str] = {}
            price_mem: Dict[int, float] = {}

            lines = []
            total = 0.0

            for it in included:
                type_id = int(it["type_id"])
                qty = int(it.get("quantity", 0))

                if type_id in name_mem:
                    name = name_mem[type_id]
                else:
                    name = await self.esi.get_type_name(session, token, conn, type_id)
                    name_mem[type_id] = name

                priced_name = self._compress_if_ore_name(name)

                price_type_id = type_id
                if priced_name != name:
                    ids_url = f"{ESI_BASE}/universe/ids/"
                    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json", "User-Agent": ESI_UA}
                    # NOTE: this POST can also fail transiently; keep it simple (rare) and allow bubble-up
                    async with session.post(ids_url, headers=headers, json=[priced_name]) as resp:
                        txt = await resp.text()
                        if resp.status != 200:
                            raise RuntimeError(f"ESI POST ids failed {resp.status}: {txt}")
                        j = await resp.json()
                        inv_types = j.get("inventory_types") or []
                        if inv_types:
                            price_type_id = int(inv_types[0]["id"])

                if price_type_id in price_mem:
                    jita_buy = price_mem[price_type_id]
                else:
                    jita_buy = await self.esi.get_jita_buy_price(session, token, conn, price_type_id)
                    price_mem[price_type_id] = jita_buy

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
        contract_status: Optional[str] = None,
        contract_type: Optional[str] = None,
    ) -> None:
        cid = payload["contract_id"]
        total = payload["total"]
        mult = payload["multiplier"]

        mention = f"<@{discord_user_id}>" if discord_user_id else None
        issuer_line = f"**{issuer_name}** (`{issuer_id}`)" + (f" — {mention}" if mention else "")

        paid = get_paid_status(cid)
        if paid:
            status_value = (
                f"**PAID** ✅\n"
                f"By: <@{paid['paid_by_discord_id']}> (`{paid['paid_by_tag']}`)\n"
                f"At: `{_utc_iso(paid['paid_at'])}`"
            )
            paid_button_disabled = True
        else:
            status_value = "**UNPAID** ❌"
            paid_button_disabled = False

        embed = discord.Embed(
            title=f"✅ Buyback Appraisal — Contract {cid}",
            description=(
                f"**Issuer:** {issuer_line}\n"
                f"Pricing = **{int(mult*100)}%** of **Jita 4-4 BUY** orders (location `{JITA_LOCATION_ID}`)."
            ),
            color=0x2ecc71,
        )

        # Paid status
        embed.add_field(name="Status", value=status_value, inline=False)

        # Extra info (useful for /buybackid when not outstanding)
        meta_bits = []
        if contract_status:
            meta_bits.append(f"status=`{contract_status}`")
        if contract_type:
            meta_bits.append(f"type=`{contract_type}`")
        if meta_bits:
            embed.add_field(name="Contract Meta", value=" | ".join(meta_bits), inline=False)

        # Items (chunk for embed limits)
        chunks: List[str] = []
        buf = ""
        for ln in payload["lines"]:
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

        view = BuybackPaidView(cid, disabled=paid_button_disabled)
        await channel.send(embed=embed, view=view)

    # =========================
    # /buyback (posts all matching outstanding every run)
    # =========================
    @app_commands.command(name="buyback", description="Appraise and post ALL matching outstanding buyback contracts (every run).")
    async def buyback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        async with self._lock:
            channel = await self._get_target_channel()
            if not channel:
                await interaction.followup.send(
                    f"❌ Channel `#{BUYBACK_CHANNEL_NAME}` not found in any guild.",
                    ephemeral=True,
                )
                return

            try:
                timeout = self._make_session_timeout()
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    token = await self.oauth.get_access_token(session)
                    all_contracts = await self.esi.get_all_character_contracts(session, token)
                    matches = [c for c in all_contracts if self._contract_matches(c)]

                    matches.sort(key=lambda x: int(x.get("contract_id", 0)), reverse=True)

                    if not matches:
                        await interaction.followup.send(
                            "⚠️ No matching outstanding contracts.\n"
                            f"Checked `{len(all_contracts)}` total contracts.\n"
                            f"Filters: status=`{BUYBACK_STATUS}`, type=`{BUYBACK_TYPE}`, assignee_id=`{BUYBACK_ASSIGNEE_ID}`, start_location_id=`{BUYBACK_START_LOCATION_ID}`",
                            ephemeral=True,
                        )
                        return

                    posted = 0
                    for c in matches:
                        if MAX_POST_PER_RUN > 0 and posted >= MAX_POST_PER_RUN:
                            break

                        cid = int(c["contract_id"])
                        issuer_id = int(c.get("issuer_id", 0) or 0)
                        c_status = str(c.get("status") or "")
                        c_type = str(c.get("type") or "")

                        conn = db_connect()
                        try:
                            issuer_name = await self.esi.get_character_name(session, token, conn, issuer_id) if issuer_id else "Unknown"
                        finally:
                            conn.close()

                        discord_user_id = self._discord_user_id_for_character_id(issuer_id) if issuer_id else None

                        payload, _total = await self._appraise_contract(session, token, cid)
                        await self._post_appraisal(
                            channel,
                            payload,
                            issuer_id=issuer_id,
                            issuer_name=issuer_name,
                            discord_user_id=discord_user_id,
                            contract_status=c_status,
                            contract_type=c_type,
                        )
                        posted += 1

                    note = ""
                    if MAX_POST_PER_RUN > 0 and len(matches) > MAX_POST_PER_RUN:
                        note = f"\n⚠️ Limited to `{MAX_POST_PER_RUN}` posts this run (set BUYBACK_MAX_POST_PER_RUN=0 for unlimited)."

                    await interaction.followup.send(
                        f"✅ Posted `{posted}` appraisal(s).\n"
                        f"- Total contracts checked: `{len(all_contracts)}`\n"
                        f"- Matching outstanding: `{len(matches)}`"
                        f"{note}",
                        ephemeral=True,
                    )

            except Exception as e:
                await interaction.followup.send(f"❌ /buyback failed: `{e}`", ephemeral=True)

    # =========================
    # /buybackid (appraise 1 specific contract regardless of status)
    # =========================
    @app_commands.command(name="buybackid", description="Appraise and post a specific contract by ID (any status).")
    async def buybackid(self, interaction: discord.Interaction, contract_id: int):
        await interaction.response.defer(ephemeral=True)

        async with self._lock:
            channel = await self._get_target_channel()
            if not channel:
                await interaction.followup.send(
                    f"❌ Channel `#{BUYBACK_CHANNEL_NAME}` not found in any guild.",
                    ephemeral=True,
                )
                return

            try:
                timeout = self._make_session_timeout()
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    token = await self.oauth.get_access_token(session)

                    # Find contract row (for issuer/status/type). If not found, we can still try items.
                    all_contracts = await self.esi.get_all_character_contracts(session, token)
                    row = next((c for c in all_contracts if int(c.get("contract_id", 0)) == int(contract_id)), None)

                    issuer_id = int(row.get("issuer_id", 0) or 0) if row else 0
                    c_status = str(row.get("status") or "unknown") if row else "unknown"
                    c_type = str(row.get("type") or "unknown") if row else "unknown"

                    # Optional sanity: only appraise item_exchange properly
                    if row and c_type != "item_exchange":
                        await interaction.followup.send(
                            f"⚠️ Contract `{contract_id}` is type `{c_type}`. This buyback appraiser is designed for `item_exchange`.\n"
                            f"I will still attempt to fetch items; if ESI rejects it, you'll see the error.",
                            ephemeral=True,
                        )

                    conn = db_connect()
                    try:
                        issuer_name = await self.esi.get_character_name(session, token, conn, issuer_id) if issuer_id else "Unknown"
                    finally:
                        conn.close()

                    discord_user_id = self._discord_user_id_for_character_id(issuer_id) if issuer_id else None

                    payload, _total = await self._appraise_contract(session, token, int(contract_id))
                    await self._post_appraisal(
                        channel,
                        payload,
                        issuer_id=issuer_id,
                        issuer_name=issuer_name,
                        discord_user_id=discord_user_id,
                        contract_status=c_status,
                        contract_type=c_type,
                    )

                    found_msg = "found in list ✅" if row else "not found in character contract list (still appraised via items endpoint if accessible) ⚠️"
                    await interaction.followup.send(
                        f"✅ Posted appraisal for contract `{contract_id}`.\n"
                        f"- Lookup: {found_msg}\n"
                        f"- Meta: status=`{c_status}`, type=`{c_type}`",
                        ephemeral=True,
                    )

            except Exception as e:
                await interaction.followup.send(f"❌ /buybackid failed for `{contract_id}`: `{e}`", ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(BuybackContracts(bot))