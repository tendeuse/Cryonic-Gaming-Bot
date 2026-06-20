# cogs/shop.py
#
# MULTI-SHOP VERSION (Main + HS) — PERM-SAFE FOR @everyone — OPTIMISED
#
# What changed vs the original and why:
#
#   OPT-1  Removed global serial edit lock + 2.5 s hard sleep.
#          discord.py's HTTP client already queues and retries 429s by itself.
#          A module-level asyncio.Semaphore(10) replaces it — caps concurrent
#          Discord API calls to 10 so we never burst, but no artificial waiting.
#
#   OPT-2  In-memory message cache (_msg_cache).
#          Each message is fetched from Discord at most once per bot session.
#          All subsequent reads come from the dict (zero network cost).
#          Cache is invalidated on NotFound or after a successful edit.
#
#   OPT-3  sync_shop_messages uses asyncio.gather.
#          All items are synced concurrently (shop + access in parallel per item,
#          all items in parallel with each other).  The per-item sleep(0.5) is gone.
#          shop_rebuild also gathers both shops instead of running them sequentially.
#
#   OPT-4  Async file I/O via asyncio.to_thread.
#          load_* / save_* are blocking; wrapping them in to_thread keeps the
#          event loop free during disk access.
#
#   OPT-5  "remove" action deletes the two item messages directly.
#          No longer triggers a full sync of the entire shop.
#
#   OPT-6  BuyItemModal uses a single SHOP_LOCK acquisition for all data work.
#          The second lock (just to write message_id back) is kept intentionally
#          small and only re-acquired after the Discord send returns.
#
#   OPT-7  Startup reduced from hard 5 s sleep → 2 s, and each guild is initialised
#          as a concurrent task via asyncio.create_task so guilds don't stall each
#          other.  The 4 s + 2 s inter-shop/inter-guild sleeps are removed.
#          A _sync_in_progress guard replaces the old "wait until ready" pattern
#          and prevents two concurrent syncs for the same shop from racing.
#
# Channels:
#   MAIN: ap-eve-shop            (display)  | ap-shop-access            (controls) | ap-shop-orders
#   HS:   ap-eve-shop-hs         (display)  | ap-shop-access-hs         (controls) | ap-shop-orders-hs

import os
import discord
import json
import asyncio
import uuid
import datetime
from pathlib import Path
from discord.ext import commands
from discord import app_commands

from . import db

# ----------------------------
# Persistence root (Railway)
# ----------------------------
PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)

SHOP_FILES = {
    "main": PERSIST_ROOT / "shop.json",
    "hs":   PERSIST_ROOT / "shop_hs.json",
}

AP_FILE     = PERSIST_ROOT / "ap_data.json"
ORDERS_FILE = PERSIST_ROOT / "shop_orders.json"

INDEX_FILES = {
    "main": PERSIST_ROOT / "shop_message_index.json",
    "hs":   PERSIST_ROOT / "shop_message_index_hs.json",
}

for p in list(SHOP_FILES.values()) + [AP_FILE, ORDERS_FILE] + list(INDEX_FILES.values()):
    p.parent.mkdir(parents=True, exist_ok=True)

# ----------------------------
# Channel config
# ----------------------------
ORDER_LOG_CHANNELS = {
    "main": "ap-shop-orders",
    "hs":   "ap-shop-orders-hs",
}

SHOPS = {
    "main": {
        "label":          "Main",
        "shop_channel":   "ap-eve-shop",
        "access_channel": "ap-shop-access",
    },
    "hs": {
        "label":          "HS",
        "shop_channel":   "ap-eve-shop-hs",
        "access_channel": "ap-shop-access-hs",
    },
}

ALLOWED_ROLES = {"Shop Steward", "ARC Security Administration Council"}

SHOP_LOCK = asyncio.Lock()

# OPT-1: replaces the old 2.5 s-per-edit serial lock.
#         10 concurrent Discord API calls is well within Discord's global limit
#         (50 req/s); discord.py handles any 429s automatically.
_EDIT_SEMAPHORE = asyncio.Semaphore(10)


# ----------------------------
# Interaction safety helpers
# ----------------------------
async def safe_defer(interaction: discord.Interaction, *, ephemeral: bool = True) -> None:
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=ephemeral)
    except Exception:
        pass


async def safe_reply(interaction: discord.Interaction, content: str, *, ephemeral: bool = True) -> None:
    try:
        if interaction.response.is_done():
            await interaction.followup.send(content, ephemeral=ephemeral)
        else:
            await interaction.response.send_message(content, ephemeral=ephemeral)
    except Exception:
        pass


async def safe_send_modal(interaction: discord.Interaction, modal: discord.ui.Modal) -> None:
    try:
        if interaction.response.is_done():
            await interaction.followup.send("❌ Please try again.", ephemeral=True)
            return
        await interaction.response.send_modal(modal)
    except (discord.InteractionResponded, discord.NotFound):
        return
    except Exception:
        await safe_reply(interaction, "❌ Failed to open the form. Please try again.", ephemeral=True)


# ----------------------------
# JSON helpers (ATOMIC WRITES)
# ----------------------------
def _load_json(path: Path, default):
    # Stored in MySQL kv_store keyed by the old filename stem.
    try:
        d = db.kv_load(path.stem, None)
        return d if d is not None else default
    except Exception:
        return default


def _save_json(path: Path, data) -> None:
    db.kv_save(path.stem, data)


def ensure_shop_schema(data: dict) -> dict:
    if not isinstance(data, dict):
        data = {}
    data.setdefault("items", {})
    if not isinstance(data["items"], dict):
        data["items"] = {}
    return data


# Sync I/O — used only by the async wrappers below
def load_shop(shop_key: str) -> dict:
    return ensure_shop_schema(_load_json(SHOP_FILES[shop_key], {}))

def save_shop(shop_key: str, data: dict) -> None:
    _save_json(SHOP_FILES[shop_key], data)

def load_ap() -> dict:
    return _load_json(AP_FILE, {})

def save_ap(data: dict) -> None:
    _save_json(AP_FILE, data)

def load_orders() -> dict:
    return _load_json(ORDERS_FILE, {"orders": {}})

def save_orders(data: dict) -> None:
    _save_json(ORDERS_FILE, data)

def load_index(shop_key: str) -> dict:
    return _load_json(INDEX_FILES[shop_key], {})

def save_index(shop_key: str, data: dict) -> None:
    _save_json(INDEX_FILES[shop_key], data)


# OPT-4: Async wrappers — file I/O runs in a thread pool so the event loop stays free.
async def aload_shop(shop_key: str) -> dict:
    return await asyncio.to_thread(load_shop, shop_key)

async def asave_shop(shop_key: str, data: dict) -> None:
    await asyncio.to_thread(save_shop, shop_key, data)

async def aload_ap() -> dict:
    return await asyncio.to_thread(load_ap)

async def asave_ap(data: dict) -> None:
    await asyncio.to_thread(save_ap, data)

async def aload_orders() -> dict:
    return await asyncio.to_thread(load_orders)

async def asave_orders(data: dict) -> None:
    await asyncio.to_thread(save_orders, data)

async def aload_index(shop_key: str) -> dict:
    return await asyncio.to_thread(load_index, shop_key)

async def asave_index(shop_key: str, data: dict) -> None:
    await asyncio.to_thread(save_index, shop_key, data)


def utc_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def is_valid_image_url(url: str) -> bool:
    if not url or len(url) > 2048:
        return False
    try:
        from urllib.parse import urlparse
        r = urlparse(url)
        return r.scheme in ("http", "https") and bool(r.netloc)
    except Exception:
        return False


def is_manager(member: discord.abc.User | discord.Member) -> bool:
    if not isinstance(member, discord.Member):
        return False
    return any(r.name in ALLOWED_ROLES for r in member.roles)


def get_order_channel_name(shop_key: str) -> str:
    return ORDER_LOG_CHANNELS.get(shop_key, ORDER_LOG_CHANNELS["main"])


# ----------------------------
# Embed builders
# ----------------------------
def build_item_embed(shop_key: str, item_id: str, item: dict) -> discord.Embed:
    embed = discord.Embed(
        title=item.get("name", "Unnamed Item"),
        description=item.get("desc", ""),
        timestamp=discord.utils.utcnow(),
    )
    embed.add_field(name="Price", value=f'{int(item.get("price", 0))} AP', inline=True)
    embed.add_field(name="Stock", value=str(int(item.get("stock", 0))), inline=True)
    if item.get("image") and is_valid_image_url(item["image"]):
        embed.set_image(url=item["image"])
    embed.set_footer(text=f"Cryonic Gaming Shop({shop_key}) | id:{item_id}")
    return embed


def build_order_embed(order: dict) -> discord.Embed:
    status = order.get("status", "PENDING")
    title  = f"Order {order.get('order_id', '')} — {status}"
    embed  = discord.Embed(title=title, timestamp=discord.utils.utcnow())
    embed.add_field(name="Buyer",    value=f"<@{order.get('buyer_id')}> ({order.get('buyer_tag')})", inline=False)
    embed.add_field(name="Shop",     value=str(order.get("shop_key", "main")), inline=True)
    embed.add_field(name="Item",     value=str(order.get("item_name", "Unknown")), inline=True)
    embed.add_field(name="Quantity", value=str(order.get("qty", 0)), inline=True)
    embed.add_field(name="IGN",      value=f"`{order.get('ign', '')}`", inline=True)
    embed.add_field(name="Cost",     value=f"{order.get('cost', 0)} AP", inline=True)
    embed.add_field(name="Created",  value=order.get("created_at", ""), inline=True)

    if status == "DELIVERED":
        delivered_tag = order.get("delivered_by_tag") or order.get("delivered_by", "Unknown")
        embed.add_field(name="Delivered By", value=f"<@{order.get('delivered_by')}> ({delivered_tag})", inline=True)
        embed.add_field(name="Delivered At", value=order.get("delivered_at", ""), inline=True)

    if status == "UNDELIVERED":
        undelivered_tag = order.get("undelivered_by_tag") or order.get("undelivered_by", "Unknown")
        embed.add_field(name="Marked Undelivered By", value=f"<@{order.get('undelivered_by')}> ({undelivered_tag})", inline=True)
        embed.add_field(name="Undelivered At",        value=order.get("undelivered_at", ""), inline=True)
        reason = order.get("undelivered_reason") or "No reason provided."
        embed.add_field(name="Reason", value=reason[:1024], inline=False)

    embed.set_footer(text="Shop Orders")
    return embed


# ----------------------------
# Index recovery (best-effort)
# ----------------------------
async def rebuild_index_from_channels(guild: discord.Guild, shop_key: str) -> None:
    shop_ch   = discord.utils.get(guild.text_channels, name=SHOPS[shop_key]["shop_channel"])
    access_ch = discord.utils.get(guild.text_channels, name=SHOPS[shop_key]["access_channel"])
    if not shop_ch or not access_ch:
        return

    idx       = await aload_index(shop_key)
    gkey      = str(guild.id)
    gidx      = idx.setdefault(gkey, {})
    gidx.setdefault("items", {})
    items_idx: dict = gidx["items"]

    async def scan_channel(ch: discord.TextChannel, key_name: str):
        async for msg in ch.history(limit=250):
            try:
                me = guild.me
                if not me or not msg.author or msg.author.id != me.id:
                    continue
                if not msg.embeds:
                    continue
                e           = msg.embeds[0]
                footer_text = (e.footer.text or "") if e.footer else ""
                if f"Cryonic Gaming Shop({shop_key})" not in footer_text or "id:" not in footer_text:
                    continue
                item_id = footer_text.split("id:", 1)[1].strip()
                if not item_id:
                    continue
                entry = items_idx.get(item_id) or {}
                if not isinstance(entry, dict):
                    entry = {}
                entry[key_name] = int(msg.id)
                items_idx[item_id] = entry
            except Exception:
                continue

    await scan_channel(shop_ch, "shop_msg_id")
    await scan_channel(access_ch, "access_msg_id")

    if not gidx.get("management_msg_id"):
        async for msg in access_ch.history(limit=150):
            try:
                me = guild.me
                if (me and msg.author and msg.author.id == me.id
                        and msg.content.strip() == f"**Shop Management ({SHOPS[shop_key]['label']})**"):
                    gidx["management_msg_id"] = int(msg.id)
                    break
            except Exception:
                continue

    gidx["items"] = items_idx
    idx[gkey]     = gidx
    await asave_index(shop_key, idx)


# ----------------------------
# Undelivered Reason Modal
# ----------------------------
class UndeliveredReasonModal(discord.ui.Modal):
    def __init__(self, cog: "ShopCog", order_id: str):
        super().__init__(title="Mark Undelivered")
        self.cog      = cog
        self.order_id = order_id
        self.reason   = discord.ui.TextInput(
            label="Reason",
            style=discord.TextStyle.long,
            placeholder="Why is this undelivered? (e.g., buyer offline, wrong IGN, etc.)",
            required=True,
            max_length=1000,
        )
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        if not is_manager(interaction.user):
            await safe_reply(interaction, "❌ Not authorized.", ephemeral=True)
            return
        await safe_defer(interaction, ephemeral=True)

        async with SHOP_LOCK:
            data   = await aload_orders()
            orders = data.setdefault("orders", {})
            o      = orders.get(self.order_id)
            if not o:
                await safe_reply(interaction, "❌ Order not found.", ephemeral=True)
                return
            o["status"]             = "UNDELIVERED"
            o["undelivered_by"]     = str(interaction.user.id)
            o["undelivered_by_tag"] = str(interaction.user)
            o["undelivered_at"]     = utc_iso()
            o["undelivered_reason"] = str(self.reason.value).strip()
            o.pop("delivered_by", None)
            o.pop("delivered_at", None)
            orders[self.order_id] = o
            await asave_orders(data)

        if interaction.guild:
            await self.cog.refresh_order_message(interaction.guild, self.order_id)
        await safe_reply(interaction, "✅ Marked as UNDELIVERED.", ephemeral=True)


# ----------------------------
# Persistent Order Buttons
# ----------------------------
class DeliveredButton(discord.ui.Button):
    def __init__(self, cog: "ShopCog", order_id: str):
        super().__init__(label="Delivered", style=discord.ButtonStyle.success, custom_id=f"order:delivered:{order_id}")
        self.cog      = cog
        self.order_id = order_id

    async def callback(self, interaction: discord.Interaction):
        if not is_manager(interaction.user):
            await safe_reply(interaction, "❌ Not authorized.", ephemeral=True)
            return
        await safe_defer(interaction, ephemeral=True)

        async with SHOP_LOCK:
            data   = await aload_orders()
            orders = data.setdefault("orders", {})
            o      = orders.get(self.order_id)
            if not o:
                await safe_reply(interaction, "❌ Order not found.", ephemeral=True)
                return
            o["status"]           = "DELIVERED"
            o["delivered_by"]     = str(interaction.user.id)
            o["delivered_by_tag"] = str(interaction.user)
            o["delivered_at"]     = utc_iso()
            o.pop("undelivered_by",     None)
            o.pop("undelivered_at",     None)
            o.pop("undelivered_reason", None)
            orders[self.order_id] = o
            await asave_orders(data)

        if interaction.guild:
            await self.cog.refresh_order_message(interaction.guild, self.order_id)
        await safe_reply(interaction, "✅ Marked as DELIVERED.", ephemeral=True)


class UndeliveredButton(discord.ui.Button):
    def __init__(self, cog: "ShopCog", order_id: str):
        super().__init__(
            label="Undelivered (Add reason)",
            style=discord.ButtonStyle.danger,
            custom_id=f"order:undelivered:{order_id}",
        )
        self.cog      = cog
        self.order_id = order_id

    async def callback(self, interaction: discord.Interaction):
        if not is_manager(interaction.user):
            await safe_reply(interaction, "❌ Not authorized.", ephemeral=True)
            return
        await safe_send_modal(interaction, UndeliveredReasonModal(self.cog, self.order_id))


class UndoButton(discord.ui.Button):
    def __init__(self, cog: "ShopCog", order_id: str):
        super().__init__(label="Undo", style=discord.ButtonStyle.secondary, custom_id=f"order:undo:{order_id}")
        self.cog      = cog
        self.order_id = order_id

    async def callback(self, interaction: discord.Interaction):
        if not is_manager(interaction.user):
            await safe_reply(interaction, "❌ Not authorized.", ephemeral=True)
            return
        await safe_defer(interaction, ephemeral=True)

        async with SHOP_LOCK:
            data   = await aload_orders()
            orders = data.setdefault("orders", {})
            o      = orders.get(self.order_id)
            if not o:
                await safe_reply(interaction, "❌ Order not found.", ephemeral=True)
                return
            o["status"] = "PENDING"
            o.pop("delivered_by",       None)
            o.pop("delivered_at",       None)
            o.pop("undelivered_by",     None)
            o.pop("undelivered_at",     None)
            o.pop("undelivered_reason", None)
            orders[self.order_id] = o
            await asave_orders(data)

        if interaction.guild:
            await self.cog.refresh_order_message(interaction.guild, self.order_id)
        await safe_reply(interaction, "✅ Status reset to PENDING.", ephemeral=True)


class UpdateIGNModal(discord.ui.Modal):
    def __init__(self, cog: "ShopCog", order_id: str, current_ign: str):
        super().__init__(title="Update In-Game Name")
        self.cog      = cog
        self.order_id = order_id
        self.ign      = discord.ui.TextInput(
            label="New In-Game Name (IGN)",
            placeholder="Enter your correct IGN",
            default=current_ign,
            required=True,
            max_length=64,
        )
        self.add_item(self.ign)

    async def on_submit(self, interaction: discord.Interaction):
        new_ign = str(self.ign.value).strip()
        if not new_ign:
            await safe_reply(interaction, "❌ IGN cannot be empty.", ephemeral=True)
            return
        await safe_defer(interaction, ephemeral=True)

        async with SHOP_LOCK:
            data   = await aload_orders()
            orders = data.setdefault("orders", {})
            o      = orders.get(self.order_id)
            if not o:
                await safe_reply(interaction, "❌ Order not found.", ephemeral=True)
                return
            if str(interaction.user.id) != str(o.get("buyer_id")):
                await safe_reply(interaction, "❌ Only the buyer can update the IGN.", ephemeral=True)
                return
            if o.get("status") == "DELIVERED":
                await safe_reply(interaction, "❌ This order is already delivered — the IGN cannot be changed.", ephemeral=True)
                return
            old_ign  = o.get("ign", "")
            o["ign"] = new_ign
            orders[self.order_id] = o
            await asave_orders(data)

        if interaction.guild:
            await self.cog.refresh_order_message(interaction.guild, self.order_id)
        await safe_reply(interaction, f"✅ IGN updated: `{old_ign}` → `{new_ign}`", ephemeral=True)


class UpdateIGNButton(discord.ui.Button):
    def __init__(self, cog: "ShopCog", order_id: str, *, disabled: bool = False):
        super().__init__(
            label="✏️ Update IGN",
            style=discord.ButtonStyle.secondary,
            custom_id=f"order:update_ign:{order_id}",
            disabled=disabled,
        )
        self.cog      = cog
        self.order_id = order_id

    async def callback(self, interaction: discord.Interaction):
        data = await aload_orders()
        o    = (data.get("orders") or {}).get(self.order_id)
        if not o:
            await safe_reply(interaction, "❌ Order not found.", ephemeral=True)
            return
        if str(interaction.user.id) != str(o.get("buyer_id")):
            await safe_reply(interaction, "❌ Only the buyer can update the IGN on their order.", ephemeral=True)
            return
        if o.get("status") == "DELIVERED":
            await safe_reply(interaction, "❌ This order is already delivered — IGN cannot be changed.", ephemeral=True)
            return
        await safe_send_modal(interaction, UpdateIGNModal(self.cog, self.order_id, str(o.get("ign", ""))))


class OrderStatusView(discord.ui.View):
    """
    Buttons visible on every order card in the orders channel.
    • PENDING / UNDELIVERED  → all four buttons active
    • DELIVERED              → only Undo is active; the other three are disabled
    """
    def __init__(self, cog: "ShopCog", order_id: str, status: str = "PENDING"):
        super().__init__(timeout=None)
        delivered = (status == "DELIVERED")

        self.add_item(DeliveredButton(cog, order_id))
        self.add_item(UndeliveredButton(cog, order_id))
        self.add_item(UndoButton(cog, order_id))
        self.add_item(UpdateIGNButton(cog, order_id, disabled=delivered))

        if delivered:
            for item in self.children:
                if isinstance(item, (DeliveredButton, UndeliveredButton)):
                    item.disabled = True


# ----------------------------
# Buy modal (Quantity + IGN)
# ----------------------------
class BuyItemModal(discord.ui.Modal):
    def __init__(self, cog: "ShopCog", shop_key: str, item_id: str):
        super().__init__(title=f"Buy Item ({SHOPS[shop_key]['label']})")
        self.cog      = cog
        self.shop_key = shop_key
        self.item_id  = item_id

        self.qty = discord.ui.TextInput(label="Quantity",           placeholder="Enter a number (e.g., 1)", required=True)
        self.ign = discord.ui.TextInput(label="In-Game Name (IGN)", placeholder="Enter your IGN",           required=True, max_length=32)
        self.add_item(self.qty)
        self.add_item(self.ign)

    async def on_submit(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        try:
            qty = int(str(self.qty.value).strip())
            if qty <= 0:
                raise ValueError
        except ValueError:
            await safe_reply(interaction, "❌ Quantity must be a positive whole number.", ephemeral=True)
            return

        ign = str(self.ign.value).strip()
        if not ign:
            await safe_reply(interaction, "❌ IGN is required.", ephemeral=True)
            return

        # OPT-6: All validation + deduction + order record creation in one lock block.
        order_id     = None
        channel_name = None
        order        = None

        async with SHOP_LOCK:
            shop  = await aload_shop(self.shop_key)
            items = shop["items"]

            if self.item_id not in items:
                await safe_reply(interaction, "❌ This item no longer exists.", ephemeral=True)
                return

            item  = items[self.item_id]
            stock = int(item.get("stock", 0))
            if stock <= 0:
                await safe_reply(interaction, "❌ Out of stock.", ephemeral=True)
                return
            if qty > stock:
                await safe_reply(interaction, f"❌ Not enough stock (requested {qty}, available {stock}).", ephemeral=True)
                return

            ap_data    = await aload_ap()
            uid        = str(interaction.user.id)
            user_entry = ap_data.get(uid)
            if not user_entry or "ap" not in user_entry:
                await safe_reply(interaction, "❌ You have no AP account.", ephemeral=True)
                return

            price      = int(item.get("price", 0))
            total_cost = price * qty
            user_ap    = int(float(user_entry.get("ap", 0)))
            if user_ap < total_cost:
                await safe_reply(interaction, f"❌ Not enough AP (cost {total_cost}, you have {user_ap}).", ephemeral=True)
                return

            user_entry["ap"]    = user_ap - total_cost
            item["stock"]       = stock - qty
            items[self.item_id] = item
            ap_data[uid]        = user_entry
            await asave_ap(ap_data)
            await asave_shop(self.shop_key, shop)

            order_id     = uuid.uuid4().hex[:10]
            channel_name = get_order_channel_name(self.shop_key)
            order = {
                "order_id":     order_id,
                "status":       "PENDING",
                "created_at":   utc_iso(),
                "guild_id":     str(interaction.guild.id) if interaction.guild else None,
                "channel_name": channel_name,
                "message_id":   None,
                "buyer_id":     str(interaction.user.id),
                "buyer_tag":    str(interaction.user),
                "shop_key":     self.shop_key,
                "item_id":      self.item_id,
                "item_name":    item.get("name", self.item_id),
                "qty":          qty,
                "ign":          ign,
                "cost":         total_cost,
            }
            orders_data = await aload_orders()
            orders_data.setdefault("orders", {})[order_id] = order
            await asave_orders(orders_data)

        # --- Lock released — Discord API calls ---
        if not interaction.guild:
            await safe_reply(interaction, "❌ Guild context missing.", ephemeral=True)
            return

        order_ch = discord.utils.get(interaction.guild.text_channels, name=channel_name)
        if not order_ch:
            await safe_reply(interaction, f"❌ Order channel `{channel_name}` not found. Contact staff.", ephemeral=True)
            return

        self.cog.register_order_view(order_id)
        embed = build_order_embed(order)
        msg   = await order_ch.send(embed=embed, view=OrderStatusView(self.cog, order_id, "PENDING"))

        # Write message_id back (short second lock)
        async with SHOP_LOCK:
            orders_data = await aload_orders()
            if order_id in orders_data.get("orders", {}):
                orders_data["orders"][order_id]["message_id"] = str(msg.id)
                await asave_orders(orders_data)

        await self.cog.update_item_messages(interaction.guild, self.shop_key, self.item_id)
        await safe_reply(interaction, "✅ Order placed.", ephemeral=True)


# ----------------------------
# Stock / item management modals
# ----------------------------
class AdjustStockModal(discord.ui.Modal):
    def __init__(self, cog: "ShopCog", shop_key: str, item_id: str, mode: str):
        super().__init__(title=f"{mode.title()} Stock ({SHOPS[shop_key]['label']})")
        self.cog      = cog
        self.shop_key = shop_key
        self.item_id  = item_id
        self.mode     = mode
        self.amount   = discord.ui.TextInput(label="Quantity", placeholder="Enter a number", required=True)
        self.add_item(self.amount)

    async def on_submit(self, interaction: discord.Interaction):
        if not is_manager(interaction.user):
            await safe_reply(interaction, "❌ Not authorized.", ephemeral=True)
            return
        await safe_defer(interaction, ephemeral=True)

        try:
            amt = int(str(self.amount.value).strip())
            if amt <= 0:
                raise ValueError
        except ValueError:
            await safe_reply(interaction, "❌ Quantity must be a positive whole number.", ephemeral=True)
            return

        async with SHOP_LOCK:
            shop  = await aload_shop(self.shop_key)
            items = shop["items"]
            if self.item_id not in items:
                await safe_reply(interaction, "❌ This item no longer exists.", ephemeral=True)
                return
            item = items[self.item_id]
            if self.mode == "add":
                item["stock"] = int(item.get("stock", 0)) + amt
            else:
                item["stock"] = max(0, int(item.get("stock", 0)) - amt)
            items[self.item_id] = item
            await asave_shop(self.shop_key, shop)

        if interaction.guild:
            await self.cog.update_item_messages(interaction.guild, self.shop_key, self.item_id)
        await safe_reply(interaction, "✅ Stock updated.", ephemeral=True)


class UpdateItemModal(discord.ui.Modal):
    def __init__(self, cog: "ShopCog", shop_key: str, item_id: str, item: dict):
        super().__init__(title=f"Update Item ({SHOPS[shop_key]['label']})")
        self.cog      = cog
        self.shop_key = shop_key
        self.item_id  = item_id

        self.name  = discord.ui.TextInput(label="Item Name",       default=item.get("name", ""),       required=True)
        self.desc  = discord.ui.TextInput(label="Description",     style=discord.TextStyle.long,
                                           default=item.get("desc", ""),       required=True)
        self.price = discord.ui.TextInput(label="Price (AP)",       default=str(item.get("price", 0)), required=True)
        self.image = discord.ui.TextInput(label="Image Link (URL)", default=item.get("image") or "",   required=False)

        for field in (self.name, self.desc, self.price, self.image):
            self.add_item(field)

    async def on_submit(self, interaction: discord.Interaction):
        if not is_manager(interaction.user):
            await safe_reply(interaction, "❌ Not authorized.", ephemeral=True)
            return
        await safe_defer(interaction, ephemeral=True)

        try:
            price = int(str(self.price.value).strip())
            if price < 0:
                raise ValueError
        except ValueError:
            await safe_reply(interaction, "❌ Price must be a non-negative integer.", ephemeral=True)
            return

        image_url = str(self.image.value).strip() or None
        if image_url and not is_valid_image_url(image_url):
            await safe_reply(interaction,
                "❌ Invalid image URL. Must be a valid `http`/`https` URL and under 2048 characters.",
                ephemeral=True)
            return

        async with SHOP_LOCK:
            shop  = await aload_shop(self.shop_key)
            items = shop["items"]
            if self.item_id not in items:
                await safe_reply(interaction, "❌ This item no longer exists.", ephemeral=True)
                return
            item = items[self.item_id]
            item["name"]  = str(self.name.value).strip()
            item["desc"]  = str(self.desc.value).strip()
            item["price"] = price
            item["image"] = image_url
            items[self.item_id] = item
            await asave_shop(self.shop_key, shop)

        if interaction.guild:
            await self.cog.update_item_messages(interaction.guild, self.shop_key, self.item_id)
        await safe_reply(interaction, "✅ Item updated.", ephemeral=True)


class AddNewItemModal(discord.ui.Modal):
    def __init__(self, cog: "ShopCog", shop_key: str):
        super().__init__(title=f"Add New Item ({SHOPS[shop_key]['label']})")
        self.cog      = cog
        self.shop_key = shop_key

        self.name  = discord.ui.TextInput(label="Item Name",             required=True)
        self.desc  = discord.ui.TextInput(label="Item Description",      style=discord.TextStyle.long, required=True)
        self.price = discord.ui.TextInput(label="Item Price (AP)",       required=True)
        self.image = discord.ui.TextInput(label="Item Image Link (URL)", required=False)

        for field in (self.name, self.desc, self.price, self.image):
            self.add_item(field)

    async def on_submit(self, interaction: discord.Interaction):
        if not is_manager(interaction.user):
            await safe_reply(interaction, "❌ Not authorized.", ephemeral=True)
            return
        await safe_defer(interaction, ephemeral=True)

        try:
            price = int(str(self.price.value).strip())
            if price < 0:
                raise ValueError
        except ValueError:
            await safe_reply(interaction, "❌ Price must be a non-negative integer.", ephemeral=True)
            return

        image_url = str(self.image.value).strip() or None
        if image_url and not is_valid_image_url(image_url):
            await safe_reply(interaction,
                "❌ Invalid image URL. Must be a valid `http`/`https` URL and under 2048 characters.",
                ephemeral=True)
            return

        async with SHOP_LOCK:
            shop    = await aload_shop(self.shop_key)
            item_id = uuid.uuid4().hex[:10]
            shop["items"][item_id] = {
                "name":  str(self.name.value).strip(),
                "desc":  str(self.desc.value).strip(),
                "price": price,
                "stock": 0,
                "image": image_url,
            }
            await asave_shop(self.shop_key, shop)

        if interaction.guild:
            await self.cog.sync_shop_messages(interaction.guild, self.shop_key)
        await safe_reply(interaction, "✅ New item added.", ephemeral=True)


# ----------------------------
# Persistent Views (restart-safe)
# ----------------------------
class BuyView(discord.ui.View):
    def __init__(self, cog: "ShopCog", shop_key: str, item_id: str, disabled: bool):
        super().__init__(timeout=None)
        self.cog = cog
        self.add_item(discord.ui.Button(
            label="Buy",
            style=discord.ButtonStyle.success,
            custom_id=f"shop:buy:{shop_key}:{item_id}",
            disabled=disabled,
        ))


class ManageView(discord.ui.View):
    def __init__(self, cog: "ShopCog", shop_key: str, item_id: str):
        super().__init__(timeout=None)
        self.cog = cog
        self.add_item(discord.ui.Button(label="Add Stock",    style=discord.ButtonStyle.primary,   custom_id=f"shop:stock_add:{shop_key}:{item_id}"))
        self.add_item(discord.ui.Button(label="Remove Stock", style=discord.ButtonStyle.danger,    custom_id=f"shop:stock_remove:{shop_key}:{item_id}"))
        self.add_item(discord.ui.Button(label="Update Item",  style=discord.ButtonStyle.secondary, custom_id=f"shop:update:{shop_key}:{item_id}"))
        self.add_item(discord.ui.Button(label="Remove Item",  style=discord.ButtonStyle.danger,    custom_id=f"shop:remove:{shop_key}:{item_id}"))


class ShopManagementView(discord.ui.View):
    def __init__(self, cog: "ShopCog", shop_key: str):
        super().__init__(timeout=None)
        self.cog      = cog
        self.shop_key = shop_key
        self.add_item(discord.ui.Button(
            label=f"Add New Item ({SHOPS[shop_key]['label']})",
            style=discord.ButtonStyle.success,
            custom_id=f"shop:add_new:{shop_key}",
        ))


# ----------------------------
# App command permission checks
# ----------------------------
def has_required_role():
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return False
        return is_manager(interaction.user)
    return app_commands.check(predicate)


# ----------------------------
# Cog
# ----------------------------
class ShopCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._registered_order_views: set[str]      = set()
        self._startup_done:           set[int]       = set()
        # OPT-2: message cache keyed by message id — avoids repeated fetch_message per sync
        self._msg_cache: dict[int, discord.Message]  = {}
        # OPT-7: guard that prevents two concurrent syncs for the same shop/guild pair
        self._sync_in_progress: set[str]             = set()

    async def cog_load(self):
        asyncio.create_task(self._startup())

    # ----------------------------
    # OPT-2: Cache helpers
    # ----------------------------
    async def _get_cached_message(self, channel: discord.TextChannel, msg_id: int) -> discord.Message:
        """Return a cached Message, fetching from Discord only on a cache miss."""
        if msg_id in self._msg_cache:
            return self._msg_cache[msg_id]
        msg = await channel.fetch_message(msg_id)
        self._msg_cache[msg_id] = msg
        return msg

    async def _try_delete_message(self, channel: discord.TextChannel, msg_id: int) -> None:
        """Delete a message and evict it from the cache. Silently ignores failures."""
        self._msg_cache.pop(msg_id, None)
        try:
            msg = await channel.fetch_message(msg_id)
            await msg.delete()
        except Exception:
            pass

    # ----------------------------
    # OPT-1: Edit helper — no sleep, no serial lock
    # ----------------------------
    async def safe_edit_if_needed(
        self,
        msg:     discord.Message,
        *,
        content: str | None           = None,
        embed:   discord.Embed | None = None,
        view:    discord.ui.View | None = None,
    ) -> bool:
        """
        Edit a message only when the content or embed actually changed.
        Skipping unchanged messages avoids unnecessary API calls.
        _EDIT_SEMAPHORE caps concurrency to 10; discord.py handles any 429s itself.
        """
        try:
            need_edit = False

            if content is not None and (msg.content or "") != (content or ""):
                need_edit = True

            if embed is not None:
                cur = msg.embeds[0] if msg.embeds else None
                try:
                    if (cur.to_dict() if cur else None) != embed.to_dict():
                        need_edit = True
                except Exception:
                    need_edit = True

            if not need_edit:
                return False

            async with _EDIT_SEMAPHORE:
                await msg.edit(content=content, embed=embed, view=view)

            # Invalidate so the next read fetches the updated object
            self._msg_cache.pop(msg.id, None)
            return True

        except discord.NotFound:
            self._msg_cache.pop(msg.id, None)
            return False
        except Exception:
            return False

    # ----------------------------
    # OPT-7: Startup — 2 s settle, guilds initialised concurrently
    # ----------------------------
    async def _startup(self):
        await self.bot.wait_until_ready()
        await asyncio.sleep(2.0)  # give the gateway a moment to fully settle

        for shop_key in SHOPS:
            self.bot.add_view(ShopManagementView(self, shop_key))

        self.restore_order_views()

        # Each guild is a separate task — they no longer stall each other
        for guild in self.bot.guilds:
            asyncio.create_task(self._init_guild(guild))

    async def _init_guild(self, guild: discord.Guild):
        """Per-guild initialisation, run concurrently for all guilds."""
        if guild.id in self._startup_done:
            return
        self._startup_done.add(guild.id)

        await self.ensure_channels(guild)

        for shop_key in SHOPS:
            try:
                idx       = await aload_index(shop_key)
                gidx      = idx.get(str(guild.id), {})
                items_idx = gidx.get("items") or {}
                if not isinstance(items_idx, dict) or not items_idx:
                    await rebuild_index_from_channels(guild, shop_key)
            except Exception:
                pass
            await self.sync_shop_messages(guild, shop_key)
            # Small gap between the two shops for the same guild to avoid a burst
            # of sends/edits on the same channels in rapid succession.
            await asyncio.sleep(1.0)

    # ----------------------------
    # Persistent interaction router (shop buttons)
    # ----------------------------
    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        try:
            data      = interaction.data or {}
            custom_id = data.get("custom_id")
            if not custom_id or not isinstance(custom_id, str):
                return
            if not custom_id.startswith("shop:"):
                return

            parts    = custom_id.split(":")
            if len(parts) < 3:
                return

            action   = parts[1]
            shop_key = parts[2]
            item_id  = parts[3] if len(parts) >= 4 else None

            if shop_key not in SHOPS:
                return

            if action == "buy" and item_id:
                await safe_send_modal(interaction, BuyItemModal(self, shop_key, item_id))
                return

            if action == "add_new":
                if not is_manager(interaction.user):
                    await safe_reply(interaction, "❌ Not authorized.", ephemeral=True)
                    return
                await safe_send_modal(interaction, AddNewItemModal(self, shop_key))
                return

            if action in ("stock_add", "stock_remove", "update", "remove") and item_id:
                if not is_manager(interaction.user):
                    await safe_reply(interaction, "❌ Not authorized.", ephemeral=True)
                    return

                if action == "stock_add":
                    await safe_send_modal(interaction, AdjustStockModal(self, shop_key, item_id, "add"))
                    return

                if action == "stock_remove":
                    await safe_send_modal(interaction, AdjustStockModal(self, shop_key, item_id, "remove"))
                    return

                if action == "update":
                    shop  = await aload_shop(shop_key)
                    items = shop["items"]
                    if item_id not in items:
                        await safe_reply(interaction, "❌ This item no longer exists.", ephemeral=True)
                        return
                    await safe_send_modal(interaction, UpdateItemModal(self, shop_key, item_id, items[item_id]))
                    return

                if action == "remove":
                    await safe_defer(interaction, ephemeral=True)
                    entry     = None
                    item_name = item_id

                    async with SHOP_LOCK:
                        shop  = await aload_shop(shop_key)
                        items = shop["items"]
                        if item_id not in items:
                            await safe_reply(interaction, "❌ This item no longer exists.", ephemeral=True)
                            return
                        item_name = items[item_id].get("name", item_id)
                        items.pop(item_id, None)
                        await asave_shop(shop_key, shop)

                        if interaction.guild:
                            idx       = await aload_index(shop_key)
                            gidx      = idx.get(str(interaction.guild.id), {})
                            items_idx = gidx.get("items", {})
                            if isinstance(items_idx, dict):
                                entry = items_idx.pop(item_id, None)
                            gidx["items"]                    = items_idx if isinstance(items_idx, dict) else {}
                            idx[str(interaction.guild.id)]   = gidx
                            await asave_index(shop_key, idx)

                    # OPT-5: Delete just the two item messages — no full sync needed.
                    if interaction.guild and isinstance(entry, dict):
                        shop_ch   = discord.utils.get(interaction.guild.text_channels, name=SHOPS[shop_key]["shop_channel"])
                        access_ch = discord.utils.get(interaction.guild.text_channels, name=SHOPS[shop_key]["access_channel"])
                        deletions = []
                        if shop_ch and entry.get("shop_msg_id"):
                            deletions.append(self._try_delete_message(shop_ch,   int(entry["shop_msg_id"])))
                        if access_ch and entry.get("access_msg_id"):
                            deletions.append(self._try_delete_message(access_ch, int(entry["access_msg_id"])))
                        if deletions:
                            await asyncio.gather(*deletions, return_exceptions=True)

                    await safe_reply(interaction, f"🗑️ Removed **{item_name}** from **{SHOPS[shop_key]['label']}** shop.", ephemeral=True)
                    return

        except Exception:
            return

    # ----------------------------
    # Order persistence
    # ----------------------------
    def restore_order_views(self):
        data = load_orders()
        for order_id in (data.get("orders", {}) or {}).keys():
            self.register_order_view(order_id)

    def register_order_view(self, order_id: str):
        if order_id in self._registered_order_views:
            return
        self.bot.add_view(OrderStatusView(self, order_id))
        self._registered_order_views.add(order_id)

    # ----------------------------
    # Channel setup (PERM-SAFE FOR @everyone)
    # ----------------------------
    async def _patch_channel_overwrites_preserve_everyone(
        self,
        channel: discord.TextChannel,
        guild:   discord.Guild,
        *,
        bot_overwrite:   discord.PermissionOverwrite | None,
        role_overwrites: dict[discord.Role, discord.PermissionOverwrite] | None = None,
    ) -> None:
        """
        Updates overwrites WITHOUT touching @everyone's overwrite entry.
        """
        try:
            current        = dict(channel.overwrites)
            everyone       = guild.default_role
            everyone_entry = current.get(everyone, None)

            if bot_overwrite is not None:
                me = guild.me
                if me:
                    current[me] = bot_overwrite

            if role_overwrites:
                for role, ow in role_overwrites.items():
                    current[role] = ow

            if everyone_entry is not None:
                current[everyone] = everyone_entry
            else:
                current.pop(everyone, None)

            await channel.edit(overwrites=current)
        except discord.Forbidden:
            pass
        except Exception:
            pass

    async def ensure_channels(self, guild: discord.Guild):
        everyone = guild.default_role
        me       = guild.me

        bot_manage = None
        if me:
            bot_manage = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                manage_messages=True,
                read_message_history=True,
            )

        # ---- Orders channels (main + hs) ----
        for order_name in set(ORDER_LOG_CHANNELS.values()):
            orders_ch = discord.utils.get(guild.text_channels, name=order_name)
            if not orders_ch:
                overwrites: dict = {
                    everyone: discord.PermissionOverwrite(view_channel=True, send_messages=False, add_reactions=False),
                }
                if me:
                    overwrites[me] = bot_manage
                try:
                    await guild.create_text_channel(order_name, overwrites=overwrites)
                except (discord.Forbidden, Exception):
                    pass
            else:
                if bot_manage:
                    await self._patch_channel_overwrites_preserve_everyone(orders_ch, guild, bot_overwrite=bot_manage)

        # ---- Per-shop channels (display + access) ----
        for shop_key, cfg in SHOPS.items():
            shop_name   = cfg["shop_channel"]
            access_name = cfg["access_channel"]

            shop_ch = discord.utils.get(guild.text_channels, name=shop_name)
            if not shop_ch:
                overwrites = {
                    everyone: discord.PermissionOverwrite(view_channel=True, send_messages=False, add_reactions=False),
                }
                if me:
                    overwrites[me] = bot_manage
                try:
                    await guild.create_text_channel(shop_name, overwrites=overwrites)
                except (discord.Forbidden, Exception):
                    pass
            else:
                if bot_manage:
                    await self._patch_channel_overwrites_preserve_everyone(shop_ch, guild, bot_overwrite=bot_manage)

            access_ch = discord.utils.get(guild.text_channels, name=access_name)
            if not access_ch:
                overwrites = {everyone: discord.PermissionOverwrite(view_channel=False)}
                if me:
                    overwrites[me] = bot_manage
                for role_name in ALLOWED_ROLES:
                    role = discord.utils.get(guild.roles, name=role_name)
                    if role:
                        overwrites[role] = discord.PermissionOverwrite(
                            view_channel=True, send_messages=True, read_message_history=True
                        )
                try:
                    await guild.create_text_channel(access_name, overwrites=overwrites)
                except (discord.Forbidden, Exception):
                    pass
            else:
                role_ows: dict[discord.Role, discord.PermissionOverwrite] = {}
                for role_name in ALLOWED_ROLES:
                    role = discord.utils.get(guild.roles, name=role_name)
                    if role:
                        role_ows[role] = discord.PermissionOverwrite(
                            view_channel=True, send_messages=True, read_message_history=True
                        )
                if bot_manage:
                    await self._patch_channel_overwrites_preserve_everyone(
                        access_ch, guild, bot_overwrite=bot_manage, role_overwrites=role_ows
                    )

    # ----------------------------
    # OPT-2 + OPT-3: send/edit helper used by the concurrent sync
    # ----------------------------
    async def _upsert_msg(
        self,
        channel: discord.TextChannel,
        msg_id:  int | None,
        embed:   discord.Embed,
        view:    discord.ui.View,
    ) -> int:
        """
        Look up the message (cache-first), edit it if needed, or send a new one.
        Returns the final message id.
        """
        msg = None
        if msg_id:
            try:
                msg = await self._get_cached_message(channel, int(msg_id))
            except discord.NotFound:
                self._msg_cache.pop(int(msg_id), None)
            except Exception:
                pass

        if msg is None:
            async with _EDIT_SEMAPHORE:
                msg = await channel.send(embed=embed, view=view)
            self._msg_cache[msg.id] = msg
            return msg.id

        await self.safe_edit_if_needed(msg, embed=embed, view=view)
        return msg.id

    # ----------------------------
    # OPT-3: Concurrent shop message sync
    # ----------------------------
    async def sync_shop_messages(self, guild: discord.Guild, shop_key: str):
        # Guard: skip if a sync for this shop+guild is already running
        sync_key = f"{guild.id}:{shop_key}"
        if sync_key in self._sync_in_progress:
            return
        self._sync_in_progress.add(sync_key)

        try:
            shop_ch   = discord.utils.get(guild.text_channels, name=SHOPS[shop_key]["shop_channel"])
            access_ch = discord.utils.get(guild.text_channels, name=SHOPS[shop_key]["access_channel"])
            if not shop_ch or not access_ch:
                return

            # Load data under the lock (fast — just file reads)
            async with SHOP_LOCK:
                shop  = await aload_shop(shop_key)
                items = shop["items"]
                idx   = await aload_index(shop_key)
                gkey  = str(guild.id)
                gidx  = idx.setdefault(gkey, {})
                gidx.setdefault("items", {})
                items_idx = gidx["items"]
                if not isinstance(items_idx, dict):
                    items_idx = {}
                    gidx["items"] = items_idx

                if not items_idx:
                    try:
                        await rebuild_index_from_channels(guild, shop_key)
                    except Exception:
                        pass
                    idx       = await aload_index(shop_key)
                    gidx      = idx.setdefault(gkey, {})
                    gidx.setdefault("items", {})
                    items_idx = gidx["items"]
                    if not isinstance(items_idx, dict):
                        items_idx = {}
                        gidx["items"] = items_idx

            # OPT-3: All items synced concurrently.
            #         Within each item, shop + access messages are also updated in parallel.
            async def _sync_one(item_id: str, item: dict) -> tuple[str, int, int]:
                embed        = build_item_embed(shop_key, item_id, item)
                out_of_stock = int(item.get("stock", 0)) <= 0
                entry = items_idx.get(item_id)
                if not isinstance(entry, dict):
                    entry = {}

                shop_id, access_id = await asyncio.gather(
                    self._upsert_msg(shop_ch,   entry.get("shop_msg_id"),   embed, BuyView(self, shop_key, item_id, disabled=out_of_stock)),
                    self._upsert_msg(access_ch, entry.get("access_msg_id"), embed, ManageView(self, shop_key, item_id)),
                )
                return item_id, int(shop_id), int(access_id)

            results = await asyncio.gather(
                *[_sync_one(iid, itm) for iid, itm in items.items()],
                return_exceptions=True,
            )
            for r in results:
                if isinstance(r, Exception):
                    continue
                item_id, shop_id, access_id = r
                items_idx[item_id] = {"shop_msg_id": shop_id, "access_msg_id": access_id}

            # Management message
            mgmt_id         = gidx.get("management_msg_id")
            desired_content = f"**Shop Management ({SHOPS[shop_key]['label']})**"
            desired_view    = ShopManagementView(self, shop_key)
            mgmt_msg        = None
            if mgmt_id:
                try:
                    mgmt_msg = await self._get_cached_message(access_ch, int(mgmt_id))
                except Exception:
                    mgmt_msg = None

            if mgmt_msg is None:
                async with _EDIT_SEMAPHORE:
                    mgmt_msg = await access_ch.send(desired_content, view=desired_view)
                self._msg_cache[mgmt_msg.id] = mgmt_msg
                gidx["management_msg_id"] = mgmt_msg.id
            else:
                await self.safe_edit_if_needed(mgmt_msg, content=desired_content, embed=None, view=desired_view)

            gidx["items"] = items_idx
            idx[gkey]     = gidx
            await asave_index(shop_key, idx)

        finally:
            self._sync_in_progress.discard(sync_key)

    async def update_item_messages(self, guild: discord.Guild, shop_key: str, item_id: str):
        shop  = await aload_shop(shop_key)
        items = shop["items"]
        if item_id not in items:
            await self.sync_shop_messages(guild, shop_key)
            return

        item         = items[item_id]
        embed        = build_item_embed(shop_key, item_id, item)
        out_of_stock = int(item.get("stock", 0)) <= 0

        idx       = await aload_index(shop_key)
        gidx      = idx.get(str(guild.id), {})
        items_idx = (gidx.get("items") or {})
        entry     = items_idx.get(item_id)

        shop_ch   = discord.utils.get(guild.text_channels, name=SHOPS[shop_key]["shop_channel"])
        access_ch = discord.utils.get(guild.text_channels, name=SHOPS[shop_key]["access_channel"])
        if not shop_ch or not access_ch or not isinstance(entry, dict):
            await self.sync_shop_messages(guild, shop_key)
            return

        try:
            shop_msg   = await self._get_cached_message(shop_ch,   int(entry["shop_msg_id"]))
            access_msg = await self._get_cached_message(access_ch, int(entry["access_msg_id"]))
        except Exception:
            await self.sync_shop_messages(guild, shop_key)
            return

        # Both messages updated concurrently
        await asyncio.gather(
            self.safe_edit_if_needed(shop_msg,   embed=embed, view=BuyView(self, shop_key, item_id, disabled=out_of_stock)),
            self.safe_edit_if_needed(access_msg, embed=embed, view=ManageView(self, shop_key, item_id)),
        )

    async def refresh_order_message(self, guild: discord.Guild, order_id: str):
        data = await aload_orders()
        o    = (data.get("orders") or {}).get(order_id)
        if not o or not o.get("message_id"):
            return

        ch_name   = str(o.get("channel_name") or get_order_channel_name(str(o.get("shop_key") or "main")))
        orders_ch = discord.utils.get(guild.text_channels, name=ch_name)
        if not orders_ch:
            return

        try:
            msg = await self._get_cached_message(orders_ch, int(o["message_id"]))
        except Exception:
            return

        self.register_order_view(order_id)
        status = str(o.get("status", "PENDING"))
        await self.safe_edit_if_needed(msg, embed=build_order_embed(o), view=OrderStatusView(self, order_id, status))

    # ----------------------------
    # Slash Command
    # ----------------------------
    @app_commands.command(
        name="shop_rebuild",
        description="Sync BOTH shops (main + HS) without purging (Shop Steward / Admin Council only)."
    )
    @has_required_role()
    async def shop_rebuild(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)
        if not interaction.guild:
            await safe_reply(interaction, "❌ Must be used in a server.", ephemeral=True)
            return

        await self.ensure_channels(interaction.guild)

        # OPT-3: Both shops synced concurrently instead of sequentially
        await asyncio.gather(*[
            self.sync_shop_messages(interaction.guild, sk)
            for sk in SHOPS
        ])

        await safe_reply(interaction, "✅ Shops synced (main + HS), no purge.", ephemeral=True)

    @shop_rebuild.error
    async def shop_rebuild_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.CheckFailure):
            await safe_reply(interaction, "❌ Not authorized.", ephemeral=True)
            return
        await safe_reply(interaction, "❌ An error occurred.", ephemeral=True)
        raise error


async def setup(bot: commands.Bot):
    await bot.add_cog(ShopCog(bot))