# cogs/shop.py
#
# MULTI-SHOP VERSION (Main + HS)
# - Keeps your original goals (no purge, persistent views, atomic JSON, minimal edits)
# - Adds a SECOND, SEPARATE shop:
#     Shop display:     ap-eve-shop-hs
#     Shop controls:    ap-shop-access-hs
#
# IMPORTANT:
# - This is a truly separate inventory (separate shop_hs.json)
# - Custom IDs are namespaced per shop (main vs hs) to avoid collisions.
#
# Channels:
#   MAIN: ap-eve-shop            (display)  | ap-shop-access            (controls)
#   HS:   ap-eve-shop-hs         (display)  | ap-shop-access-hs         (controls)
# Orders (unchanged): ap-shop-orders

import os
import discord
import json
import asyncio
import uuid
import datetime
from pathlib import Path
from discord.ext import commands
from discord import app_commands

# ----------------------------
# Persistence root (Railway)
# ----------------------------
PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)

# Separate shop inventories (main + hs)
SHOP_FILES = {
    "main": PERSIST_ROOT / "shop.json",
    "hs": PERSIST_ROOT / "shop_hs.json",
}

AP_FILE = PERSIST_ROOT / "ap_data.json"
ORDERS_FILE = PERSIST_ROOT / "shop_orders.json"

# Separate index per shop (to keep things clean + independent)
INDEX_FILES = {
    "main": PERSIST_ROOT / "shop_message_index.json",
    "hs": PERSIST_ROOT / "shop_message_index_hs.json",
}

for p in list(SHOP_FILES.values()) + [AP_FILE, ORDERS_FILE] + list(INDEX_FILES.values()):
    p.parent.mkdir(parents=True, exist_ok=True)

# ----------------------------
# Channel config
# ----------------------------
ORDER_LOG_CHANNEL = "ap-shop-orders"

SHOPS = {
    "main": {
        "label": "Main",
        "shop_channel": "ap-eve-shop",
        "access_channel": "ap-shop-access",
    },
    "hs": {
        "label": "HS",
        "shop_channel": "ap-eve-shop-hs",
        "access_channel": "ap-shop-access-hs",
    }
}

ALLOWED_ROLES = {"Shop Steward", "ARC Security Administration Council"}

SHOP_LOCK = asyncio.Lock()

# Global spacing between ANY actual edits (only applied when edit needed)
GLOBAL_EDIT_MIN_INTERVAL_SECONDS = 1.2

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
    try:
        if not path.exists():
            return default
        raw = path.read_text(encoding="utf-8").strip()
        if not raw:
            return default
        return json.loads(raw)
    except Exception:
        return default


def _save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=4), encoding="utf-8")
    tmp.replace(path)


def ensure_shop_schema(data: dict) -> dict:
    if not isinstance(data, dict):
        data = {}
    data.setdefault("items", {})
    if not isinstance(data["items"], dict):
        data["items"] = {}
    return data


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
    # {
    #   "<guild_id>": {
    #      "items": {
    #         "<item_id>": {"shop_msg_id": 123, "access_msg_id": 456}
    #      },
    #      "management_msg_id": 789
    #   }
    # }
    return _load_json(INDEX_FILES[shop_key], {})


def save_index(shop_key: str, data: dict) -> None:
    _save_json(INDEX_FILES[shop_key], data)


def utc_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def is_manager(member: discord.abc.User | discord.Member) -> bool:
    if not isinstance(member, discord.Member):
        return False
    return any(r.name in ALLOWED_ROLES for r in member.roles)


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
    if item.get("image"):
        embed.set_image(url=item["image"])

    # Stable footer for recovery (include shop_key)
    embed.set_footer(text=f"Cryonic Gaming Shop({shop_key}) | id:{item_id}")
    return embed


def build_order_embed(order: dict) -> discord.Embed:
    status = order.get("status", "PENDING")
    title = f"Order {order.get('order_id', '')} — {status}"

    embed = discord.Embed(title=title, timestamp=discord.utils.utcnow())
    embed.add_field(name="Buyer", value=f"<@{order.get('buyer_id')}> ({order.get('buyer_tag')})", inline=False)
    embed.add_field(name="Shop", value=str(order.get("shop_key", "main")), inline=True)
    embed.add_field(name="Item", value=str(order.get("item_name", "Unknown")), inline=True)
    embed.add_field(name="Quantity", value=str(order.get("qty", 0)), inline=True)
    embed.add_field(name="IGN", value=f"`{order.get('ign', '')}`", inline=True)
    embed.add_field(name="Cost", value=f"{order.get('cost', 0)} AP", inline=True)
    embed.add_field(name="Created", value=order.get("created_at", ""), inline=True)

    if status == "DELIVERED":
        embed.add_field(name="Delivered By", value=f"<@{order.get('delivered_by')}>", inline=True)
        embed.add_field(name="Delivered At", value=order.get("delivered_at", ""), inline=True)

    if status == "UNDELIVERED":
        embed.add_field(name="Marked Undelivered By", value=f"<@{order.get('undelivered_by')}>", inline=True)
        embed.add_field(name="Undelivered At", value=order.get("undelivered_at", ""), inline=True)
        reason = order.get("undelivered_reason") or "No reason provided."
        embed.add_field(name="Reason", value=reason[:1024], inline=False)

    embed.set_footer(text="Shop Orders")
    return embed


# ----------------------------
# Index recovery (best-effort)
# ----------------------------
async def rebuild_index_from_channels(guild: discord.Guild, shop_key: str) -> None:
    shop_ch = discord.utils.get(guild.text_channels, name=SHOPS[shop_key]["shop_channel"])
    access_ch = discord.utils.get(guild.text_channels, name=SHOPS[shop_key]["access_channel"])
    if not shop_ch or not access_ch:
        return

    idx = load_index(shop_key)
    gkey = str(guild.id)
    gidx = idx.setdefault(gkey, {})
    gidx.setdefault("items", {})
    items_idx: dict = gidx["items"]

    async def scan_channel(ch: discord.TextChannel, key_name: str):
        async for msg in ch.history(limit=250):
            try:
                me = guild.me  # type: ignore
                if not me or not msg.author or msg.author.id != me.id:
                    continue
                if not msg.embeds:
                    continue
                e = msg.embeds[0]
                footer_text = (e.footer.text or "") if e.footer else ""
                # Footer contains Cryonic Gaming Shop(shop_key) | id:xxxx
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
                me = guild.me  # type: ignore
                if me and msg.author and msg.author.id == me.id and msg.content.strip() == f"**Shop Management ({SHOPS[shop_key]['label']})**":
                    gidx["management_msg_id"] = int(msg.id)
                    break
            except Exception:
                continue

    gidx["items"] = items_idx
    idx[gkey] = gidx
    save_index(shop_key, idx)


# ----------------------------
# Undelivered Reason Modal
# ----------------------------
class UndeliveredReasonModal(discord.ui.Modal):
    def __init__(self, cog: "ShopCog", order_id: str):
        super().__init__(title="Mark Undelivered")
        self.cog = cog
        self.order_id = order_id

        self.reason = discord.ui.TextInput(
            label="Reason",
            style=discord.TextStyle.long,
            placeholder="Why is this undelivered? (e.g., buyer offline, wrong IGN, etc.)",
            required=True,
            max_length=1000
        )
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        if not is_manager(interaction.user):
            await safe_reply(interaction, "❌ Not authorized.", ephemeral=True)
            return
        await safe_defer(interaction, ephemeral=True)

        async with SHOP_LOCK:
            data = load_orders()
            orders = data.setdefault("orders", {})
            o = orders.get(self.order_id)
            if not o:
                await safe_reply(interaction, "❌ Order not found.", ephemeral=True)
                return

            o["status"] = "UNDELIVERED"
            o["undelivered_by"] = str(interaction.user.id)
            o["undelivered_at"] = utc_iso()
            o["undelivered_reason"] = str(self.reason.value).strip()

            o.pop("delivered_by", None)
            o.pop("delivered_at", None)
            orders[self.order_id] = o
            save_orders(data)

        if interaction.guild:
            await self.cog.refresh_order_message(interaction.guild, self.order_id)
        await safe_reply(interaction, "✅ Marked as UNDELIVERED.", ephemeral=True)


# ----------------------------
# Persistent Order Buttons
# ----------------------------
class DeliveredButton(discord.ui.Button):
    def __init__(self, cog: "ShopCog", order_id: str):
        super().__init__(label="Delivered", style=discord.ButtonStyle.success, custom_id=f"order:delivered:{order_id}")
        self.cog = cog
        self.order_id = order_id

    async def callback(self, interaction: discord.Interaction):
        if not is_manager(interaction.user):
            await safe_reply(interaction, "❌ Not authorized.", ephemeral=True)
            return
        await safe_defer(interaction, ephemeral=True)

        async with SHOP_LOCK:
            data = load_orders()
            orders = data.setdefault("orders", {})
            o = orders.get(self.order_id)
            if not o:
                await safe_reply(interaction, "❌ Order not found.", ephemeral=True)
                return

            o["status"] = "DELIVERED"
            o["delivered_by"] = str(interaction.user.id)
            o["delivered_at"] = utc_iso()

            o.pop("undelivered_by", None)
            o.pop("undelivered_at", None)
            o.pop("undelivered_reason", None)
            orders[self.order_id] = o
            save_orders(data)

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
        self.cog = cog
        self.order_id = order_id

    async def callback(self, interaction: discord.Interaction):
        if not is_manager(interaction.user):
            await safe_reply(interaction, "❌ Not authorized.", ephemeral=True)
            return
        await safe_send_modal(interaction, UndeliveredReasonModal(self.cog, self.order_id))


class UndoButton(discord.ui.Button):
    def __init__(self, cog: "ShopCog", order_id: str):
        super().__init__(label="Undo", style=discord.ButtonStyle.secondary, custom_id=f"order:undo:{order_id}")
        self.cog = cog
        self.order_id = order_id

    async def callback(self, interaction: discord.Interaction):
        if not is_manager(interaction.user):
            await safe_reply(interaction, "❌ Not authorized.", ephemeral=True)
            return
        await safe_defer(interaction, ephemeral=True)

        async with SHOP_LOCK:
            data = load_orders()
            orders = data.setdefault("orders", {})
            o = orders.get(self.order_id)
            if not o:
                await safe_reply(interaction, "❌ Order not found.", ephemeral=True)
                return

            o["status"] = "PENDING"
            o.pop("delivered_by", None)
            o.pop("delivered_at", None)
            o.pop("undelivered_by", None)
            o.pop("undelivered_at", None)
            o.pop("undelivered_reason", None)

            orders[self.order_id] = o
            save_orders(data)

        if interaction.guild:
            await self.cog.refresh_order_message(interaction.guild, self.order_id)
        await safe_reply(interaction, "✅ Status reset to PENDING.", ephemeral=True)


class OrderStatusView(discord.ui.View):
    def __init__(self, cog: "ShopCog", order_id: str):
        super().__init__(timeout=None)
        self.add_item(DeliveredButton(cog, order_id))
        self.add_item(UndeliveredButton(cog, order_id))
        self.add_item(UndoButton(cog, order_id))


# ----------------------------
# Buy modal (Quantity + IGN)
# ----------------------------
class BuyItemModal(discord.ui.Modal):
    def __init__(self, cog: "ShopCog", shop_key: str, item_id: str):
        super().__init__(title=f"Buy Item ({SHOPS[shop_key]['label']})")
        self.cog = cog
        self.shop_key = shop_key
        self.item_id = item_id

        self.qty = discord.ui.TextInput(label="Quantity", placeholder="Enter a number (e.g., 1)", required=True)
        self.ign = discord.ui.TextInput(label="In-Game Name (IGN)", placeholder="Enter your IGN", required=True, max_length=32)
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

        async with SHOP_LOCK:
            shop = load_shop(self.shop_key)
            items = shop["items"]

            if self.item_id not in items:
                await safe_reply(interaction, "❌ This item no longer exists.", ephemeral=True)
                return

            item = items[self.item_id]
            stock = int(item.get("stock", 0))
            if stock <= 0:
                await safe_reply(interaction, "❌ Out of stock.", ephemeral=True)
                return
            if qty > stock:
                await safe_reply(interaction, f"❌ Not enough stock (requested {qty}, available {stock}).", ephemeral=True)
                return

            ap_data = load_ap()
            uid = str(interaction.user.id)
            user_entry = ap_data.get(uid)
            if not user_entry or "ap" not in user_entry:
                await safe_reply(interaction, "❌ You have no AP account.", ephemeral=True)
                return

            price = int(item.get("price", 0))
            total_cost = price * qty
            user_ap = int(float(user_entry.get("ap", 0)))
            if user_ap < total_cost:
                await safe_reply(interaction, f"❌ Not enough AP (cost {total_cost}, you have {user_ap}).", ephemeral=True)
                return

            # Apply AP + stock
            user_entry["ap"] = user_ap - total_cost
            item["stock"] = stock - qty
            items[self.item_id] = item
            ap_data[uid] = user_entry
            save_ap(ap_data)
            save_shop(self.shop_key, shop)

            # Create order record
            order_id = uuid.uuid4().hex[:10]
            order = {
                "order_id": order_id,
                "status": "PENDING",
                "created_at": utc_iso(),
                "guild_id": str(interaction.guild.id) if interaction.guild else None,
                "channel_name": ORDER_LOG_CHANNEL,
                "message_id": None,
                "buyer_id": str(interaction.user.id),
                "buyer_tag": str(interaction.user),
                "shop_key": self.shop_key,
                "item_id": self.item_id,
                "item_name": item.get("name", self.item_id),
                "qty": qty,
                "ign": ign,
                "cost": total_cost,
            }

            orders_data = load_orders()
            orders = orders_data.setdefault("orders", {})
            orders[order_id] = order
            save_orders(orders_data)

        if not interaction.guild:
            await safe_reply(interaction, "❌ Guild context missing.", ephemeral=True)
            return

        order_ch = discord.utils.get(interaction.guild.text_channels, name=ORDER_LOG_CHANNEL)
        if not order_ch:
            await safe_reply(interaction, "❌ Order channel not found. Contact staff.", ephemeral=True)
            return

        # Ensure persistent order view exists for this order
        self.cog.register_order_view(order_id)

        embed = build_order_embed(order)
        msg = await order_ch.send(embed=embed, view=OrderStatusView(self.cog, order_id))

        async with SHOP_LOCK:
            orders_data = load_orders()
            orders = orders_data.setdefault("orders", {})
            if order_id in orders:
                orders[order_id]["message_id"] = str(msg.id)
                save_orders(orders_data)

        # Update this item's embeds in the correct shop
        await self.cog.update_item_messages(interaction.guild, self.shop_key, self.item_id)

        await safe_reply(interaction, "✅ Order placed.", ephemeral=True)


# ----------------------------
# Stock / item management modals
# ----------------------------
class AdjustStockModal(discord.ui.Modal):
    def __init__(self, cog: "ShopCog", shop_key: str, item_id: str, mode: str):
        super().__init__(title=f"{mode.title()} Stock ({SHOPS[shop_key]['label']})")
        self.cog = cog
        self.shop_key = shop_key
        self.item_id = item_id
        self.mode = mode
        self.amount = discord.ui.TextInput(label="Quantity", placeholder="Enter a number", required=True)
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
            shop = load_shop(self.shop_key)
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
            save_shop(self.shop_key, shop)

        if interaction.guild:
            await self.cog.update_item_messages(interaction.guild, self.shop_key, self.item_id)
        await safe_reply(interaction, "✅ Stock updated.", ephemeral=True)


class UpdateItemModal(discord.ui.Modal):
    def __init__(self, cog: "ShopCog", shop_key: str, item_id: str, item: dict):
        super().__init__(title=f"Update Item ({SHOPS[shop_key]['label']})")
        self.cog = cog
        self.shop_key = shop_key
        self.item_id = item_id

        self.name = discord.ui.TextInput(label="Item Name", default=item.get("name", ""), required=True)
        self.desc = discord.ui.TextInput(label="Description", style=discord.TextStyle.long, default=item.get("desc", ""), required=True)
        self.price = discord.ui.TextInput(label="Price (AP)", default=str(item.get("price", 0)), required=True)
        self.image = discord.ui.TextInput(label="Image Link (URL)", default=item.get("image") or "", required=False)

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

        async with SHOP_LOCK:
            shop = load_shop(self.shop_key)
            items = shop["items"]
            if self.item_id not in items:
                await safe_reply(interaction, "❌ This item no longer exists.", ephemeral=True)
                return

            item = items[self.item_id]
            item["name"] = str(self.name.value).strip()
            item["desc"] = str(self.desc.value).strip()
            item["price"] = price
            item["image"] = str(self.image.value).strip() or None

            items[self.item_id] = item
            save_shop(self.shop_key, shop)

        if interaction.guild:
            await self.cog.update_item_messages(interaction.guild, self.shop_key, self.item_id)
        await safe_reply(interaction, "✅ Item updated.", ephemeral=True)


class AddNewItemModal(discord.ui.Modal):
    def __init__(self, cog: "ShopCog", shop_key: str):
        super().__init__(title=f"Add New Item ({SHOPS[shop_key]['label']})")
        self.cog = cog
        self.shop_key = shop_key

        self.name = discord.ui.TextInput(label="Item Name", required=True)
        self.desc = discord.ui.TextInput(label="Item Description", style=discord.TextStyle.long, required=True)
        self.price = discord.ui.TextInput(label="Item Price (AP)", required=True)
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

        async with SHOP_LOCK:
            shop = load_shop(self.shop_key)
            items = shop["items"]

            item_id = uuid.uuid4().hex[:10]
            items[item_id] = {
                "name": str(self.name.value).strip(),
                "desc": str(self.desc.value).strip(),
                "price": price,
                "stock": 0,
                "image": str(self.image.value).strip() or None
            }
            save_shop(self.shop_key, shop)

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
            disabled=disabled
        ))


class ManageView(discord.ui.View):
    def __init__(self, cog: "ShopCog", shop_key: str, item_id: str):
        super().__init__(timeout=None)
        self.cog = cog
        self.add_item(discord.ui.Button(label="Add Stock", style=discord.ButtonStyle.primary, custom_id=f"shop:stock_add:{shop_key}:{item_id}"))
        self.add_item(discord.ui.Button(label="Remove Stock", style=discord.ButtonStyle.danger, custom_id=f"shop:stock_remove:{shop_key}:{item_id}"))
        self.add_item(discord.ui.Button(label="Update Item", style=discord.ButtonStyle.secondary, custom_id=f"shop:update:{shop_key}:{item_id}"))
        self.add_item(discord.ui.Button(label="Remove Item", style=discord.ButtonStyle.danger, custom_id=f"shop:remove:{shop_key}:{item_id}"))


class ShopManagementView(discord.ui.View):
    def __init__(self, cog: "ShopCog", shop_key: str):
        super().__init__(timeout=None)
        self.cog = cog
        self.shop_key = shop_key
        self.add_item(discord.ui.Button(
            label=f"Add New Item ({SHOPS[shop_key]['label']})",
            style=discord.ButtonStyle.success,
            custom_id=f"shop:add_new:{shop_key}"
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
        self._registered_order_views: set[str] = set()
        self._startup_done: set[int] = set()

        # global edit rate limiter
        self._edit_lock = asyncio.Lock()
        self._global_last_edit_at = 0.0

    async def cog_load(self):
        asyncio.create_task(self._startup())

    # ----------------------------
    # Compare helpers (NO EDIT unless needed)
    # ----------------------------
    def _embed_to_dict(self, e: discord.Embed | None) -> dict | None:
        if not e:
            return None
        try:
            return e.to_dict()
        except Exception:
            return None

    async def safe_edit_if_needed(
        self,
        msg: discord.Message,
        *,
        content: str | None = None,
        embed: discord.Embed | None = None,
        view: discord.ui.View | None = None,
    ) -> bool:
        """
        Edits ONLY if needed:
        - content differs (when provided)
        - embed differs (when provided)
        View edits are skipped by default because persistent views are registered on startup.
        Also includes a GLOBAL spacing between edits when an edit is necessary.
        """
        try:
            need_edit = False

            if content is not None:
                if (msg.content or "") != (content or ""):
                    need_edit = True

            if embed is not None:
                cur = msg.embeds[0] if msg.embeds else None
                if self._embed_to_dict(cur) != self._embed_to_dict(embed):
                    need_edit = True

            if not need_edit:
                return False

            async with self._edit_lock:
                now = asyncio.get_running_loop().time()
                wait_for = (self._global_last_edit_at + GLOBAL_EDIT_MIN_INTERVAL_SECONDS) - now
                if wait_for > 0:
                    await asyncio.sleep(wait_for)

                await msg.edit(content=content, embed=embed, view=view)

                self._global_last_edit_at = asyncio.get_running_loop().time()
                return True

        except Exception:
            return False

    async def _startup(self):
        await self.bot.wait_until_ready()
        await asyncio.sleep(1.0)

        # Register persistent views
        # - management views (one per shop)
        for shop_key in SHOPS.keys():
            self.bot.add_view(ShopManagementView(self, shop_key))

        # - order views
        self.restore_order_views()

        for guild in self.bot.guilds:
            if guild.id in self._startup_done:
                continue
            self._startup_done.add(guild.id)

            await self.ensure_channels(guild)

            # recover indexes + sync both shops
            for shop_key in SHOPS.keys():
                try:
                    idx = load_index(shop_key)
                    gidx = idx.get(str(guild.id), {})
                    items_idx = (gidx.get("items") or {})
                    if not isinstance(items_idx, dict) or not items_idx:
                        await rebuild_index_from_channels(guild, shop_key)
                except Exception:
                    pass

                await self.sync_shop_messages(guild, shop_key)

    # ----------------------------
    # Persistent interaction router (shop buttons)
    # ----------------------------
    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        try:
            data = interaction.data or {}
            custom_id = data.get("custom_id")
            if not custom_id or not isinstance(custom_id, str):
                return
            if not custom_id.startswith("shop:"):
                return

            parts = custom_id.split(":")
            # Expected:
            # shop:buy:<shop_key>:<item_id>
            # shop:add_new:<shop_key>
            # shop:stock_add:<shop_key>:<item_id> etc
            if len(parts) < 3:
                return

            action = parts[1]
            shop_key = parts[2]
            item_id = parts[3] if len(parts) >= 4 else None

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
                    shop = load_shop(shop_key)
                    items = shop["items"]
                    if item_id not in items:
                        await safe_reply(interaction, "❌ This item no longer exists.", ephemeral=True)
                        return
                    await safe_send_modal(interaction, UpdateItemModal(self, shop_key, item_id, items[item_id]))
                    return

                if action == "remove":
                    await safe_defer(interaction, ephemeral=True)

                    async with SHOP_LOCK:
                        shop = load_shop(shop_key)
                        items = shop["items"]
                        if item_id not in items:
                            await safe_reply(interaction, "❌ This item no longer exists.", ephemeral=True)
                            return

                        item_name = items[item_id].get("name", item_id)
                        items.pop(item_id, None)
                        save_shop(shop_key, shop)

                        if interaction.guild:
                            idx = load_index(shop_key)
                            gidx = idx.get(str(interaction.guild.id), {})
                            items_idx = gidx.get("items", {})
                            if isinstance(items_idx, dict):
                                items_idx.pop(item_id, None)
                            gidx["items"] = items_idx if isinstance(items_idx, dict) else {}
                            idx[str(interaction.guild.id)] = gidx
                            save_index(shop_key, idx)

                    if interaction.guild:
                        await self.sync_shop_messages(interaction.guild, shop_key)

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
    # Channel setup
    # ----------------------------
    async def ensure_channels(self, guild: discord.Guild):
        everyone = guild.default_role
        me = guild.me  # type: ignore

        # Orders channel (unchanged)
        orders_overwrites = {
            everyone: discord.PermissionOverwrite(view_channel=True, send_messages=False, add_reactions=False),
        }
        if me:
            orders_overwrites[me] = discord.PermissionOverwrite(
                view_channel=True, send_messages=True, manage_messages=True, read_message_history=True
            )

        # Create/update orders channel
        orders_ch = discord.utils.get(guild.text_channels, name=ORDER_LOG_CHANNEL)
        if not orders_ch:
            try:
                await guild.create_text_channel(ORDER_LOG_CHANNEL, overwrites=orders_overwrites)
            except discord.Forbidden:
                pass
        else:
            try:
                await orders_ch.edit(overwrites=orders_overwrites)
            except discord.Forbidden:
                pass

        # Per-shop channels
        for shop_key, cfg in SHOPS.items():
            shop_name = cfg["shop_channel"]
            access_name = cfg["access_channel"]

            shop_overwrites = {
                everyone: discord.PermissionOverwrite(view_channel=True, send_messages=False, add_reactions=False),
            }
            if me:
                shop_overwrites[me] = discord.PermissionOverwrite(
                    view_channel=True, send_messages=True, manage_messages=True, read_message_history=True
                )

            access_overwrites = {
                everyone: discord.PermissionOverwrite(view_channel=False),
            }
            if me:
                access_overwrites[me] = discord.PermissionOverwrite(
                    view_channel=True, send_messages=True, manage_messages=True, read_message_history=True
                )
            for role_name in ALLOWED_ROLES:
                role = discord.utils.get(guild.roles, name=role_name)
                if role:
                    access_overwrites[role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True)

            for name, overwrites in (
                (shop_name, shop_overwrites),
                (access_name, access_overwrites),
            ):
                ch = discord.utils.get(guild.text_channels, name=name)
                if not ch:
                    try:
                        await guild.create_text_channel(name, overwrites=overwrites)
                    except discord.Forbidden:
                        pass
                else:
                    try:
                        await ch.edit(overwrites=overwrites)
                    except discord.Forbidden:
                        pass

    # ----------------------------
    # Shop message sync (NO PURGE) + NO EDIT unless needed
    # ----------------------------
    async def sync_shop_messages(self, guild: discord.Guild, shop_key: str):
        shop_ch = discord.utils.get(guild.text_channels, name=SHOPS[shop_key]["shop_channel"])
        access_ch = discord.utils.get(guild.text_channels, name=SHOPS[shop_key]["access_channel"])
        if not shop_ch or not access_ch:
            return

        async with SHOP_LOCK:
            shop = load_shop(shop_key)
            items = shop["items"]

            idx = load_index(shop_key)
            gkey = str(guild.id)
            gidx = idx.setdefault(gkey, {})
            gidx.setdefault("items", {})
            items_idx = gidx["items"]
            if not isinstance(items_idx, dict):
                items_idx = {}
                gidx["items"] = items_idx

            # If empty, try recovery once
            if not items_idx:
                try:
                    await rebuild_index_from_channels(guild, shop_key)
                except Exception:
                    pass
                idx = load_index(shop_key)
                gidx = idx.setdefault(gkey, {})
                gidx.setdefault("items", {})
                items_idx = gidx["items"]
                if not isinstance(items_idx, dict):
                    items_idx = {}
                    gidx["items"] = items_idx

            # Upsert each item message (shop + access)
            for item_id, item in items.items():
                embed = build_item_embed(shop_key, item_id, item)
                out_of_stock = int(item.get("stock", 0)) <= 0

                shop_msg_id = None
                access_msg_id = None
                if item_id in items_idx and isinstance(items_idx[item_id], dict):
                    shop_msg_id = items_idx[item_id].get("shop_msg_id")
                    access_msg_id = items_idx[item_id].get("access_msg_id")

                # shop message
                shop_msg = None
                if shop_msg_id:
                    try:
                        shop_msg = await shop_ch.fetch_message(int(shop_msg_id))
                    except Exception:
                        shop_msg = None
                if shop_msg is None:
                    shop_msg = await shop_ch.send(embed=embed, view=BuyView(self, shop_key, item_id, disabled=out_of_stock))
                    shop_msg_id = shop_msg.id
                else:
                    await self.safe_edit_if_needed(
                        shop_msg,
                        embed=embed,
                        view=BuyView(self, shop_key, item_id, disabled=out_of_stock),
                    )

                # access message
                access_msg = None
                if access_msg_id:
                    try:
                        access_msg = await access_ch.fetch_message(int(access_msg_id))
                    except Exception:
                        access_msg = None
                if access_msg is None:
                    access_msg = await access_ch.send(embed=embed, view=ManageView(self, shop_key, item_id))
                    access_msg_id = access_msg.id
                else:
                    await self.safe_edit_if_needed(
                        access_msg,
                        embed=embed,
                        view=ManageView(self, shop_key, item_id),
                    )

                items_idx[item_id] = {"shop_msg_id": int(shop_msg_id), "access_msg_id": int(access_msg_id)}

            # Management message (single per shop)
            mgmt_id = gidx.get("management_msg_id")
            mgmt_msg = None
            if mgmt_id:
                try:
                    mgmt_msg = await access_ch.fetch_message(int(mgmt_id))
                except Exception:
                    mgmt_msg = None

            desired_content = f"**Shop Management ({SHOPS[shop_key]['label']})**"
            desired_view = ShopManagementView(self, shop_key)

            if mgmt_msg is None:
                mgmt_msg = await access_ch.send(desired_content, view=desired_view)
                gidx["management_msg_id"] = mgmt_msg.id
            else:
                await self.safe_edit_if_needed(
                    mgmt_msg,
                    content=desired_content,
                    embed=None,
                    view=desired_view,
                )

            gidx["items"] = items_idx
            idx[gkey] = gidx
            save_index(shop_key, idx)

    async def update_item_messages(self, guild: discord.Guild, shop_key: str, item_id: str):
        shop = load_shop(shop_key)
        items = shop["items"]
        if item_id not in items:
            await self.sync_shop_messages(guild, shop_key)
            return

        item = items[item_id]
        embed = build_item_embed(shop_key, item_id, item)
        out_of_stock = int(item.get("stock", 0)) <= 0

        idx = load_index(shop_key)
        gidx = idx.get(str(guild.id), {})
        items_idx = (gidx.get("items") or {})
        entry = items_idx.get(item_id)

        shop_ch = discord.utils.get(guild.text_channels, name=SHOPS[shop_key]["shop_channel"])
        access_ch = discord.utils.get(guild.text_channels, name=SHOPS[shop_key]["access_channel"])
        if not shop_ch or not access_ch or not isinstance(entry, dict):
            await self.sync_shop_messages(guild, shop_key)
            return

        try:
            shop_msg = await shop_ch.fetch_message(int(entry["shop_msg_id"]))
            await self.safe_edit_if_needed(shop_msg, embed=embed, view=BuyView(self, shop_key, item_id, disabled=out_of_stock))
        except Exception:
            await self.sync_shop_messages(guild, shop_key)
            return

        try:
            access_msg = await access_ch.fetch_message(int(entry["access_msg_id"]))
            await self.safe_edit_if_needed(access_msg, embed=embed, view=ManageView(self, shop_key, item_id))
        except Exception:
            await self.sync_shop_messages(guild, shop_key)
            return

    async def refresh_order_message(self, guild: discord.Guild, order_id: str):
        orders_ch = discord.utils.get(guild.text_channels, name=ORDER_LOG_CHANNEL)
        if not orders_ch:
            return

        data = load_orders()
        o = (data.get("orders") or {}).get(order_id)
        if not o or not o.get("message_id"):
            return

        try:
            msg = await orders_ch.fetch_message(int(o["message_id"]))
        except Exception:
            return

        self.register_order_view(order_id)
        await self.safe_edit_if_needed(msg, embed=build_order_embed(o), view=OrderStatusView(self, order_id))

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

        for shop_key in SHOPS.keys():
            await self.sync_shop_messages(interaction.guild, shop_key)

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
