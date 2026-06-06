# cogs/role_shop.py
#
# AP ROLE SHOP — buy temporary Discord ranks/roles with AP
#
# Channels (auto-created, all hidden from "New Member"):
#   ap-role-shop       — public storefront (members browse & buy)
#   ap-role-shop-setup — admin-only (add/edit/remove role listings)
#   ap-role-shop-logs  — admin-only (refund & revert audit trail)
#
# Data files:
#   /data/role_shop.json           — role listings configured by admins
#   /data/role_shop_purchases.json — active/expired/refunded purchase records
#   /data/role_shop_index.json     — message-id tracking for embed sync
#   /data/ap_data.json             — shared AP balances (read/write)

import os
import discord
import json
import asyncio
import uuid
import datetime
from pathlib import Path
from discord.ext import commands, tasks
from discord import app_commands

# =====================
# Persistence
# =====================
PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)

ROLE_SHOP_FILE      = PERSIST_ROOT / "role_shop.json"
PURCHASES_FILE      = PERSIST_ROOT / "role_shop_purchases.json"
INDEX_FILE          = PERSIST_ROOT / "role_shop_index.json"
AP_FILE             = PERSIST_ROOT / "ap_data.json"

# =====================
# Channel & role config
# =====================
SHOP_CHANNEL   = "ap-role-shop"
SETUP_CHANNEL  = "ap-role-shop-setup"
LOGS_CHANNEL   = "ap-role-shop-logs"

NEW_MEMBER_ROLE_ID = 1419837428146901013

ADMIN_ROLES = {"Shop Steward", "ARC Security Administration Council",
               "ARC Security Corporation Leader", "Lycan King"}

ROLE_SHOP_LOCK   = asyncio.Lock()
_EDIT_SEMAPHORE  = asyncio.Semaphore(10)

# =====================
# Duration presets
# =====================
DURATION_CHOICES = [
    ("1 Day",    24),
    ("3 Days",   72),
    ("7 Days",   168),
    ("14 Days",  336),
    ("30 Days",  720),
    ("60 Days",  1440),
    ("90 Days",  2160),
    ("Permanent", 0),
]


# =====================
# Interaction helpers
# =====================
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


# =====================
# JSON helpers (atomic writes)
# =====================
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


def load_role_shop() -> dict:
    d = _load_json(ROLE_SHOP_FILE, {})
    d.setdefault("roles", {})
    return d

def save_role_shop(data: dict) -> None:
    _save_json(ROLE_SHOP_FILE, data)

def load_purchases() -> dict:
    d = _load_json(PURCHASES_FILE, {})
    d.setdefault("purchases", {})
    return d

def save_purchases(data: dict) -> None:
    _save_json(PURCHASES_FILE, data)

def load_index() -> dict:
    return _load_json(INDEX_FILE, {})

def save_index(data: dict) -> None:
    _save_json(INDEX_FILE, data)

def load_ap() -> dict:
    return _load_json(AP_FILE, {})

def save_ap(data: dict) -> None:
    _save_json(AP_FILE, data)


async def aload_role_shop() -> dict:
    return await asyncio.to_thread(load_role_shop)

async def asave_role_shop(data: dict) -> None:
    await asyncio.to_thread(save_role_shop, data)

async def aload_purchases() -> dict:
    return await asyncio.to_thread(load_purchases)

async def asave_purchases(data: dict) -> None:
    await asyncio.to_thread(save_purchases, data)

async def aload_index() -> dict:
    return await asyncio.to_thread(load_index)

async def asave_index(data: dict) -> None:
    await asyncio.to_thread(save_index, data)

async def aload_ap() -> dict:
    return await asyncio.to_thread(load_ap)

async def asave_ap(data: dict) -> None:
    await asyncio.to_thread(save_ap, data)


def utc_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def utc_now() -> datetime.datetime:
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)


def format_duration(hours: int) -> str:
    if hours == 0:
        return "Permanent"
    if hours < 24:
        return f"{hours}h"
    days = hours // 24
    rem  = hours % 24
    if rem:
        return f"{days}d {rem}h"
    return f"{days}d"


def is_admin(member: discord.abc.User | discord.Member) -> bool:
    if not isinstance(member, discord.Member):
        return False
    return any(r.name in ADMIN_ROLES for r in member.roles)


# =====================
# Embed builders
# =====================
def build_role_listing_embed(listing_id: str, listing: dict) -> discord.Embed:
    role_name = listing.get("role_name", "Unknown Role")
    price     = int(listing.get("price", 0))

    embed = discord.Embed(
        title=f"\U0001f451 {role_name}",
        color=discord.Color.gold(),
        timestamp=discord.utils.utcnow(),
    )
    embed.add_field(name="Cost", value=f"{price} AP", inline=True)
    embed.set_footer(text=f"Role Shop | id:{listing_id}")
    return embed


def build_purchase_log_embed(purchase: dict, *, action: str = "PURCHASE") -> discord.Embed:
    status   = purchase.get("status", "ACTIVE")
    colors   = {
        "PURCHASE": discord.Color.green(),
        "REFUND":   discord.Color.orange(),
        "REVERT":   discord.Color.red(),
        "EXPIRED":  discord.Color.greyple(),
    }
    embed = discord.Embed(
        title=f"Role Shop — {action}",
        color=colors.get(action, discord.Color.blurple()),
        timestamp=discord.utils.utcnow(),
    )
    embed.add_field(name="Member",       value=f"<@{purchase.get('member_id')}>", inline=True)
    embed.add_field(name="Role",         value=purchase.get("role_name", "?"),     inline=True)
    embed.add_field(name="Cost",         value=f"{purchase.get('cost', 0)} AP",    inline=True)
    embed.add_field(name="Status",       value=status,                             inline=True)
    embed.add_field(name="Purchased At", value=purchase.get("purchased_at", "?"),  inline=True)

    expires = purchase.get("expires_at")
    embed.add_field(name="Expires At", value=expires if expires else "Never", inline=True)

    if purchase.get("refunded_by"):
        embed.add_field(name="Actioned By", value=f"<@{purchase['refunded_by']}>", inline=True)
    if purchase.get("refund_reason"):
        embed.add_field(name="Reason", value=purchase["refund_reason"][:1024], inline=False)

    embed.set_footer(text=f"Purchase ID: {purchase.get('purchase_id', '?')}")
    return embed


# =====================
# Duration Select for buying
# =====================
class DurationSelect(discord.ui.Select):
    def __init__(self, cog: "RoleShopCog", listing_id: str, available_durations: list[tuple[str, int]]):
        options = []
        for label, hours in available_durations:
            options.append(discord.SelectOption(
                label=label,
                value=str(hours),
                description=f"Duration: {label}",
            ))
        super().__init__(
            placeholder="Select duration...",
            options=options,
            custom_id=f"roleshop:duration:{listing_id}",
        )
        self.cog        = cog
        self.listing_id = listing_id

    async def callback(self, interaction: discord.Interaction):
        hours = int(self.values[0])
        dur_label = format_duration(hours)

        async with ROLE_SHOP_LOCK:
            shop = await aload_role_shop()
            listing = shop["roles"].get(self.listing_id)
            if not listing:
                await safe_reply(interaction, "❌ This role listing no longer exists.", ephemeral=True)
                return
            price = int(listing.get("price", 0))
            role_name = listing.get("role_name", "Unknown")

        embed = discord.Embed(
            title=f"Confirm Purchase: {role_name}",
            description=(
                f"**Cost:** {price} AP\n"
                f"**Duration:** {dur_label}\n\n"
                "Click **Confirm** to purchase this role."
            ),
            color=discord.Color.green(),
        )
        view = ConfirmPurchaseView(self.cog, self.listing_id, hours)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


class ConfirmPurchaseView(discord.ui.View):
    def __init__(self, cog: "RoleShopCog", listing_id: str, duration_hours: int):
        super().__init__(timeout=120)
        self.cog            = cog
        self.listing_id     = listing_id
        self.duration_hours = duration_hours

    @discord.ui.button(label="Confirm Purchase", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await safe_defer(interaction, ephemeral=True)
        await self.cog.process_purchase(interaction, self.listing_id, self.duration_hours)
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await safe_reply(interaction, "Purchase cancelled.", ephemeral=True)
        self.stop()


# =====================
# Buy View (on each listing in the shop channel)
# =====================
class BuyRoleView(discord.ui.View):
    def __init__(self, cog: "RoleShopCog", listing_id: str):
        super().__init__(timeout=None)
        durations = []
        for label, hours in DURATION_CHOICES:
            durations.append((label, hours))
        self.add_item(DurationSelect(cog, listing_id, durations))


# =====================
# Admin setup views
# =====================
class SetupManagementView(discord.ui.View):
    def __init__(self, cog: "RoleShopCog"):
        super().__init__(timeout=None)
        self.cog = cog
        self.add_item(discord.ui.Button(
            label="Add Role Listing",
            style=discord.ButtonStyle.success,
            custom_id="roleshop:add_listing",
        ))


class SetupItemView(discord.ui.View):
    def __init__(self, cog: "RoleShopCog", listing_id: str):
        super().__init__(timeout=None)
        self.cog = cog
        self.add_item(discord.ui.Button(label="Edit",   style=discord.ButtonStyle.secondary, custom_id=f"roleshop:edit:{listing_id}"))
        self.add_item(discord.ui.Button(label="Remove", style=discord.ButtonStyle.danger,    custom_id=f"roleshop:remove:{listing_id}"))


# =====================
# Admin modals
# =====================
def get_roles_below_bot(guild: discord.Guild) -> list[discord.Role]:
    """Return all guild roles below the bot's highest role, excluding @everyone and managed roles."""
    me = guild.me
    if not me:
        return []
    bot_top = me.top_role
    return [
        r for r in guild.roles
        if r < bot_top
        and r != guild.default_role
        and not r.managed
    ]


class AddRoleSelectView(discord.ui.View):
    """Step 1: admin picks a role from a dropdown of roles below the bot."""
    def __init__(self, cog: "RoleShopCog", roles: list[discord.Role]):
        super().__init__(timeout=120)
        self.cog = cog
        # Discord selects max 25 options — take the top 25 by position (highest first)
        sorted_roles = sorted(roles, key=lambda r: r.position, reverse=True)[:25]
        options = [
            discord.SelectOption(label=r.name, value=str(r.id))
            for r in sorted_roles
        ]
        select = discord.ui.Select(
            placeholder="Choose a role to list...",
            options=options,
            custom_id="roleshop:add_role_select",
        )
        select.callback = self._on_select
        self.add_item(select)

    async def _on_select(self, interaction: discord.Interaction):
        role_id = int(interaction.data["values"][0])
        role    = interaction.guild.get_role(role_id) if interaction.guild else None
        if not role:
            await safe_reply(interaction, "❌ Role not found.", ephemeral=True)
            return
        await safe_send_modal(interaction, AddRolePriceModal(self.cog, role))


class AddRolePriceModal(discord.ui.Modal):
    """Step 2: admin enters the AP price for the selected role."""
    def __init__(self, cog: "RoleShopCog", role: discord.Role):
        super().__init__(title=f"Set Price — {role.name[:40]}")
        self.cog  = cog
        self.role = role

        self.price = discord.ui.TextInput(
            label="AP Cost",
            placeholder="e.g. 500",
            required=True,
        )
        self.add_item(self.price)

    async def on_submit(self, interaction: discord.Interaction):
        if not is_admin(interaction.user):
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

        async with ROLE_SHOP_LOCK:
            shop       = await aload_role_shop()
            listing_id = uuid.uuid4().hex[:10]
            shop["roles"][listing_id] = {
                "role_name":      self.role.name,
                "role_id":        str(self.role.id),
                "price":          price,
                "duration_hours": 720,
            }
            await asave_role_shop(shop)

        if interaction.guild:
            await self.cog.sync_shop_messages(interaction.guild)
        await safe_reply(interaction, f"✅ Role listing **{self.role.name}** added at **{price} AP**.", ephemeral=True)


class EditRoleSelectView(discord.ui.View):
    """Step 1 of edit: admin picks a new role (or the same one) from the dropdown."""
    def __init__(self, cog: "RoleShopCog", listing_id: str, listing: dict, roles: list[discord.Role]):
        super().__init__(timeout=120)
        self.cog        = cog
        self.listing_id = listing_id
        self.listing    = listing

        sorted_roles = sorted(roles, key=lambda r: r.position, reverse=True)[:25]
        current_id   = str(listing.get("role_id", ""))
        options = []
        for r in sorted_roles:
            options.append(discord.SelectOption(
                label=r.name, value=str(r.id),
                default=(str(r.id) == current_id),
            ))
        select = discord.ui.Select(
            placeholder="Choose a role...",
            options=options,
        )
        select.callback = self._on_select
        self.add_item(select)

    async def _on_select(self, interaction: discord.Interaction):
        role_id = int(interaction.data["values"][0])
        role    = interaction.guild.get_role(role_id) if interaction.guild else None
        if not role:
            await safe_reply(interaction, "❌ Role not found.", ephemeral=True)
            return
        await safe_send_modal(
            interaction,
            EditRolePriceModal(self.cog, self.listing_id, role, self.listing),
        )


class EditRolePriceModal(discord.ui.Modal):
    """Step 2 of edit: admin updates the AP price."""
    def __init__(self, cog: "RoleShopCog", listing_id: str, role: discord.Role, listing: dict):
        super().__init__(title=f"Edit Price — {role.name[:40]}")
        self.cog        = cog
        self.listing_id = listing_id
        self.role       = role

        self.price = discord.ui.TextInput(
            label="AP Cost",
            default=str(listing.get("price", 0)),
            required=True,
        )
        self.add_item(self.price)

    async def on_submit(self, interaction: discord.Interaction):
        if not is_admin(interaction.user):
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

        async with ROLE_SHOP_LOCK:
            shop = await aload_role_shop()
            if self.listing_id not in shop["roles"]:
                await safe_reply(interaction, "❌ Listing no longer exists.", ephemeral=True)
                return
            listing = shop["roles"][self.listing_id]
            listing["role_name"] = self.role.name
            listing["role_id"]   = str(self.role.id)
            listing["price"]     = price
            await asave_role_shop(shop)

        if interaction.guild:
            await self.cog.sync_shop_messages(interaction.guild)
        await safe_reply(interaction, f"✅ Listing updated to **{self.role.name}** at **{price} AP**.", ephemeral=True)


# =====================
# Refund reason modal
# =====================
class RefundReasonModal(discord.ui.Modal):
    def __init__(self, cog: "RoleShopCog", purchase_id: str, action: str):
        super().__init__(title=f"{'Refund' if action == 'refund' else 'Revert'} Purchase")
        self.cog         = cog
        self.purchase_id = purchase_id
        self.action      = action
        self.reason      = discord.ui.TextInput(
            label="Reason",
            style=discord.TextStyle.long,
            placeholder="Why?",
            required=True, max_length=1000,
        )
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        if not is_admin(interaction.user):
            await safe_reply(interaction, "❌ Not authorized.", ephemeral=True)
            return
        await safe_defer(interaction, ephemeral=True)

        reason = str(self.reason.value).strip()

        async with ROLE_SHOP_LOCK:
            pdata     = await aload_purchases()
            purchases = pdata["purchases"]
            p         = purchases.get(self.purchase_id)
            if not p:
                await safe_reply(interaction, "❌ Purchase not found.", ephemeral=True)
                return
            if p["status"] in ("REFUNDED", "REVERTED"):
                await safe_reply(interaction, "❌ Already processed.", ephemeral=True)
                return

            new_status = "REFUNDED" if self.action == "refund" else "REVERTED"
            p["status"]        = new_status
            p["refunded_by"]   = str(interaction.user.id)
            p["refund_reason"] = reason
            p["refunded_at"]   = utc_iso()

            if self.action == "refund":
                ap_data = await aload_ap()
                uid     = str(p["member_id"])
                entry   = ap_data.get(uid)
                if entry and "ap" in entry:
                    entry["ap"] = float(entry["ap"]) + float(p.get("cost", 0))
                    audit = entry.setdefault("audit", [])
                    audit.append({
                        "ts":       utc_iso(),
                        "delta":    float(p.get("cost", 0)),
                        "source":   "role_shop_refund",
                        "reason":   f"Refund for role {p.get('role_name', '?')} (purchase {self.purchase_id})",
                        "actor_id": interaction.user.id,
                    })
                    ap_data[uid] = entry
                    await asave_ap(ap_data)

            purchases[self.purchase_id] = p
            await asave_purchases(pdata)

        if interaction.guild:
            member = interaction.guild.get_member(int(p["member_id"]))
            if member:
                role = interaction.guild.get_role(int(p.get("role_id", 0)))
                if role and role in member.roles:
                    try:
                        await member.remove_roles(role, reason=f"Role shop {new_status.lower()}")
                    except Exception:
                        pass

            logs_ch = discord.utils.get(interaction.guild.text_channels, name=LOGS_CHANNEL)
            if logs_ch:
                log_action = "REFUND" if self.action == "refund" else "REVERT"
                embed = build_purchase_log_embed(p, action=log_action)
                try:
                    await logs_ch.send(embed=embed)
                except Exception:
                    pass

        label = "refunded (AP returned)" if self.action == "refund" else "reverted (no AP return)"
        await safe_reply(interaction, f"✅ Purchase {self.purchase_id} {label}.", ephemeral=True)


# =====================
# Cog
# =====================
class RoleShopCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot              = bot
        self._startup_done: set[int]                    = set()
        self._msg_cache: dict[int, discord.Message]     = {}
        self._sync_in_progress: set[str]                = set()

    async def cog_load(self):
        asyncio.create_task(self._startup())

    async def cog_unload(self):
        self.expiry_loop.cancel()

    # =====================
    # Startup
    # =====================
    async def _startup(self):
        await self.bot.wait_until_ready()
        await asyncio.sleep(2.5)

        self.bot.add_view(SetupManagementView(self))

        for guild in self.bot.guilds:
            asyncio.create_task(self._init_guild(guild))

        if not self.expiry_loop.is_running():
            self.expiry_loop.start()

    async def _init_guild(self, guild: discord.Guild):
        if guild.id in self._startup_done:
            return
        self._startup_done.add(guild.id)
        await self.ensure_channels(guild)
        await self.sync_shop_messages(guild)

    # =====================
    # Expiry loop — checks every 60s for expired role purchases
    # =====================
    @tasks.loop(seconds=60)
    async def expiry_loop(self):
        try:
            now = utc_now()
            async with ROLE_SHOP_LOCK:
                pdata     = await aload_purchases()
                purchases = pdata["purchases"]
                changed   = False

                for pid, p in list(purchases.items()):
                    if p.get("status") != "ACTIVE":
                        continue
                    expires_str = p.get("expires_at")
                    if not expires_str:
                        continue
                    try:
                        expires = datetime.datetime.fromisoformat(expires_str.replace("Z", "+00:00"))
                    except Exception:
                        continue
                    if now < expires:
                        continue

                    p["status"] = "EXPIRED"
                    changed     = True

                    for guild in self.bot.guilds:
                        member = guild.get_member(int(p["member_id"]))
                        if not member:
                            continue
                        role = guild.get_role(int(p.get("role_id", 0)))
                        if role and role in member.roles:
                            try:
                                await member.remove_roles(role, reason="Role shop: duration expired")
                            except Exception:
                                pass

                        logs_ch = discord.utils.get(guild.text_channels, name=LOGS_CHANNEL)
                        if logs_ch:
                            embed = build_purchase_log_embed(p, action="EXPIRED")
                            try:
                                await logs_ch.send(embed=embed)
                            except Exception:
                                pass

                if changed:
                    await asave_purchases(pdata)
        except Exception:
            pass

    @expiry_loop.before_loop
    async def before_expiry_loop(self):
        await self.bot.wait_until_ready()

    # =====================
    # Channel creation
    # =====================
    async def ensure_channels(self, guild: discord.Guild):
        everyone        = guild.default_role
        me              = guild.me
        new_member_role = guild.get_role(NEW_MEMBER_ROLE_ID)

        bot_manage = None
        if me:
            bot_manage = discord.PermissionOverwrite(
                view_channel=True, send_messages=True,
                manage_messages=True, read_message_history=True,
            )

        admin_roles = []
        for rn in ADMIN_ROLES:
            r = discord.utils.get(guild.roles, name=rn)
            if r:
                admin_roles.append(r)

        # --- ap-role-shop (public, hidden from New Member) ---
        shop_ch = discord.utils.get(guild.text_channels, name=SHOP_CHANNEL)
        if not shop_ch:
            overwrites = {
                everyone: discord.PermissionOverwrite(view_channel=True, send_messages=False, add_reactions=False),
            }
            if new_member_role:
                overwrites[new_member_role] = discord.PermissionOverwrite(view_channel=False)
            if me:
                overwrites[me] = bot_manage
            try:
                await guild.create_text_channel(SHOP_CHANNEL, overwrites=overwrites)
            except Exception:
                pass

        # --- ap-role-shop-setup (admin only, hidden from New Member & everyone) ---
        setup_ch = discord.utils.get(guild.text_channels, name=SETUP_CHANNEL)
        if not setup_ch:
            overwrites = {
                everyone: discord.PermissionOverwrite(view_channel=False),
            }
            if new_member_role:
                overwrites[new_member_role] = discord.PermissionOverwrite(view_channel=False)
            if me:
                overwrites[me] = bot_manage
            for ar in admin_roles:
                overwrites[ar] = discord.PermissionOverwrite(
                    view_channel=True, send_messages=True, read_message_history=True,
                )
            try:
                await guild.create_text_channel(SETUP_CHANNEL, overwrites=overwrites)
            except Exception:
                pass

        # --- ap-role-shop-logs (admin only, hidden from New Member & everyone) ---
        logs_ch = discord.utils.get(guild.text_channels, name=LOGS_CHANNEL)
        if not logs_ch:
            overwrites = {
                everyone: discord.PermissionOverwrite(view_channel=False),
            }
            if new_member_role:
                overwrites[new_member_role] = discord.PermissionOverwrite(view_channel=False)
            if me:
                overwrites[me] = bot_manage
            for ar in admin_roles:
                overwrites[ar] = discord.PermissionOverwrite(
                    view_channel=True, send_messages=False, read_message_history=True,
                )
            try:
                await guild.create_text_channel(LOGS_CHANNEL, overwrites=overwrites)
            except Exception:
                pass

    # =====================
    # Message cache helpers
    # =====================
    async def _get_cached_message(self, channel: discord.TextChannel, msg_id: int) -> discord.Message:
        if msg_id in self._msg_cache:
            return self._msg_cache[msg_id]
        msg = await channel.fetch_message(msg_id)
        self._msg_cache[msg_id] = msg
        return msg

    async def _try_delete_message(self, channel: discord.TextChannel, msg_id: int) -> None:
        self._msg_cache.pop(msg_id, None)
        try:
            msg = await channel.fetch_message(msg_id)
            await msg.delete()
        except Exception:
            pass

    async def safe_edit_if_needed(self, msg: discord.Message, *, content=None, embed=None, view=None) -> bool:
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
            self._msg_cache.pop(msg.id, None)
            return True
        except discord.NotFound:
            self._msg_cache.pop(msg.id, None)
            return False
        except Exception:
            return False

    async def _upsert_msg(self, channel, msg_id, embed, view) -> int:
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

    # =====================
    # Sync shop messages
    # =====================
    async def sync_shop_messages(self, guild: discord.Guild):
        sync_key = str(guild.id)
        if sync_key in self._sync_in_progress:
            return
        self._sync_in_progress.add(sync_key)

        try:
            shop_ch  = discord.utils.get(guild.text_channels, name=SHOP_CHANNEL)
            setup_ch = discord.utils.get(guild.text_channels, name=SETUP_CHANNEL)
            if not shop_ch or not setup_ch:
                return

            async with ROLE_SHOP_LOCK:
                shop  = await aload_role_shop()
                roles = shop["roles"]
                idx   = await aload_index()
                gkey  = str(guild.id)
                gidx  = idx.setdefault(gkey, {})
                gidx.setdefault("items", {})
                items_idx = gidx["items"]

            async def _sync_one(listing_id: str, listing: dict) -> tuple[str, int, int]:
                embed = build_role_listing_embed(listing_id, listing)
                entry = items_idx.get(listing_id)
                if not isinstance(entry, dict):
                    entry = {}

                shop_id, setup_id = await asyncio.gather(
                    self._upsert_msg(shop_ch,  entry.get("shop_msg_id"),  embed, BuyRoleView(self, listing_id)),
                    self._upsert_msg(setup_ch, entry.get("setup_msg_id"), embed, SetupItemView(self, listing_id)),
                )
                return listing_id, int(shop_id), int(setup_id)

            results = await asyncio.gather(
                *[_sync_one(lid, lst) for lid, lst in roles.items()],
                return_exceptions=True,
            )
            for r in results:
                if isinstance(r, Exception):
                    continue
                lid, shop_id, setup_id = r
                items_idx[lid] = {"shop_msg_id": shop_id, "setup_msg_id": setup_id}

            # Management message in setup channel
            mgmt_id      = gidx.get("management_msg_id")
            mgmt_content = "**Role Shop Management**"
            mgmt_view    = SetupManagementView(self)
            mgmt_msg     = None
            if mgmt_id:
                try:
                    mgmt_msg = await self._get_cached_message(setup_ch, int(mgmt_id))
                except Exception:
                    mgmt_msg = None
            if mgmt_msg is None:
                async with _EDIT_SEMAPHORE:
                    mgmt_msg = await setup_ch.send(mgmt_content, view=mgmt_view)
                self._msg_cache[mgmt_msg.id] = mgmt_msg
                gidx["management_msg_id"] = mgmt_msg.id
            else:
                await self.safe_edit_if_needed(mgmt_msg, content=mgmt_content, embed=None, view=mgmt_view)

            gidx["items"] = items_idx
            idx[gkey]     = gidx
            await asave_index(idx)

        finally:
            self._sync_in_progress.discard(sync_key)

    # =====================
    # Purchase processing
    # =====================
    async def process_purchase(self, interaction: discord.Interaction, listing_id: str, duration_hours: int):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await safe_reply(interaction, "❌ Must be used in a server.", ephemeral=True)
            return

        async with ROLE_SHOP_LOCK:
            shop    = await aload_role_shop()
            listing = shop["roles"].get(listing_id)
            if not listing:
                await safe_reply(interaction, "❌ This role listing no longer exists.", ephemeral=True)
                return

            price     = int(listing.get("price", 0))
            role_name = listing.get("role_name", "Unknown")
            role_id   = listing.get("role_id")

            ap_data = await aload_ap()
            uid     = str(interaction.user.id)
            entry   = ap_data.get(uid)
            if not entry or "ap" not in entry:
                await safe_reply(interaction, "❌ You have no AP account.", ephemeral=True)
                return

            user_ap = int(float(entry.get("ap", 0)))
            if user_ap < price:
                await safe_reply(interaction, f"❌ Not enough AP (cost {price}, you have {user_ap}).", ephemeral=True)
                return

            role = interaction.guild.get_role(int(role_id)) if role_id else None
            if not role:
                role = discord.utils.get(interaction.guild.roles, name=role_name)
            if not role:
                await safe_reply(interaction, f"❌ Role **{role_name}** not found in this server.", ephemeral=True)
                return

            if role in interaction.user.roles:
                await safe_reply(interaction, f"❌ You already have the **{role_name}** role.", ephemeral=True)
                return

            entry["ap"] = user_ap - price
            audit = entry.setdefault("audit", [])
            audit.append({
                "ts":       utc_iso(),
                "delta":    -price,
                "source":   "role_shop_purchase",
                "reason":   f"Purchased role {role_name} for {format_duration(duration_hours)}",
                "actor_id": interaction.user.id,
            })
            ap_data[uid] = entry
            await asave_ap(ap_data)

            now_dt      = utc_now()
            expires_dt  = None
            expires_str = None
            if duration_hours > 0:
                expires_dt  = now_dt + datetime.timedelta(hours=duration_hours)
                expires_str = expires_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")

            purchase_id = uuid.uuid4().hex[:10]
            purchase = {
                "purchase_id":    purchase_id,
                "member_id":      str(interaction.user.id),
                "member_tag":     str(interaction.user),
                "listing_id":     listing_id,
                "role_id":        str(role.id),
                "role_name":      role_name,
                "cost":           price,
                "duration_hours": duration_hours,
                "purchased_at":   utc_iso(),
                "expires_at":     expires_str,
                "status":         "ACTIVE",
                "guild_id":       str(interaction.guild.id),
            }

            pdata = await aload_purchases()
            pdata["purchases"][purchase_id] = purchase
            await asave_purchases(pdata)

        try:
            await interaction.user.add_roles(role, reason=f"Role shop purchase ({purchase_id})")
        except discord.Forbidden:
            await safe_reply(interaction, "❌ Bot lacks permission to assign this role. Contact an admin.", ephemeral=True)
            return
        except Exception as e:
            await safe_reply(interaction, f"❌ Failed to assign role: {e}", ephemeral=True)
            return

        logs_ch = discord.utils.get(interaction.guild.text_channels, name=LOGS_CHANNEL)
        if logs_ch:
            embed = build_purchase_log_embed(purchase, action="PURCHASE")
            try:
                await logs_ch.send(embed=embed)
            except Exception:
                pass

        dur_text = format_duration(duration_hours)
        await safe_reply(
            interaction,
            f"✅ You purchased **{role_name}** for **{price} AP** (duration: {dur_text}).",
            ephemeral=True,
        )

    # =====================
    # Interaction router (persistent buttons/selects)
    # =====================
    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        try:
            data      = interaction.data or {}
            custom_id = data.get("custom_id")
            if not custom_id or not isinstance(custom_id, str):
                return
            if not custom_id.startswith("roleshop:"):
                return

            parts  = custom_id.split(":")
            action = parts[1] if len(parts) >= 2 else None

            if action == "add_listing":
                if not is_admin(interaction.user):
                    await safe_reply(interaction, "❌ Not authorized.", ephemeral=True)
                    return
                if not interaction.guild:
                    await safe_reply(interaction, "❌ Must be used in a server.", ephemeral=True)
                    return
                roles = get_roles_below_bot(interaction.guild)
                if not roles:
                    await safe_reply(interaction, "❌ No assignable roles found below the bot's role.", ephemeral=True)
                    return
                view = AddRoleSelectView(self, roles)
                await interaction.response.send_message(
                    "**Select a role to add to the shop:**", view=view, ephemeral=True,
                )
                return

            if action == "edit" and len(parts) >= 3:
                listing_id = parts[2]
                if not is_admin(interaction.user):
                    await safe_reply(interaction, "❌ Not authorized.", ephemeral=True)
                    return
                if not interaction.guild:
                    await safe_reply(interaction, "❌ Must be used in a server.", ephemeral=True)
                    return
                shop = await aload_role_shop()
                listing = shop["roles"].get(listing_id)
                if not listing:
                    await safe_reply(interaction, "❌ Listing no longer exists.", ephemeral=True)
                    return
                roles = get_roles_below_bot(interaction.guild)
                if not roles:
                    await safe_reply(interaction, "❌ No assignable roles found below the bot's role.", ephemeral=True)
                    return
                view = EditRoleSelectView(self, listing_id, listing, roles)
                await interaction.response.send_message(
                    "**Select the role and update the price:**", view=view, ephemeral=True,
                )
                return

            if action == "remove" and len(parts) >= 3:
                listing_id = parts[2]
                if not is_admin(interaction.user):
                    await safe_reply(interaction, "❌ Not authorized.", ephemeral=True)
                    return
                await safe_defer(interaction, ephemeral=True)

                entry      = None
                role_name  = listing_id

                async with ROLE_SHOP_LOCK:
                    shop = await aload_role_shop()
                    if listing_id not in shop["roles"]:
                        await safe_reply(interaction, "❌ Listing no longer exists.", ephemeral=True)
                        return
                    role_name = shop["roles"][listing_id].get("role_name", listing_id)
                    shop["roles"].pop(listing_id, None)
                    await asave_role_shop(shop)

                    if interaction.guild:
                        idx       = await aload_index()
                        gidx      = idx.get(str(interaction.guild.id), {})
                        items_idx = gidx.get("items", {})
                        if isinstance(items_idx, dict):
                            entry = items_idx.pop(listing_id, None)
                        gidx["items"]                        = items_idx
                        idx[str(interaction.guild.id)]        = gidx
                        await asave_index(idx)

                if interaction.guild and isinstance(entry, dict):
                    shop_ch  = discord.utils.get(interaction.guild.text_channels, name=SHOP_CHANNEL)
                    setup_ch = discord.utils.get(interaction.guild.text_channels, name=SETUP_CHANNEL)
                    deletions = []
                    if shop_ch and entry.get("shop_msg_id"):
                        deletions.append(self._try_delete_message(shop_ch, int(entry["shop_msg_id"])))
                    if setup_ch and entry.get("setup_msg_id"):
                        deletions.append(self._try_delete_message(setup_ch, int(entry["setup_msg_id"])))
                    if deletions:
                        await asyncio.gather(*deletions, return_exceptions=True)

                await safe_reply(interaction, f"\U0001f5d1️ Removed **{role_name}** from the role shop.", ephemeral=True)
                return

            if action == "duration" and len(parts) >= 3:
                # Handled by the DurationSelect class callback
                return

        except Exception:
            return

    # =====================
    # Slash commands — admin refund/revert
    # =====================
    @app_commands.command(
        name="role_shop_refund",
        description="Refund a role purchase (returns AP and removes the role)."
    )
    @app_commands.describe(purchase_id="The purchase ID to refund")
    async def role_shop_refund(self, interaction: discord.Interaction, purchase_id: str):
        if not is_admin(interaction.user):
            await safe_reply(interaction, "❌ Not authorized.", ephemeral=True)
            return
        pdata = await aload_purchases()
        if purchase_id not in pdata.get("purchases", {}):
            await safe_reply(interaction, "❌ Purchase not found.", ephemeral=True)
            return
        await safe_send_modal(interaction, RefundReasonModal(self, purchase_id, "refund"))

    @app_commands.command(
        name="role_shop_revert",
        description="Revert a role purchase (removes the role, NO AP refund)."
    )
    @app_commands.describe(purchase_id="The purchase ID to revert")
    async def role_shop_revert(self, interaction: discord.Interaction, purchase_id: str):
        if not is_admin(interaction.user):
            await safe_reply(interaction, "❌ Not authorized.", ephemeral=True)
            return
        pdata = await aload_purchases()
        if purchase_id not in pdata.get("purchases", {}):
            await safe_reply(interaction, "❌ Purchase not found.", ephemeral=True)
            return
        await safe_send_modal(interaction, RefundReasonModal(self, purchase_id, "revert"))

    @app_commands.command(
        name="role_shop_rebuild",
        description="Re-sync all role shop messages (admin only)."
    )
    async def role_shop_rebuild(self, interaction: discord.Interaction):
        if not is_admin(interaction.user):
            await safe_reply(interaction, "❌ Not authorized.", ephemeral=True)
            return
        await safe_defer(interaction, ephemeral=True)
        if interaction.guild:
            await self.ensure_channels(interaction.guild)
            await self.sync_shop_messages(interaction.guild)
        await safe_reply(interaction, "✅ Role shop synced.", ephemeral=True)

    @app_commands.command(
        name="role_shop_purchases",
        description="View active role purchases for a member (admin only)."
    )
    @app_commands.describe(member="The member to check")
    async def role_shop_purchases(self, interaction: discord.Interaction, member: discord.Member):
        if not is_admin(interaction.user):
            await safe_reply(interaction, "❌ Not authorized.", ephemeral=True)
            return
        await safe_defer(interaction, ephemeral=True)

        pdata     = await aload_purchases()
        purchases = pdata.get("purchases", {})
        member_ps = [
            (pid, p) for pid, p in purchases.items()
            if str(p.get("member_id")) == str(member.id)
        ]

        if not member_ps:
            await safe_reply(interaction, f"No role shop purchases found for {member.mention}.", ephemeral=True)
            return

        lines = []
        for pid, p in sorted(member_ps, key=lambda x: x[1].get("purchased_at", ""), reverse=True):
            status  = p.get("status", "?")
            role    = p.get("role_name", "?")
            cost    = p.get("cost", 0)
            expires = p.get("expires_at", "Never")
            lines.append(f"`{pid}` | **{role}** | {cost} AP | {status} | Expires: {expires}")

        text = "\n".join(lines[:25])
        embed = discord.Embed(
            title=f"Role Purchases — {member}",
            description=text,
            color=discord.Color.blurple(),
        )
        await safe_reply(interaction, embed=embed, ephemeral=True)

    @role_shop_purchases.error
    async def role_shop_purchases_error(self, interaction, error):
        if isinstance(error, app_commands.CheckFailure):
            await safe_reply(interaction, "❌ Not authorized.", ephemeral=True)


# Patch safe_reply to accept embeds
_original_safe_reply = safe_reply
async def safe_reply(interaction: discord.Interaction, content: str = None, *, embed: discord.Embed = None, ephemeral: bool = True) -> None:
    try:
        kwargs = {"ephemeral": ephemeral}
        if content:
            kwargs["content"] = content
        if embed:
            kwargs["embed"] = embed
        if interaction.response.is_done():
            await interaction.followup.send(**kwargs)
        else:
            await interaction.response.send_message(**kwargs)
    except Exception:
        pass


async def setup(bot: commands.Bot):
    await bot.add_cog(RoleShopCog(bot))
