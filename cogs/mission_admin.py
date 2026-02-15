from __future__ import annotations

import json
import os
from typing import Any

import discord
from discord import app_commands
from discord.ext import commands
import httpx


def parse_admin_roles() -> set[int]:
    raw = os.getenv("ADMIN_ROLE_IDS", "")
    return {int(x.strip()) for x in raw.split(",") if x.strip().isdigit()}


class MissionAdmin(commands.Cog):
    """Slash-command mission management for ArcOverlay mission packs."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.api_base_url = os.getenv("API_BASE_URL", "http://127.0.0.1:8010")
        self.admin_key = os.getenv("MISSION_ADMIN_API_KEY", "dev-admin-key")
        self.allowed_roles = parse_admin_roles()

    def _authorized(self, interaction: discord.Interaction) -> bool:
        if not self.allowed_roles:
            return True
        if not isinstance(interaction.user, discord.Member):
            return False
        member_role_ids = {r.id for r in interaction.user.roles}
        return bool(self.allowed_roles & member_role_ids)

    async def _reject_if_unauthorized(self, interaction: discord.Interaction) -> bool:
        if self._authorized(interaction):
            return False
        await interaction.response.send_message("You are not authorized for mission admin commands.", ephemeral=True)
        return True

    async def _admin_post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(base_url=self.api_base_url, timeout=15.0) as client:
            response = await client.post(path, json=payload, headers={"x-admin-key": self.admin_key})
            response.raise_for_status()
            return response.json()

    @app_commands.command(name="mission_create", description="Create a mission from JSON payload")
    async def mission_create(self, interaction: discord.Interaction, mission_json: str) -> None:
        if await self._reject_if_unauthorized(interaction):
            return
        payload = json.loads(mission_json)
        result = await self._admin_post("/api/v1/admin/missions", payload)
        await interaction.response.send_message(f"Mission saved: `{result['mission_id']}`", ephemeral=True)

    @app_commands.command(name="mission_edit", description="Edit mission using full JSON payload")
    async def mission_edit(self, interaction: discord.Interaction, mission_json: str) -> None:
        if await self._reject_if_unauthorized(interaction):
            return
        payload = json.loads(mission_json)
        result = await self._admin_post("/api/v1/admin/missions", payload)
        await interaction.response.send_message(f"Mission updated: `{result['mission_id']}`", ephemeral=True)

    @app_commands.command(name="mission_publish", description="Publish/update a mission pack")
    async def mission_publish(self, interaction: discord.Interaction, pack_json: str) -> None:
        if await self._reject_if_unauthorized(interaction):
            return
        payload = json.loads(pack_json)
        payload["published"] = True
        result = await self._admin_post("/api/v1/admin/packs", payload)
        await interaction.response.send_message(f"Pack published: `{result['pack_id']}`", ephemeral=True)

    @app_commands.command(name="mission_deprecate", description="Deprecate mission by mission_id")
    async def mission_deprecate(self, interaction: discord.Interaction, mission_id: str) -> None:
        if await self._reject_if_unauthorized(interaction):
            return
        async with httpx.AsyncClient(base_url=self.api_base_url, timeout=15.0) as client:
            response = await client.post(
                f"/api/v1/admin/missions/{mission_id}/deprecate",
                headers={"x-admin-key": self.admin_key},
            )
            response.raise_for_status()
        await interaction.response.send_message(f"Mission deprecated: `{mission_id}`", ephemeral=True)

    @app_commands.command(name="mission_list", description="List mission pack IDs")
    async def mission_list(self, interaction: discord.Interaction) -> None:
        if await self._reject_if_unauthorized(interaction):
            return
        async with httpx.AsyncClient(base_url=self.api_base_url, timeout=15.0) as client:
            response = await client.get("/api/v1/packs")
            response.raise_for_status()
            packs = response.json().get("packs", [])
        lines = [f"{p['pack_id']} rev{p.get('revision', '?')} ({len(p.get('mission_ids', []))} missions)" for p in packs]
        await interaction.response.send_message("\n".join(lines) if lines else "No packs found", ephemeral=True)

    @app_commands.command(name="mission_show", description="Show mission JSON by ID")
    async def mission_show(self, interaction: discord.Interaction, mission_id: str) -> None:
        if await self._reject_if_unauthorized(interaction):
            return
        async with httpx.AsyncClient(base_url=self.api_base_url, timeout=15.0) as client:
            response = await client.get(f"/api/v1/missions/{mission_id}")
            if response.status_code == 404:
                await interaction.response.send_message("Mission not found", ephemeral=True)
                return
            response.raise_for_status()
        payload = json.dumps(response.json(), indent=2)
        await interaction.response.send_message(f"```json\n{payload[:1800]}\n```", ephemeral=True)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(MissionAdmin(bot))
