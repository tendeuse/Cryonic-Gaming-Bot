from __future__ import annotations

import os
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from bot.db import MissionDB, utc_now


API_KEY = os.getenv("MISSION_ADMIN_API_KEY", "dev-admin-key")
SQLITE_PATH = os.getenv("SQLITE_PATH", "./data/missions.sqlite3")
db = MissionDB(SQLITE_PATH)
db.seed_defaults()

app = FastAPI(title="Arc Mission API", version="1.0.0")


class MissionPayload(BaseModel):
    mission_id: str
    pack_id: str
    title: str
    lore: str
    faction: str
    objectives: list[dict[str, Any]] = Field(default_factory=list)
    rewards: dict[str, Any] = Field(default_factory=dict)
    alpha_omega: str = "BOTH"
    tags: list[str] = Field(default_factory=list)
    created_at: str | None = None
    updated_at: str | None = None


class PackPayload(BaseModel):
    pack_id: str
    name: str
    description: str
    faction: str
    published: bool = False
    mission_ids: list[str] = Field(default_factory=list)


def admin_guard(x_admin_key: str | None = Header(default=None)) -> None:
    if x_admin_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid admin API key")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "time": utc_now()}


@app.get("/api/v1/packs")
def list_packs() -> dict[str, Any]:
    return {"packs": db.list_packs()}


@app.get("/api/v1/packs/{pack_id}")
def get_pack(pack_id: str) -> dict[str, Any]:
    pack = db.get_pack_with_missions(pack_id)
    if not pack:
        raise HTTPException(status_code=404, detail="Pack not found")
    return pack


@app.get("/api/v1/missions/{mission_id}")
def get_mission(mission_id: str) -> dict[str, Any]:
    mission = db.get_mission(mission_id)
    if not mission:
        raise HTTPException(status_code=404, detail="Mission not found")
    return mission


@app.get("/api/v1/updates")
def updates(since: str) -> dict[str, Any]:
    return db.updates_since(since)


@app.post("/api/v1/admin/missions", dependencies=[Depends(admin_guard)])
def create_or_edit_mission(payload: MissionPayload) -> dict[str, Any]:
    mission = payload.model_dump()
    mission.setdefault("created_at", utc_now())
    mission.setdefault("updated_at", utc_now())
    db.upsert_mission(mission, actor="discord-admin")
    return {"status": "ok", "mission_id": mission["mission_id"]}


@app.post("/api/v1/admin/packs", dependencies=[Depends(admin_guard)])
def create_or_edit_pack(payload: PackPayload) -> dict[str, Any]:
    db.upsert_pack(payload.model_dump(), actor="discord-admin")
    return {"status": "ok", "pack_id": payload.pack_id}


@app.post("/api/v1/admin/missions/{mission_id}/deprecate", dependencies=[Depends(admin_guard)])
def deprecate_mission(mission_id: str) -> dict[str, Any]:
    if not db.deprecate_mission(mission_id, actor="discord-admin"):
        raise HTTPException(status_code=404, detail="Mission not found")
    return {"status": "ok", "mission_id": mission_id, "deprecated": True}
