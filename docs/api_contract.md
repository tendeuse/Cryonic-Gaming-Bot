# ArcOverlay Mission API Contract

## Public Endpoints
- `GET /api/v1/packs`
- `GET /api/v1/packs/{pack_id}`
- `GET /api/v1/missions/{mission_id}`
- `GET /api/v1/updates?since={ISO-8601}`

## Admin Endpoints (Discord Cog)
- `POST /api/v1/admin/missions`
- `POST /api/v1/admin/packs`
- `POST /api/v1/admin/missions/{mission_id}/deprecate`

Admin calls require header `x-admin-key`.

## Mission Schema (MVP)
```json
{
  "mission_id": "caldari-001",
  "revision": 3,
  "pack_id": "default-caldari",
  "title": "For the State: First Steps",
  "lore": "...",
  "faction": "CALDARI",
  "objectives": [],
  "rewards": {"ap": 50, "badge": "State Initiate"},
  "alpha_omega": "BOTH",
  "tags": ["newbro"],
  "created_at": "ISO-8601",
  "updated_at": "ISO-8601"
}
```
