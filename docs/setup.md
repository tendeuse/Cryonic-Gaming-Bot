# Build + Run

## Overlay (.NET 8 WPF)
```bash
# from repo root on Windows with .NET 8 SDK
set EVE_SSO_CLIENT_ID=your_client_id
set EVE_SSO_REDIRECT_URI=http://127.0.0.1:5050/callback
set API_BASE_URL=http://127.0.0.1:8010

dotnet restore ArcOverlay.sln
dotnet build ArcOverlay.sln -c Release

dotnet publish src/ArcOverlay/ArcOverlay.csproj -c Release -r win-x64 -p:PublishSingleFile=true -p:SelfContained=true -o ./dist/ArcOverlay
```

## Python API + Existing Discord Bot
```bash
python -m venv .venv
. .venv/Scripts/activate  # Windows
pip install -r requirements.txt
pip install -r bot/requirements.txt
```

Environment variables:
- `EVE_SSO_CLIENT_ID`
- `EVE_SSO_REDIRECT_URI`
- `API_BASE_URL`
- `DISCORD_TOKEN`
- `ADMIN_ROLE_IDS`
- `SQLITE_PATH`
- `MISSION_ADMIN_API_KEY`

Run API (from repo root):
```bash
uvicorn bot.api_service:app --host 0.0.0.0 --port 8010 --reload
```

Run Discord bot (existing entrypoint):
```bash
python bot.py
```

`cogs/mission_admin.py` is now loaded by the existing cog auto-loader in `bot.py`.
