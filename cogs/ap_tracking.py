# =====================
# CONFIG
# =====================
import os
import asyncio
from pathlib import Path


# Railway persistent volume mount point
PERSIST_ROOT = Path(os.getenv("PERSIST_ROOT", "/data"))

# Store AP data + exports on the persistent volume
DATA_FILE = PERSIST_ROOT / "ap_data.json"
EXPORT_DIR = PERSIST_ROOT / "ap_exports"

# Ensure directories exist
PERSIST_ROOT.mkdir(parents=True, exist_ok=True)
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

# Hierarchy data (owned by arc_hierarchy.py)
# NOTE: This remains in the project filesystem by default.
# If you also want it persisted, move it under PERSIST_ROOT as well.
HIERARCHY_FILE = Path("arc_hierarchy.json")
HIERARCHY_LOG_CH = "arc-hierarchy-log"

VOICE_INTERVAL = 180        # 3 minutes
VOICE_AP = 1
CHAT_INTERVAL = 1800       # 30 minutes
CHAT_AP = 15

MIN_ACCOUNT_AGE_DAYS = 14  # Alt-account mitigation

LYCAN_ROLE = "Lycan King"

AP_CHECK_CHANNEL = "ap-check"
AP_CHECK_EMBED_TITLE = "AP Balance"
AP_CHECK_EMBED_TEXT = "Click check ap to see your point balance"
AP_CHECK_BUTTON_LABEL = "Check Balance"

META_KEY = "_meta"
AP_CHECK_MESSAGE_ID_KEY = "ap_check_message_id"
LAST_WIPE_KEY = "last_wipe_utc"

# ARC roles (used for CEO bonus eligibility and permissions)
CEO_ROLE = "ARC Security Corporation Leader"
SECURITY_ROLE = "ARC Security"

# Join bonus
JOIN_BONUS_AP = 100
JOIN_BONUS_KEY = "join_bonus_awarded"

# AP distribution log channel
AP_DISTRIBUTION_LOG_CH = "member-join-logs-points-distribute"

# Claim keys
CLAIM_IGN_KEY = "ign"
CLAIM_GAME_KEY = "game"

# Game rates
GAME_EVE = "EVE online"
GAME_WOW = "World of Warcraft"
EVE_ISK_PER_AP = 100_000
WOW_GOLD_PER_AP = 10

# ARC ranks (read from hierarchy file)
RANK_SECURITY = "security"
RANK_OFFICER = "officer"
RANK_COMMANDER = "commander"
RANK_GENERAL = "general"
RANK_DIRECTOR = "director"

RANK_ORDER = [RANK_SECURITY, RANK_OFFICER, RANK_COMMANDER, RANK_GENERAL, RANK_DIRECTOR]
RANK_INDEX = {r: i for i, r in enumerate(RANK_ORDER)}

# -------------------------
# Utility / Persistence
# -------------------------
file_lock = asyncio.Lock()

def utcnow():
    return datetime.datetime.utcnow().isoformat()

async def load():
    async with file_lock:
        if not DATA_FILE.exists():
            return {}
        try:
            return json.loads(DATA_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            # Corrupt JSON: keep a backup and reset
            backup = DATA_FILE.with_suffix(".bak")
            try:
                DATA_FILE.replace(backup)
            except Exception:
                pass
            return {}
        except Exception:
            return {}

async def save(data):
    """
    Atomic write to reduce risk of file corruption (write temp then replace).
    """
    async with file_lock:
        tmp = DATA_FILE.with_suffix(".tmp")
        payload = json.dumps(data, indent=4)
        tmp.write_text(payload, encoding="utf-8")
        tmp.replace(DATA_FILE)
