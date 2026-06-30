"""
cogs/uiutil.py — small shared UI helpers (not a cog; see NON_COG_MODULES in bot.py).
"""
from __future__ import annotations

from typing import Optional

import discord


def _embed_sig(d: Optional[dict]) -> Optional[dict]:
    """Embed dict minus volatile fields, for change detection. ``timestamp``
    is dropped so a panel that stamps itself 'now' on every render doesn't look
    'changed' each time."""
    if not d:
        return d
    d = dict(d)
    d.pop("timestamp", None)
    return d


async def edit_if_changed(
    message: discord.Message,
    *,
    embed: Optional[discord.Embed] = None,
    view=None,
    content: Optional[str] = None,
) -> bool:
    """Edit ``message`` only if its embed/content actually differs from what's
    already on it. Returns True if an edit was issued, False if skipped.

    Why: cogs re-edit their persistent panels on every on_ready to refresh
    content, but the content is usually identical — so on each boot a burst of
    no-op edits floods Discord's per-channel edit rate limit (the 429 spam).
    Persistent button views are re-registered separately via ``bot.add_view``
    in on_ready, so skipping a no-op edit does NOT break the buttons.
    """
    try:
        if embed is not None:
            cur = message.embeds[0].to_dict() if message.embeds else None
            if _embed_sig(cur) == _embed_sig(embed.to_dict()) and content is None:
                return False
        elif content is not None:
            if message.content == content:
                return False
    except Exception:
        pass  # on any comparison issue, fall through and edit

    kwargs = {}
    if embed is not None:
        kwargs["embed"] = embed
    if view is not None:
        kwargs["view"] = view
    if content is not None:
        kwargs["content"] = content
    await message.edit(**kwargs)
    return True
