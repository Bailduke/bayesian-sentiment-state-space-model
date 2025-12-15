from typing import List, Optional
from calendar import timegm
from telethon.tl.types import Message
import re
import unicodedata

_MULTISPACE_RE = re.compile(r"[ ]{2,}")

def sanitize_text(text: str) -> str:
    if not text:
        return ""
    
    # 1. Normalize (NFKC is aggressive with formatting, NFC is more standard for web)
    s = unicodedata.normalize("NFC", text) 
    
    # 2. Standardize newlines and tabs
    s = s.replace("\r\n", "\n").replace("\r", "\n").replace("\t", " ")
    
    # 3. Delete control characters (C) but keep format (Cf) and newlines
    # This allows Emojis, Chinese, Russian, etc.
    s = "".join(ch for ch in s if ch in ["\n", "\t"] or not unicodedata.category(ch).startswith("C"))
    
    # 4. Clean up spaces
    s = re.sub(r"[ ]+\n", "\n", s)
    s = _MULTISPACE_RE.sub(" ", s).strip()
    
    return s

def message_to_row(msg: Message) -> List[str]:
    """
    Each telegram is a row in the database.
    """
    # Store UTC unix timestamp for message date
    if msg.date:
        # Telethon msg.date is UTC; use timegm to avoid local TZ assumptions
        date_unix = str(int(timegm(msg.date.utctimetuple())))
    else:
        date_unix = ""
    sender_id = str(getattr(msg, "sender_id", "") or "")
    sender_name = ""
    if hasattr(msg, "sender") and msg.sender:
        sender = msg.sender
        sender_name = getattr(sender, "username", "") or getattr(sender, "first_name", "") or ""
    views = getattr(msg, "views", None)
    forwards = getattr(msg, "forwards", None)
    replies = getattr(getattr(msg, "replies", None), "replies", None)
    text = sanitize_text(msg.message or "")
    return [
        str(msg.id),
        date_unix,
        sender_id,
        sender_name,
        "" if views is None else str(views),
        "" if forwards is None else str(forwards),
        "" if replies is None else str(replies),
        text,
    ]

async def fetch_new_rows(client, channel: str, min_id: int = 0, limit: Optional[int] = None) -> List[List[str]]:
    """
    Fetch new messages from a channel with id > min_id and return rows suitable for DB insertion.
    """
    rows: List[List[str]] = []
    count = 0
    async for msg in client.iter_messages(channel, limit=None, min_id=min_id):
        if msg is None or getattr(msg, "id", None) is None:
            continue
        
        row = message_to_row(msg)
        # Empty text or text without spaces (links, single words, etc.)
        if row[-1].strip() == "" or not " " in row[-1]:
            continue
        rows.append(row)
        count += 1
        if limit and count >= limit:
            break
    return rows


