import os
import sys
import time
from typing import List, Tuple
import logging
import psycopg2

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import ChannelPrivateError, UsernameInvalidError, FloodWaitError

from database import ensure_messages_table, get_last_saved_id, insert_rows
from fetch import fetch_new_rows
from keywords_filter import keyword_filter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_env() -> Tuple[int, str, str, str]:
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
    api_id_str = os.getenv("TELEGRAM_API_ID") or ""
    api_hash = os.getenv("TELEGRAM_API_HASH") or ""
    phone = os.getenv("TELEGRAM_PHONE") or ""
    session_name = os.getenv("TELEGRAM_SESSION_NAME") or "telegram_news"
    if not api_id_str or not api_hash:
        print("Missing TELEGRAM_API_ID or TELEGRAM_API_HASH in .env", file=sys.stderr)
        sys.exit(1)
    try:
        api_id = int(api_id_str)
    except ValueError:
        print("TELEGRAM_API_ID must be an integer", file=sys.stderr)
        sys.exit(1)
    return api_id, api_hash, phone, session_name


def read_channels(file_path: str) -> List[str]:
    if not os.path.exists(file_path):
        print(f"channels file not found: {file_path}", file=sys.stderr)
        sys.exit(1)
    channels: List[str] = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            name = line.strip()
            if not name or name.startswith("#"):
                continue
            channels.append(name)
    return channels


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--channels", default=os.path.join(os.path.dirname(__file__), "channels.txt"))
    parser.add_argument("--limit", type=int, default=None, help="Max messages per channel in this run")
    args = parser.parse_args()

    api_id, api_hash, phone, session_name = load_env()
    conn = psycopg2.connect("postgresql://ian@localhost:5432/telegram_news")
    ensure_messages_table(conn)

    channels = read_channels(args.channels)
    if not channels:
        print("No channels found in channels.txt", file=sys.stderr)
        sys.exit(1)

    client = TelegramClient(os.path.join(os.path.dirname(__file__), session_name), api_id, api_hash)
    if phone:
        client = client.start(phone=phone)
    else:
        client = client.start()

    async def runner():
        for ch in channels:
            try:
                last_id = get_last_saved_id(conn, ch)
                rows = await fetch_new_rows(client, ch, min_id=last_id, limit=args.limit)
                keyword_filtered_rows = keyword_filter(rows)
                inserted = insert_rows(conn, ch, keyword_filtered_rows)
                logger.info(f"[{ch}] +{inserted} insertedmessages (fetched {len(rows)}, passed keyword filter {len(keyword_filtered_rows)})")
            
            except FloodWaitError as e:
                logger.info(f"[{ch}] rate limited: waiting {e.seconds}s ...")
                time.sleep(e.seconds)
            except (ChannelPrivateError, UsernameInvalidError) as e:
                logger.warning(f"[{ch}] skipped: {e.__class__.__name__}: {e}")
            except Exception as e:
                logger.error(f"[{ch}] error: {e}")
    with client:
        client.loop.run_until_complete(runner())
    conn.close()


if __name__ == "__main__":
    main()


