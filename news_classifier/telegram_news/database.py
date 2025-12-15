"""
Database schema/helpers for Telegram news (PostgreSQL).

date_unix is the UTC unix timestamp of the message.
"""
from typing import List
from psycopg2.extensions import connection as PGConnection


def ensure_messages_table(conn: PGConnection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            channel TEXT NOT NULL,
            id BIGINT NOT NULL,
            date_unix BIGINT,
            sender_id TEXT,
            sender TEXT,
            views BIGINT,
            forwards BIGINT,
            replies BIGINT,
            text TEXT,
            PRIMARY KEY (channel, id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date_unix)")
    conn.commit()


def get_last_saved_id(conn: PGConnection, channel: str) -> int | None:
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(id), 0) FROM messages WHERE channel = %s", (channel,))
    row = cur.fetchone()
    if row is None:
        return None
    return int(row[0])

def get_closest_timestamp_id(conn: PGConnection, timestamp: int) -> int | None:
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM messages WHERE date_unix <= %s ORDER BY date_unix ASC LIMIT 1",
        (timestamp,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return int(row[0])

def insert_rows(conn: PGConnection, channel: str, rows: List[List[str]]) -> int:
    if not rows:
        return 0

    payload = [
        (
            channel,                        # channel (table PK part 1)
            int(r[0]) if r[0] else None,    # id (table PK part 2) - Telegram message id
            int(r[1]) if r[1] else None,    # date_unix (UTC seconds since epoch)
            r[2],                           # sender_id (Telegram internal sender id)
            r[3],                           # sender (username or first_name)
            int(r[4]) if r[4] else None,    # views count
            int(r[5]) if r[5] else None,    # forwards count
            int(r[6]) if r[6] else None,    # replies count
            r[7],                           # text (sanitized message body)
        )
        for r in rows
    ]
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT INTO messages
        (channel, id, date_unix, sender_id, sender, views, forwards, replies, text)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (channel, id) DO NOTHING
        """,
        payload,
    )
    conn.commit()
    return cur.rowcount


