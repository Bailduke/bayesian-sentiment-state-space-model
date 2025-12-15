from functools import wraps
import time
import logging
from typing import Tuple, Sequence
from psycopg2.extensions import connection as PGConnection
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def timeit(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        try:
            return func(*args, **kwargs)
        finally:
            elapsed = time.perf_counter() - start
            logger.info(f"{func.__name__} took {elapsed:.3f}s")
    return wrapper

@timeit
def get_db_news(
    conn: PGConnection,
    max_rows: int | None = None,
    channels: Sequence[str] | None = None,
    min_unix_time: int | None = None,
    table: str = "message_sentiment",
) -> pd.DataFrame:
    """
    Fetch messages pending sentiment (i.e., not present in message_sentiment),
    with optional filters:
      - channel: only this channel if provided
      - min_unix_time: only rows with date_unix >= this value
      - max_rows: limit number of returned rows
      - table: table to check for existing rows
    """
    allowed_tables = ["message_sentiment", "message_tag"]
    if table not in allowed_tables:
        raise ValueError(f"Invalid table: {table}. Allowed tables are: {allowed_tables}")
    try:
        base_sql = f"""
        SELECT m.*
        FROM messages m
        LEFT JOIN {table} t
          ON t.channel = m.channel AND t.id = m.id
        WHERE t.channel IS NULL
        """
        params: list = []
        if channels is not None:
            if len(channels) == 0:
                return pd.DataFrame()
            # Use PostgreSQL ANY with a Python list; cast to text[] for type safety
            base_sql += " AND m.channel = ANY(%s::text[])"
            params.append(list(channels))
        if min_unix_time is not None:
            base_sql += " AND m.date_unix >= %s"
            params.append(int(min_unix_time))
        base_sql += " ORDER BY m.date_unix ASC"
        if max_rows is not None:
            base_sql += " LIMIT %s"
            params.append(int(max_rows))
        news = pd.read_sql_query(base_sql, conn, params=params)
        return news
    except Exception as e:
        logger.error(f"Error getting news: {e}")
        return pd.DataFrame()