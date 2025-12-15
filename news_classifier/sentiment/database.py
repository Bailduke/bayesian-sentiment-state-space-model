from psycopg2.extensions import connection as PGConnection
import pandas as pd

def ensure_sentiment_table(conn: PGConnection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS message_sentiment (
            channel TEXT NOT NULL,
            id BIGINT NOT NULL,
            positive REAL NOT NULL,
            neutral REAL NOT NULL,
            negative REAL NOT NULL,
            created_at INTEGER,
            PRIMARY KEY (channel, id),
            FOREIGN KEY (channel, id) REFERENCES messages(channel, id) ON DELETE CASCADE
        )
        """
    )
    conn.commit()

def insert_sentiment_rows(conn: PGConnection, rows: pd.DataFrame) -> int:
    """
    Insert or upsert sentiment rows into message_sentiment.
    rows can be:
      - a pandas DataFrame with the same columns as the table
    Returns number of rows processed.
    """
    if rows.empty:
        return 0
        
    # Required sentiment fields; created_at is optional and will be set to now if missing
    required = ["channel", "id", "positive", "neutral", "negative"]
    missing = [c for c in required if c not in rows.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Start with required fields
    df = rows[required].copy()
    # Ensure created_at exists; fill missing with current unix time (seconds)
    import time
    now_unix = int(time.time())
    if "created_at" in rows.columns:
        df["created_at"] = rows["created_at"]
    else:
        df["created_at"] = now_unix
    # Coerce types; created_at may have NaNs -> fill with now
    df["channel"] = df["channel"].astype(str)
    df["id"] = df["id"].astype(int)
    df["positive"] = df["positive"].astype(float)
    df["neutral"] = df["neutral"].astype(float)
    df["negative"] = df["negative"].astype(float)
    df["created_at"] = df["created_at"].apply(lambda x: int(x) if pd.notna(x) else now_unix)

    sql = """
    INSERT INTO message_sentiment (channel, id, positive, neutral, negative, created_at)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT(channel, id) DO UPDATE SET
        positive=excluded.positive,
        neutral=excluded.neutral,
        negative=excluded.negative,
        created_at=COALESCE(excluded.created_at, message_sentiment.created_at)
    """
    data = list(df.itertuples(index=False, name=None))
    if not data:
        return 0
    cur = conn.cursor()
    cur.executemany(sql, data)
    conn.commit()
    return len(data)