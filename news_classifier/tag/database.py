from psycopg2.extensions import connection as PGConnection
import pandas as pd

def ensure_tag_table(conn: PGConnection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS message_tag (
            channel TEXT NOT NULL,
            id BIGINT NOT NULL,
            economics_finance_and_markets REAL NOT NULL,
            corporate_business_industry_and_innovation REAL NOT NULL,
            technology_ai_and_digital_platforms REAL NOT NULL,
            geopolitics_war_security_and_international_relations REAL NOT NULL,
            domestic_politics_elections_and_government REAL NOT NULL,
            energy_commodities_and_environment REAL NOT NULL,
            society_human_rights_and_public_health REAL NOT NULL,
            sports_entertainment_and_culture REAL NOT NULL,
            created_at BIGINT,
            PRIMARY KEY (channel, id),
            FOREIGN KEY (channel, id) REFERENCES messages(channel, id) ON DELETE CASCADE
        )
        """
    )
    conn.commit()

def insert_tag_rows(conn: PGConnection, rows: pd.DataFrame) -> int:
    """
    Insert or upsert tag rows into message_tag.
    rows can be:
      - a pandas DataFrame with the same columns as the table
    Returns number of rows processed.
    """
    if rows.empty:
        return 0
        
    # Required fields; created_at is optional and will be set to now if missing
    required = [
        "channel",
        "id",
        "economics_finance_and_markets",
        "corporate_business_industry_and_innovation",
        "technology_ai_and_digital_platforms",
        "geopolitics_war_security_and_international_relations",
        "domestic_politics_elections_and_government",
        "energy_commodities_and_environment",
        "society_human_rights_and_public_health",
        "sports_entertainment_and_culture",
    ]
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
    float_cols = required[2:]
    for c in float_cols:
        df[c] = df[c].astype(float)
    df["created_at"] = df["created_at"].apply(lambda x: int(x) if pd.notna(x) else now_unix)

    sql = """
    INSERT INTO message_tag (
        channel, id,
        economics_finance_and_markets,
        corporate_business_industry_and_innovation,
        technology_ai_and_digital_platforms,
        geopolitics_war_security_and_international_relations,
        domestic_politics_elections_and_government,
        energy_commodities_and_environment,
        society_human_rights_and_public_health,
        sports_entertainment_and_culture,
        created_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT(channel, id) DO UPDATE SET
        economics_finance_and_markets = excluded.economics_finance_and_markets,
        corporate_business_industry_and_innovation = excluded.corporate_business_industry_and_innovation,
        technology_ai_and_digital_platforms = excluded.technology_ai_and_digital_platforms,
        geopolitics_war_security_and_international_relations = excluded.geopolitics_war_security_and_international_relations,
        domestic_politics_elections_and_government = excluded.domestic_politics_elections_and_government,
        energy_commodities_and_environment = excluded.energy_commodities_and_environment,
        society_human_rights_and_public_health = excluded.society_human_rights_and_public_health,
        sports_entertainment_and_culture = excluded.sports_entertainment_and_culture,
        created_at = COALESCE(excluded.created_at, message_tag.created_at)
    """
    ordered_cols = required + ["created_at"]
    data = list(df[ordered_cols].itertuples(index=False, name=None))
    if not data:
        return 0
    cur = conn.cursor()
    cur.executemany(sql, data)
    conn.commit()
    return len(data)